"""add performance indexes: pg_trgm for ILIKE search, attack_chains sort/filter

- `GET /rules` ILIKE search seq-scans the whole table on every request, with a
  pathological worst case on low-selectivity terms — a trigram GIN index makes
  the plan stable regardless of the search term (patterns >= 3 chars).
- `attack_chains` has no index for its list sortings (`last_seen`/`first_seen`
  DESC) nor for the `lower(status)` filter/count. Harmless while the table is
  small (the planner keeps seq-scanning), but the table grows unbounded from
  the correlation job — these are preventive for large volumes.

Plain (non-CONCURRENT) CREATE INDEX is intentional: alembic migrations run at
deploy time inside a transaction, and CONCURRENTLY cannot run in a transaction
block.

Revision ID: a1b2c3d4e5f7
Revises: 757bde054750
Create Date: 2026-08-12 17:40:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "a1b2c3d4e5f7"
down_revision: str | Sequence[str] | None = "757bde054750"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Trigram index for the rules name search (leading-wildcard ILIKE).
    # CREATE EXTENSION may require elevated privileges depending on the
    # deployment's DB role — degrade gracefully instead of failing the whole
    # deploy: without the extension the search simply keeps its current plan.
    op.execute(
        """
        DO $$
        BEGIN
            CREATE EXTENSION IF NOT EXISTS pg_trgm;
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'pg_trgm: insufficient privileges to create the extension; skipping trigram index';
        END
        $$
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
                CREATE INDEX IF NOT EXISTS ix_rules_name_trgm ON rules USING gin (name gin_trgm_ops);
            END IF;
        END
        $$
        """
    )

    # attack_chains list: sort columns (DESC scans) and the lower(status) filter.
    op.execute("CREATE INDEX IF NOT EXISTS ix_attack_chains_last_seen_desc ON attack_chains (last_seen DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_attack_chains_first_seen_desc ON attack_chains (first_seen DESC)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_attack_chains_status_lower ON attack_chains (lower(status))")
    # Partial index for the most common list query: non-filtered-out chains,
    # newest first (matches show_filtered_out=false + ORDER BY last_seen DESC).
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_attack_chains_active_last_seen "
        "ON attack_chains (last_seen DESC) WHERE lower(status) <> 'filtered-out'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_attack_chains_active_last_seen")
    op.execute("DROP INDEX IF EXISTS ix_attack_chains_status_lower")
    op.execute("DROP INDEX IF EXISTS ix_attack_chains_first_seen_desc")
    op.execute("DROP INDEX IF EXISTS ix_attack_chains_last_seen_desc")
    op.execute("DROP INDEX IF EXISTS ix_rules_name_trgm")
    # pg_trgm extension is left installed: other objects may depend on it.
