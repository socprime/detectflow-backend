"""add rule versioning

Revision ID: c4f2cc774378
Revises: b0c1d2e3f4a5, b1c2d3e4f5a6, c2d3e4f5a6b7
Create Date: 2026-03-27 12:53:51.125595

Move rule body into separate rule_content table with versioning support.
- Create rule_content table to store all versions of rule bodies
- Add version column to rules table
- Migrate existing body data from rules to rule_content
- Drop body column from rules

NOTE ON ORDERING: this migration drops rules.body, so it MUST run after every
migration that still reads rules.body - in particular b3c4d5e6f7a8 (add rule
sigma uuid), whose backfill does `SELECT id, body FROM rules`. It is therefore
parented on all of the prior heads, making it the single final head; this both
guarantees the ordering and merges the previously divergent branches.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c4f2cc774378"
down_revision: str | Sequence[str] | None = ("b0c1d2e3f4a5", "b1c2d3e4f5a6", "c2d3e4f5a6b7")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create rule_content table and migrate body data from rules."""
    op.create_table(
        "rule_content",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("rule_id", sa.UUID(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column("changed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("change_source", sa.String(length=50), nullable=True),
        sa.Column("changed_by", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["rule_id"], ["rules.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("rule_id", "version", name="uq_rule_content_rule_version"),
    )

    op.add_column("rules", sa.Column("version", sa.Integer(), server_default="1", nullable=False))

    op.execute(
        sa.text(
            """
            INSERT INTO rule_content (id, rule_id, version, body, content_hash, changed_at)
            SELECT
                gen_random_uuid(),
                id,
                1,
                body,
                encode(sha256(convert_to(body, 'UTF8')), 'hex'),
                COALESCE(updated, created, now())
            FROM rules
            WHERE body IS NOT NULL
            """
        )
    )

    op.drop_column("rules", "body")


def downgrade() -> None:
    """Restore body column to rules and drop rule_content table."""
    op.add_column("rules", sa.Column("body", sa.Text(), nullable=True))

    op.execute(
        sa.text(
            """
            UPDATE rules r
            SET body = rc.body
            FROM rule_content rc
            WHERE rc.rule_id = r.id AND rc.version = r.version
            """
        )
    )

    op.execute(sa.text("UPDATE rules SET body = '' WHERE body IS NULL"))
    op.alter_column("rules", "body", nullable=False)

    op.drop_column("rules", "version")
    op.drop_table("rule_content")
