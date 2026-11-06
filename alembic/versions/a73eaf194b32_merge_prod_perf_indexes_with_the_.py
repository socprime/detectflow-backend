"""merge prod perf indexes with the correlation pipelines chain

Empty on purpose: `prod` grew the trigram / attack-chains performance indexes while this branch
grew the correlation-pipeline tables, and the two touch disjoint schema. This revision only
rejoins the graph so the deploy has a single head — two heads make the PostgreSQL "Database
Relevance Check" report ERROR because the stamped revision can never equal `head`.

Revision ID: a73eaf194b32
Revises: a1b2c3d4e5f7, c7e1f04b93aa
Create Date: 2026-08-17 18:12:44.000000

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "a73eaf194b32"
down_revision: str | Sequence[str] | None = ("a1b2c3d4e5f7", "c7e1f04b93aa")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
