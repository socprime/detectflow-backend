"""merge pipeline destinations with perf and sigma level heads

Revision ID: 2707429c4545
Revises: 2f00e9751dd0, a3b4c5d6e7f8
Create Date: 2026-08-25 11:27:55.455174

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "2707429c4545"
down_revision: str | Sequence[str] | None = ("2f00e9751dd0", "a3b4c5d6e7f8")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
