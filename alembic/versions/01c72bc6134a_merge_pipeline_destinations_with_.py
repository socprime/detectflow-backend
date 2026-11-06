"""merge pipeline destinations with correlation pipelines chain

Revision ID: 01c72bc6134a
Revises: 2707429c4545, a73eaf194b32
Create Date: 2026-08-25 12:00:51.677750

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "01c72bc6134a"
down_revision: str | Sequence[str] | None = ("2707429c4545", "a73eaf194b32")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
