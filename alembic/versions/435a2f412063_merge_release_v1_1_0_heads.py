"""merge release-v1.1.0 heads

Revision ID: 435a2f412063
Revises: c64935ba9742, d37b8532eba5
Create Date: 2026-04-17 12:36:39.270444

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "435a2f412063"
down_revision: str | Sequence[str] | None = ("c64935ba9742", "d37b8532eba5")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
