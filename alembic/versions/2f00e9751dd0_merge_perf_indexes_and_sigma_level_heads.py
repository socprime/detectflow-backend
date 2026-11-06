"""merge perf indexes and sigma level heads

Revision ID: 2f00e9751dd0
Revises: a1b2c3d4e5f7, bf9230603f85
Create Date: 2026-08-25 08:54:15.393299

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "2f00e9751dd0"
down_revision: str | Sequence[str] | None = ("a1b2c3d4e5f7", "bf9230603f85")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
