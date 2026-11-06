"""merge integrations and detectflow column rename heads

Revision ID: 555c40b973b3
Revises: c64935ba9742, e1f2a3b4c5d6
Create Date: 2026-03-24 13:19:03.213459

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "555c40b973b3"
down_revision: str | Sequence[str] | None = ("c64935ba9742", "e1f2a3b4c5d6")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Merge revision: no schema changes (unifies migration branches)."""
    pass


def downgrade() -> None:
    """Merge revision: no schema changes."""
    pass
