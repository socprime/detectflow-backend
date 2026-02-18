"""rename window_size_seconds to window_size_sec

Revision ID: ad3013f0bf85
Revises: d1e2f3a4b5c6
Create Date: 2026-01-30 12:51:24.252106

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ad3013f0bf85"
down_revision: str | Sequence[str] | None = "d1e2f3a4b5c6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column(
        "pipelines",
        "window_size_seconds",
        new_column_name="window_size_sec",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column(
        "pipelines",
        "window_size_sec",
        new_column_name="window_size_seconds",
    )
