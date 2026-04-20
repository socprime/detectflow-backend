"""rename flink_version to detectflow_matchnode_version

Revision ID: c64935ba9742
Revises: 7c05c641f5a0
Create Date: 2026-02-23 19:13:31.330604

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c64935ba9742"
down_revision: str | Sequence[str] | None = "7c05c641f5a0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Rename flink_version to detectflow_matchnode_version."""
    op.alter_column(
        "pipelines",
        "flink_version",
        new_column_name="detectflow_matchnode_version",
    )


def downgrade() -> None:
    """Rename detectflow_matchnode_version back to flink_version."""
    op.alter_column(
        "pipelines",
        "detectflow_matchnode_version",
        new_column_name="flink_version",
    )
