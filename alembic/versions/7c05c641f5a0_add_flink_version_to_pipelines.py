"""add flink_version to pipelines

Revision ID: 7c05c641f5a0
Revises: f018bdf92ae6
Create Date: 2026-02-20 13:37:07.830010

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c05c641f5a0"
down_revision: str | Sequence[str] | None = "b8a9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add flink_version column to store last known Flink job version."""
    op.add_column("pipelines", sa.Column("flink_version", sa.String(length=50), nullable=True))


def downgrade() -> None:
    """Remove flink_version column."""
    op.drop_column("pipelines", "flink_version")
