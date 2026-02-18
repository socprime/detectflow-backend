"""add_external_repos

Revision ID: 340ed7d7acbb
Revises: 30ea38f0b5c5
Create Date: 2026-01-14 10:37:55.534895

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "340ed7d7acbb"
down_revision: str | Sequence[str] | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # Add columns to repositories table
    op.add_column("repositories", sa.Column("sync_enabled", sa.Boolean(), nullable=True))
    op.add_column("repositories", sa.Column("source_link", sa.String(length=512), nullable=True))
    # Note: Indexes ix_pipeline_rule_metrics_pipeline_id and ix_pipeline_rule_metrics_rule_id
    # already exist from initial_schema migration, so they are not recreated here


def downgrade() -> None:
    """Downgrade schema."""
    # Remove columns from repositories table
    op.drop_column("repositories", "source_link")
    op.drop_column("repositories", "sync_enabled")
    # Note: Indexes are not dropped here as they were created in initial_schema migration
