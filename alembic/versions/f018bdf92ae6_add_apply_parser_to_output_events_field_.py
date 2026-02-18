"""add apply_parser_to_output_events field to pipeline

Revision ID: f018bdf92ae6
Revises: ad3013f0bf85
Create Date: 2026-02-04 16:31:47.627592

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f018bdf92ae6"
down_revision: str | Sequence[str] | None = "ad3013f0bf85"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "pipelines", sa.Column("apply_parser_to_output_events", sa.Boolean(), server_default="false", nullable=True)
    )
    # Backfill existing rows
    op.execute("UPDATE pipelines SET apply_parser_to_output_events = false WHERE apply_parser_to_output_events IS NULL")


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("pipelines", "apply_parser_to_output_events")
