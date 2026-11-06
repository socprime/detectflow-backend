"""allow a detection pipeline to join several correlation pipelines

Revision ID: d8e9f0a1b2c3
Revises: 01c72bc6134a
Create Date: 2026-08-25 15:50:00.000000

Drops ``uq_correlation_pipeline_members_pipeline_id`` so the same detection pipeline can be a
member of more than one correlation pipeline. Each job independently consumes that member's
tagged-events topic; duplicate chains are expected. The composite primary key
``(correlation_pipeline_id, pipeline_id)`` still prevents attaching the same pipeline twice to
one job. A non-unique index keeps the reverse ``pipeline_id`` lookup.

"""

from collections.abc import Sequence

from alembic import op

revision: str = "d8e9f0a1b2c3"
down_revision: str | Sequence[str] | None = "01c72bc6134a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        "uq_correlation_pipeline_members_pipeline_id",
        "correlation_pipeline_members",
        type_="unique",
    )
    op.create_index(
        "ix_correlation_pipeline_members_pipeline_id",
        "correlation_pipeline_members",
        ["pipeline_id"],
    )


def downgrade() -> None:
    """Downgrade schema.

    Fails if any detection pipeline is already a member of more than one correlation pipeline.
    """
    op.drop_index("ix_correlation_pipeline_members_pipeline_id", table_name="correlation_pipeline_members")
    op.create_unique_constraint(
        "uq_correlation_pipeline_members_pipeline_id",
        "correlation_pipeline_members",
        ["pipeline_id"],
    )
