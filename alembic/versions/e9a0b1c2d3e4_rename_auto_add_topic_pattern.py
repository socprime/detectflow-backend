"""rename auto_add_topic_pattern to auto_add_pipeline_name_pattern

Revision ID: e9a0b1c2d3e4
Revises: d8e9f0a1b2c3
Create Date: 2026-08-31 16:00:00.000000

Auto-add now globs a new detection pipeline's **name**, not its SIEM destination topics.
The stored glob strings are unchanged; only the column name and CHECK identifier move.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e9a0b1c2d3e4"
down_revision: str | Sequence[str] | None = "d8e9f0a1b2c3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        "ck_correlation_pipelines_auto_add_topic_pattern",
        "correlation_pipelines",
        type_="check",
    )
    op.alter_column(
        "correlation_pipelines",
        "auto_add_topic_pattern",
        new_column_name="auto_add_pipeline_name_pattern",
    )
    op.create_check_constraint(
        "ck_correlation_pipelines_auto_add_pipeline_name_pattern",
        "correlation_pipelines",
        "auto_add_pipeline_name_pattern IS NULL OR length(btrim(auto_add_pipeline_name_pattern)) > 0",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "ck_correlation_pipelines_auto_add_pipeline_name_pattern",
        "correlation_pipelines",
        type_="check",
    )
    op.alter_column(
        "correlation_pipelines",
        "auto_add_pipeline_name_pattern",
        new_column_name="auto_add_topic_pattern",
    )
    op.create_check_constraint(
        "ck_correlation_pipelines_auto_add_topic_pattern",
        "correlation_pipelines",
        "auto_add_topic_pattern IS NULL OR length(btrim(auto_add_topic_pattern)) > 0",
    )
