"""add_critical_indexes_enabled_pipeline_rules

Revision ID: bcd6a58fca88
Revises: f018bdf92ae6
Create Date: 2026-03-05 15:07:42.870206

Critical indexes for dashboard performance:
1. ix_pipelines_enabled - speeds up WHERE enabled=true queries (used 6+ times per dashboard load)
2. ix_pipeline_rules_composite - speeds up rule management queries with (pipeline_id, rule_id)
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "bcd6a58fca88"
down_revision: str | Sequence[str] | None = "f018bdf92ae6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add critical indexes for dashboard and rule management performance."""
    # Index on Pipeline.enabled - critical for dashboard queries
    # SELECT * FROM pipelines WHERE enabled = true (called 6+ times per dashboard refresh)
    op.create_index(
        "ix_pipelines_enabled",
        "pipelines",
        ["enabled"],
    )

    # Composite index on PipelineRule(pipeline_id, rule_id)
    # Optimizes queries like: WHERE pipeline_id = ? AND rule_id IN (...)
    op.create_index(
        "ix_pipeline_rules_composite",
        "pipeline_rules",
        ["pipeline_id", "rule_id"],
    )


def downgrade() -> None:
    """Remove added indexes."""
    op.drop_index("ix_pipeline_rules_composite", table_name="pipeline_rules")
    op.drop_index("ix_pipelines_enabled", table_name="pipelines")
