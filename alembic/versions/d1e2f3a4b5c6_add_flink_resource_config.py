"""Add Flink resource configuration columns to pipelines

Revision ID: d1e2f3a4b5c6
Revises: c9696e1b02f7
Create Date: 2026-01-16

Adds columns for user-configurable Flink resource parameters:
- taskmanager_memory_mb: Memory per TaskManager (MB)
- taskmanager_cpu: CPU cores per TaskManager
- checkpoint_interval_sec: Checkpoint interval (seconds)
- autoscaler_enabled: Enable Flink autoscaling
- autoscaler_min_parallelism: Minimum parallelism for autoscaler
- autoscaler_max_parallelism: Maximum parallelism for autoscaler

Note: parallelism and window_size_seconds already exist in the table.
All new columns are nullable - NULL means "use global default".
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "d1e2f3a4b5c6"
down_revision: str | Sequence[str] | None = "c9696e1b02f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add Flink resource configuration columns to pipelines table."""
    # TaskManager resources
    op.add_column(
        "pipelines",
        sa.Column(
            "taskmanager_memory_mb",
            sa.Integer(),
            nullable=True,
            comment="Memory per TaskManager in MB (NULL = use default)",
        ),
    )
    op.add_column(
        "pipelines",
        sa.Column(
            "taskmanager_cpu",
            sa.Float(),
            nullable=True,
            comment="CPU cores per TaskManager (NULL = use default)",
        ),
    )

    # Checkpointing
    op.add_column(
        "pipelines",
        sa.Column(
            "checkpoint_interval_sec",
            sa.Integer(),
            nullable=True,
            comment="Checkpoint interval in seconds (NULL = use default)",
        ),
    )

    # Autoscaler settings
    op.add_column(
        "pipelines",
        sa.Column(
            "autoscaler_enabled",
            sa.Boolean(),
            nullable=True,
            comment="Enable Flink autoscaling (NULL = use default)",
        ),
    )
    op.add_column(
        "pipelines",
        sa.Column(
            "autoscaler_min_parallelism",
            sa.Integer(),
            nullable=True,
            comment="Minimum parallelism for autoscaler (NULL = use default)",
        ),
    )
    op.add_column(
        "pipelines",
        sa.Column(
            "autoscaler_max_parallelism",
            sa.Integer(),
            nullable=True,
            comment="Maximum parallelism for autoscaler (NULL = use default)",
        ),
    )


def downgrade() -> None:
    """Remove Flink resource configuration columns from pipelines table."""
    op.drop_column("pipelines", "autoscaler_max_parallelism")
    op.drop_column("pipelines", "autoscaler_min_parallelism")
    op.drop_column("pipelines", "autoscaler_enabled")
    op.drop_column("pipelines", "checkpoint_interval_sec")
    op.drop_column("pipelines", "taskmanager_cpu")
    op.drop_column("pipelines", "taskmanager_memory_mb")
