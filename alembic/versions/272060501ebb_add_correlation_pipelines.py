"""add correlation pipelines

Revision ID: 272060501ebb
Revises: 757bde054750
Create Date: 2026-08-11 15:09:20.111334

Purely additive: two new tables, two new nullable columns and one dropped CHECK. No existing
row is rewritten, no Kafka topic is created and nothing reads the new columns yet — the
singleton ``attack_chain_configuration`` row keeps driving production until the data-migration
task runs. Only the singleton CHECK is dropped here; the table, its row and its model survive.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "272060501ebb"
down_revision: str | Sequence[str] | None = "757bde054750"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (column suffix, sa type, short CHECK-name suffix, bounds SQL)
_RESOURCE_SPEC = [
    ("parallelism", sa.Integer, "parallelism", "BETWEEN 1 AND 64"),
    ("taskmanager_memory_mb", sa.Integer, "tm_memory_mb", "BETWEEN 1024 AND 65536"),
    ("taskmanager_cpu", sa.Float, "tm_cpu", "> 0 AND {col} <= 32"),
    ("jobmanager_memory_mb", sa.Integer, "jm_memory_mb", "BETWEEN 512 AND 32768"),
    ("jobmanager_cpu", sa.Float, "jm_cpu", "> 0 AND {col} <= 16"),
]


def _resource_columns(prefix: str) -> list[sa.Column]:
    return [sa.Column(f"{prefix}_{suffix}", col_type(), nullable=True) for suffix, col_type, _, _ in _RESOURCE_SPEC]


def _resource_checks(prefix: str) -> list[sa.CheckConstraint]:
    """Five bounds CHECKs per job. NULL is always allowed = inherit the settings default."""
    checks = []
    for suffix, _, short, bounds in _RESOURCE_SPEC:
        col = f"{prefix}_{suffix}"
        checks.append(
            sa.CheckConstraint(
                f"{col} IS NULL OR ({col} {bounds.format(col=col)})",
                name=f"ck_correlation_pipelines_{prefix}_{short}",
            )
        )
    return checks


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "correlation_pipelines",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("jobs_status", sa.String(length=20), server_default="stopped", nullable=False),
        sa.Column("threshold_pct", sa.Integer(), server_default="60", nullable=False),
        # NULL = inherit each HoS rule's detection.timeframe (architecture §19).
        sa.Column("correlation_window_seconds", sa.Integer(), nullable=True),
        sa.Column("auto_add_new_topics", sa.Boolean(), server_default="false", nullable=False),
        # "Topic pattern name": NULL/absent = auto-add matches every new destination topic
        # (today's behaviour). Shell-style glob, see apps/core/constants.py.
        sa.Column("auto_add_topic_pattern", sa.String(length=255), nullable=True),
        *_resource_columns("correlation"),
        *_resource_columns("agent"),
        sa.Column("correlation_deployment_name", sa.String(length=63), nullable=True),
        sa.Column("agent_deployment_name", sa.String(length=63), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.CheckConstraint(
            "threshold_pct >= 50 AND threshold_pct <= 100 AND threshold_pct % 5 = 0",
            name="ck_correlation_pipelines_threshold_pct",
        ),
        sa.CheckConstraint(
            "correlation_window_seconds IS NULL OR "
            "(correlation_window_seconds >= 30 AND correlation_window_seconds <= 10800)",
            name="ck_correlation_pipelines_window_seconds",
        ),
        sa.CheckConstraint(
            "jobs_status IN ('starting', 'stopping', 'stopped')",
            name="ck_correlation_pipelines_jobs_status",
        ),
        sa.CheckConstraint(
            "auto_add_topic_pattern IS NULL OR length(btrim(auto_add_topic_pattern)) > 0",
            name="ck_correlation_pipelines_auto_add_topic_pattern",
        ),
        *_resource_checks("correlation"),
        *_resource_checks("agent"),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_correlation_pipelines_name"),
    )

    op.create_table(
        "correlation_pipeline_members",
        sa.Column("correlation_pipeline_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("pipeline_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["correlation_pipeline_id"], ["correlation_pipelines.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pipeline_id"], ["pipelines.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("correlation_pipeline_id", "pipeline_id"),
        # A detection pipeline belongs to at most ONE correlation pipeline.
        # Its index also serves the reverse ``pipeline_id`` lookup, so no extra index.
        sa.UniqueConstraint("pipeline_id", name="uq_correlation_pipeline_members_pipeline_id"),
    )

    op.add_column("attack_chains", sa.Column("correlation_pipeline_id", postgresql.UUID(as_uuid=True), nullable=True))
    op.create_index("ix_attack_chains_correlation_pipeline_id", "attack_chains", ["correlation_pipeline_id"])
    op.create_foreign_key(
        "fk_attack_chains_correlation_pipeline_id",
        "attack_chains",
        "correlation_pipelines",
        ["correlation_pipeline_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # Added EMPTY on purpose — NULL means "still on the legacy shared tagged-events topic".
    # No backfill here: the explicit assign endpoint is the only sanctioned writer, and the only
    # point where a customer's detection pipeline restarts. UNIQUE over a nullable column is safe:
    # Postgres permits any number of NULLs.
    op.add_column("pipelines", sa.Column("tagged_events_topic", sa.String(length=255), nullable=True))
    op.create_unique_constraint("uq_pipelines_tagged_events_topic", "pipelines", ["tagged_events_topic"])

    # De-pin "there is exactly one correlation config". The table, its row and its model all
    # survive this epic; deprecation and the eventual DROP are separate, later tasks.
    op.drop_constraint("ck_attack_chain_configuration_singleton_id", "attack_chain_configuration", type_="check")


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text("DELETE FROM attack_chain_configuration WHERE id <> 1"))
    op.create_check_constraint("ck_attack_chain_configuration_singleton_id", "attack_chain_configuration", "id = 1")

    op.drop_constraint("uq_pipelines_tagged_events_topic", "pipelines", type_="unique")
    op.drop_column("pipelines", "tagged_events_topic")

    op.drop_constraint("fk_attack_chains_correlation_pipeline_id", "attack_chains", type_="foreignkey")
    op.drop_index("ix_attack_chains_correlation_pipeline_id", table_name="attack_chains")
    op.drop_column("attack_chains", "correlation_pipeline_id")

    op.drop_table("correlation_pipeline_members")
    op.drop_table("correlation_pipelines")
