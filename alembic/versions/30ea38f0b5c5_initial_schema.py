"""initial_schema

Revision ID: 30ea38f0b5c5
Revises:
Create Date: 2026-01-06 12:41:22.512174

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "30ea38f0b5c5"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create all tables for initial schema."""
    # === Independent tables (no foreign keys) ===

    # users
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("email", sa.String(255), nullable=False),
        sa.Column("password", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true"),
        sa.Column("role", sa.String(50), nullable=False, server_default="user"),
        sa.Column("must_change_password", sa.Boolean(), server_default="false"),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("email", name="users_email_key"),
    )

    # repositories
    op.create_table(
        "repositories",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("type", sa.String(50), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("last_synced_rule_date", sa.DateTime(timezone=True), nullable=True),
    )

    # filters
    op.create_table(
        "filters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default="true"),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
    )

    # log_sources
    op.create_table(
        "log_sources",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("parsing_script", sa.Text(), nullable=True),
        sa.Column("parsing_config", postgresql.JSONB(), nullable=True),
        sa.Column("mapping", sa.Text(), nullable=True),
        sa.Column("test_topics", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("test_repository_ids", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
    )

    # parsers
    op.create_table(
        "parsers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("parser_query", sa.Text(), nullable=False),
        sa.Column("parser_config", postgresql.JSONB(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # config
    op.create_table(
        "config",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("key", sa.String(255), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("key", name="config_key_key"),
    )

    # events
    op.create_table(
        "events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("message", sa.String(500), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("type", sa.String(50), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # === Tables with foreign keys ===

    # rules (FK -> repositories)
    op.create_table(
        "rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "repository_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("repositories.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("product", sa.String(255), nullable=True),
        sa.Column("service", sa.String(255), nullable=True),
        sa.Column("category", sa.String(255), nullable=True),
    )
    op.create_index("ix_rules_repository_id", "rules", ["repository_id"])

    # pipelines (FK -> log_sources)
    op.create_table(
        "pipelines",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true"),
        sa.Column("source_topic", sa.String(255), nullable=False),
        sa.Column("destination_topic", sa.String(255), nullable=False),
        sa.Column("save_untagged", sa.Boolean(), server_default="false"),
        sa.Column("filters", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), server_default="{}"),
        sa.Column(
            "log_source_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("log_sources.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("custom_fields", sa.Text(), nullable=True),
        sa.Column("events_tagged", sa.BigInteger(), server_default="0"),
        sa.Column("events_untagged", sa.BigInteger(), server_default="0"),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rules_topic", sa.String(255), server_default="sigma-rules"),
        sa.Column("metrics_topic", sa.String(255), server_default="rule-statistics"),
        sa.Column("parallelism", sa.Integer(), server_default="1"),
        sa.Column("window_size_seconds", sa.Integer(), server_default="30"),
        sa.Column("status", sa.String(20), server_default="reconciling"),
        sa.Column("deployment_name", sa.String(255), nullable=True),
        sa.Column("namespace", sa.String(63), server_default="security"),
        sa.Column("last_sync_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_pipelines_source_topic", "pipelines", ["source_topic"])
    op.create_index("ix_pipelines_destination_topic", "pipelines", ["destination_topic"])
    op.create_index("ix_pipelines_log_source_id", "pipelines", ["log_source_id"])

    # audit_logs (FK -> users)
    op.create_table(
        "audit_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("action", sa.String(50), nullable=False),
        sa.Column("entity_type", sa.String(50), nullable=False),
        sa.Column("entity_id", sa.String(100), nullable=True),
        sa.Column("entity_name", sa.String(255), nullable=True),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("user_email", sa.String(255), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("changes", postgresql.JSONB(), nullable=True),
        sa.Column("source", sa.String(50), server_default="backend", nullable=False),
        sa.Column("severity", sa.String(20), server_default="info", nullable=False),
    )
    op.create_index("ix_audit_logs_timestamp", "audit_logs", ["timestamp"])
    op.create_index("ix_audit_logs_action", "audit_logs", ["action"])
    op.create_index("ix_audit_logs_entity_type", "audit_logs", ["entity_type"])
    op.create_index("ix_audit_logs_entity_id", "audit_logs", ["entity_id"])
    op.create_index("ix_audit_logs_user_id", "audit_logs", ["user_id"])
    op.create_index("ix_audit_logs_severity", "audit_logs", ["severity"])

    # === Junction and metric tables ===

    # pipeline_repositories (many-to-many junction)
    # Composite PK (pipeline_id, repository_id) already ensures uniqueness
    op.create_table(
        "pipeline_repositories",
        sa.Column(
            "pipeline_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pipelines.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "repository_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("repositories.id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )

    # pipeline_rules
    op.create_table(
        "pipeline_rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "pipeline_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pipelines.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column(
            "rule_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("rules.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("enabled", sa.Boolean(), server_default="true"),
        sa.Column("tagged_events", sa.Integer(), server_default="0"),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_pipeline_rules_pipeline_id", "pipeline_rules", ["pipeline_id"])
    op.create_index("ix_pipeline_rules_rule_id", "pipeline_rules", ["rule_id"])

    # pipeline_metrics
    op.create_table(
        "pipeline_metrics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "pipeline_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pipelines.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("timestamp", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("window_total_events", sa.BigInteger(), server_default="0"),
        sa.Column("window_matched_events", sa.BigInteger(), server_default="0"),
        sa.Column("window_duration_seconds", sa.Float(), server_default="0"),
        sa.Column("throughput_eps", sa.Integer(), server_default="0"),
        sa.Column("event_parsing_errors", sa.Integer(), server_default="0"),
        sa.Column("rule_parsing_errors", sa.Integer(), server_default="0"),
        sa.Column("matching_errors", sa.Integer(), server_default="0"),
    )
    op.create_index("ix_pipeline_metrics_pipeline_id", "pipeline_metrics", ["pipeline_id"])
    op.create_index("ix_pipeline_metrics_pipeline_ts", "pipeline_metrics", ["pipeline_id", "timestamp"])
    op.create_index("ix_pipeline_metrics_timestamp", "pipeline_metrics", ["timestamp"])

    # pipeline_rule_metrics
    op.create_table(
        "pipeline_rule_metrics",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "pipeline_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("pipelines.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "rule_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("rules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("total_matches", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("pipeline_id", "rule_id", name="uq_pipeline_rule_metric"),
    )
    op.create_index("ix_pipeline_rule_metrics_pipeline_id", "pipeline_rule_metrics", ["pipeline_id"])
    op.create_index("ix_pipeline_rule_metrics_rule_id", "pipeline_rule_metrics", ["rule_id"])


def downgrade() -> None:
    """Drop all tables in reverse order."""
    # Drop tables with foreign keys first
    op.drop_table("pipeline_rule_metrics")
    op.drop_table("pipeline_metrics")
    op.drop_table("pipeline_rules")
    op.drop_table("pipeline_repositories")
    op.drop_table("audit_logs")
    op.drop_table("pipelines")
    op.drop_table("rules")

    # Drop independent tables
    op.drop_table("events")
    op.drop_table("config")
    op.drop_table("parsers")
    op.drop_table("log_sources")
    op.drop_table("filters")
    op.drop_table("repositories")
    op.drop_table("users")
