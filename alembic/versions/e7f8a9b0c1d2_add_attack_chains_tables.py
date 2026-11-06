"""add attack chains, events, configuration and active_threats.threat_actors

Revision ID: e7f8a9b0c1d2
Revises: c4d5e6f7a8b9
Create Date: 2026-05-15

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "e7f8a9b0c1d2"
down_revision: str | Sequence[str] | None = "c4d5e6f7a8b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "active_threats",
        sa.Column("threat_actors", postgresql.ARRAY(sa.String(length=255)), nullable=True),
    )

    op.create_table(
        "attack_chain_configuration",
        sa.Column("id", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("threshold_pct", sa.Integer(), server_default="60", nullable=False),
        sa.Column("correlation_window_seconds", sa.Integer(), server_default="300", nullable=False),
        sa.Column(
            "monitored_topics",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{}'::text[]"),
            nullable=False,
        ),
        sa.Column("auto_add_new_topics", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("correlation_job_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("id = 1", name="ck_attack_chain_configuration_singleton_id"),
        sa.CheckConstraint(
            "threshold_pct >= 50 AND threshold_pct <= 100 AND threshold_pct % 5 = 0",
            name="ck_attack_chain_configuration_threshold_pct",
        ),
        sa.CheckConstraint(
            "correlation_window_seconds >= 30 AND correlation_window_seconds <= 10800",
            name="ck_attack_chain_configuration_window_seconds",
        ),
        sa.ForeignKeyConstraint(["updated_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        sa.text(
            "INSERT INTO attack_chain_configuration "
            "(id, threshold_pct, correlation_window_seconds, monitored_topics, auto_add_new_topics, "
            "updated_at, correlation_job_started_at) "
            "VALUES (1, 60, 300, '{}'::text[], false, now(), NULL)"
        )
    )

    op.create_table(
        "attack_chains",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("hos_rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("active_threat_id", sa.String(length=255), nullable=False),
        sa.Column("chain_type", sa.String(length=20), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=20), server_default="new", nullable=False),
        sa.Column("first_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("rule_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "seqs",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("hos_tags", postgresql.ARRAY(sa.String(length=255)), nullable=True),
        sa.Column("coverage", sa.Numeric(5, 2), nullable=False),
        sa.Column(
            "pipeline_names",
            postgresql.ARRAY(sa.Text()),
            server_default=sa.text("'{}'::text[]"),
            nullable=False,
        ),
        sa.Column("ai_summary", sa.Text(), nullable=True),
        sa.Column("llm_verdict", sa.String(length=20), nullable=True),
        sa.Column("llm_confidence", sa.Numeric(3, 2), nullable=True),
        sa.Column("llm_review", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("revision", sa.Integer(), server_default="0", nullable=False),
        sa.Column("analysed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["active_threat_id"], ["active_threats.tdm_id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["hos_rule_id"], ["high_order_sigmas.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_attack_chains_active_threat_id", "attack_chains", ["active_threat_id"])
    op.create_index("ix_attack_chains_hos_rule_id", "attack_chains", ["hos_rule_id"])
    op.create_index("ix_attack_chains_window_start", "attack_chains", ["window_start"])

    op.create_table(
        "attack_chain_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("chain_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(length=255), nullable=False),
        sa.Column("sigma_rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sigma_rule_title", sa.String(length=255), nullable=True),
        sa.Column("seq", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("technique_ids", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("raw_event", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["chain_id"], ["attack_chains.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("chain_id", "event_id", name="uq_attack_chain_events_chain_event"),
    )
    op.create_index(
        "ix_attack_chain_events_chain_seq_ts",
        "attack_chain_events",
        ["chain_id", "seq", "timestamp"],
    )


def downgrade() -> None:
    op.drop_index("ix_attack_chain_events_chain_seq_ts", table_name="attack_chain_events")
    op.drop_table("attack_chain_events")

    op.drop_index("ix_attack_chains_window_start", table_name="attack_chains")
    op.drop_index("ix_attack_chains_hos_rule_id", table_name="attack_chains")
    op.drop_index("ix_attack_chains_active_threat_id", table_name="attack_chains")
    op.drop_table("attack_chains")

    op.drop_table("attack_chain_configuration")

    op.drop_column("active_threats", "threat_actors")
