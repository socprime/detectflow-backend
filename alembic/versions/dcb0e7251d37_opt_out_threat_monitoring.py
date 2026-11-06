"""opt-out threat monitoring: is_blocked + per-pipeline exclusions

Revision ID: dcb0e7251d37
Revises: f9d122667dac
Create Date: 2026-08-12 15:10:44.812733

Architecture decision D8: monitoring is opt-OUT. A threat is monitored in every correlation
pipeline unless it is globally blocked or individually excluded from that pipeline —
``monitored = NOT at.is_blocked AND at_id NOT IN pipeline.excluded``.

D9 pins the cutover: ``is_blocked = NOT enabled``, so nothing starts correlating that was not
correlating before and there is no chain-volume or LLM-spend spike at deploy time.

**Downgrade is lossy.** Every per-pipeline exclusion disappears and every pipeline reverts to the
single global flag. Run ``pg_dump -t correlation_pipeline_excluded_active_threats`` before
downgrading if that analyst intent matters.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dcb0e7251d37"
down_revision: str | Sequence[str] | None = "f9d122667dac"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "active_threats",
        sa.Column("is_blocked", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    # D9: preserve today's behaviour exactly. COALESCE because ``enabled`` was nullable.
    op.execute(sa.text("UPDATE active_threats SET is_blocked = NOT COALESCE(enabled, false)"))
    op.create_index("ix_active_threats_is_blocked", "active_threats", ["is_blocked"])
    op.drop_column("active_threats", "enabled")

    op.create_table(
        "correlation_pipeline_excluded_active_threats",
        sa.Column("correlation_pipeline_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("active_threat_tdm_id", sa.String(length=255), nullable=False),
        sa.Column("excluded_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("excluded_by", postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(["correlation_pipeline_id"], ["correlation_pipelines.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["active_threat_tdm_id"], ["active_threats.tdm_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["excluded_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("correlation_pipeline_id", "active_threat_tdm_id"),
    )
    # Reverse lookup for the list view's Pipelines column and the cascade capture.
    op.create_index(
        "ix_cp_excluded_active_threats_at_id",
        "correlation_pipeline_excluded_active_threats",
        ["active_threat_tdm_id"],
    )

    # Backs the threat-detail chain breakdown, which groups by all three columns at once.
    op.create_index(
        "ix_attack_chains_at_cp_status",
        "attack_chains",
        ["active_threat_id", "correlation_pipeline_id", "status"],
    )


def downgrade() -> None:
    """Downgrade schema. LOSSY: per-pipeline exclusions are dropped, not converted."""
    op.drop_index("ix_attack_chains_at_cp_status", table_name="attack_chains")
    op.drop_index("ix_cp_excluded_active_threats_at_id", table_name="correlation_pipeline_excluded_active_threats")
    op.drop_table("correlation_pipeline_excluded_active_threats")

    op.add_column("active_threats", sa.Column("enabled", sa.Boolean(), nullable=True, server_default=sa.text("false")))
    op.execute(sa.text("UPDATE active_threats SET enabled = NOT is_blocked"))
    op.drop_index("ix_active_threats_is_blocked", table_name="active_threats")
    op.drop_column("active_threats", "is_blocked")
