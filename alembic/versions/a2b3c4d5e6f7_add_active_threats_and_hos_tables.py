"""add active threats and high order sigmas tables

Revision ID: a2b3c4d5e6f7
Revises: f7a8b9c0d1e2
Create Date: 2026-04-29

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a2b3c4d5e6f7"
down_revision: str | Sequence[str] | None = "f7a8b9c0d1e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "active_threats",
        sa.Column("tdm_id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="false", nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("severity", sa.String(length=255), nullable=True),
        sa.Column("investigation", sa.Text(), nullable=True),
        sa.Column("response", sa.Text(), nullable=True),
        sa.Column("mitigation", sa.Text(), nullable=True),
        sa.Column("industries", postgresql.ARRAY(sa.String(length=255)), nullable=True),
        sa.Column("geographies", postgresql.ARRAY(sa.String(length=255)), nullable=True),
        sa.Column("threat_types", postgresql.ARRAY(sa.String(length=255)), nullable=True),
        sa.Column("source", sa.String(length=512), nullable=True),
        sa.Column("attack_flow", sa.Text(), nullable=True),
        sa.Column("tdm_created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tdm_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("tdm_id"),
    )

    op.create_table(
        "high_order_sigmas",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("tdm_id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("tdm_created", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tdm_updated", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tdm_id"),
    )

    op.create_table(
        "active_threat_high_order_sigmas",
        sa.Column("active_threat_id", sa.String(length=255), nullable=False),
        sa.Column("high_order_sigma_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["active_threat_id"], ["active_threats.tdm_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["high_order_sigma_id"], ["high_order_sigmas.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("active_threat_id", "high_order_sigma_id"),
    )
    op.create_index(
        "ix_active_threat_high_order_sigmas_high_order_sigma_id",
        "active_threat_high_order_sigmas",
        ["high_order_sigma_id"],
    )

    op.create_table(
        "high_order_sigma_rule_refs",
        sa.Column("high_order_sigma_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sigma_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["high_order_sigma_id"], ["high_order_sigmas.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("high_order_sigma_id", "sigma_uuid"),
    )
    op.create_index(
        "ix_high_order_sigma_rule_refs_sigma_uuid",
        "high_order_sigma_rule_refs",
        ["sigma_uuid"],
    )


def downgrade() -> None:
    op.drop_index("ix_high_order_sigma_rule_refs_sigma_uuid", table_name="high_order_sigma_rule_refs")
    op.drop_table("high_order_sigma_rule_refs")
    op.drop_index(
        "ix_active_threat_high_order_sigmas_high_order_sigma_id",
        table_name="active_threat_high_order_sigmas",
    )
    op.drop_table("active_threat_high_order_sigmas")
    op.drop_table("high_order_sigmas")
    op.drop_table("active_threats")
