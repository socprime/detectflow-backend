"""add HOS rule check failures snapshot

Revision ID: b0c1d2e3f4a5
Revises: a9b0c1d2e3f4
Create Date: 2026-05-28

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b0c1d2e3f4a5"
down_revision: str | Sequence[str] | None = "a9b0c1d2e3f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "high_order_sigma_rule_check_failures",
        sa.Column("high_order_sigma_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("is_not_supported", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("is_not_connected", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "disabled_pipeline_ids",
            postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
            server_default=sa.text("'{}'::uuid[]"),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["high_order_sigma_id"], ["high_order_sigmas.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["rule_id"], ["rules.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("high_order_sigma_id", "rule_id"),
    )
    op.create_index(
        "ix_hos_rule_check_failures_hos_id",
        "high_order_sigma_rule_check_failures",
        ["high_order_sigma_id"],
    )
    op.create_index(
        "ix_hos_rule_check_failures_rule_id",
        "high_order_sigma_rule_check_failures",
        ["rule_id"],
    )
    op.execute(
        sa.text(
            """
            INSERT INTO high_order_sigma_rule_check_failures (
                high_order_sigma_id,
                rule_id,
                is_not_supported,
                is_not_connected,
                disabled_pipeline_ids
            )
            SELECT
                pairs.high_order_sigma_id,
                pairs.rule_id,
                pairs.is_not_supported,
                pairs.connected_pipeline_count = 0 AS is_not_connected,
                COALESCE(pairs.disabled_pipeline_ids, '{}'::uuid[]) AS disabled_pipeline_ids
            FROM (
                SELECT
                    refs.high_order_sigma_id,
                    rules.id AS rule_id,
                    bool_or(rules.is_supported IS FALSE) AS is_not_supported,
                    count(DISTINCT pipeline_rules.pipeline_id) AS connected_pipeline_count,
                    array_remove(
                        array_agg(DISTINCT pipeline_rules.pipeline_id)
                            FILTER (WHERE pipeline_rules.enabled IS FALSE),
                        NULL
                    ) AS disabled_pipeline_ids
                FROM high_order_sigma_rule_refs refs
                JOIN rules ON rules.sigma_uuid = refs.sigma_uuid
                LEFT JOIN pipeline_rules ON pipeline_rules.rule_id = rules.id
                GROUP BY refs.high_order_sigma_id, rules.id
            ) pairs
            WHERE
                pairs.is_not_supported IS TRUE
                OR pairs.connected_pipeline_count = 0
                OR COALESCE(cardinality(pairs.disabled_pipeline_ids), 0) > 0
            """
        )
    )


def downgrade() -> None:
    op.drop_index("ix_hos_rule_check_failures_rule_id", table_name="high_order_sigma_rule_check_failures")
    op.drop_index("ix_hos_rule_check_failures_hos_id", table_name="high_order_sigma_rule_check_failures")
    op.drop_table("high_order_sigma_rule_check_failures")
