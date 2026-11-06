"""add active threat hidden flag and user edit markers

Revision ID: f1a2b3c4d5e6
Revises: a9b0c1d2e3f4
Create Date: 2026-05-25

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f1a2b3c4d5e6"
down_revision: str | Sequence[str] | None = "a9b0c1d2e3f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "active_threats",
        sa.Column("is_hidden", sa.Boolean(), server_default="false", nullable=False),
    )
    op.create_index(
        "ix_active_threats_visible_name",
        "active_threats",
        ["name"],
        postgresql_where=sa.text("is_hidden = false"),
    )

    op.add_column(
        "high_order_sigmas",
        sa.Column("is_user_edited", sa.Boolean(), server_default="false", nullable=False),
    )
    op.add_column(
        "high_order_sigmas",
        sa.Column("is_kafka_tombstoned", sa.Boolean(), server_default="false", nullable=False),
    )
    op.create_index(
        "ix_high_order_sigmas_pending_kafka_tombstone",
        "high_order_sigmas",
        ["tdm_id"],
        postgresql_where=sa.text("is_kafka_tombstoned = false"),
    )

    op.add_column(
        "rules",
        sa.Column("is_user_edited_system_rule", sa.Boolean(), server_default="false", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("rules", "is_user_edited_system_rule")

    op.drop_index("ix_high_order_sigmas_pending_kafka_tombstone", table_name="high_order_sigmas")
    op.drop_column("high_order_sigmas", "is_kafka_tombstoned")
    op.drop_column("high_order_sigmas", "is_user_edited")

    op.drop_index("ix_active_threats_visible_name", table_name="active_threats")
    op.drop_column("active_threats", "is_hidden")
