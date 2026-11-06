"""add chains_analysed_dead_letters

Revision ID: f9d122667dac
Revises: 272060501ebb
Create Date: 2026-08-12 10:31:36.123098

Purely additive: one new table, no existing table gains a column and nothing references it, so
``alembic downgrade -1`` simply drops it. Roll the code back first — code deployed against a DB
without this table cannot park, and a record that cannot be parked is retried rather than skipped,
i.e. exactly the pre-change behaviour, never loss.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f9d122667dac"
down_revision: str | Sequence[str] | None = "272060501ebb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "chains_analysed_dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("kafka_topic", sa.String(length=255), nullable=False),
        sa.Column("kafka_partition", sa.Integer(), nullable=False),
        sa.Column("kafka_offset", sa.BigInteger(), nullable=False),
        # No FK: the chain, its high-order Sigma and its Active Threat may all be absent — that is
        # usually why the record could not be applied in the first place.
        sa.Column("chain_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("hos_rule_id", sa.String(length=255), nullable=True),
        sa.Column("error_class", sa.String(length=20), nullable=False),
        sa.Column("error_type", sa.String(length=255), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default="1", nullable=False),
        sa.Column("payload", sa.Text(), nullable=True),
        sa.Column("payload_truncated", sa.Boolean(), server_default="false", nullable=False),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("last_failed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        # One row per record: redelivery after a rebalance bumps ``attempts`` instead of duplicating.
        sa.UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset", name="uq_chains_dlq_tp_offset"),
    )
    op.create_index("ix_chains_dlq_created_at", "chains_analysed_dead_letters", ["created_at"], unique=False)
    op.create_index(
        op.f("ix_chains_analysed_dead_letters_chain_id"), "chains_analysed_dead_letters", ["chain_id"], unique=False
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_chains_analysed_dead_letters_chain_id"), table_name="chains_analysed_dead_letters")
    op.drop_index("ix_chains_dlq_created_at", table_name="chains_analysed_dead_letters")
    op.drop_table("chains_analysed_dead_letters")
