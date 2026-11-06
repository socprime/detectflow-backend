"""add attack_chains.last_wire_chain_id for sticky-open consumer idempotency

Revision ID: c8d9e0f1a2b3
Revises: c3d4e5f6a7b8
Create Date: 2026-09-09

Sticky-open merge keys open chains by ``(correlation_pipeline_id, hos_rule_id, hostname)``.
``revision`` is monotonic only per wire ``chain_id`` and restarts at 1 each tumbling window, so
idempotency cannot compare revisions across windows. ``last_wire_chain_id`` records the last
applied Kafka identity; NULL is treated as not equal to the incoming id (post-migrate
cross-window messages still merge). Backfill uses the row PK, which was originally the first
wire id.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c8d9e0f1a2b3"
down_revision: str | Sequence[str] | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "attack_chains",
        sa.Column("last_wire_chain_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute(sa.text("UPDATE attack_chains SET last_wire_chain_id = id WHERE last_wire_chain_id IS NULL"))
    op.create_index(
        "ix_attack_chains_sticky_lookup",
        "attack_chains",
        ["correlation_pipeline_id", "hos_rule_id", "hostname"],
        postgresql_where=sa.text("lower(status) IN ('new', 'investigating')"),
    )


def downgrade() -> None:
    op.drop_index("ix_attack_chains_sticky_lookup", table_name="attack_chains")
    op.drop_column("attack_chains", "last_wire_chain_id")
