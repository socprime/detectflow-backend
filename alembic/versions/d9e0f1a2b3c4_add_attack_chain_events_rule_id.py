"""add attack_chain_events.rule_id without FK onto rules

Revision ID: d9e0f1a2b3c4
Revises: c8d9e0f1a2b3
Create Date: 2026-09-10

Kafka ``chains-analysed`` events carry ``rule_db_id`` (local ``rules.id``). Persist it on
``attack_chain_events.rule_id`` so the chain-detail read path can look up name/level/repo
by PK. No FK: deleting a rule must not fail ingest or cascade-delete correlated events.
Index is not required — the column is read as a small IN-list from already-loaded events.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "d9e0f1a2b3c4"
down_revision: str | Sequence[str] | None = "c8d9e0f1a2b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "attack_chain_events",
        sa.Column("rule_id", postgresql.UUID(as_uuid=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("attack_chain_events", "rule_id")
