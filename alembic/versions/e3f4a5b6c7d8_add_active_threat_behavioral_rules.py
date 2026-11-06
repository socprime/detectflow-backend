"""add active_threat_behavioral_rules table

Revision ID: e3f4a5b6c7d8
Revises: d2e3f4a5b6c7
Create Date: 2026-06-29

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "e3f4a5b6c7d8"
down_revision: str | Sequence[str] | None = "d2e3f4a5b6c7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "active_threat_behavioral_rules",
        sa.Column("active_threat_id", sa.String(length=255), nullable=False),
        # TDM ``case.id`` (our ``rules.external_id``) stored verbatim from the AT relation.
        # Not FK'd to ``rules`` because ``rules.external_id`` is nullable and not unique
        # (reused across repositories); the read path scopes the join to the system repo.
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.ForeignKeyConstraint(["active_threat_id"], ["active_threats.tdm_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("active_threat_id", "external_id"),
    )
    op.create_index(
        "ix_active_threat_behavioral_rules_external_id",
        "active_threat_behavioral_rules",
        ["external_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_active_threat_behavioral_rules_external_id",
        table_name="active_threat_behavioral_rules",
    )
    op.drop_table("active_threat_behavioral_rules")
