"""add rule sigma uuid

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7, 435a2f412063
Create Date: 2026-04-30

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op
from apps.modules.utils import get_sigma_uuid_from_sigma_body

revision: str = "b3c4d5e6f7a8"
down_revision: str | Sequence[str] | None = ("a2b3c4d5e6f7", "435a2f412063")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _backfill_sigma_uuid() -> None:
    connection = op.get_bind()
    rows = connection.execute(sa.text("SELECT id, body FROM rules WHERE body IS NOT NULL")).mappings()
    updates = [
        {"id": str(row["id"]), "sigma_uuid": sigma_uuid}
        for row in rows
        if (sigma_uuid := get_sigma_uuid_from_sigma_body(row["body"])) is not None
    ]
    if not updates:
        return

    connection.execute(
        sa.text(
            """
            UPDATE rules
            SET sigma_uuid = CAST(:sigma_uuid AS uuid)
            WHERE id = CAST(:id AS uuid)
            """
        ),
        updates,
    )


def upgrade() -> None:
    op.add_column("rules", sa.Column("sigma_uuid", postgresql.UUID(as_uuid=True), nullable=True))
    _backfill_sigma_uuid()
    op.create_index("ix_rules_sigma_uuid", "rules", ["sigma_uuid"])


def downgrade() -> None:
    op.drop_index("ix_rules_sigma_uuid", table_name="rules")
    op.drop_column("rules", "sigma_uuid")
