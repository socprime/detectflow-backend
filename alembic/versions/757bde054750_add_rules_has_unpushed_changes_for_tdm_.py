"""add rules.has_unpushed_changes for tdm push toggle

Revision ID: 757bde054750
Revises: f2a3b4c5d6e7
Create Date: 2026-07-09 17:32:52.900258

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "757bde054750"
down_revision: str | Sequence[str] | None = "f2a3b4c5d6e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "rules",
        sa.Column("has_unpushed_changes", sa.Boolean(), server_default="false", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("rules", "has_unpushed_changes")
