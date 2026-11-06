"""add active_threats.techniques

Revision ID: c3d4e5f6a7b8
Revises: f0a1b2c3d4e5
Create Date: 2026-09-03

Stores MITRE ATT&CK technique IDs from TDM ``GET /v1/active-threats`` (``techniques``).
HTTP list/detail resolve names from the cached enterprise ATT&CK bundle.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "c3d4e5f6a7b8"
down_revision: str | Sequence[str] | None = "f0a1b2c3d4e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "active_threats",
        sa.Column("techniques", postgresql.ARRAY(sa.String(length=255)), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("active_threats", "techniques")
