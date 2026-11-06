"""socprime_tdm at most one integration profile

Revision ID: f7a8b9c0d1e2
Revises: 555c40b973b3
Create Date: 2026-04-28

Partial unique index: only one row with integration_type = socprime_tdm (Threat Detection Marketplace).
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f7a8b9c0d1e2"
down_revision: str | Sequence[str] | None = "555c40b973b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "uq_integrations_socprime_tdm_singleton",
        "integrations",
        ["integration_type"],
        unique=True,
        postgresql_where=sa.text("integration_type = 'socprime_tdm'"),
    )


def downgrade() -> None:
    op.drop_index("uq_integrations_socprime_tdm_singleton", table_name="integrations")
