"""replace runtime started_at with jobs_enabled

Revision ID: a9b0c1d2e3f4
Revises: f8a9b0c1d2e3
Create Date: 2026-05-27

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "a9b0c1d2e3f4"
down_revision: str | Sequence[str] | None = "f8a9b0c1d2e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "attack_chain_configuration",
        sa.Column("jobs_enabled", sa.Boolean(), server_default="false", nullable=False),
    )
    op.execute(
        sa.text(
            """
            UPDATE attack_chain_configuration
            SET jobs_enabled = correlation_job_started_at IS NOT NULL
            """
        )
    )
    op.drop_column("attack_chain_configuration", "correlation_job_started_at")


def downgrade() -> None:
    op.add_column(
        "attack_chain_configuration",
        sa.Column("correlation_job_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE attack_chain_configuration
            SET correlation_job_started_at = now()
            WHERE jobs_enabled IS TRUE
            """
        )
    )
    op.drop_column("attack_chain_configuration", "jobs_enabled")
