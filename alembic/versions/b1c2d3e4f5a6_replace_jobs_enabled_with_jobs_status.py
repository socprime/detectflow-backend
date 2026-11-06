"""replace jobs_enabled with jobs_status

Revision ID: b1c2d3e4f5a6
Revises: a9b0c1d2e3f4
Create Date: 2026-05-28

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "b1c2d3e4f5a6"
down_revision: str | Sequence[str] | None = "a9b0c1d2e3f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "attack_chain_configuration",
        sa.Column("jobs_status", sa.String(length=20), server_default="stopped", nullable=False),
    )
    op.execute(
        sa.text(
            """
            UPDATE attack_chain_configuration
            SET jobs_status = CASE WHEN jobs_enabled IS TRUE THEN 'starting' ELSE 'stopped' END
            """
        )
    )
    op.create_check_constraint(
        "ck_attack_chain_configuration_jobs_status",
        "attack_chain_configuration",
        "jobs_status IN ('starting', 'stopping', 'stopped')",
    )
    op.drop_column("attack_chain_configuration", "jobs_enabled")


def downgrade() -> None:
    op.add_column(
        "attack_chain_configuration",
        sa.Column("jobs_enabled", sa.Boolean(), server_default="false", nullable=False),
    )
    op.execute(
        sa.text(
            """
            UPDATE attack_chain_configuration
            SET jobs_enabled = CASE WHEN jobs_status = 'starting' THEN true ELSE false END
            """
        )
    )
    op.drop_constraint(
        "ck_attack_chain_configuration_jobs_status",
        "attack_chain_configuration",
        type_="check",
    )
    op.drop_column("attack_chain_configuration", "jobs_status")
