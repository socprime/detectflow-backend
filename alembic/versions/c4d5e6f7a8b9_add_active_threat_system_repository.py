"""add active threat system repository

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
Create Date: 2026-05-06

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c4d5e6f7a8b9"
down_revision: str | Sequence[str] | None = "b3c4d5e6f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY = "active_threat_sigma_rules"
SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_NAME = "SOC Prime Active Threats"


def upgrade() -> None:
    op.add_column("repositories", sa.Column("is_system", sa.Boolean(), server_default="false", nullable=False))
    op.add_column("repositories", sa.Column("system_key", sa.String(length=100), nullable=True))
    op.create_unique_constraint("uq_repositories_system_key", "repositories", ["system_key"])

    op.add_column("rules", sa.Column("external_id", sa.String(length=255), nullable=True))
    op.create_index("ix_rules_external_id", "rules", ["external_id"])

    op.execute(
        sa.text(
            f"""
            INSERT INTO repositories (
                id,
                name,
                type,
                sync_enabled,
                is_system,
                system_key
            )
            VALUES (
                gen_random_uuid(),
                '{SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_NAME}',
                'api',
                true,
                true,
                '{SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY}'
            )
            ON CONFLICT (system_key) DO UPDATE
            SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                sync_enabled = EXCLUDED.sync_enabled,
                is_system = EXCLUDED.is_system
            """
        )
    )


def downgrade() -> None:
    op.execute(f"DELETE FROM repositories WHERE system_key = '{SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY}'")

    op.drop_index("ix_rules_external_id", table_name="rules")
    op.drop_column("rules", "external_id")

    op.drop_constraint("uq_repositories_system_key", "repositories", type_="unique")
    op.drop_column("repositories", "system_key")
    op.drop_column("repositories", "is_system")
