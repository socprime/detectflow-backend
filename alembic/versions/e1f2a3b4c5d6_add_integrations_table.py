"""add integrations table

Revision ID: e1f2a3b4c5d6
Revises: b8a9c0d1e2f3
Create Date: 2026-03-18 15:00:00.000000

Add integrations module
- Create integrations table for storing integration profiles (SIEM, SOCPrime, Git, etc.)
- JSONB config field for provider-specific configuration
- Indexes on integration_type, category, status for efficient filtering
- Migrate existing SOCPrime API key from config table to integrations table
"""

import base64
from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1f2a3b4c5d6"
down_revision: str | Sequence[str] | None = "b8a9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. Create integrations table
    op.create_table(
        "integrations",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("integration_type", sa.String(50), nullable=False),
        sa.Column("category", sa.String(50), nullable=False),
        sa.Column("status", sa.String(20), server_default="disconnected", nullable=False),
        sa.Column("config", postgresql.JSONB(), server_default="{}", nullable=False),
        sa.Column("status_message", sa.String(512), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_integrations_integration_type", "integrations", ["integration_type"])
    op.create_index("ix_integrations_category", "integrations", ["category"])
    op.create_index("ix_integrations_status", "integrations", ["status"])

    # 2. Migrate existing SOCPrime API key from config table → integrations table
    connection = op.get_bind()
    result = connection.execute(sa.text("SELECT value FROM config WHERE key = 'repository_api_key' LIMIT 1"))
    row = result.fetchone()

    if row:
        encoded_api_key = row[0]
        try:
            api_key = base64.b64decode(encoded_api_key.encode()).decode()
        except Exception:
            api_key = encoded_api_key  # fallback: use raw value if decoding fails

        # Insert into integrations with JSONB config
        connection.execute(
            sa.text("""
                INSERT INTO integrations (name, integration_type, category, status, config)
                VALUES (
                    'Threat Detection Marketplace',
                    'socprime_tdm',
                    'soc_prime',
                    'connected',
                    jsonb_build_object('api_key', :api_key)
                )
            """),
            {"api_key": api_key},
        )

        # Remove old key from config table
        connection.execute(sa.text("DELETE FROM config WHERE key = 'repository_api_key'"))


def downgrade() -> None:
    # 1. Migrate API key back to config table
    connection = op.get_bind()
    result = connection.execute(
        sa.text("""
            SELECT config->>'api_key' FROM integrations
            WHERE integration_type = 'socprime_tdm'
            ORDER BY created ASC LIMIT 1
        """)
    )
    row = result.fetchone()

    if row and row[0]:
        api_key = row[0]
        encoded = base64.b64encode(api_key.encode()).decode()
        connection.execute(
            sa.text("""
                INSERT INTO config (key, value)
                VALUES ('repository_api_key', :encoded)
                ON CONFLICT (key) DO UPDATE SET value = :encoded
            """),
            {"encoded": encoded},
        )

    # 2. Drop integrations table
    op.drop_index("ix_integrations_status", table_name="integrations")
    op.drop_index("ix_integrations_category", table_name="integrations")
    op.drop_index("ix_integrations_integration_type", table_name="integrations")
    op.drop_table("integrations")
