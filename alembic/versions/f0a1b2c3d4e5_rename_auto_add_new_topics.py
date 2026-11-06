"""rename auto_add_new_topics to auto_add_new_pipelines

Revision ID: f0a1b2c3d4e5
Revises: e9a0b1c2d3e4
Create Date: 2026-09-01 12:35:00.000000

The flag auto-adds newly created detection pipelines as members, not destination topics.
Boolean values are unchanged; only the column name moves on both correlation_pipelines and
the deprecated attack_chain_configuration singleton (kept one release for rollback).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "f0a1b2c3d4e5"
down_revision: str | Sequence[str] | None = "e9a0b1c2d3e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column(
        "correlation_pipelines",
        "auto_add_new_topics",
        new_column_name="auto_add_new_pipelines",
    )
    op.alter_column(
        "attack_chain_configuration",
        "auto_add_new_topics",
        new_column_name="auto_add_new_pipelines",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column(
        "correlation_pipelines",
        "auto_add_new_pipelines",
        new_column_name="auto_add_new_topics",
    )
    op.alter_column(
        "attack_chain_configuration",
        "auto_add_new_pipelines",
        new_column_name="auto_add_new_topics",
    )
