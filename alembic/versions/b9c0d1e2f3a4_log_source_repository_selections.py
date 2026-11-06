"""store log source test repository selections

Revision ID: b9c0d1e2f3a4
Revises: a8b9c0d1e2f3
Create Date: 2026-05-25

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b9c0d1e2f3a4"
down_revision: str | Sequence[str] | None = "a8b9c0d1e2f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Convert log source test repositories from UUID array to selection JSON."""
    op.add_column("log_sources", sa.Column("test_repository_ids_new", postgresql.JSONB(), nullable=True))
    op.execute(
        sa.text(
            "UPDATE log_sources "
            "SET test_repository_ids_new = CASE "
            "WHEN test_repository_ids IS NULL THEN NULL "
            "ELSE COALESCE(("
            "SELECT jsonb_agg(jsonb_build_object('id', repo_id::text, 'folders', '[]'::jsonb)) "
            "FROM unnest(test_repository_ids) AS repo_id"
            "), '[]'::jsonb) "
            "END"
        )
    )
    op.drop_column("log_sources", "test_repository_ids")
    op.alter_column(
        "log_sources",
        "test_repository_ids_new",
        new_column_name="test_repository_ids",
        existing_type=postgresql.JSONB(),
        existing_nullable=True,
    )


def downgrade() -> None:
    """Restore UUID array storage, dropping folder selections."""
    op.add_column(
        "log_sources",
        sa.Column("test_repository_ids_old", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=True),
    )
    op.execute(
        sa.text(
            "UPDATE log_sources "
            "SET test_repository_ids_old = CASE "
            "WHEN test_repository_ids IS NULL THEN NULL "
            "ELSE ARRAY("
            "SELECT (selection->>'id')::uuid "
            "FROM jsonb_array_elements(test_repository_ids) AS selection"
            ") "
            "END"
        )
    )
    op.drop_column("log_sources", "test_repository_ids")
    op.alter_column(
        "log_sources",
        "test_repository_ids_old",
        new_column_name="test_repository_ids",
        existing_type=postgresql.ARRAY(postgresql.UUID(as_uuid=True)),
        existing_nullable=True,
    )
