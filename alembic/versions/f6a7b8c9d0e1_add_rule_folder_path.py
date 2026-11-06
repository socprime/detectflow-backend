"""add rule folder path

Revision ID: f6a7b8c9d0e1
Revises: e7f8a9b0c1d2
Create Date: 2026-05-20

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f6a7b8c9d0e1"
down_revision: str | Sequence[str] | None = "e7f8a9b0c1d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FOLDER_PATH_EXPRESSION = """
CASE
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(category), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
        THEN 'product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/other'
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
        THEN 'category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/other'
    WHEN NULLIF(BTRIM(service), '') IS NOT NULL
        THEN 'service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    ELSE 'generic'
END
"""


def upgrade() -> None:
    """Add generated folder path for virtual rule folders."""
    op.execute(
        f"""
        ALTER TABLE rules
        ADD COLUMN folder_path TEXT GENERATED ALWAYS AS ({FOLDER_PATH_EXPRESSION}) STORED
        """
    )
    op.execute("ALTER TABLE rules ALTER COLUMN folder_path SET NOT NULL")
    op.create_index("ix_rules_repository_folder_path", "rules", ["repository_id", "folder_path"])


def downgrade() -> None:
    """Remove generated folder path."""
    op.drop_index("ix_rules_repository_folder_path", table_name="rules")
    op.drop_column("rules", "folder_path")
