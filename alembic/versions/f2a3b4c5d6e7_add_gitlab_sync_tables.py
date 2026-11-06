"""add gitlab sync tables and make rules.folder_path writable

Revision ID: f2a3b4c5d6e7
Revises: e3f4a5b6c7d8
Create Date: 2026-07-07

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f2a3b4c5d6e7"
down_revision: str | Sequence[str] | None = "e3f4a5b6c7d8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FOLDER_PATH_EXPRESSION = """
CASE
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(category), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_')) || '/_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(product), '') IS NOT NULL
        THEN '_product/' || LOWER(REPLACE(NULLIF(BTRIM(product), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
     AND NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_')) || '/_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    WHEN NULLIF(BTRIM(category), '') IS NOT NULL
        THEN '_category/' || LOWER(REPLACE(NULLIF(BTRIM(category), ''), '/', '_'))
    WHEN NULLIF(BTRIM(service), '') IS NOT NULL
        THEN '_service/' || LOWER(REPLACE(NULLIF(BTRIM(service), ''), '/', '_'))
    ELSE NULL
END
"""


def upgrade() -> None:
    op.execute("ALTER TABLE rules ALTER COLUMN folder_path DROP EXPRESSION")

    op.create_table(
        "gitlab_repository",
        sa.Column("repository_id", sa.UUID(), nullable=False),
        sa.Column("integration_id", sa.UUID(), nullable=False),
        sa.Column("last_synced_commit_sha", sa.Text(), nullable=True),
        sa.Column("created", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["repository_id"], ["repositories.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["integration_id"], ["integrations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("repository_id"),
        sa.UniqueConstraint("integration_id", name="uq_gitlab_repository_integration_id"),
    )
    op.create_index("ix_gitlab_repository_integration_id", "gitlab_repository", ["integration_id"])

    op.create_table(
        "gitlab_rule_metadata",
        sa.Column("rule_id", sa.UUID(), nullable=False),
        sa.Column("repository_id", sa.UUID(), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("last_commit_id", sa.Text(), nullable=True),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["rule_id"], ["rules.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["repository_id"], ["repositories.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("rule_id"),
    )
    op.create_index(
        "ix_gitlab_rule_metadata_repository_file_path",
        "gitlab_rule_metadata",
        ["repository_id", "file_path"],
    )


def downgrade() -> None:
    op.drop_index("ix_gitlab_rule_metadata_repository_file_path", table_name="gitlab_rule_metadata")
    op.drop_table("gitlab_rule_metadata")
    op.drop_index("ix_gitlab_repository_integration_id", table_name="gitlab_repository")
    op.drop_table("gitlab_repository")

    op.execute(f"ALTER TABLE rules ALTER COLUMN folder_path ADD GENERATED ALWAYS AS ({FOLDER_PATH_EXPRESSION}) STORED")
