"""Convert source_topic to source_topics array

Revision ID: a1b2c3d4e5f6
Revises: 30ea38f0b5c5
Create Date: 2026-01-13

Migrates Pipeline.source_topic (String) to Pipeline.source_topics (ARRAY).
Existing single topics are wrapped in arrays during migration.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "30ea38f0b5c5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Convert source_topic (String) to source_topics (Array)."""
    # Step 1: Add new column source_topics as ARRAY (nullable initially)
    op.add_column(
        "pipelines",
        sa.Column("source_topics", postgresql.ARRAY(sa.String(255)), nullable=True),
    )

    # Step 2: Migrate data - wrap existing source_topic in array
    op.execute("UPDATE pipelines SET source_topics = ARRAY[source_topic] WHERE source_topic IS NOT NULL")

    # Step 3: Set default for empty rows (if any)
    op.execute("UPDATE pipelines SET source_topics = '{}' WHERE source_topics IS NULL")

    # Step 4: Make source_topics NOT NULL
    op.alter_column("pipelines", "source_topics", nullable=False, server_default="{}")

    # Step 5: Drop old index on source_topic
    op.drop_index("ix_pipelines_source_topic", table_name="pipelines")

    # Step 6: Drop old column
    op.drop_column("pipelines", "source_topic")

    # Step 7: Create GIN index for array containment queries
    # GIN is optimal for: @> (contains), <@ (contained by), && (overlap)
    op.create_index(
        "ix_pipelines_source_topics_gin",
        "pipelines",
        ["source_topics"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    """Convert back to single source_topic (takes first element)."""
    # Step 1: Add source_topic column
    op.add_column(
        "pipelines",
        sa.Column("source_topic", sa.String(255), nullable=True),
    )

    # Step 2: Migrate data - take first element of array
    op.execute(
        "UPDATE pipelines SET source_topic = source_topics[1] "
        "WHERE source_topics IS NOT NULL AND array_length(source_topics, 1) > 0"
    )

    # Step 3: Handle empty arrays - set to empty string or NULL
    op.execute("UPDATE pipelines SET source_topic = '' WHERE source_topic IS NULL")

    # Step 4: Make NOT NULL
    op.alter_column("pipelines", "source_topic", nullable=False)

    # Step 5: Drop GIN index
    op.drop_index("ix_pipelines_source_topics_gin", table_name="pipelines")

    # Step 6: Drop source_topics column
    op.drop_column("pipelines", "source_topics")

    # Step 7: Recreate old B-tree index
    op.create_index("ix_pipelines_source_topic", "pipelines", ["source_topic"])
