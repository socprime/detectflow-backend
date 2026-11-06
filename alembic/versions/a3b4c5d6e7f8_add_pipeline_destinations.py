"""add pipeline destinations jsonb for multi-destination output

Revision ID: a3b4c5d6e7f8
Revises: 757bde054750
Create Date: 2026-08-10 10:15:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "a3b4c5d6e7f8"
down_revision: str | Sequence[str] | None = "757bde054750"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "pipelines",
        sa.Column(
            "destinations",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    # Backfill one destination object from legacy singular columns.
    op.execute(
        """
        UPDATE pipelines
        SET destinations = jsonb_build_array(
            jsonb_build_object(
                'topic', destination_topic,
                'output_mode', CASE WHEN save_untagged THEN 'all_events' ELSE 'matched_only' END,
                'apply_parser_to_output_events', COALESCE(apply_parser_to_output_events, false)
            )
        )
        WHERE destinations = '[]'::jsonb
          AND destination_topic IS NOT NULL
          AND destination_topic <> ''
        """
    )


def downgrade() -> None:
    op.drop_column("pipelines", "destinations")
