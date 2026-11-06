"""add attack chain match percentage

Revision ID: f8a9b0c1d2e3
Revises: e7f8a9b0c1d2
Create Date: 2026-05-27

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "f8a9b0c1d2e3"
down_revision: str | Sequence[str] | None = "e7f8a9b0c1d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "attack_chains",
        sa.Column("match", sa.Integer(), server_default="0", nullable=False),
    )
    op.execute(
        sa.text(
            """
            UPDATE attack_chains
            SET match = CASE
                WHEN jsonb_array_length(seqs) = 0 THEN 0
                ELSE (
                    SELECT round((count(*)::numeric / jsonb_array_length(attack_chains.seqs)) * 100)::int
                    FROM jsonb_array_elements(attack_chains.seqs) AS seq
                    WHERE seq @> '{"hit": true}'::jsonb
                )
            END
            """
        )
    )


def downgrade() -> None:
    op.drop_column("attack_chains", "match")
