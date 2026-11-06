"""prefix pipeline rules/metrics topic defaults with detectflow

Renames the default Kafka topic names stored on the ``pipelines`` table:
- ``pipelines.rules_topic``  : ``sigma-rules``     -> ``detectflow.sigma-rules``
- ``pipelines.metrics_topic``: ``rule-statistics`` -> ``detectflow.rule-metrics``

Updates both the column ``server_default`` and any existing rows that still
hold the old default values.

Revision ID: c2d3e4f5a6b7
Revises: f1a2b3c4d5e6
Create Date: 2026-05-29

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "c2d3e4f5a6b7"
down_revision: str | Sequence[str] | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "pipelines",
        "rules_topic",
        existing_type=sa.String(255),
        server_default="detectflow.sigma-rules",
    )
    op.alter_column(
        "pipelines",
        "metrics_topic",
        existing_type=sa.String(255),
        server_default="detectflow.rule-metrics",
    )

    op.execute("UPDATE pipelines SET rules_topic = 'detectflow.sigma-rules' WHERE rules_topic = 'sigma-rules'")
    op.execute("UPDATE pipelines SET metrics_topic = 'detectflow.rule-metrics' WHERE metrics_topic = 'rule-statistics'")


def downgrade() -> None:
    op.execute("UPDATE pipelines SET rules_topic = 'sigma-rules' WHERE rules_topic = 'detectflow.sigma-rules'")
    op.execute("UPDATE pipelines SET metrics_topic = 'rule-statistics' WHERE metrics_topic = 'detectflow.rule-metrics'")

    op.alter_column(
        "pipelines",
        "rules_topic",
        existing_type=sa.String(255),
        server_default="sigma-rules",
    )
    op.alter_column(
        "pipelines",
        "metrics_topic",
        existing_type=sa.String(255),
        server_default="rule-statistics",
    )
