"""add sigma severity level to rules and high_order_sigmas

Revision ID: bf9230603f85
Revises: 757bde054750
Create Date: 2026-08-07

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from apps.modules.utils import get_level_from_sigma_body

revision: str = "bf9230603f85"
down_revision: str | Sequence[str] | None = "757bde054750"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _backfill_rule_level() -> None:
    """Derive ``rules.level`` from each rule's CURRENT body version.

    The body lives in ``rule_content``, keyed by ``(rule_id, version)``; the current version is
    the one matching ``rules.version``. Parsing goes through YAML rather than a regex because
    real bodies quote the value, use CRLF endings, and sometimes carry an indented ``Level:``
    inside ``detection`` that must not be mistaken for the rule's severity. Bodies that are not
    valid YAML yield no level, exactly as they already yield no ``sigma_uuid``/``product``.
    """
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            """
            SELECT r.id, c.body
            FROM rules r
            JOIN rule_content c ON c.rule_id = r.id AND c.version = r.version
            """
        )
    ).mappings()
    updates = [
        {"id": str(row["id"]), "level": level}
        for row in rows
        if (level := get_level_from_sigma_body(row["body"])) is not None
    ]
    if not updates:
        return

    connection.execute(
        sa.text("UPDATE rules SET level = :level WHERE id = CAST(:id AS uuid)"),
        updates,
    )


def _backfill_high_order_sigma_level() -> None:
    connection = op.get_bind()
    rows = connection.execute(sa.text("SELECT id, body FROM high_order_sigmas WHERE body IS NOT NULL")).mappings()
    updates = [
        {"id": str(row["id"]), "level": level}
        for row in rows
        if (level := get_level_from_sigma_body(row["body"])) is not None
    ]
    if not updates:
        return

    connection.execute(
        sa.text("UPDATE high_order_sigmas SET level = :level WHERE id = CAST(:id AS uuid)"),
        updates,
    )


def upgrade() -> None:
    op.add_column("rules", sa.Column("level", sa.String(length=255), nullable=True))
    op.add_column("high_order_sigmas", sa.Column("level", sa.String(length=255), nullable=True))
    _backfill_rule_level()
    _backfill_high_order_sigma_level()


def downgrade() -> None:
    op.drop_column("high_order_sigmas", "level")
    op.drop_column("rules", "level")
