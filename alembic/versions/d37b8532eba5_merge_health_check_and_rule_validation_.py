"""merge_health_check_and_rule_validation_heads

Revision ID: d37b8532eba5
Revises: b8a9c0d1e2f3, e4f5a6b7c8d9
Create Date: 2026-02-27 11:56:50.976242

"""

from collections.abc import Sequence

# revision identifiers, used by Alembic.
revision: str = "d37b8532eba5"
down_revision: str | Sequence[str] | None = ("b8a9c0d1e2f3", "e4f5a6b7c8d9")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
