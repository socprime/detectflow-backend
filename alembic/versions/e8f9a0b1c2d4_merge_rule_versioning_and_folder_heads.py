"""merge rule versioning and virtual-folder heads

Revision ID: e8f9a0b1c2d4
Revises: c4f2cc774378, b9c0d1e2f3a4
Create Date: 2026-06-19

Prod is stamped at ``c4f2cc774378`` (rule versioning). Folder
migrations branch from ``e7f8a9b0c1d2`` via ``f6a7b8c9d0e1`` →
``b9c0d1e2f3a4``. This empty merge lets ``alembic upgrade head`` apply the
folder branch on prod, then continue with ``d2e3f4a5b6c7``.
"""

from collections.abc import Sequence

revision: str = "e8f9a0b1c2d4"
down_revision: str | Sequence[str] | None = ("c4f2cc774378", "b9c0d1e2f3a4")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Merge revision: no schema changes."""
    pass


def downgrade() -> None:
    """Merge revision: no schema changes."""
    pass
