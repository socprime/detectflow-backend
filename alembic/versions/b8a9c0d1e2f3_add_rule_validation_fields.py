"""add rule validation fields

Revision ID: b8a9c0d1e2f3
Revises: bcd6a58fca88
Create Date: 2026-02-25 10:00:00.000000

Unsupported sigma label feature
- Add validation fields to rules table (is_supported, unsupported_reason, validated_at, validated_with_version)
- Add needs_restart flag to pipelines table
- Create rule_loader_module_versions table for tracking validation module versions
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b8a9c0d1e2f3"
down_revision: str | Sequence[str] | None = "bcd6a58fca88"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add rule validation fields and rule_loader_module_versions table."""
    # Add validation fields to rules table
    op.add_column(
        "rules",
        sa.Column("is_supported", sa.Boolean(), nullable=False, server_default="true"),
    )
    op.add_column(
        "rules",
        sa.Column("unsupported_reason", sa.String(length=512), nullable=True),
    )
    op.add_column(
        "rules",
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "rules",
        sa.Column("validated_with_version", sa.String(length=50), nullable=True),
    )

    # Add needs_restart flag to pipelines table
    op.add_column(
        "pipelines",
        sa.Column("needs_restart", sa.Boolean(), nullable=False, server_default="false"),
    )

    # Add index on is_supported for filtering queries
    op.create_index("ix_rules_is_supported", "rules", ["is_supported"])

    # Add index on validated_with_version for efficient re-validation queries (ISSUE #7)
    op.create_index("ix_rules_validated_with_version", "rules", ["validated_with_version"])

    # Create rule_loader_module_versions table
    op.create_table(
        "rule_loader_module_versions",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("version", sa.String(length=50), nullable=False),
        sa.Column("is_current", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
    )

    # Add partial unique index to ensure only one version can be current (ISSUE #8)
    op.create_index(
        "ix_rule_loader_module_versions_is_current_unique",
        "rule_loader_module_versions",
        ["is_current"],
        unique=True,
        postgresql_where=sa.text("is_current = true"),
    )

    # Seed initial version "0.0.0" so that first deploy triggers needs_restart on all pipelines
    # When backend starts with schema-parser 0.3.0, it will detect version change 0.0.0 → 0.3.0
    # and mark all enabled pipelines for restart, triggering re-validation of all rules
    op.execute(
        sa.text(
            """
            INSERT INTO rule_loader_module_versions (id, version, is_current, created)
            VALUES (gen_random_uuid(), '0.0.0', true, now())
            """
        )
    )


def downgrade() -> None:
    """Remove rule validation fields and rule_loader_module_versions table."""
    # Drop partial unique index on is_current
    op.drop_index("ix_rule_loader_module_versions_is_current_unique", table_name="rule_loader_module_versions")

    # Drop rule_loader_module_versions table
    op.drop_table("rule_loader_module_versions")

    # Drop index on validated_with_version
    op.drop_index("ix_rules_validated_with_version", table_name="rules")

    # Drop index on is_supported
    op.drop_index("ix_rules_is_supported", table_name="rules")

    # Remove needs_restart from pipelines
    op.drop_column("pipelines", "needs_restart")

    # Remove validation fields from rules
    op.drop_column("rules", "validated_with_version")
    op.drop_column("rules", "validated_at")
    op.drop_column("rules", "unsupported_reason")
    op.drop_column("rules", "is_supported")
