"""Alembic environment configuration for async SQLAlchemy."""

from logging.config import fileConfig

from sqlalchemy import create_engine, pool

from alembic import context
from apps.core.database import Base

# Import all models to register them with Base.metadata
from apps.core.models import (  # noqa: F401
    AuditLog,
    Config,
    Event,
    Filter,
    LogSource,
    Parser,
    Pipeline,
    PipelineMetric,
    PipelineRule,
    PipelineRuleMetric,
    Repository,
    Rule,
    User,
    pipeline_repositories,
)

# Import our app's settings and models
from apps.core.settings import settings

# Alembic Config object
config = context.config

# Setup Python logging from config file
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set target metadata for autogenerate support
target_metadata = Base.metadata


def get_sync_database_url() -> str:
    """Convert async database URL to sync for Alembic.

    Alembic runs synchronously, so we need to use psycopg2 driver
    instead of asyncpg.
    """
    url = settings.database_url
    # Replace async driver with sync driver
    if "postgresql+asyncpg://" in url:
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg2://")
    if "postgresql://" in url:
        return url.replace("postgresql://", "postgresql+psycopg2://")
    return url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This generates SQL scripts without connecting to the database.
    Useful for reviewing migrations before applying them.
    """
    url = get_sync_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    Creates a database connection and applies migrations directly.
    """
    connectable = create_engine(
        get_sync_database_url(),
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
