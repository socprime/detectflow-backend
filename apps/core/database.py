from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import Pool

from apps.core.logger import get_logger
from apps.core.settings import settings

logger = get_logger(__name__)

engine = create_async_engine(
    settings.database_url,
    echo=settings.database_echo,
    future=True,
    # Connection pool configuration (best practices for production)
    pool_size=settings.database_pool_size,  # Number of permanent connections
    max_overflow=settings.database_max_overflow,  # Extra connections when pool is exhausted
    pool_recycle=settings.database_pool_recycle,  # Recycle connections to prevent stale connections
    pool_timeout=settings.database_pool_timeout,  # Timeout waiting for connection
    pool_pre_ping=True,  # Verify connection is alive before using (prevents "connection closed" errors)
)


# Connection pool monitoring events
@event.listens_for(Pool, "checkout")
def _on_checkout(dbapi_conn, connection_record, connection_proxy):
    """Log when a connection is checked out from the pool."""
    logger.debug(
        "Connection checked out from pool",
        extra={"connection_id": id(dbapi_conn)},
    )


@event.listens_for(Pool, "checkin")
def _on_checkin(dbapi_conn, connection_record):
    """Log when a connection is returned to the pool."""
    logger.debug(
        "Connection returned to pool",
        extra={"connection_id": id(dbapi_conn)},
    )


@event.listens_for(Pool, "connect")
def _on_connect(dbapi_conn, connection_record):
    """Log when a new connection is created."""
    logger.info(
        "New database connection created",
        extra={"connection_id": id(dbapi_conn)},
    )


@event.listens_for(Pool, "invalidate")
def _on_invalidate(dbapi_conn, connection_record, exception):
    """Log when a connection is invalidated (e.g., due to error)."""
    logger.warning(
        "Database connection invalidated",
        extra={
            "connection_id": id(dbapi_conn),
            "reason": str(exception) if exception else "unknown",
        },
    )


AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def get_pool_status() -> dict:
    """Get current connection pool status for monitoring.

    Returns:
        Dictionary with pool statistics:
        - pool_size: Configured pool size
        - max_overflow: Configured max overflow
        - checked_out: Currently active connections
        - overflow: Current overflow connections in use
        - checked_in: Idle connections in pool
        - total: Total connections (checked_out + checked_in)
    """
    pool = engine.pool
    return {
        "pool_size": pool.size(),
        "max_overflow": pool.overflow(),
        "checked_out": pool.checkedout(),
        "checked_in": pool.checkedin(),
        "total": pool.checkedout() + pool.checkedin(),
        "overflow_current": max(0, pool.checkedout() - pool.size()),
    }
