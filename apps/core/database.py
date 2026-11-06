from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import Pool

from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.settings import settings

logger = get_logger(__name__)

# Emit a throttled warning once pool utilization reaches this percentage — an early signal of
# a connection-hold leak before it becomes a hard ``QueuePool limit ... reached`` failure.
POOL_UTILIZATION_WARN_PERCENT = 80.0

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
    """Log when a connection is checked out from the pool; warn on high utilization."""
    logger.debug(
        "Connection checked out from pool",
        extra={"connection_id": id(dbapi_conn)},
    )
    _warn_on_high_pool_utilization()


def _warn_on_high_pool_utilization() -> None:
    """Emit a throttled warning when pool utilization crosses the warn threshold.

    Called from the pool ``checkout`` listener (no extra background task). ``ErrorTracker``
    throttles the message to at most once per cooldown so a sustained drain does not spam logs;
    the full ``get_pool_status()`` snapshot is attached for diagnosis.
    """
    status = get_pool_status()
    if status["utilization_percent"] >= POOL_UTILIZATION_WARN_PERCENT and ErrorTracker.should_log(
        "db_pool_high_utilization"
    ):
        logger.warning(
            "Database connection pool utilization high",
            extra={"db_pool": status},
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
    """Log when a connection is invalidated (e.g., due to error).

    Note: This is logged to application logs only, not audit logs.
    Connection invalidation is normal during request cancellation (client disconnect,
    SSE close, timeout) and would create noise in audit UI.
    """
    reason = str(exception) if exception else "unknown"
    logger.warning(
        "Database connection invalidated",
        extra={
            "connection_id": id(dbapi_conn),
            "reason": reason,
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

    The denominator (``max_capacity``) is the **configured** limit
    ``settings.database_pool_size + settings.database_max_overflow``. It must NOT be derived
    from ``pool.overflow()``: in SQLAlchemy that is the *runtime* overflow counter, which
    starts at ``-pool_size`` and only reaches the configured maximum when every overflow
    connection is open — deriving capacity from it makes ``utilization_percent`` read ~100%
    with a single checkout and renders the high-utilization warning meaningless.

    Returns:
        Dictionary with pool statistics:
        - pool_size: Configured number of permanent pooled connections
        - max_overflow: Configured max overflow connections beyond pool_size
        - checked_out: Currently active (in-use) connections
        - checked_in: Idle connections in pool
        - total: Total connections (checked_out + checked_in)
        - overflow_current: Overflow connections currently in use beyond pool_size
        - max_capacity: Total possible connections (configured pool_size + max_overflow)
        - utilization_percent: checked_out as a percentage of max_capacity
    """
    pool = engine.pool
    checked_out = pool.checkedout()
    max_capacity = settings.database_pool_size + settings.database_max_overflow
    utilization_percent = (checked_out / max_capacity * 100) if max_capacity > 0 else 0

    return {
        "pool_size": settings.database_pool_size,
        "max_overflow": settings.database_max_overflow,
        "checked_out": checked_out,
        "checked_in": pool.checkedin(),
        "total": checked_out + pool.checkedin(),
        "overflow_current": max(0, checked_out - settings.database_pool_size),
        "max_capacity": max_capacity,
        "utilization_percent": round(utilization_percent, 1),
    }
