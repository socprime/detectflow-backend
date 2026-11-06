"""Single-leader election for multi-worker deployments (``uvicorn --workers N``).

The app starts several **singleton** background services in the server lifespan
(Kafka rule-metrics consumer, Flink monitor, scheduler, mapping watcher,
periodic audit cleanup). With more than one worker process those must run in
*exactly one* process, or the work is duplicated — e.g. the metrics consumer
subscribes with a unique Kafka group per instance and writes counters to
Postgres, so two workers would double-count every metric.

The Flink metrics poller is **not** in this set: it only fills an in-process
EPS cache that dashboard snapshot/SSE read, so every worker that serves those
endpoints must poll.

Election is a Postgres **session-level advisory lock** held on a dedicated
asyncpg connection (outside the SQLAlchemy pool, so it never occupies a pool
slot) for the process lifetime. ``pg_try_advisory_lock`` is granted to exactly
one session; Postgres releases the lock automatically when the holding
connection dies with its process, so a crashed leader frees the seat and a
standby worker's retry loop takes over.

Failure semantics:
- DB unreachable at startup → nobody is leader; every worker keeps retrying
  every ``retry_seconds`` and logs the error, so services come up as soon as
  the DB does.
- The leader's lock connection dies mid-flight (DB restart/failover) → the
  leader immediately tries to re-acquire; if another worker won the seat in
  the meantime, the old leader demotes (stops the singleton services).
"""

import asyncio
from typing import Any
from urllib.parse import urlencode

import asyncpg
from sqlalchemy.engine.url import make_url

from apps.core.logger import get_logger
from apps.core.settings import settings

logger = get_logger(__name__)

# Arbitrary but stable application-wide lock key ("DFLD" — DetectFlow LeaDer).
_LOCK_KEY = 0x44464C44


def _asyncpg_ssl(query: dict[str, str]) -> bool | str | None:
    """Map libpq ``ssl`` / ``sslmode`` query params to asyncpg's ``ssl`` kwarg.

    ``ssl=True`` is verify-full (system CAs + hostname). libpq ``require`` / ``prefer`` /
    ``allow`` are weaker, so those mode strings are passed through — asyncpg 0.30+ accepts them.
    """
    raw = query.get("sslmode") or query.get("ssl")
    if raw is None:
        return None
    value = str(raw).lower()
    if value in {"disable", "false", "0"}:
        return False
    if value in {"true", "1"}:
        return True
    if value in {"allow", "prefer", "require", "verify-ca", "verify-full"}:
        return value
    return None


def _asyncpg_connect_kwargs(database_url: str | None = None) -> dict[str, Any]:
    """Build ``asyncpg.connect`` kwargs from a SQLAlchemy ``DATABASE_URL``.

    SQLAlchemy's engine uses its own URL parser, so passwords with ``[`` / ``]`` work for
    the app pool. ``asyncpg.connect(dsn=...)`` uses ``urllib.parse``, which treats ``[`` in
    userinfo as an IPv6 host and raises ``Invalid IPv6 URL`` — so the leader never elects
    and metrics/audit consumers never start. Discrete ``host`` / ``user`` / ``password``
    kwargs skip that parser. ``make_url`` also decodes percent-encoded userinfo.

    Remaining query params (``sslrootcert``, ``application_name``, …) go on a userinfo-free
    ``dsn`` so asyncpg still sees them without putting the password through ``urllib.parse``.
    """
    url = make_url(database_url if database_url is not None else settings.database_url)
    args = url.translate_connect_args(username="user")
    query = dict(url.query)
    if "host" not in args and "host" in query:
        args["host"] = query.pop("host")
    if "port" not in args and "port" in query:
        args["port"] = int(query.pop("port"))
    ssl = _asyncpg_ssl(query)
    if ssl is not None:
        args["ssl"] = ssl
        query.pop("sslmode", None)
        query.pop("ssl", None)
    if query:
        args["dsn"] = "postgresql://?" + urlencode(query)
    return args


class LeaderElector:
    """Elect one leader among worker processes via a Postgres advisory lock.

    Usage (from the server lifespan)::

        elector = LeaderElector(on_promote=..., on_demote=...)
        await elector.start()      # tries to acquire immediately, then keeps a watch loop
        ...
        await elector.stop()       # demotes (if leader) and releases the lock

    ``on_promote`` is awaited when this process wins the seat (services start);
    ``on_demote`` is awaited if the seat is lost mid-flight (services stop).
    """

    def __init__(self, on_promote, on_demote, lock_key: int = _LOCK_KEY, retry_seconds: float = 15.0) -> None:
        self._on_promote = on_promote
        self._on_demote = on_demote
        self._key = lock_key
        self._retry_seconds = retry_seconds
        self._conn: asyncpg.Connection | None = None
        self._task: asyncio.Task | None = None
        self.is_leader: bool = False

    # Upper bound for any single statement on the lock connection. Without it a
    # dead-but-not-reset TCP connection could hang the liveness probe for
    # minutes while Postgres reaps the session (releasing the lock) and a
    # standby promotes — i.e. two leaders.
    _STATEMENT_TIMEOUT_S = 10.0

    async def _try_acquire(self) -> bool:
        """One acquisition attempt; opens the dedicated connection when needed."""
        try:
            if self._conn is None or self._conn.is_closed():
                self._conn = await asyncpg.connect(timeout=10, **_asyncpg_connect_kwargs())
            return bool(
                await asyncio.wait_for(
                    self._conn.fetchval("SELECT pg_try_advisory_lock($1)", self._key),
                    timeout=self._STATEMENT_TIMEOUT_S,
                )
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Leader election: acquire attempt failed: {e}")
            if self._conn is not None:
                try:
                    await self._conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self._conn = None
            return False

    async def _promote(self) -> None:
        self.is_leader = True
        logger.info("Leader election: this worker is now the LEADER — starting singleton services")
        await self._on_promote()

    async def _demote(self) -> None:
        self.is_leader = False
        logger.warning("Leader election: leadership lost — stopping singleton services on this worker")
        await self._on_demote()

    async def start(self) -> bool:
        """Attempt to become leader now, then keep a background watch/retry loop."""
        if await self._try_acquire():
            await self._promote()
        else:
            logger.info("Leader election: another worker holds the singleton-services lock; running as API-only worker")
        self._task = asyncio.create_task(self._watch_loop())
        return self.is_leader

    async def _watch_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._retry_seconds)
                if self.is_leader:
                    # Verify the lock connection is still alive; if it died, the
                    # lock is gone — try to win it back, else demote.
                    try:
                        await asyncio.wait_for(self._conn.fetchval("SELECT 1"), timeout=self._STATEMENT_TIMEOUT_S)
                    except Exception:  # noqa: BLE001
                        logger.warning("Leader election: lock connection lost; re-acquiring")
                        if self._conn is not None:
                            try:
                                await self._conn.close()
                            except Exception:  # noqa: BLE001
                                pass
                            self._conn = None
                        if not await self._try_acquire():
                            await self._demote()
                else:
                    if await self._try_acquire():
                        await self._promote()
            except asyncio.CancelledError:
                break
            except Exception as e:  # noqa: BLE001
                logger.error(f"Leader election: watch loop error: {e}")

    async def stop(self) -> None:
        """Cancel the watch loop and release the seat (used at graceful shutdown)."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._conn is not None:
            try:
                await self._conn.close()  # closing the session releases the advisory lock
            except Exception:  # noqa: BLE001
                pass
            self._conn = None
        self.is_leader = False
