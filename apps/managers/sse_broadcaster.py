"""Single-producer / many-consumer broadcaster for SSE dashboards.

Each SSE endpoint (dashboard, correlation graph) used to recompute its payload **per
client per tick** — N open browser tabs meant N duplicate bursts of DB queries plus Flink/K8s
HTTP every ``dashboard_broadcast_interval_seconds``. Combined with the cookie-auth connection
leak (see ``get_current_user_from_cookie``) that produced the production pool-exhaustion
outage; even without the leak it is a needless O(clients) load on PostgreSQL, Flink REST, and
the Kubernetes API for identical data.

``SSEBroadcaster`` runs a single background refresher that computes the payload **once per
tick** and caches it; every subscribed SSE generator serializes the same cached value, so the
cost is O(1) regardless of client count. The refresher runs **only while at least one client
is connected** (ref-counted ``subscribe``/``unsubscribe``) — an idle deployment does no
periodic SSE work.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from apps.core.logger import get_logger

logger = get_logger(__name__)

T = TypeVar("T")


class SSEBroadcaster(Generic[T]):
    """Compute a payload once per tick and fan it out to all SSE subscribers.

    Args:
        name: Identifier used in log messages.
        compute: Async factory that produces the latest payload (its own short-lived DB
            session / HTTP calls live inside it — the broadcaster never holds a session).
        interval: Seconds between refreshes.
        on_error: Optional async callback invoked once per failed tick (e.g. throttled
            activity logging). Never raises out of the refresher.
    """

    def __init__(
        self,
        name: str,
        compute: Callable[[], Awaitable[T]],
        interval: float,
        on_error: Callable[[Exception], Awaitable[None]] | None = None,
    ) -> None:
        self._name = name
        self._compute = compute
        self._interval = interval
        self._on_error = on_error
        self._subscribers = 0
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._latest: T | None = None
        self._error: Exception | None = None
        self._has_value = False

    @property
    def subscriber_count(self) -> int:
        """Number of currently connected SSE clients."""
        return self._subscribers

    @property
    def is_running(self) -> bool:
        """True while the background refresher task is active."""
        return self._task is not None and not self._task.done()

    def get_cached(self) -> tuple[T | None, Exception | None]:
        """Return the latest ``(payload, error)`` snapshot without any DB/HTTP work.

        Exactly one of the two is meaningful: a successful tick sets ``payload`` and clears
        ``error``; a failed tick keeps the last ``payload`` (may be ``None``) and sets
        ``error`` so subscribers can emit a ``type: "error"`` event.
        """
        return self._latest, self._error

    async def subscribe(self) -> None:
        """Register an SSE client; start the refresher on the first subscriber.

        Guarantees a cached payload exists before returning so a new client can emit an event
        immediately on connect.

        The refcount increment and the refresher-task creation happen atomically under
        ``self._lock`` (as do the decrement + cancel in :meth:`unsubscribe`). Deciding outside
        the lock allowed a race where a last-unsubscribe (1 -> 0) could cancel the refresher a
        concurrent first-subscribe (0 -> 1) had just created, leaving a live client with a dead
        refresher and an empty cache.
        """
        async with self._lock:
            self._subscribers += 1
            if self._task is None or self._task.done():
                # Compute the first payload synchronously so the very first client gets data
                # at once, then start the periodic refresher.
                await self._refresh_once()
                self._task = asyncio.create_task(self._run())
            elif not self._has_value:
                # Refresher is running for another client but has not produced a value yet.
                await self._refresh_once()

    async def unsubscribe(self) -> None:
        """Deregister an SSE client; stop the refresher when the last one leaves.

        Runs entirely under ``self._lock`` so the decision to stop and the cancel itself are
        atomic with respect to a concurrent :meth:`subscribe` — it can never cancel a refresher
        that a newer subscriber owns (see :meth:`subscribe`).
        """
        async with self._lock:
            if self._subscribers > 0:
                self._subscribers -= 1
            if self._subscribers == 0:
                await self._stop_task_locked()

    async def refresh(self) -> None:
        """Recompute the cached payload now, off the tick schedule.

        For callers whose compute set can change between ticks: it reuses the broadcaster's own
        compute path, so there is still exactly ONE producer of the payload.
        No-op when the refresher is not running — a subscribe will compute the first value anyway.
        """
        if self.is_running:
            await self._refresh_once()

    async def stop(self) -> None:
        """Force-stop the refresher on shutdown regardless of subscriber count."""
        async with self._lock:
            self._subscribers = 0
            await self._stop_task_locked()

    async def _stop_task_locked(self) -> None:
        """Cancel the refresher and clear the cache. Caller must hold ``self._lock``.

        Safe to await the cancelled task while holding the lock: the refresher task never
        acquires the lock, so no deadlock is possible.
        """
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._latest = None
        self._error = None
        self._has_value = False

    async def _refresh_once(self) -> None:
        """Recompute the cached payload once; capture (never raise) compute failures."""
        try:
            self._latest = await self._compute()
            self._error = None
            self._has_value = True
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - broadcaster must survive any compute failure
            self._error = e
            self._has_value = True
            logger.error("SSE broadcaster '%s' tick failed", self._name, extra={"error": str(e)})
            if self._on_error is not None:
                try:
                    await self._on_error(e)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("SSE broadcaster '%s' on_error callback failed", self._name)

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._refresh_once()
