"""In-memory invalidation bus for the attack-chains list SSE stream.

Bumps a monotonic ``revision`` when the catalog changes (Kafka ``chains-analysed`` upsert or
user ``PATCH`` status). SSE subscribers receive lightweight ``attack_chains_changed`` events;
clients refetch ``GET /api/v1/attack_chains/chains`` with their current filters.
"""

import asyncio

from apps.core.logger import get_logger

logger = get_logger(__name__)


class AttackChainsListInvalidationService:
    """Global revision counter and per-connection subscriber queues."""

    def __init__(self) -> None:
        self._revision: int = 0
        self._subscribers: list[asyncio.Queue[int]] = []

    @property
    def revision(self) -> int:
        return self._revision

    async def subscribe(self) -> asyncio.Queue[int]:
        queue: asyncio.Queue[int] = asyncio.Queue()
        self._subscribers.append(queue)
        logger.debug("Attack chains list SSE subscriber added", extra={"total": len(self._subscribers)})
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[int]) -> None:
        if queue in self._subscribers:
            self._subscribers.remove(queue)
            logger.debug("Attack chains list SSE subscriber removed", extra={"total": len(self._subscribers)})

    def notify_changed(self) -> int:
        """Increment revision and wake all SSE subscribers."""
        self._revision += 1
        revision = self._revision
        for queue in list(self._subscribers):
            queue.put_nowait(revision)
        logger.debug("Attack chains list revision bumped", extra={"revision": revision})
        return revision


attack_chains_list_invalidation_service = AttackChainsListInvalidationService()
