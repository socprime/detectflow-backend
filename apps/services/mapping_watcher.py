"""Background worker that processes mapping generation jobs from a queue.

MappingWatcherService is started in the application lifespan (server.py).
Jobs are enqueued via POST /api/v1/generate-mapping; status is polled via
GET /api/v1/generate-mapping/status/{job_id}. Completed/failed jobs are
removed after the client fetches the result once.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.managers.mapping import MappingManager

logger = get_logger(__name__)


@dataclass
class MappingJobStatus:
    """Status of a single mapping generation job.

    Attributes:
        status: One of 'queued', 'running', 'completed', 'failed'.
        started_at: When the job started processing (None while queued).
        completed_at: When the job finished (None until completed/failed).
        error: Error message when status is 'failed'.
        result: Mapping YAML string when status is 'completed'.
    """

    status: str  # queued, running, completed, failed
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    result: str | None = None  # mapping YAML when completed


@dataclass
class _MappingJobPayload:
    """Internal payload for a single job in the queue."""

    job_id: str
    repository_ids: list[str]
    topics: list[str]
    parser_query: str


_QUEUE_SENTINEL: object = object()


class MappingWatcherService:
    """Background service that processes mapping generation jobs from a queue.

    Maintains an in-memory queue and per-job status (queued → running → completed/failed).
    Start with start(); stop with shutdown() (e.g. from application lifespan).
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[_MappingJobPayload | object] = asyncio.Queue()
        self._jobs: dict[str, MappingJobStatus] = {}
        self._worker_task: asyncio.Task[None] | None = None

    def enqueue(
        self,
        repository_ids: list[str],
        topics: list[str],
        parser_query: str,
    ) -> str:
        """Put a mapping job into the queue. Returns job_id."""
        job_id = str(uuid4())
        self._jobs[job_id] = MappingJobStatus(status="queued")
        self._queue.put_nowait(
            _MappingJobPayload(
                job_id=job_id,
                repository_ids=repository_ids,
                topics=topics,
                parser_query=parser_query,
            )
        )
        logger.info(
            "Mapping job enqueued",
            extra={"job_id": job_id, "repository_ids": repository_ids, "topics": topics},
        )
        return job_id

    def get_status(self, job_id: str) -> MappingJobStatus | None:
        """Get status for a job. Returns None if job_id is unknown."""
        return self._jobs.get(job_id)

    def remove_job(self, job_id: str) -> None:
        """Remove job from storage after client has retrieved the result (completed or failed)."""
        self._jobs.pop(job_id, None)

    async def _worker(self) -> None:
        """Process jobs from the queue one by one."""
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                break
            if item is _QUEUE_SENTINEL:
                break
            payload = item
            job_id = payload.job_id
            self._jobs[job_id] = MappingJobStatus(
                status="running",
                started_at=datetime.now(UTC),
            )
            try:
                async with AsyncSessionLocal() as db:
                    manager = MappingManager(db)
                    result = await manager.generate_mapping(
                        repository_ids=payload.repository_ids,
                        topics=payload.topics,
                        parser_query=payload.parser_query,
                    )
                    self._jobs[job_id] = MappingJobStatus(
                        status="completed",
                        started_at=self._jobs[job_id].started_at,
                        completed_at=datetime.now(UTC),
                        error=None,
                        result=result,
                    )
                    logger.info("Mapping job completed successfully", extra={"job_id": job_id})
            except Exception as e:
                self._jobs[job_id] = MappingJobStatus(
                    status="failed",
                    started_at=self._jobs[job_id].started_at,
                    completed_at=datetime.now(UTC),
                    error=str(e),
                    result=None,
                )
                logger.error("Mapping job failed", exc_info=True, extra={"job_id": job_id})

    def start(self) -> None:
        """Start the background worker that processes the queue."""
        if self._worker_task is not None and not self._worker_task.done():
            return
        self._worker_task = asyncio.create_task(self._worker())
        logger.info("Mapping watcher started")

    async def shutdown(self) -> None:
        """Stop the worker and drain the queue."""
        if self._worker_task is None:
            return
        self._queue.put_nowait(_QUEUE_SENTINEL)
        self._worker_task.cancel()
        try:
            await asyncio.wait_for(self._worker_task, timeout=5.0)
        except (TimeoutError, asyncio.CancelledError):
            pass
        self._worker_task = None
        logger.info("Mapping watcher stopped")


_mapping_watcher: MappingWatcherService | None = None


def get_mapping_watcher() -> MappingWatcherService:
    """Return the global MappingWatcherService instance (creates it on first call)."""
    global _mapping_watcher
    if _mapping_watcher is None:
        _mapping_watcher = MappingWatcherService()
    return _mapping_watcher
