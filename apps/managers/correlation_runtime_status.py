"""In-memory TTL cache for observed correlation-pipeline runtime state.

Mirrors :class:`apps.managers.pipeline_status.PipelineStatusManager`: the correlation-pipeline
list serves ``runtime_state`` from this cache instead of hitting Kubernetes on every request.
TTL is 15s, the same as detection-pipeline status, so a UI that polls the list is almost always
a cache hit. ``jobs_status`` is part of the key so a start or stop cannot return a stale
composite; lifecycle methods invalidate.

Stopped pipelines never enter the cache — their observed state is derived from ``jobs_status``
alone (both jobs missing), the same way a disabled detection pipeline never queries Flink.

SSE ticks and live ``get_runtime_state`` write through so a warm process (someone has the
dashboard open, or a recent detail fetch) serves the list from memory without a dedicated
background monitor.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from apps.core.schemas import AttackChainsRuntimeStateResponse

# Same bound as ``PipelineStatusManager.CACHE_TTL_SECONDS``. A UI poll faster than this is a
# cache hit; start/stop/restart invalidate so the next read is live.
CACHE_TTL_SECONDS = 15


@dataclass
class _CachedRuntime:
    jobs_status: str
    state: AttackChainsRuntimeStateResponse
    expires_at: datetime


class CorrelationRuntimeStatusCache:
    """Process-local cache of ``AttackChainsRuntimeStateResponse`` keyed by pipeline id."""

    def __init__(self) -> None:
        self._cache: dict[UUID, _CachedRuntime] = {}

    def get(self, pipeline_id: UUID, jobs_status: str) -> AttackChainsRuntimeStateResponse | None:
        """Return a copy of the cached state when it is still valid for this desired state."""
        entry = self._cache.get(pipeline_id)
        if entry is None or entry.jobs_status != jobs_status or entry.expires_at <= datetime.now(UTC):
            return None
        return entry.state.model_copy(deep=True)

    def put(self, pipeline_id: UUID, jobs_status: str, state: AttackChainsRuntimeStateResponse) -> None:
        """Store a copy so later health-signal mutation of the live object cannot corrupt the cache."""
        if jobs_status == "stopped":
            return
        self._cache[pipeline_id] = _CachedRuntime(
            jobs_status=jobs_status,
            state=state.model_copy(deep=True),
            expires_at=datetime.now(UTC) + timedelta(seconds=CACHE_TTL_SECONDS),
        )

    def invalidate(self, pipeline_id: UUID | None = None) -> None:
        """Drop one pipeline, or the whole cache when ``pipeline_id`` is omitted."""
        if pipeline_id is None:
            self._cache.clear()
        else:
            self._cache.pop(pipeline_id, None)


correlation_runtime_status_cache = CorrelationRuntimeStatusCache()
