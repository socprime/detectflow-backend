"""In-memory TTL cache for correlation-pipeline ``runtime_state``.

Mirrors detection-pipeline ``PipelineStatusManager``: 15s TTL, ``jobs_status`` is part of the
key, start/stop invalidate, stopped rows never enter the cache.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import UUID

from apps.core.schemas import AttackChainsJobState, AttackChainsRuntimeStateResponse
from apps.managers.correlation_runtime_status import (
    CACHE_TTL_SECONDS,
    CorrelationRuntimeStatusCache,
)

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


def _state(overall: str = "running") -> AttackChainsRuntimeStateResponse:
    return AttackChainsRuntimeStateResponse(
        overall=overall,
        correlation_job=AttackChainsJobState(deployment_name="corr-a", status="running"),
        agent_job=AttackChainsJobState(deployment_name="agent-a", status="running"),
        job_uptime="1m 0s",
    )


def test_miss_then_hit_within_ttl() -> None:
    cache = CorrelationRuntimeStatusCache()
    assert cache.get(PIPELINE_A, "starting") is None

    cache.put(PIPELINE_A, "starting", _state())

    hit = cache.get(PIPELINE_A, "starting")
    assert hit is not None
    assert hit.overall == "running"
    assert hit.job_uptime == "1m 0s"


def test_jobs_status_mismatch_is_a_miss() -> None:
    cache = CorrelationRuntimeStatusCache()
    cache.put(PIPELINE_A, "starting", _state())

    assert cache.get(PIPELINE_A, "stopping") is None
    assert cache.get(PIPELINE_A, "starting") is not None


def test_stopped_is_never_cached() -> None:
    cache = CorrelationRuntimeStatusCache()
    cache.put(PIPELINE_A, "stopped", _state("stopped"))

    assert cache.get(PIPELINE_A, "stopped") is None


def test_put_stores_a_copy_so_later_mutation_cannot_corrupt_the_cache() -> None:
    cache = CorrelationRuntimeStatusCache()
    live = _state()
    cache.put(PIPELINE_A, "starting", live)
    live.overall = "failed"
    live.correlation_job.health = "error"

    hit = cache.get(PIPELINE_A, "starting")
    assert hit is not None
    assert hit.overall == "running"
    assert hit.correlation_job.health == "healthy"


def test_get_returns_a_copy() -> None:
    cache = CorrelationRuntimeStatusCache()
    cache.put(PIPELINE_A, "starting", _state())
    first = cache.get(PIPELINE_A, "starting")
    assert first is not None
    first.overall = "failed"

    second = cache.get(PIPELINE_A, "starting")
    assert second is not None
    assert second.overall == "running"


def test_invalidate_one_leaves_the_other() -> None:
    cache = CorrelationRuntimeStatusCache()
    cache.put(PIPELINE_A, "starting", _state())
    cache.put(PIPELINE_B, "starting", _state())

    cache.invalidate(PIPELINE_A)

    assert cache.get(PIPELINE_A, "starting") is None
    assert cache.get(PIPELINE_B, "starting") is not None


def test_invalidate_all() -> None:
    cache = CorrelationRuntimeStatusCache()
    cache.put(PIPELINE_A, "starting", _state())
    cache.put(PIPELINE_B, "starting", _state())

    cache.invalidate()

    assert cache.get(PIPELINE_A, "starting") is None
    assert cache.get(PIPELINE_B, "starting") is None


def test_expired_entry_is_a_miss() -> None:
    cache = CorrelationRuntimeStatusCache()
    now = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)
    with patch("apps.managers.correlation_runtime_status.datetime") as dt:
        dt.now.return_value = now
        cache.put(PIPELINE_A, "starting", _state())

        dt.now.return_value = now + timedelta(seconds=CACHE_TTL_SECONDS + 1)
        assert cache.get(PIPELINE_A, "starting") is None
