"""Tests for the SSE broadcaster (compute once per tick, fan out to all clients).

Covers ``apps/managers/sse_broadcaster.SSEBroadcaster`` plus the dashboard SSE envelope
serialization helper. See ``tasks/T2_dashboard_broadcaster.md``.
"""

import asyncio
import json

import pytest

from apps.core.schemas import CorrelationJobGraphData
from apps.managers.sse_broadcaster import SSEBroadcaster
from apps.routers.correlation_pipelines import _correlation_dashboard_sse_event
from apps.routers.dashboard import _dashboard_sse_event


class _Payload:
    """Minimal stand-in with a ``model_dump`` for the dashboard envelope helper."""

    def __init__(self, value: int) -> None:
        self.value = value

    def model_dump(self, mode: str = "python") -> dict:
        return {"value": self.value}


def _counting_compute() -> tuple[callable, list[int]]:
    """Return an async compute that counts invocations and the shared call log."""
    calls: list[int] = []

    async def compute() -> _Payload:
        calls.append(1)
        return _Payload(len(calls))

    return compute, calls


async def test_single_initial_compute_for_many_clients() -> None:
    """Three concurrent subscribers trigger exactly one initial computation (O(1), not O(N))."""
    compute, calls = _counting_compute()
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=100.0)

    await broadcaster.subscribe()
    await broadcaster.subscribe()
    await broadcaster.subscribe()

    try:
        assert broadcaster.subscriber_count == 3
        assert broadcaster.is_running is True
        # Only the first subscriber computed; the other two reused the cache.
        assert len(calls) == 1
        # New client receives data immediately on connect.
        payload, error = broadcaster.get_cached()
        assert error is None
        assert payload is not None and payload.value == 1
    finally:
        await broadcaster.stop()


async def test_one_compute_per_tick_regardless_of_client_count() -> None:
    """The refresher computes once per tick for all clients, not once per client per tick."""
    compute, calls = _counting_compute()
    interval = 0.02
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=interval)

    await broadcaster.subscribe()
    await broadcaster.subscribe()  # 2 clients share one refresher
    try:
        await asyncio.sleep(interval * 4.5)
        # ~1 initial + ~4 ticks. If it were per-client (x2) we would see ~10+.
        assert len(calls) <= 7
        assert len(calls) >= 2
    finally:
        await broadcaster.stop()


async def test_no_recompute_when_no_clients() -> None:
    """After the last unsubscribe the refresher stops and no further compute happens."""
    compute, calls = _counting_compute()
    interval = 0.02
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=interval)

    await broadcaster.subscribe()
    await broadcaster.unsubscribe()

    assert broadcaster.is_running is False
    assert broadcaster.subscriber_count == 0
    # Cache cleared and no periodic work continues.
    assert broadcaster.get_cached() == (None, None)

    calls_after_stop = len(calls)
    await asyncio.sleep(interval * 3)
    assert len(calls) == calls_after_stop


async def test_error_tick_caches_error_and_calls_on_error() -> None:
    """A failing compute is captured as an error (not raised) and on_error fires once."""
    on_error_calls: list[str] = []

    async def compute() -> _Payload:
        raise RuntimeError("boom")

    async def on_error(exc: Exception) -> None:
        on_error_calls.append(str(exc))

    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=100.0, on_error=on_error)

    await broadcaster.subscribe()
    try:
        payload, error = broadcaster.get_cached()
        assert payload is None
        assert isinstance(error, RuntimeError)
        assert on_error_calls == ["boom"]
    finally:
        await broadcaster.stop()


async def test_stop_cancels_refresher_without_warnings() -> None:
    """stop() force-cancels the refresher task cleanly regardless of subscriber count."""
    compute, _ = _counting_compute()
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=0.01)

    await broadcaster.subscribe()
    assert broadcaster.is_running is True

    await broadcaster.stop()
    assert broadcaster.is_running is False
    assert broadcaster.subscriber_count == 0


async def test_last_unsubscribe_racing_first_subscribe_keeps_live_refresher() -> None:
    """Regression: unsubscribe(1->0) racing subscribe(0->1) must not kill the new refresher.

    Old bug: the refcount decision and the task create/cancel were not atomic — client X's
    last-unsubscribe could cancel the refresher client Y's concurrent first-subscribe had just
    created, leaving Y subscribed with a dead refresher and an empty cache (permanent
    ``no data available`` error frames). Interleaving is driven deterministically: the
    unsubscribe task is started first so it enters the critical section, and the subscribe
    task queues behind it on the lock.
    """
    compute, calls = _counting_compute()
    interval = 0.02
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=interval)

    # Client X is the sole subscriber.
    await broadcaster.subscribe()
    assert broadcaster.is_running is True

    # X's unsubscribe (1 -> 0, decides to stop) starts first; Y's subscribe (0 -> 1) races it.
    unsubscribe_task = asyncio.create_task(broadcaster.unsubscribe())
    await asyncio.sleep(0)  # let unsubscribe enter the critical section first
    subscribe_task = asyncio.create_task(broadcaster.subscribe())
    await asyncio.gather(unsubscribe_task, subscribe_task)

    try:
        # Y survives with a LIVE refresher and a populated cache.
        assert broadcaster.subscriber_count == 1
        assert broadcaster.is_running is True
        payload, error = broadcaster.get_cached()
        assert error is None
        assert payload is not None

        # And the refresher actually keeps refreshing (not a cancelled zombie).
        ticks_before = len(calls)
        await asyncio.sleep(interval * 3)
        assert len(calls) > ticks_before
    finally:
        await broadcaster.stop()


async def test_concurrent_churn_never_strands_a_subscriber() -> None:
    """Stress the subscribe/unsubscribe transitions in both orders; invariants must hold."""
    compute, calls = _counting_compute()
    interval = 0.02
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=interval)

    await broadcaster.subscribe()
    for i in range(10):
        if i % 2 == 0:
            await asyncio.gather(broadcaster.unsubscribe(), broadcaster.subscribe())
        else:
            await asyncio.gather(broadcaster.subscribe(), broadcaster.unsubscribe())

    try:
        # Exactly one subscriber remains, its refresher is alive, and the cache refreshes.
        assert broadcaster.subscriber_count == 1
        assert broadcaster.is_running is True
        assert broadcaster.get_cached()[0] is not None
        ticks_before = len(calls)
        await asyncio.sleep(interval * 3)
        assert len(calls) > ticks_before
    finally:
        await broadcaster.stop()


async def test_refresh_recomputes_only_while_the_refresher_runs() -> None:
    """The SSEBroadcaster refresh path — one compute mechanism, not two."""
    compute, calls = _counting_compute()
    broadcaster: SSEBroadcaster[_Payload] = SSEBroadcaster("t", compute, interval=100.0)

    # Idle: nothing to refresh, and no accidental first compute.
    await broadcaster.refresh()
    assert calls == []

    await broadcaster.subscribe()
    try:
        assert len(calls) == 1
        await broadcaster.refresh()
        assert len(calls) == 2
        payload, error = broadcaster.get_cached()
        assert error is None
        assert payload is not None and payload.value == 2
    finally:
        await broadcaster.stop()


def test_dashboard_envelope_success_shape() -> None:
    """Success frame keeps the ``dashboard_update`` envelope keys unchanged."""
    frame = _dashboard_sse_event(_Payload(5), None, include_recent_activity=True)
    assert frame.startswith("data: ")
    assert frame.endswith("\n\n")
    event = json.loads(frame[len("data: ") :].strip())
    assert set(event) == {"type", "data", "timestamp"}
    assert event["type"] == "dashboard_update"
    assert event["data"] == {"value": 5}


def test_dashboard_envelope_error_shape() -> None:
    """A tick error yields a ``type: 'error'`` frame."""
    frame = _dashboard_sse_event(None, RuntimeError("kaboom"), include_recent_activity=True)
    event = json.loads(frame[len("data: ") :].strip())
    assert event["type"] == "error"
    assert event["error"] == "kaboom"
    assert "timestamp" in event


@pytest.mark.parametrize("bad", [None])
def test_envelope_none_without_error_is_error_frame(bad) -> None:
    """A missing payload with no explicit error still degrades to an error frame."""
    event = json.loads(_dashboard_sse_event(bad, None, include_recent_activity=True)[len("data: ") :].strip())
    assert event["type"] == "error"


def test_correlation_dashboard_envelope_success_shape() -> None:
    """Success frame is graph-only: ``blocks``, no ``runtime_state``."""
    frame = _correlation_dashboard_sse_event(CorrelationJobGraphData(blocks=[]), None)
    assert frame.startswith("data: ")
    assert frame.endswith("\n\n")
    event = json.loads(frame[len("data: ") :].strip())
    assert set(event) == {"type", "data", "timestamp"}
    assert event["type"] == "correlation_dashboard_update"
    assert event["data"] == {"blocks": []}
    assert "runtime_state" not in event["data"]


def test_correlation_dashboard_envelope_error_shape() -> None:
    event = json.loads(_correlation_dashboard_sse_event(None, RuntimeError("kaboom"))[len("data: ") :].strip())
    assert event["type"] == "error"
    assert event["error"] == "kaboom"
    assert "timestamp" in event
