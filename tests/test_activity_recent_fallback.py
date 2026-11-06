"""Tests: ``ActivityService.get_recent`` DB fallback runs once, not on every dashboard tick.

After a restart the in-memory activity buffer is empty; the dashboard SSE refresher calls
``get_recent()`` every 2s tick. The DB fallback must populate the buffer (newest-first,
capped at ``_max_recent``) so it queries once instead of once per tick until the first
Kafka activity event arrives.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from apps.core.schemas import ActivityEvent
from apps.managers import activity as activity_module
from apps.managers.activity import ActivityService


class _FakeSessionCM:
    async def __aenter__(self) -> SimpleNamespace:
        return SimpleNamespace()

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


def _audit_row(action: str, ts: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        timestamp=ts,
        action=action,
        entity_type="pipeline",
        entity_id="e1",
        entity_name="p1",
        user_id=None,
        user_email=None,
        details="details",
        source="user",
        severity="info",
    )


def _patch_db(monkeypatch, rows: list[SimpleNamespace]) -> tuple[list[int], AsyncMock]:
    """Patch AsyncSessionLocal (counting acquisitions) and AuditLogDAO in the activity module."""
    factory_calls: list[int] = []

    def factory() -> _FakeSessionCM:
        factory_calls.append(1)
        return _FakeSessionCM()

    dao = SimpleNamespace(get_recent=AsyncMock(return_value=rows))
    monkeypatch.setattr(activity_module, "AsyncSessionLocal", factory)
    monkeypatch.setattr(activity_module, "AuditLogDAO", lambda _db: dao)
    return factory_calls, dao.get_recent


async def test_db_fallback_queries_once_then_serves_from_buffer(monkeypatch) -> None:
    now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=UTC)
    rows = [_audit_row("create", now), _audit_row("update", now)]  # newest-first, like the DAO
    factory_calls, dao_get_recent = _patch_db(monkeypatch, rows)

    service = ActivityService(max_recent=5)

    first = await service.get_recent()
    assert len(factory_calls) == 1
    assert [item.action for item in first] == ["create", "update"]
    # Buffer populated newest-first from the fallback result.
    assert [item.action for item in service._recent_activities] == ["create", "update"]

    second = await service.get_recent()
    assert len(factory_calls) == 1  # NO second DB session — served from the buffer
    dao_get_recent.assert_awaited_once()
    assert [item.id for item in second] == [item.id for item in first]


async def test_db_fallback_caps_buffer_at_max_recent(monkeypatch) -> None:
    now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=UTC)
    rows = [_audit_row(f"a{i}", now) for i in range(5)]
    _patch_db(monkeypatch, rows)

    service = ActivityService(max_recent=3)

    result = await service.get_recent(limit=5)
    assert len(result) == 5  # this call's return honors the requested limit (unchanged)
    assert len(service._recent_activities) == 3  # buffer capped like on_activity_received


async def test_empty_db_fallback_is_not_cached(monkeypatch) -> None:
    """An empty DB result must not poison the buffer (semantics preserved: retry next call)."""
    factory_calls, _ = _patch_db(monkeypatch, [])

    service = ActivityService(max_recent=5)

    assert await service.get_recent() == []
    assert service._recent_activities == []
    assert await service.get_recent() == []
    assert len(factory_calls) == 2  # empty result -> fallback retried


async def test_kafka_event_still_wins_over_fallback_buffer(monkeypatch) -> None:
    """A Kafka event landing after the fallback keeps newest-first ordering in the buffer."""
    now = datetime(2026, 7, 6, 12, 0, 0, tzinfo=UTC)
    _patch_db(monkeypatch, [_audit_row("old", now)])

    service = ActivityService(max_recent=5)
    await service.get_recent()

    service.on_activity_received(
        ActivityEvent(
            id=str(uuid4()),
            timestamp=now,
            action="new",
            entity_type="pipeline",
            source="user",
        )
    )

    recent = await service.get_recent()
    assert [item.action for item in recent] == ["new", "old"]
