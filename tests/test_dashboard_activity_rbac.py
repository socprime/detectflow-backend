"""Viewer RBAC for dashboard ``recent_activity`` (same gate as ``GET /audit-logs``).

Viewers still receive graph / pipeline stats; activity items are emptied at the router
so the shared SSE cache is not mutated.
"""

import json
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

from apps.core.auth import get_current_active_user
from apps.core.enums import UserRole
from apps.core.schemas import ActivityItem, DashboardData, DashboardGraphData, DashboardSnapshotResponse
from apps.routers.dashboard import _dashboard_sse_event, _recent_activity_for_user
from server import app

_ACTIVITY = ActivityItem(
    id=str(uuid.uuid4()),
    timestamp="2026-08-14T09:00:00+00:00",
    action="create",
    entity_type="pipeline",
    entity_name="demo",
    details="created pipeline",
)


def _user(role: UserRole) -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), email=f"{role}@example.com", is_active=True, role=role)


def _graph() -> DashboardGraphData:
    return DashboardGraphData(
        source_topics=[],
        repositories=[],
        destination_topics=[],
        pipelines_count=0,
        total_events_eps=0.0,
        total_tagged_eps=0.0,
        total_untagged_eps=0.0,
        total_rules=0,
    )


def _snapshot() -> DashboardSnapshotResponse:
    return DashboardSnapshotResponse(
        graph=_graph(),
        pipelines_stats=[],
        recent_activity=[_ACTIVITY],
        last_kafka_update=datetime(2026, 8, 14, 9, 0, tzinfo=UTC),
        consumer_healthy=True,
    )


def test_recent_activity_kept_for_write_roles() -> None:
    items = [_ACTIVITY]
    assert _recent_activity_for_user(_user(UserRole.ADMIN), items) is items
    assert _recent_activity_for_user(_user(UserRole.USER), items) is items


def test_recent_activity_emptied_for_viewer() -> None:
    assert _recent_activity_for_user(_user(UserRole.VIEWER), [_ACTIVITY]) == []


def test_sse_frame_empties_activity_without_mutating_cache() -> None:
    data = DashboardData(graph=_graph(), pipelines_stats=[], recent_activity=[_ACTIVITY])

    frame = _dashboard_sse_event(data, None, include_recent_activity=False)
    payload = json.loads(frame.removeprefix("data: ").strip())
    assert payload["type"] == "dashboard_update"
    assert payload["data"]["recent_activity"] == []
    assert data.recent_activity == [_ACTIVITY]

    kept = _dashboard_sse_event(data, None, include_recent_activity=True)
    kept_payload = json.loads(kept.removeprefix("data: ").strip())
    assert kept_payload["data"]["recent_activity"][0]["id"] == _ACTIVITY.id


async def test_snapshot_keeps_activity_for_admin(api_client, monkeypatch) -> None:
    monkeypatch.setattr("apps.routers.dashboard.dashboard_service.get_snapshot", AsyncMock(return_value=_snapshot()))

    response = await api_client.get("/api/v1/dashboard/snapshot")
    assert response.status_code == 200
    items = response.json()["recent_activity"]
    assert len(items) == 1
    assert items[0]["id"] == _ACTIVITY.id


async def test_snapshot_empties_activity_for_viewer(api_client, monkeypatch) -> None:
    app.dependency_overrides[get_current_active_user] = lambda: _user(UserRole.VIEWER)
    monkeypatch.setattr("apps.routers.dashboard.dashboard_service.get_snapshot", AsyncMock(return_value=_snapshot()))

    response = await api_client.get("/api/v1/dashboard/snapshot")
    assert response.status_code == 200
    body = response.json()
    assert body["recent_activity"] == []
    assert "graph" in body
    assert "pipelines_stats" in body
