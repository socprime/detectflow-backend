"""Dashboard API endpoints.

This module provides SSE and REST endpoints for the real-time dashboard,
including graph data, pipeline statistics, and recent events.
"""

import asyncio
import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from apps.core.auth import WRITE_ROLES, get_current_active_user, get_current_active_user_from_cookie
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED
from apps.core.schemas import ActivityItem, DashboardData, DashboardSnapshotResponse
from apps.core.settings import settings
from apps.managers.dashboard import dashboard_service

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/dashboard", tags=["Dashboard"])


def _recent_activity_for_user(user: User, items: list[ActivityItem]) -> list[ActivityItem]:
    """Audit-log items are write-gated (same as ``GET /audit-logs``); viewers get an empty list."""
    if user.role in WRITE_ROLES:
        return items
    return []


def _dashboard_sse_event(
    data: DashboardData | None,
    error: Exception | None,
    *,
    include_recent_activity: bool,
) -> str:
    """Serialize the shared cached payload into one SSE ``data:`` frame.

    A failed tick (``error`` set by the broadcaster) yields a ``type: "error"`` envelope;
    otherwise the cached ``DashboardData`` is serialized under ``type: "dashboard_update"``.
    ``recent_activity`` is copied out and emptied for viewers so the shared cache is not mutated.
    """
    if error is not None or data is None:
        event = {
            "type": "error",
            "error": str(error) if error is not None else "no data available",
            "timestamp": datetime.now(UTC).isoformat(),
        }
    else:
        payload = data.model_dump(mode="json")
        if not include_recent_activity:
            payload["recent_activity"] = []
        event = {
            "type": "dashboard_update",
            "data": payload,
            "timestamp": datetime.now(UTC).isoformat(),
        }
    return f"data: {json.dumps(event)}\n\n"


async def _sse_event_generator(request: Request, *, include_recent_activity: bool):
    """Generate SSE events for dashboard updates.

    Subscribes to the shared :class:`DashboardService` broadcaster (which computes the payload
    once per tick for all clients) and serializes the cached snapshot every
    ``dashboard_broadcast_interval_seconds``. Stops when the client disconnects.
    """
    interval = settings.dashboard_broadcast_interval_seconds

    await dashboard_service.broadcaster.subscribe()
    try:
        # Send the current cached snapshot immediately (computed on first subscribe).
        data, error = dashboard_service.broadcaster.get_cached()
        yield _dashboard_sse_event(data, error, include_recent_activity=include_recent_activity)

        while True:
            if await request.is_disconnected():
                logger.debug("SSE client disconnected")
                break

            try:
                await asyncio.sleep(interval)

                if await request.is_disconnected():
                    break

                data, error = dashboard_service.broadcaster.get_cached()
                yield _dashboard_sse_event(data, error, include_recent_activity=include_recent_activity)
            except asyncio.CancelledError:
                break
    finally:
        await dashboard_service.broadcaster.unsubscribe()


@router.get(
    "/stream",
    summary="Stream dashboard updates (SSE)",
    responses={
        200: {
            "description": "Server-Sent Events stream of `DashboardSSEMessage` envelopes.",
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                    "example": (
                        'data: {"type": "dashboard_update", "data": {"graph": {}, "pipelines_stats": []}, '
                        '"timestamp": "2024-01-01T00:00:00Z"}\n\n'
                    ),
                }
            },
        },
        **UNAUTHORIZED,
    },
)
async def stream_dashboard(
    request: Request,
    current_user: User = Depends(get_current_active_user_from_cookie),
) -> StreamingResponse:
    """Stream real-time dashboard updates as Server-Sent Events.

    Emits one event immediately, then a fresh snapshot every
    ``dashboard_broadcast_interval_seconds`` (~2s) until the client disconnects. Each event body is
    a JSON ``DashboardSSEMessage`` envelope (``type`` / ``data`` / ``timestamp``, or ``type:
    "error"`` with an ``error`` field on a transient failure); ``data`` is a ``DashboardData``
    payload (pipeline metrics, throughput, system health). ``recent_activity`` is emptied for
    viewer accounts (same gate as ``GET /audit-logs``). Read-only — no Kafka publish or Flink
    side effect; failures to build a tick are logged as throttled ``dashboard`` activity events.

    **Authentication:** the `CookieAuth` scheme — the HttpOnly `access_token` cookie set by login,
    not the `Authorization` header (`EventSource` cannot send custom headers).
    """
    logger.info("SSE client connected")

    return StreamingResponse(
        _sse_event_generator(request, include_recent_activity=current_user.role in WRITE_ROLES),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get(
    "/snapshot",
    response_model=DashboardSnapshotResponse,
    summary="Get dashboard snapshot",
    responses=UNAUTHORIZED,
)
async def get_dashboard_snapshot(
    current_user: User = Depends(get_current_active_user),
) -> DashboardSnapshotResponse:
    """Return the current dashboard state as a single point-in-time snapshot.

    Same payload the SSE stream broadcasts (pipeline metrics, throughput, system health, recent
    activity, Kafka consumer health), delivered as one read-only response. Use it for the initial
    page load before opening the SSE stream, as an SSE fallback, or for monitoring probes.
    ``recent_activity`` is emptied for viewer accounts (same gate as ``GET /audit-logs``).
    """
    snapshot = await dashboard_service.get_snapshot()
    snapshot.recent_activity = _recent_activity_for_user(current_user, snapshot.recent_activity)
    return snapshot
