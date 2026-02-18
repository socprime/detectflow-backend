"""Dashboard API endpoints.

This module provides SSE and REST endpoints for the real-time dashboard,
including graph data, pipeline statistics, and recent events.
"""

import asyncio
import json
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse

from apps.core.auth import get_current_active_user, get_current_active_user_from_cookie
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import DashboardSnapshotResponse, ErrorResponse
from apps.core.settings import settings
from apps.managers.dashboard import dashboard_service

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/dashboard", tags=["Dashboard"])


async def _sse_event_generator(request: Request):
    """Generate SSE events for dashboard updates.

    Yields dashboard data every N seconds (configurable).
    Stops when client disconnects.
    """
    interval = settings.dashboard_broadcast_interval_seconds

    # Send initial data immediately
    try:
        data = await dashboard_service.get_dashboard_data()
        event_data = {
            "type": "dashboard_update",
            "data": data.model_dump(mode="json"),
            "timestamp": datetime.now(UTC).isoformat(),
        }
        yield f"data: {json.dumps(event_data)}\n\n"
    except Exception as e:
        logger.error("Failed to send initial SSE data", extra={"error": str(e)})

    # Stream updates
    while True:
        # Check if client disconnected
        if await request.is_disconnected():
            logger.debug("SSE client disconnected")
            break

        try:
            await asyncio.sleep(interval)

            # Check again after sleep
            if await request.is_disconnected():
                break

            data = await dashboard_service.get_dashboard_data()
            event_data = {
                "type": "dashboard_update",
                "data": data.model_dump(mode="json"),
                "timestamp": datetime.now(UTC).isoformat(),
            }
            yield f"data: {json.dumps(event_data)}\n\n"

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error generating SSE event", extra={"error": str(e)})
            # Send error event
            error_event = {
                "type": "error",
                "error": str(e),
                "timestamp": datetime.now(UTC).isoformat(),
            }
            yield f"data: {json.dumps(error_event)}\n\n"


@router.get(
    "/stream",
    summary="Stream dashboard updates (SSE)",
    responses={
        200: {"description": "Server-Sent Events stream with dashboard updates", "content": {"text/event-stream": {}}},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def stream_dashboard(
    request: Request,
    _: User = Depends(get_current_active_user_from_cookie),
) -> StreamingResponse:
    """
    Real-time dashboard updates via Server-Sent Events.

    Provides continuous stream of dashboard data including pipeline metrics,
    throughput statistics, and system health. Updates every ~2 seconds.

    **Authentication:** Uses HttpOnly cookie `access_token` (set on login).
    This is required because `EventSource` does not support custom headers.

    **Event Format:**
    ```json
    {"type": "dashboard_update", "data": {...}, "timestamp": "2024-01-01T00:00:00Z"}
    ```

    **JavaScript Example:**
    ```javascript
    // Cookie is sent automatically after login
    const eventSource = new EventSource('/api/v1/dashboard/stream', {
        withCredentials: true
    });
    eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        updateDashboard(data);
    };
    ```
    """
    logger.info("SSE client connected")

    return StreamingResponse(
        _sse_event_generator(request),
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
    responses={
        200: {"description": "Current dashboard state"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_dashboard_snapshot(
    _: User = Depends(get_current_active_user),
) -> DashboardSnapshotResponse:
    """
    Get current dashboard state as a single snapshot.

    Returns the same data as the SSE stream but as a one-time response.

    **Use cases:**
    - Initial page load before SSE connection
    - Fallback when SSE is unavailable
    - Monitoring and health checks
    """
    return await dashboard_service.get_snapshot()
