"""Platform-wide correlation graph dashboard: one SSEBroadcaster, graph SQL only.

One compute per tick for every client. The payload is the five unscoped graph blocks
(:meth:`AttackChainsManager.get_graph_data`) — no Kubernetes, no per-pipeline slice, no
subscriber cap. The refresher runs only while at least one SSE client is connected.
"""

from apps.core.database import AsyncSessionLocal
from apps.core.error_tracker import ErrorTracker
from apps.core.schemas import CorrelationJobGraphData
from apps.core.settings import settings
from apps.managers.attack_chains import AttackChainsManager
from apps.managers.sse_broadcaster import SSEBroadcaster
from apps.modules.kafka.activity import activity_producer


async def compute_correlation_dashboard() -> CorrelationJobGraphData:
    """One tick: the five platform-wide graph blocks, then drop the session."""
    async with AsyncSessionLocal() as db:
        return await AttackChainsManager(db).get_graph_data()


async def _log_correlation_dashboard_error(exc: Exception) -> None:
    if ErrorTracker.should_log("correlation_pipelines_dashboard_sse_event"):
        await activity_producer.log_action(
            action="error",
            entity_type="correlation_pipeline",
            details=f"Error generating correlation dashboard SSE: {str(exc)}",
            source="system",
            severity="error",
        )


correlation_dashboard_broadcaster: SSEBroadcaster[CorrelationJobGraphData] = SSEBroadcaster(
    name="correlation_pipelines_dashboard",
    compute=compute_correlation_dashboard,
    interval=settings.dashboard_broadcast_interval_seconds,
    on_error=_log_correlation_dashboard_error,
)


async def get_correlation_dashboard_snapshot() -> CorrelationJobGraphData:
    """REST snapshot: reuse a live SSE tick when one exists, otherwise compute once."""
    if correlation_dashboard_broadcaster.is_running:
        cached, error = correlation_dashboard_broadcaster.get_cached()
        if error is None and cached is not None:
            return cached
    return await compute_correlation_dashboard()
