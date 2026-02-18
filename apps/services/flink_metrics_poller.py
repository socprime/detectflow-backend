"""Flink metrics poller service.

This module provides a background service that periodically polls Flink REST API
to retrieve pipeline metrics (input/output EPS, consumer lag) for the dashboard.

Replaces the Kafka metrics consumer approach with direct REST API polling.
Also updates pipeline events_tagged/events_untagged counters in PostgreSQL.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from apps.clients.flink_rest import FlinkJobMetrics, FlinkMetricsService
from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.models import Pipeline
from apps.core.settings import settings

logger = get_logger(__name__)


@dataclass
class PipelineMetricsCache:
    """Cached metrics for a single pipeline.

    Stores both raw Flink metrics and calculated EPS rates for tagged/untagged events.
    EPS rates are calculated from the delta of custom gauges between polls.
    """

    pipeline_id: UUID
    metrics: FlinkJobMetrics | None
    last_updated: datetime
    error: str | None = None
    # Calculated EPS from custom gauges (accurate, not based on IOMetrics)
    tagged_eps: float = 0.0  # Rate of matched events per second
    untagged_eps: float = 0.0  # Rate of unmatched events per second
    # Previous counter values for rate calculation
    _prev_matched_events: int = 0
    _prev_total_events: int = 0
    _prev_timestamp: datetime | None = None


class FlinkMetricsPoller:
    """Background service that polls Flink REST API for pipeline metrics.

    Maintains an in-memory cache of metrics for each active pipeline.
    Dashboard service reads from this cache instead of Kafka consumer.

    Attributes:
        is_running: Whether the poller is actively running
        poll_interval: Seconds between polling cycles
        metrics_cache: Dict mapping pipeline_id to cached metrics
        last_poll_time: Timestamp of last successful poll cycle
    """

    def __init__(self, poll_interval: float | None = None):
        """Initialize the metrics poller.

        Args:
            poll_interval: Seconds between polling cycles (defaults to settings)
        """
        self.poll_interval = poll_interval or settings.flink_metrics_poll_interval
        self.is_running = False
        self._poll_task: asyncio.Task | None = None
        self._flink_service = FlinkMetricsService()

        # Cache: pipeline_id -> PipelineMetricsCache
        self.metrics_cache: dict[UUID, PipelineMetricsCache] = {}
        self.last_poll_time: datetime | None = None

    async def start(self) -> None:
        """Start the background polling task."""
        if self.is_running:
            logger.warning("FlinkMetricsPoller is already running")
            return

        logger.info(
            "Starting FlinkMetricsPoller",
            extra={"poll_interval": self.poll_interval},
        )

        self.is_running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        logger.info("FlinkMetricsPoller started successfully")

    async def stop(self) -> None:
        """Stop the background polling task."""
        if not self.is_running:
            return

        logger.info("Stopping FlinkMetricsPoller")
        self.is_running = False

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        logger.info("FlinkMetricsPoller stopped")

    async def _poll_loop(self) -> None:
        """Main polling loop - runs until stopped."""
        logger.info("Starting metrics polling loop")

        while self.is_running:
            try:
                await self._poll_all_pipelines()
                self.last_poll_time = datetime.now(UTC)
            except asyncio.CancelledError:
                logger.info("Polling loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")

            # Wait before next poll
            await asyncio.sleep(self.poll_interval)

    async def _poll_all_pipelines(self) -> None:
        """Poll metrics for all enabled pipelines."""
        try:
            # Get all enabled pipelines from database
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Pipeline).where(Pipeline.enabled == True)  # noqa: E712
                )
                pipelines = result.scalars().all()

            if not pipelines:
                logger.debug("No enabled pipelines to poll")
                return

            # Poll each pipeline concurrently
            tasks = [
                self._poll_pipeline(
                    pipeline_id=pipeline.id,
                    deployment_name=pipeline.deployment_name,
                )
                for pipeline in pipelines
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            logger.debug(
                "Polled metrics for pipelines",
                extra={"count": len(pipelines)},
            )

        except Exception as e:
            logger.error(f"Failed to poll pipelines: {e}")

    async def _poll_pipeline(self, pipeline_id: UUID, deployment_name: str | None) -> None:
        """Poll metrics for a single pipeline.

        NOTE: Pipeline totals (events_tagged/events_untagged) are updated by
        MetricsConsumerService via Kafka topic. This poller only caches real-time
        metrics (EPS, state, lag) for dashboard display.

        Calculates accurate tagged_eps/untagged_eps from custom Flink gauges.

        Args:
            pipeline_id: Pipeline UUID
            deployment_name: FlinkDeployment name (optional)
        """
        now = datetime.now(UTC)
        prev_cache = self.metrics_cache.get(pipeline_id)

        try:
            metrics = await self._flink_service.get_pipeline_metrics(
                pipeline_id=str(pipeline_id),
                deployment_name=deployment_name,
            )

            # Calculate EPS from custom gauges (delta / time)
            tagged_eps = 0.0
            untagged_eps = 0.0

            if metrics and prev_cache and prev_cache._prev_timestamp:
                time_delta = (now - prev_cache._prev_timestamp).total_seconds()
                if time_delta > 0:
                    # Delta of matched events
                    matched_delta = metrics.matched_events - prev_cache._prev_matched_events
                    total_delta = metrics.total_events - prev_cache._prev_total_events

                    # Handle counter resets (job restart)
                    if matched_delta >= 0 and total_delta >= 0:
                        tagged_eps = matched_delta / time_delta
                        untagged_delta = total_delta - matched_delta
                        untagged_eps = max(0, untagged_delta / time_delta)

            # Update cache with metrics and calculated EPS
            self.metrics_cache[pipeline_id] = PipelineMetricsCache(
                pipeline_id=pipeline_id,
                metrics=metrics,
                last_updated=now,
                error=None if metrics else "No running job found",
                tagged_eps=tagged_eps,
                untagged_eps=untagged_eps,
                _prev_matched_events=metrics.matched_events if metrics else 0,
                _prev_total_events=metrics.total_events if metrics else 0,
                _prev_timestamp=now,
            )

        except Exception as e:
            logger.debug(f"Failed to poll metrics for pipeline {pipeline_id}: {e}")
            # Update cache with error but preserve previous counter values for next poll
            self.metrics_cache[pipeline_id] = PipelineMetricsCache(
                pipeline_id=pipeline_id,
                metrics=None,
                last_updated=now,
                error=str(e),
                tagged_eps=0.0,
                untagged_eps=0.0,
                _prev_matched_events=prev_cache._prev_matched_events if prev_cache else 0,
                _prev_total_events=prev_cache._prev_total_events if prev_cache else 0,
                _prev_timestamp=prev_cache._prev_timestamp if prev_cache else None,
            )

    def get_metrics(self, pipeline_id: UUID) -> FlinkJobMetrics | None:
        """Get cached metrics for a pipeline.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            FlinkJobMetrics or None if not cached or errored
        """
        cache_entry = self.metrics_cache.get(pipeline_id)
        if cache_entry and cache_entry.metrics:
            return cache_entry.metrics
        return None

    def get_cache_entry(self, pipeline_id: UUID) -> PipelineMetricsCache | None:
        """Get full cache entry including calculated EPS.

        Use this when you need tagged_eps/untagged_eps calculated from custom gauges.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            PipelineMetricsCache or None if not cached
        """
        return self.metrics_cache.get(pipeline_id)

    def get_all_metrics(self) -> dict[UUID, FlinkJobMetrics]:
        """Get all cached metrics.

        Returns:
            Dict mapping pipeline_id to FlinkJobMetrics (only successful entries)
        """
        return {pid: entry.metrics for pid, entry in self.metrics_cache.items() if entry.metrics is not None}

    @property
    def is_healthy(self) -> bool:
        """Check if poller is healthy (running and polled recently)."""
        if not self.is_running:
            return False

        # Consider healthy if polled in last 30 seconds
        if self.last_poll_time:
            age = (datetime.now(UTC) - self.last_poll_time).total_seconds()
            return age < 30

        # No poll yet - still healthy if just started
        return True

    def clear_cache(self, pipeline_id: UUID | None = None) -> None:
        """Clear the metrics cache.

        Args:
            pipeline_id: Optional specific pipeline to clear. If None, clears all.
        """
        if pipeline_id:
            self.metrics_cache.pop(pipeline_id, None)
        else:
            self.metrics_cache.clear()


# Singleton instance
flink_metrics_poller = FlinkMetricsPoller()
