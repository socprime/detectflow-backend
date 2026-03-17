"""Flink metrics poller service.

This module provides a background service that periodically polls Flink REST API
to retrieve pipeline metrics (input/output EPS, consumer lag) for the dashboard.

Replaces the Kafka metrics consumer approach with direct REST API polling.
Also updates pipeline events_tagged/events_untagged counters in PostgreSQL.
"""

import asyncio
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from apps.clients.flink_rest import FlinkJobMetrics, FlinkMetricsService
from apps.core.database import AsyncSessionLocal
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import Pipeline
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer

logger = get_logger(__name__)


@dataclass
class PipelineMetricsCache:
    """Cached metrics for a single pipeline.

    EPS values are read DIRECTLY from Flink custom gauges:
    - inputEventsPerSecond: input events from source
    - outputTaggedPerSecond: tagged events written to output topic
    - outputUntaggedPerSecond: untagged events written to output (0 if matched_only)

    This provides accurate real-time rates without delta calculation artifacts
    that caused spiky dashboard behavior (0 -> spike -> 0 pattern).
    """

    pipeline_id: UUID
    metrics: FlinkJobMetrics | None
    last_updated: datetime
    error: str | None = None
    # Rate gauges from Flink custom metrics
    input_eps: float = 0.0  # Input events/sec from source
    tagged_eps: float = 0.0  # Tagged events/sec written to output
    untagged_eps: float = 0.0  # Untagged events/sec (0 if matched_only mode)
    # Timestamp of last processed window (for stale detection)
    last_window_timestamp_ms: int = 0
    # Window size for stale detection (per-pipeline, from Pipeline.window_size_sec)
    window_size_sec: int = 30


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
                if ErrorTracker.should_log("poller_loop"):
                    await activity_producer.log_action(
                        action="error",
                        entity_type="poller",
                        details=f"Error in Flink metrics polling loop: {str(e)}",
                        source="system",
                        severity="error",
                    )

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
                    window_size_sec=pipeline.window_size_sec or 30,
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
            if ErrorTracker.should_log("poller_poll_pipelines"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="poller",
                    details=f"Failed to poll pipelines: {str(e)}",
                    source="system",
                    severity="error",
                )

    async def _poll_pipeline(
        self,
        pipeline_id: UUID,
        deployment_name: str | None,
        window_size_sec: int = 30,
    ) -> None:
        """Poll metrics for a single pipeline.

        NOTE: Pipeline totals (events_tagged/events_untagged) are updated by
        MetricsConsumerService via Kafka topic. This poller only caches real-time
        metrics (EPS, state, lag) for dashboard display.

        Uses rate gauges from Flink custom metrics (inputEventsPerSecond,
        outputTaggedPerSecond, outputUntaggedPerSecond) which are calculated
        at the end of each processing window.

        Args:
            pipeline_id: Pipeline UUID
            deployment_name: FlinkDeployment name (optional)
            window_size_sec: Window size in seconds for stale detection
        """
        now = datetime.now(UTC)

        try:
            metrics = await self._flink_service.get_pipeline_metrics(
                pipeline_id=str(pipeline_id),
                deployment_name=deployment_name,
            )

            # Read rate gauges directly from Flink custom metrics
            input_eps = 0.0
            tagged_eps = 0.0
            untagged_eps = 0.0
            last_window_ts = 0

            if metrics:
                input_eps = metrics.input_events_per_second
                tagged_eps = metrics.output_tagged_per_second
                untagged_eps = metrics.output_untagged_per_second  # Will be 0 if matched_only mode
                last_window_ts = metrics.last_window_timestamp_ms

            # Update cache with metrics and rate gauge values
            self.metrics_cache[pipeline_id] = PipelineMetricsCache(
                pipeline_id=pipeline_id,
                metrics=metrics,
                last_updated=now,
                error=None if metrics else "No running job found",
                input_eps=input_eps,
                tagged_eps=tagged_eps,
                untagged_eps=untagged_eps,
                last_window_timestamp_ms=last_window_ts,
                window_size_sec=window_size_sec,
            )

        except Exception as e:
            logger.debug(f"Failed to poll metrics for pipeline {pipeline_id}: {e}")
            # Update cache with error
            self.metrics_cache[pipeline_id] = PipelineMetricsCache(
                pipeline_id=pipeline_id,
                metrics=None,
                last_updated=now,
                error=str(e),
                input_eps=0.0,
                tagged_eps=0.0,
                untagged_eps=0.0,
                last_window_timestamp_ms=0,
                window_size_sec=window_size_sec,
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
        """Get full cache entry with stale detection.

        If metrics are stale (last window > 3x poll_interval ago),
        returns entry with zeroed EPS values.

        Use this when you need tagged_eps/untagged_eps calculated from custom gauges.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            PipelineMetricsCache or None if not cached
        """
        entry = self.metrics_cache.get(pipeline_id)
        if not entry:
            return None

        # Stale detection: if last window timestamp is too old, zero out EPS
        # Use max of (3x poll_interval, 2x window_size) to account for both scenarios:
        # - Fast polling with slow windows (e.g., 5s poll, 30s window -> 60s threshold)
        # - Slow polling with fast windows (e.g., 30s poll, 5s window -> 90s threshold)
        # Note: last_window_timestamp_ms == 0 means no window has completed yet (job just started
        # or no events received). In this case we trust the current EPS values (likely 0.0).
        if entry.last_window_timestamp_ms > 0:
            age_seconds = (time.time() * 1000 - entry.last_window_timestamp_ms) / 1000
            stale_threshold = max(
                self.poll_interval * 3,
                entry.window_size_sec * 2,  # Per-pipeline window size
            )

            if age_seconds > stale_threshold:
                # Return copy with zeroed EPS
                return replace(
                    entry,
                    input_eps=0.0,
                    tagged_eps=0.0,
                    untagged_eps=0.0,
                )

        return entry

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
