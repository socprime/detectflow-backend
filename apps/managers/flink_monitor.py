"""Flink Monitor Service for tracking pipeline health and status changes.

This module provides a background service that monitors Flink deployments
for status changes, exceptions (OOM, etc.), and other health indicators.
Events are published to the activity log via Kafka.

Architecture:
- Runs as a background asyncio task
- Periodically checks all enabled pipelines
- Detects changes and emits activity events
- Clean separation from dashboard (read-only) concerns
"""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.flink_rest import FlinkRestClient
from apps.core.database import AsyncSessionLocal
from apps.core.enums import get_status_level
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import Pipeline
from apps.core.schemas import StatusDetails
from apps.managers.pipeline_status import pipeline_status_manager
from apps.modules.kafka.activity import activity_producer
from apps.providers import get_flink_provider

logger = get_logger(__name__)


@dataclass
class PipelineState:
    """Tracked state for a pipeline."""

    status: str
    last_exception: str | None = None
    exception_timestamp: int | None = None
    # Checkpoint tracking
    failed_checkpoints: int = 0
    # Consumer lag tracking
    pending_records: int = 0
    pending_records_growing_since: datetime | None = None
    # Backpressure tracking
    backpressure_level: str = "ok"  # ok, low, high
    # Resource tracking
    memory_usage_percent: float = 0.0
    memory_alert_sent: bool = False
    # Kafka consumer health tracking
    kafka_commits_failed: int = 0
    kafka_records_in_errors: int = 0
    kafka_consumer_stuck_alerted: bool = False
    kafka_rebalance_alerted: bool = False
    last_check: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FlinkException:
    """Flink job exception from REST API."""

    root_exception: str
    timestamp: int
    task_name: str | None = None


def _normalize_status_for_comparison(status: str) -> str:
    """Normalize status string for comparison.

    Status values are now clean strings like 'running', 'failed', 'reconciling'.
    Error details are stored separately in status_details.error field.

    Args:
        status: Status string from Flink/K8s

    Returns:
        Status as-is (already normalized)
    """
    return status


class FlinkMonitorService:
    """Background service for monitoring Flink pipeline health.

    Monitors:
    - Pipeline status changes (created → running, running → failed, etc.)
    - Flink job exceptions (OOM, TaskManager failures, etc.)
    - Future: checkpoint failures, backpressure, etc.

    Usage:
        monitor = FlinkMonitorService()
        await monitor.start()
        # ... later ...
        await monitor.stop()
    """

    # Check interval in seconds
    CHECK_INTERVAL_SECONDS = 10

    # Flink REST API timeout
    FLINK_TIMEOUT_SECONDS = 5.0

    # Lag growth threshold - alert if lag grows continuously for this duration
    LAG_GROWTH_ALERT_SECONDS = 60

    # Lag spike threshold - alert if lag increases by this factor
    LAG_SPIKE_THRESHOLD = 2.0  # 2x increase

    # Memory thresholds
    MEMORY_WARNING_PERCENT = 80  # Warning at 80%
    MEMORY_CRITICAL_PERCENT = 90  # Critical at 90%

    # Kafka consumer health thresholds
    KAFKA_POLL_STUCK_SECONDS = 60  # Consumer stuck if no poll for 60s
    KAFKA_REBALANCE_RATE_THRESHOLD = 5  # Alert if > 5 rebalances per hour

    def __init__(self):
        """Initialize the monitor service."""
        self._flink = get_flink_provider()

        # Track pipeline states
        # Key: pipeline_id (str), Value: PipelineState
        self._states: dict[str, PipelineState] = {}

        # Background task handle
        self._task: asyncio.Task | None = None
        self._is_running: bool = False

    async def start(self) -> None:
        """Start the background monitoring task."""
        if self._is_running:
            return

        self._is_running = True
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(
            "FlinkMonitorService started",
            extra={"check_interval": self.CHECK_INTERVAL_SECONDS},
        )

    async def stop(self) -> None:
        """Stop the background monitoring task."""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("FlinkMonitorService stopped")

    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._is_running:
            try:
                await self._check_all_pipelines()
            except Exception as e:
                logger.exception("Error in monitor loop", extra={"error": str(e)})
                if ErrorTracker.should_log("monitor_loop"):
                    await activity_producer.log_action(
                        action="error",
                        entity_type="monitor",
                        details=f"Error in Flink monitor loop: {str(e)}",
                        source="system",
                        severity="error",
                    )

            await asyncio.sleep(self.CHECK_INTERVAL_SECONDS)

    async def _check_all_pipelines(self) -> None:
        """Check all enabled pipelines for status changes and exceptions."""
        try:
            # Get pipelines and close session immediately to avoid holding
            # connection during long HTTP calls to Flink REST API
            async with AsyncSessionLocal() as db:
                pipelines = await self._get_enabled_pipelines(db)

            # Process pipelines without holding DB session
            # Each pipeline check involves multiple HTTP calls (5s timeout each)
            # Status updates create their own short-lived sessions when needed
            for pipeline in pipelines:
                await self._check_pipeline(pipeline)

        except Exception as e:
            logger.error("Failed to check pipelines", extra={"error": str(e)})
            if ErrorTracker.should_log("monitor_check_pipelines"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="monitor",
                    details=f"Failed to check pipelines in monitor: {str(e)}",
                    source="system",
                    severity="error",
                )

    async def _get_enabled_pipelines(self, db: AsyncSession) -> list[Pipeline]:
        """Get all enabled pipelines from database.

        Args:
            db: Database session

        Returns:
            List of enabled Pipeline objects
        """
        result = await db.execute(select(Pipeline).where(Pipeline.enabled.is_(True)))
        return list(result.scalars().all())

    async def _check_pipeline(self, pipeline: Pipeline) -> None:
        """Check a single pipeline for status changes and exceptions.

        Args:
            pipeline: Pipeline to check
        """
        pipeline_id = str(pipeline.id)

        # Get current status with full details from Flink/K8s
        status_str, status_details = await pipeline_status_manager.get_status_details(
            pipeline_id=pipeline_id,
            deployment_name=pipeline.deployment_name,
            enabled=pipeline.enabled,
        )

        # Check for status change (creates short-lived session only when update needed)
        await self._check_status_change(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline.name,
            new_status=status_str,
            status_details=status_details,
        )

        # Additional checks only for running jobs with deployment
        if status_str == "running" and pipeline.deployment_name:
            # Check for exceptions (OOM, etc.)
            await self._check_exceptions(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

            # Check for checkpoint failures
            await self._check_checkpoints(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

            # Check for consumer lag growth
            await self._check_lag(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

            # Check for backpressure
            await self._check_backpressure(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

            # Check for resource usage (memory/CPU)
            await self._check_resources(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

            # Check Kafka consumer health
            await self._check_kafka_health(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline.name,
            )

    async def _check_status_change(
        self,
        pipeline_id: str,
        pipeline_name: str,
        new_status: str,
        status_details: StatusDetails | None = None,
    ) -> None:
        """Check if pipeline status changed and emit activity event.

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name for display
            new_status: New status from Flink/K8s
            status_details: Optional status details with warnings and error info
        """
        previous_state = self._states.get(pipeline_id)

        # First check - just store state, don't emit event
        if previous_state is None:
            self._states[pipeline_id] = PipelineState(status=new_status)
            return

        # Normalize statuses for comparison (reduces noise from backoff time changes, etc.)
        normalized_old = _normalize_status_for_comparison(previous_state.status)
        normalized_new = _normalize_status_for_comparison(new_status)

        # Status unchanged (after normalization)
        if normalized_old == normalized_new:
            # Update raw status silently (for accurate display) but don't emit event
            self._states[pipeline_id].status = new_status
            self._states[pipeline_id].last_check = datetime.now(UTC)
            return

        # Status changed - emit activity event
        old_status = previous_state.status
        self._states[pipeline_id].status = new_status
        self._states[pipeline_id].last_check = datetime.now(UTC)

        await self._emit_status_change(
            pipeline_id=pipeline_id,
            pipeline_name=pipeline_name,
            old_status=old_status,
            new_status=new_status,
            status_details=status_details,
        )

    async def _emit_status_change(
        self,
        pipeline_id: str,
        pipeline_name: str,
        old_status: str,
        new_status: str,
        status_details: StatusDetails | None = None,
    ) -> None:
        """Emit activity event for status change and update DB.

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
            old_status: Previous status
            new_status: New status
            status_details: Optional status details with warnings and error info
        """
        # Update status in database first - if this fails, don't emit activity event
        db_updated = await self._update_pipeline_status(pipeline_id, new_status)
        if not db_updated:
            logger.warning(
                "Skipping activity event due to DB update failure",
                extra={"pipeline_id": pipeline_id, "new_status": new_status},
            )
            return

        # Determine severity based on new status
        severity = get_status_level(new_status)

        # Build changes dict with status and optional warnings/error
        changes: dict = {"status": {"old": old_status, "new": new_status}}
        if status_details:
            if status_details.warnings:
                changes["warnings"] = status_details.warnings
            if status_details.error:
                changes["error"] = status_details.error

        try:
            await activity_producer.log_action(
                action="status_change",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,  # System event
                details=f"Pipeline status changed: {old_status} → {new_status}",
                changes=changes,
                source="system",
                severity=severity,
            )

            logger.info(
                "Pipeline status change logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "pipeline_name": pipeline_name,
                    "old_status": old_status,
                    "new_status": new_status,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit status change event",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _update_pipeline_status(self, pipeline_id: str, status: str) -> bool:
        """Update pipeline status in database.

        Creates a short-lived session to avoid holding connections during HTTP calls.

        Args:
            pipeline_id: Pipeline UUID string
            status: New status value

        Returns:
            True if update succeeded, False otherwise
        """
        try:
            async with AsyncSessionLocal() as db:
                await db.execute(update(Pipeline).where(Pipeline.id == UUID(pipeline_id)).values(status=status))
                await db.commit()
                logger.debug(
                    "Pipeline status updated in DB",
                    extra={"pipeline_id": pipeline_id, "status": status},
                )
            return True
        except Exception as e:
            logger.error(
                "Failed to update pipeline status in DB",
                extra={"pipeline_id": pipeline_id, "status": status, "error": str(e)},
            )
            if ErrorTracker.should_log(f"monitor_db_update_{pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="database",
                    entity_id=pipeline_id,
                    details=f"Failed to update pipeline status in DB: {str(e)}",
                    source="system",
                    severity="error",
                )
            return False

    async def _check_exceptions(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check for Flink job exceptions (OOM, etc.).

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            exception = await self._get_latest_exception(pipeline_id)
            if exception is None:
                return

            previous_state = self._states.get(pipeline_id)
            if previous_state is None:
                return

            # Check if this is a new exception (different timestamp)
            if (
                previous_state.exception_timestamp is not None
                and previous_state.exception_timestamp >= exception.timestamp
            ):
                return

            # New exception detected
            self._states[pipeline_id].last_exception = exception.root_exception
            self._states[pipeline_id].exception_timestamp = exception.timestamp

            await self._emit_exception(
                pipeline_id=pipeline_id,
                pipeline_name=pipeline_name,
                exception=exception,
            )

        except Exception as e:
            logger.debug(
                "Failed to check exceptions",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _get_latest_exception(self, pipeline_id: str) -> FlinkException | None:
        """Get latest exception from Flink REST API.

        Args:
            pipeline_id: Pipeline UUID string

        Returns:
            FlinkException or None if no exceptions
        """
        rest_url = self._flink.get_rest_url(pipeline_id)
        client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

        try:
            # Get running job ID
            job_id = await client.get_running_job_id()
            if job_id is None:
                return None

            # GET /jobs/{job_id}/exceptions

            async with httpx.AsyncClient(timeout=self.FLINK_TIMEOUT_SECONDS) as http:
                response = await http.get(f"{rest_url}/jobs/{job_id}/exceptions")
                if response.status_code != 200:
                    return None

                data = response.json()

                # Parse root exception
                root_exception = data.get("root-exception")
                timestamp = data.get("timestamp", 0)

                if not root_exception:
                    return None

                # Get task name from first exception entry
                task_name = None
                exceptions = data.get("all-exceptions", [])
                if exceptions:
                    task_name = exceptions[0].get("taskName")

                return FlinkException(
                    root_exception=root_exception,
                    timestamp=timestamp,
                    task_name=task_name,
                )

        except Exception as e:
            logger.debug(f"Failed to get exceptions for pipeline {pipeline_id}: {e}")
            return None

    async def _emit_exception(
        self,
        pipeline_id: str,
        pipeline_name: str,
        exception: FlinkException,
    ) -> None:
        """Emit activity event for Flink exception.

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
            exception: FlinkException with details
        """
        try:
            # Truncate long exception messages
            error_msg = exception.root_exception
            if len(error_msg) > 500:
                error_msg = error_msg[:500] + "..."

            # Determine error type and user-friendly prefix from exception message
            if "OutOfMemoryError" in exception.root_exception:
                error_type = "oom"
                prefix = "Pipeline crashed: out of memory — increase TaskManager memory"
            elif "TaskManager" in exception.root_exception:
                error_type = "taskmanager_failure"
                prefix = "Pipeline worker crashed"
            elif "Checkpoint" in exception.root_exception:
                error_type = "checkpoint_failure"
                prefix = "Pipeline failed to save state"
            else:
                error_type = "exception"
                prefix = "Pipeline error"

            await activity_producer.log_action(
                action="error",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=f"{prefix}: {error_msg}",
                changes={
                    "error_type": error_type,
                    "task_name": exception.task_name,
                    "timestamp": exception.timestamp,
                },
                source="flink",
            )

            logger.warning(
                "Flink exception logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "pipeline_name": pipeline_name,
                    "error_type": error_type,
                    "task_name": exception.task_name,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit exception event",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _check_checkpoints(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check for checkpoint failures.

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            rest_url = self._flink.get_rest_url(pipeline_id)
            client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

            job_id = await client.get_running_job_id()
            if job_id is None:
                return

            async with httpx.AsyncClient(timeout=self.FLINK_TIMEOUT_SECONDS) as http:
                response = await http.get(f"{rest_url}/jobs/{job_id}/checkpoints")
                if response.status_code != 200:
                    return

                data = response.json()
                counts = data.get("counts", {})
                failed_count = counts.get("failed", 0)

                previous_state = self._states.get(pipeline_id)
                if previous_state is None:
                    return

                # Check if we have new checkpoint failures
                if failed_count > previous_state.failed_checkpoints:
                    new_failures = failed_count - previous_state.failed_checkpoints
                    self._states[pipeline_id].failed_checkpoints = failed_count

                    await self._emit_checkpoint_failure(
                        pipeline_id=pipeline_id,
                        pipeline_name=pipeline_name,
                        new_failures=new_failures,
                        total_failures=failed_count,
                    )

        except Exception as e:
            logger.debug(
                "Failed to check checkpoints",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _emit_checkpoint_failure(
        self,
        pipeline_id: str,
        pipeline_name: str,
        new_failures: int,
        total_failures: int,
    ) -> None:
        """Emit activity event for checkpoint failures."""
        try:
            await activity_producer.log_action(
                action="alert",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=f"Pipeline failed to save state ({new_failures} failures) — recovery after restart may take longer",
                changes={
                    "new_failures": new_failures,
                    "total_failures": total_failures,
                },
                source="flink",
            )

            logger.warning(
                "Checkpoint failure logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "new_failures": new_failures,
                    "total_failures": total_failures,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit checkpoint failure event",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _check_lag(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check for consumer lag growth.

        Alerts if:
        1. Lag has been growing continuously for LAG_GROWTH_ALERT_SECONDS
        2. Lag increased by more than LAG_SPIKE_THRESHOLD factor

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            rest_url = self._flink.get_rest_url(pipeline_id)
            client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

            pending_records = await client.get_pending_records()
            if pending_records is None:
                return

            previous_state = self._states.get(pipeline_id)
            if previous_state is None:
                return

            prev_pending = previous_state.pending_records
            now = datetime.now(UTC)

            # Update state
            self._states[pipeline_id].pending_records = pending_records

            # Check for spike (sudden large increase)
            if prev_pending > 0 and pending_records > prev_pending * self.LAG_SPIKE_THRESHOLD:
                await self._emit_lag_alert(
                    pipeline_id=pipeline_id,
                    pipeline_name=pipeline_name,
                    alert_type="spike",
                    current_lag=pending_records,
                    previous_lag=prev_pending,
                )
                self._states[pipeline_id].pending_records_growing_since = None
                return

            # Check for continuous growth
            if pending_records > prev_pending:
                # Lag is growing
                if previous_state.pending_records_growing_since is None:
                    self._states[pipeline_id].pending_records_growing_since = now
                else:
                    growth_duration = (now - previous_state.pending_records_growing_since).total_seconds()
                    if growth_duration >= self.LAG_GROWTH_ALERT_SECONDS:
                        await self._emit_lag_alert(
                            pipeline_id=pipeline_id,
                            pipeline_name=pipeline_name,
                            alert_type="continuous_growth",
                            current_lag=pending_records,
                            previous_lag=prev_pending,
                            growth_duration=int(growth_duration),
                        )
                        # Reset to avoid repeated alerts
                        self._states[pipeline_id].pending_records_growing_since = now
            else:
                # Lag is stable or decreasing - reset growth tracking
                self._states[pipeline_id].pending_records_growing_since = None

        except Exception as e:
            logger.debug(
                "Failed to check lag",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _emit_lag_alert(
        self,
        pipeline_id: str,
        pipeline_name: str,
        alert_type: str,
        current_lag: int,
        previous_lag: int,
        growth_duration: int | None = None,
    ) -> None:
        """Emit activity event for consumer lag issues."""
        try:
            if alert_type == "spike":
                details = f"Consumer lag spike: {previous_lag:,} → {current_lag:,} ({current_lag / previous_lag:.1f}x increase)"
            else:
                details = f"Consumer lag growing for {growth_duration}s: {current_lag:,} pending records"

            await activity_producer.log_action(
                action="alert",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=details,
                changes={
                    "alert_type": alert_type,
                    "current_lag": current_lag,
                    "previous_lag": previous_lag,
                    "growth_duration": growth_duration,
                },
                source="system",
            )

            logger.warning(
                "Consumer lag alert logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "alert_type": alert_type,
                    "current_lag": current_lag,
                    "previous_lag": previous_lag,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit lag alert",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _check_backpressure(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check for backpressure issues.

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            rest_url = self._flink.get_rest_url(pipeline_id)
            client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

            job_id = await client.get_running_job_id()
            if job_id is None:
                return

            # Get job details to find vertices
            job_details = await client.get_job_details(job_id)
            vertices = job_details.get("vertices", [])

            max_backpressure = "ok"
            backpressured_tasks = []

            async with httpx.AsyncClient(timeout=self.FLINK_TIMEOUT_SECONDS) as http:
                for vertex in vertices:
                    vertex_id = vertex.get("id")
                    vertex_name = vertex.get("name", "Unknown")

                    try:
                        response = await http.get(f"{rest_url}/jobs/{job_id}/vertices/{vertex_id}/backpressure")
                        if response.status_code != 200:
                            continue

                        data = response.json()
                        level = data.get("backpressureLevel", "ok").lower()

                        if level == "high":
                            max_backpressure = "high"
                            backpressured_tasks.append(vertex_name)
                        elif level == "low" and max_backpressure != "high":
                            max_backpressure = "low"

                    except Exception:
                        continue

            previous_state = self._states.get(pipeline_id)
            if previous_state is None:
                return

            # Alert only when backpressure increases to HIGH
            if max_backpressure == "high" and previous_state.backpressure_level != "high":
                await self._emit_backpressure_alert(
                    pipeline_id=pipeline_id,
                    pipeline_name=pipeline_name,
                    level=max_backpressure,
                    tasks=backpressured_tasks,
                )

            self._states[pipeline_id].backpressure_level = max_backpressure

        except Exception as e:
            logger.debug(
                "Failed to check backpressure",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    def _simplify_task_name(self, task_name: str) -> str:
        """Simplify Flink operator name for user-friendly display.

        Converts technical names like:
        'Source: KafkaEventsSource -> _stream_key_by_map_operator'
        to simpler:
        'Kafka Source'
        """
        name_lower = task_name.lower()

        # Identify component type
        if "source" in name_lower and "kafka" in name_lower:
            return "Kafka Source (reading events)"
        elif "source" in name_lower:
            return "Data Source"
        elif "sink" in name_lower and "kafka" in name_lower:
            return "Kafka Sink (writing results)"
        elif "sink" in name_lower:
            return "Data Sink"
        elif "sigma" in name_lower or "rule" in name_lower:
            return "Rule Processing"
        elif "filter" in name_lower:
            return "Event Filtering"
        elif "map" in name_lower or "transform" in name_lower:
            return "Event Transformation"
        elif "window" in name_lower:
            return "Window Aggregation"
        elif "key" in name_lower:
            return "Event Routing"

        # Fallback: take first meaningful part
        parts = task_name.split("->")[0].strip()
        if parts.startswith("Source:"):
            parts = parts[7:].strip()
        return parts[:40] if len(parts) > 40 else parts

    async def _emit_backpressure_alert(
        self,
        pipeline_id: str,
        pipeline_name: str,
        level: str,
        tasks: list[str],
    ) -> None:
        """Emit activity event for backpressure issues."""
        try:
            # Simplify task names for user-friendly display
            simplified_tasks = [self._simplify_task_name(t) for t in tasks[:3]]
            # Remove duplicates while preserving order
            seen = set()
            unique_tasks = [t for t in simplified_tasks if not (t in seen or seen.add(t))]
            bottleneck = ", ".join(unique_tasks)

            if len(tasks) > 3:
                bottleneck += f" (+{len(tasks) - 3} more)"

            # User-friendly message with actionable advice
            details = (
                f"Pipeline is processing slower than data arrives. "
                f"Bottleneck: {bottleneck}. "
                f"Consider increasing parallelism or resources."
            )

            await activity_producer.log_action(
                action="alert",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=details,
                changes={
                    "backpressure_level": level,
                    "affected_tasks": tasks,  # Keep original for debugging
                    "bottleneck": bottleneck,
                },
                source="flink",
            )

            logger.warning(
                "Backpressure alert logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "level": level,
                    "tasks": tasks,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit backpressure alert",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _check_resources(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check TaskManager resource usage (memory, CPU).

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            rest_url = self._flink.get_rest_url(pipeline_id)

            async with httpx.AsyncClient(timeout=self.FLINK_TIMEOUT_SECONDS) as http:
                response = await http.get(f"{rest_url}/taskmanagers")
                if response.status_code != 200:
                    return

                data = response.json()
                taskmanagers = data.get("taskmanagers", [])

                if not taskmanagers:
                    return

                # Aggregate metrics across all TaskManagers
                total_heap_used = 0
                total_heap_max = 0
                total_physical_memory = 0
                total_free_memory = 0

                for tm in taskmanagers:
                    # Get metrics from each TaskManager
                    metrics = tm.get("metrics", {})
                    hardware = tm.get("hardware", {})

                    heap_used = metrics.get("heapUsed", 0)
                    heap_max = metrics.get("heapMax", 0)
                    physical_memory = hardware.get("physicalMemory", 0)
                    free_memory = hardware.get("freeMemory", 0)

                    total_heap_used += heap_used
                    total_heap_max += heap_max
                    total_physical_memory += physical_memory
                    total_free_memory += free_memory

                # Calculate memory usage percentage
                if total_heap_max > 0:
                    heap_usage_percent = (total_heap_used / total_heap_max) * 100
                else:
                    heap_usage_percent = 0

                if total_physical_memory > 0:
                    physical_usage_percent = ((total_physical_memory - total_free_memory) / total_physical_memory) * 100
                else:
                    physical_usage_percent = 0

                # Use the higher of heap or physical memory usage
                memory_usage = max(heap_usage_percent, physical_usage_percent)

                previous_state = self._states.get(pipeline_id)
                if previous_state is None:
                    return

                # Determine alert level
                alert_level = None
                if memory_usage >= self.MEMORY_CRITICAL_PERCENT:
                    alert_level = "critical"
                elif memory_usage >= self.MEMORY_WARNING_PERCENT:
                    alert_level = "warning"

                # Send alert only if:
                # 1. Memory is above threshold
                # 2. We haven't already sent an alert for this level
                # 3. Memory has increased significantly since last check
                should_alert = alert_level is not None and (
                    not previous_state.memory_alert_sent
                    or memory_usage > previous_state.memory_usage_percent + 5  # 5% increase
                )

                if should_alert:
                    await self._emit_resource_alert(
                        pipeline_id=pipeline_id,
                        pipeline_name=pipeline_name,
                        alert_level=alert_level,
                        heap_used_mb=total_heap_used / (1024 * 1024),
                        heap_max_mb=total_heap_max / (1024 * 1024),
                        heap_usage_percent=heap_usage_percent,
                        physical_used_mb=(total_physical_memory - total_free_memory) / (1024 * 1024),
                        physical_total_mb=total_physical_memory / (1024 * 1024),
                        physical_usage_percent=physical_usage_percent,
                        taskmanager_count=len(taskmanagers),
                    )
                    self._states[pipeline_id].memory_alert_sent = True

                # Reset alert flag if memory dropped below warning
                if alert_level is None and previous_state.memory_alert_sent:
                    self._states[pipeline_id].memory_alert_sent = False

                self._states[pipeline_id].memory_usage_percent = memory_usage

        except Exception as e:
            logger.debug(
                "Failed to check resources",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _emit_resource_alert(
        self,
        pipeline_id: str,
        pipeline_name: str,
        alert_level: str,
        heap_used_mb: float,
        heap_max_mb: float,
        heap_usage_percent: float,
        physical_used_mb: float,
        physical_total_mb: float,
        physical_usage_percent: float,
        taskmanager_count: int,
    ) -> None:
        """Emit activity event for resource usage issues."""
        try:
            action = "error" if alert_level == "critical" else "warning"

            details = (
                f"Memory usage {alert_level}: "
                f"Heap {heap_usage_percent:.1f}% ({heap_used_mb:.0f}/{heap_max_mb:.0f} MB), "
                f"Physical {physical_usage_percent:.1f}% ({physical_used_mb:.0f}/{physical_total_mb:.0f} MB)"
            )

            await activity_producer.log_action(
                action=action,
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=details,
                changes={
                    "alert_level": alert_level,
                    "heap_used_mb": round(heap_used_mb, 1),
                    "heap_max_mb": round(heap_max_mb, 1),
                    "heap_usage_percent": round(heap_usage_percent, 1),
                    "physical_used_mb": round(physical_used_mb, 1),
                    "physical_total_mb": round(physical_total_mb, 1),
                    "physical_usage_percent": round(physical_usage_percent, 1),
                    "taskmanager_count": taskmanager_count,
                },
                source="flink",
            )

            logger.warning(
                "Resource alert logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "alert_level": alert_level,
                    "heap_usage_percent": round(heap_usage_percent, 1),
                    "physical_usage_percent": round(physical_usage_percent, 1),
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit resource alert",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _check_kafka_health(
        self,
        pipeline_id: str,
        pipeline_name: str,
    ) -> None:
        """Check Kafka consumer health metrics.

        Monitors:
        - Consumer stuck (no poll for > 30s)
        - Commit failures
        - Input record errors
        - Frequent rebalancing

        Args:
            pipeline_id: Pipeline UUID string
            pipeline_name: Pipeline name
        """
        try:
            rest_url = self._flink.get_rest_url(pipeline_id)
            client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

            job_id = await client.get_running_job_id()
            if job_id is None:
                return

            # Get job details to find Events Source vertex
            job_details = await client.get_job_details(job_id)
            vertices = job_details.get("vertices", [])

            source_vertex = None
            for vertex in vertices:
                name = vertex.get("name", "")
                if "events" in name.lower() and "source" in name.lower():
                    source_vertex = vertex
                    break

            if not source_vertex:
                return

            vertex_id = source_vertex["id"]

            async with httpx.AsyncClient(timeout=self.FLINK_TIMEOUT_SECONDS) as http:
                # Get metrics for subtask 0
                response = await http.get(f"{rest_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/0/metrics")
                if response.status_code != 200:
                    return

                # Build metric name map
                available_metrics = {m["id"]: m["id"] for m in response.json()}

                # Find the metric names (they have source name prefix)
                metrics_to_fetch = []
                metric_keys = {}

                for metric_id in available_metrics:
                    if metric_id.endswith(".KafkaConsumer.last-poll-seconds-ago"):
                        metrics_to_fetch.append(metric_id)
                        metric_keys["last_poll"] = metric_id
                    elif metric_id.endswith(".KafkaSourceReader.commitsFailed"):
                        metrics_to_fetch.append(metric_id)
                        metric_keys["commits_failed"] = metric_id
                    elif metric_id.endswith(".numRecordsInErrors"):
                        metrics_to_fetch.append(metric_id)
                        metric_keys["records_errors"] = metric_id
                    elif metric_id.endswith(".KafkaConsumer.rebalance-rate-per-hour"):
                        metrics_to_fetch.append(metric_id)
                        metric_keys["rebalance_rate"] = metric_id

                if not metrics_to_fetch:
                    return

                # Fetch metric values
                metrics_param = ",".join(metrics_to_fetch)
                response = await http.get(
                    f"{rest_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/0/metrics",
                    params={"get": metrics_param},
                )
                if response.status_code != 200:
                    return

                metric_values = {}
                for item in response.json():
                    try:
                        metric_values[item["id"]] = float(item["value"])
                    except (KeyError, ValueError, TypeError):
                        pass

                previous_state = self._states.get(pipeline_id)
                if previous_state is None:
                    return

                # Check: Consumer stuck
                if "last_poll" in metric_keys:
                    last_poll_seconds = metric_values.get(metric_keys["last_poll"], 0)
                    if last_poll_seconds > self.KAFKA_POLL_STUCK_SECONDS:
                        if not previous_state.kafka_consumer_stuck_alerted:
                            await self._emit_kafka_health_alert(
                                pipeline_id=pipeline_id,
                                pipeline_name=pipeline_name,
                                alert_type="consumer_stuck",
                                details=f"Pipeline may be overloaded or processing large batch (no new events read for {last_poll_seconds:.0f}s)",
                                metric_value=last_poll_seconds,
                            )
                            self._states[pipeline_id].kafka_consumer_stuck_alerted = True
                    else:
                        self._states[pipeline_id].kafka_consumer_stuck_alerted = False

                # Check: Commit failures increased
                if "commits_failed" in metric_keys:
                    commits_failed = int(metric_values.get(metric_keys["commits_failed"], 0))
                    if commits_failed > previous_state.kafka_commits_failed:
                        new_failures = commits_failed - previous_state.kafka_commits_failed
                        await self._emit_kafka_health_alert(
                            pipeline_id=pipeline_id,
                            pipeline_name=pipeline_name,
                            alert_type="commit_failures",
                            details=f"Pipeline failed to save progress to Kafka ({new_failures} failures) — events may be reprocessed after restart",
                            metric_value=commits_failed,
                        )
                    self._states[pipeline_id].kafka_commits_failed = commits_failed

                # Check: Input errors increased
                if "records_errors" in metric_keys:
                    records_errors = int(metric_values.get(metric_keys["records_errors"], 0))
                    if records_errors > previous_state.kafka_records_in_errors:
                        new_errors = records_errors - previous_state.kafka_records_in_errors
                        await self._emit_kafka_health_alert(
                            pipeline_id=pipeline_id,
                            pipeline_name=pipeline_name,
                            alert_type="input_errors",
                            details=f"Pipeline encountered {new_errors} malformed events that couldn't be processed",
                            metric_value=records_errors,
                        )
                    self._states[pipeline_id].kafka_records_in_errors = records_errors

                # Check: Frequent rebalancing
                if "rebalance_rate" in metric_keys:
                    rebalance_rate = metric_values.get(metric_keys["rebalance_rate"], 0)
                    if rebalance_rate > self.KAFKA_REBALANCE_RATE_THRESHOLD:
                        if not previous_state.kafka_rebalance_alerted:
                            await self._emit_kafka_health_alert(
                                pipeline_id=pipeline_id,
                                pipeline_name=pipeline_name,
                                alert_type="frequent_rebalancing",
                                details=f"Pipeline workers are frequently restarting ({rebalance_rate:.0f} times/hour) — may indicate resource issues",
                                metric_value=rebalance_rate,
                            )
                            self._states[pipeline_id].kafka_rebalance_alerted = True
                    else:
                        self._states[pipeline_id].kafka_rebalance_alerted = False

        except Exception as e:
            logger.debug(
                "Failed to check Kafka health",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    async def _emit_kafka_health_alert(
        self,
        pipeline_id: str,
        pipeline_name: str,
        alert_type: str,
        details: str,
        metric_value: float,
    ) -> None:
        """Emit activity event for Kafka consumer health issues."""
        try:
            await activity_producer.log_action(
                action="alert",
                entity_type="pipeline",
                entity_id=pipeline_id,
                entity_name=pipeline_name,
                user=None,
                details=details,
                changes={
                    "alert_type": alert_type,
                    "metric_value": metric_value,
                },
                source="flink",
            )

            logger.warning(
                "Kafka health alert logged",
                extra={
                    "pipeline_id": pipeline_id,
                    "alert_type": alert_type,
                    "metric_value": metric_value,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to emit Kafka health alert",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )

    def clear_state(self, pipeline_id: str | None = None) -> None:
        """Clear tracked state for a pipeline or all pipelines.

        Useful when a pipeline is deleted or disabled.

        Args:
            pipeline_id: Specific pipeline to clear, or None for all
        """
        if pipeline_id:
            self._states.pop(pipeline_id, None)
        else:
            self._states.clear()


# Singleton instance
flink_monitor_service = FlinkMonitorService()
