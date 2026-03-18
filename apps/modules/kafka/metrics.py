"""Kafka metrics consumer service using aiokafka.

This module provides an async Kafka consumer that reads pipeline metrics
from the Flink metrics topic and stores them in PostgreSQL for dashboard use.

Architecture:
- PostgreSQL for metrics storage (append-only INSERTs + atomic UPDATE for totals)
- SSE callbacks for real-time dashboard updates
"""

import asyncio
import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from pydantic import ValidationError
from sqlalchemy import select

from apps.core.database import AsyncSessionLocal
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import Pipeline
from apps.core.schemas import KafkaMetricMessage
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.base import BaseKafkaAsyncClient
from apps.modules.postgre.metrics import MetricsDAO

logger = get_logger(__name__)


class MetricsConsumerService(BaseKafkaAsyncClient):
    """Async Kafka consumer for pipeline metrics.

    Consumes metrics from the Flink metrics Kafka topic and:
    1. Stores them in PostgreSQL (append-only history + atomic total updates)
    2. Notifies subscribers (SSE manager) for real-time updates

    PostgreSQL stores:
    - pipeline.events_tagged/events_untagged: Accumulated counters (atomic UPDATE)
    - pipeline_metrics: Time-series history (append-only INSERT, 48h retention)

    Attributes:
        consumer: AIOKafkaConsumer instance
        is_running: Whether the consumer is actively consuming
        last_message_time: Timestamp of last received message (for health checks)
        on_metric_callback: Callback invoked when new metric arrives
    """

    def __init__(self):
        """Initialize the metrics consumer service."""
        # Generate unique group_id per instance to avoid conflicts
        unique_group_id = f"{settings.kafka_metrics_consumer_group}-{uuid.uuid4().hex[:8]}"
        super().__init__(
            group_id=unique_group_id,
            auto_offset_reset="latest",  # Only new messages for dashboard
            enable_auto_commit=True,
        )
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running: bool = False
        self.last_message_time: datetime | None = None
        self._on_metric_callbacks: list[Callable] = []
        self._consume_task: asyncio.Task | None = None
        self._pipeline_id_cache: dict[str, UUID] = {}  # job_id -> pipeline_id mapping

        # Add consumer-specific config
        self._config.update(
            {
                "auto_commit_interval_ms": 5000,
            }
        )

    def _get_default_group_id(self) -> str:
        """Return default group ID for metrics consumer service."""
        return settings.kafka_metrics_consumer_group

    def add_metric_callback(self, callback: Callable) -> None:
        """Register a callback to be invoked when a new metric arrives.

        Args:
            callback: Async function that accepts (pipeline_id: UUID, metric: KafkaMetricMessage)
        """
        self._on_metric_callbacks.append(callback)

    def remove_metric_callback(self, callback: Callable) -> None:
        """Remove a previously registered callback."""
        if callback in self._on_metric_callbacks:
            self._on_metric_callbacks.remove(callback)

    async def start(self) -> None:
        """Start the Kafka consumer and begin consuming messages."""
        if self.is_running:
            logger.warning("MetricsConsumerService is already running")
            return

        logger.info(
            "Starting MetricsConsumerService",
            extra={
                "topic": settings.kafka_metrics_topic,
                "group_id": self._group_id,
                "bootstrap_servers": settings.kafka_bootstrap_servers,
            },
        )

        try:
            self.consumer = AIOKafkaConsumer(settings.kafka_metrics_topic, **self._config)

            await self.consumer.start()
            self.is_running = True

            # Start consuming in background task
            self._consume_task = asyncio.create_task(self._consume_loop())

            logger.info("MetricsConsumerService started successfully")

        except KafkaConnectionError as e:
            logger.error(
                "Failed to connect to Kafka",
                extra={"error": str(e), "bootstrap_servers": settings.kafka_bootstrap_servers},
            )
            if ErrorTracker.should_log("kafka_connect_metrics"):
                asyncio.create_task(
                    activity_producer.log_action(
                        action="error",
                        entity_type="kafka",
                        details=f"MetricsConsumer failed to connect to Kafka: {str(e)}",
                        source="system",
                        severity="error",
                    )
                )
            raise
        except Exception as e:
            logger.error("Failed to start MetricsConsumerService", extra={"error": str(e)})
            if ErrorTracker.should_log("kafka_start_metrics"):
                asyncio.create_task(
                    activity_producer.log_action(
                        action="error",
                        entity_type="kafka",
                        details=f"MetricsConsumer failed to start: {str(e)}",
                        source="system",
                        severity="error",
                    )
                )
            raise

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        if not self.is_running:
            return

        logger.info("Stopping MetricsConsumerService")
        self.is_running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        if self.consumer:
            await self.consumer.stop()
            self.consumer = None

        logger.info("MetricsConsumerService stopped")

    async def _consume_loop(self) -> None:
        """Main consumption loop - runs until stopped.

        Uses batch processing: collects all messages from getmany(),
        parses them, resolves pipeline IDs, stores in DB with single transaction,
        then notifies callbacks.
        """
        logger.info("Starting metrics consumption loop")

        while self.is_running:
            try:
                # Get batch of messages
                messages = await self.consumer.getmany(timeout_ms=1000, max_records=100)

                # Parse all messages first (no DB access yet)
                parsed_metrics: list[KafkaMetricMessage] = []
                for _tp, msgs in messages.items():
                    for message in msgs:
                        if not self.is_running:
                            break
                        metric = self._parse_message(message.value)
                        if metric:
                            parsed_metrics.append(metric)

                if not parsed_metrics:
                    continue

                # Resolve pipeline IDs and store metrics in batch
                await self._process_metrics_batch(parsed_metrics)

                self.last_message_time = datetime.now(UTC)

                logger.debug(
                    "Processed metrics batch",
                    extra={"count": len(parsed_metrics)},
                )

            except asyncio.CancelledError:
                logger.info("Consumption loop cancelled")
                break
            except KafkaError as e:
                logger.error("Kafka error in consumption loop", extra={"error": str(e)})
                if ErrorTracker.should_log("kafka_consume_metrics"):
                    await activity_producer.log_action(
                        action="error",
                        entity_type="kafka",
                        details=f"MetricsConsumer Kafka error in consumption loop: {str(e)}",
                        source="system",
                        severity="error",
                    )
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("Unexpected error in consumption loop", extra={"error": str(e)})
                if ErrorTracker.should_log("kafka_consume_metrics_unexpected"):
                    await activity_producer.log_action(
                        action="error",
                        entity_type="kafka",
                        details=f"MetricsConsumer unexpected error in consumption loop: {str(e)}",
                        source="system",
                        severity="error",
                    )
                await asyncio.sleep(5)

    def _parse_message(self, value: str | None) -> KafkaMetricMessage | None:
        """Parse a single Kafka message into KafkaMetricMessage.

        Args:
            value: Message value (JSON metric data)

        Returns:
            Parsed KafkaMetricMessage or None if parsing failed
        """
        if not value:
            return None

        try:
            raw_data = json.loads(value)
            return KafkaMetricMessage.model_validate(raw_data)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse metric JSON: {e}")
            return None
        except ValidationError as e:
            logger.warning(f"Failed to validate metric schema: {e}")
            return None

    async def _process_metrics_batch(self, metrics: list[KafkaMetricMessage]) -> None:
        """Process a batch of metrics: resolve pipeline IDs and store in DB.

        Args:
            metrics: List of parsed KafkaMetricMessage objects
        """
        if not metrics:
            return

        # Resolve pipeline IDs (uses cache, only hits DB for cache misses)
        metrics_with_ids: list[tuple[UUID, KafkaMetricMessage]] = []
        unknown_job_ids: set[str] = set()

        for metric in metrics:
            pipeline_id = self._get_pipeline_id_from_cache(metric.job_id)
            if pipeline_id:
                metrics_with_ids.append((pipeline_id, metric))
            else:
                unknown_job_ids.add(metric.job_id)

        # Batch lookup for cache misses
        if unknown_job_ids:
            resolved_ids = await self._resolve_pipeline_ids_batch(list(unknown_job_ids))
            # Re-process metrics that had cache misses
            for metric in metrics:
                if metric.job_id in unknown_job_ids:
                    pipeline_id = resolved_ids.get(metric.job_id)
                    if pipeline_id:
                        metrics_with_ids.append((pipeline_id, metric))
                    else:
                        logger.warning(
                            "Received metric for unknown pipeline",
                            extra={"job_id": metric.job_id},
                        )

        if not metrics_with_ids:
            return

        # Store all metrics in a single transaction
        await self._store_metrics_batch(metrics_with_ids)

        # Notify callbacks
        for pipeline_id, metric in metrics_with_ids:
            for callback in self._on_metric_callbacks:
                try:
                    await callback(pipeline_id, metric)
                except Exception as e:
                    logger.error(
                        "Error in metric callback",
                        extra={"error": str(e), "pipeline_id": str(pipeline_id)},
                    )

    def _get_pipeline_id_from_cache(self, job_id: str) -> UUID | None:
        """Get pipeline UUID from cache only.

        Args:
            job_id: The job_id from Kafka metric (pipeline UUID string)

        Returns:
            Pipeline UUID if in cache, None otherwise
        """
        return self._pipeline_id_cache.get(job_id)

    async def _resolve_pipeline_ids_batch(self, job_ids: list[str]) -> dict[str, UUID]:
        """Resolve multiple job_ids to pipeline UUIDs in a single DB query.

        Args:
            job_ids: List of job_id strings to resolve

        Returns:
            Dict mapping job_id to pipeline UUID (only found pipelines)
        """
        if not job_ids:
            return {}

        # Parse valid UUIDs
        uuid_map: dict[str, UUID] = {}
        for job_id in job_ids:
            try:
                uuid_map[job_id] = UUID(job_id)
            except (ValueError, TypeError):
                pass

        if not uuid_map:
            return {}

        result: dict[str, UUID] = {}
        try:
            async with AsyncSessionLocal() as db:
                query = select(Pipeline.id).where(Pipeline.id.in_(uuid_map.values()))
                db_result = await db.execute(query)
                existing_ids = {row[0] for row in db_result.fetchall()}

                for job_id, pipeline_uuid in uuid_map.items():
                    if pipeline_uuid in existing_ids:
                        result[job_id] = pipeline_uuid
                        self._pipeline_id_cache[job_id] = pipeline_uuid

        except Exception as e:
            logger.error(f"Failed to resolve pipeline IDs: {e}")

        return result

    async def _store_metrics_batch(self, metrics: list[tuple[UUID, KafkaMetricMessage]]) -> None:
        """Store multiple metrics in PostgreSQL using single transaction.

        Args:
            metrics: List of (pipeline_id, metric) tuples
        """
        if not metrics:
            return

        try:
            async with AsyncSessionLocal() as db:
                try:
                    metrics_dao = MetricsDAO(db)
                    count = await metrics_dao.append_metrics_batch(metrics)
                    await db.commit()
                    logger.debug(
                        "Batch stored metrics",
                        extra={"count": count},
                    )
                except Exception:
                    await db.rollback()
                    raise

        except Exception as e:
            logger.error(
                "Failed to batch store metrics",
                extra={"count": len(metrics), "error": str(e)},
            )
            if ErrorTracker.should_log("metrics_batch_store"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="metrics",
                    details=f"Failed to batch store metrics: {str(e)}",
                    source="system",
                    severity="error",
                )

    @property
    def is_healthy(self) -> bool:
        """Check if consumer is healthy (running and receiving messages)."""
        if not self.is_running:
            return False

        # Consider unhealthy if no messages in last 5 minutes
        if self.last_message_time:
            age = (datetime.now(UTC) - self.last_message_time).total_seconds()
            return age < 300  # 5 minutes

        # No messages received yet - still healthy if just started
        return True

    def clear_cache(self) -> None:
        """Clear the pipeline ID cache (useful when pipelines are updated)."""
        self._pipeline_id_cache.clear()


# Singleton instance
metrics_consumer = MetricsConsumerService()
