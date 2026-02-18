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

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.models import Pipeline
from apps.core.schemas import KafkaMetricMessage
from apps.core.settings import settings
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
            raise
        except Exception as e:
            logger.error("Failed to start MetricsConsumerService", extra={"error": str(e)})
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
        """Main consumption loop - runs until stopped."""
        logger.info("Starting metrics consumption loop")

        while self.is_running:
            try:
                # Use getmany with timeout instead of async for
                # This is more reliable for message delivery
                messages = await self.consumer.getmany(timeout_ms=1000, max_records=100)

                for _tp, msgs in messages.items():
                    for message in msgs:
                        if not self.is_running:
                            break

                        logger.debug(
                            "Received Kafka message",
                            extra={"partition": message.partition, "offset": message.offset},
                        )
                        await self._process_message(message.key, message.value)

            except asyncio.CancelledError:
                logger.info("Consumption loop cancelled")
                break
            except KafkaError as e:
                logger.error("Kafka error in consumption loop", extra={"error": str(e)})
                # Wait before retry
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("Unexpected error in consumption loop", extra={"error": str(e)})
                await asyncio.sleep(5)

    async def _process_message(self, key: str | None, value: str | None) -> None:
        """Process a single Kafka message.

        Args:
            key: Message key (job_id)
            value: Message value (JSON metric data)
        """
        if not value:
            return

        try:
            # Parse JSON
            raw_data = json.loads(value)

            # Validate with Pydantic
            metric = KafkaMetricMessage.model_validate(raw_data)

            self.last_message_time = datetime.now(UTC)

            # Find pipeline_id from job_id (cache lookup + DB fallback)
            pipeline_id = await self._get_pipeline_id(metric.job_id)

            if pipeline_id:
                # Store in database
                await self._store_metric(pipeline_id, metric)

                logger.info(
                    "Processed metric",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "eps": metric.window_throughput_eps,
                        "events": metric.window_total_events,
                    },
                )

                # Notify all registered callbacks
                for callback in self._on_metric_callbacks:
                    try:
                        await callback(pipeline_id, metric)
                    except Exception as e:
                        logger.error(
                            "Error in metric callback",
                            extra={"error": str(e), "pipeline_id": str(pipeline_id)},
                        )
            else:
                logger.warning(
                    "Received metric for unknown pipeline",
                    extra={"job_id": metric.job_id},
                )

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse metric JSON: {e}")
        except ValidationError as e:
            logger.warning(f"Failed to validate metric schema: {e}")
        except Exception as e:
            logger.exception(f"Error processing metric message: {e}")

    async def _get_pipeline_id(self, job_id: str) -> UUID | None:
        """Get pipeline UUID from job_id (which is also the pipeline ID).

        The job_id in metrics is the pipeline UUID, but we verify it exists.

        Args:
            job_id: The job_id from Kafka metric (pipeline UUID string)

        Returns:
            Pipeline UUID if found, None otherwise
        """
        # Check cache first
        if job_id in self._pipeline_id_cache:
            return self._pipeline_id_cache[job_id]

        # Try to parse as UUID and verify in DB
        try:
            pipeline_uuid = UUID(job_id)

            async with AsyncSessionLocal() as db:
                pipeline = await db.get(Pipeline, pipeline_uuid)
                if pipeline:
                    self._pipeline_id_cache[job_id] = pipeline_uuid
                    return pipeline_uuid

        except (ValueError, TypeError):
            # Not a valid UUID
            pass

        return None

    async def _store_metric(self, pipeline_id: UUID, metric: KafkaMetricMessage) -> None:
        """Store metrics in PostgreSQL including pipeline totals.

        Updates:
        - pipeline.events_tagged/events_untagged: Accumulated counters (from window metrics)
        - pipeline_metrics: Time-series history
        - pipeline_rule_metrics: Per-rule match counters

        Args:
            pipeline_id: Pipeline UUID
            metric: Validated metric data
        """
        try:
            async with AsyncSessionLocal() as db:
                try:
                    metrics_dao = MetricsDAO(db)
                    # Store all metrics: history + pipeline totals + per-rule counters
                    await metrics_dao.append_metric(pipeline_id, metric)
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise

            logger.debug(
                "Stored metrics in PostgreSQL",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "tagged": metric.window_matched_events,
                    "total": metric.window_total_events,
                },
            )

        except Exception as e:
            logger.error(
                "Failed to store metrics in PostgreSQL",
                extra={"pipeline_id": str(pipeline_id), "error": str(e)},
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
