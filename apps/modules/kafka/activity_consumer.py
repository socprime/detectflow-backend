"""Kafka consumer for activity events.

This module provides an async Kafka consumer that reads activity events
from the etl-activity topic and stores them in PostgreSQL for audit logs
and dashboard display.

Architecture:
- Consumes from etl-activity topic
- Validates events with Pydantic (ActivityEvent schema)
- Stores in PostgreSQL audit_logs table via AuditLogDAO
- Triggers SSE callbacks for real-time dashboard updates
"""

import asyncio
import json
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError, KafkaError
from pydantic import ValidationError

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.schemas import ActivityEvent
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaAsyncClient
from apps.modules.postgre.audit import AuditLogDAO

logger = get_logger(__name__)


class ActivityConsumerService(BaseKafkaAsyncClient):
    """Async Kafka consumer for activity events.

    Consumes activity events from the etl-activity Kafka topic and:
    1. Stores them in PostgreSQL (audit_logs table)
    2. Notifies subscribers (SSE manager) for real-time updates

    Attributes:
        consumer: AIOKafkaConsumer instance
        is_running: Whether the consumer is actively consuming
        last_message_time: Timestamp of last received message (for health checks)
    """

    def __init__(self):
        """Initialize the activity consumer service."""
        # Generate unique group_id per instance to avoid conflicts
        unique_group_id = f"{settings.kafka_activity_consumer_group}-{uuid.uuid4().hex[:8]}"
        super().__init__(
            group_id=unique_group_id,
            auto_offset_reset="latest",  # Only new messages for dashboard
            enable_auto_commit=True,
        )
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running: bool = False
        self.last_message_time: datetime | None = None
        self._on_activity_callbacks: list[Callable] = []
        self._consume_task: asyncio.Task | None = None

        # Add consumer-specific config
        self._config.update(
            {
                "auto_commit_interval_ms": 5000,
            }
        )

    def _get_default_group_id(self) -> str:
        """Return default group ID for activity consumer service."""
        return settings.kafka_activity_consumer_group

    def add_activity_callback(self, callback: Callable) -> None:
        """Register a callback to be invoked when a new activity event arrives.

        Args:
            callback: Async function that accepts (event: ActivityEvent)
        """
        self._on_activity_callbacks.append(callback)
        logger.debug(f"Added activity callback, total: {len(self._on_activity_callbacks)}")

    def remove_activity_callback(self, callback: Callable) -> None:
        """Remove a previously registered callback."""
        if callback in self._on_activity_callbacks:
            self._on_activity_callbacks.remove(callback)
            logger.debug(f"Removed activity callback, total: {len(self._on_activity_callbacks)}")

    async def start(self) -> None:
        """Start the Kafka consumer and begin consuming messages."""
        if self.is_running:
            logger.warning("ActivityConsumerService is already running")
            return

        logger.info(
            "Starting ActivityConsumerService",
            extra={
                "topic": settings.kafka_activity_topic,
                "group_id": self._group_id,
                "bootstrap_servers": settings.kafka_bootstrap_servers,
            },
        )

        try:
            self.consumer = AIOKafkaConsumer(settings.kafka_activity_topic, **self._config)

            await self.consumer.start()
            self.is_running = True

            # Start consuming in background task
            self._consume_task = asyncio.create_task(self._consume_loop())

            logger.info("ActivityConsumerService started successfully")

        except KafkaConnectionError as e:
            logger.error(
                "Failed to connect to Kafka",
                extra={"error": str(e), "bootstrap_servers": settings.kafka_bootstrap_servers},
            )
            # Note: Cannot use activity_producer here as this IS the activity consumer
            # Just log to file - this error will be visible in container logs
            raise
        except Exception as e:
            logger.error("Failed to start ActivityConsumerService", extra={"error": str(e)})
            # Note: Cannot use activity_producer here as this IS the activity consumer
            raise

    async def stop(self) -> None:
        """Stop the Kafka consumer gracefully."""
        if not self.is_running:
            return

        logger.info("Stopping ActivityConsumerService")
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

        logger.info("ActivityConsumerService stopped")

    async def _consume_loop(self) -> None:
        """Main consumption loop - runs until stopped.

        Uses batch processing: collects all messages from getmany(),
        parses them, stores in DB with single transaction, then notifies callbacks.
        """
        logger.info("Starting activity consumption loop")

        while self.is_running:
            try:
                # Get batch of messages
                messages = await self.consumer.getmany(timeout_ms=1000, max_records=100)

                # Collect all valid events from the batch
                events: list[ActivityEvent] = []
                for _tp, msgs in messages.items():
                    for message in msgs:
                        if not self.is_running:
                            break
                        event = self._parse_message(message.value)
                        if event:
                            events.append(event)

                if not events:
                    continue

                # Store all events in a single transaction
                await self._store_events_batch(events)

                self.last_message_time = datetime.now(UTC)

                logger.info(
                    "Processed activity batch",
                    extra={"count": len(events)},
                )

                # Notify callbacks for each event
                for event in events:
                    for callback in self._on_activity_callbacks:
                        try:
                            # Support both sync and async callbacks
                            if asyncio.iscoroutinefunction(callback):
                                await callback(event)
                            else:
                                callback(event)
                        except Exception as e:
                            logger.error(
                                "Error in activity callback",
                                extra={"error": str(e), "event_id": event.id},
                            )

            except asyncio.CancelledError:
                logger.info("Activity consumption loop cancelled")
                break
            except KafkaError as e:
                logger.error("Kafka error in consumption loop", extra={"error": str(e)})
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("Unexpected error in consumption loop", extra={"error": str(e)})
                await asyncio.sleep(5)

    def _parse_message(self, value: str | None) -> ActivityEvent | None:
        """Parse a single Kafka message into ActivityEvent.

        Args:
            value: Message value (JSON activity event data)

        Returns:
            Parsed ActivityEvent or None if parsing failed
        """
        if not value:
            return None

        try:
            raw_data = json.loads(value)
            return ActivityEvent.model_validate(raw_data)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse activity JSON: {e}")
            return None
        except ValidationError as e:
            logger.warning(f"Failed to validate activity event schema: {e}")
            return None

    async def _store_events_batch(self, events: list[ActivityEvent]) -> None:
        """Store multiple activity events in PostgreSQL using single transaction.

        Args:
            events: List of validated ActivityEvents
        """
        if not events:
            return

        try:
            async with AsyncSessionLocal() as db:
                try:
                    audit_dao = AuditLogDAO(db)
                    count = await audit_dao.create_batch(events)
                    await db.commit()
                    logger.debug(
                        "Batch stored activity events",
                        extra={"count": count},
                    )
                except Exception:
                    await db.rollback()
                    raise

        except Exception as e:
            logger.error(
                "Failed to batch store activity events",
                extra={"count": len(events), "error": str(e)},
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


# Singleton instance
activity_consumer = ActivityConsumerService()
