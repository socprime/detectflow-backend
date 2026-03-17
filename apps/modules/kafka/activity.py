"""Kafka producer for activity events.

This module provides a producer service for publishing activity events
to the etl-activity Kafka topic. Events are consumed by ActivityConsumerService
and stored in PostgreSQL for audit logs and dashboard display.

Features:
- Batching: Events are batched for efficient delivery
- Retry: Failed events are retried with exponential backoff
- Dead-letter logging: Permanently failed events are logged to file

Usage:
    from apps.modules.kafka.activity import activity_producer

    await activity_producer.log_action(
        action="create",
        entity_type="pipeline",
        entity_id=str(pipeline.id),
        entity_name=pipeline.name,
        user=current_user,
        details="Created pipeline from source to dest",
    )
"""

import asyncio
import json
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from confluent_kafka import KafkaError, Producer

from apps.core.audit_hooks import audit_hooks
from apps.core.enums import get_severity_for_action
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import ActivityEvent
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


@dataclass
class PendingEvent:
    """Event pending delivery with retry metadata."""

    event: ActivityEvent
    retries: int = 0
    last_error: str | None = None


class ActivityProducerService(BaseKafkaSyncClient):
    """Kafka producer for activity events with batching and retry support.

    Publishes activity events to the etl-activity topic. These events
    are consumed by ActivityConsumerService and stored in PostgreSQL.

    The producer uses confluent_kafka with ThreadPoolExecutor for
    async compatibility.

    Features:
    - Batching: Events are collected and sent in batches for efficiency
    - Retry: Failed events are retried up to MAX_RETRIES times
    - Dead-letter: Permanently failed events are logged to a file
    """

    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-activity")

    # Batching configuration
    BATCH_SIZE = 10  # Max events per batch
    BATCH_TIMEOUT_MS = 100  # Flush after this many ms even if batch not full

    # Retry configuration
    MAX_RETRIES = 3
    RETRY_BACKOFF_BASE_MS = 100  # Base delay between retries (exponential)

    # Dead-letter log file
    DLQ_LOG_PATH = Path("logs/activity_dlq.jsonl")

    def __init__(self):
        """Initialize the activity producer service."""
        super().__init__(client_id="admin-panel-activity")
        self.topic = settings.kafka_activity_topic

        # Add producer-specific config
        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",  # Wait for all replicas
                "compression.type": "gzip",
                "linger.ms": self.BATCH_TIMEOUT_MS,  # Batch timeout
                "batch.num.messages": self.BATCH_SIZE,  # Max batch size
                "retries": 0,  # We handle retries ourselves
            }
        )
        self._producer: Producer | None = None

        # Batching state
        self._batch: list[PendingEvent] = []
        self._batch_lock = threading.Lock()
        self._last_flush_time = time.monotonic()

        # Retry queue
        self._retry_queue: list[PendingEvent] = []
        self._retry_lock = threading.Lock()

        # Delivery tracking
        self._pending_deliveries: dict[str, PendingEvent] = {}
        self._delivery_lock = threading.Lock()

        # Ensure DLQ directory exists
        self.DLQ_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    def _get_default_client_id(self) -> str:
        """Return default client ID for activity producer."""
        return "admin-panel-activity"

    def _get_producer(self) -> Producer:
        """Get or create the Kafka producer (lazy initialization)."""
        if self._producer is None:
            self._producer = Producer(self.producer_config)
            logger.info(
                "ActivityProducerService initialized",
                extra={"topic": self.topic, "bootstrap_servers": self.bootstrap_servers},
            )
        return self._producer

    def _on_delivery(self, err, msg, event_id: str) -> None:
        """Handle delivery callback from Kafka producer.

        Args:
            err: Delivery error (None if successful)
            msg: Message that was delivered
            event_id: Event ID for tracking
        """
        with self._delivery_lock:
            pending = self._pending_deliveries.pop(event_id, None)

        if err:
            error_msg = str(err)
            logger.warning(
                "Activity event delivery failed",
                extra={"error": error_msg, "event_id": event_id},
            )

            if pending:
                # Check if retriable error
                if self._is_retriable_error(err) and pending.retries < self.MAX_RETRIES:
                    pending.retries += 1
                    pending.last_error = error_msg
                    with self._retry_lock:
                        self._retry_queue.append(pending)
                    logger.info(
                        "Scheduling retry for activity event",
                        extra={"event_id": event_id, "retry": pending.retries},
                    )
                else:
                    # Max retries exceeded or non-retriable error
                    self._send_to_dlq(pending, error_msg)
        else:
            logger.debug(
                "Activity event delivered",
                extra={"event_id": event_id, "partition": msg.partition()},
            )

    def _is_retriable_error(self, err) -> bool:
        """Check if error is retriable.

        Args:
            err: Kafka error

        Returns:
            True if error is retriable
        """
        if err is None:
            return False

        # Check if it's a KafkaError with retriable code
        if hasattr(err, "code"):
            retriable_codes = {
                KafkaError._TIMED_OUT,
                KafkaError._TRANSPORT,
                KafkaError._MSG_TIMED_OUT,
                KafkaError.REQUEST_TIMED_OUT,
                KafkaError.NOT_LEADER_FOR_PARTITION,
                KafkaError.LEADER_NOT_AVAILABLE,
            }
            return err.code() in retriable_codes

        return True  # Default to retriable for unknown errors

    def _send_to_dlq(self, pending: PendingEvent, error: str) -> None:
        """Send failed event to dead-letter queue (log file).

        Args:
            pending: Failed event with metadata
            error: Final error message
        """
        dlq_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "event_id": pending.event.id,
            "retries": pending.retries,
            "error": error,
            "event": pending.event.model_dump(mode="json"),
        }

        try:
            with open(self.DLQ_LOG_PATH, "a") as f:
                f.write(json.dumps(dlq_entry) + "\n")

            logger.error(
                "Activity event sent to DLQ after max retries",
                extra={
                    "event_id": pending.event.id,
                    "retries": pending.retries,
                    "error": error,
                    "dlq_path": str(self.DLQ_LOG_PATH),
                },
            )
        except Exception as e:
            logger.error(
                "Failed to write to DLQ file",
                extra={"event_id": pending.event.id, "error": str(e)},
            )

    async def publish(self, event: ActivityEvent) -> None:
        """Publish activity event to Kafka with batching.

        Args:
            event: ActivityEvent to publish
        """
        await asyncio.get_running_loop().run_in_executor(self._executor, self._add_to_batch, event)

    def _add_to_batch(self, event: ActivityEvent) -> None:
        """Add event to batch and flush if needed.

        Args:
            event: ActivityEvent to add
        """
        pending = PendingEvent(event=event)

        with self._batch_lock:
            self._batch.append(pending)

            # Check if batch should be flushed
            should_flush = (
                len(self._batch) >= self.BATCH_SIZE
                or (time.monotonic() - self._last_flush_time) * 1000 >= self.BATCH_TIMEOUT_MS
            )

            if should_flush:
                batch_to_send = self._batch
                self._batch = []
                self._last_flush_time = time.monotonic()
            else:
                batch_to_send = None

        if batch_to_send:
            self._send_batch(batch_to_send)

        # Process retry queue
        self._process_retries()

    def _send_batch(self, batch: list[PendingEvent]) -> None:
        """Send a batch of events to Kafka.

        Args:
            batch: List of pending events to send
        """
        producer = self._get_producer()

        for pending in batch:
            event = pending.event
            try:
                # Track pending delivery
                with self._delivery_lock:
                    self._pending_deliveries[event.id] = pending

                # Serialize event to JSON
                value = event.model_dump_json()
                key = event.id.encode("utf-8")

                # Create callback with event_id bound
                def make_callback(eid: str) -> Callable:
                    return lambda err, msg: self._on_delivery(err, msg, eid)

                producer.produce(
                    topic=self.topic,
                    key=key,
                    value=value.encode("utf-8"),
                    callback=make_callback(event.id),
                )

            except Exception as e:
                logger.error(
                    "Error producing activity event",
                    extra={"error": str(e), "event_id": event.id},
                )
                # Remove from pending and handle as failed
                with self._delivery_lock:
                    self._pending_deliveries.pop(event.id, None)

                if pending.retries < self.MAX_RETRIES:
                    pending.retries += 1
                    pending.last_error = str(e)
                    with self._retry_lock:
                        self._retry_queue.append(pending)
                else:
                    self._send_to_dlq(pending, str(e))

        # Poll to handle delivery callbacks
        producer.poll(0)

    def _process_retries(self) -> None:
        """Process events in the retry queue."""
        with self._retry_lock:
            if not self._retry_queue:
                return

            # Take events ready for retry
            events_to_retry = self._retry_queue
            self._retry_queue = []

        for pending in events_to_retry:
            # Apply exponential backoff
            delay_ms = self.RETRY_BACKOFF_BASE_MS * (2 ** (pending.retries - 1))
            time.sleep(delay_ms / 1000)

            self._send_batch([pending])

    async def log_action(
        self,
        action: str,
        entity_type: str,
        entity_id: str | None = None,
        entity_name: str | None = None,
        user: User | None = None,
        details: str | None = None,
        changes: dict | None = None,
        source: str = "user",
        severity: str | None = None,
    ) -> None:
        """Helper to create and publish activity event.

        This is the primary method to use in managers for logging actions.

        Args:
            action: Action type (create, update, delete, toggle, error, sync, login)
            entity_type: Entity type (pipeline, rule, filter, repository, etc.)
            entity_id: Entity UUID (optional)
            entity_name: Entity name for display (optional)
            user: User who performed the action (optional, None for system events)
            details: Human-readable description (optional)
            changes: Field changes dict {field: {old, new}} (optional)
            source: Event source (user, flink, system). Defaults to "user"
            severity: Severity level (info, warning, error). Auto-determined from action if not provided.

        Example:
            await activity_producer.log_action(
                action="update",
                entity_type="pipeline",
                entity_id=str(pipeline.id),
                entity_name=pipeline.name,
                user=current_user,
                details="Enabled pipeline",
                changes={"enabled": {"old": False, "new": True}},
            )
        """
        # Auto-determine severity from action if not provided
        if severity is None:
            severity = get_severity_for_action(action)

        event = ActivityEvent(
            id=str(uuid4()),
            timestamp=datetime.now(UTC),
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            entity_name=entity_name,
            user_id=str(user.id) if user else None,
            user_email=user.email if user else None,
            details=details,
            changes=changes,
            source=source,
            severity=severity,
        )

        try:
            await self.publish(event)
            logger.debug(
                "Activity logged",
                extra={
                    "action": action,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "user_email": user.email if user else None,
                },
            )
        except Exception as e:
            # Log but don't raise - activity logging should not break main flow
            logger.error(
                "Failed to log activity",
                extra={
                    "error": str(e),
                    "action": action,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                },
            )

    def flush(self, timeout: float = 10.0) -> None:
        """Flush pending messages to Kafka.

        Should be called during shutdown.

        Args:
            timeout: Flush timeout in seconds
        """
        # Flush any remaining batch
        with self._batch_lock:
            if self._batch:
                batch_to_send = self._batch
                self._batch = []
            else:
                batch_to_send = None

        if batch_to_send:
            self._send_batch(batch_to_send)

        # Process any remaining retries
        self._process_retries()

        # Flush Kafka producer
        if self._producer:
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning(
                    "Some activity events were not delivered",
                    extra={"remaining": remaining},
                )

    def close(self) -> None:
        """Close the producer (flush and cleanup)."""
        if self._producer:
            self.flush()
            self._producer = None
            logger.info("ActivityProducerService closed")


# Singleton instance
activity_producer = ActivityProducerService()

# Register audit callback for database.py to use without circular import
audit_hooks.register_db_callback(activity_producer.log_action)
