"""Kafka producer for activity events.

This module provides a producer service for publishing activity events
to the etl-activity Kafka topic. Events are consumed by ActivityConsumerService
and stored in PostgreSQL for audit logs and dashboard display.

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
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer

from apps.core.enums import get_severity_for_action
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import ActivityEvent
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


class ActivityProducerService(BaseKafkaSyncClient):
    """Kafka producer for activity events.

    Publishes activity events to the etl-activity topic. These events
    are consumed by ActivityConsumerService and stored in PostgreSQL.

    The producer uses confluent_kafka with ThreadPoolExecutor for
    async compatibility.
    """

    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-activity")

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
                "linger.ms": 5,  # Small delay to batch messages
            }
        )
        self._producer: Producer | None = None

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

    async def publish(self, event: ActivityEvent) -> None:
        """Publish activity event to Kafka.

        Args:
            event: ActivityEvent to publish
        """
        await asyncio.get_running_loop().run_in_executor(self._executor, self._publish_sync, event)

    def _publish_sync(self, event: ActivityEvent) -> None:
        """Synchronously publish activity event to Kafka.

        Args:
            event: ActivityEvent to publish
        """
        producer = self._get_producer()

        def on_delivery(err, msg):
            if err:
                logger.error(
                    "Failed to deliver activity event",
                    extra={"error": str(err), "event_id": event.id},
                )
            else:
                logger.debug(
                    "Activity event delivered",
                    extra={"event_id": event.id, "partition": msg.partition()},
                )

        try:
            # Serialize event to JSON
            value = event.model_dump_json()
            key = event.id.encode("utf-8")

            producer.produce(
                topic=self.topic,
                key=key,
                value=value.encode("utf-8"),
                callback=on_delivery,
            )

            # Poll to handle delivery callbacks
            producer.poll(0)

        except Exception as e:
            logger.error(
                "Error publishing activity event",
                extra={"error": str(e), "event_id": event.id},
            )
            raise

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

        This is the primary method to use in routers for logging user actions.

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
