"""Kafka Custom Fields Sync Service.

Publishes custom fields to Kafka for Flink jobs to consume.
Uses the same topic as rules (sigma-rules) but with type="custom_fields" to distinguish.
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Producer

from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


class KafkaCustomFieldsSyncService(BaseKafkaSyncClient):
    """Service for publishing custom fields to Kafka.

    Custom fields use the same topic as rules (sigma-rules) but are distinguished
    by the "type": "custom_fields" field in the message payload.

    Message format:
        {
            "type": "custom_fields",
            "job_id": "{pipeline_id}",
            "custom_fields": "{yaml_string}"
        }

    The Kafka message key format is: {pipeline_id}:custom_fields
    (one key per pipeline since custom_fields is a single value per pipeline)
    """

    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-custom-fields-sync")

    def __init__(self):
        super().__init__(client_id="admin-panel-backend-custom-fields")
        # Use same topic as rules - custom fields are distinguished by "type" field
        self.topic = settings.kafka_sigma_rules_topic

        # Producer config
        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",
                "compression.type": "gzip",
            }
        )
        self.producer = Producer(self.producer_config)

    def _get_default_client_id(self) -> str:
        """Return default client ID for custom fields sync service."""
        return "admin-panel-backend-custom-fields"

    async def send_custom_fields(self, pipeline_id: str, custom_fields: str) -> None:
        """Send custom fields to Kafka for a specific pipeline.

        Args:
            pipeline_id: Pipeline ID (used as job_id in Flink)
            custom_fields: YAML string with custom fields
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._send_custom_fields_sync, pipeline_id, custom_fields
        )

    async def delete_custom_fields(self, pipeline_id: str) -> None:
        """Delete custom fields from Kafka (logical delete + tombstone).

        Args:
            pipeline_id: Pipeline ID
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._delete_custom_fields_sync, pipeline_id
        )

    def _send_custom_fields_sync(self, pipeline_id: str, custom_fields: str) -> None:
        """Synchronous implementation of custom fields publishing."""

        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver custom_fields message: {err}")
                raise RuntimeError(f"Failed to deliver message: {err}")

        logger.info(f"Sending custom_fields to Kafka for pipeline {pipeline_id}")

        key_bytes, value_bytes = self._prepare_message(pipeline_id, custom_fields)

        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )

        self.producer.flush(timeout=10)
        logger.info(f"Successfully sent custom_fields for pipeline {pipeline_id}")

    def _delete_custom_fields_sync(self, pipeline_id: str) -> None:
        """Synchronous implementation of custom fields deletion.

        Two-step deletion:
        1. Send logical delete message with "deleted": True flag
        2. Send tombstone (null value)
        """

        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver delete message: {err}")
                raise RuntimeError(f"Failed to deliver message: {err}")

        logger.info(f"Deleting custom_fields from Kafka for pipeline {pipeline_id}")

        # Step 1: Logical delete
        key_bytes, value_bytes = self._prepare_logical_delete_message(pipeline_id)

        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )

        self.producer.flush(timeout=10)

        # Step 2: Tombstone (null value for compaction)
        key_bytes, value_bytes = self._prepare_tombstone_message(pipeline_id)

        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )

        self.producer.flush(timeout=10)
        logger.info(f"Successfully deleted custom_fields for pipeline {pipeline_id}")

    def _prepare_message(self, pipeline_id: str, custom_fields: str) -> tuple[bytes, bytes]:
        """Prepare Kafka message for custom fields.

        Returns:
            Tuple of (key_bytes, value_bytes)
        """
        kafka_message = {
            "type": "custom_fields",
            "job_id": pipeline_id,
            "custom_fields": custom_fields,
        }

        key_bytes = self._get_key(pipeline_id)
        value_bytes = json.dumps(kafka_message, ensure_ascii=False, default=str).encode("utf-8")

        return key_bytes, value_bytes

    def _prepare_logical_delete_message(self, pipeline_id: str) -> tuple[bytes, bytes]:
        """Prepare logical delete message with "deleted": True flag."""
        logical_delete_message = {
            "type": "custom_fields",
            "job_id": pipeline_id,
            "deleted": True,
        }
        key_bytes = self._get_key(pipeline_id)
        value_bytes = json.dumps(logical_delete_message, ensure_ascii=False, default=str).encode("utf-8")
        return key_bytes, value_bytes

    def _prepare_tombstone_message(self, pipeline_id: str) -> tuple[bytes, None]:
        """Prepare tombstone message (null value for compaction)."""
        key_bytes = self._get_key(pipeline_id)
        return key_bytes, None

    def _get_key(self, pipeline_id: str) -> bytes:
        """Create key for Kafka message.

        Format: {pipeline_id}:custom_fields
        """
        key = f"{pipeline_id}:custom_fields"
        return key.encode("utf-8")


# Singleton instance
_kafka_custom_fields_service: KafkaCustomFieldsSyncService | None = None


def get_kafka_custom_fields_service() -> KafkaCustomFieldsSyncService:
    """Get or create singleton instance of KafkaCustomFieldsSyncService."""
    global _kafka_custom_fields_service
    if _kafka_custom_fields_service is None:
        _kafka_custom_fields_service = KafkaCustomFieldsSyncService()
    return _kafka_custom_fields_service
