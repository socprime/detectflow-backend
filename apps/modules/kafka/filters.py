"""Kafka Filters Sync Service.

Publishes filters to Kafka for Flink jobs to consume.
Uses the same topic as rules (sigma-rules) but with type="filter" to distinguish.
"""

import asyncio
import json
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Producer

from apps.core.logger import get_logger
from apps.core.models import Filter
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


class KafkaFiltersSyncService(BaseKafkaSyncClient):
    """Service for publishing filters to Kafka.

    Filters use the same topic as rules (sigma-rules) but are distinguished
    by the "type": "filter" field in the message payload.

    Message format:
        {
            "type": "filter",
            "job_id": "{pipeline_id}",
            "filter_id": "{filter_id}",
            "filter_body": "{filter_body}"
        }

    The Kafka message key format is: {pipeline_id}:prefilter:{filter_id}
    to ensure no collision with rule keys.
    """

    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-filters-sync")

    def __init__(self):
        super().__init__(client_id="admin-panel-backend-filters")
        # Use same topic as rules - filters are distinguished by "type" field
        self.filters_topic = settings.kafka_sigma_rules_topic

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
        """Return default client ID for filters sync service."""
        return "admin-panel-backend-filters"

    async def send_filters(
        self,
        pipeline_id: str,
        filters: list[Filter],
        batch_size: int = 50,
    ) -> None:
        """Send filters to Kafka for a specific pipeline.

        Args:
            pipeline_id: Pipeline ID (used as job_id in Flink)
            filters: List of Filter objects to publish
            batch_size: Number of messages before flushing
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._send_filters_sync, pipeline_id, filters, batch_size
        )

    async def send_filter(self, pipeline_id: str, filter_obj: Filter) -> None:
        """Send a single filter to Kafka.

        Args:
            pipeline_id: Pipeline ID
            filter_obj: Filter object to publish
        """
        return await self.send_filters(pipeline_id, [filter_obj])

    async def delete_filters(self, pipeline_id: str, filter_ids: list[str]) -> None:
        """Delete filters from Kafka (logical delete + tombstone).

        Args:
            pipeline_id: Pipeline ID
            filter_ids: List of filter IDs to delete
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._delete_filters_sync, pipeline_id, filter_ids
        )

    async def delete_filter(self, pipeline_id: str, filter_id: str) -> None:
        """Delete a single filter from Kafka.

        Args:
            pipeline_id: Pipeline ID
            filter_id: Filter ID to delete
        """
        return await self.delete_filters(pipeline_id, [filter_id])

    def _send_filters_sync(
        self,
        pipeline_id: str,
        filters: list[Filter],
        batch_size: int = 50,
    ) -> None:
        """Synchronous implementation of filter publishing."""

        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver filter message: {err}")
                raise RuntimeError(f"Failed to deliver message: {err}")

        logger.info(f"Sending {len(filters)} filters to Kafka for pipeline {pipeline_id}")

        for idx, filter_obj in enumerate(filters, start=1):
            key_bytes, value_bytes = self._prepare_message(pipeline_id, filter_obj)

            self.producer.produce(
                topic=self.filters_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

            if idx % batch_size == 0:
                self.producer.flush(timeout=10)

        self.producer.flush(timeout=10)
        logger.info(f"Successfully sent {len(filters)} filters for pipeline {pipeline_id}")

    def _delete_filters_sync(self, pipeline_id: str, filter_ids: list[str]) -> None:
        """Synchronous implementation of filter deletion.

        Two-step deletion:
        1. Send logical delete message with "deleted": True flag
        2. Send tombstone (null value)
        """

        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver delete message: {err}")
                raise RuntimeError(f"Failed to deliver message: {err}")

        logger.info(f"Deleting {len(filter_ids)} filters from Kafka for pipeline {pipeline_id}")

        # Step 1: Logical delete
        for filter_id in filter_ids:
            key_bytes, value_bytes = self._prepare_logical_delete_message(pipeline_id, filter_id)

            self.producer.produce(
                topic=self.filters_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

        self.producer.flush(timeout=10)

        # Step 2: Tombstone (null value for compaction)
        for filter_id in filter_ids:
            key_bytes, value_bytes = self._prepare_tombstone_message(pipeline_id, filter_id)

            self.producer.produce(
                topic=self.filters_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

        self.producer.flush(timeout=10)
        logger.info(f"Successfully deleted {len(filter_ids)} filters for pipeline {pipeline_id}")

    def _prepare_message(self, pipeline_id: str, filter_obj: Filter) -> tuple[bytes, bytes]:
        """Prepare Kafka message for a filter.

        Returns:
            Tuple of (key_bytes, value_bytes)
        """
        kafka_filter = {
            "type": "filter",  # Distinguishes from rules in same topic
            "job_id": pipeline_id,
            "filter_id": str(filter_obj.id),
            "filter_body": filter_obj.body,
        }

        filter_id = str(filter_obj.id)
        key_bytes = self._get_composite_key(pipeline_id, filter_id)
        value_bytes = json.dumps(kafka_filter, ensure_ascii=False, default=str).encode("utf-8")

        return key_bytes, value_bytes

    def _prepare_logical_delete_message(self, pipeline_id: str, filter_id: str) -> tuple[bytes, bytes]:
        """Prepare logical delete message with "deleted": True flag."""
        logical_delete_message = {
            "type": "filter",
            "job_id": pipeline_id,
            "filter_id": filter_id,
            "deleted": True,
        }
        key_bytes = self._get_composite_key(pipeline_id, filter_id)
        value_bytes = json.dumps(logical_delete_message, ensure_ascii=False, default=str).encode("utf-8")
        return key_bytes, value_bytes

    def _prepare_tombstone_message(self, pipeline_id: str, filter_id: str) -> tuple[bytes, None]:
        """Prepare tombstone message (null value for compaction)."""
        key_bytes = self._get_composite_key(pipeline_id, filter_id)
        return key_bytes, None

    def _get_composite_key(self, pipeline_id: str, filter_id: str) -> bytes:
        """Create composite key for Kafka message.

        Format: {pipeline_id}:prefilter:{filter_id}
        The 'prefilter' prefix ensures no collision with rule keys.
        """
        composite_key = f"{pipeline_id}:prefilter:{filter_id}"
        return composite_key.encode("utf-8")


# Singleton instance
_kafka_filters_service: KafkaFiltersSyncService | None = None


def get_kafka_filters_service() -> KafkaFiltersSyncService:
    """Get or create singleton instance of KafkaFiltersSyncService."""
    global _kafka_filters_service
    if _kafka_filters_service is None:
        _kafka_filters_service = KafkaFiltersSyncService()
    return _kafka_filters_service
