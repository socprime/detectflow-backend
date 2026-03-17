import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from confluent_kafka import Producer

from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.base import BaseKafkaAsyncClient, BaseKafkaSyncClient

logger = get_logger(__name__)


class KafkaParsersEventsReader(BaseKafkaAsyncClient):
    def __init__(self):
        super().__init__(
            group_id="admin-panel-backend-parsers-tester",
            auto_offset_reset="latest",
            enable_auto_commit=False,  # Disable auto-commit to avoid affecting offsets
        )

    def _get_default_group_id(self) -> str:
        """Return default group ID for parsers sync service."""
        return "admin-panel-backend-parsers-tester"

    async def get_events(self, topic: str, limit: int = 5) -> list[str]:
        """Get the last N events from a Kafka topic.

        This method seeks to the appropriate offset to read the last N messages
        without affecting committed offsets (auto-commit is disabled).

        Args:
            topic: Kafka topic name to read from.
            limit: Number of events to retrieve (default: 5).

        Returns:
            List of event strings (decoded from bytes), ordered from oldest to newest.
        """
        consumer = None
        try:
            consumer = AIOKafkaConsumer(topic, **self._config)
            await consumer.start()

            # seek_to_end() triggers partition assignment and positions at end
            await consumer.seek_to_end()
            partitions = consumer.assignment()

            if not partitions:
                return []

            events: list[str] = []

            # Get both beginning and end offsets to calculate valid seek position
            end_offsets = await consumer.end_offsets(partitions)
            beginning_offsets = await consumer.beginning_offsets(partitions)

            for partition in partitions:
                end_offset = end_offsets[partition]
                begin_offset = beginning_offsets[partition]

                # Skip empty partitions
                if end_offset == 0 or end_offset <= begin_offset:
                    continue

                # Use begin_offset as floor to avoid seeking to deleted messages
                seek_offset = max(begin_offset, end_offset - limit)
                consumer.seek(partition, seek_offset)

            # Use getmany with timeout so we stop when no more messages are
            # available (avoids blocking forever when topic has fewer than limit events)
            while len(events) < limit:
                batch = await consumer.getmany(timeout_ms=2000, max_records=limit)
                if not batch:
                    break
                for partition_messages in batch.values():
                    for message in partition_messages:
                        if message.value:
                            events.append(message.value)
                            if len(events) >= limit:
                                break
                    if len(events) >= limit:
                        break

            return events[-limit:] if len(events) > limit else events

        except KafkaError as e:
            logger.error("Kafka error while getting events", extra={"error": str(e), "topic": topic})
            if ErrorTracker.should_log(f"kafka_parsers_events_{topic}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="kafka",
                    entity_id=topic,
                    details=f"Kafka error while getting parser test events from topic {topic}: {str(e)}",
                    source="system",
                    severity="error",
                )
            raise
        except Exception as e:
            logger.error("Unexpected error while getting events", extra={"error": str(e), "topic": topic})
            if ErrorTracker.should_log(f"kafka_parsers_events_unexpected_{topic}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="kafka",
                    entity_id=topic,
                    details=f"Unexpected error while getting parser test events from topic {topic}: {str(e)}",
                    source="system",
                    severity="error",
                )
            raise
        finally:
            if consumer:
                await consumer.stop()


class KafkaParserPublisher(BaseKafkaSyncClient):
    """Publishes parser configs and mappings to Kafka for Flink jobs.

    Uses the same topic as rules (sigma-rules) with type="parser".
    Each pipeline has exactly one parser+mapping (through LogSource).
    """

    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-parser-sync")

    def __init__(self):
        super().__init__(client_id="admin-panel-parser-publisher")
        self.topic = settings.kafka_sigma_rules_topic

        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",
                "compression.type": "gzip",
            }
        )
        self.producer = Producer(self.producer_config)

    def _get_default_client_id(self) -> str:
        return "admin-panel-parser-publisher"

    async def send_parser_config(
        self,
        pipeline_id: str,
        log_source_id: str,
        log_source_name: str,
        parsing_config: dict[str, Any],
        mapping: str | None = None,
    ) -> None:
        """Send parser config and mapping to Kafka for a pipeline.

        Args:
            pipeline_id: Pipeline UUID string
            log_source_id: LogSource UUID string (used as parser ID)
            log_source_name: LogSource name
            parsing_config: Parser configuration dict
            mapping: LogSource mapping (optional)
        """
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            self._send_parser_config_sync,
            pipeline_id,
            log_source_id,
            log_source_name,
            parsing_config,
            mapping,
        )

    async def delete_parser(self, pipeline_id: str) -> None:
        """Delete parser from Kafka for a pipeline (tombstone).

        Args:
            pipeline_id: Pipeline UUID string
        """
        return await asyncio.get_running_loop().run_in_executor(self._executor, self._delete_parser_sync, pipeline_id)

    def _send_parser_config_sync(
        self,
        pipeline_id: str,
        log_source_id: str,
        log_source_name: str,
        parsing_config: dict[str, Any],
        mapping: str | None = None,
    ) -> None:
        def on_delivery(err, _):
            if err:
                raise RuntimeError(f"Failed to deliver parser message: {err}")

        key_bytes, value_bytes = self._prepare_message(
            pipeline_id, log_source_id, log_source_name, parsing_config, mapping
        )

        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )
        self.producer.flush(timeout=10)

        logger.info(
            "Parser sent to Kafka",
            extra={
                "pipeline_id": pipeline_id,
                "log_source_id": log_source_id,
                "log_source_name": log_source_name,
                "has_mapping": mapping is not None,
            },
        )

    def _delete_parser_sync(self, pipeline_id: str) -> None:
        def on_delivery(err, _):
            if err:
                raise RuntimeError(f"Failed to deliver parser delete message: {err}")

        # Step 1: Logical delete
        key_bytes, value_bytes = self._prepare_logical_delete_message(pipeline_id)
        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )
        self.producer.flush(timeout=10)

        # Step 2: Tombstone for compaction
        key_bytes, value_bytes = self._prepare_tombstone_message(pipeline_id)
        self.producer.produce(
            topic=self.topic,
            key=key_bytes,
            value=value_bytes,
            callback=on_delivery,
        )
        self.producer.flush(timeout=10)

        logger.info("Parser deleted from Kafka", extra={"pipeline_id": pipeline_id})

    def _prepare_message(
        self,
        pipeline_id: str,
        log_source_id: str,
        log_source_name: str,
        parsing_config: dict[str, Any],
        mapping: str | None = None,
    ) -> tuple[bytes, bytes]:
        kafka_message = {
            "type": "parser",
            "job_id": pipeline_id,
            "parser": {
                "id": log_source_id,
                "name": log_source_name,
                "config": parsing_config,
            },
            "mapping": mapping,
        }

        key_bytes = self._get_composite_key(pipeline_id)
        value_bytes = json.dumps(kafka_message, ensure_ascii=False, default=str).encode("utf-8")

        return key_bytes, value_bytes

    def _prepare_logical_delete_message(self, pipeline_id: str) -> tuple[bytes, bytes]:
        logical_delete = {
            "type": "parser",
            "job_id": pipeline_id,
            "parser": None,
            "deleted": True,
        }
        key_bytes = self._get_composite_key(pipeline_id)
        value_bytes = json.dumps(logical_delete, ensure_ascii=False).encode("utf-8")
        return key_bytes, value_bytes

    def _prepare_tombstone_message(self, pipeline_id: str) -> tuple[bytes, None]:
        key_bytes = self._get_composite_key(pipeline_id)
        return key_bytes, None

    def _get_composite_key(self, pipeline_id: str) -> bytes:
        # Key format: {pipeline_id}:parser (only one parser per pipeline)
        composite_key = f"{pipeline_id}:parser"
        return composite_key.encode("utf-8")
