"""Kafka topic initialization service.

This module ensures required Kafka topics exist at application startup,
similar to how Alembic ensures database schema is up to date.

Usage from entrypoint.sh:
    python -m apps.modules.kafka.topic_init
"""

import sys
from dataclasses import dataclass

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.base import KafkaConfigBuilder

logger = get_logger(__name__)


@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""

    name: str
    partitions: int | None = None  # None = use default
    replication_factor: int | None = None  # None = use default
    cleanup_policy: str = "delete"  # "delete" or "compact"
    retention_ms: int | None = None  # None = broker default, -1 = infinite


# Required topics with their configurations
REQUIRED_TOPICS: list[TopicConfig] = [
    # sigma-rules: compacted topic for rules, filters, parsers, custom fields
    # Uses infinite retention to preserve all active rules
    TopicConfig(
        name=settings.kafka_sigma_rules_topic,
        cleanup_policy="compact",
        retention_ms=-1,  # Infinite retention for compacted topic
    ),
    # etl-activity: activity/audit events
    TopicConfig(
        name=settings.kafka_activity_topic,
        cleanup_policy="delete",
        retention_ms=604800000,  # 7 days
    ),
    # rule-statistics: metrics from Flink jobs
    TopicConfig(
        name=settings.kafka_metrics_topic,
        cleanup_policy="delete",
        retention_ms=604800000,  # 7 days
    ),
]


class KafkaTopicInitializer:
    """Service for initializing required Kafka topics at startup."""

    def __init__(self):
        """Initialize the topic initializer."""
        self._config = KafkaConfigBuilder.build_confluent_config(client_id="admin-panel-topic-init")
        self._admin_client: AdminClient | None = None

    def _get_admin_client(self) -> AdminClient:
        """Get or create AdminClient instance."""
        if self._admin_client is None:
            self._admin_client = AdminClient(self._config)
        return self._admin_client

    def _get_existing_topics(self, timeout: int = 10) -> set[str]:
        """Get set of existing topic names from Kafka."""
        admin_client = self._get_admin_client()
        metadata = admin_client.list_topics(timeout=timeout)
        return set(metadata.topics.keys()) if metadata.topics else set()

    def initialize_sync(self, timeout: int = 30) -> tuple[list[str], list[str]]:
        """Synchronously check and create missing topics.

        Returns:
            Tuple of (created_topics, existing_topics)
        """
        admin_client = self._get_admin_client()

        # Get existing topics
        existing_topics = self._get_existing_topics(timeout=timeout)
        logger.info(
            "Checking required Kafka topics",
            extra={
                "existing_count": len(existing_topics),
                "required_count": len(REQUIRED_TOPICS),
            },
        )

        # Find missing topics
        topics_to_create: list[NewTopic] = []
        created_names: list[str] = []
        existing_names: list[str] = []

        for topic_config in REQUIRED_TOPICS:
            if topic_config.name in existing_topics:
                existing_names.append(topic_config.name)
                logger.debug(f"Topic already exists: {topic_config.name}")
                continue

            # Prepare topic configuration
            partitions = topic_config.partitions or settings.kafka_default_partitions
            replication = topic_config.replication_factor or settings.kafka_default_replication_factor

            # Build topic config dict
            config = {}
            if topic_config.cleanup_policy:
                config["cleanup.policy"] = topic_config.cleanup_policy
            if topic_config.retention_ms is not None:
                config["retention.ms"] = str(topic_config.retention_ms)

            new_topic = NewTopic(
                topic=topic_config.name,
                num_partitions=partitions,
                replication_factor=replication,
                config=config,
            )
            topics_to_create.append(new_topic)
            created_names.append(topic_config.name)

            logger.info(
                f"Will create topic: {topic_config.name}",
                extra={
                    "partitions": partitions,
                    "replication_factor": replication,
                    "cleanup_policy": topic_config.cleanup_policy,
                    "retention_ms": topic_config.retention_ms,
                },
            )

        if not topics_to_create:
            logger.info("All required Kafka topics already exist")
            return [], existing_names

        # Create missing topics
        logger.info(f"Creating {len(topics_to_create)} Kafka topic(s)...")

        futures = admin_client.create_topics(topics_to_create, request_timeout=timeout)

        # Wait for each topic creation to complete
        for topic_name, future in futures.items():
            try:
                future.result()  # Blocks until topic is created
                logger.info(f"Successfully created topic: {topic_name}")
            except KafkaException as e:
                # Check if topic already exists (race condition)
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    logger.warning(f"Topic {topic_name} was created by another process")
                else:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    raise

        return created_names, existing_names

    def close(self):
        """Close the admin client connection."""
        if self._admin_client is not None:
            self._admin_client = None


def main() -> int:
    """CLI entry point for Kafka topic initialization.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print("Initializing Kafka topics...")

    initializer = KafkaTopicInitializer()
    try:
        created, existing = initializer.initialize_sync(timeout=30)

        if existing:
            print(f"Topics already exist: {', '.join(existing)}")
        if created:
            print(f"Created topics: {', '.join(created)}")

        print("Kafka topic initialization complete!")
        return 0

    except KafkaException as e:
        print(f"ERROR: Kafka error during topic initialization: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"ERROR: Failed to initialize Kafka topics: {e}", file=sys.stderr)
        return 1
    finally:
        initializer.close()


if __name__ == "__main__":
    sys.exit(main())
