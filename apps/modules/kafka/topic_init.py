"""Kafka topic initialization service.

This module ensures the **static, shared** Kafka topics exist at application startup, similar to
how Alembic ensures database schema is up to date. The per-entity families that come and go with
rows in `pipelines` / `correlation_pipelines` are provisioned at runtime instead — see
`apps/modules/kafka/topic_lifecycle.py`, which owns the `TopicSpec` and `NewTopic` primitives this
module reuses.

Usage from entrypoint.sh:
    python -m apps.modules.kafka.topic_init
"""

import sys

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.base import KafkaConfigBuilder
from apps.modules.kafka.topic_lifecycle import (
    CHAINS_RETENTION_MS,
    COMPACTED_RETENTION_MS,
    TAGGED_EVENTS_RETENTION_MS,
    TopicSpec,
    build_new_topic,
    validate_existing_topic_partitions,
)

logger = get_logger(__name__)


# Required topics with their configurations.
#
# Partition count, cleanup policy and retention for every `detectflow.*`
# topic are set EXPLICITLY here — they are part of the cross-repo wire
# contract with detectflow-agent. Do NOT replace any of them with
# `settings.kafka_default_partitions`: a future change to the default would
# silently violate the contract.
#
# The four `detectflow.{tagged-events,chains-*}` entries are the **legacy
# singletons**: pre-epic rows still ride them (`pipelines.tagged_events_topic
# IS NULL` means "still on the shared topic"), so they stay provisioned here
# and are on `topic_lifecycle.shared_topics()` — the lifecycle service may
# never touch them.
REQUIRED_TOPICS: list[TopicSpec] = [
    # ---- sigma matcher (compacted broadcasts) ----
    TopicSpec(
        name=settings.kafka_sigma_rules_topic,
        partitions=1,
        cleanup_policy="compact",
        retention_ms=COMPACTED_RETENTION_MS,  # infinite retention for compacted topic
    ),
    TopicSpec(
        name=settings.kafka_active_threats_topic,
        partitions=1,  # compacted shared catalogue broadcast
        cleanup_policy="compact",
        retention_ms=COMPACTED_RETENTION_MS,
    ),
    TopicSpec(
        name=settings.kafka_hos_rules_topic,
        partitions=1,  # compacted shared catalogue broadcast
        cleanup_policy="compact",
        retention_ms=COMPACTED_RETENTION_MS,
    ),
    # ---- legacy singletons (superseded per-entity by topic_lifecycle) ----
    TopicSpec(
        name=settings.kafka_chains_config_topic,
        partitions=1,  # compacted singleton, key "global"
        cleanup_policy="compact",
        retention_ms=COMPACTED_RETENTION_MS,
    ),
    TopicSpec(
        name=settings.kafka_tagged_events_topic,
        partitions=settings.kafka_tagged_events_partitions,  # sized for tagged-event throughput
        cleanup_policy="delete",
        retention_ms=TAGGED_EVENTS_RETENTION_MS,
    ),
    TopicSpec(
        name=settings.kafka_chains_raw_topic,
        partitions=12,
        cleanup_policy="delete",  # monotonic revision envelopes, NOT compacted
        retention_ms=CHAINS_RETENTION_MS,
    ),
    TopicSpec(
        name=settings.kafka_chains_analysed_topic,
        partitions=12,
        cleanup_policy="delete",
        retention_ms=CHAINS_RETENTION_MS,
    ),
    # ---- ops topics (not in detectflow.* wire contract) ----
    TopicSpec(
        name=settings.kafka_activity_topic,
        partitions=settings.kafka_default_partitions,
        cleanup_policy="delete",
        retention_ms=CHAINS_RETENTION_MS,  # 7 days
    ),
    TopicSpec(
        name=settings.kafka_metrics_topic,
        partitions=settings.kafka_default_partitions,
        cleanup_policy="delete",
        retention_ms=CHAINS_RETENTION_MS,  # 7 days
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
        topics_to_create = []
        created_names: list[str] = []
        existing_names: list[str] = []

        for spec in REQUIRED_TOPICS:
            if spec.name in existing_topics:
                existing_names.append(spec.name)
                logger.debug(f"Topic already exists: {spec.name}")
                validate_existing_topic_partitions(admin_client, spec.name, spec.partitions)
                continue

            new_topic = build_new_topic(spec)
            topics_to_create.append(new_topic)
            created_names.append(spec.name)

            logger.info(
                f"Will create topic: {spec.name}",
                extra={
                    "partitions": spec.partitions,
                    "replication_factor": spec.replication_factor or settings.kafka_default_replication_factor,
                    "cleanup_policy": spec.cleanup_policy,
                    "retention_ms": spec.retention_ms,
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
