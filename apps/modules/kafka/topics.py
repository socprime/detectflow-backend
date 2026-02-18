"""Service for fetching topics from Kafka."""

import asyncio

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient

from apps.core.logger import get_logger
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


class KafkaTopicsService(BaseKafkaSyncClient):
    """Service for fetching topics from Kafka."""

    def __init__(self):
        """Initialize the topics service."""
        super().__init__(client_id="admin-panel-topic-sync")
        self._admin_client: AdminClient | None = None

    def _get_default_client_id(self) -> str:
        """Return default client ID for topics service."""
        return "admin-panel-topic-sync"

    def _get_admin_client(self) -> AdminClient:
        """Get or create AdminClient instance."""
        if self._admin_client is None:
            self._admin_client = AdminClient(self._config)
        return self._admin_client

    def _get_topics_sync(self, timeout: int = 10) -> list[str]:
        """Synchronous method to fetch topics from Kafka.

        Args:
            timeout: Timeout in seconds for Kafka operations.

        Returns:
            List of topic names from Kafka.

        Raises:
            KafkaException: If Kafka connection or operation fails.
            ValueError: If Kafka configuration is invalid.
        """
        admin_client = self._get_admin_client()

        logger.info(
            "Fetching topics from Kafka",
            extra={"bootstrap_servers": self.bootstrap_servers},
        )

        # List all topics
        metadata = admin_client.list_topics(timeout=timeout)

        if not metadata.topics:
            logger.warning("No topics found in Kafka cluster")
            return []

        all_topics = sorted(metadata.topics.keys())
        topic_names = [t for t in all_topics if not t.startswith("__")]

        logger.info(
            "Successfully fetched topics from Kafka",
            extra={"topic_count": len(topic_names)},
        )

        return topic_names

    async def get_topics(self, timeout: int = 10) -> list[str]:
        """Fetch list of topic names from Kafka (async).

        This method runs the blocking Kafka operations in a thread pool
        to avoid blocking the async event loop.

        Args:
            timeout: Timeout in seconds for Kafka operations.

        Returns:
            List of topic names from Kafka.

        Raises:
            KafkaException: If Kafka connection or operation fails.
            ValueError: If Kafka configuration is invalid.
        """
        try:
            # Run blocking Kafka operations in a thread pool
            topic_names = await asyncio.to_thread(self._get_topics_sync, timeout)
            return topic_names

        except KafkaException as e:
            logger.error(
                "Kafka error while fetching topics",
                extra={"error": str(e)},
            )
            raise
        except Exception as e:
            logger.error(
                "Unexpected error while fetching topics from Kafka",
                extra={"error": str(e)},
            )
            raise


# Singleton instance
_topics_service = KafkaTopicsService()


# Backward compatibility function
async def get_topics_from_kafka(timeout: int = 10) -> list[str]:
    """Fetch list of topic names from Kafka (async).

    This function is kept for backward compatibility.
    Use KafkaTopicsService.get_topics() for new code.

    Args:
        timeout: Timeout in seconds for Kafka operations.

    Returns:
        List of topic names from Kafka.

    Raises:
        KafkaException: If Kafka connection or operation fails.
        ValueError: If Kafka configuration is invalid.
    """
    return await _topics_service.get_topics(timeout=timeout)
