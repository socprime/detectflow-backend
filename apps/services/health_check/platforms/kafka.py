from confluent_kafka import KafkaException

from apps.core.enums import HealthCheckStatus
from apps.core.settings import settings
from apps.modules.kafka.topics import get_topics_from_kafka
from apps.services.health_check.platforms.base import BaseHealthChecker

LIST_TOPICS_TIMEOUT = 10


class KafkaHealthChecker(BaseHealthChecker):
    """Health checker for Kafka: cluster connectivity and required service topics."""

    def __init__(self) -> None:
        """Register checks: Connected, Service Topics (Sigma rules, Activity, Metrics)."""
        self._kafka_topics_cache: list[str] | None = None
        self.checks = {
            "Connected": self.check_connected,
            "Service Topics - Sigma rules": self.check_sigma_rules,
            "Service Topics - Activity": self.check_activity,
            "Service Topics - Metrics": self.check_metrics,
        }

    async def _get_topics(self) -> list[str]:
        """Return list of topic names; use cache if already fetched in this run."""
        if self._kafka_topics_cache is not None:
            return self._kafka_topics_cache
        topics = await get_topics_from_kafka(timeout=LIST_TOPICS_TIMEOUT)
        self._kafka_topics_cache = topics
        return topics

    async def check_connected(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if Kafka cluster is reachable (list_topics). Clears topics cache so this run uses fresh data."""
        self._kafka_topics_cache = None  # Clear cache so each health_check run uses fresh data
        try:
            await self._get_topics()
        except KafkaException as e:
            return HealthCheckStatus.ERROR, [f"Failed to connect to Kafka cluster: {e}"]
        return HealthCheckStatus.OPERATIONAL, ["Successfully connected to Kafka cluster"]

    async def _topic_exists(self, topic: str) -> tuple[HealthCheckStatus, list[str]]:
        """Check if the given topic exists in the cluster. Returns (OPERATIONAL, ['Topic exists']) or (ERROR, ['Topic does not exist'])."""
        try:
            topics = await self._get_topics()
        except KafkaException as e:
            return HealthCheckStatus.ERROR, [f"Failed to get topics: {e}"]
        if topic in topics:
            return HealthCheckStatus.OPERATIONAL, ["Topic exists"]
        return HealthCheckStatus.ERROR, ["Topic does not exist"]

    async def check_sigma_rules(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if Sigma rules topic (from settings) exists."""
        return await self._topic_exists(settings.kafka_sigma_rules_topic)

    async def check_activity(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if Activity topic (from settings) exists."""
        return await self._topic_exists(settings.kafka_activity_topic)

    async def check_metrics(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if Metrics topic (from settings) exists."""
        return await self._topic_exists(settings.kafka_metrics_topic)
