from datetime import UTC, datetime, timedelta

from confluent_kafka import KafkaException

from apps.core.database import AsyncSessionLocal
from apps.core.enums import HealthCheckStatus
from apps.core.settings import settings
from apps.modules.kafka.topics import get_topics_from_kafka
from apps.modules.postgre.attack_chains import ChainsAnalysedDeadLetterDAO
from apps.services.health_check.platforms.base import BaseHealthChecker

LIST_TOPICS_TIMEOUT = 10
# Window the parked-record check reports on. Older rows stay in the table until the retention
# prune removes them, but a week-old parked record is history, not a live operator signal.
PARKED_RECORDS_WINDOW_HOURS = 24


class KafkaHealthChecker(BaseHealthChecker):
    """Health checker for Kafka: cluster connectivity, shared service topics and per-entity families.

    Broker-side only — ``BaseHealthChecker.health_check()`` takes no DB session, so the per-pipeline
    families are judged by what exists in the cluster, not by what the DB expects.
    """

    def __init__(self) -> None:
        """Register checks: Connected, shared service topics, and the per-pipeline topic families."""
        self._kafka_topics_cache: list[str] | None = None
        self.checks = {
            "Connected": self.check_connected,
            "Service Topics - Sigma rules": self.check_sigma_rules,
            "Service Topics - Activity": self.check_activity,
            "Service Topics - Metrics": self.check_metrics,
            "Service Topics - Broadcast catalogs": self.check_broadcast_catalogs,
            "Service Topics - Chains analysed (legacy singleton)": self.check_chains_analysed_legacy,
            "Service Topics - Per-pipeline families": self.check_pipeline_topic_families,
            "Chains analysed - Parked records": self.check_chains_analysed_dead_letters,
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

    async def check_broadcast_catalogs(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check the shared Active Threat / High-Order Sigma catalogue broadcasts.

        Vendor data, shared by every correlation pipeline (Decision 4) — the only genuinely shared
        correlation inputs left once chains-config went per-pipeline.
        """
        try:
            topics = set(await self._get_topics())
        except KafkaException as e:
            return HealthCheckStatus.ERROR, [f"Failed to get topics: {e}"]
        required = [settings.kafka_active_threats_topic, settings.kafka_hos_rules_topic]
        missing = [topic for topic in required if topic not in topics]
        if missing:
            return HealthCheckStatus.ERROR, [f"Missing catalogue topic(s): {', '.join(missing)}"]
        return HealthCheckStatus.OPERATIONAL, [f"Catalogue topics exist: {', '.join(required)}"]

    async def check_chains_analysed_legacy(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check the pre-epic singleton ``detectflow.chains-analysed``, while it is still in use.

        "In use" is ``KAFKA_CHAINS_ANALYSED_INCLUDE_LEGACY_TOPIC`` — deliberately the **same switch**
        that decides whether ``ChainsAnalysedConsumer``'s pattern subscription still matches the
        singleton. Turning the consumer off that topic while this check kept demanding it would
        report ERROR for a topic nobody reads or writes any more.

        Once per-correlation-pipeline ``chains-analysed.<id>`` topics exist the singleton is
        obsolete either way, and a fully migrated cluster must not report a permanent ERROR. That
        part has to resolve at run time: the registry is built in ``__init__``, before any broker
        call can say which world we are in.
        """
        if not settings.kafka_chains_analysed_include_legacy_topic:
            return HealthCheckStatus.NOT_ENABLED, ["The chains-analysed consumer no longer subscribes to the singleton"]
        try:
            topics = await self._get_topics()
        except KafkaException as e:
            return HealthCheckStatus.ERROR, [f"Failed to get topics: {e}"]
        scoped = _scoped_topics(topics, settings.kafka_chains_analysed_topic)
        if scoped:
            return HealthCheckStatus.NOT_ENABLED, [
                f"Superseded by {len(scoped)} per-correlation-pipeline chains-analysed topic(s)"
            ]
        return await self._topic_exists(settings.kafka_chains_analysed_topic)

    async def check_chains_analysed_dead_letters(self) -> tuple[HealthCheckStatus, list[str]]:
        """Report chains-analysed records the consumer parked and skipped instead of applying.

        The consumer never blocks a partition on a poison record, so nothing else would tell an
        operator that a customer's chain was dropped. Deliberately NOT on the public ``/health``
        route: this opens its own session (like ``PostgreSQLHealthChecker``) and runs on the
        scheduled / ``check_now`` path only, so ``/health`` keeps issuing zero DB queries.
        """
        since = datetime.now(UTC) - timedelta(hours=PARKED_RECORDS_WINDOW_HOURS)
        async with AsyncSessionLocal() as db:
            parked = await ChainsAnalysedDeadLetterDAO(db).count_unreplayed(since=since)
        if parked:
            return HealthCheckStatus.WARNING, [
                f"{parked} unprocessable chains-analysed record(s) parked in the last "
                f"{PARKED_RECORDS_WINDOW_HOURS}h; inspect chains_analysed_dead_letters"
            ]
        return HealthCheckStatus.OPERATIONAL, [f"No records parked in the last {PARKED_RECORDS_WINDOW_HOURS}h"]

    async def check_pipeline_topic_families(self) -> tuple[HealthCheckStatus, list[str]]:
        """Count the per-entity topic families and flag any incomplete correlation-pipeline triple.

        A correlation pipeline owns chains-config + chains-raw + chains-analysed; a missing member
        is exactly the failure an interrupted provision or a hand-run ``delete_topics`` produces.
        Zero dynamic topics is OPERATIONAL — a pre-epic cluster is not unhealthy.
        """
        try:
            topics = await self._get_topics()
        except KafkaException as e:
            return HealthCheckStatus.ERROR, [f"Failed to get topics: {e}"]

        tagged = _scoped_topics(topics, settings.kafka_tagged_events_topic)
        config = _scoped_topics(topics, settings.kafka_chains_config_topic)
        raw = _scoped_topics(topics, settings.kafka_chains_raw_topic)
        analysed = _scoped_topics(topics, settings.kafka_chains_analysed_topic)

        descriptions = [
            f"tagged-events: {len(tagged)}",
            f"chains-config: {len(config)}",
            f"chains-raw: {len(raw)}",
            f"chains-analysed: {len(analysed)}",
        ]

        families = [("chains-config", config), ("chains-raw", raw), ("chains-analysed", analysed)]
        incomplete = sorted((config | raw | analysed) - (config & raw & analysed))
        if incomplete:
            missing = [
                f"{entity_id} (missing {', '.join(label for label, ids in families if entity_id not in ids)})"
                for entity_id in incomplete
            ]
            return HealthCheckStatus.WARNING, [*descriptions, f"Incomplete correlation triple: {'; '.join(missing)}"]
        return HealthCheckStatus.OPERATIONAL, descriptions


def _scoped_topics(topics: list[str], prefix: str) -> set[str]:
    """Entity ids of the ``<prefix>.<entity_id>`` topics present in the cluster."""
    marker = f"{prefix}."
    return {topic[len(marker) :] for topic in topics if topic.startswith(marker)}
