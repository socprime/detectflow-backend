"""On-demand Kafka topic lifecycle for per-pipeline topic families.

``topic_init.py`` provisions the static, shared topics once at boot. This module provisions and
retires the per-entity families that come and go with rows in ``pipelines`` and
``correlation_pipelines``. Both use the same explicitness contract: partitions, ``cleanup.policy``
and ``retention.ms`` are always stated, never inherited from broker defaults.

Names are built by the pure helpers in ``apps/core/constants.py``; this module only injects the
settings prefix and enforces Kafka's name legality. It is the **only** code allowed to create,
alter or delete a per-entity topic — managers call ``ensure_*`` / ``retire_*`` and never build an
``AdminClient`` of their own.

Retiring is governed by ``settings.kafka_topic_delete_policy``:

* ``keep``   — leave the topic untouched (external archival / strict retention regimes).
* ``retire`` — the default: set ``cleanup.policy=delete`` **and**
  ``retention.ms=settings.kafka_retired_topic_retention_ms``, then let the broker reclaim the disk
  on its own. Both are required: on a compacted topic, lowering retention purges nothing until the
  cleanup policy includes ``delete``.
* ``delete`` — irreversible ``AdminClient.delete_topics``. Downgrades itself to ``retire`` when a
  consumer still needs the data.

Undo a ``retire`` inside the grace window (default 24h) with::

    kafka-configs --bootstrap-server <broker> --alter --entity-type topics \\
      --entity-name detectflow.chains-raw.<corr_id> \\
      --add-config retention.ms=604800000,cleanup.policy=delete

Shared topics (``detectflow.sigma-rules``, ``detectflow.active-threats``, ``detectflow.hos-rules``,
``detectflow.activity``, ``detectflow.rule-metrics`` and the pre-epic singletons) are vendor/ops
data, never customer data — see ``shared_topics()``. Nothing here may touch them.
"""

import asyncio
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from confluent_kafka import ConsumerGroupState, ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource, NewTopic, OffsetSpec

from apps.core.constants import (
    chains_analysed_topic,
    chains_config_topic,
    chains_raw_topic,
    tagged_events_topic,
)
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)

# Kafka legal topic name: [a-zA-Z0-9._-], 1..249 chars, and not "." / "..".
MAX_TOPIC_NAME_LENGTH = 249
TOPIC_NAME_RE = re.compile(rf"^[a-zA-Z0-9._\-]{{1,{MAX_TOPIC_NAME_LENGTH}}}$")
# The ``<entity_id>`` half of ``<prefix>.<entity_id>``. It must start with an alphanumeric, so a
# malformed id ("", ".", "...") can never compose something that merely looks like a per-entity
# topic — and can never collapse a family back onto its bare shared prefix.
ENTITY_ID_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._\-]*$")

# Retention windows shared with the legacy singletons in `topic_init.REQUIRED_TOPICS`.
TAGGED_EVENTS_RETENTION_MS = 43_200_000  # 12 hours
CHAINS_RETENTION_MS = 604_800_000  # 7 days
COMPACTED_RETENTION_MS = -1  # infinite

ADMIN_TIMEOUT_SECONDS = 30
DRAIN_POLL_INTERVAL_SECONDS = 1.0

TopicRetireAction = Literal["kept", "retired", "deleted"]

# A consumer group in one of these states has live members; EMPTY/DEAD/UNKNOWN do not.
_LIVE_GROUP_STATES = frozenset(
    {
        ConsumerGroupState.STABLE,
        ConsumerGroupState.PREPARING_REBALANCING,
        ConsumerGroupState.COMPLETING_REBALANCING,
    }
)


class TopicProvisioningError(Exception):
    """Raised when a topic create/retire/delete could not be completed."""


@dataclass(frozen=True)
class TopicSpec:
    """Full wire spec for one Kafka topic — no field may be left to broker defaults."""

    name: str
    partitions: int
    cleanup_policy: str  # "delete" | "compact"
    retention_ms: int  # -1 = infinite
    replication_factor: int | None = None  # None -> settings.kafka_default_replication_factor

    def to_topic_config(self) -> dict[str, str]:
        """Kafka topic config for this spec — both keys are always present by contract."""
        return {"cleanup.policy": self.cleanup_policy, "retention.ms": str(self.retention_ms)}


@dataclass(frozen=True)
class TopicRetirementOutcome:
    """What actually happened to one topic when its owning row was deleted.

    ``action`` may differ from ``requested_policy``: a ``delete`` downgrades itself to ``retire``
    when a consumer still needs the data, and ``reason`` then names the consumer group(s) or the
    residual lag. Managers surface that as an ``activity(severity="warning")`` record.
    """

    topic: str
    requested_policy: str
    action: TopicRetireAction
    reason: str | None = None


@dataclass(frozen=True)
class _GroupTopicState:
    """One consumer group's relationship to one topic: how far behind, and is anyone attached."""

    group_id: str
    lag: int
    live: bool


def shared_topics() -> frozenset[str]:
    """Topics this service must NEVER create, alter or delete (Decision 4 + ops topics)."""
    return frozenset(
        {
            settings.kafka_sigma_rules_topic,
            settings.kafka_active_threats_topic,
            settings.kafka_hos_rules_topic,
            settings.kafka_activity_topic,
            settings.kafka_metrics_topic,
            # legacy singletons — still owned by topic_init while pre-epic rows exist
            settings.kafka_tagged_events_topic,
            settings.kafka_chains_config_topic,
            settings.kafka_chains_raw_topic,
            settings.kafka_chains_analysed_topic,
        }
    )


def family_prefixes() -> tuple[str, ...]:
    """Prefixes of the four per-entity families, as ``<prefix>.<entity_id>``."""
    return (
        settings.kafka_tagged_events_topic,
        settings.kafka_chains_config_topic,
        settings.kafka_chains_raw_topic,
        settings.kafka_chains_analysed_topic,
    )


def validate_topic_name(name: str) -> str:
    """Reject anything Kafka would refuse, and anything that is not a per-entity topic.

    Args:
        name: Composed topic name.

    Returns:
        The name, unchanged.

    Raises:
        TopicProvisioningError: If the name is not a legal Kafka topic name, or is a shared topic.
    """
    if name in (".", "..") or not TOPIC_NAME_RE.match(name):
        raise TopicProvisioningError(f"Illegal Kafka topic name: {name!r}")
    if name in shared_topics():
        raise TopicProvisioningError(f"Refusing to manage shared topic: {name!r}")
    return name


def _scoped_topic_name(prefix: str, entity_id: UUID | str, builder: Callable[[str, str], str]) -> str:
    """Compose and validate ``<prefix>.<entity_id>`` for one family."""
    raw = str(entity_id).strip()
    if not ENTITY_ID_RE.match(raw):
        raise TopicProvisioningError(f"Illegal entity id for a per-entity topic: {entity_id!r}")
    try:
        name = builder(prefix, raw)
    except ValueError as e:  # empty settings prefix — a misconfiguration, not a default
        raise TopicProvisioningError(str(e)) from e
    return validate_topic_name(name)


def tagged_events_topic_for(pipeline_id: UUID | str) -> str:
    """``detectflow.tagged-events.<pipeline_id>`` — Decision 1, one per detection pipeline."""
    return _scoped_topic_name(settings.kafka_tagged_events_topic, pipeline_id, tagged_events_topic)


def chains_config_topic_for(correlation_pipeline_id: UUID | str) -> str:
    """``detectflow.chains-config.<corr_id>`` — compacted broadcast, key ``"global"``."""
    return _scoped_topic_name(settings.kafka_chains_config_topic, correlation_pipeline_id, chains_config_topic)


def chains_raw_topic_for(correlation_pipeline_id: UUID | str) -> str:
    """``detectflow.chains-raw.<corr_id>`` — that correlation-job → that agent-job."""
    return _scoped_topic_name(settings.kafka_chains_raw_topic, correlation_pipeline_id, chains_raw_topic)


def chains_analysed_topic_for(correlation_pipeline_id: UUID | str) -> str:
    """``detectflow.chains-analysed.<corr_id>`` — that agent-job → this control plane."""
    return _scoped_topic_name(settings.kafka_chains_analysed_topic, correlation_pipeline_id, chains_analysed_topic)


def pipeline_topic_specs(pipeline_id: UUID | str) -> list[TopicSpec]:
    """Wire spec of every topic owned by one detection pipeline."""
    return [
        TopicSpec(
            name=tagged_events_topic_for(pipeline_id),
            partitions=settings.kafka_pipeline_tagged_events_partitions,
            cleanup_policy="delete",
            retention_ms=TAGGED_EVENTS_RETENTION_MS,
        )
    ]


def correlation_pipeline_topic_specs(correlation_pipeline_id: UUID | str) -> list[TopicSpec]:
    """Wire spec of every topic owned by one correlation pipeline."""
    return [
        TopicSpec(
            name=chains_config_topic_for(correlation_pipeline_id),
            partitions=1,  # compacted broadcast, single key "global"
            cleanup_policy="compact",
            retention_ms=COMPACTED_RETENTION_MS,
        ),
        TopicSpec(
            name=chains_raw_topic_for(correlation_pipeline_id),
            partitions=settings.kafka_correlation_chains_partitions,
            cleanup_policy="delete",  # monotonic revision envelopes, NOT compacted
            retention_ms=CHAINS_RETENTION_MS,
        ),
        TopicSpec(
            name=chains_analysed_topic_for(correlation_pipeline_id),
            partitions=settings.kafka_correlation_chains_partitions,
            cleanup_policy="delete",
            retention_ms=CHAINS_RETENTION_MS,
        ),
    ]


def build_new_topic(spec: TopicSpec) -> NewTopic:
    """The repo's single ``NewTopic`` builder — static boot topics and dynamic families alike."""
    return NewTopic(
        topic=spec.name,
        num_partitions=spec.partitions,
        replication_factor=spec.replication_factor or settings.kafka_default_replication_factor,
        config=spec.to_topic_config(),
    )


def validate_existing_topic_partitions(admin_client: AdminClient, name: str, expected_partitions: int) -> None:
    """Warn if an existing topic has a different partition count than declared.

    Does NOT alter the topic — `kafka-topics --alter` (increase) or delete+recreate (decrease /
    cleanup-policy change) is a manual DevOps action. Silent in-place reconfiguration risks fights
    with cluster operators and is irreversible for partition shrinks.
    """
    try:
        md = admin_client.list_topics(topic=name, timeout=10)
        topic_md = md.topics.get(name) if md.topics else None
        actual = len(topic_md.partitions) if topic_md and topic_md.partitions else None
    except KafkaException as e:
        logger.warning(f"Could not fetch partition count for {name}: {e}")
        return
    if actual is not None and actual != expected_partitions:
        logger.warning(
            "Topic partition count drift detected",
            extra={
                "topic": name,
                "declared_partitions": expected_partitions,
                "actual_partitions": actual,
                "action_required": "manual alter / recreate by DevOps",
            },
        )


class KafkaTopicLifecycleService(BaseKafkaSyncClient):
    """Provision and retire the per-entity topic families (Decisions 1 and 2).

    Every method is ``async`` and runs the blocking ``AdminClient`` work in ``asyncio.to_thread``,
    mirroring ``KafkaTopicsService.get_topics``. Failures raise ``TopicProvisioningError``; the
    calling manager translates that to a domain exception (``ServiceUnavailableError`` → HTTP 503)
    and writes the activity record — this layer never logs activity and never swallows an error.
    """

    def __init__(self) -> None:
        super().__init__(client_id="admin-panel-topic-lifecycle")
        self._admin_client: AdminClient | None = None

    def _get_default_client_id(self) -> str:
        """Return default client ID for the lifecycle service."""
        return "admin-panel-topic-lifecycle"

    def _get_admin_client(self) -> AdminClient:
        """Get or create the AdminClient — lazily, so importing this module touches no broker."""
        if self._admin_client is None:
            self._admin_client = AdminClient(self._config)
        return self._admin_client

    async def ensure_pipeline_topics(self, pipeline_id: UUID | str) -> str:
        """Create the detection pipeline's tagged-events topic if it is missing.

        Idempotent: an existing topic is a success (its partition count is only warned about,
        never altered). Raises rather than degrading — a matcher writing to a topic that does not
        exist would get a broker-auto-created one with the wrong partitions and retention.

        Args:
            pipeline_id: ``pipelines.id`` of the owning detection pipeline.

        Returns:
            The topic name, for persisting on ``pipelines.tagged_events_topic``.

        Raises:
            TopicProvisioningError: On an illegal id or any broker failure.
        """
        specs = pipeline_topic_specs(pipeline_id)
        await self._ensure(specs)
        return specs[0].name

    async def ensure_correlation_pipeline_topics(self, correlation_pipeline_id: UUID | str) -> list[str]:
        """Create the correlation pipeline's chains-config / chains-raw / chains-analysed topics.

        Partial creation is deliberate: topics created before a failure stay. They are empty,
        idempotent and retention-bound, and the next call completes the set — compensating deletes
        would turn a transient broker blip into data loss.

        Args:
            correlation_pipeline_id: ``correlation_pipelines.id`` of the owning row.

        Returns:
            The three topic names, in ``correlation_pipeline_topic_specs`` order.

        Raises:
            TopicProvisioningError: On an illegal id or any broker failure.
        """
        specs = correlation_pipeline_topic_specs(correlation_pipeline_id)
        await self._ensure(specs)
        return [spec.name for spec in specs]

    async def reconcile(
        self,
        pipeline_ids: Iterable[UUID | str],
        correlation_pipeline_ids: Iterable[UUID | str],
    ) -> list[str]:
        """Ensure every topic owned by the given rows exists, in one broker round-trip.

        This is what makes a restored DB backup usable against a fresh cluster. Called from the
        ``server.py`` lifespan, where a failure is logged and startup continues.

        Args:
            pipeline_ids: Detection pipelines that own a per-pipeline tagged-events topic.
            correlation_pipeline_ids: Every correlation pipeline row.

        Returns:
            Names of the topics that had to be created (empty when nothing was missing).

        Raises:
            TopicProvisioningError: On an illegal id or any broker failure.
        """
        specs: list[TopicSpec] = []
        for pipeline_id in pipeline_ids:
            specs.extend(pipeline_topic_specs(pipeline_id))
        for correlation_pipeline_id in correlation_pipeline_ids:
            specs.extend(correlation_pipeline_topic_specs(correlation_pipeline_id))
        if not specs:
            return []
        return await self._ensure(specs)

    async def retire_pipeline_topics(self, pipeline_id: UUID | str) -> list[TopicRetirementOutcome]:
        """Retire the detection pipeline's tagged-events topic per ``kafka_topic_delete_policy``.

        Args:
            pipeline_id: ``pipelines.id`` of the row being deleted.

        Returns:
            One outcome per topic; ``action`` may be a downgrade of the configured policy.

        Raises:
            TopicProvisioningError: On an illegal id, a shared topic, or any broker failure.
        """
        return await self._retire(pipeline_topic_specs(pipeline_id))

    async def retire_correlation_pipeline_topics(
        self, correlation_pipeline_id: UUID | str
    ) -> list[TopicRetirementOutcome]:
        """Retire the correlation pipeline's three topics per ``kafka_topic_delete_policy``.

        Args:
            correlation_pipeline_id: ``correlation_pipelines.id`` of the row being deleted.

        Returns:
            One outcome per topic; ``action`` may be a downgrade of the configured policy.

        Raises:
            TopicProvisioningError: On an illegal id, a shared topic, or any broker failure.
        """
        return await self._retire(correlation_pipeline_topic_specs(correlation_pipeline_id))

    async def _ensure(self, specs: list[TopicSpec]) -> list[str]:
        try:
            return await asyncio.to_thread(self._ensure_sync, specs)
        except KafkaException as e:
            raise TopicProvisioningError(f"Kafka topic provisioning failed: {e}") from e

    def _ensure_sync(self, specs: list[TopicSpec]) -> list[str]:
        admin_client = self._get_admin_client()
        existing = self._existing_topics(admin_client)

        missing = []
        for spec in specs:
            if spec.name in existing:
                validate_existing_topic_partitions(admin_client, spec.name, spec.partitions)
            else:
                missing.append(spec)
        if not missing:
            return []

        logger.info(
            "Creating per-entity Kafka topic(s)",
            extra={"topics": [spec.name for spec in missing]},
        )
        futures = admin_client.create_topics(
            [build_new_topic(spec) for spec in missing],
            request_timeout=ADMIN_TIMEOUT_SECONDS,
        )
        created: list[str] = []
        for topic_name, future in futures.items():
            try:
                future.result()
                created.append(topic_name)
            except KafkaException as e:
                # Another process (or a concurrent request) won the race — idempotent success.
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    logger.warning(f"Topic {topic_name} was created by another process")
                    created.append(topic_name)
                else:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    raise
        return created

    async def _retire(self, specs: list[TopicSpec]) -> list[TopicRetirementOutcome]:
        policy = settings.kafka_topic_delete_policy
        for spec in specs:
            self._assert_retirable(spec.name)
        try:
            return await asyncio.to_thread(self._retire_sync, specs, policy)
        except KafkaException as e:
            raise TopicProvisioningError(f"Kafka topic retirement failed: {e}") from e

    @staticmethod
    def _assert_retirable(name: str) -> None:
        """Deny-list + family check, before any broker call.

        ``validate_topic_name`` already refuses a shared topic; this additionally requires the name
        to belong to one of the four per-entity families, so a caller can never hand this service a
        topic it does not own.
        """
        validate_topic_name(name)
        for prefix in family_prefixes():
            if name.startswith(f"{prefix}.") and ENTITY_ID_RE.match(name[len(prefix) + 1 :]):
                return
        raise TopicProvisioningError(f"Not a per-entity topic, refusing to retire: {name!r}")

    def _retire_sync(self, specs: list[TopicSpec], policy: str) -> list[TopicRetirementOutcome]:
        admin_client = self._get_admin_client()
        existing = self._existing_topics(admin_client)
        outcomes: list[TopicRetirementOutcome] = []

        for spec in specs:
            if spec.name not in existing:
                outcomes.append(TopicRetirementOutcome(spec.name, policy, "kept", "topic does not exist"))
                continue
            if policy == "keep":
                logger.info("Keeping topic of a deleted row", extra={"topic": spec.name})
                outcomes.append(TopicRetirementOutcome(spec.name, policy, "kept"))
                continue

            reason: str | None = None
            if policy == "delete":
                blockers = self._delete_blockers(admin_client, spec.name)
                if not blockers:
                    self._delete_topic_sync(admin_client, spec.name)
                    outcomes.append(TopicRetirementOutcome(spec.name, policy, "deleted"))
                    continue
                reason = "; ".join(blockers)
                logger.warning(
                    "Topic delete downgraded to retire — a consumer still needs the data",
                    extra={"topic": spec.name, "blockers": blockers},
                )

            self._apply_retire_config_sync(admin_client, spec.name)
            outcomes.append(TopicRetirementOutcome(spec.name, policy, "retired", reason))

        return outcomes

    def _existing_topics(self, admin_client: AdminClient) -> set[str]:
        metadata = admin_client.list_topics(timeout=ADMIN_TIMEOUT_SECONDS)
        return set(metadata.topics.keys()) if metadata.topics else set()

    def _apply_retire_config_sync(self, admin_client: AdminClient, name: str) -> None:
        """Set ``cleanup.policy=delete`` AND the retire retention, so compacted topics age out too."""
        resource = ConfigResource(
            ConfigResource.Type.TOPIC,
            name,
            incremental_configs=[
                ConfigEntry("cleanup.policy", "delete", incremental_operation=AlterConfigOpType.SET),
                ConfigEntry(
                    "retention.ms",
                    str(settings.kafka_retired_topic_retention_ms),
                    incremental_operation=AlterConfigOpType.SET,
                ),
            ],
        )
        for future in admin_client.incremental_alter_configs(
            [resource], request_timeout=ADMIN_TIMEOUT_SECONDS
        ).values():
            future.result()
        logger.info(
            "Topic retired — broker will reclaim it after the grace window",
            extra={"topic": name, "retention_ms": settings.kafka_retired_topic_retention_ms},
        )

    def _delete_topic_sync(self, admin_client: AdminClient, name: str) -> None:
        for topic_name, future in admin_client.delete_topics([name], request_timeout=ADMIN_TIMEOUT_SECONDS).items():
            try:
                future.result()
            except KafkaException as e:
                if "UNKNOWN_TOPIC" in str(e):
                    logger.warning(f"Topic {topic_name} was already gone")
                else:
                    raise
        logger.warning("Topic deleted irreversibly", extra={"topic": name})

    def _is_chains_analysed(self, name: str) -> bool:
        return name.startswith(f"{settings.kafka_chains_analysed_topic}.")

    def _delete_blockers(self, admin_client: AdminClient, topic: str) -> list[str]:
        """Reasons a delete must downgrade to retire; empty means the topic is safe to delete.

        Best-effort and racy by nature — a group can attach a millisecond after the check. It
        catches the realistic case (a correlation/agent job that failed to stop, or a control plane
        that has not caught up) and nothing more.

        ``chains-analysed.<corr_id>`` is the only per-entity topic this control plane consumes
        itself, so deleting it can drop chains an operator already paid an LLM for: it drains
        instead of refusing outright, polling until nothing blocks or
        ``kafka_topic_delete_drain_timeout_seconds`` elapses.
        """
        drain = self._is_chains_analysed(topic)
        deadline = time.monotonic() + settings.kafka_topic_delete_drain_timeout_seconds
        while True:
            blockers = self._evaluate_blockers(admin_client, topic)
            if not blockers or not drain:
                return blockers
            if time.monotonic() >= deadline:
                logger.warning(
                    "chains-analysed topic did not drain before the delete timeout",
                    extra={
                        "topic": topic,
                        "residual": blockers,
                        "timeout_seconds": settings.kafka_topic_delete_drain_timeout_seconds,
                    },
                )
                return blockers
            time.sleep(DRAIN_POLL_INTERVAL_SECONDS)

    def _evaluate_blockers(self, admin_client: AdminClient, topic: str) -> list[str]:
        """One pass over the topic's consumer groups: unconsumed records or attached members block.

        The control plane's own ``chains-analysed`` group is exempt from the *liveness* half only —
        it is this very process and is always attached, so residual lag is the meaningful signal
        for it. Every other group blocks while it still has members.
        """
        exempt_liveness = {settings.kafka_chains_analysed_consumer_group} if self._is_chains_analysed(topic) else set()
        blockers: list[str] = []
        for state in self._topic_group_states(admin_client, topic):
            if state.lag > 0:
                blockers.append(f"consumer group '{state.group_id}' has {state.lag} unconsumed record(s)")
            elif state.live and state.group_id not in exempt_liveness:
                blockers.append(f"consumer group '{state.group_id}' still has live members")
        return blockers

    def _topic_group_states(self, admin_client: AdminClient, topic: str) -> list[_GroupTopicState]:
        listings = admin_client.list_consumer_groups(request_timeout=ADMIN_TIMEOUT_SECONDS).result().valid
        if not listings:
            return []

        committed: dict[str, dict[int, int]] = {}
        requests = [ConsumerGroupTopicPartitions(listing.group_id, None) for listing in listings]
        for group_id, future in admin_client.list_consumer_group_offsets(
            requests, request_timeout=ADMIN_TIMEOUT_SECONDS
        ).items():
            try:
                result = future.result()
            except KafkaException as e:
                logger.warning(f"Could not read committed offsets for group {group_id}: {e}")
                continue
            offsets = {
                tp.partition: tp.offset
                for tp in (result.topic_partitions or [])
                if tp.topic == topic and tp.offset >= 0
            }
            if offsets:
                committed[group_id] = offsets
        if not committed:
            return []

        partitions = sorted({partition for offsets in committed.values() for partition in offsets})
        end_offsets = self._end_offsets(admin_client, topic, partitions)
        group_states = {listing.group_id: listing.state for listing in listings}

        return [
            _GroupTopicState(
                group_id=group_id,
                lag=sum(max(0, end_offsets.get(partition, offset) - offset) for partition, offset in offsets.items()),
                live=group_states.get(group_id) in _LIVE_GROUP_STATES,
            )
            for group_id, offsets in committed.items()
        ]

    def _end_offsets(self, admin_client: AdminClient, topic: str, partitions: list[int]) -> dict[int, int]:
        request = {TopicPartition(topic, partition): OffsetSpec.latest() for partition in partitions}
        end_offsets: dict[int, int] = {}
        for topic_partition, future in admin_client.list_offsets(
            request, request_timeout=ADMIN_TIMEOUT_SECONDS
        ).items():
            try:
                end_offsets[topic_partition.partition] = future.result().offset
            except KafkaException as e:
                logger.warning(f"Could not read end offset for {topic}[{topic_partition.partition}]: {e}")
        return end_offsets


# Singleton instance
topic_lifecycle_service = KafkaTopicLifecycleService()
