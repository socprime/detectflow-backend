"""Kafka integration for attack chains: ``detectflow.chains-config`` producer and ``detectflow.chains-analysed`` consumer."""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from functools import partial
from time import monotonic
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError, KafkaConnectionError, KafkaError
from aiokafka.structs import ConsumerRecord
from confluent_kafka import KafkaException, Producer
from pydantic import ValidationError
from sqlalchemy import exc as sa_exc
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import AsyncSessionLocal
from apps.core.enums import ChainsAnalysedErrorClass
from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import ChainDependencyMissingError
from apps.core.logger import get_logger
from apps.core.models import AttackChainConfiguration, CorrelationPipeline
from apps.core.schemas import ChainsAnalysedHealth, ChainsAnalysedMessage
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.base import BaseKafkaAsyncClient, BaseKafkaSyncClient
from apps.modules.postgre.attack_chains import (
    AttackChainsDAO,
    ChainsAnalysedDeadLetterDAO,
    truncate_dead_letter_payload,
)
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO
from apps.services.attack_chains_list_invalidation import attack_chains_list_invalidation_service

logger = get_logger(__name__)

# serialization_failure, deadlock_detected, lock_not_available, query_canceled (statement
# timeout), too_many_connections, connection_exception/_does_not_exist/_failure.
_RETRIABLE_SQLSTATES = frozenset({"40001", "40P01", "55P03", "57014", "53300", "08000", "08003", "08006"})
_FK_VIOLATION_SQLSTATE = "23503"

_UUID_RE = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
# How long the orphan guard trusts its cached correlation-pipeline id set. Long enough that a busy
# topic costs one query a minute, short enough that a pipeline created just now is picked up before
# an operator notices; a miss inside the window re-reads once anyway.
_PIPELINE_CACHE_TTL_SECONDS = 60.0


def build_chains_analysed_pattern(base_topic: str, include_legacy: bool) -> str:
    """Anchored regex matching per-correlation-pipeline analysed-chain topics.

    ``detectflow.chains-analysed.<uuid>`` always; the pre-epic singleton
    ``detectflow.chains-analysed`` only while ``include_legacy`` is on (migration window).

    aiokafka applies the pattern with ``re.match``, which anchors the START only — an unanchored
    tail would also match ``detectflow.chains-analysed-whatever``, i.e. a topic a customer could
    create. Both ends are anchored here.
    """
    suffix = rf"(?:\.{_UUID_RE})" + ("?" if include_legacy else "")
    return rf"^{re.escape(base_topic)}{suffix}$"


def resolve_correlation_pipeline_id(msg: ChainsAnalysedMessage, topic: str) -> tuple[UUID | None, bool]:
    """Resolve the owning correlation pipeline; ``(id, used_topic_fallback)``.

    Envelope field wins; otherwise the topic-name suffix
    (``detectflow.chains-analysed.<uuid>``); otherwise ``None`` for the legacy singleton topic.
    The envelope field is stamped by the data plane on ``ChainAnalysed``
    (``../detectflow-agent`` ``app/operators/chain_record.py``). The topic fallback keeps the control
    plane correct when a data-plane rollout lags **and** when a stamp arrived malformed and was
    dropped by the schema validator — those two cases are indistinguishable here and are both
    counted, so a silently misconfigured job is visible in ``/health`` rather than invisible.
    """
    if msg.correlation_pipeline_id is not None:
        return msg.correlation_pipeline_id, False
    try:
        return UUID(topic.rsplit(".", 1)[-1]), True
    except ValueError:
        return None, True


class _ChainsRebalanceListener(ConsumerRebalanceListener):
    """Keep the per-topic health maps honest across the rebalances a pattern subscription causes.

    Creating or deleting any correlation pipeline changes the matched topic set and reassigns every
    partition of the group, so revocation is routine here rather than exceptional. Neither callback
    may raise: aiokafka runs them inside the join/sync barrier, where an exception would fail the
    rebalance for the whole group.
    """

    def __init__(self, consumer: ChainsAnalysedConsumer) -> None:
        self._owner = consumer

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        try:
            remaining = self._owner._assigned_partitions() - set(revoked)
            remaining_topics = {tp.topic for tp in remaining}
            for tp in revoked:
                self._owner._last_offset_by_tp.pop(tp, None)
            for topic in {tp.topic for tp in revoked} - remaining_topics:
                self._owner._last_message_time_by_topic.pop(topic, None)
        except Exception as e:
            logger.warning("chains-analysed revoke bookkeeping failed (non-fatal)", extra={"error": str(e)})

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        try:
            logger.info(
                "chains-analysed partitions assigned",
                extra={"topics": sorted({tp.topic for tp in assigned}), "partitions": len(assigned)},
            )
        except Exception as e:
            logger.warning("chains-analysed assign bookkeeping failed (non-fatal)", extra={"error": str(e)})


def _sqlstate(exc: BaseException) -> str | None:
    """SQLSTATE of the wrapped DBAPI error (asyncpg exposes ``sqlstate``, psycopg2 ``pgcode``)."""
    orig = getattr(exc, "orig", None)
    return getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)


def _payload_text(value: Any) -> str | None:
    """Record value as text for parking. The consumer deserializes to ``str``; bytes are a fallback."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bytes | bytearray):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def classify_chains_analysed_error(exc: BaseException) -> ChainsAnalysedErrorClass:
    """Route one apply failure to a retry policy.

    Unknown exceptions are DEFERRABLE on purpose: a new failure mode may then neither block the
    partition forever nor be discarded — it is retried, parked with its payload, and skipped.
    """
    if isinstance(exc, ChainDependencyMissingError):
        return ChainsAnalysedErrorClass.DEFERRABLE
    if isinstance(exc, TimeoutError | sa_exc.TimeoutError | sa_exc.InterfaceError):
        return ChainsAnalysedErrorClass.INFRASTRUCTURE
    if isinstance(exc, sa_exc.DBAPIError):
        if exc.connection_invalidated or _sqlstate(exc) in _RETRIABLE_SQLSTATES:
            return ChainsAnalysedErrorClass.INFRASTRUCTURE
        # 23503 = the referenced Active Threat / high-order Sigma row is not stored yet.
        if isinstance(exc, sa_exc.IntegrityError) and _sqlstate(exc) == _FK_VIOLATION_SQLSTATE:
            return ChainsAnalysedErrorClass.DEFERRABLE
        return ChainsAnalysedErrorClass.PERMANENT
    # StatementError is checked AFTER DBAPIError — the latter subclasses the former.
    if isinstance(exc, ValidationError | ValueError | TypeError | sa_exc.StatementError):
        return ChainsAnalysedErrorClass.PERMANENT
    return ChainsAnalysedErrorClass.DEFERRABLE


# The record key stays the literal "global" now that the TOPIC is per correlation pipeline:
# isolation comes from the topic, not from the key. Two reasons it must not become per-instance.
# (1) PyFlink 2.2 cannot read a Kafka record key at all (the broadcast source deserializes the
# value only — see ../detectflow-agent/app/connectors/broadcast_filter.py), so a per-instance key
# would be unreadable by the only consumer that exists. (2) The topic is compacted with a single
# logical record; a varying key would make compaction keep one record per key instead of one
# record per pipeline, and a restarted matcher would replay stale configs.
_CHAINS_CONFIG_KEY = "global"


class KafkaChainsConfigProducer(BaseKafkaSyncClient):
    """Publish one correlation pipeline's config snapshot to its broadcast topic (Channel C).

    ONE producer, N topics: the topic is a ``publish`` argument, never instance state, so adding a
    correlation pipeline does not add a Kafka producer, a socket or a thread.
    """

    _executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka-chains-config")

    def __init__(self) -> None:
        super().__init__(client_id="admin-panel-chains-config")
        # DEPRECATED — the shared singleton topic, used only by the quarantined
        # ``attack_chain_configuration`` path; removed by
        # migrate-singleton-config-to-default-pipeline.md.
        self.chains_config_topic = settings.kafka_chains_config_topic

        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",
                "compression.type": "gzip",
            }
        )
        self.producer = Producer(self.producer_config)

    def _get_default_client_id(self) -> str:
        return "admin-panel-chains-config"

    def _publish_sync(self, envelope: dict[str, Any], topic: str) -> None:
        payload = json.dumps(envelope, default=str).encode("utf-8")
        key = _CHAINS_CONFIG_KEY.encode("utf-8")
        errors: list[Exception] = []

        def on_delivery(err, _msg) -> None:
            if err:
                errors.append(KafkaException(err))

        self.producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
        self.producer.poll(0)
        self.producer.flush(timeout=30)
        if errors:
            raise errors[0]

    def build_envelope(
        self,
        row: CorrelationPipeline,
        *,
        excluded_active_threat_ids: list[str],
    ) -> dict[str, Any]:
        """Config snapshot for one correlation pipeline's broadcast state (Channel C).

        Additive-only wire contract; the consumer is
        ``../detectflow-agent/app/operators/hos_correlation_function.py``.

        ``correlation_pipeline_id`` / ``correlation_pipeline_name`` are MANDATORY and are what the
        data plane uses to scope ``chain_id`` (§21) and to stamp the chains-raw envelope; the job
        falls back to its ``CORRELATION_PIPELINE_ID`` env var only when they are absent.

        Isolation is the Kafka subscription (``KAFKA_INPUT_TOPICS`` = member tagged-events topics).
        Destination topic names are not a correlation filter and are not on this envelope.

        ``excluded_active_threat_ids`` is a DENY-list (§20), never an allow-list: a threat is
        monitored unless it is globally blocked or excluded here, so empty is the identity. The
        global ``NOT is_blocked`` conjunct is NOT carried here — it keeps riding the shared
        ``detectflow.active-threats`` topic through the existing ``enabled`` key, and the matcher's
        ``at.enabled`` gates stay in force (§20 / D8a).

        ``correlation_window_seconds`` may be ``None`` -> inherit each HoS rule's
        ``detection.timeframe``; a value is an explicit override that wins over it (§19).
        """
        return {
            "correlation_pipeline_id": str(row.id),
            "correlation_pipeline_name": row.name,
            "threshold_pct": row.threshold_pct,
            "correlation_window_seconds": row.correlation_window_seconds,
            "excluded_active_threat_ids": excluded_active_threat_ids,
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    def build_singleton_envelope(self, row: AttackChainConfiguration) -> dict[str, Any]:
        """DEPRECATED — envelope of the pre-epic ``attack_chain_configuration`` singleton.

        Kept so a leftover pre-cutover deployment can still be driven until its pods are torn
        down. Destination-topic filtering is gone; only threshold/window remain.
        """
        return {
            "threshold_pct": row.threshold_pct,
            "correlation_window_seconds": row.correlation_window_seconds,
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }

    async def publish(self, envelope: dict[str, Any], *, topic: str) -> None:
        """Publish one envelope to one topic. Failures are logged and swallowed.

        A config publish is never allowed to fail the operation that triggered it: the matcher keeps
        running on its last broadcast value, and the next edit (or the start path, which always
        republishes) repairs the compacted record.
        """
        try:
            await asyncio.get_running_loop().run_in_executor(
                self._executor,
                partial(self._publish_sync, envelope, topic),
            )
            logger.debug(
                "Published chains-config to Kafka",
                extra={"topic": topic, "key": _CHAINS_CONFIG_KEY},
            )
        except Exception as e:
            logger.warning(
                "Failed to publish chains-config (non-fatal)",
                extra={"topic": topic, "error": str(e)},
            )

    async def publish_singleton(self, row: AttackChainConfiguration) -> None:
        """DEPRECATED — publish the singleton envelope to the shared topic.

        Removed by ``migrate-singleton-config-to-default-pipeline.md``.
        """
        await self.publish(self.build_singleton_envelope(row), topic=self.chains_config_topic)


chains_config_producer = KafkaChainsConfigProducer()


class ChainsAnalysedConsumer(BaseKafkaAsyncClient):
    """Consume every ``detectflow.chains-analysed[.<correlation_pipeline_id>]`` topic; upsert ``attack_chains``.

    ONE consumer with a regex subscription, never one per correlation pipeline (D2): N consumers
    would mean N connections, N rebalance groups and N background tasks for a topic family whose
    members are created and deleted at run time.
    """

    def __init__(self) -> None:
        super().__init__(
            group_id=settings.kafka_chains_analysed_consumer_group,
            auto_offset_reset=settings.kafka_chains_analysed_auto_offset_reset,
            enable_auto_commit=False,
            # A pattern subscription discovers a new pipeline's topic on metadata refresh, so this
            # is the only consumer that leaves aiokafka's 300s default (each refresh is a
            # FULL-cluster MetadataRequest). Literal subscription needs no faster refresh.
            metadata_max_age_ms=(
                settings.kafka_chains_analysed_metadata_max_age_ms
                if settings.kafka_chains_analysed_topic_pattern_enabled
                else None
            ),
            # chains-analysed is now an AT_LEAST_ONCE (non-transactional) sink:
            # agent-job commits independently of Flink checkpoints, so the same
            # (chain_id, revision) can be delivered more than once. Duplicates are
            # absorbed by the `stored.revision >= msg.revision` idempotency guard in
            # AttackChainsDAO.apply_chains_analysed (manual offset commit only after
            # the DB commit). read_committed is kept (harmless no-op for a
            # non-transactional topic, forward-compatible if the sink ever changes).
            isolation_level="read_committed",
        )
        self.consumer: AIOKafkaConsumer | None = None
        self.is_running: bool = False
        self.last_message_time: datetime | None = None
        self._consume_task: asyncio.Task | None = None
        self._last_poll_time: datetime | None = None
        self._infra_stalled_since: datetime | None = None
        self._consecutive_infra_failures: int = 0
        self.parked_record_count: int = 0
        self.parked_by_error_class: Counter[str] = Counter()
        self._pattern: str | None = None
        # Per-topic detail the single-topic consumer had no need for: one customer's feed can go
        # quiet (or its partitions be revoked) while every other topic keeps flowing.
        self._last_message_time_by_topic: dict[str, datetime] = {}
        self._last_offset_by_tp: dict[TopicPartition, int] = {}
        self.orphan_chain_count: int = 0
        self.pipeline_id_fallback_count: int = 0
        self._known_pipelines: set[UUID] = set()
        # None, not 0.0: monotonic() is process/boot uptime, so a zero stamp would look "fresh"
        # for the first TTL seconds of a young container and orphan every attributed chain.
        self._known_pipelines_at: float | None = None

    def _get_default_group_id(self) -> str:
        return settings.kafka_chains_analysed_consumer_group

    @property
    def pattern(self) -> str | None:
        """Subscribed topic regex, or ``None`` while subscribed to the literal legacy topic."""
        return self._pattern

    async def start(self) -> None:
        """Subscribe (by pattern, normally) and run the consume loop.

        **Discovery timeline.** A correlation pipeline's ``chains-analysed.<id>`` topic is created
        when the pipeline is; the next full-cluster metadata refresh is at most
        ``kafka_chains_analysed_metadata_max_age_ms`` (60 s) away; a changed pattern match forces a
        group rejoin (sub-second normally, worst case bounded by ``session_timeout_ms`` = 10 s).
        **Chains from a new pipeline start flowing within ~60-70 s of pipeline creation, with no
        API restart** — and the pipeline needs a full correlation window before it can emit its
        first chain anyway. Nothing is lost meanwhile: ``auto_offset_reset`` is ``earliest``, so a
        topic discovered late is still read from its beginning.

        **The rejoin is global.** This group owns every customer's partitions, so creating or
        deleting any correlation pipeline revokes and reassigns all of them: the in-flight batch is
        abandoned and uncommitted records are redelivered, absorbed by the ``(chain_id, revision)``
        guard in ``AttackChainsDAO.apply_chains_analysed``.

        **Rollback.** ``KAFKA_CHAINS_ANALYSED_TOPIC_PATTERN_ENABLED=false`` + restart returns the
        consumer to the literal legacy topic. The group id is unchanged and Kafka stores offsets per
        ``(group, topic, partition)``, so switching subscription style either way resumes from the
        stored offsets and reprocesses nothing.
        """
        if self.is_running:
            logger.warning("ChainsAnalysedConsumer is already running")
            return

        logger.info(
            "Starting ChainsAnalysedConsumer",
            extra={
                "pattern": (
                    build_chains_analysed_pattern(
                        settings.kafka_chains_analysed_topic,
                        settings.kafka_chains_analysed_include_legacy_topic,
                    )
                    if settings.kafka_chains_analysed_topic_pattern_enabled
                    else None
                ),
                "topic": settings.kafka_chains_analysed_topic,
                "group_id": self._group_id,
                "bootstrap_servers": settings.kafka_bootstrap_servers,
            },
        )

        try:
            self.consumer = AIOKafkaConsumer(**self._config)
            if settings.kafka_chains_analysed_topic_pattern_enabled:
                self._pattern = build_chains_analysed_pattern(
                    settings.kafka_chains_analysed_topic,
                    settings.kafka_chains_analysed_include_legacy_topic,
                )
                self.consumer.subscribe(pattern=self._pattern, listener=_ChainsRebalanceListener(self))
            else:
                self._pattern = None
                self.consumer.subscribe([settings.kafka_chains_analysed_topic])
            await self.consumer.start()
            self.is_running = True
            self._consume_task = asyncio.create_task(self._consume_loop())
            logger.info("ChainsAnalysedConsumer started successfully")
        except KafkaConnectionError as e:
            logger.error(
                "ChainsAnalysedConsumer failed to connect to Kafka",
                extra={"error": str(e), "bootstrap_servers": settings.kafka_bootstrap_servers},
            )
            raise
        except Exception as e:
            logger.error("Failed to start ChainsAnalysedConsumer", extra={"error": str(e)})
            raise

    async def stop(self) -> None:
        if not self.is_running:
            return

        logger.info("Stopping ChainsAnalysedConsumer")
        self.is_running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass
            self._consume_task = None

        if self.consumer:
            await self.consumer.stop()
            self.consumer = None

        logger.info("ChainsAnalysedConsumer stopped")

    async def _consume_loop(self) -> None:
        """Consume chains-analysed under a park-and-skip poison policy.

        A record that can never apply must NOT block its partition. Chain records carry no
        cross-record ordering requirement — the only ordering that matters is ``(chain_id,
        revision)``, and ``AttackChainsDAO.apply_chains_analysed`` already makes duplicate and
        out-of-order delivery safe — so head-of-line blocking buys nothing while costing one
        customer their whole chain feed until somebody notices. The record is therefore retried
        under a bounded budget, then **parked with its payload** in ``chains_analysed_dead_letters``
        and skipped: parking first is what keeps the skip recoverable, because ``events[]`` is
        batch-scoped and would otherwise be lost for good.

        The one exception is infrastructure: the park store is the same database that just failed,
        so park-and-skip is unavailable. There the loop rewinds, backs off with a cap, keeps
        retrying the same record and reports unhealthy — that degrades every customer identically,
        which is honest rather than silently singling one out.

        Every commit and every rewind goes through ``_commit_offset`` / ``_seek_if_assigned``: a
        pattern subscription rebalances whenever a correlation pipeline is created or deleted, so
        losing a partition mid-batch is routine and must not surface as an error.
        """
        logger.info("Starting chains-analysed consumption loop")

        while self.is_running:
            try:
                messages = await self.consumer.getmany(timeout_ms=1000, max_records=50)
                # Liveness is "the loop is polling", including empty batches: chains are rare by
                # design, so message age says nothing about whether the consumer is alive.
                self._last_poll_time = datetime.now(UTC)

                for tp, msgs in messages.items():
                    for kafka_msg in msgs:
                        if not self.is_running:
                            break

                        parsed = self._parse_message(kafka_msg.value)
                        if parsed is None:
                            # Undecodable payloads are evidence too: park the bytes instead of
                            # committing past them with the record discarded.
                            committable = await self._park(
                                tp,
                                kafka_msg,
                                None,
                                ValueError("chains-analysed payload failed JSON/schema validation"),
                                ChainsAnalysedErrorClass.PERMANENT,
                                attempts=1,
                            )
                        else:
                            committable = await self._apply_with_retry(
                                tp, kafka_msg, self._attribute_pipeline(parsed, tp.topic)
                            )
                        if committable:
                            if await self._commit_offset(tp, kafka_msg.offset):
                                continue
                            # The partition was revoked mid-batch: the rest of this batch belongs
                            # to whoever owns it now.
                            break
                        # Infrastructure is down, or parking itself failed: rewind and retry the
                        # SAME record. Never skipped — an unparked record must not be lost.
                        self._seek_if_assigned(tp, kafka_msg.offset)
                        await asyncio.sleep(self._infra_backoff_seconds())
                        break

            except asyncio.CancelledError:
                logger.info("chains-analysed consumption loop cancelled")
                break
            except KafkaError as e:
                logger.error("Kafka error in chains-analysed loop", extra={"error": str(e)})
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("Unexpected error in chains-analysed loop", extra={"error": str(e)})
                await asyncio.sleep(5)

    async def _commit_offset(self, tp: TopicPartition, offset: int) -> bool:
        """Commit ``offset + 1`` for ``tp``; ``False`` when the partition was revoked mid-batch.

        A pattern subscription rebalances whenever a correlation pipeline is created or deleted, so
        losing a partition between processing and committing is routine, not an error. The record is
        redelivered to the new owner and reprocessing is a no-op: the (chain_id, revision) guard
        absorbs the apply path, and dead-letter parking upserts on (topic, partition, offset).
        """
        try:
            await self.consumer.commit({tp: offset + 1})
        except (CommitFailedError, IllegalStateError) as e:
            logger.info(
                "chains-analysed commit skipped after rebalance",
                extra={"topic": tp.topic, "partition": tp.partition, "error": str(e)},
            )
            return False
        self._last_offset_by_tp[tp] = offset
        return True

    def _seek_if_assigned(self, tp: TopicPartition, offset: int) -> None:
        """Rewind only while we still own ``tp`` — ``seek`` raises ``IllegalStateError`` otherwise.

        ``assignment()`` is process-local; no round trip.
        """
        if tp in self._assigned_partitions():
            self.consumer.seek(tp, offset)

    def _assigned_partitions(self) -> set[TopicPartition]:
        """Currently assigned partitions; empty before ``start()`` and after ``stop()``."""
        if self.consumer is None:
            return set()
        return set(self.consumer.assignment())

    def _attribute_pipeline(self, parsed: ChainsAnalysedMessage, topic: str) -> ChainsAnalysedMessage:
        """Stamp the resolved correlation pipeline onto the message, counting topic fallbacks.

        A record on the legacy singleton topic has nothing to fall back to, so it is not counted —
        it is a pre-epic record, not a misconfigured producer.
        """
        resolved, used_topic_fallback = resolve_correlation_pipeline_id(parsed, topic)
        if used_topic_fallback and topic != settings.kafka_chains_analysed_topic:
            self.pipeline_id_fallback_count += 1
        if resolved == parsed.correlation_pipeline_id:
            return parsed
        return parsed.model_copy(update={"correlation_pipeline_id": resolved})

    async def _known_pipeline_ids(self, db: AsyncSession) -> set[UUID]:
        """Correlation-pipeline ids, cached for ``_PIPELINE_CACHE_TTL_SECONDS`` — no per-message query."""
        now = monotonic()
        if self._known_pipelines_at is None or now - self._known_pipelines_at > _PIPELINE_CACHE_TTL_SECONDS:
            self._known_pipelines = await CorrelationPipelineDAO(db).list_ids()
            self._known_pipelines_at = now
        return self._known_pipelines

    async def _drop_orphan_pipeline(
        self, db: AsyncSession, msg: ChainsAnalysedMessage, topic: str
    ) -> ChainsAnalysedMessage:
        """Null a stamp whose correlation pipeline is gone, instead of letting the FK fail.

        Deleting a correlation pipeline must not delete the detections it produced: a chain is
        forensic evidence and may already carry analyst state. ``ondelete="SET NULL"`` covers rows
        inserted before the delete; this covers the ones still in flight after it. Left alone, the
        FK violation would be classified as a data error and would park a perfectly good chain in
        ``chains_analysed_dead_letters`` on an entirely routine operator action.
        """
        pipeline_id = msg.correlation_pipeline_id
        if pipeline_id is None or pipeline_id in await self._known_pipeline_ids(db):
            return msg
        # A pipeline created seconds ago is a legitimate cache miss — re-read once before nulling.
        self._known_pipelines_at = None
        if pipeline_id in await self._known_pipeline_ids(db):
            return msg
        self.orphan_chain_count += 1
        logger.warning(
            "chains-analysed record names a correlation pipeline that no longer exists; storing unattributed",
            extra={"topic": topic, "chain_id": str(msg.chain_id), "correlation_pipeline_id": str(pipeline_id)},
        )
        return msg.model_copy(update={"correlation_pipeline_id": None})

    async def _apply_with_retry(
        self, tp: TopicPartition, kafka_msg: ConsumerRecord, parsed: ChainsAnalysedMessage
    ) -> bool:
        """Apply one record under the poison policy; ``True`` when the offset may advance.

        ``True`` also covers "parked and skipped" — the payload is durably stored in
        ``chains_analysed_dead_letters`` before the caller commits past it. ``False`` means the
        caller must NOT commit.
        """
        attempt = 0
        started = monotonic()
        while self.is_running:
            attempt += 1
            try:
                async with AsyncSessionLocal() as db:
                    try:
                        record = await self._drop_orphan_pipeline(db, parsed, tp.topic)
                        applied = await AttackChainsDAO(db).apply_chains_analysed(record)
                        await db.commit()
                    except Exception:
                        await db.rollback()
                        raise
            except Exception as exc:
                error_class = classify_chains_analysed_error(exc)
                if error_class is ChainsAnalysedErrorClass.INFRASTRUCTURE:
                    self._note_infra_failure(tp, exc)
                    return False
                spent = (
                    error_class is ChainsAnalysedErrorClass.PERMANENT
                    or attempt >= settings.kafka_chains_analysed_max_attempts
                    or monotonic() - started >= settings.kafka_chains_analysed_retry_deadline_seconds
                )
                if not spent:
                    logger.warning(
                        "Retrying chains-analysed record",
                        extra={
                            "topic": tp.topic,
                            "partition": tp.partition,
                            "offset": kafka_msg.offset,
                            "chain_id": str(parsed.chain_id),
                            "error_class": str(error_class),
                            "error_type": type(exc).__name__,
                            "attempt": attempt,
                        },
                    )
                    await asyncio.sleep(self._retry_backoff_seconds(attempt))
                    continue
                return await self._park(tp, kafka_msg, parsed, exc, error_class, attempt)
            self._note_apply_success(tp, parsed, applied)
            if applied:
                attack_chains_list_invalidation_service.notify_changed()
            return True
        return False

    async def _park(
        self,
        tp: TopicPartition,
        kafka_msg: ConsumerRecord,
        parsed: ChainsAnalysedMessage | None,
        exc: BaseException,
        error_class: ChainsAnalysedErrorClass,
        attempts: int,
    ) -> bool:
        """Store the payload in ``chains_analysed_dead_letters``; ``True`` when it may be skipped.

        Opens a FRESH session: the failed one was rolled back and may be invalidated. Any failure
        here returns ``False`` and is treated as an infrastructure stall — the record is retried,
        never discarded, because committing past an unparked record is unrecoverable loss.
        """
        payload, truncated = truncate_dead_letter_payload(
            _payload_text(kafka_msg.value), settings.kafka_chains_analysed_dead_letter_max_payload_bytes
        )
        try:
            async with AsyncSessionLocal() as db:
                try:
                    await ChainsAnalysedDeadLetterDAO(db).park(
                        kafka_topic=tp.topic,
                        kafka_partition=tp.partition,
                        kafka_offset=kafka_msg.offset,
                        chain_id=parsed.chain_id if parsed is not None else None,
                        hos_rule_id=parsed.hos_rule_id if parsed is not None else None,
                        error_class=str(error_class),
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        attempts=attempts,
                        payload=payload,
                        payload_truncated=truncated,
                    )
                    await db.commit()
                except Exception:
                    await db.rollback()
                    raise
        except Exception as park_exc:
            logger.error(
                "Failed to park poison chains-analysed record; keeping the offset (will retry)",
                extra={
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "offset": kafka_msg.offset,
                    "error": str(park_exc),
                },
                exc_info=True,
            )
            self._note_infra_failure(tp, park_exc)
            return False

        # A committed park proves the database is reachable, so it clears an earlier infra stall —
        # otherwise ``/health`` would stay red on a recovered loop that happens to be parking rather
        # than applying.
        self._consecutive_infra_failures = 0
        self._infra_stalled_since = None
        self.parked_record_count += 1
        self.parked_by_error_class[str(error_class)] += 1
        logger.error(
            "Parked poison chains-analysed record",
            extra={
                "topic": tp.topic,
                "partition": tp.partition,
                "offset": kafka_msg.offset,
                "chain_id": str(parsed.chain_id) if parsed is not None else None,
                "error_class": str(error_class),
                "error_type": type(exc).__name__,
                "attempts": attempts,
                "parked_total": self.parked_record_count,
            },
        )
        # Keyed per topic so one customer's storm cannot mask another customer's first failure.
        if ErrorTracker.should_log(f"chains_analysed_poison_{tp.topic}"):
            await activity_producer.log_action(
                action="error",
                entity_type="attack_chain",
                entity_id=str(parsed.chain_id) if parsed is not None else None,
                details=(
                    f"Parked an unprocessable chains-analysed record from {tp.topic} "
                    f"(partition {tp.partition}, offset {kafka_msg.offset}, {error_class}: "
                    f"{type(exc).__name__}). Ingestion continues; inspect chains_analysed_dead_letters."
                ),
                source="system",
                severity="error",
            )
        return True

    def _note_apply_success(self, tp: TopicPartition, parsed: ChainsAnalysedMessage, applied: bool) -> None:
        """Clear the infra-stall state and stamp the last successful apply, globally and per topic."""
        self._consecutive_infra_failures = 0
        self._infra_stalled_since = None
        now = datetime.now(UTC)
        self.last_message_time = now
        self._last_message_time_by_topic[tp.topic] = now
        logger.debug(
            "Processed chains-analysed message",
            extra={
                "topic": tp.topic,
                "chain_id": str(parsed.chain_id),
                "correlation_pipeline_id": str(parsed.correlation_pipeline_id)
                if parsed.correlation_pipeline_id
                else None,
                "revision": parsed.revision,
                "applied": applied,
            },
        )

    def _note_infra_failure(self, tp: TopicPartition, exc: BaseException) -> None:
        """Record an infrastructure failure: it blocks the partition and eventually turns health red."""
        self._consecutive_infra_failures += 1
        if self._infra_stalled_since is None:
            self._infra_stalled_since = datetime.now(UTC)
        logger.error(
            "chains-analysed infrastructure failure; not committing and not parking",
            extra={
                "topic": tp.topic,
                "partition": tp.partition,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "consecutive_failures": self._consecutive_infra_failures,
            },
        )
        if ErrorTracker.should_log("chains_analysed_infra_stall"):
            logger.error(
                "chains-analysed ingestion is stalled on infrastructure",
                extra={"since": self._infra_stalled_since.isoformat(), "error": str(exc)},
                exc_info=True,
            )

    @staticmethod
    def _backoff(attempt: int, base: float, ceiling: float) -> float:
        return min(base * (2 ** max(attempt - 1, 0)), ceiling)

    def _retry_backoff_seconds(self, attempt: int) -> float:
        """Backoff before the next attempt on the same record."""
        return self._backoff(
            attempt,
            settings.kafka_chains_analysed_retry_backoff_base_seconds,
            settings.kafka_chains_analysed_retry_backoff_max_seconds,
        )

    def _infra_backoff_seconds(self) -> float:
        """Backoff before re-reading a partition blocked on infrastructure (grows while it stays down)."""
        return self._backoff(
            self._consecutive_infra_failures,
            settings.kafka_chains_analysed_retry_backoff_base_seconds,
            settings.kafka_chains_analysed_retry_backoff_max_seconds,
        )

    def _parse_message(self, value: str | None) -> ChainsAnalysedMessage | None:
        if not value:
            return None
        try:
            raw = json.loads(value)
            return ChainsAnalysedMessage.model_validate(raw)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse chains-analysed JSON: %s", e)
            return None
        except ValidationError as e:
            logger.warning("Failed to validate chains-analysed payload: %s", e)
            return None

    @property
    def is_healthy(self) -> bool:
        """Poll-loop liveness plus the infra-stall signal — NOT chain throughput.

        Attack chains are rare by design, so the previous ``last_message_time`` age check was red on
        a quiet platform and green while a partition was permanently stuck. Neither is liveness.
        """
        if not self.is_running:
            return False
        if self._infra_stalled_since is not None:
            stalled = (datetime.now(UTC) - self._infra_stalled_since).total_seconds()
            if stalled >= settings.kafka_chains_analysed_retry_deadline_seconds:
                return False
        if self._last_poll_time is None:
            return True
        age = (datetime.now(UTC) - self._last_poll_time).total_seconds()
        return age < settings.kafka_chains_analysed_poll_stale_seconds

    def health_snapshot(self) -> ChainsAnalysedHealth:
        """Per-topic detail for ``GET /health`` — process-local state only.

        The handler is public and unauthenticated, so this must issue **no** Kafka request and
        **no** DB query: ``assignment()`` and ``highwater()`` both read values cached from the last
        fetch. The operator-facing parked-record count with a DB query lives in ``KafkaHealthChecker``
        instead (``Chains analysed - Parked records``), on the scheduled path.
        """
        now = datetime.now(UTC)
        assigned = self._assigned_partitions()
        return ChainsAnalysedHealth(
            running=self.is_running,
            pattern=self._pattern,
            assigned_partitions=len(assigned),
            assigned_topics=sorted({tp.topic for tp in assigned}),
            last_poll_age_seconds=(
                None if self._last_poll_time is None else (now - self._last_poll_time).total_seconds()
            ),
            topics={topic: (now - seen).total_seconds() for topic, seen in self._last_message_time_by_topic.items()},
            total_lag=self._total_lag(assigned),
            orphan_chain_count=self.orphan_chain_count,
            pipeline_id_fallback_count=self.pipeline_id_fallback_count,
            parked_record_count=self.parked_record_count,
        )

    def _total_lag(self, assigned: set[TopicPartition]) -> int | None:
        """Records behind the cached high-water mark; ``None`` while nothing has been fetched yet.

        ``highwater`` is the offset of the *next* record to be produced, so the lag behind a
        processed offset is ``highwater - offset - 1``. It returns the value from the last
        ``FetchResponse`` (no round trip) but asserts the partition is assigned — hence the guard.
        """
        if self.consumer is None:
            return None
        lags = [
            max(highwater - offset - 1, 0)
            for tp, offset in self._last_offset_by_tp.items()
            if tp in assigned and (highwater := self.consumer.highwater(tp)) is not None
        ]
        return sum(lags) if lags else None


chains_analysed_consumer = ChainsAnalysedConsumer()
