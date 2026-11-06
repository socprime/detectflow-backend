"""Pattern subscription, pipeline attribution and per-topic health for the chains-analysed consumer.

After the per-customer correlation-pipelines epic there is one analysed-chain topic per pipeline,
created and deleted at run time. One consumer with a regex subscription serves all of them (D2), so
these tests pin the three things that follow: restart-free discovery, tolerant attribution of a
chain to its correlation pipeline, and health that says something per topic.
"""

import inspect
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import CommitFailedError, IllegalStateError
from aiokafka.structs import ConsumerRecord

from apps.core.schemas import ChainsAnalysedMessage
from apps.core.settings import settings
from apps.modules.kafka import chains as chains_module
from apps.modules.kafka.activity_consumer import activity_consumer
from apps.modules.kafka.chains import (
    ChainsAnalysedConsumer,
    build_chains_analysed_pattern,
    resolve_correlation_pipeline_id,
)
from apps.modules.kafka.metrics import metrics_consumer

BASE = settings.kafka_chains_analysed_topic
PIPELINE_ID = UUID("11111111-2222-3333-4444-555555555555")
SCOPED = f"{BASE}.{PIPELINE_ID}"
TS = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def _message(**overrides) -> ChainsAnalysedMessage:
    defaults = {
        "chain_id": uuid4(),
        "revision": 1,
        "hos_rule_id": str(uuid4()),
        "active_threat_id": "at-1",
        "chain_type": "HOS",
        "hostname": "host-a",
        "first_seen": TS,
        "last_seen": TS,
        "window_start": TS,
        "event_count": 1,
        "rule_count": 1,
        "coverage": 0.5,
    }
    defaults.update(overrides)
    return ChainsAnalysedMessage(**defaults)


class _FakeSession:
    """``AsyncSessionLocal()`` stand-in: an async context manager over a mock session."""

    def __init__(self) -> None:
        self.commit = AsyncMock()
        self.rollback = AsyncMock()

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False


async def _no_pipelines() -> set[UUID]:
    return set()


@pytest.fixture
def consumer() -> ChainsAnalysedConsumer:
    """A consumer wired to a fake AIOKafkaConsumer — no broker, no group."""
    instance = ChainsAnalysedConsumer()
    instance.consumer = SimpleNamespace(
        commit=AsyncMock(),
        seek=MagicMock(),
        assignment=MagicMock(return_value=set()),
        highwater=MagicMock(return_value=None),
    )
    return instance


# ---------------------------------------------------------------------------
# Pattern
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("topic", "include_legacy", "matches"),
    [
        (SCOPED, True, True),
        (SCOPED, False, True),
        (f"{BASE}.{str(PIPELINE_ID).upper()}", True, True),
        # The singleton only while the migration window is open.
        (BASE, True, True),
        (BASE, False, False),
        # A customer-created topic must never be swept in by a half-anchored pattern.
        (f"{BASE}-evil", True, False),
        (f"{BASE}X", True, False),
        (f"{BASE}.{PIPELINE_ID}.extra", True, False),
        (f"{BASE}.not-a-uuid", True, False),
        (settings.kafka_chains_raw_topic, True, False),
    ],
)
def test_pattern_matches_only_this_topic_family(topic: str, include_legacy: bool, matches: bool) -> None:
    pattern = re.compile(build_chains_analysed_pattern(BASE, include_legacy))
    assert bool(pattern.match(topic)) is matches


def test_pattern_escapes_the_base_topic() -> None:
    """The dots in ``detectflow.chains-analysed`` are literals, not any-character wildcards."""
    pattern = re.compile(build_chains_analysed_pattern(BASE, True))
    assert pattern.match(BASE.replace(".", "x")) is None


# ---------------------------------------------------------------------------
# Subscription
# ---------------------------------------------------------------------------


async def _start_with_fake_consumer(monkeypatch) -> SimpleNamespace:
    """Run ``start()`` against a stand-in AIOKafkaConsumer and return it."""
    fake = SimpleNamespace(
        subscribe=MagicMock(),
        start=AsyncMock(),
        config=None,
    )
    instance = ChainsAnalysedConsumer()

    def _factory(**config):
        fake.config = config
        return fake

    monkeypatch.setattr(chains_module, "AIOKafkaConsumer", _factory)
    monkeypatch.setattr(chains_module.asyncio, "create_task", lambda coro: coro.close())
    await instance.start()
    fake.owner = instance
    return fake


async def test_start_subscribes_by_pattern(monkeypatch) -> None:
    monkeypatch.setattr(settings, "kafka_chains_analysed_topic_pattern_enabled", True)
    fake = await _start_with_fake_consumer(monkeypatch)

    kwargs = fake.subscribe.call_args.kwargs
    assert kwargs["pattern"] == build_chains_analysed_pattern(BASE, settings.kafka_chains_analysed_include_legacy_topic)
    assert isinstance(kwargs["listener"], chains_module._ChainsRebalanceListener)
    assert fake.owner.pattern == kwargs["pattern"]


async def test_start_falls_back_to_the_literal_topic_when_disabled(monkeypatch) -> None:
    """The rollback story: flip the flag, restart, same group id, same committed offsets."""
    monkeypatch.setattr(settings, "kafka_chains_analysed_topic_pattern_enabled", False)
    fake = await _start_with_fake_consumer(monkeypatch)

    fake.subscribe.assert_called_once_with([BASE])
    assert fake.owner.pattern is None


def test_metadata_max_age_reaches_only_this_consumer() -> None:
    """A pattern subscription refetches FULL-cluster metadata; nobody else pays for that."""
    assert ChainsAnalysedConsumer()._config["metadata_max_age_ms"] == (
        settings.kafka_chains_analysed_metadata_max_age_ms
    )
    assert "metadata_max_age_ms" not in metrics_consumer._config
    assert "metadata_max_age_ms" not in activity_consumer._config


# ---------------------------------------------------------------------------
# Attribution
# ---------------------------------------------------------------------------


def test_envelope_stamp_wins_over_the_topic_name() -> None:
    other = uuid4()
    resolved, used_fallback = resolve_correlation_pipeline_id(_message(correlation_pipeline_id=other), SCOPED)
    assert (resolved, used_fallback) == (other, False)


def test_topic_name_is_the_fallback() -> None:
    resolved, used_fallback = resolve_correlation_pipeline_id(_message(), SCOPED)
    assert (resolved, used_fallback) == (PIPELINE_ID, True)


def test_legacy_topic_resolves_to_none() -> None:
    resolved, used_fallback = resolve_correlation_pipeline_id(_message(), BASE)
    assert (resolved, used_fallback) == (None, True)


@pytest.mark.parametrize("stamp", ["corr-11111111-2222-3333-4444-555555555555", "", "none", 12345, [], {"a": 1}])
def test_malformed_stamp_parses_as_none_instead_of_failing_the_envelope(stamp) -> None:
    """Constraint 6: a strict UUID field would dead-letter a real chain over a producer typo."""
    msg = _message(correlation_pipeline_id=stamp)
    assert msg.correlation_pipeline_id is None


def test_malformed_stamp_is_recovered_from_the_topic_and_counted(consumer) -> None:
    parsed = _message(correlation_pipeline_id="corr-11111111-2222-3333-4444-555555555555")
    attributed = consumer._attribute_pipeline(parsed, SCOPED)

    assert attributed.correlation_pipeline_id == PIPELINE_ID
    assert consumer.pipeline_id_fallback_count == 1


def test_legacy_topic_record_is_not_counted_as_a_fallback(consumer) -> None:
    """A pre-epic record has nothing to fall back to; counting it would cry wolf."""
    attributed = consumer._attribute_pipeline(_message(), BASE)

    assert attributed.correlation_pipeline_id is None
    assert consumer.pipeline_id_fallback_count == 0


# ---------------------------------------------------------------------------
# Orphan guard
# ---------------------------------------------------------------------------


def _stub_pipeline_dao(monkeypatch, *id_sets: set[UUID]) -> list[int]:
    """Replace ``CorrelationPipelineDAO`` with one returning each id set in turn; count reads."""
    reads: list[int] = []
    remaining = list(id_sets)

    async def _list_ids() -> set[UUID]:
        reads.append(1)
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(chains_module, "CorrelationPipelineDAO", lambda _db: SimpleNamespace(list_ids=_list_ids))
    return reads


async def test_orphan_pipeline_is_nulled_and_counted(consumer, monkeypatch) -> None:
    """A deleted pipeline must not cost us the chains it produced, nor dead-letter them on the FK."""
    reads = _stub_pipeline_dao(monkeypatch, set())
    msg = _message(correlation_pipeline_id=PIPELINE_ID)

    stored = await consumer._drop_orphan_pipeline(AsyncMock(), msg, SCOPED)

    assert stored.correlation_pipeline_id is None
    assert consumer.orphan_chain_count == 1
    # One read for the cold cache, one forced re-read before declaring the pipeline gone.
    assert len(reads) == 2


async def test_known_pipeline_is_kept_and_the_set_is_cached(consumer, monkeypatch) -> None:
    reads = _stub_pipeline_dao(monkeypatch, {PIPELINE_ID})
    msg = _message(correlation_pipeline_id=PIPELINE_ID)

    for _ in range(3):
        stored = await consumer._drop_orphan_pipeline(AsyncMock(), msg, SCOPED)
        assert stored.correlation_pipeline_id == PIPELINE_ID

    assert len(reads) == 1
    assert consumer.orphan_chain_count == 0


async def test_a_pipeline_created_seconds_ago_survives_a_stale_cache(consumer, monkeypatch) -> None:
    """A cache miss forces one re-read; only then is the id declared orphaned."""
    _stub_pipeline_dao(monkeypatch, set(), {PIPELINE_ID})
    msg = _message(correlation_pipeline_id=PIPELINE_ID)

    stored = await consumer._drop_orphan_pipeline(AsyncMock(), msg, SCOPED)

    assert stored.correlation_pipeline_id == PIPELINE_ID
    assert consumer.orphan_chain_count == 0


async def test_unattributed_record_never_queries_the_pipeline_table(consumer, monkeypatch) -> None:
    reads = _stub_pipeline_dao(monkeypatch, {PIPELINE_ID})

    stored = await consumer._drop_orphan_pipeline(AsyncMock(), _message(), BASE)

    assert stored.correlation_pipeline_id is None
    assert reads == []


async def test_job_id_shaped_stamp_is_stored_not_dead_lettered(consumer, monkeypatch) -> None:
    """End-to-end of constraint 6: a misconfigured job costs us attribution, never the chain."""
    tp = TopicPartition(SCOPED, 0)
    payload = _message(correlation_pipeline_id=None).model_dump_json()
    payload = payload.replace('"correlation_pipeline_id":null', '"correlation_pipeline_id":"corr-2f4c"')
    record = ConsumerRecord(
        topic=tp.topic,
        partition=tp.partition,
        offset=3,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=payload,
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(payload),
        headers=(),
    )
    applied: list[ChainsAnalysedMessage] = []

    async def _apply(msg: ChainsAnalysedMessage) -> bool:
        applied.append(msg)
        return True

    park_mock = AsyncMock()
    monkeypatch.setattr(chains_module, "AsyncSessionLocal", _FakeSession)
    monkeypatch.setattr(chains_module, "AttackChainsDAO", lambda _db: SimpleNamespace(apply_chains_analysed=_apply))
    monkeypatch.setattr(chains_module, "ChainsAnalysedDeadLetterDAO", lambda _db: SimpleNamespace(park=park_mock))
    monkeypatch.setattr(chains_module, "CorrelationPipelineDAO", lambda _db: SimpleNamespace(list_ids=_no_pipelines))
    consumer.consumer.assignment = MagicMock(return_value={tp})

    pending = [{tp: [record]}]

    async def _getmany(*_args, **_kwargs):
        if pending:
            return pending.pop(0)
        consumer.is_running = False
        return {}

    consumer.consumer.getmany = _getmany
    consumer.is_running = True
    await consumer._consume_loop()

    park_mock.assert_not_awaited()
    consumer.consumer.commit.assert_awaited_once_with({tp: 4})
    assert consumer.pipeline_id_fallback_count == 1
    # The pipeline row does not exist in this test, so the chain is stored unattributed…
    assert applied[0].correlation_pipeline_id is None
    assert consumer.orphan_chain_count == 1


# ---------------------------------------------------------------------------
# Rebalance safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("error", [CommitFailedError(), IllegalStateError("not assigned")])
async def test_commit_after_a_rebalance_is_not_an_error(consumer, error) -> None:
    """Creating a correlation pipeline reassigns every partition — mid-batch revocation is routine."""
    consumer.consumer.commit = AsyncMock(side_effect=error)

    assert await consumer._commit_offset(TopicPartition(SCOPED, 0), 5) is False


async def test_successful_commit_records_the_offset_for_lag(consumer) -> None:
    tp = TopicPartition(SCOPED, 0)

    assert await consumer._commit_offset(tp, 5) is True

    consumer.consumer.commit.assert_awaited_once_with({tp: 6})
    assert consumer._last_offset_by_tp[tp] == 5


def test_seek_is_skipped_for_a_partition_we_no_longer_own(consumer) -> None:
    """``seek`` raises ``IllegalStateError`` on an unassigned partition; ``assignment()`` is local."""
    tp = TopicPartition(SCOPED, 0)

    consumer._seek_if_assigned(tp, 5)
    consumer.consumer.seek.assert_not_called()

    consumer.consumer.assignment = MagicMock(return_value={tp})
    consumer._seek_if_assigned(tp, 5)
    consumer.consumer.seek.assert_called_once_with(tp, 5)


async def test_revoked_topic_is_dropped_from_the_health_maps(consumer) -> None:
    kept, revoked = TopicPartition(SCOPED, 0), TopicPartition(f"{BASE}.{uuid4()}", 0)
    consumer.consumer.assignment = MagicMock(return_value={kept, revoked})
    consumer._last_offset_by_tp = {kept: 1, revoked: 2}
    consumer._last_message_time_by_topic = {kept.topic: TS, revoked.topic: TS}

    await chains_module._ChainsRebalanceListener(consumer).on_partitions_revoked({revoked})

    assert consumer._last_offset_by_tp == {kept: 1}
    assert set(consumer._last_message_time_by_topic) == {kept.topic}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def test_health_snapshot_reports_per_topic_detail(consumer) -> None:
    tp = TopicPartition(SCOPED, 0)
    consumer.is_running = True
    consumer._pattern = build_chains_analysed_pattern(BASE, True)
    consumer.consumer.assignment = MagicMock(return_value={tp})
    consumer.consumer.highwater = MagicMock(return_value=10)
    consumer._last_offset_by_tp = {tp: 4}
    consumer._last_poll_time = datetime.now(UTC)
    consumer._last_message_time_by_topic = {SCOPED: datetime.now(UTC) - timedelta(seconds=30)}
    consumer.orphan_chain_count = 2
    consumer.pipeline_id_fallback_count = 3
    consumer.parked_record_count = 1

    snapshot = consumer.health_snapshot()

    assert snapshot.running is True
    assert snapshot.pattern == consumer._pattern
    assert snapshot.assigned_partitions == 1
    assert snapshot.assigned_topics == [SCOPED]
    assert snapshot.last_poll_age_seconds < 5
    assert 25 < snapshot.topics[SCOPED] < 60
    assert snapshot.total_lag == 5
    assert (snapshot.orphan_chain_count, snapshot.pipeline_id_fallback_count) == (2, 3)
    assert snapshot.parked_record_count == 1


def test_health_snapshot_survives_a_consumer_that_never_started() -> None:
    """``/health`` is served before and after the consumer's lifetime."""
    snapshot = ChainsAnalysedConsumer().health_snapshot()

    assert snapshot.running is False
    assert snapshot.assigned_topics == []
    assert snapshot.total_lag is None
    assert snapshot.last_poll_age_seconds is None


def test_health_snapshot_is_synchronous() -> None:
    """Constraint 7: ``/health`` is public and unauthenticated — it may await no I/O at all.

    A previous incident drained the Postgres pool through a per-request recompute on this route.
    A synchronous snapshot cannot regress into a query or a broker round trip unnoticed.
    """
    assert not inspect.iscoroutinefunction(ChainsAnalysedConsumer.health_snapshot)


async def test_health_endpoint_exposes_the_chains_analysed_block(api_client, monkeypatch) -> None:
    monkeypatch.setattr(settings, "kafka_chains_analysed_enabled", True)

    body = (await api_client.get("/health")).json()

    assert set(body["kafka_chains_analysed"]) == {
        "running",
        "pattern",
        "assigned_partitions",
        "assigned_topics",
        "last_poll_age_seconds",
        "topics",
        "total_lag",
        "orphan_chain_count",
        "pipeline_id_fallback_count",
        "parked_record_count",
    }
    # Existing fields are intact (additive change).
    assert body["services"]["kafka_chains_analysed_consumer"] is not None
    assert body["status"] == "healthy"


async def test_health_endpoint_omits_the_block_when_the_consumer_is_disabled(api_client, monkeypatch) -> None:
    monkeypatch.setattr(settings, "kafka_chains_analysed_enabled", False)

    body = (await api_client.get("/health")).json()

    assert body["kafka_chains_analysed"] is None


def test_lag_ignores_partitions_we_do_not_own(consumer) -> None:
    """``highwater()`` asserts the partition is assigned — an unguarded call would raise."""
    consumer._last_offset_by_tp = {TopicPartition(SCOPED, 0): 4}
    consumer.consumer.highwater = MagicMock(side_effect=AssertionError("Partition is not assigned"))

    assert consumer.health_snapshot().total_lag is None
