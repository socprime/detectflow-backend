"""Poison-record policy for the ``detectflow.chains-analysed`` consumer.

One consumer serves every correlation pipeline, so a record that can never apply must not hold its
partition: it is classified, retried under a bounded budget, then parked with its payload in
``chains_analysed_dead_letters`` and skipped. Infrastructure failures are the exception — they are
never parked and never skipped, because the park store is the database that just failed.
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import ValidationError as PydanticValidationError
from sqlalchemy import exc as sa_exc
from sqlalchemy.dialects import postgresql

from apps.core.enums import ChainsAnalysedErrorClass
from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import ChainDependencyMissingError
from apps.core.schemas import ChainsAnalysedMessage
from apps.core.settings import settings
from apps.modules.kafka import chains as chains_module
from apps.modules.kafka.chains import ChainsAnalysedConsumer, classify_chains_analysed_error
from apps.modules.postgre.attack_chains import ChainsAnalysedDeadLetterDAO, truncate_dead_letter_payload

TS = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
TP = TopicPartition("detectflow.chains-analysed", 0)


class _PgError(Exception):
    """Stand-in for the wrapped asyncpg error SQLAlchemy exposes as ``.orig``."""

    def __init__(self, sqlstate: str) -> None:
        self.sqlstate = sqlstate
        super().__init__(f"pg error {sqlstate}")


def _dbapi_error(cls: type[sa_exc.DBAPIError], sqlstate: str, **kwargs) -> sa_exc.DBAPIError:
    return cls("INSERT INTO attack_chains ...", {}, _PgError(sqlstate), **kwargs)


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


def _payload(**overrides) -> str:
    """A wire payload the consumer's own ``_parse_message`` accepts."""
    return _message(**overrides).model_dump_json()


def _record(offset: int = 7, value: str | None = None) -> ConsumerRecord:
    value = _payload() if value is None else value
    return ConsumerRecord(
        topic=TP.topic,
        partition=TP.partition,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=value,
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=len(value) if value else 0,
        headers=(),
    )


class _FakeSession:
    """``AsyncSessionLocal()`` stand-in: an async context manager over a mock session."""

    def __init__(self) -> None:
        self.commit = AsyncMock()
        self.rollback = AsyncMock()

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info) -> bool:
        return False


@pytest.fixture(autouse=True)
def _reset_error_tracker():
    """The activity throttle is class-level state; a leaked cooldown would hide a test's log call."""
    ErrorTracker.reset()
    yield
    ErrorTracker.reset()


@pytest.fixture
def consumer(monkeypatch) -> ChainsAnalysedConsumer:
    """A consumer wired to mocks: no Kafka, no database, no real sleeping."""
    instance = ChainsAnalysedConsumer()
    instance.is_running = True
    instance.consumer = SimpleNamespace(
        commit=AsyncMock(),
        seek=MagicMock(),
        getmany=AsyncMock(return_value={}),
        # Commit and seek are routed through the rebalance-aware helpers, which ask what we own.
        assignment=MagicMock(return_value={TP, TopicPartition(TP.topic, 1)}),
        highwater=MagicMock(return_value=None),
    )

    monkeypatch.setattr(chains_module, "AsyncSessionLocal", _FakeSession)
    monkeypatch.setattr(chains_module.activity_producer, "log_action", AsyncMock())

    instance.slept: list[float] = []

    async def _fake_sleep(seconds: float) -> None:
        instance.slept.append(seconds)

    monkeypatch.setattr(chains_module.asyncio, "sleep", _fake_sleep)
    return instance


def _stub_apply(monkeypatch, *, side_effect=None, return_value=True) -> AsyncMock:
    """Replace ``AttackChainsDAO`` with one whose ``apply_chains_analysed`` is the returned mock."""
    apply_mock = AsyncMock(side_effect=side_effect, return_value=return_value)
    monkeypatch.setattr(chains_module, "AttackChainsDAO", lambda _db: SimpleNamespace(apply_chains_analysed=apply_mock))
    return apply_mock


def _stub_park(monkeypatch, *, side_effect=None) -> AsyncMock:
    """Replace ``ChainsAnalysedDeadLetterDAO`` with one whose ``park`` is the returned mock."""
    park_mock = AsyncMock(side_effect=side_effect)
    monkeypatch.setattr(chains_module, "ChainsAnalysedDeadLetterDAO", lambda _db: SimpleNamespace(park=park_mock))
    return park_mock


def _fake_clock(monkeypatch, values: list[float]) -> None:
    """Deterministic ``monotonic()``; the last value repeats once the list is exhausted."""
    remaining = list(values)

    def _monotonic() -> float:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(chains_module, "monotonic", _monotonic)


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_classify_missing_dependency_is_deferrable() -> None:
    exc = ChainDependencyMissingError("high_order_sigmas", "hos_rule_id=abc")
    assert classify_chains_analysed_error(exc) is ChainsAnalysedErrorClass.DEFERRABLE


def test_classify_foreign_key_violation_is_deferrable() -> None:
    """23503 = the referenced Active Threat / HoS row is not stored yet; sync may still land it."""
    exc = _dbapi_error(sa_exc.IntegrityError, "23503")
    assert classify_chains_analysed_error(exc) is ChainsAnalysedErrorClass.DEFERRABLE


def test_classify_unique_violation_is_permanent() -> None:
    exc = _dbapi_error(sa_exc.IntegrityError, "23505")
    assert classify_chains_analysed_error(exc) is ChainsAnalysedErrorClass.PERMANENT


def test_classify_deadlock_is_infrastructure() -> None:
    exc = _dbapi_error(sa_exc.OperationalError, "40P01")
    assert classify_chains_analysed_error(exc) is ChainsAnalysedErrorClass.INFRASTRUCTURE


def test_classify_connection_invalidated_is_infrastructure() -> None:
    exc = _dbapi_error(sa_exc.OperationalError, "23505", connection_invalidated=True)
    assert classify_chains_analysed_error(exc) is ChainsAnalysedErrorClass.INFRASTRUCTURE


def test_classify_pool_timeout_is_infrastructure() -> None:
    assert classify_chains_analysed_error(sa_exc.TimeoutError()) is ChainsAnalysedErrorClass.INFRASTRUCTURE
    assert classify_chains_analysed_error(TimeoutError()) is ChainsAnalysedErrorClass.INFRASTRUCTURE
    assert classify_chains_analysed_error(sa_exc.InterfaceError("s", {}, None)) is (
        ChainsAnalysedErrorClass.INFRASTRUCTURE
    )


def test_classify_value_error_is_permanent() -> None:
    assert classify_chains_analysed_error(ValueError("badly formed UUID")) is ChainsAnalysedErrorClass.PERMANENT
    assert classify_chains_analysed_error(TypeError("not a datetime")) is ChainsAnalysedErrorClass.PERMANENT
    with pytest.raises(PydanticValidationError) as excinfo:
        ChainsAnalysedMessage.model_validate({})
    assert classify_chains_analysed_error(excinfo.value) is ChainsAnalysedErrorClass.PERMANENT


def test_classify_data_error_is_permanent() -> None:
    """An over-long ``chain_type`` or over-scaled ``llm_confidence`` can never become applicable."""
    assert classify_chains_analysed_error(_dbapi_error(sa_exc.DataError, "22001")) is (
        ChainsAnalysedErrorClass.PERMANENT
    )
    # StatementError is the DBAPIError superclass — the isinstance order must still reach it.
    assert classify_chains_analysed_error(sa_exc.StatementError("bad", "SELECT 1", {}, None)) is (
        ChainsAnalysedErrorClass.PERMANENT
    )


def test_classify_unknown_exception_is_deferrable() -> None:
    """A new failure mode must neither block the partition forever nor be discarded unretried."""
    assert classify_chains_analysed_error(RuntimeError("who knows")) is ChainsAnalysedErrorClass.DEFERRABLE


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


async def test_permanent_error_parks_on_first_attempt(consumer, monkeypatch) -> None:
    apply_mock = _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))
    park_mock = _stub_park(monkeypatch)

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is True
    assert apply_mock.await_count == 1
    assert park_mock.await_args.kwargs["error_class"] == ChainsAnalysedErrorClass.PERMANENT
    assert park_mock.await_args.kwargs["attempts"] == 1
    assert consumer.slept == []


async def test_deferrable_error_retries_to_max_attempts_then_parks(consumer, monkeypatch) -> None:
    _fake_clock(monkeypatch, [0.0])
    apply_mock = _stub_apply(monkeypatch, side_effect=ChainDependencyMissingError("high_order_sigmas", "hos=1"))
    park_mock = _stub_park(monkeypatch)

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is True
    assert apply_mock.await_count == settings.kafka_chains_analysed_max_attempts
    assert len(consumer.slept) == settings.kafka_chains_analysed_max_attempts - 1
    assert park_mock.await_args.kwargs["attempts"] == settings.kafka_chains_analysed_max_attempts
    assert park_mock.await_args.kwargs["error_class"] == ChainsAnalysedErrorClass.DEFERRABLE


async def test_deferrable_error_stops_at_the_wall_clock_deadline(consumer, monkeypatch) -> None:
    """The budget is attempts AND wall clock: a slow failure must not retry past the deadline."""
    _fake_clock(monkeypatch, [0.0, settings.kafka_chains_analysed_retry_deadline_seconds + 1])
    apply_mock = _stub_apply(monkeypatch, side_effect=ChainDependencyMissingError("high_order_sigmas", "hos=1"))
    park_mock = _stub_park(monkeypatch)

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is True
    assert apply_mock.await_count == 1
    assert park_mock.await_args.kwargs["attempts"] == 1
    assert consumer.slept == []


async def test_deferrable_error_succeeding_on_retry_is_not_parked(consumer, monkeypatch) -> None:
    _fake_clock(monkeypatch, [0.0])
    apply_mock = _stub_apply(monkeypatch, side_effect=[ChainDependencyMissingError("high_order_sigmas", "hos=1"), True])
    park_mock = _stub_park(monkeypatch)

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is True
    assert apply_mock.await_count == 2
    park_mock.assert_not_awaited()
    assert consumer.parked_record_count == 0
    assert consumer.last_message_time is not None


def test_retry_backoff_is_exponential_and_capped(consumer) -> None:
    base = settings.kafka_chains_analysed_retry_backoff_base_seconds
    ceiling = settings.kafka_chains_analysed_retry_backoff_max_seconds
    delays = [consumer._retry_backoff_seconds(attempt) for attempt in range(1, 12)]

    assert delays[0] == base
    assert delays[1] == base * 2
    assert delays[2] == base * 4
    assert all(later >= earlier for earlier, later in zip(delays, delays[1:], strict=False))
    assert max(delays) == ceiling


async def test_infrastructure_error_never_parks(consumer, monkeypatch) -> None:
    apply_mock = _stub_apply(monkeypatch, side_effect=_dbapi_error(sa_exc.OperationalError, "40P01"))
    park_mock = _stub_park(monkeypatch)

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is False
    assert apply_mock.await_count == 1
    park_mock.assert_not_awaited()
    assert consumer.parked_record_count == 0
    assert consumer._infra_stalled_since is not None


async def test_infrastructure_error_does_not_commit_the_offset(consumer, monkeypatch) -> None:
    _stub_apply(monkeypatch, side_effect=_dbapi_error(sa_exc.OperationalError, "08006"))
    _stub_park(monkeypatch)
    await _drain_one_batch(consumer, [_record()])

    consumer.consumer.commit.assert_not_awaited()


async def test_infrastructure_error_rewinds_to_the_failed_offset(consumer, monkeypatch) -> None:
    _stub_apply(monkeypatch, side_effect=_dbapi_error(sa_exc.OperationalError, "08006"))
    _stub_park(monkeypatch)
    await _drain_one_batch(consumer, [_record(offset=41)])

    consumer.consumer.seek.assert_called_once_with(TP, 41)


async def test_park_failure_blocks_the_commit(consumer, monkeypatch) -> None:
    """If the payload cannot be parked the record is retried, never skipped — that would lose it."""
    _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))
    _stub_park(monkeypatch, side_effect=_dbapi_error(sa_exc.OperationalError, "08006"))

    committable = await consumer._apply_with_retry(TP, _record(), _message())

    assert committable is False
    assert consumer.parked_record_count == 0
    assert consumer._infra_stalled_since is not None


# ---------------------------------------------------------------------------
# Skip semantics and loudness
# ---------------------------------------------------------------------------


async def _drain_one_batch(consumer: ChainsAnalysedConsumer, records, *, tp: TopicPartition = TP) -> None:
    """Run ``_consume_loop`` over exactly one batch, then let it exit."""
    await _drain_batches(consumer, {tp: list(records)})


async def _drain_batches(consumer: ChainsAnalysedConsumer, batch: dict) -> None:
    pending = [batch]

    async def _getmany(*_args, **_kwargs):
        if pending:
            return pending.pop(0)
        consumer.is_running = False
        return {}

    consumer.consumer.getmany = _getmany
    await consumer._consume_loop()


async def test_parked_record_commits_the_offset(consumer, monkeypatch) -> None:
    _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))
    _stub_park(monkeypatch)

    await _drain_one_batch(consumer, [_record(offset=12)])

    consumer.consumer.commit.assert_awaited_once_with({TP: 13})
    consumer.consumer.seek.assert_not_called()


async def test_poison_on_one_partition_does_not_block_another_partition(consumer, monkeypatch) -> None:
    """After row 11 the partitions belong to different customers — one must not stall the other."""
    other_tp = TopicPartition(TP.topic, 1)
    poison_chain_id = uuid4()

    async def _apply(msg: ChainsAnalysedMessage) -> bool:
        if msg.chain_id == poison_chain_id:
            raise ValueError("badly formed UUID")
        return True

    monkeypatch.setattr(chains_module, "AttackChainsDAO", lambda _db: SimpleNamespace(apply_chains_analysed=_apply))
    park_mock = _stub_park(monkeypatch)

    await _drain_batches(
        consumer,
        {
            TP: [_record(offset=5, value=_payload(chain_id=poison_chain_id))],
            other_tp: [_record(offset=9)],
        },
    )

    assert park_mock.await_count == 1
    committed = [call.args[0] for call in consumer.consumer.commit.await_args_list]
    assert {TP: 6} in committed
    assert {other_tp: 10} in committed
    consumer.consumer.seek.assert_not_called()


async def test_unparseable_payload_is_parked_not_silently_dropped(consumer, monkeypatch) -> None:
    """The pre-change loop committed past undecodable bytes with the payload discarded."""
    park_mock = _stub_park(monkeypatch)
    _stub_apply(monkeypatch)

    await _drain_one_batch(consumer, [_record(offset=3, value="not json")])

    assert park_mock.await_args.kwargs["error_class"] == ChainsAnalysedErrorClass.PERMANENT
    assert park_mock.await_args.kwargs["payload"] == "not json"
    assert park_mock.await_args.kwargs["chain_id"] is None
    consumer.consumer.commit.assert_awaited_once_with({TP: 4})


async def test_parked_record_increments_counter_by_error_class(consumer, monkeypatch) -> None:
    _stub_park(monkeypatch)
    _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))
    await consumer._apply_with_retry(TP, _record(), _message())

    _fake_clock(monkeypatch, [0.0])
    _stub_apply(monkeypatch, side_effect=ChainDependencyMissingError("high_order_sigmas", "hos=1"))
    await consumer._apply_with_retry(TP, _record(offset=8), _message())

    assert consumer.parked_record_count == 2
    assert consumer.parked_by_error_class["permanent"] == 1
    assert consumer.parked_by_error_class["deferrable"] == 1


async def test_parked_record_emits_one_activity_error_per_cooldown(consumer, monkeypatch) -> None:
    """Every record gets its own row and log line; the audit feed gets one entry per topic per cooldown."""
    _stub_park(monkeypatch)
    _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))

    for offset in (1, 2, 3):
        await consumer._apply_with_retry(TP, _record(offset=offset), _message())

    log_action = chains_module.activity_producer.log_action
    assert log_action.await_count == 1
    kwargs = log_action.await_args.kwargs
    assert kwargs["action"] == "error"
    assert kwargs["severity"] == "error"
    assert kwargs["source"] == "system"
    assert "chains_analysed_dead_letters" in kwargs["details"]
    assert consumer.parked_record_count == 3


async def test_committed_park_clears_an_earlier_infra_stall(consumer, monkeypatch) -> None:
    """Parking commits, so it proves the database is back — health must not stay red on a recovered loop."""
    consumer._infra_stalled_since = datetime.now(UTC) - timedelta(
        seconds=settings.kafka_chains_analysed_retry_deadline_seconds + 5
    )
    consumer._consecutive_infra_failures = 3
    _stub_park(monkeypatch)
    _stub_apply(monkeypatch, side_effect=ValueError("badly formed UUID"))

    assert await consumer._apply_with_retry(TP, _record(), _message()) is True

    assert consumer._infra_stalled_since is None
    assert consumer._consecutive_infra_failures == 0


async def test_park_upsert_is_idempotent_on_topic_partition_offset() -> None:
    """Redelivery after a rebalance must bump ``attempts``, not write a second row."""
    session = AsyncMock()
    await ChainsAnalysedDeadLetterDAO(session).park(
        kafka_topic=TP.topic,
        kafka_partition=TP.partition,
        kafka_offset=17,
        error_class="permanent",
        error_type="ValueError",
        error_message="boom",
        attempts=2,
    )

    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "ON CONFLICT ON CONSTRAINT uq_chains_dlq_tp_offset" in sql
    assert "chains_analysed_dead_letters.attempts +" in sql


@pytest.mark.parametrize(
    ("payload", "max_bytes", "expected", "truncated"),
    [
        (None, 16, None, False),
        ("plain", 16, "plain", False),
        ("nul\x00byte", 16, "nulbyte", False),
        ("x" * 32, 16, "x" * 16, True),
    ],
)
def test_truncate_dead_letter_payload_caps_bytes_and_strips_nul(payload, max_bytes, expected, truncated) -> None:
    assert truncate_dead_letter_payload(payload, max_bytes) == (expected, truncated)


def test_truncate_dead_letter_payload_never_splits_a_codepoint() -> None:
    """A cut through a multi-byte character must drop it, not store invalid UTF-8."""
    cleaned, truncated = truncate_dead_letter_payload("é" * 8, 5)
    assert truncated is True
    assert cleaned == "é" * 2


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


def test_is_healthy_ignores_message_age_when_polls_are_fresh(consumer) -> None:
    """Chains are rare by design — an old last message is not a liveness signal."""
    consumer.last_message_time = datetime.now(UTC) - timedelta(hours=6)
    consumer._last_poll_time = datetime.now(UTC)

    assert consumer.is_healthy is True


def test_is_healthy_false_when_poll_loop_is_stale(consumer) -> None:
    consumer._last_poll_time = datetime.now(UTC) - timedelta(
        seconds=settings.kafka_chains_analysed_poll_stale_seconds + 5
    )

    assert consumer.is_healthy is False


def test_is_healthy_false_when_infra_stall_exceeds_deadline(consumer) -> None:
    consumer._last_poll_time = datetime.now(UTC)
    consumer._infra_stalled_since = datetime.now(UTC) - timedelta(
        seconds=settings.kafka_chains_analysed_retry_deadline_seconds + 5
    )

    assert consumer.is_healthy is False


async def test_is_healthy_recovers_after_a_successful_apply(consumer, monkeypatch) -> None:
    consumer._last_poll_time = datetime.now(UTC)
    consumer._infra_stalled_since = datetime.now(UTC) - timedelta(
        seconds=settings.kafka_chains_analysed_retry_deadline_seconds + 5
    )
    consumer._consecutive_infra_failures = 4
    assert consumer.is_healthy is False

    _stub_apply(monkeypatch, return_value=True)
    assert await consumer._apply_with_retry(TP, _record(), _message()) is True

    assert consumer._infra_stalled_since is None
    assert consumer._consecutive_infra_failures == 0
    assert consumer.is_healthy is True


async def test_parked_records_health_check_warns_while_unreplayed(monkeypatch) -> None:
    """Operator-facing count; it runs on the scheduled path, never on the public ``/health``."""
    from apps.core.enums import HealthCheckStatus
    from apps.services.health_check.platforms import kafka as kafka_health

    monkeypatch.setattr(kafka_health, "AsyncSessionLocal", _FakeSession)
    monkeypatch.setattr(
        kafka_health,
        "ChainsAnalysedDeadLetterDAO",
        lambda _db: SimpleNamespace(count_unreplayed=AsyncMock(return_value=2)),
    )
    status, descriptions = await kafka_health.KafkaHealthChecker().check_chains_analysed_dead_letters()
    assert status == HealthCheckStatus.WARNING
    assert "2 unprocessable" in descriptions[0]

    monkeypatch.setattr(
        kafka_health,
        "ChainsAnalysedDeadLetterDAO",
        lambda _db: SimpleNamespace(count_unreplayed=AsyncMock(return_value=0)),
    )
    status, _ = await kafka_health.KafkaHealthChecker().check_chains_analysed_dead_letters()
    assert status == HealthCheckStatus.OPERATIONAL
