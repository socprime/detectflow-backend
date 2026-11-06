"""Unit tests for ``AttackChainsDAO.apply_chains_analysed`` merge rules."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from apps.core.exceptions import ChainDependencyMissingError
from apps.core.models import AttackChain, AttackChainEvent
from apps.core.schemas import ChainsAnalysedEvent, ChainsAnalysedMessage
from apps.modules.postgre.attack_chains import AttackChainsDAO, _ChainsAnalysedUpdatePlan

HOS_ID = UUID("770e8400-e29b-41d4-a716-446655440000")
PIPELINE_A = UUID("11111111-2222-3333-4444-555555555555")
PIPELINE_B = UUID("66666666-7777-8888-9999-aaaaaaaaaaaa")
TS_EARLY = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
TS_MID = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
TS_LATE = datetime(2026, 1, 3, 12, 0, tzinfo=UTC)

_NON_STICKY_STATUSES = ("filtered-out", "closed", "confirmed", "false-positive", "resolved")


def _chain(**overrides) -> AttackChain:
    defaults = {
        "id": uuid4(),
        "hos_rule_id": HOS_ID,
        "active_threat_id": "at-1",
        "chain_type": "HOS",
        "hostname": "host-a",
        "username": "alice",
        "status": "new",
        "first_seen": TS_EARLY,
        "last_seen": TS_MID,
        "window_start": TS_EARLY,
        "event_count": 2,
        "rule_count": 2,
        "seqs": [{"seq": 1, "hit": True}, {"seq": 2, "hit": False}],
        "match": 90,
        "hos_tags": None,
        "coverage": Decimal("0.80"),
        "pipeline_names": ["pipe-a"],
        "ai_summary": "stored summary",
        "llm_verdict": "TRUE",
        "llm_confidence": Decimal("0.90"),
        "llm_review": {"stored": True},
        "revision": 1,
        "last_wire_chain_id": None,
        "analysed_at": TS_MID,
    }
    defaults.update(overrides)
    return AttackChain(**defaults)


def _message(**overrides) -> ChainsAnalysedMessage:
    defaults = {
        "chain_id": uuid4(),
        "revision": 2,
        # None keeps every pre-existing case on the legacy/unattributed path.
        "correlation_pipeline_id": None,
        "hos_rule_id": str(HOS_ID),
        "active_threat_id": "at-1",
        "chain_type": "HOS",
        "hostname": "host-a",
        "first_seen": TS_EARLY,
        "last_seen": TS_LATE,
        "window_start": TS_EARLY,
        "event_count": 5,
        "rule_count": 4,
        "seqs": [{"seq": 1, "hit": True}, {"seq": 2, "hit": True}],
        "coverage": 0.95,
        "pipeline_names": ["pipe-b"],
        "username": "bob",
        "ai_summary": "incoming summary",
        "llm_verdict": "TRUE",
        "llm_confidence": 0.75,
        "llm_review": {"incoming": True},
        "analysed_at": TS_LATE,
        "events": [
            ChainsAnalysedEvent(
                event_id="ev-1",
                sigma_rule_id=str(uuid4()),
                seq=1,
                timestamp=TS_LATE,
            )
        ],
    }
    defaults.update(overrides)
    return ChainsAnalysedMessage(**defaults)


def _dao_for_apply(*, sticky: AttackChain | None = None, insert_id: UUID | None = None):
    session = AsyncMock()
    session.add = MagicMock()
    dao = AttackChainsDAO(session)
    dao._resolve_hos_rule_fk = AsyncMock(return_value=HOS_ID)
    dao.get_sticky_chain = AsyncMock(return_value=sticky)
    if insert_id is not None:
        dao._resolve_insert_chain_id = AsyncMock(return_value=insert_id)
    else:
        dao._resolve_insert_chain_id = AsyncMock(side_effect=lambda proposed: proposed)
    dao._upsert_chain_events = AsyncMock()
    dao._refresh_chain_counts = AsyncMock()
    return session, dao


@pytest.mark.parametrize(
    ("stored", "incoming", "expected"),
    [
        ("new", "filtered-out", _ChainsAnalysedUpdatePlan(mode="skip")),
        ("investigating", "filtered-out", _ChainsAnalysedUpdatePlan(mode="skip")),
        ("filtered-out", "new", _ChainsAnalysedUpdatePlan(mode="full", status="new")),
        ("filtered-out", "filtered-out", _ChainsAnalysedUpdatePlan(mode="full", status="filtered-out")),
        ("new", "new", _ChainsAnalysedUpdatePlan(mode="full", status="new")),
        ("investigating", "new", _ChainsAnalysedUpdatePlan(mode="full", status="investigating")),
        ("investigating", "investigating", _ChainsAnalysedUpdatePlan(mode="full", status="investigating")),
    ],
)
def test_resolve_update_plan(stored: str, incoming: str, expected: _ChainsAnalysedUpdatePlan) -> None:
    assert AttackChainsDAO._resolve_update_plan(stored, incoming) == expected


def test_merge_helpers() -> None:
    assert AttackChainsDAO._merge_max_int(90, 80) == 90
    assert AttackChainsDAO._merge_max_int(90, 95) == 95
    assert AttackChainsDAO._merge_min_datetime(TS_MID, TS_EARLY) == TS_EARLY
    assert AttackChainsDAO._merge_max_datetime(TS_MID, TS_LATE) == TS_LATE


def test_union_seq_hits_ors_true() -> None:
    stored = [{"seq": 1, "hit": True}, {"seq": 2, "hit": False}]
    incoming = [{"seq": 1, "hit": False}, {"seq": 2, "hit": True}]
    assert AttackChainsDAO._union_seq_hits(stored, incoming) == [
        {"seq": 1, "hit": True},
        {"seq": 2, "hit": True},
    ]


def test_coverage_from_seqs_uses_percent_scale() -> None:
    assert AttackChainsDAO._coverage_from_seqs([{"seq": 1, "hit": True}, {"seq": 2, "hit": False}]) == Decimal("50")
    assert AttackChainsDAO._coverage_from_seqs([{"seq": 1, "hit": True}, {"seq": 2, "hit": True}]) == Decimal("100")


@pytest.mark.asyncio
async def test_apply_inserts_when_no_active_chain() -> None:
    msg = _message()
    session, dao = _dao_for_apply(sticky=None, insert_id=msg.chain_id)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert added.id == msg.chain_id
    assert added.status == "new"
    assert added.event_count == 0
    assert added.rule_count == 0
    assert added.last_wire_chain_id == msg.chain_id
    dao._resolve_insert_chain_id.assert_awaited_once_with(msg.chain_id)
    dao._upsert_chain_events.assert_awaited_once_with(msg.chain_id, msg.events)
    dao._refresh_chain_counts.assert_awaited_once_with(msg.chain_id, added)


@pytest.mark.asyncio
async def test_apply_inserts_when_only_closed_chain_exists() -> None:
    msg = _message()
    new_chain_id = uuid4()
    session, dao = _dao_for_apply(sticky=None, insert_id=new_chain_id)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert added.id == new_chain_id
    assert added.id != msg.chain_id
    dao._resolve_insert_chain_id.assert_awaited_once_with(msg.chain_id)
    dao._upsert_chain_events.assert_awaited_once_with(new_chain_id, msg.events)
    dao._refresh_chain_counts.assert_awaited_once_with(new_chain_id, added)


@pytest.mark.asyncio
async def test_resolve_insert_chain_id_reuses_proposed_when_pk_free() -> None:
    session = AsyncMock()
    session.get = AsyncMock(return_value=None)
    dao = AttackChainsDAO(session)
    proposed = uuid4()

    assert await dao._resolve_insert_chain_id(proposed) == proposed
    session.get.assert_awaited_once_with(AttackChain, proposed)


@pytest.mark.asyncio
async def test_resolve_insert_chain_id_generates_uuid_when_pk_taken() -> None:
    session = AsyncMock()
    session.get = AsyncMock(return_value=_chain(status="closed"))
    dao = AttackChainsDAO(session)
    proposed = uuid4()

    resolved = await dao._resolve_insert_chain_id(proposed)

    assert resolved != proposed
    session.get.assert_awaited_once_with(AttackChain, proposed)


@pytest.mark.asyncio
async def test_apply_inserts_when_stored_filtered_out_is_not_sticky() -> None:
    """A later window with LLM TRUE must INSERT; it must not promote a hidden filtered-out row."""
    msg = _message(window_start=TS_LATE, revision=1, llm_verdict="TRUE")
    session, dao = _dao_for_apply(sticky=None, insert_id=msg.chain_id)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert added.status == "new"
    assert added.window_start == TS_LATE
    assert added.id == msg.chain_id
    dao.get_sticky_chain.assert_awaited_once_with(HOS_ID, msg.hostname, None)
    dao._upsert_chain_events.assert_awaited_once_with(msg.chain_id, msg.events)


@pytest.mark.asyncio
async def test_apply_full_update_keeps_investigating_over_incoming_new() -> None:
    active_id = uuid4()
    msg = _message()
    row = _chain(id=active_id, status="investigating", last_wire_chain_id=msg.chain_id)
    _, dao = _dao_for_apply(sticky=row)

    await dao.apply_chains_analysed(msg)

    assert row.status == "investigating"


@pytest.mark.asyncio
async def test_apply_skip_preserves_status_and_llm_but_merges_metrics() -> None:
    active_id = uuid4()
    msg = _message(
        llm_verdict="FALSE_POSITIVE",
        ai_summary="drop me",
        last_seen=TS_EARLY,
        seqs=[{"seq": 1, "hit": True}, {"seq": 2, "hit": False}],
        username="bob",
        window_start=TS_LATE,
        pipeline_names=["pipe-b"],
    )
    row = _chain(
        id=active_id,
        status="new",
        match=90,
        first_seen=TS_EARLY,
        last_seen=TS_MID,
        ai_summary="keep me",
        llm_verdict="TRUE",
        username="alice",
        window_start=TS_EARLY,
        pipeline_names=["pipe-a"],
        last_wire_chain_id=msg.chain_id,
    )
    _, dao = _dao_for_apply(sticky=row)

    await dao.apply_chains_analysed(msg)

    assert row.status == "new"
    assert row.ai_summary == "keep me"
    assert row.llm_verdict == "TRUE"
    assert row.first_seen == TS_EARLY
    assert row.last_seen == TS_MID
    assert row.match == 90
    assert row.username == "alice"
    assert row.window_start == TS_EARLY
    assert row.pipeline_names == ["pipe-a"]
    assert row.revision == msg.revision
    assert row.last_wire_chain_id == msg.chain_id
    dao._upsert_chain_events.assert_awaited_once_with(active_id, msg.events)
    dao._refresh_chain_counts.assert_awaited_once_with(active_id, row)


@pytest.mark.asyncio
async def test_apply_routes_events_to_active_chain_not_msg_chain_id() -> None:
    active_id = uuid4()
    msg = _message(chain_id=uuid4())
    row = _chain(id=active_id)
    _, dao = _dao_for_apply(sticky=row)

    await dao.apply_chains_analysed(msg)

    dao._upsert_chain_events.assert_awaited_once_with(active_id, msg.events)
    assert msg.chain_id != active_id


@pytest.mark.asyncio
async def test_apply_skips_when_stored_revision_is_newer_or_equal() -> None:
    wire_id = uuid4()
    row = _chain(revision=5, ai_summary="keep me", last_wire_chain_id=wire_id)
    msg = _message(chain_id=wire_id, revision=4)
    session, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is False
    assert row.ai_summary == "keep me"
    session.flush.assert_not_awaited()
    dao._upsert_chain_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_skips_when_stored_revision_equals_incoming() -> None:
    wire_id = uuid4()
    row = _chain(revision=3, ai_summary="keep me", last_wire_chain_id=wire_id)
    msg = _message(chain_id=wire_id, revision=3)
    _, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is False
    assert row.ai_summary == "keep me"
    dao._upsert_chain_events.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_chain_counts_sets_totals_from_events() -> None:
    session = AsyncMock()
    chain_id = uuid4()
    row = _chain(id=chain_id, event_count=99, rule_count=99)
    counts_row = MagicMock()
    counts_row.event_count = 7
    counts_row.rule_count = 3
    session.execute = AsyncMock(return_value=MagicMock(one=MagicMock(return_value=counts_row)))

    dao = AttackChainsDAO(session)
    await dao._refresh_chain_counts(chain_id, row)

    assert row.event_count == 7
    assert row.rule_count == 3
    session.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_sets_correlation_pipeline_id_on_insert() -> None:
    """The stamp resolved by the consumer is what attributes the chain to a customer."""
    msg = _message(correlation_pipeline_id=PIPELINE_A)
    session, dao = _dao_for_apply(sticky=None, insert_id=msg.chain_id)

    await dao.apply_chains_analysed(msg)

    assert session.add.call_args.args[0].correlation_pipeline_id == PIPELINE_A
    dao.get_sticky_chain.assert_awaited_once_with(HOS_ID, msg.hostname, PIPELINE_A)


@pytest.mark.asyncio
async def test_apply_scopes_active_chain_lookup_by_correlation_pipeline() -> None:
    """Two customers sharing a hostname and a catalogue HoS must not collapse onto one row."""
    session, dao = _dao_for_apply()
    stored = {(HOS_ID, "host-a", PIPELINE_A): _chain(id=uuid4())}

    async def _lookup(hos_rule_id, hostname, correlation_pipeline_id):
        return stored.get((hos_rule_id, hostname, correlation_pipeline_id))

    dao.get_sticky_chain = AsyncMock(side_effect=_lookup)

    await dao.apply_chains_analysed(_message(correlation_pipeline_id=PIPELINE_A))
    session.add.assert_not_called()

    msg_b = _message(correlation_pipeline_id=PIPELINE_B)
    await dao.apply_chains_analysed(msg_b)

    session.add.assert_called_once()
    assert session.add.call_args.args[0].correlation_pipeline_id == PIPELINE_B
    assert session.add.call_args.args[0].id == msg_b.chain_id


@pytest.mark.parametrize("status", ["new", "investigating"])
@pytest.mark.asyncio
async def test_apply_merges_next_window_revision_one_into_sticky_row(status: str) -> None:
    """Later window revision=1 glues onto the open row; window_start stays the first window."""
    stored_id = uuid4()
    old_wire = uuid4()
    new_wire = uuid4()
    row = _chain(
        id=stored_id,
        status=status,
        window_start=TS_EARLY,
        revision=7,
        last_wire_chain_id=old_wire,
        last_seen=TS_MID,
        first_seen=TS_EARLY,
    )
    msg = _message(
        chain_id=new_wire,
        window_start=TS_LATE,
        revision=1,
        first_seen=TS_LATE,
        last_seen=TS_LATE,
    )
    session, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_not_called()
    dao._upsert_chain_events.assert_awaited_once_with(stored_id, msg.events)
    dao._refresh_chain_counts.assert_awaited_once_with(stored_id, row)
    assert row.last_seen == TS_LATE
    assert row.window_start == TS_EARLY
    assert row.status == status
    assert row.last_wire_chain_id == new_wire
    assert row.revision == 1


@pytest.mark.asyncio
async def test_apply_full_update_keeps_pipeline_when_incoming_is_null() -> None:
    """A lagging producer that omits the stamp must not unattribute an existing chain."""
    msg = _message(correlation_pipeline_id=None)
    row = _chain(correlation_pipeline_id=PIPELINE_A, last_wire_chain_id=msg.chain_id)
    _, dao = _dao_for_apply(sticky=row)

    await dao.apply_chains_analysed(msg)

    assert row.correlation_pipeline_id == PIPELINE_A


@pytest.mark.asyncio
async def test_apply_keeps_null_pipeline_for_legacy_message() -> None:
    """Legacy singleton records stay unattributed rather than being dropped."""
    msg = _message(correlation_pipeline_id=None)
    session, dao = _dao_for_apply(sticky=None, insert_id=msg.chain_id)

    await dao.apply_chains_analysed(msg)

    assert session.add.call_args.args[0].correlation_pipeline_id is None
    dao.get_sticky_chain.assert_awaited_once_with(HOS_ID, msg.hostname, None)


@pytest.mark.asyncio
async def test_missing_hos_raises_chain_dependency_missing_error() -> None:
    """The consumer classifies this type as DEFERRABLE, so it must not be a bare ``ValueError``."""
    dao = AttackChainsDAO(AsyncMock())
    dao._resolve_hos_rule_fk = AsyncMock(return_value=None)
    msg = _message()

    with pytest.raises(ChainDependencyMissingError, match="No high_order_sigmas row") as excinfo:
        await dao.apply_chains_analysed(msg)

    assert excinfo.value.dependency == "high_order_sigmas"
    assert excinfo.value.identifier == f"hos_rule_id={msg.hos_rule_id}"
    assert not isinstance(excinfo.value, ValueError)


@pytest.mark.parametrize("revision", [3, 2])
@pytest.mark.asyncio
async def test_apply_duplicate_delivery_same_wire_same_or_lower_revision_is_noop(revision: int) -> None:
    wire_id = uuid4()
    row = _chain(revision=3, last_wire_chain_id=wire_id, event_count=2)
    msg = _message(chain_id=wire_id, revision=revision)
    _, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is False
    dao._upsert_chain_events.assert_not_awaited()
    dao._refresh_chain_counts.assert_not_awaited()


@pytest.mark.parametrize("status", _NON_STICKY_STATUSES)
@pytest.mark.asyncio
async def test_apply_inserts_when_only_non_sticky_row_exists(status: str) -> None:
    """filtered-out / closed / confirmed / false-positive / resolved are not sticky."""
    msg = _message(window_start=TS_LATE, revision=1)
    session, dao = _dao_for_apply(sticky=None, insert_id=msg.chain_id)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert added.id == msg.chain_id
    assert added.status == "new"
    assert added.window_start == TS_LATE
    dao.get_sticky_chain.assert_awaited_once_with(HOS_ID, msg.hostname, None)


@pytest.mark.asyncio
async def test_apply_inserts_new_pk_when_non_sticky_holds_msg_chain_id() -> None:
    msg = _message()
    new_id = uuid4()
    session, dao = _dao_for_apply(sticky=None, insert_id=new_id)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    assert session.add.call_args.args[0].id == new_id
    assert session.add.call_args.args[0].id != msg.chain_id


@pytest.mark.asyncio
async def test_apply_glues_cross_window_filtered_out_onto_investigating() -> None:
    stored_id = uuid4()
    old_wire = uuid4()
    new_wire = uuid4()
    row = _chain(
        id=stored_id,
        status="investigating",
        window_start=TS_EARLY,
        revision=7,
        last_wire_chain_id=old_wire,
        ai_summary="keep me",
        llm_verdict="TRUE",
        llm_review={"stored": True},
    )
    msg = _message(
        chain_id=new_wire,
        window_start=TS_LATE,
        revision=1,
        llm_verdict="FALSE_POSITIVE",
        ai_summary="drop me",
        llm_review={"incoming": True},
        first_seen=TS_LATE,
        last_seen=TS_LATE,
    )
    session, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_not_called()
    assert row.status == "investigating"
    assert row.ai_summary == "keep me"
    assert row.llm_verdict == "TRUE"
    assert row.llm_review == {"stored": True}
    assert row.window_start == TS_EARLY
    assert row.last_seen == TS_LATE
    assert row.last_wire_chain_id == new_wire
    assert row.revision == 1
    dao._upsert_chain_events.assert_awaited_once_with(stored_id, msg.events)


@pytest.mark.asyncio
async def test_apply_inserts_separate_rows_for_two_pipelines_same_host_hos() -> None:
    session, dao = _dao_for_apply(sticky=None)

    msg_a = _message(correlation_pipeline_id=PIPELINE_A)
    msg_b = _message(correlation_pipeline_id=PIPELINE_B)
    await dao.apply_chains_analysed(msg_a)
    await dao.apply_chains_analysed(msg_b)

    assert session.add.call_count == 2
    ids = {call.args[0].id for call in session.add.call_args_list}
    pipelines = {call.args[0].correlation_pipeline_id for call in session.add.call_args_list}
    assert ids == {msg_a.chain_id, msg_b.chain_id}
    assert pipelines == {PIPELINE_A, PIPELINE_B}


@pytest.mark.asyncio
async def test_apply_unions_seq_hits_on_cross_window_merge() -> None:
    stored_id = uuid4()
    old_wire = uuid4()
    new_wire = uuid4()
    row = _chain(
        id=stored_id,
        seqs=[{"seq": 1, "hit": True}, {"seq": 2, "hit": False}],
        last_wire_chain_id=old_wire,
        revision=7,
        coverage=Decimal("50"),
        match=50,
    )
    msg = _message(
        chain_id=new_wire,
        revision=1,
        window_start=TS_LATE,
        seqs=[{"seq": 1, "hit": False}, {"seq": 2, "hit": True}],
        coverage=50,
    )
    _, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    assert row.seqs == [{"seq": 1, "hit": True}, {"seq": 2, "hit": True}]
    assert row.coverage == Decimal("100")
    assert row.match == 100
    assert row.window_start == TS_EARLY
    dao._upsert_chain_events.assert_awaited_once_with(stored_id, msg.events)


@pytest.mark.asyncio
async def test_apply_null_last_wire_chain_id_still_merges_cross_window() -> None:
    """Post-migrate NULL is not equal to msg.chain_id, so a later window still glues."""
    stored_id = uuid4()
    row = _chain(id=stored_id, revision=7, last_wire_chain_id=None, window_start=TS_EARLY)
    msg = _message(chain_id=uuid4(), revision=1, window_start=TS_LATE, last_seen=TS_LATE)
    session, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_not_called()
    assert row.last_wire_chain_id == msg.chain_id
    assert row.revision == 1
    dao._upsert_chain_events.assert_awaited_once_with(stored_id, msg.events)


@pytest.mark.asyncio
async def test_apply_skip_same_window_coverage_follows_union() -> None:
    """Same-window SKIP unions seq hits; coverage must follow the union, not the FP envelope."""
    wire_id = uuid4()
    stored_id = uuid4()
    row = _chain(
        id=stored_id,
        status="investigating",
        last_wire_chain_id=wire_id,
        revision=1,
        seqs=[{"seq": 1, "hit": True}, {"seq": 2, "hit": False}],
        coverage=Decimal("50"),
        match=50,
        ai_summary="keep me",
        llm_verdict="TRUE",
    )
    msg = _message(
        chain_id=wire_id,
        revision=2,
        llm_verdict="FALSE_POSITIVE",
        ai_summary="drop me",
        seqs=[{"seq": 1, "hit": False}, {"seq": 2, "hit": True}],
        coverage=50,
    )
    session, dao = _dao_for_apply(sticky=row)

    applied = await dao.apply_chains_analysed(msg)

    assert applied is True
    session.add.assert_not_called()
    assert row.status == "investigating"
    assert row.ai_summary == "keep me"
    assert row.llm_verdict == "TRUE"
    assert row.seqs == [{"seq": 1, "hit": True}, {"seq": 2, "hit": True}]
    assert row.coverage == Decimal("100")
    assert row.match == 100
    dao._upsert_chain_events.assert_awaited_once_with(stored_id, msg.events)


@pytest.mark.asyncio
async def test_upsert_chain_events_persists_rule_id() -> None:
    """Kafka ``rule_db_id`` must land in INSERT/ON CONFLICT, not be dropped by extra='ignore'."""
    session = AsyncMock()
    dao = AttackChainsDAO(session)
    chain_id = uuid4()
    rule_id = uuid4()
    sigma_id = uuid4()
    event = ChainsAnalysedEvent(
        event_id="ev-1",
        sigma_rule_id=str(sigma_id),
        rule_id=rule_id,
        seq=1,
        timestamp=TS_LATE,
    )

    await dao._upsert_chain_events(chain_id, [event])

    statement = session.execute.await_args.args[0]
    assert statement.compile().params["rule_id_m0"] == rule_id
    sql = str(statement.compile(dialect=postgresql.dialect()))
    # Sticky on conflict: a revision that omits ``rule_db_id`` must not erase a known PK.
    assert "rule_id = coalesce(excluded.rule_id, attack_chain_events.rule_id)" in sql.lower()


@pytest.mark.asyncio
async def test_upsert_chain_events_writes_null_rule_id_for_legacy_events() -> None:
    session = AsyncMock()
    dao = AttackChainsDAO(session)
    event = ChainsAnalysedEvent(
        event_id="ev-legacy",
        sigma_rule_id=str(uuid4()),
        seq=1,
        timestamp=TS_LATE,
    )

    await dao._upsert_chain_events(uuid4(), [event])

    statement = session.execute.await_args.args[0]
    assert statement.compile().params["rule_id_m0"] is None


def test_chain_event_rule_id_has_no_foreign_key_onto_rules() -> None:
    """Deleting a rule must not delete the evidence that it fired.

    ``rule_id`` is a bare UUID, deliberately not an FK: a cascade would erase correlated events
    from analysed chains, and a RESTRICT would make rule deletion fail on old chains. The chain
    detail view degrades a dangling PK to ``rule_id: null`` + the wire ``sigma_rule_title``.
    """
    column = AttackChainEvent.__table__.c.rule_id
    assert column.nullable is True
    assert column.foreign_keys == set()
