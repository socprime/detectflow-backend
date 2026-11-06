"""Unit tests for attack-chain list manager behavior."""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.converters import AttackChainConverter
from apps.managers import attack_chains as attack_chains_module
from apps.managers.attack_chains import AttackChainsManager


def _pagination(search: str | None = None) -> dict:
    return {
        "skip": 0,
        "limit": 10,
        "offset": 0,
        "search": search,
        "sort": None,
        "order": "asc",
    }


@pytest.fixture
def manager() -> AttackChainsManager:
    mgr = AttackChainsManager(AsyncMock())
    mgr.dao = AsyncMock()
    return mgr


@pytest.mark.asyncio
async def test_build_chain_detail_splits_pk_and_legacy_rule_lookups(manager: AttackChainsManager) -> None:
    """Events with ``rule_id`` go to the light PK DAO; a PK miss still falls back to Sigma UUID."""
    pk_id = uuid.uuid4()
    pk_sigma = uuid.uuid4()
    legacy_sigma = uuid.uuid4()
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    chain = SimpleNamespace(
        events=[
            SimpleNamespace(
                rule_id=pk_id,
                sigma_rule_id=pk_sigma,
                sigma_rule_title="PK",
                timestamp=ts,
                technique_ids=[],
            ),
            SimpleNamespace(
                rule_id=None,
                sigma_rule_id=legacy_sigma,
                sigma_rule_title="Legacy",
                timestamp=ts,
                technique_ids=[],
            ),
        ],
        hos_tags=None,
        active_threat_id="at-1",
    )
    manager.rule_dao = AsyncMock()
    manager.rule_dao.get_technique_fields_by_ids.return_value = {}
    manager.rule_dao.get_levels_by_sigma_uuids.return_value = {}
    manager.active_threats_dao = AsyncMock()
    manager.active_threats_dao.get_by_tdm_id.return_value = MagicMock()
    manager.mitre_attack = MagicMock()

    with patch.object(attack_chains_module.AttackChainConverter, "to_detail", return_value=MagicMock()):
        await manager._build_chain_detail_response(chain)

    assert manager.rule_dao.get_technique_fields_by_ids.await_args.args[0] == {pk_id}
    # ``get_technique_fields_by_ids`` returned {} above, so the PK event is unresolved too and
    # its Sigma UUID must join the level lookup.
    assert manager.rule_dao.get_levels_by_sigma_uuids.await_args.args[0] == {pk_sigma, legacy_sigma}


def test_prepare_chain_techniques_exposes_sigma_rule_id(manager: AttackChainsManager) -> None:
    # The per-rule "View in SIEM" globe needs the rule's Sigma UUID, so each technique rule row
    # must carry sigma_rule_id (the value the frontend passes to /view-in-siem).
    rid = "11111111-1111-1111-1111-111111111111"
    events = [
        SimpleNamespace(
            sigma_rule_title="RuleX",
            sigma_rule_id=rid,
            rule_id=None,
            timestamp=datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC),
            technique_ids=["T1547"],
        ),
        SimpleNamespace(
            sigma_rule_title="RuleX",
            sigma_rule_id=rid,
            rule_id=None,
            timestamp=datetime(2026, 1, 1, 10, 5, 0, tzinfo=UTC),
            technique_ids=["T1547"],
        ),
    ]
    techniques = manager._prepare_chain_techniques(events, {})
    rule_rows = techniques["T1547"]["rules"]
    assert rule_rows[0]["sigma_rule_id"] == rid
    assert rule_rows[0]["rule_name"] == "RuleX"
    assert rule_rows[0]["tagged_events"] == 2  # aggregated across both events


@pytest.mark.asyncio
async def test_list_chains_hides_filtered_out_by_default(manager: AttackChainsManager) -> None:
    manager.dao.count_filtered_out_chains.return_value = 3
    manager.dao.list_chains.return_value = ([], 5)

    groups, total, filtered_out_count = await manager.list_chains(
        _pagination(search="host-a"),
        "none",
        severity=["high"],
        status=["new"],
    )

    assert groups == {"default": []}
    assert total == 5
    assert filtered_out_count == 3
    manager.dao.count_filtered_out_chains.assert_awaited_once_with(correlation_pipeline_id=None)
    manager.dao.list_chains.assert_awaited_once()
    assert manager.dao.list_chains.await_args.kwargs["show_filtered_out"] is False
    # Platform-wide by default: no pipeline predicate, so chains whose pipeline was deleted stay in.
    assert manager.dao.list_chains.await_args.kwargs["correlation_pipeline_id"] is None


@pytest.mark.asyncio
async def test_list_chains_can_include_filtered_out(manager: AttackChainsManager) -> None:
    manager.dao.list_chains_grouped.return_value = ({}, 7)

    groups, total, filtered_out_count = await manager.list_chains(
        _pagination(),
        "hostname",
        show_filtered_out=True,
    )

    assert groups == {}
    assert total == 7
    assert filtered_out_count is None
    manager.dao.count_filtered_out_chains.assert_not_awaited()
    manager.dao.list_chains_grouped.assert_awaited_once()
    assert manager.dao.list_chains_grouped.await_args.kwargs["show_filtered_out"] is True


def _chain_row(correlation_pipeline_id=None, correlation_pipeline_name=None):
    """A ``_chains_list_select`` row: (chain, hos_name, threat_name, severity, pipeline_name)."""
    now = datetime(2026, 8, 1, tzinfo=UTC)
    chain = SimpleNamespace(
        id=uuid.uuid4(),
        chain_type="HOS",
        hostname="host-a",
        username=None,
        status="new",
        correlation_pipeline_id=correlation_pipeline_id,
        first_seen=now,
        last_seen=now,
        event_count=3,
        rule_count=2,
        match=100,
        created_at=now,
        updated_at=now,
    )
    return (chain, "Chain name", "Threat", "high", correlation_pipeline_name)


def test_chain_row_carries_its_pipeline_as_a_one_element_array() -> None:
    """D10: a chain has exactly one correlation pipeline; the array keeps the UI contract stable."""
    pipeline_id = uuid.uuid4()
    row = AttackChainConverter.to_list_item(_chain_row(pipeline_id, "tenant-a"))

    assert row.correlation_pipeline_id == pipeline_id
    assert row.correlation_pipeline_name == "tenant-a"
    assert [(ref.id, ref.name) for ref in row.correlation_pipelines] == [(pipeline_id, "tenant-a")]


def test_unattributed_chain_row_has_an_empty_pipeline_array() -> None:
    """Legacy chain, or one whose pipeline was deleted (the FK is ON DELETE SET NULL)."""
    row = AttackChainConverter.to_list_item(_chain_row())

    assert row.correlation_pipeline_id is None
    assert row.correlation_pipelines == []


@pytest.mark.asyncio
async def test_list_chains_scopes_the_hidden_count_to_the_same_pipeline(manager: AttackChainsManager) -> None:
    """A scoped list must not report how many chains ANOTHER tenant's pipeline hid."""
    pipeline_id = uuid.uuid4()
    manager.dao.count_filtered_out_chains.return_value = 2
    manager.dao.list_chains.return_value = ([], 4)

    await manager.list_chains(_pagination(), "none", correlation_pipeline_id=pipeline_id)

    manager.dao.count_filtered_out_chains.assert_awaited_once_with(correlation_pipeline_id=pipeline_id)
    assert manager.dao.list_chains.await_args.kwargs["correlation_pipeline_id"] == pipeline_id


@pytest.mark.asyncio
async def test_graph_data_is_the_five_platform_wide_blocks(manager: AttackChainsManager) -> None:
    """The dashboard graph is unscoped: every chain/event, plus the threat catalogue."""
    manager.dao.get_graph_daily_series.return_value = []
    manager.dao.get_attack_chains_count.return_value = 5
    manager.dao.get_graph_affected_hostnames_daily_series.return_value = []
    manager.dao.get_affected_hostnames_count.return_value = 2
    manager.dao.get_graph_tagged_events_daily_series.return_value = []
    manager.dao.get_tagged_events_count.return_value = 9
    manager.dao.get_graph_matched_rules_daily_series.return_value = []
    manager.dao.get_matched_rules_counts.return_value = (1, 11)
    manager.active_threats_dao = AsyncMock()
    manager.active_threats_dao.get_monitored_threats_count.return_value = 42
    manager.active_threats_dao.get_unblocked_threats_count.return_value = 40
    manager.active_threats_dao.get_graph_daily_series.return_value = []

    graph = await manager.get_graph_data()

    assert [block.name for block in graph.blocks] == [
        "ATTACK CHAINS",
        "MONITORED THREATS",
        "AFFECTED HOSTNAMES",
        "TAGGED EVENTS",
        "MATCHED RULES",
    ]
    assert graph.blocks[0].count_all == 5
    assert graph.blocks[1].count_all == 42
    assert graph.blocks[1].count_active == 40
    assert graph.blocks[4].count_all == 11
    assert graph.blocks[4].count_active == 1


@pytest.mark.asyncio
async def test_patch_chain_commits_before_notify(manager: AttackChainsManager, monkeypatch) -> None:
    chain_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    user = SimpleNamespace(email="user@test.com")
    body = SimpleNamespace(status="investigating")
    row = SimpleNamespace(status="new")
    updated = SimpleNamespace(status="investigating")

    manager.dao.get_chain_by_id.return_value = row
    manager.dao.update_chain_status.return_value = updated
    manager.db.commit = AsyncMock()
    manager._build_chain_detail_response = AsyncMock(return_value=SimpleNamespace())

    call_order: list[str] = []

    async def commit_side_effect() -> None:
        call_order.append("commit")

    manager.db.commit.side_effect = commit_side_effect

    notify_mock = MagicMock(side_effect=lambda: call_order.append("notify") or 1)
    monkeypatch.setattr(attack_chains_module.attack_chains_list_invalidation_service, "notify_changed", notify_mock)
    monkeypatch.setattr(attack_chains_module.activity_producer, "log_action", AsyncMock())

    await manager.patch_chain(chain_id, body, user)

    manager.db.commit.assert_awaited_once()
    notify_mock.assert_called_once()
    assert call_order == ["commit", "notify"]


@pytest.mark.asyncio
async def test_patch_chain_skips_notify_on_same_status(manager: AttackChainsManager, monkeypatch) -> None:
    chain_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    user = SimpleNamespace(email="user@test.com")
    body = SimpleNamespace(status="new")
    row = SimpleNamespace(status="new")

    manager.dao.get_chain_by_id.return_value = row
    manager.db.commit = AsyncMock()
    manager._build_chain_detail_response = AsyncMock(return_value=SimpleNamespace())

    notify_mock = MagicMock()
    monkeypatch.setattr(attack_chains_module.attack_chains_list_invalidation_service, "notify_changed", notify_mock)

    await manager.patch_chain(chain_id, body, user)

    manager.dao.update_chain_status.assert_not_awaited()
    manager.db.commit.assert_not_awaited()
    notify_mock.assert_not_called()
