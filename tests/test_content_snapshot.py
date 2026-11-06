from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from apps.integrations.providers.socprime_tdm import TdmActiveThreat, TdmRule
from apps.modules.postgre import init_content
from apps.modules.postgre.init_content import (
    CONTENT_DIR,
    SnapshotProvider,
    apply_content_snapshot,
    load_snapshot,
    missing_or_newer,
)
from apps.modules.utils import extract_referenced_sigma_uuids, get_sigma_uuid_from_sigma_body

T0 = datetime(2026, 10, 1, tzinfo=UTC)


def _threat(tdm_id: str, updated: datetime) -> TdmActiveThreat:
    return TdmActiveThreat(id=tdm_id, name=tdm_id, created=T0, updated=updated)


def _rule(case_id: str, sigma_uuid: str, updated: datetime = T0) -> TdmRule:
    return TdmRule(id=case_id, name=case_id, body=f"title: {case_id}\nid: {sigma_uuid}\n", created=T0, updated=updated)


def test_missing_or_newer_never_rolls_back_a_local_copy():
    threats = [_threat("new", T0), _threat("older-locally", T0), _threat("same", T0), _threat("no-date", T0)]
    local = {"older-locally": T0 - timedelta(days=1), "same": T0, "no-date": None}

    assert [t.id for t in missing_or_newer(threats, local)] == ["new", "older-locally", "no-date"]


@pytest.mark.asyncio
async def test_snapshot_provider_serves_the_tdm_calls_of_the_sync():
    sigma_a, sigma_b = "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
    hos = [_rule("hos-1", "33333333-3333-4333-8333-333333333333")]
    rules = [_rule("rule-a", sigma_a), _rule("rule-b", sigma_b, updated=T0 + timedelta(days=2))]
    provider = SnapshotProvider([_threat("at-1", T0)], hos, rules)

    assert [t.id for t in await provider.get_active_threats(updated_after=T0 + timedelta(days=30))] == ["at-1"]
    # Nothing local is ever hidden or deleted because the snapshot lacks it.
    assert await provider.check_active_threats_exist(["at-1", "cloud-only"]) == {"at-1", "cloud-only"}
    assert [r.id for r in await provider.search_rules_by_case_ids(["hos-1", "rule-a"], siem_type="hos")] == ["hos-1"]
    assert [r.id for r in await provider.search_rules_by_case_ids(["rule-a", "rule-b"])] == ["rule-a", "rule-b"]
    # Existing rows are refreshed only by newer snapshot items.
    newer = await provider.search_rules_by_sigma_uuids([sigma_a, sigma_b], updated_after=T0 + timedelta(days=1))
    assert [r.id for r in newer] == ["rule-b"]


def _patch_apply(monkeypatch, applied_version: str | None, local_updated: dict) -> SimpleNamespace:
    config_dao = AsyncMock()
    config_dao.get_by_key.return_value = SimpleNamespace(value=applied_version) if applied_version else None
    at_dao = AsyncMock()
    at_dao.get_tdm_updated_by_ids.return_value = local_updated
    sync_manager = AsyncMock()
    sync_manager.sync_active_threats.return_value = MagicMock()
    monkeypatch.setattr(init_content, "ConfigDAO", lambda session: config_dao)
    monkeypatch.setattr(init_content, "ActiveThreatDAO", lambda session: at_dao)
    monkeypatch.setattr(init_content, "ActiveThreatsSyncManager", lambda session: sync_manager)
    return SimpleNamespace(config_dao=config_dao, sync_manager=sync_manager)


@pytest.mark.asyncio
async def test_apply_skips_a_version_already_applied(monkeypatch):
    version = load_snapshot().version
    mocks = _patch_apply(monkeypatch, applied_version=version, local_updated={})

    assert await apply_content_snapshot(AsyncMock()) is False
    mocks.sync_manager.sync_active_threats.assert_not_awaited()
    mocks.config_dao.upsert_by_key.assert_not_awaited()


@pytest.mark.asyncio
async def test_apply_syncs_only_missing_or_newer_threats_and_records_the_version(monkeypatch):
    snapshot = load_snapshot()
    up_to_date = snapshot.active_threats[0]
    mocks = _patch_apply(monkeypatch, applied_version=None, local_updated={up_to_date.id: up_to_date.updated})

    assert await apply_content_snapshot(AsyncMock()) is True

    provider = mocks.sync_manager.sync_active_threats.call_args.kwargs["provider"]
    assert up_to_date.id not in {t.id for t in provider.active_threats}
    assert len(provider.active_threats) == len(snapshot.active_threats) - 1
    mocks.config_dao.upsert_by_key.assert_awaited_once_with(init_content.CONTENT_VERSION_CONFIG_KEY, snapshot.version)


def test_bundled_content_is_consistent():
    """Guards hand-dropped snapshots: every file loads, ATs link bundled HoS, rules are exactly what HoS reference."""
    snapshot = load_snapshot(CONTENT_DIR)
    assert snapshot is not None and snapshot.version
    assert snapshot.active_threats and snapshot.hos and snapshot.rules

    hos_ids = {h.id for h in snapshot.hos}
    for threat in snapshot.active_threats:
        assert set(threat.related_high_order_sigma_rules) <= hos_ids, threat.id
    for item in [*snapshot.active_threats, *snapshot.hos, *snapshot.rules]:
        assert item.created.tzinfo is not None and item.updated.tzinfo is not None, item.id
    for hos in snapshot.hos:
        assert extract_referenced_sigma_uuids(hos.body), f"HoS {hos.id} references no rules"
    # A HoS also tags its own Active Threat (`active_threat.<id>`), which is not a rule reference.
    referenced = {u for h in snapshot.hos for u in extract_referenced_sigma_uuids(h.body)}
    referenced -= {t.id.lower() for t in snapshot.active_threats}
    bundled = {get_sigma_uuid_from_sigma_body(r.body) for r in snapshot.rules}
    assert bundled <= referenced, f"rules not referenced by any HoS: {sorted(bundled - referenced)}"
