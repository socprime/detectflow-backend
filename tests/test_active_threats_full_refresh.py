from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from apps.managers.active_threats import (
    ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS,
    ACTIVE_THREAT_INITIAL_LOOKBACK_DAYS,
    ActiveThreatsSyncManager,
    HighOrderSigmaSyncResult,
    ReferencedSystemRulesSyncResult,
)


@pytest.mark.asyncio
async def test_get_updated_after_full_refresh_uses_oldest_stored_at():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    # The "latest" cursor must be ignored; the oldest stored AT bounds the window instead.
    manager.dao.get_latest_tdm_updated.return_value = datetime.now(UTC)
    earliest = datetime.now(UTC) - timedelta(days=20)
    manager.dao.get_earliest_tdm_updated.return_value = earliest

    updated_after = await manager._get_updated_after(full_refresh=True)

    assert updated_after == earliest - timedelta(minutes=5)
    manager.dao.get_latest_tdm_updated.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_updated_after_full_refresh_clamps_to_tdm_limit():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    # Oldest stored AT predates TDM's 90-day limit; the cursor must clamp to the limit.
    manager.dao.get_earliest_tdm_updated.return_value = datetime.now(UTC) - timedelta(days=200)

    updated_after = await manager._get_updated_after(full_refresh=True)

    expected_floor = datetime.now(UTC) - timedelta(days=ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS)
    assert abs((updated_after - expected_floor).total_seconds()) < 5


@pytest.mark.asyncio
async def test_get_updated_after_full_refresh_falls_back_when_catalog_empty():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    manager.dao.get_earliest_tdm_updated.return_value = None

    updated_after = await manager._get_updated_after(full_refresh=True)

    expected = datetime.now(UTC) - timedelta(days=ACTIVE_THREAT_INITIAL_LOOKBACK_DAYS)
    assert abs((updated_after - expected).total_seconds()) < 5


def _build_full_refresh_manager() -> ActiveThreatsSyncManager:
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.config_dao = AsyncMock()
    manager.config_dao.get_socprime_tdm_provider_config.return_value = {"api_key": "key"}
    manager.dao = AsyncMock()
    manager.dao.get_latest_tdm_updated.return_value = datetime.now(UTC)
    # Oldest stored AT bounds the full-refresh window (well inside the TDM limit here).
    manager.dao.get_earliest_tdm_updated.return_value = datetime.now(UTC) - timedelta(days=40)
    manager.dao.upsert_many.return_value = []
    manager.hos_dao = AsyncMock()
    # A recent HOS cursor would normally suppress re-fetching existing HOS bodies.
    manager.hos_dao.get_latest_tdm_updated.return_value = datetime.now(UTC)
    manager.repository_dao = AsyncMock()
    manager.repository_dao.get_system_repository.return_value = MagicMock(id="repo")
    manager.rule_dao = AsyncMock()
    manager.rule_dao.get_latest_updated_by_repository_id.return_value = datetime.now(UTC)
    manager.kafka = AsyncMock()

    manager._sync_high_order_sigmas = AsyncMock(
        return_value=HighOrderSigmaSyncResult(fetched=0, upserted=0, deleted_tdm_ids=[], synced_tdm_ids=[])
    )
    manager._sync_referenced_system_rules = AsyncMock(
        return_value=ReferencedSystemRulesSyncResult(fetched=0, upserted=0, deleted=0)
    )
    manager._hide_missing_active_threats = AsyncMock(return_value=[])
    manager._apply_retention = AsyncMock(return_value=[])
    manager._sync_active_threats_to_kafka = AsyncMock()
    manager._sync_hos_to_kafka = AsyncMock()
    return manager


@pytest.mark.asyncio
async def test_sync_active_threats_full_refresh_resets_incremental_cursors(monkeypatch):
    manager = _build_full_refresh_manager()
    provider = AsyncMock()
    provider.get_active_threats.return_value = []
    monkeypatch.setattr("apps.managers.active_threats.SocPrimeTdmProvider", lambda config: provider)

    result = await manager.sync_active_threats(full_refresh=True)

    assert result.configured is True

    # HOS and referenced-rule fetches drop the incremental cursor so all are re-fetched
    # (the updated_after window itself is covered by the _get_updated_after tests above).
    _, hos_kwargs = manager._sync_high_order_sigmas.call_args
    assert hos_kwargs["existing_hos_updated_after"] is None
    _, rule_kwargs = manager._sync_referenced_system_rules.call_args
    assert rule_kwargs["existing_rule_updated_after"] is None


@pytest.mark.asyncio
async def test_get_updated_after_incremental_clamps_to_tdm_limit():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    # Sync paused for 3+ months (or a catalog seeded from an old content snapshot): resume from
    # TDM's 90-day limit instead of sending an updated_after TDM rejects.
    manager.dao.get_latest_tdm_updated.return_value = datetime.now(UTC) - timedelta(days=200)

    updated_after = await manager._get_updated_after()

    expected_floor = datetime.now(UTC) - timedelta(days=ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS)
    assert abs((updated_after - expected_floor).total_seconds()) < 5


@pytest.mark.asyncio
async def test_sync_active_threats_with_given_provider_needs_no_tdm_integration():
    manager = _build_full_refresh_manager()
    manager.config_dao.get_socprime_tdm_provider_config.return_value = None
    provider = AsyncMock()
    provider.get_active_threats.return_value = []

    result = await manager.sync_active_threats(provider=provider)

    assert result.configured is True
    provider.get_active_threats.assert_awaited_once()
    manager.config_dao.get_socprime_tdm_provider_config.assert_not_awaited()
