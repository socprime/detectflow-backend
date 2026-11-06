"""Catalogue deletion silently drops analyst intent — unless it is captured first.

``ON DELETE CASCADE`` on ``active_threat_tdm_id`` takes every per-pipeline exclusion with the
deleted threat. CASCADE stays (RESTRICT would make retention fail, a soft delete would leak rows),
so the two delete paths must read the doomed rows BEFORE the DELETE, log one warning per affected
correlation pipeline, and republish those pipelines' now-shorter deny-list. The hide path deletes
nothing and must do neither.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.managers.active_threats import ActiveThreatsSyncManager

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")
CUTOFF = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def sync() -> ActiveThreatsSyncManager:
    manager = ActiveThreatsSyncManager.__new__(ActiveThreatsSyncManager)
    manager.db = AsyncMock()
    manager.dao = AsyncMock()
    manager.correlation_pipeline_dao = AsyncMock()
    manager.correlation_pipeline_dao.list_refs.return_value = [(PIPELINE_A, "tenant-a"), (PIPELINE_B, "tenant-b")]
    manager._dropped_exclusions = {}
    return manager


@pytest.fixture(autouse=True)
def _activity() -> AsyncMock:
    with patch("apps.managers.active_threats.activity_producer", AsyncMock()) as producer:
        yield producer


class TestTdmDeletePath:
    @pytest.mark.asyncio
    async def test_captures_warns_and_republishes(self, sync: ActiveThreatsSyncManager, _activity: AsyncMock) -> None:
        provider = AsyncMock()
        sync.dao.get_all_tdm_ids.return_value = {"AT-1", "AT-2"}
        provider.check_active_threats_exist.return_value = {"AT-2"}
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {"AT-1": {PIPELINE_A}}

        deleted = await sync._remove_deleted_active_threats(provider)

        assert deleted == ["AT-1"]
        assert sync._dropped_exclusions == {PIPELINE_A: {"AT-1"}}

        entry = _activity.log_action.await_args.kwargs
        assert entry["entity_type"] == "correlation_pipeline"
        assert entry["entity_id"] == str(PIPELINE_A)
        assert entry["severity"] == "warning"
        assert entry["source"] == "system"
        assert "AT-1" in entry["details"]

        pipelines_manager = AsyncMock()
        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=pipelines_manager):
            await sync._republish_chains_config_for_deleted_threats(deleted)

        assert pipelines_manager.republish_chains_config.await_args.args[0] == [PIPELINE_A]

    @pytest.mark.asyncio
    async def test_the_capture_happens_before_the_delete(self, sync: ActiveThreatsSyncManager) -> None:
        """Reading after the DELETE would always come back empty — the rows are already gone."""
        order: list[str] = []
        provider = AsyncMock()
        sync.dao.get_all_tdm_ids.return_value = {"AT-1"}
        provider.check_active_threats_exist.return_value = set()
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.side_effect = lambda ids: order.append("read") or {}
        sync.dao.delete_by_tdm_ids.side_effect = lambda ids: order.append("delete") or 1

        await sync._remove_deleted_active_threats(provider)

        assert order == ["read", "delete"]

    @pytest.mark.asyncio
    async def test_no_exclusions_means_no_warning_and_no_republish(
        self, sync: ActiveThreatsSyncManager, _activity: AsyncMock
    ) -> None:
        provider = AsyncMock()
        sync.dao.get_all_tdm_ids.return_value = {"AT-1"}
        provider.check_active_threats_exist.return_value = set()
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {}

        await sync._remove_deleted_active_threats(provider)

        _activity.log_action.assert_not_awaited()
        assert sync._dropped_exclusions == {}


class TestRetentionPath:
    @pytest.mark.asyncio
    async def test_captures_warns_and_deletes_exactly_the_doomed_ids(
        self, sync: ActiveThreatsSyncManager, _activity: AsyncMock
    ) -> None:
        sync.dao.get_ids_older_than_tdm_created.return_value = ["AT-old"]
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {"AT-old": {PIPELINE_A, PIPELINE_B}}

        deleted = await sync._apply_retention(CUTOFF)

        assert deleted == ["AT-old"]
        sync.dao.delete_by_tdm_ids.assert_awaited_once_with(["AT-old"])
        assert set(sync._dropped_exclusions) == {PIPELINE_A, PIPELINE_B}
        assert _activity.log_action.await_count == 2
        assert await sync._correlation_pipelines_for_threats(["AT-old"]) == list(sync._dropped_exclusions)

    @pytest.mark.asyncio
    async def test_disabled_retention_does_nothing(self, sync: ActiveThreatsSyncManager) -> None:
        assert await sync._apply_retention(None) == []
        sync.dao.get_ids_older_than_tdm_created.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nothing_expired_costs_no_capture(self, sync: ActiveThreatsSyncManager) -> None:
        sync.dao.get_ids_older_than_tdm_created.return_value = []

        assert await sync._apply_retention(CUTOFF) == []
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.assert_not_awaited()
        sync.dao.delete_by_tdm_ids.assert_not_awaited()


class TestHidePath:
    @pytest.mark.asyncio
    async def test_hiding_preserves_exclusions_entirely(
        self, sync: ActiveThreatsSyncManager, _activity: AsyncMock
    ) -> None:
        """``is_hidden`` only flips a flag: nothing cascades, so nothing is lost or republished."""
        provider = AsyncMock()
        sync.dao.get_all_tdm_ids.return_value = {"AT-1"}
        provider.check_active_threats_exist.return_value = set()
        sync.dao.set_hidden_by_tdm_ids.return_value = ["AT-1"]

        hidden = await sync._hide_missing_active_threats(provider)

        assert hidden == ["AT-1"]
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.assert_not_awaited()
        sync.dao.delete_by_tdm_ids.assert_not_awaited()
        _activity.log_action.assert_not_awaited()
        assert sync._dropped_exclusions == {}


class TestBothPathsAccumulate:
    @pytest.mark.asyncio
    async def test_one_sync_run_republishes_the_union(self, sync: ActiveThreatsSyncManager) -> None:
        provider = AsyncMock()
        sync.dao.get_all_tdm_ids.return_value = {"AT-1"}
        provider.check_active_threats_exist.return_value = set()
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {"AT-1": {PIPELINE_A}}
        await sync._remove_deleted_active_threats(provider)

        sync.dao.get_ids_older_than_tdm_created.return_value = ["AT-old"]
        sync.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {"AT-old": {PIPELINE_B}}
        await sync._apply_retention(CUTOFF)

        pipelines_manager = AsyncMock()
        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=pipelines_manager):
            await sync._republish_chains_config_for_deleted_threats(["AT-1", "AT-old"])

        assert set(pipelines_manager.republish_chains_config.await_args.args[0]) == {PIPELINE_A, PIPELINE_B}

    @pytest.mark.asyncio
    async def test_an_unrelated_threat_republishes_nothing(self, sync: ActiveThreatsSyncManager) -> None:
        sync._dropped_exclusions = {PIPELINE_A: {"AT-1"}}
        pipelines_manager = MagicMock()
        pipelines_manager.republish_chains_config = AsyncMock()

        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=pipelines_manager):
            await sync._republish_chains_config_for_deleted_threats(["AT-other"])

        pipelines_manager.republish_chains_config.assert_not_awaited()
