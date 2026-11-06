"""The bulk-monitoring request-shape cutover, both steps of it.

``bulk_disabled`` changed meaning: it used to stop correlation everywhere (i.e. block), it now
excludes threats from the selected correlation pipelines only. The pipeline set travels in
``correlation_pipeline_ids``, which shipped OPTIONAL for exactly one release —
``ActiveThreatBulkIdsRequest`` is ``extra="forbid"``, so the frontend could not send a field the
backend did not yet know, and the reverse order would have 422'd the live UI. Step 2 (here) makes it
required: while it was optional an omitted field meant *every* correlation pipeline, so forgetting
it changed monitoring for every customer at once.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import ValidationError

from apps.core.exceptions import BadRequestError
from apps.core.schemas import ActiveThreatBulkIdsRequest
from apps.managers.active_threats import ActiveThreatsManager

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")
UNKNOWN = UUID("99999999-9999-4999-8999-999999999999")


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "analyst@example.com"
    return user


@pytest.fixture
def manager() -> ActiveThreatsManager:
    mgr = ActiveThreatsManager.__new__(ActiveThreatsManager)
    mgr.db = AsyncMock()
    mgr.dao = AsyncMock()
    mgr.correlation_pipeline_dao = AsyncMock()
    mgr.correlation_pipelines = AsyncMock()
    mgr.kafka = AsyncMock()
    mgr.correlation_pipeline_dao.list_refs.return_value = [(PIPELINE_A, "tenant-a"), (PIPELINE_B, "tenant-b")]
    mgr.dao.get_visible_blocked_state.return_value = {"AT-1": False}
    mgr.dao.get_changed_pipeline_ids.return_value = [PIPELINE_A]
    mgr.dao.set_excluded.return_value = 1
    return mgr


@pytest.fixture(autouse=True)
def _activity() -> AsyncMock:
    with patch("apps.managers.active_threats.activity_producer", AsyncMock()) as producer:
        yield producer


class TestRequestShape:
    def test_pipeline_ids_are_required(self) -> None:
        """422, not "every pipeline".

        The field shipped optional for one release so the frontend could start sending it against a
        backend that already accepted it (``extra="forbid"`` makes the reverse order impossible).
        While it was optional, omitting it meant EVERY correlation pipeline — so a caller that
        forgot it silently changed monitoring for every customer. Required is what makes that
        unrepresentable.
        """
        with pytest.raises(ValidationError):
            ActiveThreatBulkIdsRequest(threat_ids=["AT-1"])

    def test_an_empty_pipeline_list_is_rejected(self) -> None:
        """``[]`` is the same trap wearing a different hat: it would be a silent no-op."""
        with pytest.raises(ValidationError):
            ActiveThreatBulkIdsRequest(threat_ids=["AT-1"], correlation_pipeline_ids=[])

    def test_pipeline_ids_are_accepted(self) -> None:
        body = ActiveThreatBulkIdsRequest(threat_ids=["AT-1"], correlation_pipeline_ids=[PIPELINE_A])

        assert body.correlation_pipeline_ids == [PIPELINE_A]

    def test_unknown_fields_are_still_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            ActiveThreatBulkIdsRequest(threat_ids=["AT-1"], pipeline_ids=[str(PIPELINE_A)])


class TestPipelineResolution:
    @pytest.mark.asyncio
    async def test_an_unknown_pipeline_id_is_a_400_naming_it(self, manager: ActiveThreatsManager) -> None:
        with pytest.raises(BadRequestError, match=str(UNKNOWN)):
            await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A, UNKNOWN], monitored=False, user=_user())

        manager.dao.set_excluded.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_ids_are_collapsed(self, manager: ActiveThreatsManager) -> None:
        response = await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A, PIPELINE_A], monitored=False, user=_user())

        assert response.correlation_pipeline_ids == [PIPELINE_A]


class TestPerThreatOutcome:
    @pytest.mark.asyncio
    async def test_blocked_threats_are_reported_and_skipped(self, manager: ActiveThreatsManager) -> None:
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False, "AT-2": True}

        response = await manager.bulk_set_monitoring(
            ["AT-1", "AT-2", "AT-3"], [PIPELINE_A], monitored=True, user=_user()
        )

        assert {result.threat_id: result.status for result in response.results} == {
            "AT-1": "updated",
            "AT-2": "blocked",
            "AT-3": "not_found",
        }
        assert (response.updated, response.blocked, response.not_found) == (1, 1, 1)
        # Only the eligible id reaches the write — a blocked threat is never silently excluded.
        assert manager.dao.set_excluded.await_args.args[0] == ["AT-1"]

    @pytest.mark.asyncio
    async def test_nothing_eligible_means_no_write_and_no_republish(self, manager: ActiveThreatsManager) -> None:
        manager.dao.get_visible_blocked_state.return_value = {}

        response = await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A], monitored=False, user=_user())

        assert response.not_found == 1
        manager.dao.set_excluded.assert_not_awaited()
        manager.correlation_pipelines.republish_chains_config.assert_not_awaited()


class TestRepublishScope:
    @pytest.mark.asyncio
    async def test_only_the_pipelines_whose_deny_list_moved_are_republished(
        self, manager: ActiveThreatsManager
    ) -> None:
        """Hard constraint 5: not all pipelines, not none — exactly the affected ones."""
        manager.dao.get_changed_pipeline_ids.return_value = [PIPELINE_B]

        await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A, PIPELINE_B], monitored=False, user=_user())

        assert manager.correlation_pipelines.republish_chains_config.await_args.args[0] == [PIPELINE_B]
        # The affected set is computed BEFORE the write, or every pipeline would look unchanged.
        assert manager.dao.get_changed_pipeline_ids.await_args.kwargs["excluded"] is True

    @pytest.mark.asyncio
    async def test_a_no_op_bulk_republishes_nothing(self, manager: ActiveThreatsManager) -> None:
        manager.dao.get_changed_pipeline_ids.return_value = []

        await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A], monitored=False, user=_user())

        manager.correlation_pipelines.republish_chains_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_neither_direction_touches_the_shared_topic_or_a_flink_job(
        self, manager: ActiveThreatsManager
    ) -> None:
        """``bulk_disabled`` no longer means "block": ``is_blocked`` and the AT topic stay put."""
        await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A], monitored=False, user=_user())
        await manager.bulk_set_monitoring(["AT-1"], [PIPELINE_A], monitored=True, user=_user())

        manager.dao.set_blocked.assert_not_awaited()
        manager.kafka.send_active_threats.assert_not_awaited()
        manager.correlation_pipelines.restart_if_running.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_one_activity_entry_summarises_the_whole_operation(
        self, manager: ActiveThreatsManager, _activity: AsyncMock
    ) -> None:
        """Never one entry per (threat, pipeline) pair — that is 500 × N audit rows."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False, "AT-2": False}

        await manager.bulk_set_monitoring(["AT-1", "AT-2"], [PIPELINE_A, PIPELINE_B], monitored=False, user=_user())

        _activity.log_action.assert_awaited_once()
        assert _activity.log_action.await_args.kwargs["entity_type"] == "active_threat"
