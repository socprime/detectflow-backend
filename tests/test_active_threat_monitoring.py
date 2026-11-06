"""D8/D9: opt-out threat monitoring — the effective rule, the two channels, and the block switch.

``monitored = NOT at.is_blocked AND at_id NOT IN pipeline.excluded``. The two conjuncts travel on
two different channels — the global flag on the SHARED active-threats topic, the exclusions on each
pipeline's chains-config envelope — and neither transition may restart a Flink job.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from sqlalchemy.dialects import postgresql

from apps.core.exceptions import ConflictError, NotFoundError
from apps.managers.active_threats import ActiveThreatsManager
from apps.modules.postgre.active_threats import ActiveThreatDAO

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "analyst@example.com"
    return user


def _threat(tdm_id: str, *, blocked: bool = False) -> MagicMock:
    threat = MagicMock()
    threat.tdm_id = tdm_id
    threat.is_blocked = blocked
    return threat


@pytest.fixture
def manager() -> ActiveThreatsManager:
    mgr = ActiveThreatsManager(AsyncMock())
    mgr.dao = AsyncMock()
    mgr.chains_dao = AsyncMock()
    mgr.correlation_pipeline_dao = AsyncMock()
    mgr.correlation_pipelines = AsyncMock()
    mgr.kafka = AsyncMock()
    mgr.correlation_pipeline_dao.list_refs.return_value = [(PIPELINE_A, "tenant-a"), (PIPELINE_B, "tenant-b")]
    return mgr


@pytest.fixture(autouse=True)
def _activity() -> AsyncMock:
    with patch("apps.managers.active_threats.activity_producer", AsyncMock()) as producer:
        yield producer


class TestEffectiveRule:
    """The four-way matrix of (blocked, excluded), as the API surfaces it."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("blocked", "excluded_from", "expected"),
        [
            (True, {PIPELINE_A}, []),  # blocked wins over everything
            (True, set(), []),  # blocked alone is enough
            (False, {PIPELINE_A}, [PIPELINE_B]),  # excluded from exactly one pipeline
            (False, set(), [PIPELINE_A, PIPELINE_B]),  # absent means monitored
        ],
    )
    async def test_matrix(
        self, manager: ActiveThreatsManager, blocked: bool, excluded_from: set[UUID], expected: list[UUID]
    ) -> None:
        threat = _threat("AT-1", blocked=blocked)
        manager.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = (
            {"AT-1": excluded_from} if excluded_from else {}
        )

        [(_, pipelines)] = await manager._hydrate_pipelines([threat])

        assert [ref.id for ref in pipelines] == expected

    @pytest.mark.asyncio
    async def test_a_new_pipeline_monitors_everything_with_zero_rows_written(
        self, manager: ActiveThreatsManager
    ) -> None:
        """Opt-out inheritance: no exclusion row anywhere means every threat is monitored."""
        manager.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {}

        hydrated = await manager._hydrate_pipelines([_threat("AT-1"), _threat("AT-2")])

        assert all([ref.id for ref in pipelines] == [PIPELINE_A, PIPELINE_B] for _, pipelines in hydrated)

    def test_monitored_clause_is_the_single_definition_of_the_rule(self) -> None:
        """Both conjuncts in one SQL predicate, so no read path can re-derive half of it."""
        sql = str(
            ActiveThreatDAO.monitored_clause(PIPELINE_A).compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
        )

        assert "active_threats.is_blocked IS false" in sql
        assert "NOT IN" in sql
        assert "correlation_pipeline_excluded_active_threats" in sql
        assert str(PIPELINE_A) in sql


class TestSetPipelineMonitoring:
    @pytest.mark.asyncio
    async def test_excluding_writes_a_row_and_republishes_only_that_pipeline(
        self, manager: ActiveThreatsManager
    ) -> None:
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.correlation_pipeline_dao.get_by_id.return_value = MagicMock(name="tenant-a")
        manager.dao.set_excluded.return_value = 1

        response = await manager.set_pipeline_monitoring("AT-1", PIPELINE_A, False, _user())

        assert response.changed is True
        assert response.monitored is False
        assert manager.dao.set_excluded.await_args.kwargs["excluded"] is True
        manager.correlation_pipelines.republish_chains_config.assert_awaited_once()
        assert manager.correlation_pipelines.republish_chains_config.await_args.args[0] == [PIPELINE_A]
        # Hot broadcast change only: nothing on the shared AT topic, no Flink object touched.
        manager.kafka.send_active_threats.assert_not_awaited()
        manager.correlation_pipelines.restart_if_running.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unchanged_set_publishes_nothing(self, manager: ActiveThreatsManager) -> None:
        """Idempotent PUT: re-sending the current state must not put a record on a compacted topic."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.correlation_pipeline_dao.get_by_id.return_value = MagicMock()
        manager.dao.set_excluded.return_value = 0

        response = await manager.set_pipeline_monitoring("AT-1", PIPELINE_A, False, _user())

        assert response.changed is False
        manager.correlation_pipelines.republish_chains_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_monitoring_a_blocked_threat_is_a_conflict(self, manager: ActiveThreatsManager) -> None:
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": True}
        manager.correlation_pipeline_dao.get_by_id.return_value = MagicMock()

        with pytest.raises(ConflictError, match="unblock"):
            await manager.set_pipeline_monitoring("AT-1", PIPELINE_A, True, _user())

        manager.dao.set_excluded.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_excluding_a_blocked_threat_is_allowed(self, manager: ActiveThreatsManager) -> None:
        """Only the True direction conflicts — recording intent while blocked stays possible."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": True}
        manager.correlation_pipeline_dao.get_by_id.return_value = MagicMock()
        manager.dao.set_excluded.return_value = 1

        response = await manager.set_pipeline_monitoring("AT-1", PIPELINE_A, False, _user())

        assert response.changed is True

    @pytest.mark.asyncio
    async def test_unknown_threat_and_unknown_pipeline_are_404s(self, manager: ActiveThreatsManager) -> None:
        manager.dao.get_visible_blocked_state.return_value = {}
        with pytest.raises(NotFoundError, match="Active threat"):
            await manager.set_pipeline_monitoring("AT-nope", PIPELINE_A, False, _user())

        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.correlation_pipeline_dao.get_by_id.return_value = None
        with pytest.raises(NotFoundError, match="Correlation pipeline"):
            await manager.set_pipeline_monitoring("AT-1", PIPELINE_A, False, _user())


class TestBlockSwitch:
    @pytest.mark.asyncio
    async def test_block_preserves_exclusions_and_writes_only_the_shared_topic(
        self, manager: ActiveThreatsManager
    ) -> None:
        """A mistaken block must be reversible without losing analyst intent."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.dao.set_blocked.return_value = True
        manager.dao.get_by_tdm_ids.return_value = [_threat("AT-1", blocked=True)]

        response = await manager.set_blocked("AT-1", True, _user())

        assert response.changed is True
        assert response.pipelines_republished == 0
        manager.dao.clear_exclusions_for_threat.assert_not_awaited()
        manager.kafka.send_active_threats.assert_awaited_once()
        manager.correlation_pipelines.republish_chains_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unblock_clears_exclusions_and_republishes_exactly_those_pipelines(
        self, manager: ActiveThreatsManager
    ) -> None:
        """Unblock is "enabled in all existing pipelines", so it is the transition that mutates
        per-pipeline state."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": True}
        manager.dao.set_blocked.return_value = True
        manager.dao.clear_exclusions_for_threat.return_value = [PIPELINE_A]
        manager.dao.get_by_tdm_ids.return_value = [_threat("AT-1")]

        response = await manager.set_blocked("AT-1", False, _user())

        assert response.pipelines_republished == 1
        manager.dao.clear_exclusions_for_threat.assert_awaited_once_with("AT-1")
        assert manager.correlation_pipelines.republish_chains_config.await_args.args[0] == [PIPELINE_A]
        manager.kafka.send_active_threats.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_no_op_block_touches_nothing(self, manager: ActiveThreatsManager) -> None:
        """Re-blocking an already-blocked threat must not republish or clear anything."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": True}
        manager.dao.set_blocked.return_value = False

        response = await manager.set_blocked("AT-1", True, _user())

        assert response.changed is False
        manager.kafka.send_active_threats.assert_not_awaited()
        manager.dao.clear_exclusions_for_threat.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_no_op_unblock_does_not_wipe_exclusions(self, manager: ActiveThreatsManager) -> None:
        """The flag did not move, so an analyst's per-pipeline opt-outs stay untouched."""
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.dao.set_blocked.return_value = False

        await manager.set_blocked("AT-1", False, _user())

        manager.dao.clear_exclusions_for_threat.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_both_directions_are_activity_logged(
        self, manager: ActiveThreatsManager, _activity: AsyncMock
    ) -> None:
        manager.dao.get_visible_blocked_state.return_value = {"AT-1": False}
        manager.dao.set_blocked.return_value = True
        manager.dao.clear_exclusions_for_threat.return_value = []
        manager.dao.get_by_tdm_ids.return_value = [_threat("AT-1")]

        await manager.set_blocked("AT-1", True, _user())
        await manager.set_blocked("AT-1", False, _user())

        changes = [call.kwargs["changes"] for call in _activity.log_action.await_args_list]
        assert {"is_blocked": {"old": False, "new": True}} in changes
        assert {"is_blocked": {"old": True, "new": False}} in changes
