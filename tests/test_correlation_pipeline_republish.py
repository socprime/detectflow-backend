"""Membership entry points and the restart-free chains-config republish call sites.

``republish_chains_config`` publishes threshold / window / AT deny-list without touching a Flink
object. Destination-topic edits are SIEM output and do not republish.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")
NEW_MEMBER = UUID("aaaaaaaa-0000-4000-8000-000000000009")
# The member every row starts with, so a row is startable unless a test says otherwise.
EXISTING_MEMBER = UUID("aaaaaaaa-0000-4000-8000-000000000001")


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


def _detection_pipeline(
    pipeline_id: UUID,
    *,
    name: str = "New pipeline",
    destination_topic: str = "out",
    tagged_events_topic: str | None = "detectflow.tagged-events.x",
) -> MagicMock:
    pipeline = MagicMock()
    pipeline.id = pipeline_id
    pipeline.name = name
    pipeline.destination_topic = destination_topic
    pipeline.destinations = [{"topic": destination_topic}]
    pipeline.tagged_events_topic = tagged_events_topic
    pipeline.enabled = True
    return pipeline


def _row(
    pipeline_id: UUID,
    name: str,
    *,
    auto_add: bool = False,
    pattern: str | None = None,
    created_at: datetime | None = None,
    jobs_status: str = "stopped",
    members: list[MagicMock] | None = None,
) -> MagicMock:
    row = MagicMock()
    row.id = pipeline_id
    row.name = name
    row.jobs_status = jobs_status
    row.threshold_pct = 60
    row.correlation_window_seconds = None
    row.auto_add_new_pipelines = auto_add
    row.auto_add_pipeline_name_pattern = pattern
    row.correlation_deployment_name = f"corr-{pipeline_id}"
    row.agent_deployment_name = f"agent-{pipeline_id}"
    # A member that owns its tagged-events topic — the shape of a pipeline that can actually run.
    # With none, the job has no input topics at all and the restart paths refuse to submit a spec.
    row.members = (
        members if members is not None else [_detection_pipeline(EXISTING_MEMBER, destination_topic="cust1-out")]
    )
    row.created_at = created_at or datetime(2026, 1, 1, tzinfo=UTC)
    row.updated_at = datetime(2026, 1, 1, tzinfo=UTC)
    row.updated_by = None
    for field in (
        "correlation_parallelism",
        "correlation_taskmanager_memory_mb",
        "correlation_taskmanager_cpu",
        "correlation_jobmanager_memory_mb",
        "correlation_jobmanager_cpu",
        "agent_parallelism",
        "agent_taskmanager_memory_mb",
        "agent_taskmanager_cpu",
        "agent_jobmanager_memory_mb",
        "agent_jobmanager_cpu",
    ):
        setattr(row, field, None)
    return row


@pytest.fixture
def chains_config() -> AsyncMock:
    producer = AsyncMock()
    producer.build_envelope = MagicMock(return_value={})
    return producer


@pytest.fixture
def provider() -> AsyncMock:
    provider = AsyncMock(spec=KubernetesFlinkProvider)
    provider.launch_correlation_job_deployment = AsyncMock(return_value={"operation": "updated"})
    provider.launch_agent_job_deployment = AsyncMock(return_value={"operation": "updated"})
    return provider


@pytest.fixture
def manager() -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(AsyncMock())
    # `AsyncSession.expire` is synchronous (like `add`); an AsyncMock would leave an
    # un-awaited coroutine behind every membership write.
    mgr.db.expire = MagicMock()
    mgr.dao = AsyncMock()
    mgr.pipeline_dao = AsyncMock()
    mgr.audit_dao = AsyncMock()
    # §20 deny-list lookup: empty means "monitors every non-blocked threat", the opt-out default.
    mgr.active_threats_dao = AsyncMock()
    mgr.active_threats_dao.get_excluded_threat_ids_by_pipeline_ids.return_value = {}
    return mgr


@pytest.fixture(autouse=True)
def _patched_side_channels(chains_config: AsyncMock):
    with (
        patch("apps.managers.correlation_pipelines.chains_config_producer", chains_config),
        patch("apps.managers.correlation_pipelines.activity_producer", AsyncMock()),
    ):
        yield


class TestRepublishChainsConfig:
    @pytest.mark.asyncio
    async def test_publishes_one_envelope_per_pipeline_and_restarts_nothing(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock, provider: AsyncMock
    ) -> None:
        rows = {PIPELINE_A: _row(PIPELINE_A, "tenant-a"), PIPELINE_B: _row(PIPELINE_B, "tenant-b")}
        manager.dao.get_with_relations.side_effect = lambda pipeline_id: rows.get(pipeline_id)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.republish_chains_config([PIPELINE_A, PIPELINE_B], reason="test")

        assert chains_config.publish.await_count == 2
        topics = {call.kwargs["topic"] for call in chains_config.publish.await_args_list}
        assert topics == {
            f"detectflow.chains-config.{PIPELINE_A}",
            f"detectflow.chains-config.{PIPELINE_B}",
        }
        provider.launch_correlation_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_one_failure_does_not_abort_the_rest(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock
    ) -> None:
        """Best-effort per pipeline: one tenant's broken publish must not silence another's."""
        rows = {PIPELINE_A: _row(PIPELINE_A, "tenant-a"), PIPELINE_B: _row(PIPELINE_B, "tenant-b")}

        def _lookup(pipeline_id):
            if pipeline_id == PIPELINE_A:
                raise RuntimeError("row load failed")
            return rows[pipeline_id]

        manager.dao.get_with_relations.side_effect = _lookup

        await manager.republish_chains_config([PIPELINE_A, PIPELINE_B], reason="test")

        assert chains_config.publish.await_count == 1
        assert chains_config.publish.await_args.kwargs["topic"] == f"detectflow.chains-config.{PIPELINE_B}"

    @pytest.mark.asyncio
    async def test_a_deleted_pipeline_is_skipped(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock
    ) -> None:
        manager.dao.get_with_relations.return_value = None

        await manager.republish_chains_config([PIPELINE_A], reason="test")

        chains_config.publish.assert_not_awaited()


class TestGetIdsByMember:
    @pytest.mark.asyncio
    async def test_returns_a_single_element_list(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_correlation_pipeline_ids_for_pipeline.return_value = [PIPELINE_A]

        assert await manager.get_ids_by_member(NEW_MEMBER) == [PIPELINE_A]

    @pytest.mark.asyncio
    async def test_returns_every_owner(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_correlation_pipeline_ids_for_pipeline.return_value = [PIPELINE_A, PIPELINE_B]

        assert await manager.get_ids_by_member(NEW_MEMBER) == [PIPELINE_A, PIPELINE_B]

    @pytest.mark.asyncio
    async def test_returns_empty_when_unowned(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_correlation_pipeline_ids_for_pipeline.return_value = []

        assert await manager.get_ids_by_member(NEW_MEMBER) == []


class TestAutoAddMember:
    @pytest.mark.asyncio
    async def test_attaches_to_an_opted_in_pipeline_whose_pattern_matches(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock, provider: AsyncMock
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a", auto_add=True, pattern="cust1-*")
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, name="cust1-edr")
        manager.dao.list_with_members.return_value = [row]
        manager.dao.get_with_relations.return_value = row

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            changed = await manager.auto_add_member(NEW_MEMBER, _user())

        assert changed == [PIPELINE_A]
        # Appended, not replaced: auto-add must never drop the members already there.
        assert manager.dao.set_members.await_args.args[1] == [EXISTING_MEMBER, NEW_MEMBER]
        chains_config.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_a_pipeline_whose_pattern_does_not_match(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock
    ) -> None:
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, name="cust2-edr")
        manager.dao.list_with_members.return_value = [_row(PIPELINE_A, "tenant-a", auto_add=True, pattern="cust1-*")]

        assert await manager.auto_add_member(NEW_MEMBER, _user()) == []
        manager.dao.set_members.assert_not_awaited()
        chains_config.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_destination_topics_are_not_consulted(
        self, manager: CorrelationPipelinesManager, chains_config: AsyncMock
    ) -> None:
        """A destination that would have matched the old topic glob does not select the pipeline."""
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(
            NEW_MEMBER, name="linux-edr", destination_topic="cust1-out"
        )
        manager.dao.list_with_members.return_value = [_row(PIPELINE_A, "tenant-a", auto_add=True, pattern="cust1-*")]

        assert await manager.auto_add_member(NEW_MEMBER, _user()) == []
        manager.dao.set_members.assert_not_awaited()
        chains_config.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_null_pattern_matches_everything(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """NULL/blank is the pre-epic behaviour: auto-add takes every new pipeline."""
        row = _row(PIPELINE_A, "tenant-a", auto_add=True, pattern=None)
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, name="anything")
        manager.dao.list_with_members.return_value = [row]
        manager.dao.get_with_relations.return_value = row

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            assert await manager.auto_add_member(NEW_MEMBER, _user()) == [PIPELINE_A]

    @pytest.mark.asyncio
    async def test_pipelines_that_did_not_opt_in_are_ignored(self, manager: CorrelationPipelinesManager) -> None:
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, destination_topic="cust1-out")
        manager.dao.list_with_members.return_value = [_row(PIPELINE_A, "tenant-a", auto_add=False)]

        assert await manager.auto_add_member(NEW_MEMBER, _user()) == []

    @pytest.mark.asyncio
    async def test_attaches_to_every_matching_opted_in_pipeline(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """Sharing is allowed: two opted-in jobs both take the new detection pipeline."""
        older = _row(PIPELINE_B, "tenant-b", auto_add=True, created_at=datetime(2025, 1, 1, tzinfo=UTC))
        newer = _row(PIPELINE_A, "tenant-a", auto_add=True, created_at=datetime(2026, 6, 1, tzinfo=UTC))
        rows = {PIPELINE_A: newer, PIPELINE_B: older}
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, destination_topic="cust1-out")
        manager.dao.list_with_members.return_value = [newer, older]
        manager.dao.get_with_relations.side_effect = lambda pipeline_id: rows[pipeline_id]

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            changed = await manager.auto_add_member(NEW_MEMBER, _user())

        assert changed == [PIPELINE_B, PIPELINE_A]
        assert [call.args[0] for call in manager.dao.set_members.await_args_list] == [PIPELINE_B, PIPELINE_A]

    @pytest.mark.asyncio
    async def test_restarts_only_a_running_correlation_job_and_never_the_agent(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a", auto_add=True, jobs_status="starting")
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, destination_topic="cust1-out")
        manager.dao.list_with_members.return_value = [row]
        manager.dao.get_with_relations.return_value = row

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.auto_add_member(NEW_MEMBER, _user())

        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stopped_pipeline_is_not_started_by_an_auto_add(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a", auto_add=True, jobs_status="stopped")
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(NEW_MEMBER, destination_topic="cust1-out")
        manager.dao.list_with_members.return_value = [row]
        manager.dao.get_with_relations.return_value = row

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.auto_add_member(NEW_MEMBER, _user())

        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_pipeline_without_its_own_topic_is_not_auto_added(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        """No implicit topic assignment: that restarts the customer's own detection pipeline."""
        manager.pipeline_dao.get_by_id.return_value = _detection_pipeline(
            NEW_MEMBER, destination_topic="cust1-out", tagged_events_topic=None
        )
        manager.dao.list_with_members.return_value = [_row(PIPELINE_A, "tenant-a", auto_add=True)]

        assert await manager.auto_add_member(NEW_MEMBER, _user()) == []
        manager.dao.set_members.assert_not_awaited()


class TestRestartIfRunning:
    @pytest.mark.asyncio
    async def test_no_op_when_stopped(self, manager: CorrelationPipelinesManager, provider: AsyncMock) -> None:
        manager.dao.get_with_relations.return_value = _row(PIPELINE_A, "tenant-a", jobs_status="stopped")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.restart_if_running(PIPELINE_A)

        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_refreshes_correlation_only(self, manager: CorrelationPipelinesManager, provider: AsyncMock) -> None:
        manager.dao.get_with_relations.return_value = _row(PIPELINE_A, "tenant-a", jobs_status="starting")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.restart_if_running(PIPELINE_A)

        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.launch_agent_job_deployment.assert_not_called()
        # The default is the cheap roll — nothing is deleted and no state is wiped.
        provider.delete_correlation_job_deployment.assert_not_called()
        provider.cleanup_correlation_job_storage.assert_not_called()

    @pytest.mark.asyncio
    async def test_cold_start_deletes_and_wipes_before_relaunching(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """The caller declares that its edit REMOVED a topic from ``KAFKA_INPUT_TOPICS``.

        A re-render would leave the departed topic assigned to the restored source and the job
        would keep consuming it — the leak measured in ``detectflow-agent``'s membership drill.
        """
        manager.dao.get_with_relations.return_value = _row(PIPELINE_A, "tenant-a", jobs_status="starting")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.restart_if_running(PIPELINE_A, cold_start=True)

        provider.delete_correlation_job_deployment.assert_awaited_once()
        provider.cleanup_correlation_job_storage.assert_awaited_once()
        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_cold_start_does_not_start_a_stopped_pipeline(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        manager.dao.get_with_relations.return_value = _row(PIPELINE_A, "tenant-a", jobs_status="stopped")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.restart_if_running(PIPELINE_A, cold_start=True)

        provider.delete_correlation_job_deployment.assert_not_called()
        provider.launch_correlation_job_deployment.assert_not_called()


class TestActiveThreatSyncCallSite:
    """Call site: the catalogue sync deleted threat rows, cascading away their exclusions."""

    @pytest.mark.asyncio
    async def test_deleted_threats_trigger_a_republish_of_the_affected_pipelines(self) -> None:
        from apps.managers.active_threats import ActiveThreatsSyncManager

        sync = ActiveThreatsSyncManager.__new__(ActiveThreatsSyncManager)
        sync.db = AsyncMock()
        sync._correlation_pipelines_for_threats = AsyncMock(return_value=[PIPELINE_A])
        manager = AsyncMock()

        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=manager):
            await sync._republish_chains_config_for_deleted_threats(["AT-1", "AT-2"])

        manager.republish_chains_config.assert_awaited_once()
        assert manager.republish_chains_config.await_args.kwargs["reason"] == "active_threat_catalogue_deleted"

    @pytest.mark.asyncio
    async def test_no_deletions_means_no_work(self) -> None:
        from apps.managers.active_threats import ActiveThreatsSyncManager

        sync = ActiveThreatsSyncManager.__new__(ActiveThreatsSyncManager)
        sync.db = AsyncMock()
        manager = AsyncMock()

        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=manager):
            await sync._republish_chains_config_for_deleted_threats([])

        manager.republish_chains_config.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pipelines_come_from_the_pre_delete_capture_not_a_live_query(self) -> None:
        """The exclusion rows are gone by the time the republish runs — CASCADE took them.

        A live lookup would therefore always resolve to nothing. The affected pipelines come from
        the map ``_capture_dropped_exclusions`` recorded BEFORE the delete.
        """
        from apps.managers.active_threats import ActiveThreatsSyncManager

        sync = ActiveThreatsSyncManager.__new__(ActiveThreatsSyncManager)
        sync.db = AsyncMock()
        sync._dropped_exclusions = {PIPELINE_A: {"AT-1"}, PIPELINE_B: {"AT-9"}}

        assert await sync._correlation_pipelines_for_threats(["AT-1"]) == [PIPELINE_A]
        assert await sync._correlation_pipelines_for_threats(["AT-2"]) == []

    @pytest.mark.asyncio
    async def test_nothing_captured_means_nothing_republished(self) -> None:
        from apps.managers.active_threats import ActiveThreatsSyncManager

        sync = ActiveThreatsSyncManager.__new__(ActiveThreatsSyncManager)
        sync.db = AsyncMock()
        sync._dropped_exclusions = {}
        manager = AsyncMock()

        with patch("apps.managers.active_threats.CorrelationPipelinesManager", return_value=manager):
            await sync._republish_chains_config_for_deleted_threats(["AT-1"])

        manager.republish_chains_config.assert_not_awaited()
