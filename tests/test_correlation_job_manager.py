"""Unit tests for the remaining correlation-job-scoped helpers on AttackChainsManager.

The composite runtime lifecycle (``start_runtime`` / ``stop_runtime`` /
``get_runtime_state``) is covered by ``test_attack_chains_runtime_manager.py``.
What's left here:

- the deprecated singleton surface, now bound to the ``Default`` correlation pipeline: every shim
  resolves it by name and delegates, and the ``attack_chain_configuration`` read/write path is
  gone. The HTTP contract of the remaining singleton operations is pinned by
  ``test_singleton_endpoints_rebind.py``.
- ``get_correlation_job_runtime_snapshot`` — used by pipeline refresh/dashboard compatibility
  paths. Uptime reflects the composite "whole runtime is up" semantic (both jobs must be RUNNING).
- ``restart_correlation_job_if_running`` — the orchestrator's old subscription-drift path; does
  NOT touch agent-job.
- ``deployment_config_for_row`` — maps one ``correlation_pipelines`` row to the rendering inputs of
  its two FlinkDeployments.
- ``get_graph_data`` — backs ``GET /api/v1/correlation_dashboard/snapshot`` and the SSE stream.
"""

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import ValidationError

from apps.core.exceptions import NotFoundError
from apps.core.schemas import (
    AttackChainConfigurationUpdateRequest,
    CorrelationJobGraphBlock,
    CorrelationJobGraphData,
    CorrelationJobGraphPoint,
)
from apps.managers.attack_chains import AttackChainsManager
from apps.managers.correlation_pipelines import CorrelationPipelinesManager, deployment_config_for_row
from apps.providers.kubernetes import KubernetesFlinkProvider


def _sample_user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


@pytest.fixture
def mock_db() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_dao() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_k8s_provider() -> AsyncMock:
    provider = AsyncMock(spec=KubernetesFlinkProvider)
    provider.launch_correlation_job_deployment = AsyncMock(
        return_value={"operation": "updated", "deployment_name": "correlation-job"}
    )
    provider.launch_agent_job_deployment = AsyncMock(
        return_value={"operation": "updated", "deployment_name": "agent-job"}
    )
    provider.get_correlation_job_deployment_state = AsyncMock(return_value=("missing", None))
    provider.get_agent_job_deployment_state = AsyncMock(return_value=("missing", None))
    provider.delete_correlation_job_deployment = AsyncMock(return_value=True)
    provider.delete_agent_job_deployment = AsyncMock(return_value=True)
    return provider


@pytest.fixture
def mock_correlation_pipeline_dao() -> AsyncMock:
    dao = AsyncMock()
    # No `Default` row until a test binds one: every legacy operation then 404s, which is the
    # honest answer once the singleton launch path is gone.
    dao.list_with_members.return_value = []
    dao.get_by_name.return_value = None
    dao.get_with_relations.return_value = None
    return dao


@pytest.fixture(autouse=True)
def _shared_correlation_pipeline_dao(mock_correlation_pipeline_dao: AsyncMock):
    """Give the ``CorrelationPipelinesManager`` the shims build inline the same mocked DAO.

    Every legacy operation resolves ``Default`` through a real ``CorrelationPipelinesManager``
    constructed on the manager's own session, so stubbing only ``AttackChainsManager``'s DAO would
    leave the resolver talking to a DAO nothing configured.
    """
    with patch(
        "apps.managers.correlation_pipelines.CorrelationPipelineDAO",
        return_value=mock_correlation_pipeline_dao,
    ):
        yield


def _bind_default(dao: AsyncMock, row: MagicMock) -> MagicMock:
    """Make ``Default`` resolve to ``row`` — by name, then for the members-loaded read."""
    dao.get_by_name.side_effect = lambda name: row if name == "Default" else None
    dao.get_with_relations.return_value = row
    return row


@pytest.fixture
def manager(
    mock_db: AsyncMock,
    mock_dao: AsyncMock,
    mock_correlation_pipeline_dao: AsyncMock,
) -> AttackChainsManager:
    mgr = AttackChainsManager(mock_db)
    mgr.dao = mock_dao
    mgr.correlation_pipeline_dao = mock_correlation_pipeline_dao
    return mgr


class TestRestartCorrelationJobIfRunning:
    """The shim no longer launches anything itself — it resolves ``Default`` and delegates."""

    @pytest.mark.asyncio
    async def test_raises_when_no_default_pipeline_exists(self, manager: AttackChainsManager) -> None:
        with pytest.raises(NotFoundError, match="correlation_pipelines"):
            await manager.restart_correlation_job_if_running(_sample_user())

    @pytest.mark.asyncio
    async def test_delegates_to_the_per_pipeline_manager_and_reports_its_deployment(
        self, manager: AttackChainsManager, mock_correlation_pipeline_dao: AsyncMock
    ) -> None:
        # The narrow contract is unchanged: re-render correlation-job's spec on subscription
        # drift, never agent-job's, whose spec does not depend on pipeline state.
        row = _correlation_pipeline_row()
        row.members = [_pipeline_row(UUID("aaaaaaaa-0000-4000-8000-000000000001"), "P1", tagged_events_topic="te.p1")]
        _bind_default(mock_correlation_pipeline_dao, row)

        with patch.object(CorrelationPipelinesManager, "restart_if_running", new_callable=AsyncMock) as delegate:
            result = await manager.restart_correlation_job_if_running(_sample_user())

        delegate.assert_awaited_once()
        assert delegate.await_args.args[0] == row.id
        assert result is not None
        assert result.action == "run"
        assert result.deployment_name == f"corr-{row.id}"
        assert result.input_topics == ["te.p1"]


class TestGetConfiguration:
    """The legacy shape, projected from the ``Default`` correlation pipeline."""

    @pytest.mark.asyncio
    async def test_returns_threshold_window_and_auto_add(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
    ) -> None:
        """Destination-topic toggles are gone; membership is the per-pipeline API."""
        row = _correlation_pipeline_row()
        _bind_default(mock_correlation_pipeline_dao, row)

        response = await manager.get_configuration()

        assert "monitored_topics" not in type(response).model_fields
        assert "monitored_topics" not in response.model_dump()
        assert response.id == 1  # the legacy field is an int and cannot carry the pipeline UUID
        assert response.threshold_pct == row.threshold_pct
        assert response.correlation_window_seconds == row.correlation_window_seconds
        assert response.auto_add_new_pipelines == row.auto_add_new_pipelines

    @pytest.mark.asyncio
    async def test_an_inherited_window_is_projected_as_the_legacy_default(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
    ) -> None:
        """NULL means "inherit each HoS rule's timeframe"; the legacy field is a non-null int.

        300 is what the deprecated column's own default rendered, i.e. what this dialog showed
        before the migration — the honest projection of "never tuned".
        """
        _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row(correlation_window_seconds=None))

        assert (await manager.get_configuration()).correlation_window_seconds == 300

    @pytest.mark.asyncio
    async def test_no_default_pipeline_is_a_404(self, manager: AttackChainsManager) -> None:
        with pytest.raises(NotFoundError, match="correlation_pipelines"):
            await manager.get_configuration()


class TestUpdateConfiguration:
    """The legacy Configure dialog, applied to ``Default`` through the per-pipeline manager."""

    @pytest.mark.asyncio
    async def test_threshold_delegates_without_membership_change(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
    ) -> None:
        row = _correlation_pipeline_row()
        _bind_default(mock_correlation_pipeline_dao, row)
        request = AttackChainConfigurationUpdateRequest(threshold_pct=75)

        with patch.object(CorrelationPipelinesManager, "update", new_callable=AsyncMock) as delegate:
            await manager.update(request, _sample_user())

        delegate.assert_awaited_once()
        assert delegate.await_args.args[0] == row.id
        body = delegate.await_args.args[1]
        assert body.threshold_pct == 75
        assert body.model_fields_set == {"threshold_pct"}
        assert delegate.await_args.kwargs["confirm_restart"] is True

    def test_monitored_topics_is_rejected_by_the_schema(self) -> None:
        """Membership is PATCH /correlation_pipelines/{id} with member_pipeline_ids."""
        with pytest.raises(ValidationError):
            AttackChainConfigurationUpdateRequest(monitored_topics=[{"name": "output-logs", "enabled": True}])

    @pytest.mark.asyncio
    async def test_nothing_is_written_to_the_deprecated_singleton_row(
        self,
        manager: AttackChainsManager,
        mock_dao: AsyncMock,
        mock_correlation_pipeline_dao: AsyncMock,
    ) -> None:
        """``attack_chain_configuration`` survives only as a rollback record now."""
        _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row())

        with patch.object(CorrelationPipelinesManager, "update", new_callable=AsyncMock):
            await manager.update(AttackChainConfigurationUpdateRequest(threshold_pct=75), _sample_user())

        mock_dao.ensure_configuration_singleton.assert_not_awaited()
        mock_dao.get_configuration_singleton.assert_not_awaited()


class TestCorrelationJobK8sSyncResilience:
    @pytest.mark.asyncio
    async def test_sync_skips_on_k8s_api_error(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row(jobs_status="starting"))
        mock_k8s_provider.get_correlation_job_deployment_state.side_effect = Exception(
            '(403)\nReason: Forbidden\nHTTP response body: {"reason":"Forbidden"}'
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            snapshot = await manager.get_correlation_job_runtime_snapshot()

        assert snapshot is None
        manager.db.flush.assert_not_awaited()


class TestCorrelationJobRuntimeSnapshot:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_started(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row(jobs_status="stopped"))
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("missing", None)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("missing", None)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            assert await manager.get_correlation_job_runtime_snapshot() is None

    @pytest.mark.asyncio
    async def test_returns_uptime_when_runtime_fully_up(
        self,
        manager: AttackChainsManager,
        mock_correlation_pipeline_dao: AsyncMock,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        started_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row(jobs_status="starting"))
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", started_at)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", started_at)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            snapshot = await manager.get_correlation_job_runtime_snapshot()

        assert snapshot is not None
        assert "job_uptime" in snapshot
        assert isinstance(snapshot["job_uptime"], str)
        assert len(snapshot["job_uptime"]) > 0


class TestCorrelationJobGraphData:
    @pytest.mark.asyncio
    async def test_filter_options_include_chain_and_monitored_threat_values(self, manager: AttackChainsManager) -> None:
        manager.dao.get_distinct_chain_severities.return_value = ["high"]
        manager.dao.get_distinct_chain_statuses.return_value = ["new", "resolved"]

        active_threats_dao = AsyncMock()
        active_threats_dao.get_distinct_severities.return_value = ["critical"]
        active_threats_dao.get_distinct_array_values.side_effect = [
            ["finance"],
            ["ransomware"],
            ["europe"],
        ]
        manager.active_threats_dao = active_threats_dao

        response = await manager.get_filter_options()
        payload = response.model_dump(mode="json", by_alias=True)

        assert payload["attack_chains"]["group"] == [
            {"label": "No Grouping", "value": "none"},
            {"label": "Hostname", "value": "hostname"},
            {"label": "Chain Name", "value": "chain_name"},
        ]
        assert payload["attack_chains"]["severity"] == ["high"]
        assert payload["attack_chains"]["status"] == ["new", "resolved"]
        assert payload["monitored_threats"] == {
            "severity": ["critical"],
            "industry": ["finance"],
            "threatType": ["ransomware"],
            "region": ["europe"],
        }

    @pytest.mark.asyncio
    async def test_builds_block_payload_with_totals(self, manager: AttackChainsManager) -> None:
        manager.dao.get_graph_daily_series.return_value = [(date(2026, 5, 20), 2)]
        manager.dao.get_attack_chains_count.return_value = 5
        manager.dao.get_affected_hostnames_count.return_value = 3
        manager.dao.get_graph_affected_hostnames_daily_series.return_value = [(date(2026, 5, 20), 1)]
        manager.dao.get_tagged_events_count.return_value = 11
        manager.dao.get_graph_tagged_events_daily_series.return_value = [(date(2026, 5, 21), 8)]
        manager.dao.get_matched_rules_counts.return_value = (2, 9)
        manager.dao.get_graph_matched_rules_daily_series.return_value = [(date(2026, 5, 21), 2)]

        active_threats_dao = AsyncMock()
        active_threats_dao.get_monitored_threats_count.return_value = 7
        active_threats_dao.get_unblocked_threats_count.return_value = 4
        active_threats_dao.get_graph_daily_series.return_value = [(date(2026, 5, 19), 6)]
        manager.active_threats_dao = active_threats_dao

        graph_data = await manager.get_graph_data()

        assert [block.name for block in graph_data.blocks] == [
            "ATTACK CHAINS",
            "MONITORED THREATS",
            "AFFECTED HOSTNAMES",
            "TAGGED EVENTS",
            "MATCHED RULES",
        ]
        assert graph_data.blocks[0].count_all == 5
        assert graph_data.blocks[0].count_active is None
        assert graph_data.blocks[0].points == [CorrelationJobGraphPoint(name="20 May 2026", value=2)]
        assert graph_data.blocks[1].count_all == 7
        assert graph_data.blocks[1].count_active == 4
        assert graph_data.blocks[1].points == [CorrelationJobGraphPoint(name="19 May 2026", value=6)]
        assert graph_data.blocks[2].count_all == 3
        assert graph_data.blocks[2].points == [CorrelationJobGraphPoint(name="20 May 2026", value=1)]
        assert graph_data.blocks[3].count_all == 11
        assert graph_data.blocks[3].count_active is None
        assert graph_data.blocks[3].points == [CorrelationJobGraphPoint(name="21 May 2026", value=8)]
        assert graph_data.blocks[4].count_all == 9
        assert graph_data.blocks[4].count_active == 2
        assert graph_data.blocks[4].points == [CorrelationJobGraphPoint(name="21 May 2026", value=2)]

    def test_graph_data_serializes_with_blocks_key(self) -> None:
        graph_data = CorrelationJobGraphData(
            blocks=[
                CorrelationJobGraphBlock(
                    name="ATTACK CHAINS",
                    count_all=1,
                    points=[CorrelationJobGraphPoint(name="20 May 2026", value=1)],
                )
            ]
        )

        payload = graph_data.model_dump(mode="json")

        assert "blocks" in payload
        assert "BLOCKS" not in payload
        assert payload["blocks"][0]["name"] == "ATTACK CHAINS"


def _pipeline_row(
    pipeline_id: UUID,
    name: str,
    *,
    tagged_events_topic: str | None = None,
    destination_topic: str = "output-logs",
) -> MagicMock:
    member = MagicMock()
    member.id = pipeline_id
    member.name = name
    member.tagged_events_topic = tagged_events_topic
    member.destination_topic = destination_topic
    return member


def _correlation_pipeline_row(**overrides) -> MagicMock:
    row = MagicMock()
    row.id = UUID("11111111-1111-4111-8111-111111111111")
    row.name = "Customer A"
    row.threshold_pct = 75
    row.correlation_window_seconds = 600
    row.correlation_deployment_name = None
    row.agent_deployment_name = None
    row.members = []
    row.auto_add_new_pipelines = False
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
    for field, value in overrides.items():
        setattr(row, field, value)
    return row


class TestDeploymentConfigForRow:
    """One ``correlation_pipelines`` row -> the rendering inputs of both its Flink jobs."""

    @pytest.mark.asyncio
    async def test_maps_names_topics_and_correlation_parameters(self, manager: AttackChainsManager) -> None:
        row = _correlation_pipeline_row()
        row.members = [_pipeline_row(UUID("aaaaaaaa-0000-4000-8000-000000000001"), "P1", tagged_events_topic="te.p1")]

        config = deployment_config_for_row(row)

        assert config.correlation_pipeline_id == str(row.id)
        assert config.correlation_pipeline_name == "Customer A"
        assert config.correlation_deployment_name == f"corr-{row.id}"
        assert config.agent_deployment_name == f"agent-{row.id}"
        assert config.input_topics == ["te.p1"]
        assert config.chains_config_topic == f"detectflow.chains-config.{row.id}"
        assert config.chains_raw_topic == f"detectflow.chains-raw.{row.id}"
        assert config.chains_analysed_topic == f"detectflow.chains-analysed.{row.id}"
        assert config.threshold_pct == 75
        assert config.correlation_window_seconds == 600

    @pytest.mark.asyncio
    async def test_persisted_deployment_names_win_over_generated_ones(self, manager: AttackChainsManager) -> None:
        """A K8s object must survive a rename — and a change of naming scheme."""
        row = _correlation_pipeline_row(correlation_deployment_name="corr-legacy", agent_deployment_name="agent-legacy")

        config = deployment_config_for_row(row)

        assert config.correlation_deployment_name == "corr-legacy"
        assert config.agent_deployment_name == "agent-legacy"

    @pytest.mark.asyncio
    async def test_null_resources_inherit_the_production_defaults(self, manager: AttackChainsManager) -> None:
        """NULL means "inherit settings" — that is what keeps 4g / 3.0 in force."""
        config = deployment_config_for_row(_correlation_pipeline_row())

        assert config.correlation_taskmanager_memory_mb == 4096  # settings 4g
        assert config.correlation_taskmanager_cpu == 3.0
        assert config.correlation_jobmanager_memory_mb == 2048
        assert config.agent_taskmanager_memory_mb == 3072  # settings 3g
        assert config.agent_taskmanager_cpu == 1.0

    @pytest.mark.asyncio
    async def test_row_resources_override_the_defaults(self, manager: AttackChainsManager) -> None:
        row = _correlation_pipeline_row(correlation_taskmanager_memory_mb=8192, agent_parallelism=3)

        config = deployment_config_for_row(row)

        assert config.correlation_taskmanager_memory_mb == 8192
        assert config.agent_parallelism == 3
        assert config.correlation_taskmanager_cpu == 3.0  # untouched columns still inherit

    @pytest.mark.asyncio
    async def test_members_without_their_own_topic_contribute_nothing(self, manager: AttackChainsManager) -> None:
        """A NULL ``tagged_events_topic`` renders NO topic — it used to render the shared one.

        The old fallback answered "the job must not crash-loop on an empty KAFKA_INPUT_TOPICS" and
        got "which topic" wrong: ``detectflow.tagged-events`` is the platform-wide topic carrying
        EVERY customer's tagged events, so one unassigned member quietly subscribed that customer's
        correlation job to all of them. Only assigned members count now, and a pipeline that
        resolves to nothing is refused at start (``assert_startable``) instead of started wrong.
        """
        row = _correlation_pipeline_row()
        row.members = [
            _pipeline_row(UUID("aaaaaaaa-0000-4000-8000-000000000001"), "P1", tagged_events_topic=None),
            _pipeline_row(UUID("aaaaaaaa-0000-4000-8000-000000000002"), "P2", tagged_events_topic=None),
            _pipeline_row(UUID("aaaaaaaa-0000-4000-8000-000000000003"), "P3", tagged_events_topic="te.p3"),
        ]

        config = deployment_config_for_row(row)

        assert config.input_topics == ["te.p3"]

    @pytest.mark.asyncio
    async def test_pipeline_without_members_renders_no_topics(self, manager: AttackChainsManager) -> None:
        """Empty is the honest answer, and the start path turns it into an actionable 400.

        Rendering the shared topic here made a member-less pipeline look startable and then
        correlate every other customer's events; during the singleton cutover it also built every
        chain twice, because the legacy job reads that same topic.
        """
        config = deployment_config_for_row(_correlation_pipeline_row())

        assert config.input_topics == []


class TestDefaultPipelineBinding:
    """Every quarantined operation addresses the ``Default`` correlation pipeline, or 404s."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("shim", ["start_runtime", "stop_runtime"])
    async def test_runtime_shims_delegate_to_the_default_pipeline(
        self, manager: AttackChainsManager, mock_correlation_pipeline_dao: AsyncMock, shim: str
    ) -> None:
        row = _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row())
        delegated = "start" if shim == "start_runtime" else "stop"

        with patch.object(CorrelationPipelinesManager, delegated, new_callable=AsyncMock) as delegate:
            await getattr(manager, shim)(_sample_user())

        delegate.assert_awaited_once()
        assert delegate.await_args.args[0] == row.id

    @pytest.mark.asyncio
    async def test_runtime_state_delegates_to_the_default_pipeline(
        self, manager: AttackChainsManager, mock_correlation_pipeline_dao: AsyncMock
    ) -> None:
        row = _bind_default(mock_correlation_pipeline_dao, _correlation_pipeline_row())

        with patch.object(CorrelationPipelinesManager, "get_runtime_state", new_callable=AsyncMock) as delegate:
            await manager.get_runtime_state()

        assert delegate.await_args.args[0] == row.id

    @pytest.mark.asyncio
    @pytest.mark.parametrize("shim", ["get_configuration", "get_runtime_state", "start_runtime", "stop_runtime"])
    async def test_no_default_pipeline_is_a_not_found_everywhere(self, manager: AttackChainsManager, shim: str) -> None:
        """The pre-epic fallback is gone: with no ``Default`` row there is nothing to address.

        Before the data migration the singleton path launched ``correlation-job`` / ``agent-job``
        from ``attack_chain_configuration``. That launch path was deleted with the migration, so
        the honest answer is a 404 naming its replacement.
        """
        with pytest.raises(NotFoundError, match="correlation_pipelines"):
            await getattr(manager, shim)()

    @pytest.mark.asyncio
    async def test_a_second_tenant_does_not_make_a_legacy_call_ambiguous(
        self, manager: AttackChainsManager, mock_correlation_pipeline_dao: AsyncMock
    ) -> None:
        """Resolution is by NAME, not "the only row" — otherwise every legacy call would 409."""
        default_row = _correlation_pipeline_row(name="Default")
        mock_correlation_pipeline_dao.list_with_members.return_value = [
            _correlation_pipeline_row(name="Customer B"),
            default_row,
        ]
        _bind_default(mock_correlation_pipeline_dao, default_row)

        with patch.object(CorrelationPipelinesManager, "start", new_callable=AsyncMock) as delegate:
            await manager.start_runtime(_sample_user())

        assert delegate.await_args.args[0] == default_row.id
