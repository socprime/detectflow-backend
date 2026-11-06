"""Unit tests for the composite runtime lifecycle, now scoped per correlation pipeline.

Architecture §1 + §9: a correlation pipeline's correlation-job and agent-job are launched and torn
down together. Order matters and partial failure must NOT auto-rollback (a healthy correlation-job
is more useful than nothing while we investigate the agent half's image-pull failure).

Ported from ``test_attack_chains_runtime_manager.py`` when the singleton runtime became N
instances. Every case now runs against **two** pipelines so the property the epic exists for is
actually asserted: pipeline A's lifecycle never touches pipeline B's deployments or PVC paths.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.exceptions import BadRequestError
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.providers.base import StorageCleanupOutcome
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")

CORR_A = f"corr-{PIPELINE_A}"
AGENT_A = f"agent-{PIPELINE_A}"
CORR_B = f"corr-{PIPELINE_B}"
AGENT_B = f"agent-{PIPELINE_B}"


def _cleaned(deployment_name: str) -> StorageCleanupOutcome:
    """A cleanup Job that ran and removed the state — the happy path for every test here."""
    return StorageCleanupOutcome(deployment_name, "succeeded")


def _member(pipeline_id: UUID) -> MagicMock:
    member = MagicMock()
    member.id = pipeline_id
    member.name = f"P-{pipeline_id}"
    member.destination_topic = f"dest-{pipeline_id}"
    member.tagged_events_topic = f"detectflow.tagged-events.{pipeline_id}"
    member.enabled = True
    return member


def _pipeline_row(
    pipeline_id: UUID,
    name: str,
    *,
    jobs_status: str = "stopped",
    members: list[MagicMock] | None = None,
) -> MagicMock:
    row = MagicMock()
    row.id = pipeline_id
    row.name = name
    row.jobs_status = jobs_status
    row.threshold_pct = 60
    row.correlation_window_seconds = 600
    row.auto_add_new_pipelines = False
    row.auto_add_pipeline_name_pattern = None
    row.correlation_deployment_name = f"corr-{pipeline_id}"
    row.agent_deployment_name = f"agent-{pipeline_id}"
    # One member with its own tagged-events topic: the shape of every startable pipeline. A member
    # without one contributes no input, and `start` refuses rather than falling back to the shared
    # platform-wide topic (`assert_startable`).
    row.members = members if members is not None else [_member(pipeline_id)]
    row.created_at = datetime(2026, 1, 1, tzinfo=UTC)
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


def _sample_user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


@pytest.fixture
def rows() -> dict[UUID, MagicMock]:
    """Two tenants, side by side, for every test in this module."""
    return {
        PIPELINE_A: _pipeline_row(PIPELINE_A, "tenant-a"),
        PIPELINE_B: _pipeline_row(PIPELINE_B, "tenant-b"),
    }


@pytest.fixture
def mock_dao(rows: dict[UUID, MagicMock]) -> AsyncMock:
    dao = AsyncMock()
    dao.get_with_relations.side_effect = lambda pipeline_id: rows.get(pipeline_id)
    dao.list_with_members.return_value = list(rows.values())
    return dao


@pytest.fixture
def mock_audit_dao() -> AsyncMock:
    dao = AsyncMock()
    # Default: no recent warning/error events -> every job reports healthy.
    dao.recent_health.return_value = {}
    return dao


@pytest.fixture
def mock_k8s_provider() -> AsyncMock:
    provider = AsyncMock(spec=KubernetesFlinkProvider)
    provider.launch_correlation_job_deployment = AsyncMock(
        side_effect=lambda config: {"operation": "created", "deployment_name": config.correlation_deployment_name}
    )
    provider.launch_agent_job_deployment = AsyncMock(
        side_effect=lambda config: {"operation": "created", "deployment_name": config.agent_deployment_name}
    )
    provider.delete_correlation_job_deployment = AsyncMock(return_value=True)
    provider.delete_agent_job_deployment = AsyncMock(return_value=True)
    provider.get_correlation_job_deployment_state = AsyncMock(return_value=("missing", None))
    provider.get_agent_job_deployment_state = AsyncMock(return_value=("missing", None))
    # stop() waits for FlinkDeployments to terminate, then wipes per-job state on the shared PVCs.
    provider._wait_for_deletion = MagicMock(return_value=None)
    # The provider watches the cleanup Job to a terminal state, so it reports an outcome rather
    # than "the API accepted the object" — a refusal means the state survived.
    provider.cleanup_correlation_job_storage = AsyncMock(side_effect=_cleaned)
    provider.cleanup_agent_job_storage = AsyncMock(side_effect=_cleaned)
    return provider


@pytest.fixture
def manager(mock_dao: AsyncMock, mock_audit_dao: AsyncMock) -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(AsyncMock())
    # `AsyncSession.expire` is synchronous (like `add`); an AsyncMock would leave an
    # un-awaited coroutine behind every membership write.
    mgr.db.expire = MagicMock()
    mgr.dao = mock_dao
    mgr.audit_dao = mock_audit_dao
    mgr.pipeline_dao = AsyncMock()
    # §20 deny-list lookup: empty means "monitors every non-blocked threat", the opt-out default.
    mgr.active_threats_dao = AsyncMock()
    mgr.active_threats_dao.get_excluded_threat_ids_by_pipeline_ids.return_value = {}
    return mgr


@pytest.fixture(autouse=True)
def _silence_side_channels():
    """Kafka publishes and activity records are side channels, not the behaviour under test."""
    with (
        patch("apps.managers.correlation_pipelines.chains_config_producer", AsyncMock()),
        patch("apps.managers.correlation_pipelines.activity_producer", AsyncMock()),
    ):
        yield


class TestStartRuntime:
    @pytest.mark.asyncio
    async def test_starts_correlation_first_then_agent(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        call_order: list[str] = []
        mock_k8s_provider.launch_correlation_job_deployment.side_effect = lambda config: (
            call_order.append("correlation") or {"operation": "created", "deployment_name": CORR_A}
        )
        mock_k8s_provider.launch_agent_job_deployment.side_effect = lambda config: (
            call_order.append("agent") or {"operation": "created", "deployment_name": AGENT_A}
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.start(PIPELINE_A)

        assert response.status == "starting"
        assert call_order == ["correlation", "agent"]
        # Both jobs of one pipeline are launched from ONE config object.
        corr_config = mock_k8s_provider.launch_correlation_job_deployment.await_args.args[0]
        agent_config = mock_k8s_provider.launch_agent_job_deployment.await_args.args[0]
        assert corr_config is agent_config
        assert corr_config.correlation_deployment_name == CORR_A
        assert corr_config.agent_deployment_name == AGENT_A
        assert corr_config.correlation_pipeline_id == str(PIPELINE_A)

    @pytest.mark.asyncio
    async def test_starting_one_pipeline_never_touches_the_other(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.start(PIPELINE_A)

        launched = {
            mock_k8s_provider.launch_correlation_job_deployment.await_args.args[0].correlation_deployment_name,
            mock_k8s_provider.launch_agent_job_deployment.await_args.args[0].agent_deployment_name,
        }
        assert launched == {CORR_A, AGENT_A}
        assert CORR_B not in launched and AGENT_B not in launched
        assert rows[PIPELINE_B].jobs_status == "stopped"

    @pytest.mark.asyncio
    async def test_returns_partial_when_agent_fails_without_rolling_back_correlation(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        mock_k8s_provider.launch_agent_job_deployment.side_effect = RuntimeError("image pull backoff")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.start(PIPELINE_A)

        assert response.status == "partial"
        assert response.correlation_job["deployment_name"] == CORR_A
        assert response.agent_job == {"error": "image pull backoff"}
        # Critical: a transient agent-side failure must NOT tear down a healthy correlation-job.
        mock_k8s_provider.delete_correlation_job_deployment.assert_not_called()
        mock_k8s_provider.delete_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_marks_jobs_starting_before_deployments_submitted(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.start(PIPELINE_A)

        assert rows[PIPELINE_A].jobs_status == "starting"
        manager.db.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_keeps_jobs_starting_on_partial_start(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        mock_k8s_provider.launch_agent_job_deployment.side_effect = RuntimeError("boom")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.start(PIPELINE_A)

        assert rows[PIPELINE_A].jobs_status == "starting"

    @pytest.mark.asyncio
    async def test_reverts_to_stopped_when_the_correlation_launch_raises(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        mock_k8s_provider.launch_correlation_job_deployment.side_effect = RuntimeError("quota exceeded")

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider),
            pytest.raises(RuntimeError, match="quota exceeded"),
        ):
            await manager.start(PIPELINE_A)

        assert rows[PIPELINE_A].jobs_status == "stopped"

    @pytest.mark.asyncio
    async def test_skips_agent_launch_when_stop_requested_after_correlation_start(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        async def refresh_side_effect(_: MagicMock) -> None:
            rows[PIPELINE_A].jobs_status = "stopping"

        manager.db.refresh.side_effect = refresh_side_effect

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.start(PIPELINE_A)

        assert response.status == "stopping"
        assert response.correlation_job["operation"] == "delete_requested"
        assert response.agent_job["operation"] == "skipped"
        mock_k8s_provider.launch_agent_job_deployment.assert_not_called()
        mock_k8s_provider.delete_correlation_job_deployment.assert_awaited_once_with(CORR_A)

    @pytest.mark.asyncio
    async def test_cleans_up_when_stop_requested_after_agent_start(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        refresh_count = 0

        async def refresh_side_effect(_: MagicMock) -> None:
            nonlocal refresh_count
            refresh_count += 1
            if refresh_count == 2:
                rows[PIPELINE_A].jobs_status = "stopping"

        manager.db.refresh.side_effect = refresh_side_effect

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.start(PIPELINE_A)

        assert response.status == "stopping"
        assert response.correlation_job["operation"] == "delete_requested"
        assert response.agent_job["operation"] == "delete_requested"
        mock_k8s_provider.delete_agent_job_deployment.assert_awaited_once_with(AGENT_A)
        mock_k8s_provider.delete_correlation_job_deployment.assert_awaited_once_with(CORR_A)

    @pytest.mark.asyncio
    async def test_raises_when_provider_not_kubernetes(self, manager: CorrelationPipelinesManager) -> None:
        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=MagicMock()),
            pytest.raises(BadRequestError, match="FLINK_PROVIDER=kubernetes"),
        ):
            await manager.start(PIPELINE_A)

    @pytest.mark.asyncio
    async def test_logs_single_composite_activity_entry_scoped_to_the_row(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        mock_activity = AsyncMock()

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider),
            patch("apps.managers.correlation_pipelines.activity_producer", mock_activity),
        ):
            await manager.start(PIPELINE_A, _sample_user())

        # ONE entry for the composite action, not two — and keyed by the row, never "singleton".
        mock_activity.log_action.assert_awaited_once()
        call_kwargs = mock_activity.log_action.await_args.kwargs
        assert call_kwargs["entity_type"] == "correlation_pipeline"
        assert call_kwargs["entity_id"] == str(PIPELINE_A)
        assert call_kwargs["entity_name"] == "tenant-a"

    @pytest.mark.asyncio
    async def test_publishes_config_before_launching(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        """A cold-started matcher must not correlate against a stale compacted record."""
        events: list[str] = []
        producer = AsyncMock()
        producer.publish = AsyncMock(side_effect=lambda envelope, *, topic: events.append(f"publish:{topic}"))
        mock_k8s_provider.launch_correlation_job_deployment.side_effect = lambda config: (
            events.append("launch") or {"operation": "created", "deployment_name": CORR_A}
        )

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider),
            patch("apps.managers.correlation_pipelines.chains_config_producer", producer),
        ):
            await manager.start(PIPELINE_A)

        assert events[0] == f"publish:detectflow.chains-config.{PIPELINE_A}"
        assert "launch" in events[1:]


class TestStopRuntime:
    @pytest.mark.asyncio
    async def test_stops_agent_first_then_correlation(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        call_order: list[str] = []
        mock_k8s_provider.delete_correlation_job_deployment.side_effect = lambda _name: (
            call_order.append("correlation") or True
        )
        mock_k8s_provider.delete_agent_job_deployment.side_effect = lambda _name: call_order.append("agent") or True

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        # Agent FIRST so it drains in-flight chains-raw; correlation SECOND so the producer
        # outlives the consumer.
        assert call_order == ["agent", "correlation"]
        assert response.status == "stopped"

    @pytest.mark.asyncio
    async def test_stopping_one_pipeline_never_touches_the_other(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """The whole point of the epic: tenant A's stop is invisible to tenant B."""
        rows[PIPELINE_A].jobs_status = "starting"
        rows[PIPELINE_B].jobs_status = "starting"

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.stop(PIPELINE_A)

        deleted = {
            mock_k8s_provider.delete_correlation_job_deployment.await_args.args[0],
            mock_k8s_provider.delete_agent_job_deployment.await_args.args[0],
        }
        cleaned = {
            mock_k8s_provider.cleanup_correlation_job_storage.await_args.args[0],
            mock_k8s_provider.cleanup_agent_job_storage.await_args.args[0],
        }
        waited = {call.args[0] for call in mock_k8s_provider._wait_for_deletion.call_args_list}
        assert deleted == {CORR_A, AGENT_A}
        # Cleanup keys on the deployment name, which IS the PVC subPath (§22).
        assert cleaned == {CORR_A, AGENT_A}
        assert waited == {CORR_A, AGENT_A}
        assert not ({CORR_B, AGENT_B} & (deleted | cleaned | waited))
        assert rows[PIPELINE_B].jobs_status == "starting"

    @pytest.mark.asyncio
    async def test_marks_jobs_stopping_before_delete(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", datetime.now(UTC))
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", datetime.now(UTC))

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        assert rows[PIPELINE_A].jobs_status == "stopping"
        assert response.status == "stopping"
        manager.db.flush.assert_awaited()
        manager.db.commit.assert_awaited()

    @pytest.mark.asyncio
    async def test_reports_not_found_when_deployments_missing(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        mock_k8s_provider.delete_correlation_job_deployment = AsyncMock(return_value=False)
        mock_k8s_provider.delete_agent_job_deployment = AsyncMock(return_value=False)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        assert response.correlation_job["operation"] == "not_found"
        assert response.agent_job["operation"] == "not_found"

    @pytest.mark.asyncio
    async def test_waits_for_both_deployments_before_running_cleanup(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """The cleanup-Job mustn't race a live TaskManager.

        ``_wait_for_deletion`` is called for both deployment names before either cleanup is
        dispatched (PVC mounts must release first).
        """
        rows[PIPELINE_A].jobs_status = "starting"
        events: list[str] = []
        mock_k8s_provider._wait_for_deletion = MagicMock(side_effect=lambda name, t: events.append(f"wait:{name}"))
        mock_k8s_provider.cleanup_correlation_job_storage = AsyncMock(
            side_effect=lambda name: events.append(f"cleanup:{name}") or _cleaned(name)
        )
        mock_k8s_provider.cleanup_agent_job_storage = AsyncMock(
            side_effect=lambda name: events.append(f"cleanup:{name}") or _cleaned(name)
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.stop(PIPELINE_A)

        wait_calls = mock_k8s_provider._wait_for_deletion.call_args_list
        assert len(wait_calls) == 2
        assert {call.args[0] for call in wait_calls} == {CORR_A, AGENT_A}
        for call in wait_calls:
            assert call.args[1] == 60

        # Both waits complete before either cleanup is dispatched.
        wait_indices = [i for i, e in enumerate(events) if e.startswith("wait:")]
        cleanup_indices = [i for i, e in enumerate(events) if e.startswith("cleanup:")]
        assert max(wait_indices) < min(cleanup_indices)

    @pytest.mark.asyncio
    async def test_cleanup_runs_for_both_jobs_on_stop(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        mock_k8s_provider.cleanup_correlation_job_storage.assert_awaited_once_with(CORR_A)
        mock_k8s_provider.cleanup_agent_job_storage.assert_awaited_once_with(AGENT_A)
        assert response.correlation_job["cleanup"] == "ok"
        assert response.agent_job["cleanup"] == "ok"

    @pytest.mark.asyncio
    async def test_cleanup_failure_does_not_fail_stop(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """Kafka-side recoverability beats K8s-side perfection.

        A failed cleanup must not propagate; ``status`` remains ``"stopped"`` and the cleanup error
        surfaces inline in the per-job payload — never the verbose ``ApiException`` body.
        """
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.cleanup_correlation_job_storage = AsyncMock(side_effect=RuntimeError("PVC offline"))

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        assert response.status == "stopped"
        assert response.correlation_job["cleanup"] == "error"
        assert response.agent_job["cleanup"] == "ok"
        mock_k8s_provider.cleanup_agent_job_storage.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_refused_cleanup_reports_the_reason_and_still_stops(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """The §22 script fails closed, so a refusal means the state SURVIVED.

        That is not an exception — the Job ran and said no — and reporting it as ``"ok"`` sends the
        next start into checkpoints the operator believes are gone. It must be visible in the stop
        payload and in the activity log, and it must still not fail the stop.
        """
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.cleanup_correlation_job_storage = AsyncMock(
            return_value=StorageCleanupOutcome(CORR_A, "failed", "refusing: '/mnt/ha/corr-a' is a symlink")
        )

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider),
            patch("apps.managers.correlation_pipelines.activity_producer", AsyncMock()) as activity,
        ):
            response = await manager.stop(PIPELINE_A)

        assert response.status == "stopped"
        assert "is a symlink" in response.correlation_job["cleanup"]
        assert response.agent_job["cleanup"] == "ok"
        entry = activity.log_action.await_args.kwargs
        assert entry["severity"] == "warning"
        assert "is a symlink" in entry["details"]

    @pytest.mark.asyncio
    async def test_wait_timeout_skips_cleanup_to_avoid_live_pvc_race(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """A wait timeout MUST short-circuit cleanup for that deployment.

        The TaskManager pod may still be holding the PVC mount; dispatching ``rm -rf`` against a
        live mount is the corruption vector this whole feature is meant to avoid.
        """
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider._wait_for_deletion = MagicMock(
            side_effect=Exception("Timeout waiting for FlinkDeployment to be deleted after 60s")
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        assert response.status == "stopped"
        mock_k8s_provider.cleanup_correlation_job_storage.assert_not_called()
        mock_k8s_provider.cleanup_agent_job_storage.assert_not_called()
        assert response.correlation_job["cleanup"] == "skipped: wait_timeout"
        assert response.agent_job["cleanup"] == "skipped: wait_timeout"

    @pytest.mark.asyncio
    async def test_one_side_wait_timeout_does_not_block_other_cleanup(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """When ONLY correlation-job's wait times out, the agent-job leg still cleans up."""
        rows[PIPELINE_A].jobs_status = "starting"

        def _wait_side_effect(name: str, timeout: int) -> None:
            if name == CORR_A:
                raise Exception(f"Timeout waiting for FlinkDeployment {name} after 60s")
            return None

        mock_k8s_provider._wait_for_deletion = MagicMock(side_effect=_wait_side_effect)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            response = await manager.stop(PIPELINE_A)

        assert response.status == "stopped"
        mock_k8s_provider.cleanup_correlation_job_storage.assert_not_called()
        assert response.correlation_job["cleanup"] == "skipped: wait_timeout"
        mock_k8s_provider.cleanup_agent_job_storage.assert_awaited_once()
        assert response.agent_job["cleanup"] == "ok"


class TestGetRuntimeState:
    @pytest.mark.asyncio
    async def test_running_when_both_jobs_running(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "running"
        assert state.correlation_job.status == "running"
        assert state.agent_job.status == "running"
        assert state.correlation_job.deployment_name == CORR_A
        assert state.job_uptime is not None

    @pytest.mark.asyncio
    async def test_partial_when_only_one_running(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = (
            "running",
            datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("failed", None)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "partial"
        # job_uptime is only meaningful when the WHOLE runtime is up.
        assert state.job_uptime is None

    @pytest.mark.asyncio
    async def test_starting_when_enabled_and_neither_running(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "starting"
        assert state.correlation_job.status == "starting"
        assert state.agent_job.status == "starting"

    @pytest.mark.asyncio
    async def test_stopped_when_disabled_and_neither_running(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
    ) -> None:
        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "stopped"
        assert state.correlation_job.status == "missing"
        assert state.agent_job.status == "missing"

    @pytest.mark.asyncio
    async def test_stopping_when_stop_requested_and_deployments_still_exist(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "stopping"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("starting", None)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "stopping"
        assert state.correlation_job.status == "stopping"
        assert state.agent_job.status == "stopping"
        assert state.job_uptime is None

    @pytest.mark.asyncio
    async def test_persists_stopped_when_stop_requested_and_deployments_are_missing(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "stopping"

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "stopped"
        assert rows[PIPELINE_A].jobs_status == "stopped"
        manager.db.flush.assert_awaited()
        manager.db.commit.assert_awaited()


class TestRuntimeStateFromK8s:
    @pytest.mark.asyncio
    async def test_partial_when_enabled_and_agent_failed(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = (
            "running",
            datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("failed", None)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "partial"
        # A failed observation never rewrites the operator's desired state.
        assert rows[PIPELINE_A].jobs_status == "starting"

    @pytest.mark.asyncio
    async def test_uses_later_started_at_for_runtime_uptime(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = (
            "running",
            datetime(2026, 1, 1, 9, 0, 0, tzinfo=UTC),
        )
        mock_k8s_provider.get_agent_job_deployment_state.return_value = (
            "running",
            datetime(2026, 1, 1, 9, 5, 0, tzinfo=UTC),
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "running"
        assert state.job_uptime is not None

    @pytest.mark.asyncio
    async def test_k8s_api_error_falls_back_to_the_desired_state(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.side_effect = Exception(
            '(403)\nReason: Forbidden\nHTTP response body: {"reason":"Forbidden"}'
        )

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.correlation_job.status == "starting"


class TestRuntimeHealthSignal:
    """Window-based degraded/health signal derived from recent audit activity.

    The signal is ADDITIVE: it never alters the K8s-derived ``status`` tile or the composite
    ``overall``. It self-heals because ``recent_health`` only sees events inside a trailing window —
    so when warnings/errors stop arriving the summary goes empty and health falls back to healthy.
    """

    @pytest.mark.asyncio
    async def test_healthy_when_no_recent_events(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)
        mock_audit_dao.recent_health.return_value = {}

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.correlation_job.health == "healthy"
        assert state.correlation_job.last_issue is None
        assert state.agent_job.health == "healthy"
        assert state.overall == "running"

    @pytest.mark.asyncio
    async def test_degraded_on_recent_warning(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)
        mock_audit_dao.recent_health.return_value = {
            CORR_A: {
                "worst_severity": "warning",
                "last_issue_details": "backpressure on union source",
                "last_event_at": ts,
            }
        }

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.correlation_job.health == "degraded"
        assert state.correlation_job.last_issue == "backpressure on union source"
        assert state.agent_job.health == "healthy"
        # Degraded is independent of the still-running tile.
        assert state.correlation_job.status == "running"
        assert state.overall == "running"

    @pytest.mark.asyncio
    async def test_error_on_recent_error(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)
        mock_audit_dao.recent_health.return_value = {
            AGENT_A: {
                "worst_severity": "error",
                "last_issue_details": "OpenAI request failed: 503",
                "last_event_at": ts,
            }
        }

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.agent_job.health == "error"
        assert state.agent_job.last_issue == "OpenAI request failed: 503"
        assert state.correlation_job.health == "healthy"
        assert state.agent_job.status == "running"
        assert state.overall == "running"

    @pytest.mark.asyncio
    async def test_query_failure_degrades_gracefully(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        """A DB/query failure must NOT break the runtime-state response."""
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)
        mock_audit_dao.recent_health.side_effect = RuntimeError("DB connection refused")

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            state = await manager.get_runtime_state(PIPELINE_A)

        assert state.overall == "running"
        assert state.correlation_job.health == "healthy"
        assert state.agent_job.health == "healthy"
        assert state.job_uptime is not None

    @pytest.mark.asyncio
    async def test_queries_audit_for_this_rows_deployment_names_only(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
        rows: dict[UUID, MagicMock],
    ) -> None:
        ts = datetime(2026, 1, 1, tzinfo=UTC)
        rows[PIPELINE_A].jobs_status = "starting"
        mock_k8s_provider.get_correlation_job_deployment_state.return_value = ("running", ts)
        mock_k8s_provider.get_agent_job_deployment_state.return_value = ("running", ts)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            await manager.get_runtime_state(PIPELINE_A)

        mock_audit_dao.recent_health.assert_awaited_once()
        kwargs = mock_audit_dao.recent_health.await_args.kwargs
        assert set(kwargs["entity_ids"]) == {CORR_A, AGENT_A}
        assert kwargs["window_seconds"] > 0


class TestListRuntimeSnapshots:
    """Constraint 11: one SSE tick, one health query, bounded Kubernetes fan-out."""

    @pytest.mark.asyncio
    async def test_one_batched_health_query_for_every_pipeline(
        self,
        manager: CorrelationPipelinesManager,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
    ) -> None:
        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            snapshots = await manager.list_runtime_snapshots()

        assert {snapshot.correlation_pipeline_id for snapshot in snapshots} == {PIPELINE_A, PIPELINE_B}
        # 2N deployment names, ONE query — never one call per pipeline.
        mock_audit_dao.recent_health.assert_awaited_once()
        assert set(mock_audit_dao.recent_health.await_args.kwargs["entity_ids"]) == {
            CORR_A,
            AGENT_A,
            CORR_B,
            AGENT_B,
        }

    @pytest.mark.asyncio
    async def test_no_pipelines_means_no_kubernetes_and_no_health_query(
        self,
        manager: CorrelationPipelinesManager,
        mock_dao: AsyncMock,
        mock_k8s_provider: AsyncMock,
        mock_audit_dao: AsyncMock,
    ) -> None:
        mock_dao.list_with_members.return_value = []

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=mock_k8s_provider):
            assert await manager.list_runtime_snapshots() == []

        mock_k8s_provider.get_correlation_job_deployment_state.assert_not_called()
        mock_audit_dao.recent_health.assert_not_awaited()
