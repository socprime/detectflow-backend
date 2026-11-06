"""Duplicating a correlation pipeline (frontend contract).

The ticket's "works like copying a regular pipeline" is false — no duplicate capability exists for
any pipeline type in this control plane — so the semantics live in the manager and are pinned here:
copy the configuration (including members and the deny-list), never the identity, never the runtime.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.exceptions import ConflictError, ServiceUnavailableError
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.modules.kafka.topic_lifecycle import TopicProvisioningError
from apps.providers.kubernetes import KubernetesFlinkProvider

SOURCE_ID = UUID("11111111-1111-4111-8111-111111111111")
COPY_ID = UUID("22222222-2222-4222-8222-222222222222")
MEMBER_ONE = UUID("aaaaaaaa-0000-4000-8000-000000000001")


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


def _member(pipeline_id: UUID) -> MagicMock:
    member = MagicMock()
    member.id = pipeline_id
    member.name = "P1"
    member.destination_topic = "cust1-out"
    member.tagged_events_topic = f"detectflow.tagged-events.{pipeline_id}"
    member.enabled = True
    return member


def _row(pipeline_id: UUID, name: str, **overrides) -> MagicMock:
    row = MagicMock()
    row.id = pipeline_id
    row.name = name
    row.jobs_status = "stopped"
    row.threshold_pct = 60
    row.correlation_window_seconds = None
    row.auto_add_new_pipelines = False
    row.auto_add_pipeline_name_pattern = None
    row.correlation_deployment_name = f"corr-{pipeline_id}"
    row.agent_deployment_name = f"agent-{pipeline_id}"
    row.members = []
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
    for field, value in overrides.items():
        setattr(row, field, value)
    return row


@pytest.fixture
def source() -> MagicMock:
    row = _row(
        SOURCE_ID,
        "tenant-a",
        threshold_pct=85,
        correlation_window_seconds=1800,
        auto_add_new_pipelines=True,
        auto_add_pipeline_name_pattern="cust1-*",
        correlation_taskmanager_memory_mb=8192,
        jobs_status="starting",
    )
    row.members = [_member(MEMBER_ONE)]
    return row


@pytest.fixture
def copy_row() -> MagicMock:
    return _row(COPY_ID, "tenant-a (Copy)")


@pytest.fixture
def topic_service() -> AsyncMock:
    service = AsyncMock()
    service.ensure_correlation_pipeline_topics.return_value = []
    return service


@pytest.fixture
def manager(source: MagicMock, copy_row: MagicMock, topic_service: AsyncMock) -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(AsyncMock())
    # `AsyncSession.expire` is synchronous (like `add`); an AsyncMock would leave an
    # un-awaited coroutine behind every membership write.
    mgr.db.expire = MagicMock()
    mgr.dao = AsyncMock()
    mgr.pipeline_dao = AsyncMock()

    async def _get_by_ids(ids: list) -> list:
        return [_member(pipeline_id) for pipeline_id in ids]

    mgr.pipeline_dao.get_by_ids.side_effect = _get_by_ids
    mgr.audit_dao = AsyncMock()
    # §20 deny-list lookup: empty means "monitors every non-blocked threat", the opt-out default.
    mgr.active_threats_dao = AsyncMock()
    mgr.active_threats_dao.get_excluded_threat_ids_by_pipeline_ids.return_value = {}

    rows = {SOURCE_ID: source, COPY_ID: copy_row}
    mgr.dao.get_with_relations.side_effect = lambda pipeline_id: rows.get(pipeline_id)
    mgr.dao.get_by_name.return_value = None

    async def _set_members(_pipeline_id, member_ids: list) -> None:
        copy_row.members = [_member(pipeline_id) for pipeline_id in member_ids]

    mgr.dao.set_members.side_effect = _set_members

    def _add(entity) -> None:
        entity.id = COPY_ID
        entity.created_at = copy_row.created_at
        entity.updated_at = copy_row.updated_at
        # Whatever create() writes onto the new row is what the copy must carry.
        for field in (
            "name",
            "threshold_pct",
            "correlation_window_seconds",
            "auto_add_new_pipelines",
            "auto_add_pipeline_name_pattern",
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
            setattr(copy_row, field, getattr(entity, field, None))

    mgr.db.add = MagicMock(side_effect=_add)
    return mgr


@pytest.fixture(autouse=True)
def _patched_side_channels(topic_service: AsyncMock):
    with (
        patch("apps.managers.correlation_pipelines.topic_lifecycle_service", topic_service),
        patch("apps.managers.correlation_pipelines.chains_config_producer", AsyncMock()),
        patch("apps.managers.correlation_pipelines.activity_producer", AsyncMock()),
    ):
        yield


class TestDuplicate:
    @pytest.mark.asyncio
    async def test_copy_is_stopped_with_the_copy_suffix(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.name == "tenant-a (Copy)"
        assert response.jobs_status == "stopped"
        # The source is untouched — including its running state.
        assert source.name == "tenant-a"
        assert source.jobs_status == "starting"

    @pytest.mark.asyncio
    async def test_copies_the_tuning_parameters(self, manager: CorrelationPipelinesManager) -> None:
        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.threshold_pct == 85
        assert response.correlation_window_seconds == 1800
        assert response.auto_add_new_pipelines is True
        assert response.auto_add_pipeline_name_pattern == "cust1-*"
        assert response.correlation_resources.taskmanager_memory_mb == 8192

    @pytest.mark.asyncio
    async def test_null_resources_stay_null_rather_than_freezing_todays_defaults(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        """A copy must keep inheriting the platform defaults, not snapshot them."""
        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.correlation_resources.parallelism is None
        assert response.agent_resources.taskmanager_memory_mb is None

    @pytest.mark.asyncio
    async def test_members_are_copied(self, manager: CorrelationPipelinesManager) -> None:
        """A detection pipeline may belong to several correlation jobs; the copy shares the source's members."""
        response = await manager.duplicate(SOURCE_ID, _user())

        assert [member.pipeline_id for member in response.members] == [MEMBER_ONE]
        assert manager.dao.set_members.await_args.args[1] == [MEMBER_ONE]
        assert response.warnings == []

    @pytest.mark.asyncio
    async def test_copy_gets_its_own_identity_and_topics(
        self, manager: CorrelationPipelinesManager, topic_service: AsyncMock
    ) -> None:
        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.id == COPY_ID
        assert response.correlation_deployment_name == f"corr-{COPY_ID}"
        assert response.agent_deployment_name == f"agent-{COPY_ID}"
        topic_service.ensure_correlation_pipeline_topics.assert_awaited_once_with(COPY_ID)

    @pytest.mark.asyncio
    async def test_never_calls_the_provider(self, manager: CorrelationPipelinesManager) -> None:
        provider = AsyncMock(spec=KubernetesFlinkProvider)

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.duplicate(SOURCE_ID, _user())

        provider.launch_correlation_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_suffix_escalates_on_collision(self, manager: CorrelationPipelinesManager, source: MagicMock) -> None:
        taken = {"tenant-a (Copy)", "tenant-a (Copy 2)"}
        manager.dao.get_by_name.side_effect = lambda name: _row(COPY_ID, name) if name in taken else None

        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.name == "tenant-a (Copy 3)"

    @pytest.mark.asyncio
    async def test_gives_up_after_fifty_attempts(self, manager: CorrelationPipelinesManager, source: MagicMock) -> None:
        manager.dao.get_by_name.side_effect = lambda name: _row(COPY_ID, name)

        with pytest.raises(ConflictError, match="free name"):
            await manager.duplicate(SOURCE_ID, _user())

    @pytest.mark.asyncio
    async def test_long_name_is_truncated_to_fit_the_column(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        source.name = "x" * 300

        response = await manager.duplicate(SOURCE_ID, _user())

        # Bit-identical to the shipped detection-pipeline rule: first 248 chars + " (Copy)".
        assert response.name == "x" * 248 + " (Copy)"
        assert len(response.name) == 255

    @pytest.mark.asyncio
    async def test_second_copy_of_a_long_name_reserves_room_for_the_longer_suffix(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        source.name = "x" * 300
        taken = {"x" * 248 + " (Copy)"}
        manager.dao.get_by_name.side_effect = lambda name: _row(COPY_ID, name) if name in taken else None

        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.name == "x" * 246 + " (Copy 2)"
        assert len(response.name) <= 255

    @pytest.mark.asyncio
    async def test_truncation_collision_falls_through_to_the_next_suffix(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        """Two distinct long sources sharing their first 248 chars both want the same copy name.

        Proves the uniqueness probe runs on the TRUNCATED candidate, not on the raw name.
        """
        source.name = "y" * 248 + "-first-source"
        taken = {"y" * 248 + " (Copy)"}
        manager.dao.get_by_name.side_effect = lambda name: _row(COPY_ID, name) if name in taken else None

        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.name == "y" * 246 + " (Copy 2)"

    @pytest.mark.asyncio
    async def test_truncation_on_whitespace_does_not_double_the_space(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        source.name = "z" * 247 + " " + "tail"

        response = await manager.duplicate(SOURCE_ID, _user())

        assert response.name == "z" * 247 + " (Copy)"

    @pytest.mark.asyncio
    async def test_logs_exactly_one_create_activity_naming_both_pipelines(
        self, manager: CorrelationPipelinesManager, source: MagicMock
    ) -> None:
        """One creation, one activity row — and ``create``, never a new ``duplicate`` action type."""
        with patch("apps.managers.correlation_pipelines.activity_producer") as producer:
            producer.log_action = AsyncMock()
            response = await manager.duplicate(SOURCE_ID, _user())

        producer.log_action.assert_awaited_once()
        logged = producer.log_action.await_args.kwargs
        assert logged["action"] == "create"
        assert logged["entity_type"] == "correlation_pipeline"
        assert logged["entity_id"] == str(response.id)
        assert logged["details"] == "Duplicated correlation pipeline 'tenant-a' as 'tenant-a (Copy)' (created stopped)"

    @pytest.mark.asyncio
    async def test_topic_provisioning_failure_rolls_back_and_names_the_orphans(
        self,
        manager: CorrelationPipelinesManager,
        source: MagicMock,
        topic_service: AsyncMock,
    ) -> None:
        """No ghost row, no activity event, source untouched — and the dead id's topics are named.

        The rolled-back id is discarded forever and a retry mints a new one, so anything already
        created under it is never re-adopted; ``chains-config`` is compacted with ``retention.ms=-1``
        and would linger, hence the operator-facing warning.
        """
        topic_service.ensure_correlation_pipeline_topics.side_effect = TopicProvisioningError("broker down")

        with (
            patch("apps.managers.correlation_pipelines.activity_producer") as producer,
            patch("apps.managers.correlation_pipelines.logger") as log,
        ):
            producer.log_action = AsyncMock()
            with pytest.raises(ServiceUnavailableError):
                await manager.duplicate(SOURCE_ID, _user())

        manager.db.rollback.assert_awaited()
        manager.db.commit.assert_not_awaited()
        producer.log_action.assert_not_awaited()
        assert source.name == "tenant-a"

        log.warning.assert_called_once()
        orphaned = log.warning.call_args.kwargs["extra"]["orphaned_topics"]
        assert len(orphaned) == 3
        assert all(str(COPY_ID) in topic for topic in orphaned)
