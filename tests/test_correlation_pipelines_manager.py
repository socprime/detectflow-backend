"""CRUD for correlation pipelines: name/topic allocation, provisioning rollback, per-row publish.

The load-bearing invariants here are the ones that make two tenants actually independent — disjoint
deployment names, disjoint topics, and a chains-config envelope that names its own pipeline — plus
the one that keeps "Create" honest: it must never touch Kubernetes.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.exceptions import DuplicateEntityError, NotFoundError, ServiceUnavailableError
from apps.core.schemas import CorrelationPipelineCreateRequest
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.modules.kafka.chains import KafkaChainsConfigProducer
from apps.modules.kafka.topic_lifecycle import TopicProvisioningError, TopicRetirementOutcome
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")
MEMBER_ONE = UUID("aaaaaaaa-0000-4000-8000-000000000001")


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


def _member(pipeline_id: UUID, name: str, *, destination_topic: str) -> MagicMock:
    member = MagicMock()
    member.id = pipeline_id
    member.name = name
    member.destination_topic = destination_topic
    member.tagged_events_topic = f"detectflow.tagged-events.{pipeline_id}"
    member.enabled = True
    return member


def _row(pipeline_id: UUID, name: str, *, members: list[MagicMock] | None = None) -> MagicMock:
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
    row.members = members or []
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


@pytest.fixture
def topic_service() -> AsyncMock:
    service = AsyncMock()
    service.ensure_correlation_pipeline_topics.return_value = [
        f"detectflow.chains-config.{PIPELINE_A}",
        f"detectflow.chains-raw.{PIPELINE_A}",
        f"detectflow.chains-analysed.{PIPELINE_A}",
    ]
    service.retire_correlation_pipeline_topics.return_value = []
    return service


@pytest.fixture
def chains_config() -> AsyncMock:
    producer = AsyncMock()
    # Keep the REAL envelope builder — the wire shape is the contract under test.
    producer.build_envelope = KafkaChainsConfigProducer.build_envelope.__get__(producer, KafkaChainsConfigProducer)
    return producer


@pytest.fixture
def provider() -> AsyncMock:
    return AsyncMock(spec=KubernetesFlinkProvider)


@pytest.fixture
def manager(topic_service: AsyncMock, chains_config: AsyncMock) -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(AsyncMock())
    # `AsyncSession.expire` is synchronous (like `add`); an AsyncMock would leave an
    # un-awaited coroutine behind every membership write.
    mgr.db.expire = MagicMock()
    mgr.dao = AsyncMock()
    mgr.dao.get_by_name.return_value = None
    mgr.pipeline_dao = AsyncMock()
    mgr.pipeline_dao.get_by_ids.return_value = []
    mgr.audit_dao = AsyncMock()
    # §20 deny-list lookup: empty means "monitors every non-blocked threat", the opt-out default.
    mgr.active_threats_dao = AsyncMock()
    mgr.active_threats_dao.get_excluded_threat_ids_by_pipeline_ids.return_value = {}
    return mgr


@pytest.fixture(autouse=True)
def _patched_side_channels(topic_service: AsyncMock, chains_config: AsyncMock):
    with (
        patch("apps.managers.correlation_pipelines.topic_lifecycle_service", topic_service),
        patch("apps.managers.correlation_pipelines.chains_config_producer", chains_config),
        patch("apps.managers.correlation_pipelines.activity_producer", AsyncMock()),
    ):
        yield


def _wire_created_row(manager: CorrelationPipelinesManager, row: MagicMock) -> None:
    """``db.add`` + ``flush`` assign the UUID in production; emulate that for the mock session."""

    def _add(entity) -> None:
        entity.id = row.id
        entity.created_at = row.created_at
        entity.updated_at = row.updated_at
        entity.jobs_status = "stopped"

    # ``Session.add`` is synchronous: an AsyncMock would never run the side effect.
    manager.db.add = MagicMock(side_effect=_add)
    manager.dao.get_with_relations.return_value = row


class TestCreate:
    @pytest.mark.asyncio
    async def test_allocates_names_provisions_topics_and_publishes_initial_config(
        self,
        manager: CorrelationPipelinesManager,
        topic_service: AsyncMock,
        chains_config: AsyncMock,
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        _wire_created_row(manager, row)

        response = await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

        assert response.correlation_deployment_name == f"corr-{PIPELINE_A}"
        assert response.agent_deployment_name == f"agent-{PIPELINE_A}"
        topic_service.ensure_correlation_pipeline_topics.assert_awaited_once_with(PIPELINE_A)
        # The compacted topic must carry a value before correlation-job's broadcast source subscribes.
        chains_config.publish.assert_awaited_once()
        assert chains_config.publish.await_args.kwargs["topic"] == f"detectflow.chains-config.{PIPELINE_A}"

    @pytest.mark.asyncio
    async def test_never_calls_the_provider(self, manager: CorrelationPipelinesManager, provider: AsyncMock) -> None:
        """ "Create & Run" is two calls. The plain button must leave Kubernetes untouched."""
        _wire_created_row(manager, _row(PIPELINE_A, "tenant-a"))

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            response = await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

        assert response.jobs_status == "stopped"
        provider.launch_correlation_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_duplicate_name_is_rejected(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_by_name.return_value = _row(PIPELINE_B, "tenant-a")

        with pytest.raises(DuplicateEntityError):
            await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

    @pytest.mark.asyncio
    async def test_unknown_member_is_not_found(self, manager: CorrelationPipelinesManager) -> None:
        _wire_created_row(manager, _row(PIPELINE_A, "tenant-a"))
        manager.pipeline_dao.get_by_ids.return_value = []  # requested id does not exist

        with pytest.raises(NotFoundError):
            await manager.create(CorrelationPipelineCreateRequest(name="tenant-a", pipeline_ids=[MEMBER_ONE]), _user())

    @pytest.mark.asyncio
    async def test_topic_provisioning_failure_rolls_back_and_503s(
        self, manager: CorrelationPipelinesManager, topic_service: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """A pipeline whose topics do not exist is unstartable — better no row than a broken one."""
        _wire_created_row(manager, _row(PIPELINE_A, "tenant-a"))
        topic_service.ensure_correlation_pipeline_topics.side_effect = TopicProvisioningError("broker down")

        with pytest.raises(ServiceUnavailableError):
            await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

        manager.db.rollback.assert_awaited()
        manager.db.commit.assert_not_awaited()
        chains_config.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_null_resources_stay_null_so_they_inherit_the_platform_defaults(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        _wire_created_row(manager, row)

        response = await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

        assert response.correlation_resources.taskmanager_memory_mb is None
        assert response.agent_resources.parallelism is None

    @pytest.mark.asyncio
    async def test_seeds_no_exclusion_rows(self, manager: CorrelationPipelinesManager) -> None:
        """§20 is opt-OUT: a new pipeline inherits every non-blocked threat by excluding nothing."""
        _wire_created_row(manager, _row(PIPELINE_A, "tenant-a"))

        await manager.create(CorrelationPipelineCreateRequest(name="tenant-a"), _user())

        assert await manager._excluded_active_threat_ids([PIPELINE_A]) == {}


class TestTwoTenantsAreDisjoint:
    def test_deployment_names_and_topics_never_collide(self) -> None:
        from apps.managers.correlation_pipelines import deployment_config_for_row

        config_a = deployment_config_for_row(_row(PIPELINE_A, "tenant-a"))
        config_b = deployment_config_for_row(_row(PIPELINE_B, "tenant-b"))

        assert config_a.correlation_deployment_name != config_b.correlation_deployment_name
        assert config_a.agent_deployment_name != config_b.agent_deployment_name
        assert {config_a.chains_config_topic, config_a.chains_raw_topic, config_a.chains_analysed_topic}.isdisjoint(
            {config_b.chains_config_topic, config_b.chains_raw_topic, config_b.chains_analysed_topic}
        )

    def test_deployment_and_cleanup_job_names_are_dns_1123(self) -> None:
        """``cleanup-agent-<uuid>`` is the longest derived name at 50 chars — inside the 63 budget."""
        import re

        from apps.core.constants import agent_deployment_name, correlation_deployment_name

        label = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
        names = [
            correlation_deployment_name(PIPELINE_A),
            agent_deployment_name(PIPELINE_A),
            f"cleanup-{correlation_deployment_name(PIPELINE_A)}",
            f"cleanup-{agent_deployment_name(PIPELINE_A)}",
        ]
        for name in names:
            assert label.match(name), name
            assert len(name) <= 63, f"{name} is {len(name)} chars"


class TestChainsConfigEnvelope:
    def test_carries_pipeline_identity_and_the_deny_list(self) -> None:
        row = _row(PIPELINE_A, "tenant-a", members=[_member(MEMBER_ONE, "P1", destination_topic="cust1-out")])
        producer = KafkaChainsConfigProducer.__new__(KafkaChainsConfigProducer)

        envelope = producer.build_envelope(
            row,
            excluded_active_threat_ids=["AT-1"],
        )

        assert envelope["correlation_pipeline_id"] == str(PIPELINE_A)
        assert envelope["correlation_pipeline_name"] == "tenant-a"
        assert "monitored_topics" not in envelope
        assert "auto_add_new_pipelines" not in envelope
        assert envelope["excluded_active_threat_ids"] == ["AT-1"]
        # The deny-list is the ONLY threat channel here; the global flag rides active-threats (§20).
        assert "monitored_active_threat_ids" not in envelope

    def test_destination_topics_are_not_on_the_envelope(self) -> None:
        """Isolation is KAFKA_INPUT_TOPICS; destination names are SIEM output only."""
        row = _row(PIPELINE_A, "tenant-a", members=[])
        producer = KafkaChainsConfigProducer.__new__(KafkaChainsConfigProducer)

        envelope = producer.build_envelope(row, excluded_active_threat_ids=[])

        assert "monitored_topics" not in envelope

    @pytest.mark.asyncio
    async def test_publish_targets_the_rows_topic_with_the_global_key(self) -> None:
        """Isolation is the TOPIC; the key stays "global" because PyFlink cannot read record keys."""
        producer = KafkaChainsConfigProducer.__new__(KafkaChainsConfigProducer)
        producer.producer = MagicMock()
        producer.producer.flush.return_value = 0

        producer._publish_sync({"correlation_pipeline_id": str(PIPELINE_A)}, f"detectflow.chains-config.{PIPELINE_A}")

        args, kwargs = producer.producer.produce.call_args
        assert args[0] == f"detectflow.chains-config.{PIPELINE_A}"
        assert kwargs["key"] == b"global"


class TestDelete:
    @pytest.mark.asyncio
    async def test_stops_then_retires_topics_and_drops_the_row(
        self, manager: CorrelationPipelinesManager, topic_service: AsyncMock
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        manager.dao.get_with_relations.return_value = row
        manager.stop = AsyncMock()

        warnings = await manager.delete(PIPELINE_A, _user())

        manager.stop.assert_awaited_once()
        topic_service.retire_correlation_pipeline_topics.assert_awaited_once_with(PIPELINE_A)
        manager.db.delete.assert_awaited_once_with(row)
        assert warnings == []

    @pytest.mark.asyncio
    async def test_surfaces_a_policy_downgrade_as_a_warning(
        self, manager: CorrelationPipelinesManager, topic_service: AsyncMock
    ) -> None:
        """A ``delete`` that the service downgraded to ``retire`` must not look like a clean delete."""
        manager.dao.get_with_relations.return_value = _row(PIPELINE_A, "tenant-a")
        manager.stop = AsyncMock()
        topic_service.retire_correlation_pipeline_topics.return_value = [
            TopicRetirementOutcome(
                topic=f"detectflow.chains-analysed.{PIPELINE_A}",
                requested_policy="delete",
                action="retire",
                reason="consumer group admin-panel-chains-analysed still has lag",
            )
        ]

        warnings = await manager.delete(PIPELINE_A, _user())

        assert len(warnings) == 1
        assert "downgraded to retire" in warnings[0]

    @pytest.mark.asyncio
    async def test_topic_failure_never_blocks_the_delete(
        self, manager: CorrelationPipelinesManager, topic_service: AsyncMock
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        manager.dao.get_with_relations.return_value = row
        manager.stop = AsyncMock()
        topic_service.retire_correlation_pipeline_topics.side_effect = TopicProvisioningError("broker down")

        warnings = await manager.delete(PIPELINE_A, _user())

        manager.db.delete.assert_awaited_once_with(row)
        assert any("not retired" in warning for warning in warnings)

    @pytest.mark.asyncio
    async def test_never_touches_a_kafka_admin_client(self, manager: CorrelationPipelinesManager) -> None:
        """Constraint 9: every topic call goes through the lifecycle service."""
        import inspect as inspect_module

        from apps.managers import correlation_pipelines

        source = inspect_module.getsource(correlation_pipelines)
        for forbidden in ("AdminClient", "delete_topics", "create_topics"):
            assert forbidden not in source


class TestListItem:
    def test_destination_topics_are_unique_sorted_and_include_extra_destinations(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        """List rows flatten every member destination, not only ``destination_topic`` (destinations[0])."""
        multi = _member(MEMBER_ONE, "P1", destination_topic="b-out")
        multi.destinations = [
            {"topic": "b-out", "output_mode": "matched_only", "apply_parser_to_output_events": False},
            {"topic": "z-out", "output_mode": "matched_only", "apply_parser_to_output_events": False},
        ]
        other = _member(UUID("bbbbbbbb-0000-4000-8000-000000000002"), "P2", destination_topic="a-out")
        other.destinations = None
        row = _row(PIPELINE_A, "tenant-a", members=[multi, other])

        item = manager._to_list_item(row, 2)

        assert item.destination_topics == ["a-out", "b-out", "z-out"]


_LIST_PAGINATION = {
    "limit": 25,
    "offset": 0,
    "sort": "name",
    "order": "asc",
    "search": None,
    "skip": 0,
}


class TestListPipelinesRuntime:
    """List fills ``runtime_state`` from the 15s cache; stopped rows skip Kubernetes."""

    @pytest.mark.asyncio
    async def test_stopped_row_skips_kubernetes_and_reports_overall_stopped(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        manager.dao.list_with_chain_counts.return_value = ([(row, 0)], 1)

        with patch("apps.managers.correlation_pipelines.read_runtime_k8s_state", new_callable=AsyncMock) as k8s:
            result = await manager.list_pipelines(_LIST_PAGINATION)

        k8s.assert_not_awaited()
        assert result.data[0].runtime_state is not None
        assert result.data[0].runtime_state.overall == "stopped"
        assert result.data[0].runtime_state.correlation_job.status == "missing"
        assert result.data[0].runtime_state.agent_job.status == "missing"

    @pytest.mark.asyncio
    async def test_starting_row_observes_kubernetes_then_cache_hit(self, manager: CorrelationPipelinesManager) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        row.jobs_status = "starting"
        manager.dao.list_with_chain_counts.return_value = ([(row, 0)], 1)
        ts = datetime(2026, 1, 1, tzinfo=UTC)

        with patch("apps.managers.correlation_pipelines.read_runtime_k8s_state", new_callable=AsyncMock) as k8s:
            k8s.return_value = (("running", ts), ("running", ts))
            first = await manager.list_pipelines(_LIST_PAGINATION)
            second = await manager.list_pipelines(_LIST_PAGINATION)

        assert k8s.await_count == 1
        assert first.data[0].runtime_state is not None
        assert first.data[0].runtime_state.overall == "running"
        assert second.data[0].runtime_state is not None
        assert second.data[0].runtime_state.overall == "running"

    @pytest.mark.asyncio
    async def test_commits_before_kubernetes(self, manager: CorrelationPipelinesManager) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        row.jobs_status = "starting"
        manager.dao.list_with_chain_counts.return_value = ([(row, 0)], 1)
        events: list[str] = []

        async def _commit() -> None:
            events.append("commit")

        async def _k8s(*_args, **_kwargs):
            events.append("k8s")
            return (("missing", None), ("missing", None))

        manager.db.commit = _commit
        with patch("apps.managers.correlation_pipelines.read_runtime_k8s_state", side_effect=_k8s):
            await manager.list_pipelines(_LIST_PAGINATION)

        assert events == ["commit", "k8s"]

    @pytest.mark.asyncio
    async def test_runtime_snapshot_write_through_warms_the_list(self, manager: CorrelationPipelinesManager) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        row.jobs_status = "starting"
        manager.dao.list_with_members.return_value = [row]
        manager.dao.list_with_chain_counts.return_value = ([(row, 0)], 1)
        manager.audit_dao.recent_health.return_value = {}
        ts = datetime(2026, 1, 1, tzinfo=UTC)

        with patch("apps.managers.correlation_pipelines.read_runtime_k8s_state", new_callable=AsyncMock) as k8s:
            k8s.return_value = (("running", ts), ("running", ts))
            await manager.list_runtime_snapshots([PIPELINE_A])
            result = await manager.list_pipelines(_LIST_PAGINATION)

        assert k8s.await_count == 1
        assert result.data[0].runtime_state is not None
        assert result.data[0].runtime_state.overall == "running"

    @pytest.mark.asyncio
    async def test_one_failed_half_is_failed_on_the_list_not_partial(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        row = _row(PIPELINE_A, "tenant-a")
        row.jobs_status = "starting"
        manager.dao.list_with_chain_counts.return_value = ([(row, 0)], 1)
        ts = datetime(2026, 1, 1, tzinfo=UTC)

        with patch("apps.managers.correlation_pipelines.read_runtime_k8s_state", new_callable=AsyncMock) as k8s:
            k8s.return_value = (("running", ts), ("failed", None))
            result = await manager.list_pipelines(_LIST_PAGINATION)

        state = result.data[0].runtime_state
        assert state is not None
        assert state.overall == "failed"
        assert state.correlation_job.status == "running"
        assert state.agent_job.status == "failed"


class TestDefaultPipelineResolution:
    """``get_default_pipeline_id`` — what the deprecated singleton endpoints are bound to."""

    @pytest.mark.asyncio
    async def test_resolves_the_default_row_by_name(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_by_name.side_effect = lambda name: _row(PIPELINE_A, name) if name == "Default" else None

        assert await manager.get_default_pipeline_id() == PIPELINE_A
        manager.dao.get_by_name.assert_awaited_with("Default")

    @pytest.mark.asyncio
    async def test_a_second_tenant_does_not_make_it_ambiguous(self, manager: CorrelationPipelinesManager) -> None:
        """By name, never "the only row" — otherwise a second customer 409s every legacy call."""
        rows = {"Default": _row(PIPELINE_A, "Default"), "Customer B": _row(PIPELINE_B, "Customer B")}
        manager.dao.get_by_name.side_effect = lambda name: rows.get(name)

        assert await manager.get_default_pipeline_id() == PIPELINE_A

    @pytest.mark.asyncio
    async def test_tolerates_the_migration_collision_name(self, manager: CorrelationPipelinesManager) -> None:
        """The migration renames its row when an operator already created a ``Default`` by hand."""
        rows = {"Default (migrated singleton)": _row(PIPELINE_B, "Default (migrated singleton)")}
        manager.dao.get_by_name.side_effect = lambda name: rows.get(name)

        assert await manager.get_default_pipeline_id() == PIPELINE_B

    @pytest.mark.asyncio
    async def test_a_hand_made_default_wins_over_the_collision_fallback(
        self, manager: CorrelationPipelinesManager
    ) -> None:
        """Both names present is the anomaly the runbook's step 1.2 asks an operator to resolve.

        Until they do, the canonical name is the answer: it is the one the frontend, the runbook
        and every other by-name lookup mean by "Default".
        """
        rows = {
            "Default": _row(PIPELINE_A, "Default"),
            "Default (migrated singleton)": _row(PIPELINE_B, "Default (migrated singleton)"),
        }
        manager.dao.get_by_name.side_effect = lambda name: rows.get(name)

        assert await manager.get_default_pipeline_id() == PIPELINE_A

    @pytest.mark.asyncio
    async def test_missing_row_names_the_replacement_api(self, manager: CorrelationPipelinesManager) -> None:
        manager.dao.get_by_name.return_value = None

        with pytest.raises(NotFoundError, match="/api/v1/correlation_pipelines"):
            await manager.get_default_pipeline_id()

    @pytest.mark.asyncio
    async def test_nothing_is_cached_between_calls(self, manager: CorrelationPipelinesManager) -> None:
        """A renamed or deleted row must be observed immediately, not from a stale id."""
        manager.dao.get_by_name.side_effect = lambda name: _row(PIPELINE_A, name) if name == "Default" else None
        await manager.get_default_pipeline_id()

        manager.dao.get_by_name.side_effect = None
        manager.dao.get_by_name.return_value = None

        with pytest.raises(NotFoundError):
            await manager.get_default_pipeline_id()
