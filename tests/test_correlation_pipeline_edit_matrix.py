"""The edit-safety matrix: which fields are hot, which restart what, which never move.

One test per matrix row, plus the exhaustiveness guard that fails the build when a new column is
added to ``correlation_pipelines`` without being classified. The matrix is the most important part
of the manager: a restart is a ~30s correlation gap for a paying customer's tenant, and an operator
editing a memory value has no reason to expect one.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from sqlalchemy import inspect

from apps.core.exceptions import BadRequestError, ConfigurationError, ConflictError
from apps.core.models import CorrelationPipeline
from apps.core.schemas import CorrelationPipelineUpdateRequest
from apps.managers.correlation_pipelines import (
    AGENT_RESTART_FIELDS,
    CHAINS_CONFIG_HOT_FIELDS,
    CORRELATION_RESTART_FIELDS,
    FREE_FIELDS,
    IMMUTABLE_FIELDS,
    RELATION_FIELDS,
    CorrelationPipelinesManager,
)
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_ID = UUID("11111111-1111-4111-8111-111111111111")
MEMBER_ONE = UUID("aaaaaaaa-0000-4000-8000-000000000001")
MEMBER_TWO = UUID("aaaaaaaa-0000-4000-8000-000000000002")

CORR = f"corr-{PIPELINE_ID}"
AGENT = f"agent-{PIPELINE_ID}"

# Columns the manager owns rather than the operator: surrogate key, audit stamps.
_NON_SETTABLE_COLUMNS = {"id", "created_at", "updated_at", "updated_by"}


def _member(pipeline_id: UUID, name: str, *, destination_topic: str) -> MagicMock:
    member = MagicMock()
    member.id = pipeline_id
    member.name = name
    member.destination_topic = destination_topic
    member.tagged_events_topic = f"detectflow.tagged-events.{pipeline_id}"
    member.enabled = True
    return member


def _row(*, jobs_status: str = "starting", members: list[MagicMock] | None = None) -> MagicMock:
    row = MagicMock()
    row.id = PIPELINE_ID
    row.name = "tenant-a"
    row.jobs_status = jobs_status
    row.threshold_pct = 60
    row.correlation_window_seconds = 600
    row.auto_add_new_pipelines = False
    row.auto_add_pipeline_name_pattern = None
    row.correlation_deployment_name = CORR
    row.agent_deployment_name = AGENT
    row.members = members if members is not None else [_member(MEMBER_ONE, "P1", destination_topic="cust1-out")]
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


def _user() -> MagicMock:
    user = MagicMock()
    user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    return user


@pytest.fixture
def row() -> MagicMock:
    return _row()


@pytest.fixture
def provider() -> AsyncMock:
    provider = AsyncMock(spec=KubernetesFlinkProvider)
    provider.launch_correlation_job_deployment = AsyncMock(return_value={"operation": "updated"})
    provider.launch_agent_job_deployment = AsyncMock(return_value={"operation": "updated"})
    return provider


@pytest.fixture
def chains_config() -> AsyncMock:
    producer = AsyncMock()
    producer.build_envelope = MagicMock(return_value={"correlation_pipeline_id": str(PIPELINE_ID)})
    return producer


@pytest.fixture
def manager(row: MagicMock, chains_config: AsyncMock) -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(AsyncMock())
    # `AsyncSession.expire` is synchronous (like `add`); an AsyncMock would leave an
    # un-awaited coroutine behind every membership write.
    mgr.db.expire = MagicMock()
    mgr.dao = AsyncMock()
    mgr.dao.get_with_relations.return_value = row
    mgr.pipeline_dao = AsyncMock()
    mgr.pipeline_dao.get_by_ids.side_effect = lambda ids: [
        _member(pipeline_id, f"P{i}", destination_topic=f"cust-{i}") for i, pipeline_id in enumerate(ids)
    ]
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


async def _update(manager, provider, body, **kwargs):
    with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
        return await manager.update(PIPELINE_ID, body, _user(), **kwargs)


class TestExhaustiveness:
    def test_every_settable_column_is_classified_exactly_once(self) -> None:
        """A new column must be classified, or this fails the build.

        The point is not tidiness: an unclassified column silently falls into "applied to the DB and
        never propagated", which is how a customer's threshold quietly stops matching the running
        job.
        """
        sets = [
            CHAINS_CONFIG_HOT_FIELDS,
            CORRELATION_RESTART_FIELDS,
            AGENT_RESTART_FIELDS,
            IMMUTABLE_FIELDS,
            FREE_FIELDS,
        ]
        columns = {column.key for column in inspect(CorrelationPipeline).columns} - _NON_SETTABLE_COLUMNS
        union = set().union(*sets)

        assert union == columns | RELATION_FIELDS

        # No field may appear in two classes: the behaviour would be ambiguous.
        seen: set[str] = set()
        for field_set in sets:
            assert not (seen & field_set), f"field classified twice: {sorted(seen & field_set)}"
            seen |= field_set


class TestHotFields:
    @pytest.mark.asyncio
    async def test_threshold_publishes_and_never_calls_the_provider(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        response = await _update(manager, provider, CorrelationPipelineUpdateRequest(threshold_pct=75))

        chains_config.publish.assert_awaited_once()
        assert chains_config.publish.await_args.kwargs["topic"] == f"detectflow.chains-config.{PIPELINE_ID}"
        provider.launch_correlation_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()
        assert response.restarted == []

    @pytest.mark.asyncio
    async def test_auto_add_new_pipelines_is_hot(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        await _update(manager, provider, CorrelationPipelineUpdateRequest(auto_add_new_pipelines=True))

        chains_config.publish.assert_awaited_once()
        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_window_change_publishes_and_warns_about_rebucketing(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """§21: ``chain_id`` is seeded with ``window_start``, so moving the window mints new ids."""
        response = await _update(manager, provider, CorrelationPipelineUpdateRequest(correlation_window_seconds=1800))

        chains_config.publish.assert_awaited_once()
        provider.launch_correlation_job_deployment.assert_not_called()
        assert any("re-buckets chain_id" in warning for warning in response.warnings)

    @pytest.mark.asyncio
    async def test_explicit_null_window_clears_the_override(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock, row: MagicMock
    ) -> None:
        """``exclude_unset``, not ``exclude_none``: a null window means "inherit the rule timeframe",
        which is a real edit (§19) and must not be mistaken for an omitted field."""
        body = CorrelationPipelineUpdateRequest(name="tenant-a-renamed", correlation_window_seconds=None)

        await _update(manager, provider, body)

        assert row.correlation_window_seconds is None
        chains_config.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_partial_resource_update_does_not_wipe_its_siblings(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, row: MagicMock
    ) -> None:
        """Sending one resource field must not NULL the other four — that would silently hand the
        tenant back the platform defaults mid-flight."""
        row.correlation_taskmanager_memory_mb = 8192
        row.correlation_taskmanager_cpu = 3.0

        await _update(
            manager,
            provider,
            CorrelationPipelineUpdateRequest(correlation_resources={"parallelism": 4}),
            confirm_restart=True,
        )

        assert row.correlation_parallelism == 4
        assert row.correlation_taskmanager_memory_mb == 8192
        assert row.correlation_taskmanager_cpu == 3.0

    @pytest.mark.asyncio
    async def test_unchanged_value_publishes_nothing(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock, row: MagicMock
    ) -> None:
        """An idempotent PATCH must not put a new record on a compacted topic."""
        await _update(manager, provider, CorrelationPipelineUpdateRequest(threshold_pct=row.threshold_pct))

        chains_config.publish.assert_not_awaited()
        provider.launch_correlation_job_deployment.assert_not_called()


class TestFreeFields:
    @pytest.mark.asyncio
    async def test_name_is_db_only(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock, row: MagicMock
    ) -> None:
        await _update(manager, provider, CorrelationPipelineUpdateRequest(name="tenant-a-renamed"))

        assert row.name == "tenant-a-renamed"
        chains_config.publish.assert_not_awaited()
        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_add_pipeline_name_pattern_never_reaches_the_wire(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """It is evaluated control-plane-side at detection-pipeline creation, not by the matcher."""
        await _update(manager, provider, CorrelationPipelineUpdateRequest(auto_add_pipeline_name_pattern="cust1-*"))

        chains_config.publish.assert_not_awaited()
        provider.launch_correlation_job_deployment.assert_not_called()


class TestRestartFields:
    @pytest.mark.asyncio
    async def test_correlation_resource_change_without_confirm_conflicts(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        body = CorrelationPipelineUpdateRequest(correlation_resources={"taskmanager_memory_mb": 8192})

        with pytest.raises(ConflictError, match="confirm_restart=true"):
            await _update(manager, provider, body)

        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_stopped_pipeline_applies_restart_fields_silently(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, row: MagicMock
    ) -> None:
        """Nothing is interrupted when nothing is running; the spec is picked up at next start."""
        row.jobs_status = "stopped"
        body = CorrelationPipelineUpdateRequest(correlation_resources={"taskmanager_memory_mb": 8192})

        response = await _update(manager, provider, body)

        assert row.correlation_taskmanager_memory_mb == 8192
        assert response.restarted == []
        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_confirmed_correlation_change_restarts_correlation_only(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        body = CorrelationPipelineUpdateRequest(correlation_resources={"parallelism": 4})

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.launch_agent_job_deployment.assert_not_called()
        assert response.restarted == ["correlation"]

    @pytest.mark.asyncio
    async def test_confirmed_agent_change_restarts_agent_only(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        body = CorrelationPipelineUpdateRequest(agent_resources={"taskmanager_memory_mb": 6144})

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.launch_agent_job_deployment.assert_awaited_once()
        provider.launch_correlation_job_deployment.assert_not_called()
        assert response.restarted == ["agent"]

    @pytest.mark.asyncio
    async def test_both_sides_change_restarts_both(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        body = CorrelationPipelineUpdateRequest(
            correlation_resources={"parallelism": 2},
            agent_resources={"parallelism": 3},
        )

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.launch_agent_job_deployment.assert_awaited_once()
        assert sorted(response.restarted) == ["agent", "correlation"]


class TestMembershipIsBothHotAndRestart:
    @pytest.mark.asyncio
    async def test_membership_publishes_then_restarts_in_that_order(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """Publishing first is deliberate: the restarted job must read a config that already
        matches its new input set."""
        events: list[str] = []
        chains_config.publish = AsyncMock(side_effect=lambda envelope, *, topic: events.append("publish"))
        provider.launch_correlation_job_deployment = AsyncMock(
            side_effect=lambda config: events.append("restart") or {"operation": "updated"}
        )

        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE, MEMBER_TWO])
        response = await _update(manager, provider, body, confirm_restart=True)

        assert events == ["publish", "restart"]
        assert response.restarted == ["correlation"]
        # Membership never affects the agent job's spec.
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_membership_change_without_confirm_conflicts(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE, MEMBER_TWO])

        with pytest.raises(ConflictError, match="member_pipeline_ids"):
            await _update(manager, provider, body)

        # Refused means refused: nothing published either.
        chains_config.publish.assert_not_awaited()
        provider.launch_correlation_job_deployment.assert_not_called()


class TestRemovingAMemberIsAColdStart:
    """A removal cannot be applied by re-rendering the spec.

    A ``KafkaSource`` restores its assigned splits from the checkpoint, not from the rendered
    ``KAFKA_INPUT_TOPICS``, so a ``last-state`` roll leaves the departed member's partitions
    assigned and the job keeps consuming that customer's events — measured in ``detectflow-agent``.
    Only a delete + state wipe + launch applies it.
    """

    @pytest.mark.asyncio
    async def test_removal_deletes_wipes_and_relaunches(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        row.members = [
            _member(MEMBER_ONE, "P1", destination_topic="cust1-out"),
            _member(MEMBER_TWO, "P2", destination_topic="cust2-out"),
        ]
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE])

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.delete_correlation_job_deployment.assert_awaited_once_with(f"corr-{PIPELINE_ID}")
        provider.cleanup_correlation_job_storage.assert_awaited_once_with(f"corr-{PIPELINE_ID}")
        provider.launch_correlation_job_deployment.assert_awaited_once()
        assert response.restarted == ["correlation"]
        # The agent job reads this pipeline's own chains-raw topic; membership cannot change it,
        # and cold-starting it would re-pay the LLM or skip chains in flight.
        provider.delete_agent_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_removal_warns_that_in_flight_chains_were_lost(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        row.members = [
            _member(MEMBER_ONE, "P1", destination_topic="cust1-out"),
            _member(MEMBER_TWO, "P2", destination_topic="cust2-out"),
        ]
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE])

        response = await _update(manager, provider, body, confirm_restart=True)

        assert any("cold-starts" in warning for warning in response.warnings), response.warnings

    @pytest.mark.asyncio
    async def test_a_failed_state_wipe_says_the_topic_may_still_be_consumed(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        """The dangerous case: the job comes back, but possibly restored — say so."""
        row.members = [
            _member(MEMBER_ONE, "P1", destination_topic="cust1-out"),
            _member(MEMBER_TWO, "P2", destination_topic="cust2-out"),
        ]
        provider.cleanup_correlation_job_storage = AsyncMock(side_effect=RuntimeError("PVC busy"))
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE])

        response = await _update(manager, provider, body, confirm_restart=True)

        assert any("may have restored" in warning for warning in response.warnings), response.warnings
        provider.launch_correlation_job_deployment.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_adding_a_member_stays_a_cheap_roll(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """Additions are not affected: the new topic is discovered and the buffers survive."""
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE, MEMBER_TWO])

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.launch_correlation_job_deployment.assert_awaited_once()
        provider.delete_correlation_job_deployment.assert_not_called()
        provider.cleanup_correlation_job_storage.assert_not_called()
        assert response.warnings == []
        assert response.restarted == ["correlation"]

    @pytest.mark.asyncio
    async def test_a_stopped_pipeline_is_not_touched(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        """Nothing to cold-start: the next start reads the new member set anyway."""
        row.jobs_status = "stopped"
        row.members = [
            _member(MEMBER_ONE, "P1", destination_topic="cust1-out"),
            _member(MEMBER_TWO, "P2", destination_topic="cust2-out"),
        ]
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE])

        response = await _update(manager, provider, body)

        provider.delete_correlation_job_deployment.assert_not_called()
        provider.launch_correlation_job_deployment.assert_not_called()
        assert response.restarted == []

    @pytest.mark.asyncio
    async def test_the_conflict_names_the_cost_before_it_is_paid(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        """A removal costs more than the ~30s gap a plain restart costs; the 409 must say so."""
        row.members = [
            _member(MEMBER_ONE, "P1", destination_topic="cust1-out"),
            _member(MEMBER_TWO, "P2", destination_topic="cust2-out"),
        ]
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_ONE])

        with pytest.raises(ConflictError, match="COLD-STARTS"):
            await _update(manager, provider, body)

        provider.delete_correlation_job_deployment.assert_not_called()


class TestUnstartablePipelinesAreRefused:
    """No member owns a tagged-events topic ⇒ no input topics ⇒ do not start, do not fall back.

    The fallback this replaces rendered ``settings.kafka_tagged_events_topic``, the platform-wide
    topic every customer's match node writes to. Only the ``Default`` migration and hand-written SQL
    can create such a member — ``add_member`` refuses one — but the failure was silent cross-tenant
    correlation, so it is refused loudly instead.
    """

    @pytest.mark.asyncio
    async def test_a_mixed_member_set_renders_only_the_assigned_topics(self, row: MagicMock) -> None:
        from apps.managers.correlation_pipelines import deployment_config_for_row

        assigned = _member(MEMBER_ONE, "P1", destination_topic="cust1-out")
        unassigned_a = _member(MEMBER_TWO, "P2", destination_topic="cust2-out")
        unassigned_a.tagged_events_topic = None
        unassigned_b = _member(UUID("aaaaaaaa-0000-4000-8000-000000000003"), "P3", destination_topic="cust3-out")
        unassigned_b.tagged_events_topic = None
        row.members = [assigned, unassigned_a, unassigned_b]

        assert deployment_config_for_row(row).input_topics == [assigned.tagged_events_topic]

    @pytest.mark.asyncio
    async def test_start_refuses_and_submits_nothing_when_no_member_has_a_topic(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        unassigned = _member(MEMBER_ONE, "P1", destination_topic="cust1-out")
        unassigned.tagged_events_topic = None
        row.members = [unassigned]
        row.jobs_status = "stopped"

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider),
            pytest.raises(ConfigurationError, match="P1"),
        ):
            await manager.start(PIPELINE_ID)

        provider.launch_correlation_job_deployment.assert_not_called()
        provider.launch_agent_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_of_a_memberless_pipeline_says_so(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        row.members = []
        row.jobs_status = "stopped"

        with (
            patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider),
            pytest.raises(ConfigurationError, match="no members"),
        ):
            await manager.start(PIPELINE_ID)

        provider.launch_correlation_job_deployment.assert_not_called()

    @pytest.mark.asyncio
    async def test_removing_the_last_member_tears_down_without_relaunching(
        self, manager: CorrelationPipelinesManager, row: MagicMock, provider: AsyncMock
    ) -> None:
        """The cold start still runs — that is what stops the departed member's events — but there
        is nothing left to launch, so the job stays down and the response says why."""
        # The manager re-reads the row after writing membership; the mocked DAO has to model that
        # second read returning the emptied member set (a real session reloads it — the collection
        # is expired at the raw write).
        manager.dao.get_with_relations.side_effect = [row, _row(members=[])]
        body = CorrelationPipelineUpdateRequest(pipeline_ids=[])

        response = await _update(manager, provider, body, confirm_restart=True)

        provider.delete_correlation_job_deployment.assert_awaited_once()
        provider.cleanup_correlation_job_storage.assert_awaited_once()
        provider.launch_correlation_job_deployment.assert_not_called()
        assert any("left DOWN" in warning for warning in response.warnings), response.warnings


class TestImmutableFields:
    @pytest.mark.asyncio
    async def test_deployment_name_is_rejected_if_it_ever_reaches_the_manager(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """``extra="forbid"`` gives a 422 at the schema; this is the belt-and-braces layer."""
        body = CorrelationPipelineUpdateRequest(name="whatever")
        object.__setattr__(body, "__pydantic_fields_set__", {"name"})
        manager._flatten_update = staticmethod(lambda _body: {"correlation_deployment_name": "corr-hijacked"})

        with pytest.raises(BadRequestError, match="correlation_deployment_name is immutable"):
            await _update(manager, provider, body)

    @pytest.mark.asyncio
    async def test_jobs_status_is_immutable(self, manager: CorrelationPipelinesManager, provider: AsyncMock) -> None:
        body = CorrelationPipelineUpdateRequest(name="whatever")
        manager._flatten_update = staticmethod(lambda _body: {"jobs_status": "starting"})

        with pytest.raises(BadRequestError, match="jobs_status is immutable"):
            await _update(manager, provider, body)
