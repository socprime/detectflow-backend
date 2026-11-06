"""A membership change must publish, restart and answer with the NEW member set.

``CorrelationPipelineDAO.set_members`` replaces the association table with **core**
``DELETE``/``INSERT`` statements, which the ORM never sees. The session is ``expire_on_commit=False``
(``apps/core/database.py``) and a second ``selectinload`` does not overwrite an already-loaded
collection — the identity map returns the same instance as it stands. Without an explicit expire the
manager's re-read hands back pre-edit ``members``, and the two things it derives from them are one
edit behind: the deployment's ``KAFKA_INPUT_TOPICS`` and the response body. On a *removal* the job
restarts still subscribed to the departed member's topic — the cross-tenant leak §17 exists to prevent.

The mocked-DAO tests elsewhere cannot express this: their "re-read" returns whatever the mock was
told to return. The fakes below model the two behaviours that produce it — the identity map, and a
loaded collection surviving a re-SELECT until expired — and ``TestTheHarness`` pins that they still
can, so the regression tests cannot go vacuous. Both were verified against a real SQLAlchemy session
over a SQLite mirror of the mapping (``lazy="noload"`` m2m via a secondary table,
``expire_on_commit=False``): re-SELECT alone returned the old member, ``session.expire(row,
["members"])`` after the raw write returned the new one.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.schemas import CorrelationPipelineUpdateRequest
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.modules.kafka.chains import KafkaChainsConfigProducer
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
MEMBER_OLD = UUID("aaaaaaaa-0000-4000-8000-000000000001")
MEMBER_NEW = UUID("bbbbbbbb-0000-4000-8000-000000000002")


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


MEMBERS = {
    MEMBER_OLD: _member(MEMBER_OLD, "P-old", destination_topic="cust-old-out"),
    MEMBER_NEW: _member(MEMBER_NEW, "P-new", destination_topic="cust-new-out"),
}


def _row(members: list[MagicMock]) -> MagicMock:
    row = MagicMock()
    row.id = PIPELINE_A
    row.name = "tenant-a"
    # "starting" is the DB's desired state for "both jobs up" — it is what makes a membership
    # edit restart the correlation job instead of only re-rendering at the next start.
    row.jobs_status = "starting"
    row.threshold_pct = 60
    row.correlation_window_seconds = None
    row.auto_add_new_pipelines = False
    row.auto_add_pipeline_name_pattern = None
    row.correlation_deployment_name = f"corr-{PIPELINE_A}"
    row.agent_deployment_name = f"agent-{PIPELINE_A}"
    row.members = members
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


class _FakeSession:
    """The two ``AsyncSession`` behaviours that produce this bug, and nothing else.

    ``expire_on_commit=False`` means a commit leaves loaded state alone, so only an explicit
    ``expire`` marks an attribute for reload. ``expire`` is a plain ``def`` on purpose — it is
    synchronous on ``AsyncSession``, like ``add``, and a test whose fake made it awaitable would
    hide a production ``TypeError``.
    """

    def __init__(self) -> None:
        self._expired: set[tuple[int, str]] = set()
        self.commits = 0

    def expire(self, instance: Any, attribute_names: list[str] | None = None) -> None:
        for name in attribute_names or ["members"]:
            self._expired.add((id(instance), name))

    def is_expired(self, instance: Any, name: str) -> bool:
        return (id(instance), name) in self._expired

    def mark_loaded(self, instance: Any, name: str) -> None:
        self._expired.discard((id(instance), name))

    def add(self, instance: Any) -> None: ...

    async def flush(self) -> None: ...

    async def commit(self) -> None:
        self.commits += 1  # expire_on_commit=False: nothing is invalidated here

    async def rollback(self) -> None: ...

    async def refresh(self, instance: Any) -> None: ...


class _FakeCorrelationPipelineDAO:
    """``CorrelationPipelineDAO`` over an in-memory association table, with the identity map.

    ``get_with_relations`` always returns the SAME instance, and re-populates ``members`` only when
    the collection has been expired — a ``selectinload`` never overwrites one that is already
    loaded. ``set_members`` writes the association table without touching ORM state, exactly as the
    core ``DELETE``/``INSERT`` does.
    """

    def __init__(self, session: _FakeSession, row: MagicMock, member_ids: list[UUID]) -> None:
        self.session = session
        self.row = row
        self.membership: dict[UUID, list[UUID]] = {row.id: list(member_ids)}

    async def get_with_relations(self, pipeline_id: UUID) -> MagicMock | None:
        if pipeline_id != self.row.id:
            return None
        if self.session.is_expired(self.row, "members"):
            self.row.members = [MEMBERS[member_id] for member_id in self.membership[pipeline_id]]
            self.session.mark_loaded(self.row, "members")
        return self.row

    async def set_members(self, correlation_pipeline_id: UUID, pipeline_ids: list[UUID]) -> None:
        self.membership[correlation_pipeline_id] = list(dict.fromkeys(pipeline_ids))


@pytest.fixture
def chains_config() -> AsyncMock:
    producer = AsyncMock()
    # Keep the REAL envelope builder — identity + deny-list on the wire is what is under test.
    producer.build_envelope = KafkaChainsConfigProducer.build_envelope.__get__(producer, KafkaChainsConfigProducer)
    return producer


@pytest.fixture
def provider() -> AsyncMock:
    return AsyncMock(spec=KubernetesFlinkProvider)


@pytest.fixture
def session() -> _FakeSession:
    return _FakeSession()


@pytest.fixture
def manager(session: _FakeSession, chains_config: AsyncMock) -> CorrelationPipelinesManager:
    mgr = CorrelationPipelinesManager(session)
    mgr.dao = _FakeCorrelationPipelineDAO(session, _row([MEMBERS[MEMBER_OLD]]), [MEMBER_OLD])
    mgr.pipeline_dao = AsyncMock()
    mgr.pipeline_dao.get_by_ids.return_value = [MEMBERS[MEMBER_NEW]]
    mgr.audit_dao = AsyncMock()
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


async def _swap_the_member(manager: CorrelationPipelinesManager, provider: AsyncMock):
    """PATCH the membership from ``MEMBER_OLD`` to ``MEMBER_NEW`` — an add AND a removal."""
    with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
        return await manager.update(
            PIPELINE_A,
            CorrelationPipelineUpdateRequest(pipeline_ids=[MEMBER_NEW]),
            _user(),
            confirm_restart=True,
        )


class TestMembershipPatchUsesTheNewMemberSet:
    @pytest.mark.asyncio
    async def test_chains_config_carries_pipeline_identity(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """Hot envelope after a membership swap: identity + deny-list, no destination topics."""
        await _swap_the_member(manager, provider)

        envelope = chains_config.publish.await_args.args[0]
        assert envelope["correlation_pipeline_id"] == str(PIPELINE_A)
        assert "monitored_topics" not in envelope

    @pytest.mark.asyncio
    async def test_the_restart_renders_the_new_input_topics(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        """``KAFKA_INPUT_TOPICS``. Stale means the ~30s gap is paid to resubscribe to the OLD set —
        and on a removal the job keeps reading another tenant's topic (§17)."""
        await _swap_the_member(manager, provider)

        config = provider.launch_correlation_job_deployment.await_args.args[0]
        assert config.input_topics == [f"detectflow.tagged-events.{MEMBER_NEW}"]

    @pytest.mark.asyncio
    async def test_the_response_echoes_the_new_members(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock
    ) -> None:
        response = await _swap_the_member(manager, provider)

        assert [member.pipeline_id for member in response.members] == [MEMBER_NEW]

    @pytest.mark.asyncio
    async def test_add_member_publishes_and_restarts_with_the_added_member(
        self, manager: CorrelationPipelinesManager, provider: AsyncMock, chains_config: AsyncMock
    ) -> None:
        """The same write→derive sequence, reached from the detection-pipeline side."""
        manager.pipeline_dao.get_by_id.return_value = MEMBERS[MEMBER_NEW]

        with patch("apps.managers.correlation_pipelines.get_flink_provider", return_value=provider):
            await manager.add_member(PIPELINE_A, MEMBER_NEW, _user())

        assert "monitored_topics" not in chains_config.publish.await_args.args[0]
        config = provider.launch_correlation_job_deployment.await_args.args[0]
        assert sorted(config.input_topics) == [
            f"detectflow.tagged-events.{MEMBER_OLD}",
            f"detectflow.tagged-events.{MEMBER_NEW}",
        ]


class TestTheHarness:
    """Without these, the tests above could pass on a fake that simply always reloads."""

    @pytest.mark.asyncio
    async def test_a_re_select_alone_does_not_refresh_a_loaded_collection(
        self, manager: CorrelationPipelinesManager, session: _FakeSession
    ) -> None:
        row = await manager.dao.get_with_relations(PIPELINE_A)
        await manager.dao.set_members(PIPELINE_A, [MEMBER_NEW])  # raw DELETE/INSERT
        await session.commit()  # expire_on_commit=False

        reread = await manager.dao.get_with_relations(PIPELINE_A)

        assert reread is row, "the identity map must return the same instance"
        assert [member.id for member in reread.members] == [MEMBER_OLD], "stale, as SQLAlchemy really behaves"

    @pytest.mark.asyncio
    async def test_expiring_the_collection_is_what_refreshes_it(
        self, manager: CorrelationPipelinesManager, session: _FakeSession
    ) -> None:
        row = await manager.dao.get_with_relations(PIPELINE_A)
        await manager.dao.set_members(PIPELINE_A, [MEMBER_NEW])
        session.expire(row, ["members"])
        await session.commit()

        reread = await manager.dao.get_with_relations(PIPELINE_A)

        assert [member.id for member in reread.members] == [MEMBER_NEW]
