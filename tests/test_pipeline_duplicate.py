"""Pipeline duplication.

Covers ``PipelineOrchestrator.duplicate_pipeline`` (what a copy inherits and what is reset),
``RulesOrchestrator.handle_pipeline_duplication`` (repository selections, per-rule ``enabled``
flags, Kafka sync under the new ``job_id``) and ``POST /api/v1/pipeline/{pipeline_id}/duplicate``.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from apps.core.schemas import PipelineResponse
from apps.managers import pipeline_orchestrator as po_module
from apps.managers.pipeline_orchestrator import (
    DUPLICATE_NAME_SUFFIX,
    PIPELINE_NAME_MAX_LENGTH,
    PipelineNotFoundError,
    PipelineOrchestrator,
)
from apps.managers.rule import RulesOrchestrator

# Runtime state that must never leak into a copy (a copy has never been deployed).
RUNTIME_STATE_FIELDS = (
    "status",
    "deployment_name",
    "events_tagged",
    "events_untagged",
    "last_sync_at",
    "detectflow_matchnode_version",
    "needs_restart",
)


def _source_pipeline(**overrides) -> SimpleNamespace:
    """A fully populated source pipeline, including runtime state that must not be copied."""
    source = SimpleNamespace(
        id=uuid.uuid4(),
        name="Windows Logs",
        enabled=True,
        source_topics=["prod-winlogbeat", "prod-winlogbeat-dc"],
        destination_topic="prod-windows-tagged",
        log_source_id=uuid.uuid4(),
        save_untagged=True,
        apply_parser_to_output_events=True,
        filters=[uuid.uuid4()],
        custom_fields="env: prod",
        rules_topic="detectflow.sigma-rules",
        metrics_topic="detectflow.rule-metrics",
        namespace="security",
        parallelism=4,
        taskmanager_memory_mb=2048,
        taskmanager_cpu=2.0,
        window_size_sec=45,
        checkpoint_interval_sec=90,
        autoscaler_enabled=True,
        autoscaler_min_parallelism=2,
        autoscaler_max_parallelism=8,
        status="running",
        deployment_name="pipeline-abc",
        events_tagged=2453,
        events_untagged=17,
        last_sync_at="2026-08-01T00:00:00Z",
        detectflow_matchnode_version="v1.2.0",
        needs_restart=True,
    )
    for key, value in overrides.items():
        setattr(source, key, value)
    return source


def _orchestrator(source, events: list[str], monkeypatch) -> tuple[PipelineOrchestrator, AsyncMock, AsyncMock]:
    """PipelineOrchestrator with only the collaborators duplicate_pipeline touches.

    ``__new__`` bypasses ``__init__`` (which builds every DAO and the Flink provider).
    """
    new_pipeline = SimpleNamespace(id=uuid.uuid4(), name=f"{source.name}{DUPLICATE_NAME_SUFFIX}")

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = SimpleNamespace(
        refresh=AsyncMock(),
        commit=AsyncMock(side_effect=lambda: events.append("commit")),
    )
    orchestrator.pipeline_repo = SimpleNamespace(
        get_by_id=AsyncMock(return_value=source),
        create=AsyncMock(return_value=new_pipeline),
    )
    handle_duplication = AsyncMock()
    orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_duplication=handle_duplication)
    orchestrator._provision_tagged_events_topic = AsyncMock()
    orchestrator._auto_add_to_correlation_pipelines_non_critical = AsyncMock()

    orchestrator.log_action = AsyncMock()
    monkeypatch.setattr(po_module.activity_producer, "log_action", orchestrator.log_action)
    orchestrator.invalidated = []
    monkeypatch.setattr(
        po_module.dashboard_service,
        "invalidate_pipeline_cache",
        lambda pipeline_id=None: (events.append("invalidate"), orchestrator.invalidated.append(pipeline_id)),
    )
    return orchestrator, handle_duplication, orchestrator.pipeline_repo.create


async def test_duplicate_pipeline_copies_configuration_and_disables_the_copy(monkeypatch) -> None:
    source = _source_pipeline()
    events: list[str] = []
    orchestrator, _, create = _orchestrator(source, events, monkeypatch)

    response = await orchestrator.duplicate_pipeline(source.id, SimpleNamespace(email="tester@example.com"))

    assert response.status is True
    created = create.await_args.kwargs
    assert created["name"] == "Windows Logs (Copy)"
    assert created["enabled"] is False
    for field in (
        "destination_topic",
        "log_source_id",
        "save_untagged",
        "apply_parser_to_output_events",
        "custom_fields",
        "parallelism",
        "taskmanager_memory_mb",
        "taskmanager_cpu",
        "window_size_sec",
        "checkpoint_interval_sec",
        "autoscaler_enabled",
        "autoscaler_min_parallelism",
        "autoscaler_max_parallelism",
    ):
        assert created[field] == getattr(source, field), field
    # Arrays are copied by value, not shared with the source row.
    assert created["source_topics"] == source.source_topics
    assert created["source_topics"] is not source.source_topics
    assert created["filters"] == source.filters
    assert created["filters"] is not source.filters
    # Runtime state stays with the source pipeline.
    for field in RUNTIME_STATE_FIELDS:
        assert field not in created, field


async def test_duplicate_pipeline_resolves_infrastructure_fields_from_settings(monkeypatch) -> None:
    """Topics/namespace are deployment-owned: a copy must not inherit a stale row's wiring."""
    source = _source_pipeline(
        rules_topic="legacy.sigma-rules",
        metrics_topic="legacy.rule-metrics",
        namespace="legacy-namespace",
    )
    orchestrator, _, create = _orchestrator(source, [], monkeypatch)

    await orchestrator.duplicate_pipeline(source.id, SimpleNamespace(email="tester@example.com"))

    created = create.await_args.kwargs
    assert created["rules_topic"] == po_module.settings.kafka_sigma_rules_topic
    assert created["metrics_topic"] == po_module.settings.kafka_metrics_topic
    assert created["namespace"] == po_module.settings.kubernetes_namespace


async def test_duplicate_pipeline_delegates_rules_and_invalidates_cache_after_commit(monkeypatch) -> None:
    source = _source_pipeline()
    events: list[str] = []
    orchestrator, handle_duplication, create = _orchestrator(source, events, monkeypatch)

    await orchestrator.duplicate_pipeline(source.id, SimpleNamespace(email="tester@example.com"))

    new_pipeline_id = create.return_value.id
    handle_duplication.assert_awaited_once_with(source.id, new_pipeline_id)
    # Topology invalidated exactly once, for the copy, AFTER the commit.
    assert events == ["commit", "invalidate"]
    assert orchestrator.invalidated == [new_pipeline_id]

    logged = orchestrator.log_action.await_args.kwargs
    assert logged["entity_type"] == "pipeline"
    assert logged["entity_id"] == str(new_pipeline_id)
    assert source.name in logged["details"]


async def test_duplicate_pipeline_truncates_long_names_to_the_column_limit(monkeypatch) -> None:
    source = _source_pipeline(name="W" * PIPELINE_NAME_MAX_LENGTH)
    orchestrator, _, create = _orchestrator(source, [], monkeypatch)

    await orchestrator.duplicate_pipeline(source.id, SimpleNamespace(email="tester@example.com"))

    name = create.await_args.kwargs["name"]
    assert len(name) == PIPELINE_NAME_MAX_LENGTH
    assert name.endswith(DUPLICATE_NAME_SUFFIX)


async def test_duplicate_pipeline_provisions_topic_then_rules_then_auto_adds(monkeypatch) -> None:
    """Same as create: the tagged-events topic must exist before any other side effect."""
    source = _source_pipeline()
    events: list[str] = []
    orchestrator, handle_duplication, create = _orchestrator(source, events, monkeypatch)
    new_pipeline = create.return_value
    user = SimpleNamespace(email="tester@example.com")
    orchestrator._provision_tagged_events_topic = AsyncMock(side_effect=lambda _pipeline: events.append("topic"))
    handle_duplication.side_effect = lambda *_args: events.append("rules")
    orchestrator._auto_add_to_correlation_pipelines_non_critical = AsyncMock(
        side_effect=lambda *_args: events.append("auto_add")
    )

    await orchestrator.duplicate_pipeline(source.id, user)

    assert events == ["topic", "rules", "auto_add", "commit", "invalidate"]
    orchestrator._provision_tagged_events_topic.assert_awaited_once_with(new_pipeline)
    orchestrator._auto_add_to_correlation_pipelines_non_critical.assert_awaited_once_with(user, new_pipeline.id)


async def test_duplicate_pipeline_raises_when_source_is_missing(monkeypatch) -> None:
    source = _source_pipeline()
    orchestrator, _, create = _orchestrator(source, [], monkeypatch)
    orchestrator.pipeline_repo.get_by_id = AsyncMock(return_value=None)

    with pytest.raises(PipelineNotFoundError):
        await orchestrator.duplicate_pipeline(source.id, SimpleNamespace(email="tester@example.com"))

    create.assert_not_awaited()


# ---------------------------------------------------------------------------
# RulesOrchestrator.handle_pipeline_duplication
# ---------------------------------------------------------------------------


@pytest.fixture
def rules_orchestrator() -> RulesOrchestrator:
    orchestrator = RulesOrchestrator(AsyncMock())
    orchestrator.pipeline_repo = AsyncMock()
    orchestrator.pipeline_rule_repo = AsyncMock()
    orchestrator.rule_check_service = AsyncMock()
    orchestrator.kafka = AsyncMock()
    return orchestrator


def _pipeline_rule(*, enabled: bool, is_supported: bool = True) -> SimpleNamespace:
    rule_id = uuid.uuid4()
    return SimpleNamespace(
        rule_id=rule_id,
        enabled=enabled,
        rule=SimpleNamespace(id=rule_id, name=f"rule-{rule_id}", is_supported=is_supported),
    )


async def test_handle_pipeline_duplication_preserves_per_rule_enabled_flags(rules_orchestrator) -> None:
    source_id, new_id = uuid.uuid4(), uuid.uuid4()
    enabled_rule = _pipeline_rule(enabled=True)
    disabled_rule = _pipeline_rule(enabled=False)
    rules_orchestrator.pipeline_repo.get_repository_selections.return_value = {}
    rules_orchestrator.pipeline_rule_repo.get_by_pipeline.return_value = ([enabled_rule, disabled_rule], 2)

    await rules_orchestrator.handle_pipeline_duplication(source_id, new_id)

    # Links are read from the SOURCE pipeline, unpaginated.
    read_call = rules_orchestrator.pipeline_rule_repo.get_by_pipeline.await_args
    assert read_call.args[0] == source_id
    assert read_call.kwargs["limit"] >= 100000

    rows = rules_orchestrator.pipeline_rule_repo.create_many.await_args.args[0]
    assert rows == [
        {"pipeline_id": new_id, "rule_id": enabled_rule.rule_id, "enabled": True},
        {"pipeline_id": new_id, "rule_id": disabled_rule.rule_id, "enabled": False},
    ]
    # The HOS check snapshot reads the copied rows, so it must be refreshed for every rule.
    rules_orchestrator.rule_check_service.refresh_for_rule_ids.assert_awaited_once_with(
        [enabled_rule.rule_id, disabled_rule.rule_id]
    )


async def test_handle_pipeline_duplication_publishes_only_enabled_supported_rules(rules_orchestrator) -> None:
    source_id, new_id = uuid.uuid4(), uuid.uuid4()
    live_rule = _pipeline_rule(enabled=True)
    disabled_rule = _pipeline_rule(enabled=False)
    unsupported_rule = _pipeline_rule(enabled=True, is_supported=False)
    rules_orchestrator.pipeline_repo.get_repository_selections.return_value = {}
    rules_orchestrator.pipeline_rule_repo.get_by_pipeline.return_value = (
        [live_rule, disabled_rule, unsupported_rule],
        3,
    )

    await rules_orchestrator.handle_pipeline_duplication(source_id, new_id)

    # Published under the copy's own job_id — Kafka keys are "{pipeline_id}:{rule_id}".
    rules_orchestrator.kafka.send_rules.assert_awaited_once_with(str(new_id), [live_rule.rule])


async def test_handle_pipeline_duplication_copies_repository_folder_selections(rules_orchestrator) -> None:
    source_id, new_id, repository_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    selections = {repository_id: ["_product/windows"]}
    rules_orchestrator.pipeline_repo.get_repository_selections.return_value = selections
    rules_orchestrator.pipeline_rule_repo.get_by_pipeline.return_value = ([], 0)

    await rules_orchestrator.handle_pipeline_duplication(source_id, new_id)

    rules_orchestrator.pipeline_repo.get_repository_selections.assert_awaited_once_with(source_id)
    rules_orchestrator.pipeline_repo.add_repositories.assert_awaited_once_with(new_id, selections)


async def test_handle_pipeline_duplication_without_rules_touches_no_rule_links(rules_orchestrator) -> None:
    rules_orchestrator.pipeline_repo.get_repository_selections.return_value = {}
    rules_orchestrator.pipeline_rule_repo.get_by_pipeline.return_value = ([], 0)

    await rules_orchestrator.handle_pipeline_duplication(uuid.uuid4(), uuid.uuid4())

    rules_orchestrator.pipeline_repo.add_repositories.assert_not_awaited()
    rules_orchestrator.pipeline_rule_repo.create_many.assert_not_awaited()
    rules_orchestrator.kafka.send_rules.assert_not_awaited()


# ---------------------------------------------------------------------------
# POST /api/v1/pipeline/{pipeline_id}/duplicate
# ---------------------------------------------------------------------------


def _patch_route_orchestrator(monkeypatch, duplicate: AsyncMock) -> None:
    """Replace the orchestrator class in the router module.

    Patching the bound method is not enough here: ``PipelineOrchestrator.__init__`` builds every
    DAO and the Flink provider (which loads a kubeconfig), so the class must never be constructed.
    """
    monkeypatch.setattr(
        "apps.routers.pipelines.PipelineOrchestrator",
        lambda db: SimpleNamespace(duplicate_pipeline=duplicate),
    )


async def test_duplicate_route_returns_201_with_the_new_pipeline_id(api_client, monkeypatch) -> None:
    new_id = str(uuid.uuid4())
    duplicate = AsyncMock(
        return_value=PipelineResponse(id=new_id, status=True, message="Pipeline duplicated successfully")
    )
    _patch_route_orchestrator(monkeypatch, duplicate)
    source_id = uuid.uuid4()

    response = await api_client.post(f"/api/v1/pipeline/{source_id}/duplicate")

    assert response.status_code == 201
    assert response.json() == {"id": new_id, "status": True, "message": "Pipeline duplicated successfully"}
    # Source id and the authenticated user (needed for activity logging) reach the manager.
    source_arg, user_arg = duplicate.await_args.args
    assert source_arg == source_id
    assert user_arg.email == "tester@example.com"


async def test_duplicate_route_maps_missing_pipeline_to_404(api_client, monkeypatch) -> None:
    source_id = uuid.uuid4()
    duplicate = AsyncMock(side_effect=PipelineNotFoundError(str(source_id)))
    _patch_route_orchestrator(monkeypatch, duplicate)

    response = await api_client.post(f"/api/v1/pipeline/{source_id}/duplicate")

    assert response.status_code == 404
    assert str(source_id) in response.json()["detail"]
