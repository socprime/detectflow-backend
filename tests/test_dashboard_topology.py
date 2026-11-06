"""Tests for the dashboard topology cache and the batched repo rule-count query (T6).

The 2s SSE refresher must not re-read static DB topology every tick: rule counts collapse to
one GROUP BY (``RuleDAO.get_rule_counts_by_repositories``), and the topology snapshot is cached
on the ``DashboardService`` singleton for ``dashboard_topology_ttl_seconds`` — a warm tick
performs ZERO DB session acquisitions while EPS values stay tick-fresh from the poller cache.
See ``tasks/T6_dashboard_db_load.md``.
"""

import asyncio
import inspect
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import server
from apps.core.settings import settings
from apps.managers import dashboard as dashboard_module
from apps.managers import pipeline_orchestrator as po_module
from apps.managers.dashboard import DashboardService, DashboardTopology
from apps.managers.pipeline_orchestrator import PipelineOrchestrator
from apps.modules.postgre.rule import RuleDAO

# ---------------------------------------------------------------------------
# DAO: one GROUP BY aggregate instead of a per-repository count loop
# ---------------------------------------------------------------------------


async def test_rule_counts_by_repositories_single_grouped_query() -> None:
    repo_a, repo_b, repo_empty = uuid4(), uuid4(), uuid4()

    result = MagicMock()
    result.all.return_value = [(repo_a, 3), (repo_b, 5)]  # repo_empty has no rules -> no row
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)

    counts = await RuleDAO(session).get_rule_counts_by_repositories([repo_a, repo_b, repo_empty])

    assert counts == {repo_a: 3, repo_b: 5}
    assert repo_empty not in counts  # callers .get(id, 0)
    session.execute.assert_awaited_once()  # exactly ONE query for all repos


async def test_rule_counts_by_repositories_empty_input_runs_no_query() -> None:
    session = MagicMock()
    session.execute = AsyncMock()

    assert await RuleDAO(session).get_rule_counts_by_repositories([]) == {}
    session.execute.assert_not_awaited()


# ---------------------------------------------------------------------------
# Topology cache behavior on DashboardService
# ---------------------------------------------------------------------------


def _topology(
    fetched_at: float,
    enabled_pipelines: list[dict] | None = None,
    source_topics: list[str] | None = None,
    dest_topics: list[str] | None = None,
) -> DashboardTopology:
    return DashboardTopology(
        source_topics=source_topics or [],
        dest_topics=dest_topics or [],
        repositories=[],
        total_rules=0,
        pipelines_count=len(enabled_pipelines or []),
        enabled_pipelines=enabled_pipelines or [],
        pipelines_data=[],
        fetched_at=fetched_at,
    )


def _service_with_stubs(monkeypatch) -> DashboardService:
    """Fresh service; activity/status/poller stubbed so ticks touch no external systems."""
    service = DashboardService()
    monkeypatch.setattr(dashboard_module.activity_service, "get_recent", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        dashboard_module.pipeline_status_manager, "get_status_details", AsyncMock(return_value=("running", None))
    )
    monkeypatch.setattr(dashboard_module.flink_metrics_poller, "get_cache_entry", lambda _pid: None)
    return service


async def test_warm_tick_performs_zero_db_session_acquisitions(monkeypatch) -> None:
    """With a warm topology cache, get_dashboard_data must never touch AsyncSessionLocal."""
    service = _service_with_stubs(monkeypatch)
    service._topology = _topology(fetched_at=time.monotonic(), source_topics=["t1"], dest_topics=["d1"])

    session_factory_spy = MagicMock(side_effect=AssertionError("DB session acquired on a warm tick"))
    monkeypatch.setattr(dashboard_module, "AsyncSessionLocal", session_factory_spy)

    data = await service.get_dashboard_data()

    session_factory_spy.assert_not_called()
    # Payload shape unchanged (same DashboardData / DashboardGraphData fields).
    dumped = data.model_dump(mode="json")
    assert set(dumped) == {"graph", "pipelines_stats", "recent_activity"}
    assert {"source_topics", "repositories", "destination_topics", "pipelines_count"} <= set(dumped["graph"])
    assert dumped["graph"]["source_topics"] == [{"id": "t1", "name": "t1", "eps": 0.0}]


async def test_expired_topology_recomputes_exactly_once_under_concurrency(monkeypatch) -> None:
    """Two concurrent callers with an expired snapshot trigger ONE reload (lock + re-check)."""
    service = _service_with_stubs(monkeypatch)
    ttl = settings.dashboard_topology_ttl_seconds
    service._topology = _topology(fetched_at=time.monotonic() - ttl - 1)

    load_calls: list[int] = []

    async def fake_load() -> DashboardTopology:
        load_calls.append(1)
        await asyncio.sleep(0.01)  # force the callers to overlap
        return _topology(fetched_at=time.monotonic())

    service._load_topology = fake_load

    first, second = await asyncio.gather(service._get_topology(), service._get_topology())

    assert len(load_calls) == 1
    assert first is second  # the loser of the race reuses the winner's snapshot


async def test_invalidate_pipeline_cache_drops_topology_and_reloads_once(monkeypatch) -> None:
    service = _service_with_stubs(monkeypatch)
    service._topology = _topology(fetched_at=time.monotonic())

    service.invalidate_pipeline_cache()
    assert service._topology is None

    fresh = _topology(fetched_at=time.monotonic())
    load = AsyncMock(return_value=fresh)
    service._load_topology = load

    assert await service._get_topology() is fresh
    load.assert_awaited_once()
    # Subsequent warm call serves the cache without reloading.
    assert await service._get_topology() is fresh
    load.assert_awaited_once()


async def test_fresh_topology_is_served_without_reload() -> None:
    service = DashboardService()
    warm = _topology(fetched_at=time.monotonic())
    service._topology = warm
    service._load_topology = AsyncMock(side_effect=AssertionError("must not reload a fresh snapshot"))

    assert await service._get_topology() is warm


async def test_eps_values_fresh_on_every_tick_with_cached_topology(monkeypatch) -> None:
    """Changing poller cache values between two warm ticks changes the payload — no DB."""
    pipeline_id = uuid4()
    service = DashboardService()
    monkeypatch.setattr(dashboard_module.activity_service, "get_recent", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        dashboard_module.pipeline_status_manager, "get_status_details", AsyncMock(return_value=("running", None))
    )
    monkeypatch.setattr(
        dashboard_module,
        "AsyncSessionLocal",
        MagicMock(side_effect=AssertionError("DB session acquired on a warm tick")),
    )

    service._topology = _topology(
        fetched_at=time.monotonic(),
        enabled_pipelines=[
            {
                "id": pipeline_id,
                "source_topics": ["t1"],
                "destinations": [{"topic": "d1", "output_mode": "matched_only"}],
            }
        ],
        source_topics=["t1"],
        dest_topics=["d1"],
    )

    poller_entry = SimpleNamespace(input_eps=10.0, tagged_eps=4.0, untagged_eps=1.0, metrics=None)
    monkeypatch.setattr(dashboard_module.flink_metrics_poller, "get_cache_entry", lambda _pid: poller_entry)

    tick1 = await service.get_dashboard_data()
    assert tick1.graph.source_topics[0].eps == 10.0
    assert tick1.graph.destination_topics[0].tagged_eps == 4.0
    assert tick1.graph.total_events_eps == 10.0

    # Poller cache moves between ticks; topology stays cached.
    poller_entry.input_eps = 20.0
    poller_entry.tagged_eps = 8.0
    poller_entry.untagged_eps = 2.0

    tick2 = await service.get_dashboard_data()
    assert tick2.graph.source_topics[0].eps == 20.0
    assert tick2.graph.destination_topics[0].tagged_eps == 8.0
    assert tick2.graph.total_events_eps == 20.0


async def test_invalidation_during_inflight_reload_is_not_cached() -> None:
    """Regression: invalidate arriving mid-``_load_topology`` must not be swallowed.

    Without the generation counter the locked reload assigned its result unconditionally
    AFTER ``invalidate_pipeline_cache`` set ``_topology = None`` — a pre-CRUD snapshot
    silently served for a full TTL. The stale result may be returned to the in-flight
    caller, but it must NOT be cached; the next caller reloads fresh state.
    """
    service = DashboardService()
    release = asyncio.Event()
    load_calls: list[int] = []

    async def slow_load() -> DashboardTopology:
        load_calls.append(1)
        await release.wait()
        return _topology(fetched_at=time.monotonic())

    service._load_topology = slow_load

    reload_task = asyncio.create_task(service._get_topology())
    while not load_calls:  # deterministic: wait until the reload is inside _load_topology
        await asyncio.sleep(0)

    service.invalidate_pipeline_cache()  # CRUD lands mid-reload
    release.set()
    stale = await reload_task

    assert stale is not None  # in-flight caller still gets a payload
    assert service._topology is None  # ...but the pre-CRUD snapshot was NOT cached

    # Next caller reloads (second _load_topology call) instead of serving stale topology.
    fresh = _topology(fetched_at=time.monotonic())
    service._load_topology = AsyncMock(return_value=fresh)
    assert await service._get_topology() is fresh


# ---------------------------------------------------------------------------
# Pipeline CRUD invalidates the topology snapshot (create/update; delete already did)
# ---------------------------------------------------------------------------


def _flink_config_stub() -> SimpleNamespace:
    return SimpleNamespace(
        parallelism=1,
        taskmanager_memory_mb=1024,
        taskmanager_cpu=1.0,
        window_size_sec=60,
        checkpoint_interval_sec=60,
        autoscaler_enabled=False,
        autoscaler_min_parallelism=1,
        autoscaler_max_parallelism=1,
    )


async def test_create_pipeline_invalidates_topology_after_commit(monkeypatch) -> None:
    events: list[str] = []
    new_pipeline = SimpleNamespace(id=uuid4(), name="p1", source_topics=["t1"], destination_topic="d1")

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = SimpleNamespace(
        refresh=AsyncMock(),
        commit=AsyncMock(side_effect=lambda: events.append("commit")),
    )
    orchestrator.pipeline_repo = SimpleNamespace(create=AsyncMock(return_value=new_pipeline), update=AsyncMock())
    orchestrator.config_dao = SimpleNamespace(get_flink_defaults=AsyncMock(return_value={}))
    orchestrator._validate_resources = AsyncMock()
    orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_creation=AsyncMock())
    # Kafka + correlation side effects of create are covered by their own tests; both are
    # side channels here and neither may reorder commit/invalidate.
    orchestrator._provision_tagged_events_topic = AsyncMock()
    orchestrator._auto_add_to_correlation_pipelines_non_critical = AsyncMock()

    monkeypatch.setattr(
        po_module.FlinkConfigManager, "resolve_config", staticmethod(lambda **_kw: _flink_config_stub())
    )
    monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())

    invalidated: list = []
    monkeypatch.setattr(
        po_module.dashboard_service,
        "invalidate_pipeline_cache",
        lambda pipeline_id=None: (events.append("invalidate"), invalidated.append(pipeline_id)),
    )

    request = SimpleNamespace(
        name="p1",
        source_topics=["t1"],
        destinations=None,  # legacy singular shape; orchestrator synthesizes destinations
        destination_topic="d1",
        log_source_id=str(uuid4()),
        enabled=False,  # skip Flink deployment path
        save_untagged=False,
        apply_parser_to_output_events=False,
        filters=[],
        custom_fields=None,
        repository_ids=None,
        resources=None,
        kafka_starting_offset_earliest=False,
    )
    user = SimpleNamespace(email="tester@example.com")

    response = await orchestrator.create_pipeline(request, user)

    assert response.status is True
    # Topology invalidated exactly once, for the new pipeline, AFTER the commit.
    assert events == ["commit", "invalidate"]
    assert invalidated == [new_pipeline.id]


async def test_update_pipeline_invalidates_topology_after_commit(monkeypatch) -> None:
    events: list[str] = []
    pipeline_id = uuid4()
    existing = SimpleNamespace(enabled=False, filters=[])
    updated = SimpleNamespace(id=pipeline_id, enabled=False)

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = SimpleNamespace(commit=AsyncMock(side_effect=lambda: events.append("commit")))
    orchestrator.pipeline_repo = SimpleNamespace(
        get_with_relations=AsyncMock(return_value=existing),
        get_repository_selections=AsyncMock(return_value={}),
        update=AsyncMock(return_value=updated),
        get_by_id=AsyncMock(return_value=updated),
    )
    orchestrator._handle_stays_disabled_scenario = AsyncMock()
    orchestrator._log_update_activity = AsyncMock()

    monkeypatch.setattr(
        po_module,
        "detect_pipeline_changes",
        lambda _req, _existing: SimpleNamespace(update_data={}, flink_restart_needed=False),
    )

    invalidated: list = []
    monkeypatch.setattr(
        po_module.dashboard_service,
        "invalidate_pipeline_cache",
        lambda pipeline_id=None: (events.append("invalidate"), invalidated.append(pipeline_id)),
    )

    request = SimpleNamespace(repository_ids=None, filters=None)
    user = SimpleNamespace(email="tester@example.com")

    response = await orchestrator.update_pipeline(pipeline_id, request, user)

    assert response.status is True
    assert events == ["commit", "invalidate"]
    assert invalidated == [pipeline_id]


def test_flink_metrics_poller_runs_on_every_worker_not_leader_only() -> None:
    """Dashboard EPS lives in an in-process cache; leader-only polling makes followers serve 0 EPS."""
    start_src = inspect.getsource(server._start_singleton_services)
    stop_src = inspect.getsource(server._stop_singleton_services)
    lifespan_src = inspect.getsource(server.lifespan)

    assert "flink_metrics_poller" not in start_src
    assert "flink_metrics_poller" not in stop_src
    assert "flink_metrics_poller.start" in lifespan_src
    assert "flink_metrics_poller.stop" in lifespan_src
