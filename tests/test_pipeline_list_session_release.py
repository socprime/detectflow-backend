"""Regression test: GET /pipeline releases its DB connection before Flink/K8s status HTTP.

``PipelineOrchestrator.get_pipelines_list`` must read all rows, ``commit()`` (returning the
pooled connection), and only then perform the per-pipeline status lookups. Holding the
request's connection across those ~5s Flink REST -> K8s calls was a pool-drain contributor.
See ``tasks/T3_release_sessions_before_external_http.md``.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from apps.managers import pipeline_orchestrator as po_module
from apps.managers.pipeline_orchestrator import PipelineOrchestrator


class _SpySession:
    """Records the ordering of commit() vs. status lookups in a shared event log."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def commit(self) -> None:
        self._events.append("commit")


def _fake_pipeline() -> SimpleNamespace:
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    return SimpleNamespace(
        id=uuid4(),
        enabled=True,
        name="pipeline-a",
        deployment_name="pipeline-a-dep",
        source_topics=["events"],
        destination_topic="out",
        tagged_events_topic=None,
        repositories=[SimpleNamespace(name="repo", type="local")],
        log_source=None,
        filters=[],
        pipeline_rules=[],
        correlation_pipelines=[],
        events_tagged=1,
        events_untagged=2,
        detectflow_matchnode_version="v1",
        needs_restart=False,
        created=now,
        updated=now,
    )


async def test_get_pipelines_list_commits_before_status_calls(monkeypatch) -> None:
    events: list[str] = []

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = _SpySession(events)
    orchestrator.pipeline_repo = SimpleNamespace(
        get_list_with_relations=AsyncMock(return_value=([_fake_pipeline(), _fake_pipeline()], 2)),
        get_rule_counts=AsyncMock(return_value={}),
    )

    async def fake_status(pipeline_id: str, deployment_name: str, enabled: bool):
        events.append("status")
        return "running", None

    monkeypatch.setattr(po_module.pipeline_status_manager, "get_status_details", fake_status)

    pagination = {
        "page": 1,
        "limit": 20,
        "skip": 0,
        "offset": 0,
        "search": None,
        "sort": None,
        "order": "asc",
    }

    result = await orchestrator.get_pipelines_list(pagination)

    assert result.total == 2
    assert len(result.data) == 2
    assert result.data[0].correlation_pipelines == []
    # The single commit must happen before ANY status HTTP call — no connection held across HTTP.
    assert events == ["commit", "status", "status"]
    assert events.index("commit") < events.index("status")
