"""Detection-pipeline list/detail expose correlation-pipeline membership chips."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

from apps.core.schemas import CorrelationPipelineRef
from apps.managers.pipeline_orchestrator import PipelineOrchestrator, correlation_pipeline_refs


def test_correlation_pipeline_refs_empty_when_unloaded_or_none() -> None:
    assert correlation_pipeline_refs(SimpleNamespace(correlation_pipelines=None)) == []
    assert correlation_pipeline_refs(SimpleNamespace(correlation_pipelines=[])) == []


def test_correlation_pipeline_refs_sorted_by_name_then_id() -> None:
    alpha_b = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000002"), name="Alpha")
    zebra = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000099"), name="Zebra")
    alpha_a = SimpleNamespace(id=UUID("00000000-0000-0000-0000-000000000001"), name="Alpha")
    refs = correlation_pipeline_refs(
        SimpleNamespace(correlation_pipelines=[zebra, alpha_b, alpha_a]),
    )
    assert refs == [
        CorrelationPipelineRef(id=alpha_a.id, name="Alpha"),
        CorrelationPipelineRef(id=alpha_b.id, name="Alpha"),
        CorrelationPipelineRef(id=zebra.id, name="Zebra"),
    ]


async def test_get_pipelines_list_includes_correlation_pipeline_refs(monkeypatch) -> None:
    from apps.managers import pipeline_orchestrator as po_module

    cp_id = uuid4()
    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    pipeline = SimpleNamespace(
        id=uuid4(),
        enabled=True,
        name="pipeline-a",
        deployment_name="pipeline-a-dep",
        source_topics=["events"],
        destination_topic="out",
        tagged_events_topic=None,
        repositories=[],
        log_source=None,
        filters=[],
        events_tagged=0,
        events_untagged=0,
        detectflow_matchnode_version=None,
        needs_restart=False,
        created=now,
        updated=now,
        correlation_pipelines=[SimpleNamespace(id=cp_id, name="Default")],
    )

    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = AsyncMock()
    orchestrator.pipeline_repo = SimpleNamespace(
        get_list_with_relations=AsyncMock(return_value=([pipeline], 1)),
        get_rule_counts=AsyncMock(return_value={}),
    )

    async def fake_status(pipeline_id: str, deployment_name: str, enabled: bool):
        return "running", None

    monkeypatch.setattr(po_module.pipeline_status_manager, "get_status_details", fake_status)

    result = await orchestrator.get_pipelines_list(
        {"page": 1, "limit": 10, "skip": 0, "offset": 0, "search": None, "sort": None, "order": "asc"}
    )

    assert result.data[0].correlation_pipelines == [CorrelationPipelineRef(id=cp_id, name="Default")]
