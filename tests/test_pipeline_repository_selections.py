import pytest
from pydantic import ValidationError

from apps.core.schemas import PipelineCreateRequest, PipelineUpdateRequest


def test_pipeline_create_merges_duplicate_repository_folder_selections():
    request = PipelineCreateRequest(
        name="Pipeline",
        source_topics=["raw"],
        destination_topic="tagged",
        log_source_id="550e8400-e29b-41d4-a716-446655440000",
        repository_ids=[
            {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "folders": ["_product/windows"],
            },
            {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "folders": ["_service/security", "_product/windows"],
            },
        ],
    )

    assert request.repository_ids is not None
    assert len(request.repository_ids) == 1
    assert request.repository_ids[0].id == "550e8400-e29b-41d4-a716-446655440001"
    assert request.repository_ids[0].folders == ["_product/windows", "_service/security"]


def test_pipeline_update_empty_folders_selects_whole_repository():
    request = PipelineUpdateRequest(
        repository_ids=[
            {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "folders": ["_product/windows"],
            },
            {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "folders": [],
            },
        ],
    )

    assert request.repository_ids is not None
    assert len(request.repository_ids) == 1
    assert request.repository_ids[0].folders == []


def test_pipeline_update_rejects_legacy_repository_id_list():
    with pytest.raises(ValidationError):
        PipelineUpdateRequest(repository_ids=["550e8400-e29b-41d4-a716-446655440001"])
