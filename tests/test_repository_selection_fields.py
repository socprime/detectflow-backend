"""Tests for repository selections on log sources and mapping APIs."""

import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from pydantic import ValidationError

from apps.core.converters import LogSourceConverter
from apps.core.schemas import LogSourceCreateRequest, LogSourceUpdateRequest, RepositorySelection
from apps.managers.log_sources import LogSourcesManager
from apps.managers.mapping import MappingManager

REPOSITORY_ID = "550e8400-e29b-41d4-a716-446655440001"
OTHER_REPOSITORY_ID = "550e8400-e29b-41d4-a716-446655440002"


def _load_mapping_router_module():
    """Load mapping router without importing apps.routers.__init__ and all routers."""
    module_path = Path(__file__).resolve().parents[1] / "apps" / "routers" / "mapping.py"
    spec = importlib.util.spec_from_file_location("test_mapping_router_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_folder_path_migration_module():
    """Load the folder path migration for lightweight SQL regression checks."""
    module_path = (
        Path(__file__).resolve().parents[1] / "alembic" / "versions" / "d2e3f4a5b6c7_update_rule_folder_paths.py"
    )
    spec = importlib.util.spec_from_file_location("test_folder_path_migration_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_log_source_create_merges_duplicate_test_repository_selections():
    request = LogSourceCreateRequest(
        name="Windows",
        test_repository_ids=[
            {"id": REPOSITORY_ID, "folders": ["_product/windows"]},
            {"id": REPOSITORY_ID, "folders": ["_service/sysmon", "_product/windows"]},
        ],
    )

    assert request.test_repository_ids is not None
    assert len(request.test_repository_ids) == 1
    assert request.test_repository_ids[0].id == REPOSITORY_ID
    assert request.test_repository_ids[0].folders == ["_product/windows", "_service/sysmon"]


def test_log_source_update_empty_folders_selects_whole_repository():
    request = LogSourceUpdateRequest(
        test_repository_ids=[
            {"id": REPOSITORY_ID, "folders": ["_product/windows"]},
            {"id": REPOSITORY_ID, "folders": []},
        ],
    )

    assert request.test_repository_ids is not None
    assert len(request.test_repository_ids) == 1
    assert request.test_repository_ids[0].folders == []


def test_log_source_test_repository_ids_rejects_legacy_uuid_list():
    with pytest.raises(ValidationError):
        LogSourceCreateRequest(name="Windows", test_repository_ids=[REPOSITORY_ID])


def test_repository_selection_rejects_invalid_uuid():
    with pytest.raises(ValidationError):
        LogSourceCreateRequest(name="Windows", test_repository_ids=[{"id": "not-a-uuid", "folders": []}])


def test_folder_path_migration_appends_scalar_segments_safely():
    migration = _load_folder_path_migration_module()

    assert "normalized := normalized || '" not in migration.NEW_FOLDER_PATH_FUNCTION
    assert "normalized := normalized || '" not in migration.OLD_FOLDER_PATH_FUNCTION
    assert "array_append(normalized, '_service')" in migration.NEW_FOLDER_PATH_FUNCTION
    assert "array_append(normalized, 'service')" in migration.OLD_FOLDER_PATH_FUNCTION


@pytest.mark.asyncio
async def test_log_source_manager_stores_test_repository_selections_as_json_dicts():
    created_log_source = MagicMock()
    created_log_source.id = UUID("550e8400-e29b-41d4-a716-446655440010")
    created_log_source.name = "Windows"

    log_source_repo = AsyncMock()
    log_source_repo.create.return_value = created_log_source

    request = LogSourceCreateRequest(
        name="Windows",
        test_repository_ids=[{"id": REPOSITORY_ID, "folders": ["_product/windows"]}],
    )
    user = MagicMock()

    with patch("apps.managers.log_sources.activity_producer") as activity_producer:
        activity_producer.log_action = AsyncMock()
        manager = LogSourcesManager(AsyncMock())
        manager.log_source_repo = log_source_repo

        result = await manager.create(request, user)

    assert result == created_log_source
    log_source_repo.create.assert_called_once_with(
        name="Windows",
        test_repository_ids=[{"id": REPOSITORY_ID, "folders": ["_product/windows"]}],
    )


def test_log_source_converter_returns_repository_selection_objects():
    log_source = MagicMock()
    log_source.id = UUID("550e8400-e29b-41d4-a716-446655440010")
    log_source.name = "Windows"
    log_source.parsing_script = None
    log_source.parsing_config = None
    log_source.mapping = None
    log_source.test_topics = ["raw"]
    log_source.test_repository_ids = [{"id": REPOSITORY_ID, "folders": ["_product/windows"]}]
    log_source.created = None
    log_source.updated = None

    response = LogSourceConverter.to_response(log_source)

    assert response.test_repository_ids is not None
    assert response.test_repository_ids[0].id == REPOSITORY_ID
    assert response.test_repository_ids[0].folders == ["_product/windows"]


@pytest.mark.asyncio
async def test_mapping_manager_uses_folder_aware_repository_selection_query():
    rule = MagicMock()
    rule.body = """
title: Test
detection:
  selection:
    EventID: 1
    CommandLine|contains: powershell
  condition: selection
"""
    rule_dao = AsyncMock()
    rule_dao.get_all_by_repository_selections.return_value = [rule]

    manager = MappingManager(AsyncMock())
    manager.rule_dao = rule_dao

    fields = await manager._get_sigma_fields([RepositorySelection(id=REPOSITORY_ID, folders=["_product/windows"])])

    rule_dao.get_all_by_repository_selections.assert_called_once_with({UUID(REPOSITORY_ID): ["_product/windows"]})
    assert sorted(fields) == ["CommandLine", "EventID"]


def test_mapping_request_schemas_accept_repository_selections():
    mapping_router = _load_mapping_router_module()

    generate_request = mapping_router.GenerateMappingRequest(
        repository_ids=[
            {"id": REPOSITORY_ID, "folders": ["_product/windows"]},
            {"id": REPOSITORY_ID, "folders": ["_service/sysmon"]},
            {"id": OTHER_REPOSITORY_ID, "folders": []},
        ],
        topics=["raw"],
        parser_query="parse-json",
    )
    sigma_fields_request = mapping_router.GetSigmaFieldsRequest(
        repository_ids=[{"id": REPOSITORY_ID, "folders": ["_product/windows"]}]
    )

    assert len(generate_request.repository_ids) == 2
    assert generate_request.repository_ids[0].folders == ["_product/windows", "_service/sysmon"]
    assert sigma_fields_request.repository_ids[0].id == REPOSITORY_ID
