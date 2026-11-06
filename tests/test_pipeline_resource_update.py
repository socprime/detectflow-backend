"""Tests for PATCH pipeline resource/processing fields and last-state Flink upgrades."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from apps.core.schemas import FlinkResourceConfig, PipelineUpdateRequest
from apps.managers.pipeline_orchestrator import (
    FLINK_RESOURCE_FIELDS,
    PipelineChanges,
    PipelineOrchestrator,
    detect_pipeline_changes,
)
from apps.providers.base import FlinkDeploymentResult, FlinkQuotaExceededError


def _existing_pipeline(**overrides: object) -> SimpleNamespace:
    values = {
        "name": "p1",
        "source_topics": ["in"],
        "destination_topic": "out",
        "save_untagged": False,
        "apply_parser_to_output_events": False,
        "custom_fields": None,
        "enabled": True,
        "filters": [],
        "log_source_id": uuid4(),
        "parallelism": 2,
        "taskmanager_memory_mb": 2048,
        "taskmanager_cpu": 1.0,
        "window_size_sec": 30,
        "checkpoint_interval_sec": 60,
        "autoscaler_enabled": False,
        "autoscaler_min_parallelism": 1,
        "autoscaler_max_parallelism": 24,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _resource_changes(**update_data: object) -> PipelineChanges:
    return PipelineChanges(
        update_data=dict(update_data),
        flink_restart_needed=True,
        cleanup_needed=False,
        filters_changed=False,
        log_source_changed=False,
        old_log_source_id=None,
        new_log_source_id=None,
        custom_fields_changed=False,
        old_custom_fields=None,
        new_custom_fields=None,
    )


@pytest.mark.parametrize(
    ("field", "old", "new"),
    [
        ("parallelism", 2, 4),
        ("taskmanager_memory_mb", 2048, 4096),
        ("taskmanager_cpu", 1.0, 2.0),
        ("window_size_sec", 30, 15),
        ("checkpoint_interval_sec", 60, 120),
        ("autoscaler_enabled", False, True),
        ("autoscaler_min_parallelism", 1, 2),
        ("autoscaler_max_parallelism", 24, 48),
    ],
)
def test_resource_field_change_triggers_restart_without_cleanup(field: str, old: object, new: object) -> None:
    existing = _existing_pipeline(**{field: old})
    request = PipelineUpdateRequest(resources=FlinkResourceConfig(**{field: new}))

    changes = detect_pipeline_changes(request, existing)

    assert changes.update_data == {field: new}
    assert changes.flink_restart_needed is True
    assert changes.cleanup_needed is False


def test_unchanged_and_omitted_resources_do_not_restart() -> None:
    existing = _existing_pipeline()

    identical = detect_pipeline_changes(
        PipelineUpdateRequest(resources=FlinkResourceConfig(taskmanager_cpu=1.0, parallelism=2)),
        existing,
    )
    omitted = detect_pipeline_changes(PipelineUpdateRequest(name="renamed"), existing)
    empty = detect_pipeline_changes(PipelineUpdateRequest(resources=FlinkResourceConfig()), existing)

    assert identical.flink_restart_needed is False
    assert identical.update_data == {}
    assert omitted.flink_restart_needed is False
    assert omitted.update_data == {"name": "renamed"}
    assert empty.flink_restart_needed is False
    assert empty.update_data == {}


def test_partial_resources_only_copy_provided_fields() -> None:
    existing = _existing_pipeline()
    request = PipelineUpdateRequest(resources=FlinkResourceConfig(taskmanager_memory_mb=4096, window_size_sec=45))

    changes = detect_pipeline_changes(request, existing)

    assert changes.update_data == {"taskmanager_memory_mb": 4096, "window_size_sec": 45}
    assert set(changes.update_data) <= set(FLINK_RESOURCE_FIELDS)
    assert changes.cleanup_needed is False


def test_source_topics_still_cleanup() -> None:
    existing = _existing_pipeline()
    request = PipelineUpdateRequest(source_topics=["other-in"])

    changes = detect_pipeline_changes(request, existing)

    assert changes.flink_restart_needed is True
    assert changes.cleanup_needed is True


async def test_restart_resource_only_skips_storage_cleanup() -> None:
    pipeline_id = uuid4()
    updated = SimpleNamespace(id=pipeline_id)
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._flink = SimpleNamespace(
        cleanup_storage=AsyncMock(),
        update_deployment=AsyncMock(return_value=FlinkDeploymentResult(deployment_name="flink-x", success=True)),
    )
    orchestrator._build_deployment_config = lambda _pipeline: SimpleNamespace()
    orchestrator._sync_kafka_data_on_enable = AsyncMock()

    await orchestrator._handle_restart_scenario(pipeline_id, updated, _resource_changes(taskmanager_cpu=2.0))

    orchestrator._flink.update_deployment.assert_awaited_once()
    orchestrator._flink.cleanup_storage.assert_not_called()
    orchestrator._sync_kafka_data_on_enable.assert_awaited_once()


async def test_restart_quota_exceeded_keeps_pipeline_enabled() -> None:
    pipeline_id = uuid4()
    updated = SimpleNamespace(id=pipeline_id)
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._flink = SimpleNamespace(
        update_deployment=AsyncMock(side_effect=FlinkQuotaExceededError("quota", "details")),
    )
    orchestrator._build_deployment_config = lambda _pipeline: SimpleNamespace()
    orchestrator.db = SimpleNamespace(rollback=AsyncMock(), commit=AsyncMock())
    orchestrator.pipeline_repo = SimpleNamespace(update=AsyncMock())

    with pytest.raises(HTTPException) as exc_info:
        await orchestrator._handle_restart_scenario(pipeline_id, updated, _resource_changes(parallelism=8))

    assert exc_info.value.status_code == 409
    orchestrator.pipeline_repo.update.assert_not_called()
    orchestrator.db.rollback.assert_not_called()
    orchestrator.db.commit.assert_not_called()


async def test_enable_applies_current_spec_via_update_deployment() -> None:
    pipeline_id = uuid4()
    updated = SimpleNamespace(id=pipeline_id)
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._flink = SimpleNamespace(
        find_deployment=AsyncMock(return_value="flink-x"),
        update_deployment=AsyncMock(return_value=FlinkDeploymentResult(deployment_name="flink-x", success=True)),
        resume_deployment=AsyncMock(),
        create_deployment=AsyncMock(),
    )
    orchestrator._build_deployment_config = lambda _pipeline: SimpleNamespace()
    orchestrator._sync_kafka_data_on_enable = AsyncMock()

    await orchestrator._handle_enable_scenario(pipeline_id, updated)

    orchestrator._flink.update_deployment.assert_awaited_once()
    orchestrator._flink.resume_deployment.assert_not_called()
    orchestrator._flink.create_deployment.assert_not_called()
    orchestrator._sync_kafka_data_on_enable.assert_awaited_once()


async def test_enable_creates_when_no_deployment() -> None:
    pipeline_id = uuid4()
    updated = SimpleNamespace(id=pipeline_id)
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._flink = SimpleNamespace(
        find_deployment=AsyncMock(return_value=None),
        update_deployment=AsyncMock(),
        create_deployment=AsyncMock(return_value=FlinkDeploymentResult(deployment_name="flink-new", success=True)),
    )
    orchestrator._build_deployment_config = lambda _pipeline: SimpleNamespace()
    orchestrator._sync_kafka_data_on_enable = AsyncMock()
    orchestrator.pipeline_repo = SimpleNamespace(update=AsyncMock())

    await orchestrator._handle_enable_scenario(pipeline_id, updated)

    orchestrator._flink.update_deployment.assert_not_called()
    orchestrator._flink.create_deployment.assert_awaited_once()
    orchestrator.pipeline_repo.update.assert_awaited_once_with(pipeline_id, deployment_name="flink-new")
    orchestrator._sync_kafka_data_on_enable.assert_awaited_once()
