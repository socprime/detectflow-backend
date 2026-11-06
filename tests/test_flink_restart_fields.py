"""``FLINK_RESTART_FIELDS`` / ``CLEANUP_TRIGGER_FIELDS`` — the authoritative trigger sets.

``apps/routers/pipelines.py`` points operators at ``FLINK_RESTART_FIELDS`` as the source of truth
for "does this edit cost 30 s of downtime", so its membership is a documented contract, not an
implementation detail.

``tagged_events_topic`` is the interesting member: it is in the restart set but deliberately NOT in
the cleanup set. Only the matcher's side-output SINK moves — its input offsets stay valid, so
deleting checkpoints would replay the source for nothing. It also has no field on
``PipelineUpdateRequest`` (the name is immutable, epic Decision 1), so ``detect_pipeline_changes``
can never set it; the assign endpoint constructs its own ``PipelineChanges`` and routes through the
same restart scenario.
"""

from types import SimpleNamespace
from uuid import uuid4

from apps.core.schemas import PipelineUpdateRequest
from apps.managers.pipeline_orchestrator import (
    CLEANUP_TRIGGER_FIELDS,
    FLINK_RESTART_FIELDS,
    PipelineChanges,
    detect_pipeline_changes,
)


def _existing(**overrides) -> SimpleNamespace:
    base = {
        "id": uuid4(),
        "name": "p1",
        "source_topics": ["in"],
        "destination_topic": "out",
        "tagged_events_topic": None,
        "save_untagged": False,
        "apply_parser_to_output_events": False,
        "custom_fields": None,
        "filters": [],
        "log_source_id": None,
        "enabled": True,
    }
    return SimpleNamespace(**{**base, **overrides})


def test_tagged_events_topic_triggers_restart() -> None:
    assert "tagged_events_topic" in FLINK_RESTART_FIELDS

    # The assign path's own PipelineChanges, as the manager builds it.
    changes = PipelineChanges(
        update_data={"tagged_events_topic": "detectflow.tagged-events.x"},
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
    assert changes.flink_restart_needed is True
    assert changes.cleanup_needed is False


def test_tagged_events_topic_does_not_trigger_cleanup() -> None:
    """Cleanup resets consumer offsets; the matcher's INPUT offsets are untouched by a sink move."""
    assert "tagged_events_topic" not in CLEANUP_TRIGGER_FIELDS
    assert CLEANUP_TRIGGER_FIELDS <= FLINK_RESTART_FIELDS


def test_update_request_cannot_set_the_topic() -> None:
    """Hard constraint 1 — the assign endpoint is the only writer, so no PATCH may reach it."""
    assert "tagged_events_topic" not in PipelineUpdateRequest.model_fields

    request = PipelineUpdateRequest(name="renamed")
    changes = detect_pipeline_changes(request, _existing())

    assert "tagged_events_topic" not in changes.update_data
    assert changes.flink_restart_needed is False


def test_destination_topic_still_restarts_but_does_not_clean_up() -> None:
    """Guard the neighbours: this task must not widen or narrow the pre-existing triggers."""
    changes = detect_pipeline_changes(PipelineUpdateRequest(destination_topic="new-out"), _existing())

    assert changes.flink_restart_needed is True
    assert changes.cleanup_needed is False


def test_source_topics_still_trigger_both() -> None:
    changes = detect_pipeline_changes(PipelineUpdateRequest(source_topics=["other-in"]), _existing())

    assert changes.flink_restart_needed is True
    assert changes.cleanup_needed is True
