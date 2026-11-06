"""The correlation-pipeline status vocabulary (owned by the backend).

Six values, derived, never persisted. The sixth (``failed``) exists because the pre-epic five-value
vocabulary reported a TOTAL outage as ``partial`` — telling the UI a half was still working when
none was.
"""

import pathlib

from apps.core.enums import CorrelationPipelineStatus
from apps.core.schemas import (
    AttackChainsJobState,
    AttackChainsRuntimeResponse,
    AttackChainsRuntimeStateResponse,
)
from apps.managers.correlation_pipelines import derive_overall_status, list_chip_runtime_state


def test_exactly_six_members_with_their_wire_values() -> None:
    assert [member.value for member in CorrelationPipelineStatus] == [
        "running",
        "starting",
        "stopping",
        "stopped",
        "partial",
        "failed",
    ]


def test_stopped_is_the_only_disabled_and_there_is_no_seventh_member() -> None:
    """ "Disabled" is the UI's label for ``stopped``, not a state of its own."""
    assert "disabled" not in {member.value for member in CorrelationPipelineStatus}
    assert 'renders as **"Disabled"**' in (CorrelationPipelineStatus.__doc__ or "")


def test_both_halves_failed_is_failed() -> None:
    assert derive_overall_status("failed", "failed", jobs_status="starting") == "failed"


def test_exactly_one_half_failed_is_partial() -> None:
    assert derive_overall_status("failed", "running", jobs_status="starting") == "partial"
    assert derive_overall_status("running", "failed", jobs_status="starting") == "partial"


def test_list_chip_remaps_partial_to_failed_and_keeps_job_tiles() -> None:
    """The list badge has no room for Partial; detail / run still use the derived value."""
    state = AttackChainsRuntimeStateResponse(
        overall=CorrelationPipelineStatus.PARTIAL,
        correlation_job=AttackChainsJobState(deployment_name="corr", status="running"),
        agent_job=AttackChainsJobState(deployment_name="agent", status="failed"),
    )
    chip = list_chip_runtime_state(state)
    assert chip.overall == CorrelationPipelineStatus.FAILED
    assert chip.correlation_job.status == "running"
    assert chip.agent_job.status == "failed"
    assert state.overall == CorrelationPipelineStatus.PARTIAL


def test_list_chip_leaves_non_partial_overall_alone() -> None:
    running = AttackChainsRuntimeStateResponse(
        overall=CorrelationPipelineStatus.RUNNING,
        correlation_job=AttackChainsJobState(deployment_name="corr", status="running"),
        agent_job=AttackChainsJobState(deployment_name="agent", status="running"),
    )
    assert list_chip_runtime_state(running) is running


def test_the_other_branches_are_unchanged() -> None:
    assert derive_overall_status("running", "running", jobs_status="starting") == "running"
    assert derive_overall_status("starting", "running", jobs_status="starting") == "starting"
    assert derive_overall_status("missing", "missing", jobs_status="stopped") == "stopped"
    assert derive_overall_status("running", "missing", jobs_status="stopping") == "stopping"


def test_every_derived_status_is_a_member_of_the_enum() -> None:
    states = ("running", "starting", "stopping", "failed", "missing")
    values = {member.value for member in CorrelationPipelineStatus}
    for corr in states:
        for agent in states:
            for jobs_status in ("starting", "stopping", "stopped"):
                assert derive_overall_status(corr, agent, jobs_status=jobs_status) in values


def test_composite_status_fields_are_typed_with_the_enum_not_a_bare_string() -> None:
    assert AttackChainsRuntimeStateResponse.model_fields["overall"].annotation is CorrelationPipelineStatus
    assert AttackChainsRuntimeResponse.model_fields["status"].annotation is CorrelationPipelineStatus


def test_per_job_vocabulary_stays_separate() -> None:
    """The two tiles and the composite badge are different fields with different vocabularies."""
    from apps.core.schemas import AttackChainsJobState

    per_job = AttackChainsJobState.model_fields["status"].annotation
    assert set(per_job.__args__) == {"running", "starting", "stopping", "failed", "missing"}


def test_the_db_check_vocabulary_is_untouched() -> None:
    """``jobs_status`` is DESIRED state; widening its CHECK would persist a derived value."""
    migration = next(
        path
        for path in pathlib.Path("alembic/versions").glob("*.py")
        if "jobs_status" in path.read_text() and "CheckConstraint" in path.read_text()
    )
    body = migration.read_text()
    assert "'starting'" in body and "'stopping'" in body and "'stopped'" in body
    assert "'partial'" not in body and "'failed'" not in body
