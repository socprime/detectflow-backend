"""Pipeline management endpoints.

This module provides REST API endpoints for managing ETL pipelines,
including CRUD operations, statistics, and Flink deployment integration.
"""

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.database import get_db
from apps.core.enums import PipelineRuleSortField, PipelineSortField
from apps.core.exceptions import NotFoundError
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.schemas import (
    PipelineCreateRequest,
    PipelineDetailResponse,
    PipelineListResponse,
    PipelineResponse,
    PipelineRulesListResponse,
    PipelineUpdateRequest,
    RuleListItem,
    TaggedEventsTopicAssignRequest,
)
from apps.managers.pipeline_orchestrator import (
    PipelineNotFoundError,
    PipelineOrchestrator,
    ResourceNotFoundError,
)
from apps.managers.rule import RulesOrchestrator
from apps.modules.postgre.metrics import MetricsDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO

router = APIRouter(prefix="/api/v1/pipeline", tags=["Pipelines"])
logger = get_logger(__name__)


@router.get(
    "",
    response_model=PipelineListResponse,
    summary="List pipelines",
    responses=errors(UNAUTHORIZED),
)
async def get_pipelines(
    _: User = Depends(get_current_active_user),
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    sort: PipelineSortField | None = Query(default=None, description="Field to sort by"),
    order: Literal["asc", "desc"] = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search by pipeline name (case-insensitive)"),
    db: AsyncSession = Depends(get_db),
):
    """
    List all ETL pipelines with pagination and filtering.

    Returns pipelines with their associated topics, log sources, repositories,
    correlation-pipeline membership (`correlation_pipelines` id/name chips),
    real-time statistics (events tagged/untagged), and live Flink deployment status
    (resolved per-pipeline at request time, not stored).

    Sort options: name, source_topics, destination_topic, log_source, filters, rules,
    events_tagged, events_untagged, created, enabled, status
    """
    skip = (page - 1) * limit
    pagination = {
        "page": page,
        "limit": limit,
        "sort": sort.value if sort else None,
        "order": order,
        "search": search,
        "skip": skip,
        "offset": 0,
    }
    orchestrator = PipelineOrchestrator(db)
    return await orchestrator.get_pipelines_list(pagination)


@router.post(
    "",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Referenced log source, filter, or repository not found"),
        {
            422: {
                "description": (
                    "Request validation failed (e.g. source topic equals destination topic, "
                    "or a reserved system topic was used)"
                )
            }
        },
        err(500, "Failed to create the Flink deployment (pipeline is rolled back)"),
    ),
)
async def create_pipeline(
    pipeline: PipelineCreateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new ETL pipeline.

    Persists the pipeline configuration. If `enabled=true`, also deploys a Flink job to
    Kubernetes and publishes the pipeline's rules, filters, parser, and custom fields to
    Kafka so the match node picks them up. On Flink-deployment failure the deployment is
    rolled back and a 500 is returned. The action is recorded in the activity log.

    **Required fields:** name, source_topics, destinations (or legacy destination_topic), log_source_id

    **Kafka offset:** set optional `kafka_starting_offset_earliest=true` to consume from the
    earliest offset on first start; otherwise consumption starts from `latest`.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.create_pipeline(pipeline, current_user)
    except ResourceNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.post(
    "/{pipeline_id}/duplicate",
    response_model=PipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Duplicate pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Pipeline not found"),
        err(503, "Kafka unavailable — the topic could not be provisioned"),
    ),
)
async def duplicate_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a copy of an existing pipeline.

    The copy is named `<source name> (Copy)` and keeps the source configuration: source and
    destination topics, log source, filters, custom fields, Flink resources, the selected
    repositories with their folder paths, and the pipeline's rules — including each rule's
    enabled/disabled state.

    The copy is always created **disabled** (Run/Stop off): no Flink deployment is created and no
    filters/custom fields/parser are published to Kafka. A dedicated tagged-events topic is
    provisioned and the copy is auto-added to matching correlation pipelines (same as create).
    Runtime state (status, deployment, event counters, last sync) is not copied. Pipeline names
    are not unique, so duplicating the same pipeline twice yields two identically named copies.
    The action is recorded in the activity log.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.duplicate_pipeline(UUID(pipeline_id), current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.get(
    "/{pipeline_id}",
    response_model=PipelineDetailResponse,
    summary="Get pipeline details",
    responses=errors(UNAUTHORIZED, err(404, "Pipeline not found")),
)
async def get_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a pipeline.

    Returns full configuration, associated resources (including correlation-pipeline
    membership as `correlation_pipelines` id/name chips), rule-validation counts, warnings,
    and the real-time Flink deployment status (queried live, not stored). Read-only.

    **Status values:** `running`, `disabled`, `deploying`, `failed`, `not_found`, `unknown`
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.get_pipeline_details(UUID(pipeline_id))
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.patch(
    "/{pipeline_id}",
    response_model=PipelineResponse,
    summary="Update pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Pipeline not found"),
        {
            422: {
                "description": (
                    "Request validation failed (e.g. source topic equals destination topic, "
                    "or a reserved system topic was used)"
                )
            }
        },
        err(409, "Enable rejected: cluster resource quota (CPU/memory/pods) exceeded"),
        err(500, "Failed to update the Flink deployment"),
    ),
)
async def update_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    pipeline: PipelineUpdateRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing pipeline.

    The update path depends on which fields changed and on the enabled state. Changes are
    classified into two buckets (the authoritative restart set is `FLINK_RESTART_FIELDS` in
    `apps/managers/pipeline_orchestrator.py` — consult it rather than this summary):

    - **Hot-reload (no downtime):** name, filters, log source, repositories, and custom fields
      are republished to Kafka and the match node applies them live.
    - **Flink last-state upgrade (~30s):** changing source topics, destinations (topic /
      output_mode / apply_parser, including via the legacy `destination_topic`, `save_untagged`,
      parser-on-output fields), or `resources` (CPU, RAM, parallelism, window/checkpoint,
      autoscaler) patches the FlinkDeployment spec. The operator restores from the last
      checkpoint; job ID is kept. Checkpoints are wiped only when `source_topics` change.

    **Enable/Disable behavior:**
    - Enable: patches the existing deployment with the current DB spec (so resource edits made
      while disabled take effect) and resumes from checkpoint, or creates a fresh one; then
      republishes all rules/filters/parser/custom fields to Kafka.
    - Disable: suspends the deployment (state and job ID preserved); DB and Kafka data are kept.
    - Neither restarts any correlation pipeline any more: a correlation job's input list is derived
      from membership, and a member's tagged-events topic exists from creation, so a disabled
      pipeline simply stops producing into a topic that still exists.

    Changing destination topics is SIEM output only and does not republish chains-config or
    restart a correlation job. Membership is ``member_pipeline_ids``; isolation is the dedicated
    tagged-events topic. The action is recorded in the activity log.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.update_pipeline(UUID(pipeline_id), pipeline, current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.post(
    "/{pipeline_id}/tagged-events-topic",
    response_model=PipelineResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Assign a dedicated tagged-events topic",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Pipeline not found"),
        err(503, "Kafka unavailable — the topic could not be provisioned"),
    ),
)
async def assign_tagged_events_topic(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    request: TaggedEventsTopicAssignRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Move a pre-epic pipeline off the shared tagged-events topic onto its own.

    Creates `detectflow.tagged-events.<pipeline_id>`, pins it on the pipeline, and — if the
    pipeline is enabled — updates `KAFKA_TAGGED_EVENTS_TOPIC` on the matcher deployment, which
    the Flink Operator applies by upgrading the job from its last checkpoint.

    **⚠️ This restarts the detection pipeline (~30s)** and is the only operation that does so for
    correlation reasons, hence the required `confirm_restart=true`. The topic name is then fixed
    for the pipeline's lifetime. Pipelines without one keep writing to the shared legacy topic.

    Idempotent: a pipeline that already has a dedicated topic returns 202 with no restart. The
    owning correlation pipeline (if any) is refreshed afterwards, non-critically. The action is
    recorded in the activity log.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.assign_tagged_events_topic(UUID(pipeline_id), request, current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.delete(
    "/{pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete pipeline",
    responses=errors(
        {204: {"description": "Pipeline deleted successfully"}},
        UNAUTHORIZED,
        err(404, "Pipeline not found"),
    ),
)
async def delete_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Permanently delete a pipeline.

    **This action is irreversible.** Deletes the Flink deployment from Kubernetes, its
    checkpoints/savepoints, the pipeline's rules/filters/parser/custom fields from Kafka,
    and the pipeline configuration (with its metrics history) from the database. Membership of a
    correlation pipeline is cascade-removed: that correlation job is re-rendered without the
    departing topic, and only then is the pipeline's own tagged-events topic retired. The deletion
    is recorded in the activity log.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        await orchestrator.delete_pipeline(UUID(pipeline_id), current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.get(
    "/{pipeline_id}/details/rules",
    response_model=PipelineRulesListResponse,
    summary="List pipeline rules",
    responses=errors(UNAUTHORIZED, err(404, "Pipeline not found")),
)
async def get_pipeline_rules(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    _: User = Depends(get_current_active_user),
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    sort: PipelineRuleSortField | None = Query(default=None, description="Field to sort by"),
    order: Literal["asc", "desc"] = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search by rule name or Sigma UUID"),
    tagged_filter: Literal["all", "tagged", "untagged"] = Query(
        default="all", description="Filter by tagged events: all, tagged (>0 matches), untagged (0 matches)"
    ),
    supported_filter: Literal["all", "supported", "unsupported"] = Query(
        default="all", description="Filter by validation status: all, supported, unsupported"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    List all rules associated with a pipeline.

    Returns rules with their enabled status, tagged events count, and validation status.
    Supports pagination and filtering by supported/unsupported status. `search` matches either the
    rule name (substring) or a whole Sigma UUID — dashed, undashed, upper-case or URN-wrapped. Only
    a whole UUID is treated as an id; any other term stays a plain name search.
    """
    skip = (page - 1) * limit
    logger.info(
        "Fetching pipeline rules",
        extra={"pipeline_id": pipeline_id, "page": page, "limit": limit},
    )

    repo = PipelineDAO(db)
    pipeline_uuid = UUID(pipeline_id)

    pipeline = await repo.get_by_id(pipeline_uuid)
    if not pipeline:
        logger.warning("Pipeline not found for rules list", extra={"pipeline_id": pipeline_id})
        raise NotFoundError(f"Pipeline with id {pipeline_id} not found")

    rule_repo = PipelineRulesDAO(db)
    rules, total = await rule_repo.get_by_pipeline(
        pipeline_uuid,
        skip=skip,
        limit=limit,
        search=search,
        sort=sort.value if sort else None,
        order=order,
        tagged_filter=tagged_filter,
        supported_filter=supported_filter,
    )

    # Get rule metrics from pipeline_rule_metrics table
    metrics_dao = MetricsDAO(db)
    rule_metrics_map = await metrics_dao.get_rule_metrics_map(pipeline_id)

    # Track supported/unsupported counts for the response
    supported_count = 0
    unsupported_count = 0

    data = []
    for pipeline_rule in rules:
        rule_name = pipeline_rule.rule.name if pipeline_rule.rule else ""
        repository_name = ""
        repository_id = None
        sigma_uuid = None
        is_supported = True
        unsupported_reason = None
        level = None

        if pipeline_rule.rule:
            if pipeline_rule.rule.repository:
                repository_name = pipeline_rule.rule.repository.name
                repository_id = str(pipeline_rule.rule.repository.id)
            sigma_uuid = str(pipeline_rule.rule.sigma_uuid) if pipeline_rule.rule.sigma_uuid else None
            is_supported = pipeline_rule.rule.is_supported
            unsupported_reason = pipeline_rule.rule.unsupported_reason
            level = pipeline_rule.rule.level

        # Track counts
        if is_supported:
            supported_count += 1
        else:
            unsupported_count += 1

        # Get tagged_events from pipeline_rule_metrics table
        rule_id_str = str(pipeline_rule.rule_id) if pipeline_rule.rule_id else ""
        tagged_events = rule_metrics_map.get(rule_id_str, 0)

        data.append(
            RuleListItem(
                id=str(pipeline_rule.rule_id),
                name=rule_name,
                repository=repository_name,
                repository_id=repository_id,
                sigma_uuid=sigma_uuid,
                enabled=pipeline_rule.enabled,
                tagged_events=tagged_events,
                created=pipeline_rule.created.isoformat() if pipeline_rule.created else "",
                updated=pipeline_rule.updated.isoformat() if pipeline_rule.updated else "",
                is_supported=is_supported,
                unsupported_reason=unsupported_reason,
                level=level,
            )
        )

    logger.info(
        f"Retrieved {len(data)} rules out of {total} total (supported: {supported_count}, unsupported: {unsupported_count})"
    )

    return PipelineRulesListResponse(
        total=total,
        page=page,
        limit=limit,
        sort=sort.value if sort else "",
        offset=0,
        order=order,
        data=data,
        supported_count=supported_count,
        unsupported_count=unsupported_count,
    )


@router.post(
    "/{pipeline_id}/details/rules/{rule_id}/disable",
    response_model=PipelineResponse,
    summary="Disable rule in pipeline",
    responses=errors(UNAUTHORIZED, err(404, "Pipeline or rule not found")),
)
async def disable_rule_in_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    rule_id: str = Path(..., description="Rule UUID"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Disable a specific rule in a pipeline.

    The rule stops being used for event tagging in this pipeline. The change is a
    **hot-reload**: the rule is removed from Kafka (no Flink restart) and the match node
    drops it live. Recorded in the activity log.
    """
    await RulesOrchestrator(db).disable_rule_in_pipeline(UUID(pipeline_id), UUID(rule_id), current_user)
    return PipelineResponse(id=pipeline_id, status=True, message="Rule disabled successfully")


@router.post(
    "/{pipeline_id}/details/rules/{rule_id}/enable",
    response_model=PipelineResponse,
    summary="Enable rule in pipeline",
    responses=errors(UNAUTHORIZED, err(404, "Pipeline or rule not found")),
)
async def enable_rule_in_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    rule_id: str = Path(..., description="Rule UUID"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Enable a specific rule in a pipeline.

    The rule starts being used for event tagging in this pipeline. The change is a
    **hot-reload** (no Flink restart): a supported rule is republished to Kafka and the
    match node applies it live; an unsupported rule is not sent. Recorded in the activity log.
    """
    await RulesOrchestrator(db).enable_rule_in_pipeline(UUID(pipeline_id), UUID(rule_id), current_user)
    return PipelineResponse(id=pipeline_id, status=True, message="Rule enabled successfully")
