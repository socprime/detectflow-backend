"""Pipeline management endpoints.

This module provides REST API endpoints for managing ETL pipelines,
including CRUD operations, statistics, and Flink deployment integration.
"""

from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.enums import PipelineRuleSortField, PipelineSortField
from apps.core.exceptions import NotFoundError
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import (
    ErrorResponse,
    PipelineCreateRequest,
    PipelineDetailResponse,
    PipelineListResponse,
    PipelineResponse,
    PipelineRulesListResponse,
    PipelineUpdateRequest,
    RuleListItem,
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
    responses={
        200: {"description": "Paginated list of pipelines"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_pipelines(
    _: User = Depends(get_current_active_user),
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    sort: PipelineSortField | None = Query(default=None, description="Field to sort by"),
    order: Literal["asc", "desc"] = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search by pipeline name"),
    db: AsyncSession = Depends(get_db),
):
    """
    List all ETL pipelines with pagination and filtering.

    Returns pipelines with their associated topics, log sources, repositories,
    and real-time statistics (events tagged/untagged).

    Sort options: name, source_topics, destination_topic, log_source, filters, rules,
    events_tagged, events_untagged, created, enabled
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
    responses={
        201: {"description": "Pipeline created successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Log source, filter, or repository not found"},
        422: {
            "model": ErrorResponse,
            "description": "Validation error: source=destination (infinite loop), or reserved system topic used",
        },
        500: {"model": ErrorResponse, "description": "Failed to create Flink deployment"},
    },
)
async def create_pipeline(
    pipeline: PipelineCreateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new ETL pipeline.

    Creates a pipeline with the specified configuration. If `enabled=true`,
    automatically deploys a Flink job to Kubernetes.

    **Required fields:** name, source_topic, destination_topic, log_source_id

    **Flink deployment:** Created automatically when enabled, reading from `latest` offset.
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.create_pipeline(pipeline, current_user)
    except ResourceNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.get(
    "/{pipeline_id}",
    response_model=PipelineDetailResponse,
    summary="Get pipeline details",
    responses={
        200: {"description": "Pipeline details with Flink status"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline not found"},
    },
)
async def get_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a pipeline.

    Returns full configuration, associated resources, and real-time Flink deployment status.

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
    responses={
        200: {"description": "Pipeline updated successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline not found"},
        422: {
            "model": ErrorResponse,
            "description": "Validation error: source=destination (infinite loop), or reserved system topic used",
        },
        500: {"model": ErrorResponse, "description": "Failed to update Flink deployment"},
    },
)
async def update_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    pipeline: PipelineUpdateRequest = ...,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing pipeline.

    **Hot-reloadable fields** (no Flink restart):
    - `name`, `filters`, `log_source_id`, `repository_ids`, `custom_fields`

    **Restart-required fields** (Flink job recreated):
    - `source_topics`, `destination_topic`, `save_untagged`

    **Not modifiable** (delete and recreate pipeline to change):
    - `resources` (parallelism, memory, CPU) - changing causes Flink checkpoint incompatibility

    **Enable/Disable behavior:**
    - Enable: Creates new Flink deployment (offset=latest) and sends all data to Kafka
    - Disable: Deletes Flink deployment only (data preserved in DB and Kafka for future enable)
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.update_pipeline(UUID(pipeline_id), pipeline, current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.delete(
    "/{pipeline_id}",
    response_model=PipelineResponse,
    summary="Delete pipeline",
    responses={
        200: {"description": "Pipeline deleted successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline not found"},
    },
)
async def delete_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Permanently delete a pipeline.

    **This action is irreversible.** Deletes:
    - Flink deployment from Kubernetes
    - Checkpoints and savepoints
    - Pipeline configuration from database
    - Associated metrics history
    """
    try:
        orchestrator = PipelineOrchestrator(db)
        return await orchestrator.delete_pipeline(UUID(pipeline_id), current_user)
    except PipelineNotFoundError as e:
        raise NotFoundError(str(e)) from e


@router.get(
    "/{pipeline_id}/details/rules",
    response_model=PipelineRulesListResponse,
    summary="List pipeline rules",
    responses={
        200: {"description": "Paginated list of rules with statistics"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline not found"},
    },
)
async def get_pipeline_rules(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    _: User = Depends(get_current_active_user),
    page: int = Query(default=1, ge=1, description="Page number"),
    limit: int = Query(default=10, ge=1, le=100, description="Items per page"),
    sort: PipelineRuleSortField | None = Query(default=None, description="Field to sort by"),
    order: Literal["asc", "desc"] = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search by rule name"),
    tagged_filter: Literal["all", "tagged", "untagged"] = Query(
        default="all", description="Filter by tagged events: all, tagged (>0 matches), untagged (0 matches)"
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    List all rules associated with a pipeline.

    Returns rules with their enabled status and tagged events count.
    Supports pagination and search by rule name.
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
    )

    # Get rule metrics from pipeline_rule_metrics table
    metrics_dao = MetricsDAO(db)
    rule_metrics_map = await metrics_dao.get_rule_metrics_map(pipeline_id)

    data = []
    for pipeline_rule in rules:
        rule_name = pipeline_rule.rule.name if pipeline_rule.rule else ""
        repository_name = ""
        repository_id = None
        if pipeline_rule.rule and pipeline_rule.rule.repository:
            repository_name = pipeline_rule.rule.repository.name
            repository_id = str(pipeline_rule.rule.repository.id)

        # Get tagged_events from pipeline_rule_metrics table
        rule_id_str = str(pipeline_rule.rule_id) if pipeline_rule.rule_id else ""
        tagged_events = rule_metrics_map.get(rule_id_str, 0)

        data.append(
            RuleListItem(
                id=str(pipeline_rule.rule_id),
                name=rule_name,
                repository=repository_name,
                repository_id=repository_id,
                enabled=pipeline_rule.enabled,
                tagged_events=tagged_events,
                created=pipeline_rule.created.isoformat() if pipeline_rule.created else "",
                updated=pipeline_rule.updated.isoformat() if pipeline_rule.updated else "",
            )
        )

    logger.info(f"Retrieved {len(data)} rules out of {total} total")

    return PipelineRulesListResponse(
        total=total,
        page=page,
        limit=limit,
        sort=sort.value if sort else "",
        offset=0,
        order=order,
        data=data,
    )


@router.post(
    "/{pipeline_id}/details/rules/{rule_id}/disable",
    response_model=PipelineResponse,
    summary="Disable rule in pipeline",
    responses={
        200: {"description": "Rule disabled successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline or rule not found"},
    },
)
async def disable_rule_in_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    rule_id: str = Path(..., description="Rule UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Disable a specific rule in a pipeline.

    The rule will no longer be used for event tagging in this pipeline.
    Change is synced to Kafka for hot-reload.
    """
    await RulesOrchestrator(db).disable_rule_in_pipeline(UUID(pipeline_id), UUID(rule_id))
    return PipelineResponse(id=pipeline_id, status=True, message="Rule disabled successfully")


@router.post(
    "/{pipeline_id}/details/rules/{rule_id}/enable",
    response_model=PipelineResponse,
    summary="Enable rule in pipeline",
    responses={
        200: {"description": "Rule enabled successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Pipeline or rule not found"},
    },
)
async def enable_rule_in_pipeline(
    pipeline_id: str = Path(..., description="Pipeline UUID"),
    rule_id: str = Path(..., description="Rule UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Enable a specific rule in a pipeline.

    The rule will be used for event tagging in this pipeline.
    Change is synced to Kafka for hot-reload.
    """
    await RulesOrchestrator(db).enable_rule_in_pipeline(UUID(pipeline_id), UUID(rule_id))
    return PipelineResponse(id=pipeline_id, status=True, message="Rule enabled successfully")
