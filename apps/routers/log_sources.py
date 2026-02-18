"""Log source management endpoints.

This module provides REST API endpoints for managing log sources,
which define parsing configuration and field mapping for ETL pipelines.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.converters import LogSourceConverter
from apps.core.database import get_db
from apps.core.models import User
from apps.core.pagination import get_pagination_params
from apps.core.schemas import (
    ErrorResponse,
    LogSourceActionResponse,
    LogSourceCreateRequest,
    LogSourceListResponse,
    LogSourceResponse,
    LogSourceUpdateRequest,
)
from apps.managers.log_sources import LogSourcesManager

router = APIRouter(prefix="/api/v1/log_sources", tags=["Log Sources"])


@router.get(
    "",
    response_model=LogSourceListResponse,
    summary="List log sources",
    responses={
        200: {"description": "Paginated list of log sources"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_log_sources(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params),
    db: AsyncSession = Depends(get_db),
):
    """
    List all log sources with pagination.

    Returns log sources with their parsing configuration and field mapping.
    """
    manager = LogSourcesManager(db)
    log_sources, total = await manager.get_all(pagination)

    data = [LogSourceConverter.to_response(ls) for ls in log_sources]

    return LogSourceListResponse(
        total=total,
        page=pagination["page"],
        limit=pagination["limit"],
        sort=pagination["sort"] or "",
        offset=pagination["offset"],
        order=pagination["order"],
        data=data,
    )


@router.get(
    "/{log_source_id}",
    response_model=LogSourceResponse,
    summary="Get log source by ID",
    responses={
        200: {"description": "Log source details"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Log source not found"},
    },
)
async def get_log_source(
    log_source_id: str = Path(..., description="Log Source UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a log source.

    Returns full configuration including parsing script, config, and mapping.
    """
    manager = LogSourcesManager(db)
    log_source = await manager.get_by_id(UUID(log_source_id))
    return LogSourceConverter.to_response(log_source)


@router.post(
    "",
    response_model=LogSourceActionResponse,
    status_code=201,
    summary="Create log source",
    responses={
        201: {"description": "Log source created successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def create_log_source(
    log_source: LogSourceCreateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new log source.

    If `parsing_script` is provided, `parsing_config` is automatically generated.
    The log source can then be assigned to pipelines.
    """
    manager = LogSourcesManager(db)
    new_log_source = await manager.create(log_source, current_user)
    return LogSourceActionResponse(id=str(new_log_source.id), status=True, message="Log source created successfully")


@router.patch(
    "/{log_source_id}",
    response_model=LogSourceActionResponse,
    summary="Update log source",
    responses={
        200: {"description": "Log source updated successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Log source not found"},
        422: {"description": "Validation error"},
    },
)
async def update_log_source(
    log_source_id: str = Path(..., description="Log Source UUID"),
    log_source: LogSourceUpdateRequest = ...,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing log source.

    If `parsing_script` is changed, `parsing_config` is regenerated and
    synced to Kafka for all pipelines using this log source (hot-reload).
    """
    manager = LogSourcesManager(db)
    updated = await manager.update(UUID(log_source_id), log_source, current_user)
    return LogSourceActionResponse(id=str(updated.id), status=True, message="Log source updated successfully")


@router.delete(
    "/{log_source_id}",
    response_model=LogSourceActionResponse,
    summary="Delete log source",
    responses={
        200: {"description": "Log source deleted successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Log source not found"},
        409: {"model": ErrorResponse, "description": "Log source is in use by pipelines"},
    },
)
async def delete_log_source(
    log_source_id: str = Path(..., description="Log Source UUID"),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Permanently delete a log source.

    **Note:** Ensure no pipelines are using this log source before deletion.
    """
    manager = LogSourcesManager(db)
    await manager.delete(UUID(log_source_id), current_user)
    return LogSourceActionResponse(id=log_source_id, status=True, message="Log source deleted successfully")
