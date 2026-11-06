"""Filter management endpoints.

This module provides REST API endpoints for managing detection filters,
which are used to filter events in ETL pipelines based on YAML-defined rules.

Filters are published to Kafka (same topic as rules) with type="prefilter"
so Flink jobs can consume them for event prefiltering.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Path, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.converters import FilterConverter
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import FORBIDDEN, NOT_FOUND, UNAUTHORIZED, err, errors
from apps.core.pagination import get_pagination_params
from apps.core.schemas import (
    FilterActiveResponse,
    FilterCreateRequest,
    FilterFullResponse,
    FilterListResponse,
    FilterResponse,
    FilterUpdateRequest,
)
from apps.managers.filters import FiltersManager

router = APIRouter(prefix="/api/v1/filters", tags=["Filters"])


@router.get(
    "/active",
    response_model=list[FilterActiveResponse],
    summary="List active filters",
    responses=errors(UNAUTHORIZED),
)
async def get_active_filters(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get filters currently assigned to pipelines.

    Returns only filters that are in use. Useful for filter selectors and usage statistics.
    """
    manager = FiltersManager(db)
    filters = await manager.get_active()
    return [FilterConverter.to_active(f) for f in filters]


@router.get(
    "",
    response_model=FilterListResponse,
    summary="List filters",
    responses=errors(UNAUTHORIZED),
)
async def get_filters(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params),
    db: AsyncSession = Depends(get_db),
):
    """
    List all detection filters with pagination.

    Supports search by name and sorting.
    """
    manager = FiltersManager(db)
    filters, total = await manager.get_all(pagination)

    data = [FilterConverter.to_detail(f) for f in filters]

    return FilterListResponse(
        total=total,
        page=pagination["page"],
        limit=pagination["limit"],
        sort=pagination["sort"] or "",
        offset=pagination["offset"],
        order=pagination["order"],
        data=data,
    )


@router.get(
    "/{filter_id}",
    response_model=FilterFullResponse,
    summary="Get filter by ID",
    responses=errors(UNAUTHORIZED, err(404, "Filter not found")),
)
async def get_filter(
    filter_id: str = Path(..., description="Filter UUID"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a filter.

    Returns full filter configuration including the YAML body for editing.
    """
    manager = FiltersManager(db)
    filter_obj = await manager.get_by_id(UUID(filter_id))
    return FilterConverter.to_full(filter_obj)


@router.post(
    "",
    response_model=FilterResponse,
    status_code=201,
    summary="Create filter",
    responses=errors(UNAUTHORIZED, FORBIDDEN),
)
async def create_filter(
    filter: FilterCreateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new detection filter.

    The filter body must be
    valid YAML. After creation, the filter can be assigned to pipelines, after which
    it is published to Kafka for the match node to consume. Creation is recorded in
    the activity log.
    """
    manager = FiltersManager(db)
    new_filter = await manager.create(filter, current_user)
    return FilterResponse(id=str(new_filter.id), status=True, message="Filter created successfully")


@router.patch(
    "/{filter_id}",
    response_model=FilterResponse,
    summary="Update filter",
    responses=errors(UNAUTHORIZED, NOT_FOUND),
)
async def update_filter(
    filter_id: str = Path(..., description="Filter UUID"),
    filter: FilterUpdateRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing filter.

    Only provided fields are updated. Changes are published to Kafka for every
    pipeline using this filter, so the match node hot-reloads the prefilter with
    no Flink restart. The update is recorded in the activity log.
    """
    manager = FiltersManager(db)
    updated = await manager.update(UUID(filter_id), filter, current_user)
    return FilterResponse(id=str(updated.id), status=True, message="Filter updated successfully")


@router.delete(
    "/{filter_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete filter",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Filter not found"),
        err(409, "Filter is in use by pipelines"),
    ),
)
async def delete_filter(
    filter_id: str = Path(..., description="Filter UUID"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Permanently delete a filter.

    Rejected with 409 if any pipeline still references the filter. On success a
    delete message is published to Kafka so the match node drops the prefilter
    (hot-reload, no Flink restart), stale pipeline references are cleared, and the
    deletion is recorded in the activity log.
    """
    manager = FiltersManager(db)
    await manager.delete(UUID(filter_id), current_user)
