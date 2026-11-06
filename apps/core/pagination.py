"""Pagination utilities for API endpoints."""

from typing import Literal

from fastapi import Query

from apps.core.enums import ActiveThreatSortField, AttackChainSortField, CorrelationPipelineSortField


def get_pagination_params(
    page: int | None = Query(default=1, ge=1, description="Page number"),
    limit: int | None = Query(default=10, ge=1, le=100, description="Items per page"),
    sort: str | None = Query(default=None, description="Field to sort by"),
    offset: int | None = Query(default=0, ge=0, description="Additional offset"),
    order: Literal["asc", "desc"] | None = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search query"),
):
    """Dependency for pagination params with page.

    Returns:
        dict with pagination params and computed skip.
    """
    skip = (page - 1) * limit + offset
    return {
        "page": page,
        "limit": limit,
        "sort": sort,
        "offset": offset,
        "order": order,
        "search": search,
        "skip": skip,
    }


def get_pagination_params_no_page(
    limit: int | None = Query(default=10, ge=1, le=500, description="Items per page"),
    offset: int | None = Query(default=0, ge=0, description="Offset"),
    sort: str | None = Query(default=None, description="Field to sort by"),
    order: Literal["asc", "desc"] | None = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Search query"),
):
    """Dependency for pagination params without page (offset/limit only).

    Returns:
        dict with pagination params and computed skip.
    """
    return {
        "limit": limit,
        "offset": offset,
        "sort": sort,
        "order": order,
        "search": search,
        "skip": offset,
    }


def get_attack_chains_list_params(
    limit: int | None = Query(default=10, ge=1, le=500, description="Items per page"),
    offset: int | None = Query(default=0, ge=0, description="Offset"),
    sort: AttackChainSortField | None = Query(
        default=AttackChainSortField.LAST_SEEN,
        description=(
            "Field to sort by (date fields: `last_seen`, `first_seen`, `created_at`, `updated_at`, `window_start`)"
        ),
    ),
    order: Literal["asc", "desc"] | None = Query(default="asc", description="Sort order"),
    search: str | None = Query(
        default=None,
        description=(
            "Case-insensitive search over hostname, username, status, threat id, chain type, "
            "HoS/threat name, severity, and hit MITRE technique IDs"
        ),
    ),
):
    """Pagination params for attack chains list (offset/limit, typed sort fields)."""
    return {
        "limit": limit,
        "offset": offset,
        "sort": sort.value if sort else None,
        "order": order,
        "search": search,
        "skip": offset,
    }


def get_correlation_pipelines_list_params(
    limit: int | None = Query(default=10, ge=1, le=500, description="Items per page"),
    offset: int | None = Query(default=0, ge=0, description="Offset"),
    sort: CorrelationPipelineSortField | None = Query(
        default=CorrelationPipelineSortField.NAME,
        description="Field to sort by (`attack_chains` sorts on the per-pipeline chain count)",
    ),
    order: Literal["asc", "desc"] | None = Query(default="asc", description="Sort order"),
    search: str | None = Query(default=None, description="Case-insensitive substring match on the pipeline name"),
):
    """Pagination params for the correlation pipelines list (offset/limit, typed sort fields)."""
    return {
        "limit": limit,
        "offset": offset,
        "sort": sort.value if sort else None,
        "order": order,
        "search": search,
        "skip": offset,
    }


def get_active_threats_list_params(
    limit: int | None = Query(default=10, ge=1, le=500, description="Items per page"),
    offset: int | None = Query(default=0, ge=0, description="Offset"),
    sort: ActiveThreatSortField | None = Query(
        default=None,
        description="Field to sort by (`release_date` uses TDM release time with local fallback)",
    ),
    order: Literal["asc", "desc"] | None = Query(default="asc", description="Sort order"),
    search: str | None = Query(
        default=None,
        description="Case-insensitive search over name, severity, and MITRE technique IDs",
    ),
):
    """Pagination params for active threats list (offset/limit, typed sort fields)."""
    return {
        "limit": limit,
        "offset": offset,
        "sort": sort.value if sort else None,
        "order": order,
        "search": search,
        "skip": offset,
    }


def calculate_skip(page: int, limit: int, offset: int = 0) -> int:
    """Compute skip value for pagination.

    Args:
        page: Page number (1-indexed).
        limit: Items per page.
        offset: Additional offset.

    Returns:
        Skip value for DB query.
    """
    return (page - 1) * limit + offset
