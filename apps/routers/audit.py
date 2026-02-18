"""Audit logs REST API endpoints.

This module provides REST API endpoints for viewing and exporting
audit logs - the activity history of user actions and system events.

Endpoints:
- GET /api/v1/audit-logs - List with filtering, search, pagination
- GET /api/v1/audit-logs/export - CSV export
- GET /api/v1/audit-logs/filters - Get available filter options
"""

import csv
import io
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import (
    AuditLogsFiltersResponse,
    AuditLogsResponse,
    ErrorResponse,
)
from apps.managers.activity import ActivityService
from apps.modules.postgre.audit import AuditLogDAO

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/audit-logs", tags=["Audit Logs"])


@router.get(
    "",
    response_model=AuditLogsResponse,
    summary="List audit logs",
    responses={
        200: {"description": "Paginated list of audit logs"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_audit_logs(
    action: list[str] | None = Query(None, description="Filter by action type(s)"),
    entity_type: list[str] | None = Query(None, description="Filter by entity type(s)"),
    user_id: str | None = Query(None, description="Filter by user ID"),
    severity: list[str] | None = Query(None, description="Filter by severity level(s)"),
    search: str | None = Query(None, description="Search in entity_name, details, user_email"),
    start_date: datetime | None = Query(None, description="Filter from this date (ISO format)"),
    end_date: datetime | None = Query(None, description="Filter until this date (ISO format)"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=100, description="Pagination limit"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List audit logs with filtering, search, and pagination.

    Supports filtering by:
    - action: create, update, delete, toggle, error, sync, login (multiple allowed)
    - entity_type: pipeline, rule, filter, repository, topic, node, user (multiple allowed)
    - severity: info, warning, error (multiple allowed)
    - user_id: specific user UUID
    - search: text search in entity_name, details, user_email
    - start_date/end_date: date range filter
    """
    audit_dao = AuditLogDAO(db)

    items, total = await audit_dao.search(
        actions=action,
        entity_types=entity_type,
        user_id=user_id,
        severities=severity,
        search=search,
        start_date=start_date,
        end_date=end_date,
        offset=offset,
        limit=limit,
    )

    data = [ActivityService.from_db(item) for item in items]

    return AuditLogsResponse(
        data=data,
        total=total,
        offset=offset,
        limit=limit,
    )


@router.get(
    "/filters",
    response_model=AuditLogsFiltersResponse,
    summary="Get available filter options",
    responses={
        200: {"description": "Available filter options"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_filter_options(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get available filter options for audit logs UI.

    Returns distinct values for action types, entity types, and severities
    that exist in the database.
    """
    audit_dao = AuditLogDAO(db)

    actions = await audit_dao.get_distinct_actions()
    entity_types = await audit_dao.get_distinct_entity_types()
    severities = await audit_dao.get_distinct_severities()

    return AuditLogsFiltersResponse(
        actions=actions,
        entity_types=entity_types,
        severities=severities,
    )


@router.get(
    "/export",
    summary="Export audit logs to CSV",
    responses={
        200: {"description": "CSV file download", "content": {"text/csv": {}}},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def export_audit_logs(
    action: list[str] | None = Query(None, description="Filter by action type(s)"),
    entity_type: list[str] | None = Query(None, description="Filter by entity type(s)"),
    user_id: str | None = Query(None, description="Filter by user ID"),
    severity: list[str] | None = Query(None, description="Filter by severity level(s)"),
    search: str | None = Query(None, description="Search text"),
    start_date: datetime | None = Query(None, description="Filter from this date"),
    end_date: datetime | None = Query(None, description="Filter until this date"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Export audit logs to CSV file.

    Applies the same filters as the list endpoint but exports
    all matching records (up to 10,000) as a downloadable CSV file.
    """
    audit_dao = AuditLogDAO(db)

    # Get all matching records (up to 10k for safety)
    items, _ = await audit_dao.search(
        actions=action,
        entity_types=entity_type,
        user_id=user_id,
        severities=severity,
        search=search,
        start_date=start_date,
        end_date=end_date,
        offset=0,
        limit=10000,
    )

    # Generate CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Write header
    writer.writerow(
        [
            "Timestamp",
            "Action",
            "Severity",
            "Entity Type",
            "Entity ID",
            "Entity Name",
            "User Email",
            "Details",
            "Source",
        ]
    )

    # Write data
    for item in items:
        writer.writerow(
            [
                item.timestamp.isoformat() if item.timestamp else "",
                item.action,
                item.severity,
                item.entity_type,
                item.entity_id or "",
                item.entity_name or "",
                item.user_email or "",
                item.details or "",
                item.source,
            ]
        )

    output.seek(0)

    # Generate filename with current date
    filename = f"audit_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
