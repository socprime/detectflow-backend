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

from apps.core.auth import require_write_access
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED
from apps.core.schemas import (
    AuditLogsFiltersResponse,
    AuditLogsResponse,
)
from apps.managers.activity import ActivityService
from apps.modules.postgre.audit import AuditLogDAO

# Known filter vocabularies (see apps/core/enums.py: ACTION_SEVERITY_MAP / AuditSeverity, and the
# entity_type values logged across the codebase). The audit list/filters endpoints surface only the
# values actually present in the DB, so these are guidance, not an exhaustive validated allow-list.
_ACTION_VALUES = "login, logout, login_failed, create, read, update, delete, toggle, sync, status_change, alert, error"
_ENTITY_TYPE_VALUES = (
    "pipeline, rule, pipeline_rule, filter, repository, log_source, mapping, user, settings, "
    "integration, attack_chain, attack_chain_configuration, attack_chains_runtime, correlation_job, "
    "high_order_sigma, dashboard, system, database, kafka, kafka_sync, kubernetes, cmf, flink, "
    "flink_api, metrics, monitor, poller"
)
_SEVERITY_VALUES = "info, warning, error"

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1/audit-logs", tags=["Audit Logs"])


@router.get(
    "",
    response_model=AuditLogsResponse,
    summary="List audit logs",
    responses=UNAUTHORIZED,
)
async def get_audit_logs(
    action: list[str] | None = Query(None, description=f"Filter by action type(s). Known values: {_ACTION_VALUES}."),
    entity_type: list[str] | None = Query(
        None, description=f"Filter by entity type(s). Known values: {_ENTITY_TYPE_VALUES}."
    ),
    user_id: str | None = Query(None, description="Filter by the acting user's UUID."),
    severity: list[str] | None = Query(None, description=f"Filter by severity level(s): {_SEVERITY_VALUES}."),
    search: str | None = Query(None, description="Case-insensitive search across entity_name, details, user_email."),
    start_date: datetime | None = Query(None, description="Include entries at or after this ISO-8601 timestamp."),
    end_date: datetime | None = Query(None, description="Include entries at or before this ISO-8601 timestamp."),
    offset: int = Query(0, ge=0, description="Number of entries to skip (pagination)."),
    limit: int = Query(50, ge=1, le=100, description="Maximum entries to return (1–100)."),
    _: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """List audit-log entries with filtering, full-text search, and pagination.

    Read-only view of the activity trail. Filters combine with AND; list-valued filters (``action``,
    ``entity_type``, ``severity``) match any of the supplied values. Results are newest-first and the
    response carries ``total`` for paging. Known vocabularies for the categorical filters are listed
    on each query parameter.
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
    responses=UNAUTHORIZED,
)
async def get_filter_options(
    _: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Return the distinct action types, entity types, and severities currently present in the
    audit log, for populating the filter controls. Read-only; values are derived live from the
    stored rows, so the list reflects only what has actually been logged.
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
        200: {
            "description": (
                "CSV download. Served as `text/csv` with a "
                "`Content-Disposition: attachment; filename=audit_logs_<YYYYmmdd_HHMMSS>.csv` header."
            ),
            "content": {"text/csv": {"schema": {"type": "string"}}},
        },
        **UNAUTHORIZED,
    },
)
async def export_audit_logs(
    action: list[str] | None = Query(None, description=f"Filter by action type(s). Known values: {_ACTION_VALUES}."),
    entity_type: list[str] | None = Query(
        None, description=f"Filter by entity type(s). Known values: {_ENTITY_TYPE_VALUES}."
    ),
    user_id: str | None = Query(None, description="Filter by the acting user's UUID."),
    severity: list[str] | None = Query(None, description=f"Filter by severity level(s): {_SEVERITY_VALUES}."),
    search: str | None = Query(None, description="Case-insensitive search across entity_name, details, user_email."),
    start_date: datetime | None = Query(None, description="Include entries at or after this ISO-8601 timestamp."),
    end_date: datetime | None = Query(None, description="Include entries at or before this ISO-8601 timestamp."),
    _: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Export the filtered audit log as a downloadable CSV file.

    Applies the same filters as ``GET /audit-logs`` but streams all matching rows (capped at 10,000)
    as ``text/csv``. The response sets ``Content-Disposition: attachment`` with a timestamped
    filename so browsers save it directly. Read-only.
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
