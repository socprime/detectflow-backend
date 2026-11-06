"""Health check API: get or refresh platform status from DB.

Endpoints:
- GET /all — returns last saved status of all platforms (read from DB only) and versions
  (detectflow_backend_version, match_node: pipeline name → detectflow_matchnode_version).
- POST /check_now/all — runs health checks for all registered platforms, saves results to DB,
  returns new status and versions.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED
from apps.core.schemas import HealthCheckAllResponse
from apps.services.health_check.main import health_check_service

router = APIRouter(prefix="/api/v1/health_check", tags=["Health Check"])


@router.get(
    "/all",
    response_model=HealthCheckAllResponse,
    summary="Get status of all platforms",
    responses=UNAUTHORIZED,
)
async def get_all_platforms_status(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> HealthCheckAllResponse:
    """Return the last persisted status of all platforms plus version info; runs no checks.

    Reads cached results from the ``health_check`` table (via ``HealthCheckDAO.get_all()``) and
    versions from ``PipelineDAO`` (``detectflow_backend_version`` and per-pipeline
    ``detectflow_matchnode_version``). This is a pure read — no live health probes are issued.
    """
    return await health_check_service.get_all_platforms_status(db)


@router.post(
    "/check_now/all",
    response_model=HealthCheckAllResponse,
    summary="Re-check status of all platforms",
    responses=UNAUTHORIZED,
)
async def update_all_platforms_status(
    _: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> HealthCheckAllResponse:
    """Run live health checks for every registered platform, persist the results, and return them.

    Probes PostgreSQL, Cloud Repositories, and Kafka via ``health_check_service.check_platforms()``,
    saves each result to the ``health_check`` table, then returns the freshly computed status
    alongside version info (``get_versions()`` + ``rows_to_platforms_status()``). Side effect:
    overwrites the persisted status that ``GET /all`` reads.
    """
    rows = await health_check_service.check_platforms()
    versions = await health_check_service.get_versions(db)
    return HealthCheckAllResponse(
        platforms=health_check_service.rows_to_platforms_status(rows),
        versions=versions,
    )
