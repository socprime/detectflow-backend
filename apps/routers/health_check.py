"""Health check API: get or refresh platform status from DB.

Endpoints:
- GET /all — returns last saved status of all platforms (read from DB only) and versions
  (detectflow_backend_version, match_node: pipeline name → detectflow_matchnode_version).
- POST /check_now/all — runs health checks for all registered platforms, saves results to DB,
  returns new status and versions.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import get_db
from apps.core.schemas import HealthCheckAllResponse
from apps.services.health_check.main import health_check_service

router = APIRouter(prefix="/api/v1/health_check", tags=["Health Check"])


@router.get(
    "/all",
    response_model=HealthCheckAllResponse,
    summary="Get status of all platforms",
    description="Returns the last saved status of all platforms from the database and versions (detectflow_backend_version, match_node). No health checks are run.",
)
async def get_all_platforms_status(db: AsyncSession = Depends(get_db)) -> HealthCheckAllResponse:
    """Return last saved status of all platforms from the database and versions.

    Reads from health_check table via HealthCheckDAO.get_all(); versions from PipelineDAO.
    No health checks are run.
    """
    return await health_check_service.get_all_platforms_status(db)


@router.post(
    "/check_now/all",
    response_model=HealthCheckAllResponse,
    summary="Update status of all platforms",
    description="Runs health checks for all registered platforms (PostgreSQL, Cloud Repositories, Kafka), saves results to the database, and returns the new status and versions.",
)
async def update_all_platforms_status(db: AsyncSession = Depends(get_db)) -> HealthCheckAllResponse:
    """Run health checks for all registered platforms and save results; return new status and versions.

    Delegates to health_check_service.check_platforms(), then get_versions() and rows_to_platforms_status().
    """
    rows = await health_check_service.check_platforms()
    versions = await health_check_service.get_versions(db)
    return HealthCheckAllResponse(
        platforms=health_check_service.rows_to_platforms_status(rows),
        versions=versions,
    )
