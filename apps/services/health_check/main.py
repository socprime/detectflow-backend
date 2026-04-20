"""Health check service: registry of platform checkers and aggregate status.

Runs platform-specific checks (PostgreSQL, Cloud Repositories, Kafka), persists results
to the health_check table via HealthCheckDAO, and returns HealthCheck model instances.
Also builds the full API response (platforms + versions) for GET/POST health_check endpoints:
versions include detectflow_backend_version and per-pipeline match_node (detectflow_matchnode_version).
"""

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.models import HealthCheck
from apps.core.schemas import (
    HealthCheckAllResponse,
    HealthCheckPlatformStatus,
    HealthCheckSingleCheck,
)
from apps.core.settings import settings
from apps.modules.postgre.health_check import HealthCheckDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.services.health_check.platforms import PLATFORMS
from apps.services.health_check.platforms.base import BaseHealthChecker

logger = get_logger(__name__)

# Max pipelines to include in versions.match_node (avoids unbounded response).
# If there are more pipelines, only the first PIPELINE_VERSIONS_LIMIT are returned (by id order).
PIPELINE_VERSIONS_LIMIT = 10_000


class HealthCheckService:
    """Service for running platform health checks and persisting results.

    Holds a registry of platform checkers (PLATFORMS). Runs checks via each checker's
    health_check() method, serializes results to JSONB, and saves via HealthCheckDAO.
    Also provides get_versions(), rows_to_platforms_status(), and get_all_platforms_status()
    to build the full HealthCheckAllResponse (platforms + versions) for GET/POST endpoints.
    """

    _checkers: dict[str, BaseHealthChecker] = PLATFORMS

    def get_platforms(self) -> list[str]:
        """Return list of registered platform names.

        Returns:
            List of keys from PLATFORMS (e.g. ['PostgreSQL', 'Cloud Repositories', 'Kafka']).
        """
        return list(self._checkers.keys())

    async def check_platforms(self, platforms: list[str] | None = None) -> list[HealthCheck]:
        """Run health checks for given platforms and save results to DB.

        For each platform name, looks up the checker, calls checker.health_check(),
        then create_or_update(name, checks) and commits. Unknown names are skipped with a warning.

        Args:
            platforms: Platform names to check; must match keys in _checkers. If None, all platforms.

        Returns:
            List of HealthCheck model instances (one per platform), after commit.
        """
        result: list[HealthCheck] = []
        if not platforms:
            platforms = self.get_platforms()
        logger.info("Starting health checks for platforms: %s", platforms)
        for platform_name in platforms:
            checker = self._checkers.get(platform_name)
            if checker is None:
                logger.warning("Unknown platform requested: %s", platform_name)
                continue
            hc_result: list[HealthCheckSingleCheck] = await checker.health_check()
            statuses = [c.status.value for c in hc_result]
            logger.info("Platform %s checks completed: %s", platform_name, statuses)
            row = await self._save_results_in_db(platform_name=platform_name, hc_result=hc_result)
            result.append(row)
        logger.info("Health checks completed for %d platform(s)", len(result))
        return result

    async def get_versions(self, session: AsyncSession) -> dict[str, str | dict[str, str | None]]:
        """Build versions dict for HealthCheckAllResponse.

        Returns backend version (detectflow_backend_version) and per-pipeline Flink job
        version (match_node: pipeline name → detectflow_matchnode_version). Pipeline list
        is limited to PIPELINE_VERSIONS_LIMIT to avoid unbounded response size.
        """
        pipelines, _ = await PipelineDAO(session).get_all(limit=PIPELINE_VERSIONS_LIMIT)
        return {
            "detectflow_backend_version": settings.detectflow_backend_version,
            "match_node": {p.name: p.detectflow_matchnode_version for p in pipelines},
        }

    def rows_to_platforms_status(self, rows: list[HealthCheck]) -> list[HealthCheckPlatformStatus]:
        """Map HealthCheck DB rows to list of HealthCheckPlatformStatus for API response."""
        return [
            HealthCheckPlatformStatus(
                name=row.name,
                checks=[HealthCheckSingleCheck.model_validate(c) for c in (row.checks or [])],
                updated=row.updated,
            )
            for row in rows
        ]

    async def get_all_platforms_status(self, session: AsyncSession) -> HealthCheckAllResponse:
        """Return last saved status of all platforms plus versions (for GET /all).

        Reads health_check rows via HealthCheckDAO.get_all(), fetches versions via
        get_versions(session), and builds HealthCheckAllResponse.
        """
        rows = await HealthCheckDAO(session).get_all()
        versions = await self.get_versions(session)
        return HealthCheckAllResponse(
            platforms=self.rows_to_platforms_status(rows),
            versions=versions,
        )

    async def _save_results_in_db(self, platform_name: str, hc_result: list[HealthCheckSingleCheck]) -> HealthCheck:
        """Persist platform check results and return the saved row.

        Serializes hc_result to JSON (model_dump(mode='json')), then calls
        HealthCheckDAO.create_or_update(platform_name, checks) and commits.

        Args:
            platform_name: Platform identifier (e.g. 'PostgreSQL', 'Kafka').
            hc_result: List of HealthCheckSingleCheck to store (serialized to JSONB).

        Returns:
            The created or updated HealthCheck instance after commit.
        """
        async with AsyncSessionLocal() as session:
            row = await HealthCheckDAO(session).create_or_update(
                name=platform_name,
                checks=[health_check.model_dump(mode="json") for health_check in hc_result],
            )
            await session.commit()
            logger.info("Health check results saved for platform: %s", platform_name)
            return row


# Singleton instance
health_check_service = HealthCheckService()
