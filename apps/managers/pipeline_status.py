"""Pipeline status service with Flink REST as primary source.

This module provides a centralized service for getting pipeline status
with caching support. Primary source is Flink REST API with Kubernetes
API as fallback.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from apps.clients.flink_rest import FlinkRestClient
from apps.core.enums import get_status_level
from apps.core.logger import get_logger
from apps.core.schemas import StatusDetails
from apps.providers import FlinkDeploymentStatus, FlinkProvider, get_flink_provider

logger = get_logger(__name__)


@dataclass
class PipelineStatus:
    """Status of a pipeline from Flink or Kubernetes.

    Attributes:
        status: Clean status string (running, failed, disabled, reconciling, etc.)
        job_id: Flink job ID if available
        flink_job_name: Job name from Flink UI
        error: Error message (details only, not in status string)
        source: Where status came from (flink, kubernetes, local)
        cached: Whether this was returned from cache
        job_manager_status: K8s JobManager status (READY, DEPLOYING, ERROR, etc.)
        lifecycle_state: K8s lifecycle state (STABLE, DEPLOYING, etc.)
        warnings: K8s warning messages (scheduling issues, etc.)
    """

    status: str
    job_id: str | None = None
    flink_job_name: str | None = None
    error: str | None = None
    source: str = "flink"
    cached: bool = False
    job_manager_status: str | None = None
    lifecycle_state: str | None = None
    warnings: list[str] | None = None


@dataclass
class _CachedStatus:
    """Internal cache entry with expiration."""

    status: PipelineStatus
    expires_at: datetime


class PipelineStatusManager:
    """Service for getting pipeline status with Flink REST as primary source.

    Features:
    - Flink REST API as primary source (faster, more accurate)
    - Kubernetes API as fallback (when Flink is unavailable)
    - In-memory caching with TTL to reduce load

    Usage:
        status = await pipeline_status_manager.get_status(...)
        print(status.status)  # "running"

        # Or with full details for UI:
        status_str, details = await pipeline_status_manager.get_status_details(...)
    """

    CACHE_TTL_SECONDS = 5
    FLINK_TIMEOUT_SECONDS = 3.0

    def __init__(self, flink_provider: FlinkProvider | None = None):
        self._cache: dict[str, _CachedStatus] = {}
        self._flink = flink_provider or get_flink_provider()

    # =========================================================================
    # Public API
    # =========================================================================

    async def get_status(
        self,
        pipeline_id: str,
        deployment_name: str | None = None,
        enabled: bool = True,
    ) -> PipelineStatus:
        """Get pipeline status with caching.

        Strategy:
        1. If not enabled -> return "disabled"
        2. Resolve deployment_name if not provided
        3. Check cache -> return if valid
        4. Try Flink REST API first
        5. Fallback to Kubernetes API
        6. Cache and return result
        """
        if not enabled:
            return PipelineStatus(status="disabled", source="local")

        if not deployment_name:
            deployment_name = await self._resolve_deployment_name(pipeline_id)
            if not deployment_name:
                return PipelineStatus(
                    status="unknown",
                    error="Deployment not found",
                    source="kubernetes",
                )

        cached = self._get_from_cache(deployment_name)
        if cached:
            cached.cached = True
            return cached

        # Try Flink REST first, fallback to provider API
        status = await self._get_from_flink(pipeline_id)
        if status is None:
            status = await self._get_from_kubernetes(pipeline_id)

        if status:
            self._cache_status(deployment_name, status)
            return status

        return PipelineStatus(status="unknown", source="none")

    async def get_status_details(
        self,
        pipeline_id: str,
        deployment_name: str | None = None,
        enabled: bool = True,
    ) -> tuple[str, StatusDetails | None]:
        """Get pipeline status with detailed diagnostics for UI.

        Returns:
            Tuple of (status_string, StatusDetails or None for disabled)
        """
        status = await self.get_status(pipeline_id, deployment_name, enabled)

        if status.status == "disabled":
            return status.status, None

        # Enrich with K8s diagnostic info if missing (happens when status came from Flink)
        job_manager_status = status.job_manager_status
        lifecycle_state = status.lifecycle_state
        error = status.error

        if not job_manager_status:
            k8s_info = await self._get_k8s_diagnostic_info(pipeline_id)
            job_manager_status = k8s_info.get("job_manager_status")
            lifecycle_state = k8s_info.get("lifecycle_state")
            error = error or k8s_info.get("error")

        # Fetch pod warnings for non-running states
        warnings = status.warnings
        if status.status != "running" and not warnings:
            warnings = await self._get_pod_warnings(pipeline_id)

        # Determine severity level
        level = get_status_level(
            status.status,
            has_warnings=bool(warnings) or job_manager_status == "MISSING",
            has_error=bool(error) or job_manager_status == "ERROR",
        )

        return status.status, StatusDetails(
            level=level,
            job_manager_status=job_manager_status,
            lifecycle_state=lifecycle_state,
            warnings=warnings or [],
            error=error,
            source=status.source,
        )

    def invalidate_cache(self, deployment_name: str | None = None) -> None:
        """Clear cache for specific deployment or all."""
        if deployment_name:
            self._cache.pop(deployment_name, None)
        else:
            self._cache.clear()

    # =========================================================================
    # Status Sources
    # =========================================================================

    async def _get_from_flink(self, pipeline_id: str) -> PipelineStatus | None:
        """Query Flink REST API for job status (primary source)."""
        rest_url = self._flink.get_rest_url(pipeline_id)
        client = FlinkRestClient(rest_url, timeout=self.FLINK_TIMEOUT_SECONDS)

        try:
            job_status = await client.get_job_status()
            if job_status is None:
                logger.debug("No jobs found in Flink", extra={"pipeline_id": pipeline_id})
                return None

            return PipelineStatus(
                status=job_status.state.lower() if job_status.state else "unknown",
                job_id=job_status.job_id,
                flink_job_name=job_status.name,
                source="flink",
            )
        except Exception as e:
            logger.debug(
                "Failed to get status from Flink REST",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )
            return None

    async def _get_from_kubernetes(self, pipeline_id: str) -> PipelineStatus | None:
        """Query Kubernetes API for deployment status (fallback)."""
        try:
            deployment_status: FlinkDeploymentStatus | None = await self._flink.get_deployment_status(pipeline_id)

            if deployment_status is None:
                return PipelineStatus(
                    status="not_found",
                    error="Deployment not found in Kubernetes",
                    source="kubernetes",
                )

            return PipelineStatus(
                status=deployment_status.state,
                job_id=deployment_status.job_id,
                error=deployment_status.error,
                source="kubernetes",
                job_manager_status=deployment_status.job_manager_status,
                lifecycle_state=deployment_status.lifecycle_state,
                warnings=deployment_status.warnings if deployment_status.warnings else None,
            )
        except Exception as e:
            logger.warning(
                "Failed to get status from Kubernetes",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )
            return None

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _get_k8s_diagnostic_info(self, pipeline_id: str) -> dict:
        """Get diagnostic info from K8s (job_manager_status, lifecycle_state, error)."""
        try:
            deployment_status = await self._flink.get_deployment_status(pipeline_id)
            if deployment_status:
                return {
                    "job_manager_status": deployment_status.job_manager_status,
                    "lifecycle_state": deployment_status.lifecycle_state,
                    "error": deployment_status.error,
                }
        except Exception as e:
            logger.debug(
                "Failed to get K8s diagnostic info",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )
        return {}

    async def _get_pod_warnings(self, pipeline_id: str) -> list[str] | None:
        """Get pod warnings from K8s events."""
        try:
            warnings = await asyncio.to_thread(self._flink.get_pod_warnings, pipeline_id, 5)
            if warnings:
                logger.info(
                    "Pipeline has warnings",
                    extra={"pipeline_id": pipeline_id, "warnings_count": len(warnings)},
                )
            return warnings or None
        except Exception as e:
            logger.debug(
                "Failed to get pod warnings",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )
            return None

    async def _resolve_deployment_name(self, pipeline_id: str) -> str | None:
        """Find deployment name via K8s labels."""
        try:
            return await self._flink.find_deployment(pipeline_id)
        except Exception as e:
            logger.debug(
                "Failed to resolve deployment name",
                extra={"pipeline_id": pipeline_id, "error": str(e)},
            )
            return None

    def _get_from_cache(self, deployment_name: str) -> PipelineStatus | None:
        """Get status from cache if still valid."""
        cached = self._cache.get(deployment_name)
        if cached and cached.expires_at > datetime.now(UTC):
            return cached.status
        return None

    def _cache_status(self, deployment_name: str, status: PipelineStatus) -> None:
        """Cache status with TTL."""
        self._cache[deployment_name] = _CachedStatus(
            status=status,
            expires_at=datetime.now(UTC) + timedelta(seconds=self.CACHE_TTL_SECONDS),
        )


# Singleton instance
pipeline_status_manager = PipelineStatusManager()
