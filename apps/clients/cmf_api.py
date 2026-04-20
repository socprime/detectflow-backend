"""Confluent Manager for Apache Flink (CMF) REST API client.

This module provides a client for interacting with CMF REST API
to manage Flink applications. Uses mTLS for authentication.

CMF API Endpoints:
    POST   /cmf/api/v1/environments/{env}/applications       - Create application
    GET    /cmf/api/v1/environments/{env}/applications/{app} - Get application
    DELETE /cmf/api/v1/environments/{env}/applications/{app} - Delete application
    POST   /cmf/api/v1/environments/{env}/applications/{app}/suspend - Suspend
    POST   /cmf/api/v1/environments/{env}/applications/{app}/start   - Start/Resume
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx

from apps.core.logger import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 60.0  # Increased for slow CMF API responses


@dataclass
class CMFApplicationStatus:
    """Status of a CMF Flink Application."""

    name: str
    state: str  # RUNNING, SUSPENDED, STARTING, STOPPING, FAILED, etc.
    job_id: str | None = None
    error: str | None = None
    lifecycle_state: str | None = None  # Raw lifecycle state from CMF API


class CMFApiClient:
    """Client for Confluent Manager for Apache Flink REST API.

    Uses persistent connection pool for efficient HTTP communication.
    Supports both mTLS authentication (when cert/key provided) and
    plain HTTP for internal cluster communication.

    Attributes:
        base_url: CMF API base URL (e.g., https://cmf.example.com:8080)
        cert: Tuple of (cert_path, key_path) for mTLS, or None
        verify: CA cert path, True for system CA, or False to disable
    """

    def __init__(
        self,
        base_url: str,
        cert_path: str | None = None,
        key_path: str | None = None,
        ca_path: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """Initialize CMF API client.

        Args:
            base_url: CMF API base URL
            cert_path: Path to client certificate file (optional, for mTLS)
            key_path: Path to client private key file (optional, for mTLS)
            ca_path: Path to CA certificate file (optional)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.cert = (cert_path, key_path) if cert_path and key_path else None
        self.verify = ca_path if ca_path else True
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

        logger.info(
            "CMFApiClient initialized",
            extra={
                "base_url": self.base_url,
                "mtls_enabled": self.cert is not None,
            },
        )

    def _get_client_config(self) -> dict:
        """Get HTTP client configuration."""
        return {
            "base_url": self.base_url,
            "cert": self.cert,
            "verify": self.verify,
            "timeout": self.timeout,
            "headers": {"Content-Type": "application/json"},
            "limits": httpx.Limits(max_keepalive_connections=5, max_connections=10),
        }

    @asynccontextmanager
    async def _get_client(self) -> AsyncIterator[httpx.AsyncClient]:
        """Get or create HTTP client with connection pooling."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(**self._get_client_config())
        yield self._client

    async def close(self) -> None:
        """Close the HTTP client and release connections."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def create_application(self, environment: str, manifest: dict) -> dict:
        """Create a new Flink application in CMF.

        Args:
            environment: CMF environment name
            manifest: FlinkApplication manifest (JSON)

        Returns:
            Created application resource from CMF API

        Raises:
            httpx.HTTPStatusError: If API returns error
        """
        app_name = manifest.get("metadata", {}).get("name", "unknown")
        logger.info(f"Creating CMF application: env={environment}, app={app_name}")
        logger.debug(f"CMF manifest: {manifest}")

        async with self._get_client() as client:
            response = await client.post(
                f"/cmf/api/v1/environments/{environment}/applications",
                json=manifest,
            )

            if response.status_code >= 400:
                error_body = response.text[:2000]
                logger.error(
                    f"CMF API error: status={response.status_code}, body={error_body}",
                )
                response.raise_for_status()

            result = response.json()
            logger.info(
                "CMF application created successfully",
                extra={"environment": environment, "app_name": app_name},
            )
            return result

    async def update_application(self, environment: str, app_name: str, manifest: dict) -> dict:
        """Update an existing Flink application in CMF.

        Args:
            environment: CMF environment name
            app_name: Application name to update
            manifest: Updated FlinkApplication manifest (JSON)

        Returns:
            Updated application resource from CMF API

        Raises:
            httpx.HTTPStatusError: If API returns error
        """
        logger.info(
            "Updating CMF application",
            extra={"environment": environment, "app_name": app_name},
        )

        async with self._get_client() as client:
            response = await client.put(
                f"/cmf/api/v1/environments/{environment}/applications/{app_name}",
                json=manifest,
            )

            if response.status_code >= 400:
                error_body = response.text
                logger.error(
                    "CMF API error on update",
                    extra={
                        "status_code": response.status_code,
                        "response_body": error_body[:1000],
                        "environment": environment,
                        "app_name": app_name,
                    },
                )
                response.raise_for_status()

            result = response.json()
            logger.info(
                "CMF application updated successfully",
                extra={"environment": environment, "app_name": app_name},
            )
            return result

    async def get_application(self, environment: str, app_name: str) -> dict | None:
        """Get a Flink application from CMF.

        Args:
            environment: CMF environment name
            app_name: Application name to retrieve

        Returns:
            Application resource dict or None if not found
        """
        logger.debug(
            "Getting CMF application",
            extra={"environment": environment, "app_name": app_name},
        )

        async with self._get_client() as client:
            try:
                response = await client.get(f"/cmf/api/v1/environments/{environment}/applications/{app_name}")
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.debug(
                        "CMF application not found",
                        extra={"environment": environment, "app_name": app_name},
                    )
                    return None
                raise

    async def delete_application(self, environment: str, app_name: str) -> bool:
        """Delete a Flink application from CMF.

        Args:
            environment: CMF environment name
            app_name: Application name to delete

        Returns:
            True if deleted successfully or didn't exist
        """
        logger.info(
            "Deleting CMF application",
            extra={"environment": environment, "app_name": app_name},
        )

        async with self._get_client() as client:
            try:
                response = await client.delete(f"/cmf/api/v1/environments/{environment}/applications/{app_name}")
                response.raise_for_status()
                logger.info(
                    "CMF application deleted successfully",
                    extra={"environment": environment, "app_name": app_name},
                )
                return True
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.warning(
                        "CMF application not found (already deleted)",
                        extra={"environment": environment, "app_name": app_name},
                    )
                    return True
                raise

    async def suspend_application(self, environment: str, app_name: str) -> bool:
        """Suspend a running Flink application.

        Args:
            environment: CMF environment name
            app_name: Application name to suspend

        Returns:
            True if suspended successfully
        """
        logger.info(
            "Suspending CMF application",
            extra={"environment": environment, "app_name": app_name},
        )

        async with self._get_client() as client:
            response = await client.post(f"/cmf/api/v1/environments/{environment}/applications/{app_name}/suspend")
            response.raise_for_status()
            logger.info(
                "CMF application suspended successfully",
                extra={"environment": environment, "app_name": app_name},
            )
            return True

    async def start_application(self, environment: str, app_name: str) -> bool:
        """Start (resume) a suspended Flink application.

        Args:
            environment: CMF environment name
            app_name: Application name to start

        Returns:
            True if started successfully
        """
        logger.info(
            "Starting CMF application",
            extra={"environment": environment, "app_name": app_name},
        )

        async with self._get_client() as client:
            response = await client.post(f"/cmf/api/v1/environments/{environment}/applications/{app_name}/start")
            response.raise_for_status()
            logger.info(
                "CMF application started successfully",
                extra={"environment": environment, "app_name": app_name},
            )
            return True

    async def get_application_status(self, environment: str, app_name: str) -> CMFApplicationStatus | None:
        """Get the status of a Flink application.

        Args:
            environment: CMF environment name
            app_name: Application name

        Returns:
            CMFApplicationStatus or None if not found
        """
        app = await self.get_application(environment, app_name)
        if not app:
            return None

        status = app.get("status", {})
        job_status = status.get("jobStatus", {})

        return CMFApplicationStatus(
            name=app_name,
            state=job_status.get("state", status.get("lifecycleState", "UNKNOWN")),
            job_id=job_status.get("jobId"),
            error=status.get("error"),
            lifecycle_state=status.get("lifecycleState"),
        )
