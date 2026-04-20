import asyncio

from apps.clients.tdm_api import TDMAPIClient
from apps.core.database import AsyncSessionLocal
from apps.core.enums import HealthCheckStatus
from apps.modules.postgre.config import ConfigDAO
from apps.services.health_check.platforms.base import BaseHealthChecker

GITHUB_SSH_HOST = "ssh.github.com"
GITHUB_SSH_PORT = 22
CONNECT_TIMEOUT = 5.0


class CloudRepositoriesHealthChecker(BaseHealthChecker):
    """Health checker for cloud repositories: SOC Prime TDM API and GitHub (SSH endpoint) reachability."""

    def __init__(self) -> None:
        """Register checks: API Check SOC Prime TDM, GitHub API check (TCP to ssh.github.com)."""
        self.checks = {
            "API Check SOC Prime TDM": self._check_api_connection,
            "GitHub API check": self._check_github_connection,
        }

    async def _check_api_connection(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check TDM API: config API key exists and get_repositories() succeeds.

        Returns:
            (OPERATIONAL, msg) if API key is set and repositories fetched;
            (NOT_ENABLED, msg) if API key is not configured;
            (ERROR, msg) if request failed.
        """
        async with AsyncSessionLocal() as db:
            api_key = await ConfigDAO(db).get_api_key()
            if not api_key:
                return HealthCheckStatus.NOT_ENABLED, [
                    "API key is not configured. Enable sync in Repositories settings."
                ]
        try:
            await TDMAPIClient(api_key=api_key).get_repositories()
        except Exception as e:
            return HealthCheckStatus.ERROR, [f"Failed to connect to the API: {e}"]
        return HealthCheckStatus.OPERATIONAL, ["Connected to the API"]

    async def _check_github_connection(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if GitHub SSH (ssh.github.com:22) is reachable via TCP.

        Returns:
            (OPERATIONAL, msg) if connection succeeds;
            (ERROR, msg) on timeout or OSError.
        """
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(GITHUB_SSH_HOST, GITHUB_SSH_PORT),
                timeout=CONNECT_TIMEOUT,
            )
            writer.close()
            await writer.wait_closed()
            return HealthCheckStatus.OPERATIONAL, ["Connected to GitHub SSH"]
        except TimeoutError:
            return HealthCheckStatus.ERROR, ["Connection to GitHub timed out"]
        except OSError as e:
            return HealthCheckStatus.ERROR, [f"Cannot reach GitHub: {e}"]
