import asyncio

from apps.core.database import AsyncSessionLocal
from apps.core.enums import HealthCheckStatus
from apps.integrations.providers.socprime_tdm import SocPrimeTdmProvider, TdmApiError
from apps.modules.postgre.config import ConfigDAO
from apps.services.health_check.platforms.base import BaseHealthChecker

GITHUB_HOST = "github.com"
GITHUB_PORT = 443
CONNECT_TIMEOUT = 5.0


class CloudRepositoriesHealthChecker(BaseHealthChecker):
    """Health checker for cloud repositories: SOC Prime TDM API and GitHub (HTTPS) reachability."""

    def __init__(self) -> None:
        """Register checks: API Check SOC Prime TDM, GitHub API check (TCP to github.com:443)."""
        self.checks = {
            "API Check SOC Prime TDM": self._check_api_connection,
            "GitHub API check": self._check_github_connection,
        }

    async def _check_api_connection(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check TDM API via :class:`~apps.integrations.providers.socprime_tdm.SocPrimeTdmProvider`.

        Uses :meth:`~apps.modules.postgre.config.ConfigDAO.get_socprime_tdm_provider_config`
        (Threat Detection Marketplace profile: ``api_key``, optional ``base_url``) and
        ``get_repositories()`` like the rest of the app.

        Returns:
            (OPERATIONAL, msg) if config exists and repositories fetch succeeds;
            (NOT_ENABLED, msg) if integration is missing or API key is unset;
            (ERROR, msg) on transport/HTTP failures (:exc:`TdmApiError` subclasses).
        """
        async with AsyncSessionLocal() as db:
            cfg = await ConfigDAO(db).get_socprime_tdm_provider_config()
            if not cfg:
                return HealthCheckStatus.NOT_ENABLED, [
                    "Threat Detection Marketplace (socprime_tdm) is not configured or API key is missing."
                ]
        try:
            await SocPrimeTdmProvider(cfg).get_repositories()
        except TdmApiError as e:
            return HealthCheckStatus.ERROR, [f"TDM API check failed: {e}"]
        except Exception as e:
            return HealthCheckStatus.ERROR, [f"Failed to connect to the API: {e}"]
        return HealthCheckStatus.OPERATIONAL, ["Connected to TDM API"]

    async def _check_github_connection(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if GitHub HTTPS (github.com:443) is reachable via TCP.

        Returns:
            (OPERATIONAL, msg) if connection succeeds;
            (ERROR, msg) on timeout or OSError.
        """
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(GITHUB_HOST, GITHUB_PORT),
                timeout=CONNECT_TIMEOUT,
            )
            writer.close()
            await writer.wait_closed()
            return HealthCheckStatus.OPERATIONAL, ["Connected to GitHub"]
        except TimeoutError:
            return HealthCheckStatus.ERROR, ["Connection to GitHub timed out"]
        except OSError as e:
            return HealthCheckStatus.ERROR, [f"Cannot reach GitHub: {e}"]
