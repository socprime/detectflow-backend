"""Base class for platform health checkers.

Subclasses must set self.checks: dict[str, callable] mapping check title to an async
function. Each check function must return tuple[HealthCheckStatus, list[str]] (status and
human-readable descriptions). Exceptions in a check are caught and reported as ERROR.
"""

import datetime

from apps.core.enums import HealthCheckStatus
from apps.core.logger import get_logger
from apps.core.schemas import HealthCheckSingleCheck

logger = get_logger(__name__)


class BaseHealthChecker:
    """Base class for platform health checkers.

    Subclasses must set self.checks: dict[str, callable] mapping check name
    to an async function that returns (HealthCheckStatus, list[str]).

    Contract for each check function:
        async def my_check(self) -> tuple[HealthCheckStatus, list[str]]:
            return HealthCheckStatus.OPERATIONAL, ["Optional message"]
    """

    async def health_check(self) -> list[HealthCheckSingleCheck]:
        """Run all checks for this platform and return their results.

        Iterates over self.checks; each check returns (status, descriptions).
        Exceptions are caught and reported as HealthCheckStatus.ERROR.

        Returns:
            List of HealthCheckSingleCheck (one per entry in self.checks).
        """
        result: list[HealthCheckSingleCheck] = []
        for check_title, check_func in self.checks.items():
            try:
                status, descriptions = await check_func()
                result.append(
                    HealthCheckSingleCheck(
                        status=status,
                        title=check_title,
                        descriptions=descriptions,
                        updated=datetime.datetime.now(datetime.UTC),
                    )
                )
                if status != HealthCheckStatus.OPERATIONAL:
                    logger.warning("Check %s returned %s: %s", check_title, status.value, descriptions)
            except Exception as e:
                logger.exception("Health check %s failed: %s", check_title, e)
                result.append(
                    HealthCheckSingleCheck(
                        status=HealthCheckStatus.ERROR,
                        title=check_title,
                        descriptions=[str(e)],
                        updated=datetime.datetime.now(datetime.UTC),
                    )
                )
        return result
