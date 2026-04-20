"""DAO for health_check table: create or update by platform name, list all."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import HealthCheck


class HealthCheckDAO:
    """DAO for health_check table.

    One row per platform (name unique). checks column stores JSONB array of
    {status, title, descriptions, updated} per sub-check.
    """

    def __init__(self, session: AsyncSession) -> None:
        """Inject the async session for all operations."""
        self.session = session

    async def create_or_update(
        self,
        name: str,
        checks: list[dict],
    ) -> HealthCheck:
        """Create a new health check row or update existing one by name.

        If no row with this name exists, creates HealthCheck(name=name, checks=checks) and adds to session.
        Otherwise updates existing row: row.checks = checks. Flushes and refreshes the row before return.

        Args:
            name: Platform name (e.g. 'PostgreSQL', 'Kafka'). Unique key.
            checks: List of check dicts: [{"status", "title", "descriptions", "updated"}, ...].

        Returns:
            The created or updated HealthCheck instance (after flush/refresh).
        """
        result = await self.session.execute(select(HealthCheck).where(HealthCheck.name == name))
        row = result.scalar_one_or_none()
        if row is None:
            row = HealthCheck(name=name, checks=checks)
            self.session.add(row)
        else:
            row.checks = checks
        await self.session.flush()
        await self.session.refresh(row)
        return row

    async def get_all(self) -> list[HealthCheck]:
        """Return all health check records, ordered by platform name.

        Returns:
            List of HealthCheck rows (one per platform), sorted by name.
        """
        result = await self.session.execute(select(HealthCheck).order_by(HealthCheck.name))
        return list(result.scalars().all())
