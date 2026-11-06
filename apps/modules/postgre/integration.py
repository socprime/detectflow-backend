from typing import Any, Literal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Integration
from apps.modules.postgre.base import BaseDAO


class IntegrationDAO(BaseDAO[Integration]):
    def __init__(self, session: AsyncSession):
        super().__init__(Integration, session)

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 10,
        search: str | None = None,
        search_fields: list[str] | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[Integration], int]:
        return await super().get_all(
            skip=skip,
            limit=limit,
            search=search,
            search_fields=search_fields or ["name"],
            sort=sort,
            order=order,
            filters=filters,
        )

    async def count_by_type(self) -> dict[str, int]:
        """Count integration profiles grouped by integration_type."""
        result = await self.session.execute(
            select(
                Integration.integration_type,
                func.count(Integration.id),
            ).group_by(Integration.integration_type)
        )
        return {row[0]: row[1] for row in result.all()}

    async def count_for_integration_type(self, integration_type: str) -> int:
        """Return number of profiles for a single integration_type."""
        result = await self.session.execute(
            select(func.count()).select_from(Integration).where(Integration.integration_type == integration_type)
        )
        return int(result.scalar() or 0)

    async def get_first_by_integration_type(self, integration_type: str) -> Integration | None:
        """Return first integration row for the given type, or None."""
        result = await self.session.execute(
            select(Integration).where(Integration.integration_type == integration_type).limit(1)
        )
        return result.scalar_one_or_none()
