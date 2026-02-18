from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Filter, Pipeline
from apps.modules.postgre.base import BaseDAO


class FilterDAO(BaseDAO[Filter]):
    def __init__(self, session: AsyncSession):
        super().__init__(Filter, session)

    async def get_active(self):
        """Get filters that are used in at least one pipeline."""
        # Get all unique filter IDs from pipelines.filters arrays
        # unnest() expands the array, then we select distinct filter IDs
        subquery = (
            select(func.unnest(Pipeline.filters).label("filter_id"))
            .where(Pipeline.filters.isnot(None))
            .distinct()
            .subquery()
        )

        # Join with filters table to get filter details
        result = await self.session.execute(select(Filter).where(Filter.id.in_(select(subquery.c.filter_id))))
        return result.scalars().all()

    async def remove_from_pipelines(self, filter_id: UUID) -> int:
        """Remove filter ID from all pipelines that reference it.

        Uses PostgreSQL array_remove() to remove the filter ID from
        the filters array in all pipelines.

        Args:
            filter_id: UUID of the filter to remove.

        Returns:
            Number of pipelines updated.
        """
        result = await self.session.execute(
            update(Pipeline)
            .where(Pipeline.filters.any(filter_id))
            .values(filters=func.array_remove(Pipeline.filters, filter_id))
        )
        await self.session.flush()
        return result.rowcount
