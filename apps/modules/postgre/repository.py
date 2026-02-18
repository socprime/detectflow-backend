from typing import Any, Literal
from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Repository, Rule
from apps.modules.postgre.base import BaseDAO


class RepositoryDAO(BaseDAO[Repository]):
    def __init__(self, session: AsyncSession):
        super().__init__(Repository, session)

    async def get_by_id(self, id: UUID) -> Repository | None:
        """Get repository by ID with rules_count calculated."""
        # Calculate rules_count using a subquery
        rules_count_subquery = (
            select(func.count(Rule.id))
            .where(Rule.repository_id == Repository.id)
            .correlate(Repository)
            .scalar_subquery()
            .label("rules_count")
        )

        query = select(Repository, rules_count_subquery).where(Repository.id == id)
        result = await self.session.execute(query)
        row = result.first()

        if row is None:
            return None

        repo = row[0]
        repo.rules_count = row[1] or 0
        return repo

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 1000,
        filters: dict[str, Any] | None = None,
        search: str | None = None,
        search_fields: list[str] | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> tuple[list[Repository], int]:
        """Get all repositories."""
        query = select(Repository)

        # Apply filters
        if filters:
            conditions = []
            for key, value in filters.items():
                if hasattr(Repository, key):
                    conditions.append(getattr(Repository, key) == value)
            if conditions:
                query = query.where(and_(*conditions))

        # Apply search
        if search and search_fields:
            search_conditions = []
            for field in search_fields:
                if hasattr(Repository, field):
                    search_conditions.append(getattr(Repository, field).ilike(f"%{search}%"))
            if search_conditions:
                query = query.where(or_(*search_conditions))

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # Apply sorting
        if sort and hasattr(Repository, sort):
            sort_column = getattr(Repository, sort)
            if order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            # Fallback to id ascending to prevent inconsistent order
            query = query.order_by(Repository.id.asc())

        # Apply pagination
        query = query.offset(skip).limit(limit)

        # Fetch repositories
        result = await self.session.execute(query)
        items = list(result.scalars().all())

        # Calculate rules_count for all repositories in one query
        if items:
            repo_ids = [repo.id for repo in items]
            rules_count_query = (
                select(Rule.repository_id, func.count(Rule.id).label("count"))
                .where(Rule.repository_id.in_(repo_ids))
                .group_by(Rule.repository_id)
            )
            count_result = await self.session.execute(rules_count_query)
            counts_dict = {row[0]: row[1] for row in count_result.all()}

            # Set rules_count on each repository
            for repo in items:
                repo.rules_count = counts_dict.get(repo.id, 0)

        return items, total
