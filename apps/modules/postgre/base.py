from typing import Any, Generic, Literal, TypeVar
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import Base

ModelType = TypeVar("ModelType", bound=Base)


class BaseDAO(Generic[ModelType]):
    def __init__(self, model: type[ModelType], session: AsyncSession):
        self.model = model
        self.session = session

    async def get_by_id(self, id: UUID) -> ModelType | None:
        result = await self.session.execute(select(self.model).where(self.model.id == id))
        return result.scalar_one_or_none()

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 1000,
        filters: dict[str, Any] | None = None,
        search: str | None = None,
        search_fields: list[str] | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> tuple[list[ModelType], int]:
        query = select(self.model)

        # Apply filters
        if filters:
            conditions = []
            for key, value in filters.items():
                if hasattr(self.model, key):
                    conditions.append(getattr(self.model, key) == value)
            if conditions:
                query = query.where(and_(*conditions))

        # Apply search
        if search and search_fields:
            search_conditions = []
            for field in search_fields:
                if hasattr(self.model, field):
                    search_conditions.append(getattr(self.model, field).ilike(f"%{search}%"))
            if search_conditions:
                query = query.where(or_(*search_conditions))

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # Apply sorting
        if sort and hasattr(self.model, sort):
            sort_column = getattr(self.model, sort)
            if order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            # Fallback to id ascending to prevent inconsistent order
            query = query.order_by(self.model.id.asc())

        # Apply pagination
        query = query.offset(skip).limit(limit)

        result = await self.session.execute(query)
        items = result.scalars().all()
        return list(items), total

    async def create(self, **kwargs) -> ModelType:
        instance = self.model(**kwargs)
        self.session.add(instance)
        await self.session.flush()
        await self.session.refresh(instance)
        # Set updated = created on insert when both fields exist
        if hasattr(instance, "created") and hasattr(instance, "updated"):
            if instance.created and not instance.updated:
                instance.updated = instance.created
                await self.session.flush()
        return instance

    async def update(self, id: UUID, **kwargs) -> ModelType | None:
        await self.session.execute(update(self.model).where(self.model.id == id).values(**kwargs))
        await self.session.flush()
        # Fetch the object and refresh it to get database-generated values
        instance = await self.get_by_id(id)
        if instance:
            await self.session.refresh(instance)
        return instance

    async def delete(self, id: UUID) -> bool:
        result = await self.session.execute(delete(self.model).where(self.model.id == id))
        await self.session.flush()
        return result.rowcount > 0

    async def delete_many(self, ids: list[UUID]) -> bool:
        result = await self.session.execute(delete(self.model).where(self.model.id.in_(ids)))
        await self.session.flush()
        return result.rowcount > 0

    async def exists(self, id: UUID) -> bool:
        result = await self.session.execute(
            select(func.count()).select_from(select(self.model).where(self.model.id == id).subquery())
        )
        return (result.scalar() or 0) > 0
