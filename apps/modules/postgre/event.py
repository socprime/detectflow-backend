from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Event
from apps.modules.postgre.base import BaseDAO


class EventDAO(BaseDAO[Event]):
    def __init__(self, session: AsyncSession):
        super().__init__(Event, session)

    async def get_recent(self, limit: int = 10):
        result = await self.session.execute(select(Event).order_by(desc(Event.timestamp)).limit(limit))
        return result.scalars().all()
