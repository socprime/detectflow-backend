from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import LogSource
from apps.modules.postgre.base import BaseDAO


class LogSourceDAO(BaseDAO[LogSource]):
    def __init__(self, session: AsyncSession):
        super().__init__(LogSource, session)
