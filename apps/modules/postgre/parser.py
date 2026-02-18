from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Parser
from apps.managers.parser import parser_manager
from apps.modules.postgre.base import BaseDAO


class ParserDAO(BaseDAO[Parser]):
    def __init__(self, session: AsyncSession):
        super().__init__(Parser, session)

    async def create(
        self,
        name: str,
        parser_query: str,
        parser_config: dict[str, Any] | None = None,
    ) -> Parser:
        if not parser_config:
            parser_config = await parser_manager.create_parser_config(parser_query=parser_query)
        return await super().create(name=name, parser_query=parser_query, parser_config=parser_config)
