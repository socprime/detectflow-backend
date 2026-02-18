from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import User
from apps.modules.postgre.base import BaseDAO


class UserDAO(BaseDAO[User]):
    def __init__(self, session: AsyncSession):
        super().__init__(User, session)

    async def get_by_email(self, email: str) -> User | None:
        """Get user by email."""
        result = await self.session.execute(select(User).where(User.email == email.lower()))
        return result.scalar_one_or_none()

    async def update_password(self, user_id: UUID, hashed_password: str, must_change: bool = False) -> User | None:
        """Update user password and must_change_password flag."""
        return await self.update(user_id, password=hashed_password, must_change_password=must_change)

    async def count(self) -> int:
        """Get total number of users."""
        result = await self.session.execute(select(func.count()).select_from(User))
        return result.scalar() or 0
