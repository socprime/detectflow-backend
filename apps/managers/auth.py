"""Authentication business logic orchestrator.

This module handles authentication operations:
- User login validation
- Token generation
- User logout
"""

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import create_access_token, hash_password, verify_password
from apps.core.models import User
from apps.modules.postgre.user import UserDAO


class InvalidCredentialsError(Exception):
    """Raised when user credentials are invalid."""

    pass


class AuthOrchestrator:
    """Orchestrator for authentication operations."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.user_repo = UserDAO(db)

    async def login(self, email: str, password: str) -> tuple[User, str]:
        """Authenticate user and generate access token.

        Args:
            email: User email.
            password: User password.

        Returns:
            Tuple of (User, access_token).

        Raises:
            InvalidCredentialsError: If credentials are invalid.
            NotFoundError: If user not found.
        """
        user = await self.user_repo.get_by_email(email)
        if not user:
            raise InvalidCredentialsError("Invalid email or password")

        if not user.is_active:
            raise InvalidCredentialsError("User account is inactive")

        if not verify_password(password, user.password):
            raise InvalidCredentialsError("Invalid email or password")

        # Generate access token
        token_data = {
            "sub": str(user.id),
            "email": user.email,
            "role": user.role,
        }
        access_token = create_access_token(data=token_data)

        return user, access_token

    def hash_user_password(self, password: str) -> str:
        """Hash password for user creation/update.

        Args:
            password: Plain text password.

        Returns:
            Hashed password.
        """
        return hash_password(password)
