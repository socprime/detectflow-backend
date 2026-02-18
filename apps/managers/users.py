"""User management orchestrator.

This module provides the UsersManager class for handling
user lifecycle operations with activity logging.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import generate_temporary_password, hash_password
from apps.core.exceptions import DuplicateEntityError, NotFoundError
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import UserCreateRequest, UserUpdateRequest
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.user import UserDAO

logger = get_logger(__name__)


class UsersManager:
    """Manager for user lifecycle operations."""

    def __init__(self, db: AsyncSession):
        """Initialize manager with database session."""
        self.db = db
        self.user_repo = UserDAO(db)

    async def get_all(
        self,
        pagination: dict,
    ) -> tuple[list[User], int]:
        """Get paginated list of users."""
        return await self.user_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            search_fields=["full_name", "email"],
            sort=pagination["sort"],
            order=pagination["order"],
        )

    async def get_by_id(self, user_id: UUID) -> User:
        """Get user by ID.

        Raises:
            NotFoundError: If user not found
        """
        user = await self.user_repo.get_by_id(user_id)
        if not user:
            raise NotFoundError(f"User with id {user_id} not found")
        return user

    async def create(
        self,
        request: UserCreateRequest,
        admin_user: User,
    ) -> User:
        """Create a new user.

        Args:
            request: User creation request
            admin_user: Admin user performing the action

        Returns:
            Created User instance

        Raises:
            HTTPException: If email already exists
        """
        # Check if user with this email already exists
        existing_user = await self.user_repo.get_by_email(request.email)
        if existing_user:
            raise DuplicateEntityError("User", f"email {request.email}")

        new_user = await self.user_repo.create(
            full_name=request.full_name,
            email=request.email.lower(),
            password=hash_password(request.password),
            is_active=request.is_active,
            role=request.role,
            must_change_password=True,
        )

        await activity_producer.log_action(
            action="create",
            entity_type="user",
            entity_id=str(new_user.id),
            entity_name=new_user.full_name,
            user=admin_user,
            details=f"Created user {new_user.full_name} ({new_user.email})",
        )

        return new_user

    async def update(
        self,
        user_id: UUID,
        request: UserUpdateRequest,
        admin_user: User,
    ) -> User:
        """Update an existing user.

        Args:
            user_id: UUID of user to update
            request: Update request with new values
            admin_user: Admin user performing the action

        Returns:
            Updated User instance

        Raises:
            NotFoundError: If user not found
            HTTPException: If email already exists
        """
        existing_user = await self.user_repo.get_by_id(user_id)
        if not existing_user:
            raise NotFoundError(f"User with id {user_id} not found")

        # Check if email is being updated and if it's already taken
        email_changed = request.email and request.email.lower() != existing_user.email
        if email_changed:
            email_user = await self.user_repo.get_by_email(request.email)
            if email_user:
                raise DuplicateEntityError("User", f"email {request.email}")

        update_data = {}
        if request.full_name is not None:
            update_data["full_name"] = request.full_name
        if request.email is not None:
            update_data["email"] = request.email.lower()
        if request.password is not None:
            update_data["password"] = hash_password(request.password)
        if request.is_active is not None:
            update_data["is_active"] = request.is_active
        if request.role is not None:
            update_data["role"] = request.role

        updated_user = await self.user_repo.update(user_id, **update_data)
        if not updated_user:
            raise NotFoundError(f"Failed to update user with id {user_id}")

        await activity_producer.log_action(
            action="update",
            entity_type="user",
            entity_id=str(updated_user.id),
            entity_name=updated_user.full_name,
            user=admin_user,
            details=f"Updated user {updated_user.full_name}",
        )

        return updated_user

    async def delete(
        self,
        user_id: UUID,
        admin_user: User,
    ) -> bool:
        """Delete a user.

        Args:
            user_id: UUID of user to delete
            admin_user: Admin user performing the action

        Returns:
            True if deleted successfully

        Raises:
            NotFoundError: If user not found
        """
        existing_user = await self.user_repo.get_by_id(user_id)
        if not existing_user:
            raise NotFoundError(f"User with id {user_id} not found")

        user_name = existing_user.full_name
        user_email = existing_user.email

        deleted = await self.user_repo.delete(user_id)
        if not deleted:
            raise NotFoundError(f"Failed to delete user with id {user_id}")

        # Activity logging (was missing!)
        await activity_producer.log_action(
            action="delete",
            entity_type="user",
            entity_id=str(user_id),
            entity_name=user_name,
            user=admin_user,
            details=f"Deleted user {user_name} ({user_email})",
        )

        return True

    async def reset_password(
        self,
        user_id: UUID,
        admin_user: User,
    ) -> str:
        """Reset user's password.

        Args:
            user_id: UUID of user
            admin_user: Admin user performing the action

        Returns:
            Temporary password

        Raises:
            NotFoundError: If user not found
        """
        existing_user = await self.user_repo.get_by_id(user_id)
        if not existing_user:
            raise NotFoundError(f"User with id {user_id} not found")

        temp_password = generate_temporary_password()
        hashed_password = hash_password(temp_password)

        await self.user_repo.update_password(user_id, hashed_password, must_change=True)

        await activity_producer.log_action(
            action="update",
            entity_type="user",
            entity_id=str(user_id),
            entity_name=existing_user.full_name,
            user=admin_user,
            details=f"Reset password for user {existing_user.full_name}",
        )

        return temp_password
