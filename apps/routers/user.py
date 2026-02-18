"""User management endpoints.

This module provides REST API endpoints for user administration.
All endpoints require admin privileges.
"""

from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_admin_user
from apps.core.converters import UserConverter
from apps.core.database import get_db
from apps.core.models import User
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    ErrorResponse,
    ResetPasswordResponse,
    UserCreateRequest,
    UserDetailResponse,
    UserListResponse,
    UserUpdateRequest,
)
from apps.managers.users import UsersManager

router = APIRouter(prefix="/api/v1/users", tags=["User Management"])


@router.get(
    "",
    response_model=UserListResponse,
    summary="List users",
    responses={
        200: {"description": "List of users with pagination"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
    },
)
@router.get("/", include_in_schema=False)
async def get_users(
    _: User = Depends(get_current_admin_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    db: AsyncSession = Depends(get_db),
):
    """
    List all users with pagination and filtering.

    **Admin only.** Supports search by name and email, sorting, and pagination.
    """
    manager = UsersManager(db)
    users, total = await manager.get_all(pagination)

    data = [UserConverter.to_detail(u) for u in users]

    return UserListResponse(
        total=total,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"] or "",
        order=pagination["order"],
        data=data,
    )


@router.post(
    "",
    response_model=UserDetailResponse,
    status_code=201,
    summary="Create user",
    responses={
        201: {"description": "User created successfully"},
        409: {"model": ErrorResponse, "description": "Email already exists"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        422: {"description": "Validation error"},
    },
)
@router.post("/", include_in_schema=False)
async def create_user(
    user: UserCreateRequest,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new user account.

    **Admin only.** The new user will be required to change their password on first login.
    """
    manager = UsersManager(db)
    new_user = await manager.create(user, current_user)
    return UserConverter.to_detail(new_user)


@router.get(
    "/{user_id}",
    response_model=UserDetailResponse,
    summary="Get user by ID",
    responses={
        200: {"description": "User details"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        404: {"model": ErrorResponse, "description": "User not found"},
    },
)
@router.get("/{user_id}/", include_in_schema=False)
async def get_user(
    user_id: UUID,
    _: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get user details by ID.

    **Admin only.**
    """
    manager = UsersManager(db)
    user = await manager.get_by_id(user_id)
    return UserConverter.to_detail(user)


@router.patch(
    "/{user_id}",
    response_model=UserDetailResponse,
    summary="Update user",
    responses={
        200: {"description": "User updated successfully"},
        409: {"model": ErrorResponse, "description": "Email already exists"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        404: {"model": ErrorResponse, "description": "User not found"},
        422: {"description": "Validation error"},
    },
)
@router.patch("/{user_id}/", include_in_schema=False)
async def update_user(
    user_id: UUID,
    user: UserUpdateRequest,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update user account.

    **Admin only.** All fields are optional - only provided fields will be updated.
    """
    manager = UsersManager(db)
    updated_user = await manager.update(user_id, user, current_user)
    return UserConverter.to_detail(updated_user)


@router.delete(
    "/{user_id}",
    status_code=204,
    summary="Delete user",
    responses={
        204: {"description": "User deleted successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        404: {"model": ErrorResponse, "description": "User not found"},
    },
)
@router.delete("/{user_id}/", include_in_schema=False)
async def delete_user(
    user_id: UUID,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a user account.

    **Admin only.** This action is irreversible.
    """
    manager = UsersManager(db)
    await manager.delete(user_id, current_user)


@router.post(
    "/{user_id}/reset-password",
    response_model=ResetPasswordResponse,
    summary="Reset user password",
    responses={
        200: {"description": "Password reset successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        404: {"model": ErrorResponse, "description": "User not found"},
    },
)
async def reset_user_password(
    user_id: UUID,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Reset a user's password.

    **Admin only.** Generates a temporary password that the user must change on next login.
    Returns the temporary password in the response.
    """
    manager = UsersManager(db)
    temp_password = await manager.reset_password(user_id, current_user)

    return ResetPasswordResponse(
        temporary_password=temp_password,
        message="Password reset successfully. User must change password on next login.",
    )
