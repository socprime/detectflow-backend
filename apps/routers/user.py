"""User management endpoints.

This module provides REST API endpoints for user administration.
All endpoints require admin privileges.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_admin_user
from apps.core.converters import UserConverter
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import FORBIDDEN, UNAUTHORIZED, err, errors
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
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
    responses=errors(UNAUTHORIZED, FORBIDDEN),
)
@router.get("/", include_in_schema=False)
async def get_users(
    _: User = Depends(get_current_admin_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    db: AsyncSession = Depends(get_db),
):
    """
    List user accounts with pagination, search, and sorting.

    **Admin only** (403 otherwise). Supports case-insensitive search across name and email plus
    limit/offset and sort/order. Read-only — no side effects.
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
    responses=errors(
        UNAUTHORIZED,
        FORBIDDEN,
        err(409, "A user with this email already exists"),
    ),
)
@router.post("/", include_in_schema=False)
async def create_user(
    user: UserCreateRequest,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new user account.

    **Admin only** (403 otherwise). The email must be unique (409 on collision). New admin and user
    accounts must change their password on first login; viewer accounts are exempt. A `create` entry
    is written to the activity log.
    """
    manager = UsersManager(db)
    new_user = await manager.create(user, current_user)
    return UserConverter.to_detail(new_user)


@router.get(
    "/{user_id}",
    response_model=UserDetailResponse,
    summary="Get user by ID",
    responses=errors(
        UNAUTHORIZED,
        FORBIDDEN,
        err(404, "User not found"),
    ),
)
@router.get("/{user_id}/", include_in_schema=False)
async def get_user(
    user_id: UUID = Path(..., description="User UUID"),
    _: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Fetch a single user's detail record by ID.

    **Admin only** (403 otherwise); 404 if no user has this UUID. Read-only — no side effects.
    """
    manager = UsersManager(db)
    user = await manager.get_by_id(user_id)
    return UserConverter.to_detail(user)


@router.patch(
    "/{user_id}",
    response_model=UserDetailResponse,
    summary="Update user",
    responses=errors(
        UNAUTHORIZED,
        FORBIDDEN,
        err(404, "User not found"),
        err(409, "Email already used by another user"),
    ),
)
@router.patch("/{user_id}/", include_in_schema=False)
async def update_user(
    user: UserUpdateRequest,
    user_id: UUID = Path(..., description="User UUID"),
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Partially update a user account.

    **Admin only** (403); 404 if the user does not exist; 409 if the new email is already taken by
    another user. All body fields are optional — only provided fields change. A password supplied
    here is hashed before storage, and an `update` entry is written to the activity log.
    """
    manager = UsersManager(db)
    updated_user = await manager.update(user_id, user, current_user)
    return UserConverter.to_detail(updated_user)


@router.delete(
    "/{user_id}",
    status_code=204,
    summary="Delete user",
    responses=errors(
        {204: {"description": "User deleted successfully"}},
        UNAUTHORIZED,
        FORBIDDEN,
        err(404, "User not found"),
    ),
)
@router.delete("/{user_id}/", include_in_schema=False)
async def delete_user(
    user_id: UUID = Path(..., description="User UUID"),
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Permanently delete a user account.

    **Admin only** (403); 404 if the user does not exist. Irreversible. Writes a `delete` entry to
    the activity log and returns 204 with no body.
    """
    manager = UsersManager(db)
    await manager.delete(user_id, current_user)


@router.post(
    "/{user_id}/reset-password",
    response_model=ResetPasswordResponse,
    summary="Reset user password",
    responses=errors(
        UNAUTHORIZED,
        FORBIDDEN,
        err(404, "User not found"),
    ),
)
async def reset_user_password(
    user_id: UUID = Path(..., description="User UUID"),
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Reset a user's password to a generated temporary value.

    **Admin only** (403); 404 if the user does not exist. Generates a random temporary password,
    stores its hash, sets `must_change_password=True` (including for viewers), and writes an
    `update` entry to the activity log. The plaintext temporary password is returned once in the
    response body.
    """
    manager = UsersManager(db)
    temp_password = await manager.reset_password(user_id, current_user)

    return ResetPasswordResponse(
        temporary_password=temp_password,
        message="Password reset successfully. User must change password on next login.",
    )
