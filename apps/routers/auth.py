"""Authentication endpoints.

This module provides REST API endpoints for user authentication:
- Login
- Logout
- Get current user
- Update profile
- Change password
"""

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Cookie, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import (
    create_access_token,
    create_refresh_token,
    decode_refresh_token,
    get_current_active_user,
    hash_password,
    validate_password,
    verify_password,
)
from apps.core.database import get_db
from apps.core.exceptions import BadRequestError, NotFoundError
from apps.core.models import User
from apps.core.schemas import (
    ChangePasswordRequest,
    ChangePasswordResponse,
    ErrorResponse,
    LoginRequest,
    LoginResponse,
    LogoutResponse,
    ProfileUpdateRequest,
    RefreshResponse,
    UserDetailResponse,
)
from apps.core.settings import settings
from apps.managers.auth import AuthOrchestrator, InvalidCredentialsError
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.user import UserDAO

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])

# Cookie params used for clearing auth cookies
_COOKIE_PARAMS = {"httponly": True, "secure": True, "samesite": "strict"}


def _clear_auth_cookies(resp: Response) -> None:
    """Delete access_token and refresh_token cookies from a response."""
    resp.delete_cookie(key="access_token", **_COOKIE_PARAMS)
    resp.delete_cookie(key="refresh_token", **_COOKIE_PARAMS)


def _auth_error_response(detail: str) -> JSONResponse:
    """Create a 401 JSON response that clears auth cookies."""
    resp = JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={"detail": detail},
    )
    _clear_auth_cookies(resp)
    return resp


def _convert_user_to_user_detail_response(user: User) -> UserDetailResponse:
    """Convert User model to UserDetailResponse."""
    return UserDetailResponse(
        id=str(user.id),
        full_name=user.full_name,
        email=user.email,
        is_active=user.is_active,
        role=user.role,
        must_change_password=user.must_change_password,
        created=user.created,
        updated=user.updated,
    )


@router.post(
    "/login",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    summary="User login",
    responses={
        200: {"description": "Successfully authenticated"},
        401: {"model": ErrorResponse, "description": "Invalid credentials"},
        422: {"description": "Validation error"},
    },
)
async def login(
    login_data: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Authenticate user with email and password.

    Returns a JWT access token for subsequent API requests.
    Include the token in the `Authorization` header as `Bearer <token>`.

    Also sets an HttpOnly cookie `access_token` for SSE endpoint authentication.
    """
    auth_orchestrator = AuthOrchestrator(db)

    try:
        user, access_token = await auth_orchestrator.login(email=login_data.email, password=login_data.password)
    except InvalidCredentialsError as e:
        # Log failed login attempt
        await activity_producer.log_action(
            action="login_failed",
            entity_type="user",
            entity_name=login_data.email,
            user=None,
            details=f"Failed login attempt for {login_data.email}",
            source="user",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    # Log successful login
    await activity_producer.log_action(
        action="login",
        entity_type="user",
        entity_id=str(user.id),
        entity_name=user.email,
        user=user,
        details=f"User {user.email} logged in",
        source="user",
    )

    # Create refresh token
    refresh_token = create_refresh_token(data={"sub": str(user.id)})

    # Set HttpOnly cookie for SSE authentication (access token)
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=settings.jwt_access_token_expire_minutes * 60,
    )

    # Set HttpOnly cookie for refresh token
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=settings.jwt_refresh_token_expire_days * 24 * 60 * 60,
    )

    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user=_convert_user_to_user_detail_response(user),
    )


@router.post(
    "/refresh",
    response_model=RefreshResponse,
    status_code=status.HTTP_200_OK,
    summary="Refresh access token",
    responses={
        200: {"description": "New access token generated"},
        401: {"model": ErrorResponse, "description": "Invalid or expired refresh token"},
    },
)
async def refresh_token(
    response: Response,
    refresh_token: Annotated[str | None, Cookie()] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Refresh the access token using a valid refresh token from HttpOnly cookie.

    Returns a new access token with rotated refresh token.
    On any failure, clears auth cookies so the browser doesn't keep stale tokens.
    """
    if refresh_token is None:
        return _auth_error_response("Refresh token not found")

    try:
        payload = decode_refresh_token(refresh_token)
    except HTTPException:
        return _auth_error_response("Invalid or expired refresh token")

    user_id = payload.get("sub")
    if user_id is None:
        return _auth_error_response("Invalid refresh token")

    user_dao = UserDAO(db)
    user = await user_dao.get_by_id(UUID(user_id))
    if user is None:
        return _auth_error_response("User not found")
    if not user.is_active:
        return _auth_error_response("User account is inactive")

    # Create new access token
    new_access_token = create_access_token(data={"sub": str(user.id), "email": user.email, "role": user.role})

    # Create new refresh token (rotation)
    new_refresh_token = create_refresh_token(data={"sub": str(user.id)})

    # Update access_token cookie for SSE
    response.set_cookie(
        key="access_token",
        value=new_access_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=settings.jwt_access_token_expire_minutes * 60,
    )

    # Update refresh_token cookie (rotation)
    response.set_cookie(
        key="refresh_token",
        value=new_refresh_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=settings.jwt_refresh_token_expire_days * 24 * 60 * 60,
    )

    return RefreshResponse(access_token=new_access_token, token_type="bearer")


@router.post(
    "/logout",
    response_model=LogoutResponse,
    status_code=status.HTTP_200_OK,
    summary="User logout",
    responses={
        200: {"description": "Successfully logged out"},
    },
)
async def logout(
    response: Response,
    refresh_token: Annotated[str | None, Cookie()] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Logout the current user and clear all auth cookies.

    Does not require a valid access token — authenticates via refresh_token cookie.
    Always returns 200 and clears cookies, even if the session is already expired.
    Activity is logged on a best-effort basis.
    """
    # Best-effort: try to identify user for activity logging
    if refresh_token:
        try:
            payload = decode_refresh_token(refresh_token)
            user_id = payload.get("sub")
            if user_id:
                user_dao = UserDAO(db)
                user = await user_dao.get_by_id(UUID(user_id))
                if user:
                    await activity_producer.log_action(
                        action="logout",
                        entity_type="user",
                        entity_id=str(user.id),
                        entity_name=user.email,
                        user=user,
                        details=f"User {user.email} logged out",
                        source="user",
                    )
        except Exception:
            pass

    # Always clear cookies
    _clear_auth_cookies(response)

    return LogoutResponse(message="Successfully logged out")


@router.get(
    "/me",
    response_model=UserDetailResponse,
    summary="Get current user",
    responses={
        200: {"description": "Current user information"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user),
):
    """
    Get the authenticated user's profile.

    Returns detailed information about the currently logged-in user,
    including role and account status.
    """
    return _convert_user_to_user_detail_response(current_user)


@router.patch(
    "/profile",
    response_model=UserDetailResponse,
    summary="Update profile",
    responses={
        200: {"description": "Profile updated successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        500: {"model": ErrorResponse, "description": "Internal server error"},
    },
)
async def update_profile(
    profile_data: ProfileUpdateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update the current user's profile.

    Currently only supports updating the full name.
    """
    user_dao = UserDAO(db)
    updated_user = await user_dao.update(current_user.id, full_name=profile_data.full_name)
    if not updated_user:
        raise NotFoundError(f"Failed to update profile for user {current_user.id}")

    await activity_producer.log_action(
        action="update",
        entity_type="user",
        entity_id=str(current_user.id),
        entity_name=current_user.email,
        user=current_user,
        details=f"User {current_user.email} updated profile (name: {profile_data.full_name})",
        source="user",
    )

    return _convert_user_to_user_detail_response(updated_user)


@router.post(
    "/change-password",
    response_model=ChangePasswordResponse,
    summary="Change password",
    responses={
        200: {"description": "Password changed successfully"},
        400: {"model": ErrorResponse, "description": "Invalid password or validation error"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def change_password(
    password_data: ChangePasswordRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Change the current user's password.

    Requires the current password for verification. The new password must:
    - Be at least 8 characters long
    - Contain at least one uppercase letter
    - Contain at least one lowercase letter
    - Contain at least one digit
    - Be different from the current password
    """
    # Verify current password
    if not verify_password(password_data.current_password, current_user.password):
        raise BadRequestError("Current password is incorrect")

    # Validate new password
    is_valid, error_msg = validate_password(password_data.new_password)
    if not is_valid:
        raise BadRequestError(error_msg)

    # Check passwords match
    if password_data.new_password != password_data.confirm_password:
        raise BadRequestError("New password and confirm password do not match")

    # Check new password is different from current
    if password_data.new_password == password_data.current_password:
        raise BadRequestError("New password must be different from current password")

    # Update password
    user_dao = UserDAO(db)
    hashed_password = hash_password(password_data.new_password)
    await user_dao.update_password(current_user.id, hashed_password, must_change=False)

    await activity_producer.log_action(
        action="update",
        entity_type="user",
        entity_id=str(current_user.id),
        entity_name=current_user.email,
        user=current_user,
        details=f"User {current_user.email} changed password",
        source="user",
    )

    return ChangePasswordResponse(message="Password changed successfully")
