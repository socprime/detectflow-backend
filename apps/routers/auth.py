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
    hash_password_async,
    validate_password,
    verify_password_async,
)
from apps.core.converters import UserConverter
from apps.core.database import get_db
from apps.core.exceptions import BadRequestError, NotFoundError
from apps.core.models import User
from apps.core.openapi import SET_COOKIE_AUTH, UNAUTHORIZED, err, errors
from apps.core.schemas import (
    ChangePasswordRequest,
    ChangePasswordResponse,
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


@router.post(
    "/login",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    summary="User login",
    responses=errors(
        {200: {"description": "Successfully authenticated", "headers": SET_COOKIE_AUTH}},
        err(401, "Invalid credentials or inactive account"),
    ),
)
async def login(
    login_data: LoginRequest,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """
    Authenticate a user with email and password.

    Returns a JWT access token in the body (send it as `Authorization: Bearer <token>` on
    subsequent requests) and, as a side effect, sets two HttpOnly cookies via `Set-Cookie`:
    `access_token` (authenticates SSE endpoints) and `refresh_token` (rotated by
    `POST /api/v1/auth/refresh`). Successful and failed attempts are recorded in the
    activity log.
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
        user=UserConverter.to_detail(user),
    )


@router.post(
    "/refresh",
    response_model=RefreshResponse,
    status_code=status.HTTP_200_OK,
    summary="Refresh access token",
    responses=errors(
        {200: {"description": "New access token generated", "headers": SET_COOKIE_AUTH}},
        err(401, "Missing, invalid, or expired refresh token, or user not found/inactive"),
    ),
)
async def refresh_token(
    response: Response,
    refresh_token: Annotated[
        str | None,
        Cookie(
            description=(
                "HttpOnly refresh-token JWT set by `POST /api/v1/auth/login` and rotated on each "
                "successful refresh. Read from the request cookies, not a request body."
            )
        ),
    ] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Mint a new access token from the HttpOnly `refresh_token` cookie.

    Issues a fresh `access_token` and a rotated `refresh_token`, both written back via
    `Set-Cookie`. On any failure (missing/invalid/expired token, unknown or inactive user)
    it returns 401 and clears both auth cookies so the browser does not retain stale tokens.
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
        200: {"description": "Successfully logged out", "headers": SET_COOKIE_AUTH},
    },
)
async def logout(
    response: Response,
    refresh_token: Annotated[
        str | None,
        Cookie(
            description=(
                "HttpOnly refresh-token JWT set by `POST /api/v1/auth/login` and rotated on refresh. "
                "Used only to identify the user for activity logging; logout never fails if it is "
                "missing or invalid."
            )
        ),
    ] = None,
    db: AsyncSession = Depends(get_db),
):
    """
    Log the current user out, clearing both auth cookies via `Set-Cookie`.

    Best-effort: does not require a valid access token. It tries to decode the `refresh_token`
    cookie purely to attribute a `logout` entry in the activity log; any decode/lookup failure
    is swallowed. The endpoint always returns 200 and always clears `access_token` and
    `refresh_token`, even when the session is already expired or the token is invalid.
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
    responses=errors(UNAUTHORIZED),
)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user),
):
    """
    Return the authenticated user's own profile.

    Resolves the caller from the Bearer token and returns their detail record (role,
    active flag, password-change requirement). Read-only — no side effects.
    """
    return UserConverter.to_detail(current_user)


@router.patch(
    "/profile",
    response_model=UserDetailResponse,
    summary="Update profile",
    responses=errors(
        UNAUTHORIZED,
        err(404, "User record could not be updated"),
    ),
)
async def update_profile(
    profile_data: ProfileUpdateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update the calling user's own profile (currently only `full_name`).

    Persists the change for the authenticated user and writes an `update` entry to the
    activity log. Does not touch authentication state or cookies.
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

    return UserConverter.to_detail(updated_user)


@router.post(
    "/change-password",
    response_model=ChangePasswordResponse,
    summary="Change password",
    responses=errors(
        UNAUTHORIZED,
        err(
            400,
            "Current password is incorrect; new password fails the length rule (min 8 characters); "
            "new password does not match the confirmation; or new password equals the current one",
        ),
    ),
)
async def change_password(
    password_data: ChangePasswordRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Change the authenticated user's password.

    Verifies the supplied current password, then enforces, in order: the new password is at
    least 8 characters long; it matches the confirmation field; and it differs from the current
    password. On success it clears the user's `must_change_password` flag and records an
    `update` entry in the activity log. Any rule violation returns 400 with the specific reason.
    """
    # Verify current password (bcrypt runs in the threadpool — see core.auth)
    if not await verify_password_async(password_data.current_password, current_user.password):
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
    hashed_password = await hash_password_async(password_data.new_password)
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
