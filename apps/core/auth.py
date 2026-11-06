"""Authentication utilities for JWT tokens and password hashing.

This module provides utilities for:
- Password hashing and verification using bcrypt
- JWT token generation and validation
- Password generation and validation
- FastAPI dependencies for authentication
"""

import secrets
import string
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

import bcrypt
from fastapi import Depends, HTTPException, Security, status
from fastapi.concurrency import run_in_threadpool
from fastapi.security import APIKeyCookie, HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import AsyncSessionLocal, get_db
from apps.core.enums import UserRole
from apps.core.models import User
from apps.core.settings import settings
from apps.modules.postgre.user import UserDAO

# Security scheme for FastAPI
security = HTTPBearer()

# Cookie security scheme for SSE endpoints (EventSource cannot send an Authorization
# header). Declared via Security() so it appears in OpenAPI as an apiKey-in-cookie
# scheme with its own Authorize entry; auto_error=False keeps the custom 401 below.
cookie_scheme = APIKeyCookie(
    name="access_token",
    scheme_name="CookieAuth",
    description=(
        "HttpOnly `access_token` JWT cookie set by `POST /api/v1/auth/login`. "
        "Used by SSE endpoints where the `Authorization` header is unavailable."
    ),
    auto_error=False,
)

# Password requirements
MIN_PASSWORD_LENGTH = 8


def hash_password(password: str) -> str:
    """Hash password using bcrypt.

    Args:
        password: Plain text password.

    Returns:
        Hashed password.
    """
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify password against bcrypt hash.

    Args:
        plain_password: Plain text password to verify.
        hashed_password: Hashed password to compare against.

    Returns:
        True if password matches, False otherwise.
    """
    return bcrypt.checkpw(plain_password.encode(), hashed_password.encode())


async def hash_password_async(password: str) -> str:
    """Hash password using bcrypt without blocking the event loop.

    bcrypt is deliberately CPU-expensive (~1s per call on a small vCPU), so in
    async request handlers it must run in the threadpool: calling the sync
    version directly freezes the entire event loop for the duration — every
    concurrent request, SSE stream, and DB-session release stalls behind it,
    which under a login storm cascades into pool exhaustion
    (``QueuePool limit reached``).

    Args:
        password: Plain text password.

    Returns:
        Hashed password.
    """
    return await run_in_threadpool(hash_password, password)


async def verify_password_async(plain_password: str, hashed_password: str) -> bool:
    """Verify password against bcrypt hash without blocking the event loop.

    See ``hash_password_async`` for why bcrypt must not run on the event loop.
    Use this in all async request handlers (login, change-password); the sync
    ``verify_password`` remains for CLI scripts and startup code.

    Args:
        plain_password: Plain text password to verify.
        hashed_password: Hashed password to compare against.

    Returns:
        True if password matches, False otherwise.
    """
    return await run_in_threadpool(verify_password, plain_password, hashed_password)


def generate_temporary_password(length: int = 12) -> str:
    """Generate a secure random temporary password.

    Args:
        length: Password length (default 12).

    Returns:
        Random password string.
    """
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def validate_password(password: str) -> tuple[bool, str | None]:
    """Validate password meets requirements.

    Args:
        password: Password to validate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(password) < MIN_PASSWORD_LENGTH:
        return False, f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
    return True, None


def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create JWT access token.

    Args:
        data: Data to encode in token (typically user_id, email, role).
        expires_delta: Optional expiration time delta.

    Returns:
        Encoded JWT token.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(minutes=settings.jwt_access_token_expire_minutes)
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, settings.jwt_secret_key, algorithm=settings.jwt_algorithm)
    return encoded_jwt


def create_refresh_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """Create JWT refresh token.

    Args:
        data: Data to encode in token (typically user_id).
        expires_delta: Optional expiration time delta.

    Returns:
        Encoded JWT refresh token.
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(UTC) + expires_delta
    else:
        expire = datetime.now(UTC) + timedelta(days=settings.jwt_refresh_token_expire_days)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, settings.jwt_refresh_secret_key, algorithm=settings.jwt_algorithm)
    return encoded_jwt


def decode_refresh_token(token: str) -> dict:
    """Decode and validate JWT refresh token.

    Args:
        token: JWT refresh token to decode.

    Returns:
        Decoded token payload.

    Raises:
        HTTPException: If token is invalid or expired.
    """
    try:
        payload = jwt.decode(token, settings.jwt_refresh_secret_key, algorithms=[settings.jwt_algorithm])
        if payload.get("type") != "refresh":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type",
            )
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        ) from e


def decode_access_token(token: str) -> dict:
    """Decode and validate JWT token.

    Args:
        token: JWT token to decode.

    Returns:
        Decoded token payload.

    Raises:
        HTTPException: If token is invalid or expired.
    """
    try:
        payload = jwt.decode(token, settings.jwt_secret_key, algorithms=[settings.jwt_algorithm])
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        ) from e


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> User:
    """FastAPI dependency to get current authenticated user.

    Args:
        credentials: HTTP Bearer token from request.
        db: Database session.

    Returns:
        Current authenticated user.

    Raises:
        HTTPException: If token is invalid or user not found.
    """
    token = credentials.credentials
    payload = decode_access_token(token)

    user_id: str | None = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_dao = UserDAO(db)
    user = await user_dao.get_by_id(UUID(user_id))
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


async def get_current_active_user(
    current_user: Annotated[User, Depends(get_current_user)],
) -> User:
    """FastAPI dependency to get current active user.

    Args:
        current_user: Current authenticated user.

    Returns:
        Current active user.

    Raises:
        HTTPException: If user is not active.
    """
    if not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user")
    return current_user


# Roles allowed to mutate resources (POST/PUT/PATCH/DELETE and side-effecting actions).
WRITE_ROLES = frozenset({UserRole.ADMIN, UserRole.USER})


async def get_current_admin_user(
    current_user: Annotated[User, Depends(get_current_active_user)],
) -> User:
    """FastAPI dependency to get current admin user.

    Args:
        current_user: Current active user.

    Returns:
        Current admin user.

    Raises:
        HTTPException: If user is not admin.
    """
    if current_user.role != UserRole.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


async def require_write_access(
    current_user: Annotated[User, Depends(get_current_active_user)],
) -> User:
    """FastAPI dependency requiring write access (admin or user; not viewer).

    Use on mutating endpoints. Leave read endpoints on ``get_current_active_user``
    so viewers can still call them.

    Args:
        current_user: Current active user.

    Returns:
        Current user with write permission.

    Raises:
        HTTPException: If user role is not allowed to write (e.g. viewer).
    """
    if current_user.role not in WRITE_ROLES:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Write access required",
        )
    return current_user


async def get_current_user_from_cookie(
    access_token: Annotated[str | None, Security(cookie_scheme)] = None,
) -> User:
    """FastAPI dependency to get current user from HttpOnly cookie.

    Used for SSE endpoints where Authorization header is not supported. The cookie is
    read via the ``CookieAuth`` security scheme so it is visible in OpenAPI/Swagger.

    Unlike ``get_current_user`` (header auth), this dependency does **not** take
    ``Depends(get_db)``. FastAPI runs yield-dependency teardown only after the response
    is fully delivered, and the SSE endpoints that use this dependency return a
    ``StreamingResponse`` that never completes while the client tab is open. A
    ``Depends(get_db)`` here would therefore pin one pooled connection (checked out and
    idle-in-transaction) for the entire lifetime of every SSE stream — the root cause of
    production ``QueuePool limit ... reached`` pool exhaustion. Instead we open a
    short-lived session inside the function body so the connection is held for
    milliseconds (the auth SELECT) and returned to the pool before the stream begins.

    Args:
        access_token: JWT token from HttpOnly cookie.

    Returns:
        Current authenticated user.

    Raises:
        HTTPException: If cookie is missing, token is invalid, or user not found.
    """
    if access_token is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    payload = decode_access_token(access_token)

    user_id: str | None = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )

    async with AsyncSessionLocal() as db:
        user_dao = UserDAO(db)
        user = await user_dao.get_by_id(UUID(user_id))

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    return user


async def get_current_active_user_from_cookie(
    current_user: Annotated[User, Depends(get_current_user_from_cookie)],
) -> User:
    """FastAPI dependency to get current active user from HttpOnly cookie.

    Used for SSE endpoints where Authorization header is not supported.

    Args:
        current_user: Current authenticated user from cookie.

    Returns:
        Current active user.

    Raises:
        HTTPException: If user is not active.
    """
    if not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Inactive user")
    return current_user
