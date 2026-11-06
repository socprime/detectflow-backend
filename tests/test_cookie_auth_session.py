"""Regression tests for the SSE cookie-auth dependency session hygiene (pool exhaustion fix).

``get_current_user_from_cookie`` must NOT hold a pooled DB connection for the lifetime of an
SSE stream. It opens and closes a short-lived ``AsyncSessionLocal()`` session inside the call,
so the connection is returned to the pool before the ``StreamingResponse`` starts. See
``apps/core/auth.py`` and ``tasks/T1_sse_auth_session_leak.md``.
"""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from apps.core import auth
from apps.core.auth import create_access_token, get_current_user_from_cookie


class _FakeSessionCM:
    """Stand-in for ``AsyncSessionLocal()`` recording that the context manager closed."""

    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return SimpleNamespace()  # session object; UserDAO is patched so it is unused

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        self.exited = True
        return False


def test_cookie_auth_dependency_has_no_get_db_param() -> None:
    """The dependency must not declare ``Depends(get_db)`` (that would pin a connection)."""
    params = inspect.signature(get_current_user_from_cookie).parameters
    assert "db" not in params
    assert set(params) == {"access_token"}


async def test_cookie_auth_authenticates_and_closes_session(monkeypatch) -> None:
    """A valid token authenticates and the short-lived session is closed before returning."""
    user_id = uuid4()
    fake_user = SimpleNamespace(id=user_id, email="tester@example.com", is_active=True)

    fake_cm = _FakeSessionCM()
    monkeypatch.setattr(auth, "AsyncSessionLocal", lambda: fake_cm)

    dao = AsyncMock()
    dao.get_by_id.return_value = fake_user
    monkeypatch.setattr(auth, "UserDAO", lambda _db: dao)

    token = create_access_token({"sub": str(user_id), "email": fake_user.email})

    user = await get_current_user_from_cookie(access_token=token)

    assert user is fake_user
    dao.get_by_id.assert_awaited_once_with(user_id)
    # Session was opened and — critically — closed (connection returned to the pool)
    # before the dependency returned, so it is not held for the SSE stream lifetime.
    assert fake_cm.entered is True
    assert fake_cm.exited is True


async def test_cookie_auth_rejects_missing_cookie() -> None:
    """No cookie → 401, without ever opening a session."""
    with pytest.raises(HTTPException) as exc_info:
        await get_current_user_from_cookie(access_token=None)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Not authenticated"


async def test_cookie_auth_rejects_unknown_user(monkeypatch) -> None:
    """Valid token but no matching user → 401, session still closed."""
    user_id = uuid4()
    fake_cm = _FakeSessionCM()
    monkeypatch.setattr(auth, "AsyncSessionLocal", lambda: fake_cm)

    dao = AsyncMock()
    dao.get_by_id.return_value = None
    monkeypatch.setattr(auth, "UserDAO", lambda _db: dao)

    token = create_access_token({"sub": str(user_id)})

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user_from_cookie(access_token=token)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "User not found"
    assert fake_cm.exited is True
