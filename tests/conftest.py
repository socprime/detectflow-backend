"""Pytest fixtures for unit tests."""

import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from apps.core.auth import get_current_active_user, get_current_admin_user, require_write_access
from apps.core.database import get_db
from apps.core.enums import UserRole
from server import app


@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Set up test environment variables before any tests run."""
    # Set required environment variables for Settings
    os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")
    yield


@pytest.fixture(autouse=True)
def _clear_correlation_runtime_status_cache() -> None:
    """Process-local cache must not leak observed runtime between tests."""
    from apps.managers.correlation_runtime_status import correlation_runtime_status_cache

    correlation_runtime_status_cache.invalidate()
    yield
    correlation_runtime_status_cache.invalidate()


@pytest.fixture
async def api_client() -> AsyncIterator[httpx.AsyncClient]:
    """ASGI client against ``server.app`` with auth + DB dependencies overridden.

    No lifespan, no real DB/Kafka — routes run against dependency overrides; tests patch the
    manager layer themselves. Seed pattern for router-level (HTTP-contract) tests.
    """
    # Admin-shaped user: write/admin deps still run when only get_current_active_user is
    # overridden (they read ``role``), and we override them too so router tests stay RBAC-agnostic.
    fake_user = SimpleNamespace(
        id=uuid.uuid4(),
        email="tester@example.com",
        is_active=True,
        role=UserRole.ADMIN,
    )
    app.dependency_overrides[get_current_active_user] = lambda: fake_user
    app.dependency_overrides[require_write_access] = lambda: fake_user
    app.dependency_overrides[get_current_admin_user] = lambda: fake_user
    app.dependency_overrides[get_db] = lambda: None
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def fake_integration():
    """Factory for a saved-integration DB row stand-in (id/name/integration_type/config)."""

    def _make(integration_type: str, config: dict) -> SimpleNamespace:
        return SimpleNamespace(
            id=uuid.uuid4(),
            name=f"{integration_type}-profile",
            integration_type=integration_type,
            config=config,
        )

    return _make


@pytest.fixture
def activity_log(monkeypatch) -> AsyncMock:
    """Patch the integrations manager's activity producer; returns the log mock for asserts."""
    log = AsyncMock()
    monkeypatch.setattr("apps.managers.integrations.activity_producer.log_action", log)
    return log


@pytest.fixture
def fake_chains_dao(monkeypatch, activity_log) -> AsyncMock:
    """Replace ``AttackChainsDAO`` with an AsyncMock holding a default chain context.

    Defaults: hostname ``host-x``, a fixed June-2026 window, no matched rule ids — override
    the return values per test. Activity logging is patched via :func:`activity_log`.
    """
    dao = AsyncMock()
    dao.get_chain_link_context.return_value = (
        "host-x",
        datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC),
        datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC),
    )
    dao.get_distinct_sigma_rule_ids.return_value = []
    monkeypatch.setattr("apps.managers.integrations.AttackChainsDAO", lambda _db: dao)
    return dao


@pytest.fixture
def valid_yaml():
    """Return valid YAML string."""
    return """
key: value
list:
  - item1
  - item2
nested:
  key: nested_value
"""


@pytest.fixture
def invalid_yaml():
    """Return invalid YAML string."""
    return """
key: value
  bad indent: error
    - broken
"""


@pytest.fixture
def valid_uuid():
    """Return valid UUID string."""
    return "550e8400-e29b-41d4-a716-446655440000"


@pytest.fixture
def invalid_uuid():
    """Return invalid UUID string."""
    return "not-a-valid-uuid"


@pytest.fixture
def sample_sigma_rule():
    """Return sample Sigma rule for testing."""
    return """
title: Suspicious PowerShell
status: experimental
logsource:
  product: windows
  service: powershell
detection:
  selection:
    EventID: 4104
    CommandLine|contains:
      - "-enc"
      - "-encoded"
  filter:
    User: SYSTEM
  condition: selection and not filter
"""


@pytest.fixture
def sigma_rule_no_detection():
    """Return Sigma rule without detection section."""
    return """
title: Rule Without Detection
status: experimental
logsource:
  product: windows
"""


@pytest.fixture
def sigma_rule_with_modifiers():
    """Return Sigma rule with field modifiers."""
    return """
title: Rule With Modifiers
detection:
  selection:
    CommandLine|contains|all:
      - "powershell"
      - "-exec"
    TargetFilename|endswith: ".exe"
    User|re: ".*admin.*"
  condition: selection
"""
