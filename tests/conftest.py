"""Pytest fixtures for unit tests."""

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def setup_test_env():
    """Set up test environment variables before any tests run."""
    # Set required environment variables for Settings
    os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test_db")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")
    yield


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
