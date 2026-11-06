"""Tests for DB pool observability: /health exposure and high-utilization warning.

The ``get_pool_status`` tests run against the REAL function and a real pool — no hand-built
snapshot dicts — so a broken denominator (e.g. deriving capacity from SQLAlchemy's runtime
``pool.overflow()`` counter, which starts at ``-pool_size``) fails here instead of shipping.
See ``tasks/T4_pool_observability.md``.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.pool import QueuePool

from apps.core import database
from apps.core.error_tracker import ErrorTracker
from apps.core.settings import settings


@pytest.fixture(autouse=True)
def _reset_tracker():
    ErrorTracker.reset("db_pool_high_utilization")
    yield
    ErrorTracker.reset("db_pool_high_utilization")


def _pool_snapshot(utilization: float) -> dict:
    return {
        "pool_size": 20,
        "max_overflow": 30,
        "checked_out": 40,
        "checked_in": 0,
        "total": 40,
        "overflow_current": 20,
        "max_capacity": 50,
        "utilization_percent": utilization,
    }


def test_real_get_pool_status_reports_configured_capacity() -> None:
    """Against the real engine (0 checkouts): capacity is the CONFIGURED limit, utilization ~0.

    Regression: the old implementation used ``pool.size() + pool.overflow()`` — with 0
    checkouts the runtime overflow counter is ``-pool_size``, so it reported
    ``max_capacity == 0`` instead of ``pool_size + max_overflow``.
    """
    status = database.get_pool_status()

    assert status["pool_size"] == settings.database_pool_size
    assert status["max_overflow"] == settings.database_max_overflow
    assert status["max_capacity"] == settings.database_pool_size + settings.database_max_overflow
    assert status["checked_out"] == 0
    assert status["overflow_current"] == 0
    assert status["utilization_percent"] == 0.0


def test_real_get_pool_status_low_utilization_with_one_checkout(monkeypatch) -> None:
    """A real QueuePool with 1 of 50 connections checked out reads ~2%, not ~100%.

    Regression for the exact production symptom: with the runtime ``pool.overflow()`` as the
    denominator, a single checkout reported utilization 100.0 and fired the high-utilization
    warning constantly. Builds a real ``QueuePool`` (fake DBAPI creator, same configured
    sizes as the app engine) and points the module's ``engine`` at it.
    """
    pool = QueuePool(
        lambda: MagicMock(),
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
    )
    monkeypatch.setattr(database, "engine", SimpleNamespace(pool=pool))
    warn = MagicMock()
    monkeypatch.setattr(database.logger, "warning", warn)

    conn = pool.connect()  # 1 real checkout (fires the global checkout listener too)
    try:
        status = database.get_pool_status()

        assert status["checked_out"] == 1
        assert status["max_capacity"] == settings.database_pool_size + settings.database_max_overflow
        assert status["utilization_percent"] < database.POOL_UTILIZATION_WARN_PERCENT
        assert status["utilization_percent"] == pytest.approx(100 / status["max_capacity"], abs=0.1)
        # And critically: one connection in use must NOT trip the high-utilization warning.
        warn.assert_not_called()
    finally:
        conn.close()
        pool.dispose()


def test_warning_fires_above_threshold(monkeypatch) -> None:
    monkeypatch.setattr(database, "get_pool_status", lambda: _pool_snapshot(85.0))
    warn = MagicMock()
    monkeypatch.setattr(database.logger, "warning", warn)

    database._warn_on_high_pool_utilization()

    warn.assert_called_once()
    # Snapshot attached for diagnosis.
    assert warn.call_args.kwargs["extra"]["db_pool"]["utilization_percent"] == 85.0


def test_warning_silent_below_threshold(monkeypatch) -> None:
    monkeypatch.setattr(database, "get_pool_status", lambda: _pool_snapshot(50.0))
    warn = MagicMock()
    monkeypatch.setattr(database.logger, "warning", warn)

    database._warn_on_high_pool_utilization()

    warn.assert_not_called()


def test_warning_is_rate_limited(monkeypatch) -> None:
    monkeypatch.setattr(database, "get_pool_status", lambda: _pool_snapshot(95.0))
    warn = MagicMock()
    monkeypatch.setattr(database.logger, "warning", warn)

    database._warn_on_high_pool_utilization()
    database._warn_on_high_pool_utilization()
    database._warn_on_high_pool_utilization()

    # ErrorTracker cooldown collapses the burst to a single warning.
    warn.assert_called_once()


async def test_health_endpoint_exposes_db_pool(api_client) -> None:
    resp = await api_client.get("/health")
    assert resp.status_code == 200
    body = resp.json()

    assert "db_pool" in body
    db_pool = body["db_pool"]
    for key in ("pool_size", "checked_out", "overflow_current", "max_capacity", "utilization_percent"):
        assert key in db_pool
    # Existing field is intact (additive change).
    assert "database_pool" in body
    assert body["status"] == "healthy"
