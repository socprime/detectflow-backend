"""Leader election DB URL parsing — discrete asyncpg kwargs, not a raw DSN.

``asyncpg.connect(dsn=...)`` uses ``urllib.parse``. A ``[`` in the username or password is
treated as an IPv6 host (``Invalid IPv6 URL``), so election never succeeds and the
metrics/audit consumers never start. SQLAlchemy's pool uses ``make_url`` and is unaffected.
"""

from unittest.mock import AsyncMock, MagicMock
from urllib.parse import parse_qs, urlsplit

import pytest

from apps.core.leader import LeaderElector, _asyncpg_connect_kwargs


def test_urllib_rejects_unmatched_bracket_in_password() -> None:
    """Pin the urllib failure the old ``dsn=`` path hit in production."""
    dsn = "postgresql://app:pass[word@db.internal:5432/detectflow"
    with pytest.raises(ValueError, match="Invalid IPv6 URL"):
        urlsplit(dsn)


def test_brackets_in_password_become_discrete_kwargs() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:p[ass]word@db.internal:5432/detectflow")

    assert kwargs == {
        "host": "db.internal",
        "port": 5432,
        "user": "app",
        "password": "p[ass]word",
        "database": "detectflow",
    }
    assert "dsn" not in kwargs


def test_unmatched_bracket_in_password_does_not_raise() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass[word@db.internal:5432/detectflow")

    assert kwargs["password"] == "pass[word"
    assert kwargs["host"] == "db.internal"


def test_brackets_in_username() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://u[ser]:p[ass]@db:5432/df")

    assert kwargs["user"] == "u[ser]"
    assert kwargs["password"] == "p[ass]"
    assert kwargs["host"] == "db"


def test_percent_encoded_userinfo_is_decoded() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:p%5Bass%5Dword%40x@db:5432/df")

    assert kwargs["password"] == "p[ass]word@x"


def test_ipv6_host_without_userinfo_brackets() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@[::1]:5432/df")

    assert kwargs["host"] == "::1"
    assert kwargs["port"] == 5432


def test_missing_port_is_omitted() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db/df")

    assert "port" not in kwargs
    assert kwargs["host"] == "db"


def test_sslmode_require_is_passed_as_string() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db:5432/df?sslmode=require")

    assert kwargs["ssl"] == "require"
    assert "dsn" not in kwargs


def test_sslmode_prefer_is_passed_as_string() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db:5432/df?sslmode=prefer")

    assert kwargs["ssl"] == "prefer"


def test_ssl_true_query_sets_ssl_true() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db:5432/df?ssl=true")

    assert kwargs["ssl"] is True


def test_sslmode_disable_sets_ssl_false() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db:5432/df?sslmode=disable")

    assert kwargs["ssl"] is False


def test_no_ssl_query_omits_ssl_kwarg() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@db:5432/df")

    assert "ssl" not in kwargs


def test_sslrootcert_goes_on_userinfo_free_dsn() -> None:
    kwargs = _asyncpg_connect_kwargs(
        "postgresql+asyncpg://app:p[ass]@db:5432/df?sslmode=require&sslrootcert=/etc/ssl/rds.pem"
    )

    assert kwargs["ssl"] == "require"
    assert kwargs["password"] == "p[ass]"
    assert kwargs["dsn"].startswith("postgresql://?")
    assert "@" not in kwargs["dsn"]
    assert parse_qs(urlsplit(kwargs["dsn"]).query)["sslrootcert"] == ["/etc/ssl/rds.pem"]


def test_unix_socket_host_from_query() -> None:
    kwargs = _asyncpg_connect_kwargs("postgresql+asyncpg://app:pass@/df?host=/var/run/postgresql")

    assert kwargs["host"] == "/var/run/postgresql"
    assert kwargs["database"] == "df"
    assert "dsn" not in kwargs


async def test_try_acquire_connects_with_kwargs_not_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "apps.core.leader.settings.database_url",
        "postgresql+asyncpg://app:pass[word@db.internal:5432/detectflow",
    )
    conn = MagicMock()
    conn.is_closed.return_value = False
    conn.fetchval = AsyncMock(return_value=True)
    connect = AsyncMock(return_value=conn)
    monkeypatch.setattr("apps.core.leader.asyncpg.connect", connect)

    elector = LeaderElector(on_promote=AsyncMock(), on_demote=AsyncMock())
    assert await elector._try_acquire() is True

    connect.assert_awaited_once()
    kwargs = connect.await_args.kwargs
    assert "dsn" not in kwargs
    assert kwargs["timeout"] == 10
    assert kwargs["password"] == "pass[word"
    assert kwargs["host"] == "db.internal"
    assert kwargs["user"] == "app"
    assert kwargs["database"] == "detectflow"
    assert kwargs["port"] == 5432
