"""DAO-level unit tests for ``AuditLogDAO.recent_health``.

These complement ``tests/test_attack_chains_runtime_manager.py`` (which mocks
``recent_health`` and only validates the manager's healthy/degraded/error
mapping). Here we exercise the DAO's *own* logic that the manager relies on:

* the ``WHERE`` clause it builds (source scope, entity_id ``IN``, trailing
  window cutoff, warning/error-only severity filter),
* the ``ORDER BY timestamp DESC`` the ranking loop depends on,
* the ``entity_id is None`` skip,
* worst-severity ranking (``error`` outranks ``warning``) regardless of arrival
  order — including the warning-then-error *upgrade* path,
* and the invariant that ``last_issue_details`` / ``last_event_at`` always
  describe the same event that drives ``worst_severity``.

No live DB is required: a fake session reproduces the Postgres semantics by
reading the bind parameters off the compiled statement and filtering the
seeded rows in Python, then returning them newest-first the way the real query
would.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from apps.core.enums import AuditSeverity
from apps.core.models import AuditLog
from apps.modules.postgre.audit import AuditLogDAO


def _row(
    *,
    entity_id: str | None,
    severity: str,
    timestamp: datetime,
    details: str,
    source: str = "flink",
) -> AuditLog:
    """Build a detached AuditLog instance (no DB session needed)."""
    return AuditLog(
        id=uuid4(),
        timestamp=timestamp,
        action="alert",
        entity_type="attack_chains_runtime",
        entity_id=entity_id,
        entity_name=entity_id,
        details=details,
        source=source,
        severity=severity,
    )


class _FakeResult:
    def __init__(self, rows: list[AuditLog]):
        self._rows = rows

    def scalars(self) -> "_FakeResult":
        return self

    def all(self) -> list[AuditLog]:
        return self._rows


class _FakeSession:
    """Async session stub that mimics the audit_logs query semantics.

    It reads the bind parameters off the compiled ``select`` to learn the
    source, entity_id set, severity allow-list and the trailing-window cutoff
    the DAO built, filters the seeded corpus accordingly, then returns the
    survivors ordered by timestamp DESC (matching the real query's ORDER BY).
    """

    def __init__(self, rows: list[AuditLog]):
        self._rows = rows
        self.captured_sql: str | None = None

    async def execute(self, statement):
        compiled = statement.compile(dialect=postgresql.dialect())
        self.captured_sql = str(compiled)
        params = compiled.params

        # Bind params: scalars (source_1, timestamp_1) plus IN-clause lists
        # (entity_id_1 -> [...], severity_1 -> [...]). Flatten the lists.
        def _flatten(prefix: str) -> set:
            out: set = set()
            for key, value in params.items():
                if not key.startswith(prefix):
                    continue
                if isinstance(value, (list, tuple)):
                    out.update(value)
                else:
                    out.add(value)
            return out

        source = next(v for k, v in params.items() if k.startswith("source"))
        cutoff = next(v for k, v in params.items() if k.startswith("timestamp"))
        entity_ids = _flatten("entity_id")
        severities = _flatten("severity")

        matched = [
            r
            for r in self._rows
            if r.source == source and r.entity_id in entity_ids and r.timestamp >= cutoff and r.severity in severities
        ]
        matched.sort(key=lambda r: r.timestamp, reverse=True)
        return _FakeResult(matched)


def _dao(rows: list[AuditLog]) -> tuple[AuditLogDAO, _FakeSession]:
    session = _FakeSession(rows)
    return AuditLogDAO(session), session  # type: ignore[arg-type]


WINDOW = 300  # 5 minutes

# recent_health computes its window cutoff from datetime.now(UTC) internally,
# so seed rows relative to a "now" captured at import time. The few-ms drift
# between this anchor and the DAO's own now() is negligible against the 300s
# window and the multi-minute "outside window" offsets used below.
_NOW = datetime.now(UTC)


def _inside(seconds_ago: int) -> datetime:
    return _NOW - timedelta(seconds=seconds_ago)


class TestRecentHealthQueryShape:
    @pytest.mark.asyncio
    async def test_empty_entity_ids_short_circuits_without_querying(self) -> None:
        dao, session = _dao([])
        result = await dao.recent_health([], WINDOW)
        assert result == {}
        # No SQL should have been issued for an empty entity list.
        assert session.captured_sql is None

    @pytest.mark.asyncio
    async def test_query_scopes_source_severity_and_orders_newest_first(self) -> None:
        rows = [_row(entity_id="correlation-job", severity="warning", timestamp=_inside(10), details="bp")]
        dao, session = _dao(rows)

        await dao.recent_health(["correlation-job"], WINDOW)

        sql = session.captured_sql or ""
        assert "FROM audit_logs" in sql
        assert "audit_logs.source =" in sql
        assert "audit_logs.entity_id IN" in sql
        assert "audit_logs.timestamp >=" in sql
        assert "audit_logs.severity IN" in sql
        # The ranking loop relies on newest-first ordering.
        assert "ORDER BY audit_logs.timestamp DESC" in sql


class TestRecentHealthRanking:
    @pytest.mark.asyncio
    async def test_warning_only_reports_warning(self) -> None:
        rows = [
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(30), details="bp newer"),
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(90), details="bp older"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job"], WINDOW)

        assert summary["correlation-job"]["worst_severity"] == AuditSeverity.WARNING.value
        # Most recent warning's details surface.
        assert summary["correlation-job"]["last_issue_details"] == "bp newer"
        assert summary["correlation-job"]["last_event_at"] == _inside(30)

    @pytest.mark.asyncio
    async def test_newest_error_reports_error_with_its_own_details(self) -> None:
        rows = [
            _row(entity_id="agent-job", severity="error", timestamp=_inside(10), details="503 newer"),
            _row(entity_id="agent-job", severity="warning", timestamp=_inside(60), details="warn"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["agent-job"], WINDOW)

        assert summary["agent-job"]["worst_severity"] == AuditSeverity.ERROR.value
        assert summary["agent-job"]["last_issue_details"] == "503 newer"
        assert summary["agent-job"]["last_event_at"] == _inside(10)

    @pytest.mark.asyncio
    async def test_older_error_upgrades_severity_and_repoints_last_issue(self) -> None:
        """Regression: a newer warning + an older error must report ``error``
        AND surface the error's details — not the warning's. Previously
        worst_severity was upgraded but last_issue_details/last_event_at kept
        pointing at the newer warning, yielding a misleading diagnostic.
        """
        rows = [
            # Newest row is the lower-severity warning.
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(10), details="warn newest"),
            # Older row is the higher-severity error.
            _row(entity_id="correlation-job", severity="error", timestamp=_inside(120), details="error older"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job"], WINDOW)

        entry = summary["correlation-job"]
        assert entry["worst_severity"] == AuditSeverity.ERROR.value
        # last_issue + timestamp now describe the SAME event that drives health.
        assert entry["last_issue_details"] == "error older"
        assert entry["last_event_at"] == _inside(120)

    @pytest.mark.asyncio
    async def test_error_wins_regardless_of_arrival_order_multiple_errors(self) -> None:
        """With several errors and warnings, worst_severity is error and the
        surfaced issue is the *most recent error* (rows are newest-first)."""
        rows = [
            _row(entity_id="agent-job", severity="warning", timestamp=_inside(5), details="warn newest"),
            _row(entity_id="agent-job", severity="error", timestamp=_inside(40), details="error recent"),
            _row(entity_id="agent-job", severity="error", timestamp=_inside(200), details="error old"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["agent-job"], WINDOW)

        entry = summary["agent-job"]
        assert entry["worst_severity"] == AuditSeverity.ERROR.value
        assert entry["last_issue_details"] == "error recent"
        assert entry["last_event_at"] == _inside(40)


class TestRecentHealthFiltering:
    @pytest.mark.asyncio
    async def test_events_older_than_window_are_excluded(self) -> None:
        rows = [
            # WINDOW is 300s; this error is 10 minutes old → outside the window.
            _row(entity_id="correlation-job", severity="error", timestamp=_inside(600), details="stale error"),
            # This warning is inside the window.
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(30), details="fresh warn"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job"], WINDOW)

        # Stale error aged out → only the fresh warning remains → degraded.
        assert summary["correlation-job"]["worst_severity"] == AuditSeverity.WARNING.value
        assert summary["correlation-job"]["last_issue_details"] == "fresh warn"

    @pytest.mark.asyncio
    async def test_all_events_outside_window_yield_empty_summary(self) -> None:
        rows = [
            _row(entity_id="agent-job", severity="error", timestamp=_inside(900), details="ancient"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["agent-job"], WINDOW)

        assert summary == {}

    @pytest.mark.asyncio
    async def test_non_flink_source_rows_excluded(self) -> None:
        rows = [
            _row(
                entity_id="correlation-job", severity="error", timestamp=_inside(20), details="user-side", source="user"
            ),
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(25), details="flink warn"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job"], WINDOW)

        # The user-source error is filtered out; only the flink warning counts.
        assert summary["correlation-job"]["worst_severity"] == AuditSeverity.WARNING.value
        assert summary["correlation-job"]["last_issue_details"] == "flink warn"

    @pytest.mark.asyncio
    async def test_custom_source_scope(self) -> None:
        rows = [
            _row(entity_id="agent-job", severity="error", timestamp=_inside(20), details="sys err", source="system"),
            _row(entity_id="agent-job", severity="error", timestamp=_inside(20), details="flink err", source="flink"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["agent-job"], WINDOW, source="system")

        assert summary["agent-job"]["last_issue_details"] == "sys err"

    @pytest.mark.asyncio
    async def test_info_severity_rows_excluded(self) -> None:
        rows = [
            _row(entity_id="correlation-job", severity="info", timestamp=_inside(10), details="normal op"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job"], WINDOW)

        # Info is not warning/error → entity omitted entirely.
        assert summary == {}

    @pytest.mark.asyncio
    async def test_entity_id_none_rows_are_skipped(self) -> None:
        rows = [
            _row(entity_id=None, severity="error", timestamp=_inside(10), details="orphan"),
            _row(entity_id="agent-job", severity="warning", timestamp=_inside(15), details="real"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["agent-job"], WINDOW)

        assert None not in summary
        assert set(summary.keys()) == {"agent-job"}

    @pytest.mark.asyncio
    async def test_other_entity_ids_excluded(self) -> None:
        rows = [
            _row(entity_id="some-other-job", severity="error", timestamp=_inside(10), details="not ours"),
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(20), details="ours"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job", "agent-job"], WINDOW)

        assert set(summary.keys()) == {"correlation-job"}

    @pytest.mark.asyncio
    async def test_per_entity_independence_across_both_deployments(self) -> None:
        rows = [
            _row(entity_id="correlation-job", severity="warning", timestamp=_inside(30), details="corr warn"),
            _row(entity_id="agent-job", severity="error", timestamp=_inside(40), details="agent err"),
        ]
        dao, _ = _dao(rows)

        summary = await dao.recent_health(["correlation-job", "agent-job"], WINDOW)

        assert summary["correlation-job"]["worst_severity"] == AuditSeverity.WARNING.value
        assert summary["correlation-job"]["last_issue_details"] == "corr warn"
        assert summary["agent-job"]["worst_severity"] == AuditSeverity.ERROR.value
        assert summary["agent-job"]["last_issue_details"] == "agent err"
