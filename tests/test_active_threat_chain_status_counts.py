"""The threat detail's per-pipeline chain-status bar: one grouped query, seven statuses, one total.

The ticket names six statuses; ``AttackChainStatus`` has seven. The API returns all seven zero-filled
and sums all seven into ``attack_chains_total``, so the UI can render six while the total still
reconciles with ``GET /attack_chains``.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from apps.core.schemas import ActiveThreatChainStatusCounts, AttackChainStatus
from apps.managers.active_threats import ActiveThreatsManager, _chain_status_counts
from apps.modules.postgre.attack_chains import AttackChainsDAO

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


def _snapshot(correlation_pipeline_id: UUID, name: str, jobs_status: str) -> SimpleNamespace:
    """One ``PipelineRuntimeSnapshot``-shaped row (``name=`` is a MagicMock keyword, hence this)."""
    return SimpleNamespace(
        correlation_pipeline_id=correlation_pipeline_id,
        name=name,
        jobs_status=jobs_status,
        runtime_state=None,
    )


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def all(self) -> list[tuple]:
        return self._rows


class _CountingSession:
    def __init__(self, rows: list[tuple]) -> None:
        self.statements: list = []
        self._rows = rows

    async def execute(self, statement, *args, **kwargs) -> _Result:
        self.statements.append(statement)
        return _Result(self._rows)


class TestGroupedAggregate:
    @pytest.mark.asyncio
    async def test_one_statement_for_every_threat_and_every_pipeline(self) -> None:
        session = _CountingSession(
            [
                ("AT-1", PIPELINE_A, "new", 3),
                ("AT-1", PIPELINE_B, "confirmed", 1),
                ("AT-2", PIPELINE_A, "false-positive", 2),
            ]
        )

        counts = await AttackChainsDAO(session).get_chain_status_counts_by_active_threat_and_pipeline(["AT-1", "AT-2"])

        assert len(session.statements) == 1
        assert counts[("AT-1", PIPELINE_A)] == {"new": 3}
        assert counts[("AT-1", PIPELINE_B)] == {"confirmed": 1}
        assert counts[("AT-2", PIPELINE_A)] == {"false-positive": 2}

    @pytest.mark.asyncio
    async def test_empty_input_costs_no_statement(self) -> None:
        session = _CountingSession([])

        assert await AttackChainsDAO(session).get_chain_status_counts_by_active_threat_and_pipeline([]) == {}
        assert session.statements == []

    @pytest.mark.asyncio
    async def test_unattributed_chains_are_excluded_and_the_grouping_is_three_deep(self) -> None:
        """A NULL ``correlation_pipeline_id`` belongs to no pipeline column (legacy or orphaned)."""
        session = _CountingSession([])

        await AttackChainsDAO(session).get_chain_status_counts_by_active_threat_and_pipeline(["AT-1"])

        sql = str(session.statements[0])
        assert "correlation_pipeline_id IS NOT NULL" in sql
        assert "GROUP BY" in sql
        group_by = sql.split("GROUP BY")[1]
        for column in ("active_threat_id", "correlation_pipeline_id", "status"):
            assert column in group_by


class TestSeenWindowAggregate:
    @pytest.mark.asyncio
    async def test_one_statement_returns_min_first_seen_and_max_last_seen(self) -> None:
        first_seen = datetime(2026, 1, 1, tzinfo=UTC)
        last_seen = datetime(2026, 1, 4, tzinfo=UTC)
        session = _CountingSession(
            [
                ("AT-1", PIPELINE_A, first_seen, last_seen),
            ]
        )

        windows = await AttackChainsDAO(session).get_chain_seen_window_by_active_threat_and_pipeline(["AT-1"])

        assert len(session.statements) == 1
        assert windows[("AT-1", PIPELINE_A)] == (first_seen, last_seen)

    @pytest.mark.asyncio
    async def test_empty_input_costs_no_statement(self) -> None:
        session = _CountingSession([])

        assert await AttackChainsDAO(session).get_chain_seen_window_by_active_threat_and_pipeline([]) == {}
        assert session.statements == []

    @pytest.mark.asyncio
    async def test_unattributed_chains_are_excluded(self) -> None:
        session = _CountingSession([])

        await AttackChainsDAO(session).get_chain_seen_window_by_active_threat_and_pipeline(["AT-1"])

        sql = str(session.statements[0])
        assert "correlation_pipeline_id IS NOT NULL" in sql
        assert "min(attack_chains.first_seen)" in sql.lower()
        assert "max(attack_chains.last_seen)" in sql.lower()
        group_by = sql.split("GROUP BY")[1]
        assert "status" not in group_by
        assert "active_threat_id" in group_by
        assert "correlation_pipeline_id" in group_by


class TestSevenStatuses:
    def test_every_status_is_present_and_zero_filled(self) -> None:
        counts = _chain_status_counts({"new": 2})

        assert counts.model_dump() == {
            "new": 2,
            "investigating": 0,
            "confirmed": 0,
            "false_positive": 0,
            "resolved": 0,
            "closed": 0,
            "filtered_out": 0,
        }

    def test_the_schema_covers_the_whole_status_vocabulary(self) -> None:
        """If ``AttackChainStatus`` grows an eighth value, this breaks instead of silently dropping it."""
        wire_statuses = {status.replace("-", "_") for status in AttackChainStatus.__args__}

        assert wire_statuses == set(ActiveThreatChainStatusCounts.model_fields)

    def test_hyphenated_wire_statuses_map_onto_the_response_fields(self) -> None:
        counts = _chain_status_counts({"false-positive": 4, "filtered-out": 5})

        assert counts.false_positive == 4
        assert counts.filtered_out == 5

    def test_an_unknown_status_cannot_crash_the_page(self) -> None:
        assert _chain_status_counts({"not-a-status": 9}).model_dump() == ActiveThreatChainStatusCounts().model_dump()


class TestDetailItems:
    @pytest.mark.asyncio
    async def test_total_sums_all_seven_including_filtered_out(self) -> None:
        manager = ActiveThreatsManager.__new__(ActiveThreatsManager)
        manager.dao = AsyncMock()
        manager.dao.get_pipeline_ids_with_exclusion.return_value = [PIPELINE_B]
        manager.chains_dao = AsyncMock()
        manager.chains_dao.get_chain_status_counts_by_active_threat_and_pipeline.return_value = {
            ("AT-1", PIPELINE_A): {"new": 2, "closed": 1, "filtered-out": 3},
        }
        first_seen = datetime(2026, 1, 1, tzinfo=UTC)
        last_seen = datetime(2026, 1, 4, tzinfo=UTC)
        manager.chains_dao.get_chain_seen_window_by_active_threat_and_pipeline.return_value = {
            ("AT-1", PIPELINE_A): (first_seen, last_seen),
        }
        manager.correlation_pipelines = AsyncMock()
        manager.correlation_pipelines.list_runtime_snapshots.return_value = [
            _snapshot(PIPELINE_A, "tenant-a", "starting"),
            _snapshot(PIPELINE_B, "tenant-b", "stopped"),
        ]
        threat = MagicMock(tdm_id="AT-1", is_blocked=False)

        items = await manager._pipeline_monitoring_items(threat)

        assert [item.correlation_pipeline_id for item in items] == [PIPELINE_A, PIPELINE_B]
        assert items[0].attack_chains_total == 6
        assert items[0].attack_chain_counts.filtered_out == 3
        # Every pipeline is listed, monitoring or not — the toggle needs both states.
        assert items[0].monitored is True
        assert items[1].monitored is False
        assert items[1].attack_chains_total == 0
        assert items[0].first_seen == first_seen
        assert items[0].last_seen == last_seen
        assert items[1].first_seen is None
        assert items[1].last_seen is None

    @pytest.mark.asyncio
    async def test_a_blocked_threat_monitors_nowhere(self) -> None:
        manager = ActiveThreatsManager.__new__(ActiveThreatsManager)
        manager.dao = AsyncMock()
        manager.dao.get_pipeline_ids_with_exclusion.return_value = []
        manager.chains_dao = AsyncMock()
        manager.chains_dao.get_chain_status_counts_by_active_threat_and_pipeline.return_value = {}
        manager.chains_dao.get_chain_seen_window_by_active_threat_and_pipeline.return_value = {}
        manager.correlation_pipelines = AsyncMock()
        manager.correlation_pipelines.list_runtime_snapshots.return_value = [
            _snapshot(PIPELINE_A, "tenant-a", "starting"),
        ]

        items = await manager._pipeline_monitoring_items(MagicMock(tdm_id="AT-1", is_blocked=True))

        assert items[0].monitored is False
