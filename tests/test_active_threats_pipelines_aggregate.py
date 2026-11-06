"""The Monitored Threats list's ``Pipelines`` column: batched, never N+1.

The SSE pool-exhaustion incident came from hidden per-row DB work, so the cost of this column is a
contract: two statements for a whole page, whatever the page size. The statements are counted by
driving the REAL DAO code over a fake session rather than by mocking the DAO away — a lookup moved
into a loop must fail this file.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from apps.managers.active_threats import ActiveThreatsManager
from apps.modules.postgre.active_threats import ActiveThreatDAO
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def all(self) -> list[tuple]:
        return self._rows

    def scalar(self):
        return self._rows[0][0] if self._rows else None

    def scalars(self) -> "_Result":
        return self

    def unique(self) -> "_Result":
        return self


class _CountingSession:
    """Records every statement the DAOs execute and replays canned rows in order."""

    def __init__(self, rows: list[list[tuple]]) -> None:
        self.statements: list = []
        self._rows = rows

    async def execute(self, statement, *args, **kwargs) -> _Result:
        self.statements.append(statement)
        index = len(self.statements) - 1
        return _Result(self._rows[index] if index < len(self._rows) else [])


def _threat(tdm_id: str, *, blocked: bool = False) -> MagicMock:
    threat = MagicMock()
    threat.tdm_id = tdm_id
    threat.is_blocked = blocked
    return threat


def _manager(session: _CountingSession) -> ActiveThreatsManager:
    manager = ActiveThreatsManager.__new__(ActiveThreatsManager)
    manager.db = session
    manager.dao = ActiveThreatDAO(session)
    manager.correlation_pipeline_dao = CorrelationPipelineDAO(session)
    return manager


class TestPipelinesColumnIsBatched:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("page_size", [1, 10, 100])
    async def test_query_count_is_constant_as_the_page_grows(self, page_size: int) -> None:
        threats = [_threat(f"AT-{i}") for i in range(page_size)]
        session = _CountingSession(
            [
                [(PIPELINE_A, "tenant-a"), (PIPELINE_B, "tenant-b")],  # list_refs
                [("AT-0", PIPELINE_A)],  # exclusions for the whole page
            ]
        )

        hydrated = await _manager(session)._hydrate_pipelines(threats)

        assert len(session.statements) == 2, "the Pipelines column must not scale with page size"
        assert len(hydrated) == page_size

    @pytest.mark.asyncio
    async def test_each_row_lists_every_pipeline_minus_its_own_exclusions(self) -> None:
        session = _CountingSession(
            [
                [(PIPELINE_A, "tenant-a"), (PIPELINE_B, "tenant-b")],
                [("AT-1", PIPELINE_B)],
            ]
        )

        hydrated = await _manager(session)._hydrate_pipelines([_threat("AT-1"), _threat("AT-2")])

        assert [ref.id for ref in hydrated[0][1]] == [PIPELINE_A]
        assert [ref.id for ref in hydrated[1][1]] == [PIPELINE_A, PIPELINE_B]
        assert hydrated[0][1][0].name == "tenant-a"

    @pytest.mark.asyncio
    async def test_a_blocked_threat_lists_no_pipelines_and_costs_no_extra_query(self) -> None:
        session = _CountingSession([[(PIPELINE_A, "tenant-a")], []])

        hydrated = await _manager(session)._hydrate_pipelines([_threat("AT-1", blocked=True)])

        assert hydrated[0][1] == []
        assert len(session.statements) == 2

    @pytest.mark.asyncio
    async def test_no_correlation_pipelines_means_no_exclusion_lookup_at_all(self) -> None:
        session = _CountingSession([[]])

        hydrated = await _manager(session)._hydrate_pipelines([_threat("AT-1")])

        assert hydrated == [(hydrated[0][0], [])]
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_an_empty_page_touches_the_database_not_at_all(self) -> None:
        session = _CountingSession([])

        assert await _manager(session)._hydrate_pipelines([]) == []
        assert session.statements == []


class TestPipelineScopedListing:
    @pytest.mark.asyncio
    async def test_the_filter_reaches_the_dao_as_the_monitored_rule(self) -> None:
        """``?correlation_pipeline_id=`` narrows the page to that pipeline's monitored set."""
        manager = ActiveThreatsManager.__new__(ActiveThreatsManager)
        manager.dao = AsyncMock()
        manager.dao.get_all.return_value = ([], 0)
        manager.correlation_pipeline_dao = AsyncMock()

        await manager.get_all(
            {"skip": 0, "limit": 10, "search": None, "sort": None, "order": "asc"},
            correlation_pipeline_id=PIPELINE_A,
        )

        assert manager.dao.get_all.await_args.kwargs["correlation_pipeline_id"] == PIPELINE_A

    @pytest.mark.asyncio
    async def test_omitting_it_reads_the_whole_catalogue(self) -> None:
        manager = ActiveThreatsManager.__new__(ActiveThreatsManager)
        manager.dao = AsyncMock()
        manager.dao.get_all.return_value = ([], 0)
        manager.correlation_pipeline_dao = AsyncMock()

        await manager.get_all({"skip": 0, "limit": 10, "search": None, "sort": None, "order": "asc"})

        assert manager.dao.get_all.await_args.kwargs["correlation_pipeline_id"] is None

    @pytest.mark.asyncio
    async def test_the_scoped_page_applies_both_conjuncts(self) -> None:
        """The DAO composes ``monitored_clause``, so a scoped page cannot drift from the rule."""
        session = _CountingSession([[(0,)], []])
        dao = ActiveThreatDAO(session)

        await dao.get_all(correlation_pipeline_id=PIPELINE_A)

        page = str(session.statements[-1])
        assert "is_blocked IS false" in page
        assert "correlation_pipeline_excluded_active_threats" in page

    @pytest.mark.asyncio
    async def test_an_unscoped_page_carries_no_exclusion_subquery(self) -> None:
        session = _CountingSession([[(0,)], []])
        dao = ActiveThreatDAO(session)

        await dao.get_all()

        assert "correlation_pipeline_excluded_active_threats" not in str(session.statements[-1])


class TestExclusionLookups:
    @pytest.mark.asyncio
    async def test_the_reverse_lookup_groups_a_whole_page_in_one_statement(self) -> None:
        session = _CountingSession([[("AT-1", PIPELINE_A), ("AT-1", PIPELINE_B), ("AT-2", PIPELINE_A)]])

        grouped = await ActiveThreatDAO(session).get_excluded_pipeline_ids_by_threat_ids(["AT-1", "AT-2", "AT-3"])

        assert grouped == {"AT-1": {PIPELINE_A, PIPELINE_B}, "AT-2": {PIPELINE_A}}
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_the_wire_lookup_groups_n_pipelines_in_one_statement(self) -> None:
        """One grouped query feeds every republished ``excluded_active_threat_ids``."""
        session = _CountingSession([[(PIPELINE_A, "AT-1"), (PIPELINE_A, "AT-2"), (PIPELINE_B, "AT-9")]])

        grouped = await ActiveThreatDAO(session).get_excluded_threat_ids_by_pipeline_ids([PIPELINE_A, PIPELINE_B])

        assert grouped == {PIPELINE_A: ["AT-1", "AT-2"], PIPELINE_B: ["AT-9"]}
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_empty_inputs_short_circuit_without_a_statement(self) -> None:
        session = _CountingSession([])
        dao = ActiveThreatDAO(session)

        assert await dao.get_excluded_pipeline_ids_by_threat_ids([]) == {}
        assert await dao.get_excluded_threat_ids_by_pipeline_ids([]) == {}
        assert await dao.set_excluded([], [uuid4()], excluded=True, user_id=None) == 0
        assert await dao.get_changed_pipeline_ids(["AT-1"], [], excluded=True) == []
        assert session.statements == []
