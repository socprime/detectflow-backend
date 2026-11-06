"""Searching detection rules by Sigma UUID — the "By ID" search type.

``rules.sigma_uuid`` is a native PostgreSQL ``uuid``: there is no ``uuid ~~* text`` operator, so a
plain ILIKE against that column is a runtime error surfacing as a 500. These tests compile the
statements (no database) and pin both the predicate shape and the allow-list that keeps unvalidated
query input away from reflection over the model.

Covered surfaces: ``sigma_uuid_search_condition`` semantics, ``RuleDAO.get_all``
(``GET /api/v1/rules?search_fields=sigma_uuid`` — the Repositories page), ``PipelineRulesDAO``
(the single search box on the pipeline details page) and the rules router contract.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

from sqlalchemy.dialects import postgresql

from apps.core.enums import PipelineRuleSortField, RuleSearchField
from apps.core.schemas import RuleListItem
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO
from apps.modules.postgre.rule import RULE_SEARCH_CONDITIONS, RuleDAO, sigma_uuid_search_condition

FULL_UUID = "1a2b3c4d-5e6f-4a8b-9c0d-1e2f3a4b5c6d"
ORCHESTRATOR_GET_ALL = "apps.managers.rule.RulesOrchestrator.get_all"


def _sql(statement) -> str:
    """Compile a statement to lower-cased PostgreSQL SQL with literals inlined.

    ``%`` is un-escaped back from the pyformat paramstyle doubling so LIKE patterns read normally.
    """
    compiled = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    return compiled.replace("%%", "%").lower()


def _condition_sql(term: str) -> str:
    condition = sigma_uuid_search_condition(term)
    assert condition is not None, f"expected a predicate for {term!r}"
    return _sql(condition)


def _rule_session() -> AsyncMock:
    """Session stub for ``RuleDAO.get_all``: count query first, then the page query."""
    session = AsyncMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    rows_result = MagicMock()
    rows_result.unique.return_value.scalars.return_value.all.return_value = []
    session.execute.side_effect = [count_result, rows_result]
    return session


def _pipeline_session() -> AsyncMock:
    """Session stub for ``PipelineRulesDAO.get_by_pipeline``: count query first, then the page query."""
    session = AsyncMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    rows_result = MagicMock()
    rows_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = [count_result, rows_result]
    return session


def _statements(session: AsyncMock) -> list:
    return [call.args[0] for call in session.execute.await_args_list]


# ---------------------------------------------------------------------------
# sigma_uuid_search_condition — matching semantics
# ---------------------------------------------------------------------------


def test_whole_uuid_matches_by_equality():
    # Equality keeps the pasted-UUID case on ix_rules_sigma_uuid instead of scanning.
    sql = _condition_sql(FULL_UUID)
    assert sql == f"rules.sigma_uuid = '{FULL_UUID}'"
    assert "ilike" not in sql


def test_uppercase_uuid_is_normalised_to_the_canonical_form():
    assert _condition_sql(FULL_UUID.upper()) == f"rules.sigma_uuid = '{FULL_UUID}'"


def test_uuid_without_dashes_is_normalised_to_the_canonical_form():
    assert _condition_sql(FULL_UUID.replace("-", "")) == f"rules.sigma_uuid = '{FULL_UUID}'"


def test_surrounding_whitespace_is_stripped():
    assert _condition_sql(f"  {FULL_UUID}  ") == f"rules.sigma_uuid = '{FULL_UUID}'"


def test_urn_and_brace_wrapped_uuids_match_by_equality():
    for term in (f"urn:uuid:{FULL_UUID}", f"{{{FULL_UUID}}}"):
        assert _condition_sql(term) == f"rules.sigma_uuid = '{FULL_UUID}'", term


def test_hex_fragment_matches_against_the_dash_stripped_rendering():
    # Type-ahead: the search box must work before all 36 characters are typed.
    sql = _condition_sql("1a2b")
    assert "replace(cast(rules.sigma_uuid as text), '-', '') ilike" in sql
    assert "%1a2b%" in sql


def test_hex_fragment_matching_is_case_insensitive():
    assert "ilike" in _condition_sql("1A2B")


def test_fragment_spanning_a_dash_boundary_still_matches():
    # '4d5e6f' straddles the first dash of the canonical form: dashes are stripped on both sides.
    assert "%4d5e6f%" in _condition_sql("4d5e6f")
    assert "%1a2b3c4d5e6f%" in _condition_sql("1a2b3c4d-5e6f")


def test_term_that_cannot_be_part_of_a_uuid_yields_no_predicate():
    # Non-hex terms match no UUID by construction, and a dash-only term carries no hex digits.
    for term in ("windows", "T1059", "", "   ", "a%b", "a_b", "'", "-", "---"):
        assert sigma_uuid_search_condition(term) is None, term


def test_fragments_are_not_ids_when_the_caller_forbids_them():
    # The pipeline details box shares one term with a name search: "4" is in nearly every UUID.
    assert sigma_uuid_search_condition("4", allow_fragment=False) is None
    assert sigma_uuid_search_condition("1a2b3c4d", allow_fragment=False) is None
    assert sigma_uuid_search_condition(FULL_UUID, allow_fragment=False) is not None


# ---------------------------------------------------------------------------
# Allow-list — unvalidated query input must never reach getattr(Rule, ...)
# ---------------------------------------------------------------------------


def test_allow_list_covers_exactly_the_public_search_fields():
    assert set(RULE_SEARCH_CONDITIONS) == {field.value for field in RuleSearchField}


# ---------------------------------------------------------------------------
# RuleDAO.get_all — the Repositories page
# ---------------------------------------------------------------------------


async def test_rule_dao_never_ilikes_the_uuid_column():
    session = _rule_session()

    await RuleDAO(session).get_all(search="1a2b", search_fields=[RuleSearchField.SIGMA_UUID.value])

    statements = _statements(session)
    assert len(statements) == 2  # count query + page query
    for statement in statements:
        sql = _sql(statement)
        assert "replace(cast(rules.sigma_uuid as text), '-', '') ilike" in sql
        assert "rules.sigma_uuid ilike" not in sql


async def test_rule_dao_returns_nothing_when_the_id_term_cannot_match():
    # The dangerous failure mode: no predicate must mean "no rows", not "no filter" — otherwise
    # a "By ID" search for a rule title would present the whole catalog as hits.
    session = _rule_session()

    await RuleDAO(session).get_all(search="windows", search_fields=[RuleSearchField.SIGMA_UUID.value])

    for statement in _statements(session):
        assert "where false" in _sql(statement)


async def test_rule_dao_scoped_id_search_that_cannot_match_returns_nothing():
    # Inside a selected repository the same bug would show the whole repository as hits.
    # SQLAlchemy folds `repository_id = X AND false` down to `false` — still zero rows.
    session = _rule_session()

    await RuleDAO(session).get_all(
        filters={"repository_id": uuid4()},
        search="windows",
        search_fields=[RuleSearchField.SIGMA_UUID.value],
    )

    for statement in _statements(session):
        sql = _sql(statement)
        assert "where false" in sql
        assert "rules.repository_id" not in sql.split("where", 1)[1]


async def test_rule_dao_keeps_matching_fields_when_one_leg_cannot_match():
    # A mixed field set must not collapse to "no rows" just because the uuid leg is impossible.
    session = _rule_session()

    await RuleDAO(session).get_all(
        search="windows",
        search_fields=[RuleSearchField.NAME.value, RuleSearchField.SIGMA_UUID.value],
    )

    sql = _sql(_statements(session)[1])
    assert "rules.name ilike '%windows%'" in sql
    assert "false" not in sql


async def test_rule_dao_matches_a_whole_uuid_across_all_repositories():
    session = _rule_session()

    await RuleDAO(session).get_all(search=FULL_UUID, search_fields=[RuleSearchField.SIGMA_UUID.value])

    sql = _sql(_statements(session)[1])
    assert f"rules.sigma_uuid = '{FULL_UUID}'" in sql
    assert "rules.repository_id =" not in sql


async def test_rule_dao_combines_sigma_uuid_search_with_repository_and_folder_filters():
    session = _rule_session()
    repository_id = uuid4()

    await RuleDAO(session).get_all(
        filters={"repository_id": repository_id, "folder_path": "_product/windows"},
        search=FULL_UUID,
        search_fields=[RuleSearchField.SIGMA_UUID.value],
    )

    for statement in _statements(session):
        sql = _sql(statement)
        assert f"rules.repository_id = '{repository_id}'" in sql
        assert "lower(rules.folder_path) = '_product/windows'" in sql
        assert f"rules.sigma_uuid = '{FULL_UUID}'" in sql


async def test_rule_dao_ignores_a_field_outside_the_allow_list():
    # ``body`` is a Python property and ``id``/``is_supported`` are non-text columns: reflection
    # used to build a broken predicate out of them and 500. Unknown fields are dropped, which
    # leaves the search unapplied — distinct from a known field that simply cannot match.
    session = _rule_session()

    await RuleDAO(session).get_all(search="anything", search_fields=["body", "id", "is_supported"])

    assert "where" not in _sql(_statements(session)[1])


async def test_rule_dao_default_text_search_is_unchanged():
    session = _rule_session()

    await RuleDAO(session).get_all(search="windows", search_fields=["name", "product", "service", "category"])

    sql = _sql(_statements(session)[1])
    for field in ("name", "product", "service", "category"):
        assert f"rules.{field} ilike '%windows%'" in sql
    assert "sigma_uuid" not in sql.split("where", 1)[1]


# ---------------------------------------------------------------------------
# PipelineRulesDAO.get_by_pipeline — the pipeline details page
# ---------------------------------------------------------------------------


async def test_pipeline_search_matches_rule_name_or_sigma_uuid():
    session = _pipeline_session()

    await PipelineRulesDAO(session).get_by_pipeline(uuid4(), search=FULL_UUID)

    sql = _sql(_statements(session)[1])
    assert "rules.name ilike" in sql
    assert f"rules.sigma_uuid = '{FULL_UUID}'" in sql
    assert " or " in sql


async def test_pipeline_count_query_applies_the_same_search():
    session = _pipeline_session()

    await PipelineRulesDAO(session).get_by_pipeline(uuid4(), search=FULL_UUID)

    count_sql = _sql(_statements(session)[0])
    assert "count(*)" in count_sql
    assert f"rules.sigma_uuid = '{FULL_UUID}'" in count_sql


async def test_pipeline_search_joins_rules_exactly_once():
    # The uuid predicate rides on the join the search branch already made; a second JOIN rules
    # would break the supported-filter and repository-sort branches.
    session = _pipeline_session()

    await PipelineRulesDAO(session).get_by_pipeline(
        uuid4(),
        search=FULL_UUID,
        supported_filter="supported",
        sort=PipelineRuleSortField.REPOSITORY.value,
    )

    assert _sql(_statements(session)[1]).count("join rules") == 1


async def test_pipeline_search_with_a_non_uuid_term_stays_a_name_search():
    session = _pipeline_session()

    await PipelineRulesDAO(session).get_by_pipeline(uuid4(), search="windows")

    sql = _sql(_statements(session)[1])
    assert "rules.name ilike '%windows%'" in sql
    assert "rules.sigma_uuid" not in sql


async def test_pipeline_search_does_not_treat_a_short_fragment_as_an_id():
    # "4" sits in every v4 UUID: matching it as an id would make the name search stop narrowing.
    for term in ("4", "a", "-", "1a2b3c4d"):
        session = _pipeline_session()

        await PipelineRulesDAO(session).get_by_pipeline(uuid4(), search=term)

        sql = _sql(_statements(session)[1])
        assert f"rules.name ilike '%{term}%'" in sql, term
        assert "rules.sigma_uuid" not in sql, term


def test_pipeline_rule_row_exposes_sigma_uuid():
    row = RuleListItem(
        id=str(uuid4()),
        name="Suspicious PowerShell",
        repository="Windows Rules",
        sigma_uuid=FULL_UUID,
        created="2026-08-07T10:00:00",
        updated="2026-08-07T10:00:00",
        enabled=True,
        tagged_events=0,
    )
    assert row.sigma_uuid == FULL_UUID
    assert RuleListItem.model_fields["sigma_uuid"].default is None


def _pipeline_rule_stub(sigma_uuid: UUID | None) -> SimpleNamespace:
    """Stand-in for a PipelineRule row with its Rule and Repository loaded."""
    now = datetime(2026, 8, 7, 10, 0, 0, tzinfo=UTC)
    rule = SimpleNamespace(
        id=uuid4(),
        name="Suspicious PowerShell",
        sigma_uuid=sigma_uuid,
        level="high",
        is_supported=True,
        unsupported_reason=None,
        repository=SimpleNamespace(id=uuid4(), name="Windows Rules"),
    )
    return SimpleNamespace(rule_id=rule.id, rule=rule, enabled=True, created=now, updated=now)


def _patch_pipeline_rules_endpoint(monkeypatch, rows: list) -> AsyncMock:
    """Stub the three DAOs the endpoint builds; returns the pipeline-rules DAO mock."""
    pipeline_dao = AsyncMock()
    pipeline_dao.get_by_id.return_value = SimpleNamespace(id=uuid4(), name="PpIn3")
    rules_dao = AsyncMock()
    rules_dao.get_by_pipeline.return_value = (rows, len(rows))
    metrics_dao = AsyncMock()
    metrics_dao.get_rule_metrics_map.return_value = {}
    monkeypatch.setattr("apps.routers.pipelines.PipelineDAO", lambda _db: pipeline_dao)
    monkeypatch.setattr("apps.routers.pipelines.PipelineRulesDAO", lambda _db: rules_dao)
    monkeypatch.setattr("apps.routers.pipelines.MetricsDAO", lambda _db: metrics_dao)
    return rules_dao


async def test_pipeline_rules_endpoint_returns_the_sigma_uuid_of_each_rule(api_client, monkeypatch):
    # Searching by id is only verifiable in the UI if the matched id comes back with the row.
    _patch_pipeline_rules_endpoint(monkeypatch, [_pipeline_rule_stub(UUID(FULL_UUID))])

    response = await api_client.get(f"/api/v1/pipeline/{uuid4()}/details/rules")

    assert response.status_code == 200
    assert response.json()["data"][0]["sigma_uuid"] == FULL_UUID


async def test_pipeline_rules_endpoint_returns_null_sigma_uuid_when_the_body_has_no_id(api_client, monkeypatch):
    _patch_pipeline_rules_endpoint(monkeypatch, [_pipeline_rule_stub(None)])

    response = await api_client.get(f"/api/v1/pipeline/{uuid4()}/details/rules")

    assert response.status_code == 200
    assert response.json()["data"][0]["sigma_uuid"] is None


async def test_pipeline_rules_endpoint_forwards_a_uuid_search_term_unchanged(api_client, monkeypatch):
    rules_dao = _patch_pipeline_rules_endpoint(monkeypatch, [])

    response = await api_client.get(f"/api/v1/pipeline/{uuid4()}/details/rules", params={"search": FULL_UUID})

    assert response.status_code == 200
    assert rules_dao.get_by_pipeline.await_args.kwargs["search"] == FULL_UUID


# ---------------------------------------------------------------------------
# GET /api/v1/rules — router contract
# ---------------------------------------------------------------------------


async def test_router_forwards_the_sigma_uuid_search_field(api_client, monkeypatch):
    get_all = AsyncMock(return_value=([], 0))
    monkeypatch.setattr(ORCHESTRATOR_GET_ALL, get_all)

    response = await api_client.get("/api/v1/rules", params={"search": FULL_UUID, "search_fields": "sigma_uuid"})

    assert response.status_code == 200
    assert get_all.await_args.kwargs["search_fields"] == [RuleSearchField.SIGMA_UUID]


async def test_router_forwards_a_repository_scoped_sigma_uuid_search(api_client, monkeypatch):
    get_all = AsyncMock(return_value=([], 0))
    monkeypatch.setattr(ORCHESTRATOR_GET_ALL, get_all)
    repository_id = uuid4()

    response = await api_client.get(
        "/api/v1/rules",
        params={"search": FULL_UUID, "search_fields": "sigma_uuid", "repository_id": str(repository_id)},
    )

    assert response.status_code == 200
    assert get_all.await_args.kwargs["repository_id"] == repository_id
    assert get_all.await_args.kwargs["search_fields"] == [RuleSearchField.SIGMA_UUID]


async def test_router_defaults_search_fields_to_none(api_client, monkeypatch):
    # The default field set belongs to the manager, not the router.
    get_all = AsyncMock(return_value=([], 0))
    monkeypatch.setattr(ORCHESTRATOR_GET_ALL, get_all)

    response = await api_client.get("/api/v1/rules", params={"search": "windows"})

    assert response.status_code == 200
    assert get_all.await_args.kwargs["search_fields"] is None


async def test_router_rejects_a_search_field_outside_the_allow_list(api_client):
    response = await api_client.get("/api/v1/rules", params={"search": "x", "search_fields": "body"})

    assert response.status_code == 422
