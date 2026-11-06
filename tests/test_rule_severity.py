"""Sigma severity (``level:``) derivation and its exposure on the rule/HOS REST surfaces.

Severity is a contract shared with the frontend (rule tables on the Attack Chain, Active Threat,
Pipeline and Repositories screens) and already with the match node over Kafka, so both the
derivation rules and the response shapes are pinned here.
"""

import re
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from apps.core.converters import ActiveThreatConverter, HighOrderSigmaConverter, RuleConverter
from apps.core.models import HighOrderSigma, Rule
from apps.core.schemas import (
    ActiveThreatLinkedRuleResponse,
    ActiveThreatResponse,
    RuleListItem,
    ShortRuleDetailResponse,
)
from apps.integrations.providers.socprime_tdm import TdmRule
from apps.managers.active_threats import ActiveThreatsSyncManager
from apps.managers.attack_chains import AttackChainsManager
from apps.managers.high_order_sigma import HighOrderSigmasManager
from apps.modules.postgre.high_order_sigma_rule_checks import (
    DISABLED_PIPELINE_ERROR_MESSAGE,
    NOT_CONNECTED_RULE_ERROR_MESSAGE,
    UNSUPPORTED_RULE_ERROR_MESSAGE,
)
from apps.modules.postgre.rule import RuleDAO, RuleTechniqueFields
from apps.modules.utils import SIGMA_LEVEL_MAX_LENGTH, get_level_from_sigma_body, normalize_sigma_level
from apps.routers.pipelines import get_pipeline_rules

SIGMA_BODY = """
title: Example
id: 11111111-2222-3333-4444-555555555555
logsource:
  product: windows
level: high
"""


# --- model / column -----------------------------------------------------------------


def test_rule_and_hos_models_have_level_columns():
    assert hasattr(Rule, "level")
    assert hasattr(HighOrderSigma, "level")


@pytest.mark.asyncio
async def test_rule_dao_get_all_selects_the_level_column():
    """``get_all`` restricts columns with ``load_only``. A ``level`` missing from that list is
    deferred, and touching it on the async session raises MissingGreenlet instead of returning
    null — so assert it reaches the emitted SELECT.
    """
    session = AsyncMock()
    result = MagicMock()
    result.scalar.return_value = 0
    result.unique.return_value.scalars.return_value.all.return_value = []
    session.execute.return_value = result

    await RuleDAO(session).get_all()

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert any("rules.level" in statement for statement in statements)


# --- parsing ------------------------------------------------------------------------


def test_parse_additional_fields_extracts_level():
    rule = {"body": SIGMA_BODY}

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["level"] == "high"


def test_parse_additional_fields_sets_level_none_when_missing():
    rule = {"body": "title: Example\nlogsource:\n  product: windows\n"}

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["level"] is None


def test_parse_additional_fields_leaves_level_untouched_without_body():
    """A metadata-only update (e.g. a rename) must not wipe the derived severity."""
    rule = {"name": "renamed"}

    RuleDAO._parse_additional_fields_from_body(rule, None)

    assert "level" not in rule


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ('title: t\nlevel: "high"\n', "high"),  # quoted value
        ("title: t\r\nlevel: medium\r\n", "medium"),  # CRLF line endings
        ("title: t\nlevel: HIGH\n", "high"),  # canonicalised to lowercase
        ("title: t\nlevel:   low   \n", "low"),  # surrounding whitespace
        ("title: t\ndetection:\n  selection:\n    Level: Error\n", None),  # indented, not top level
        ("title: t\nlevel:\n", None),  # present but empty
        ("title: t\nlevel: 3\n", None),  # non-string
        ("title: t\n  bad: [yaml\n", None),  # unparseable
        ("- not a mapping\n", None),
        (None, None),
    ],
)
def test_get_level_from_sigma_body_handles_real_world_shapes(body, expected):
    assert get_level_from_sigma_body(body) == expected


def test_normalize_sigma_level_passes_through_non_standard_values():
    """The column is a plain string, not an enum — unknown levels are preserved, not dropped."""
    assert normalize_sigma_level("Emergency") == "emergency"
    assert normalize_sigma_level("") is None
    assert normalize_sigma_level(None) is None


def test_normalize_sigma_level_truncates_to_the_column_width():
    """PostgreSQL raises 22001 rather than truncating, which would abort a whole sync batch (or
    the Alembic backfill) over one malformed body — so the value is bounded at derivation."""
    overlong = "critical " + ("x" * 400)

    value = normalize_sigma_level(overlong)

    assert len(value) == SIGMA_LEVEL_MAX_LENGTH
    assert SIGMA_LEVEL_MAX_LENGTH == Rule.__table__.c.level.type.length
    assert SIGMA_LEVEL_MAX_LENGTH == HighOrderSigma.__table__.c.level.type.length


def test_get_level_from_sigma_body_truncates_overlong_values():
    body = 'title: t\nlevel: "' + ("y" * 400) + '"\n'

    assert len(get_level_from_sigma_body(body)) == SIGMA_LEVEL_MAX_LENGTH


def test_migration_column_width_matches_the_truncation_bound():
    """The model and the truncation bound agreeing is not enough — the real column is created by
    the migration. If those drift, the cap stops protecting the write and PostgreSQL raises 22001
    again for values between the two widths.
    """
    migration = (
        Path(__file__).resolve().parents[1] / "alembic" / "versions" / "bf9230603f85_add_rule_and_hos_level.py"
    ).read_text()

    widths = re.findall(r'op\.add_column\(\s*"(\w+)",\s*sa\.Column\("level",\s*sa\.String\(length=(\d+)\)', migration)

    assert dict(widths) == {
        "rules": str(SIGMA_LEVEL_MAX_LENGTH),
        "high_order_sigmas": str(SIGMA_LEVEL_MAX_LENGTH),
    }


# --- response schemas ---------------------------------------------------------------


def _rule(level: str | None = "high") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        name="Example",
        version=1,
        repository_id=uuid4(),
        repository=SimpleNamespace(id=uuid4(), type="api", name="PRODUCTION Rules"),
        product="windows",
        service=None,
        category=None,
        folder_path=None,
        sigma_uuid=UUID("11111111-2222-3333-4444-555555555555"),
        level=level,
        external_id=None,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        is_supported=True,
        unsupported_reason=None,
        is_user_edited_system_rule=False,
    )


def test_rule_converter_exposes_level():
    assert RuleConverter.to_detail(_rule()).level == "high"
    assert RuleConverter.to_detail(_rule(level=None)).level is None


def test_level_is_declared_on_short_rule_detail_response():
    """``ActiveThreatResponse.rules`` is annotated ``list[ShortRuleDetailResponse]`` while the
    converter feeds it ``RuleDetailResponse``. Pydantic serializes by the DECLARED type, so a
    ``level`` living only on the subclass would be silently dropped from the Active Threat
    details page. Keep it on the base class.
    """
    assert "level" in ShortRuleDetailResponse.model_fields


def test_active_threat_detail_serializes_rule_level():
    threat = SimpleNamespace(
        tdm_id="at-1",
        name="Threat",
        is_blocked=False,
        summary=None,
        severity="high",
        investigation=None,
        response=None,
        mitigation=None,
        industries=None,
        geographies=None,
        threat_types=None,
        threat_actors=None,
        source=None,
        attack_flow=None,
        tdm_created=None,
        tdm_updated=None,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        is_hidden=False,
        high_order_sigmas=[],
        linked_rules=[_rule(level="critical")],
    )

    payload = ActiveThreatConverter.to_detail(threat).model_dump()

    assert payload["rules"][0]["level"] == "critical"
    assert payload["rules"][0]["rule_check_results_reason"] is None


def test_active_threat_detail_stamps_rule_check_results_reason():
    unsupported = _rule()
    not_connected = _rule()
    disabled = _rule()
    healthy = _rule()
    threat = SimpleNamespace(
        tdm_id="at-1",
        name="Threat",
        is_blocked=False,
        summary=None,
        severity="high",
        investigation=None,
        response=None,
        mitigation=None,
        industries=None,
        geographies=None,
        threat_types=None,
        threat_actors=None,
        source=None,
        attack_flow=None,
        tdm_created=None,
        tdm_updated=None,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        is_hidden=False,
        high_order_sigmas=[],
        linked_rules=[unsupported, not_connected, disabled, healthy],
        rule_check_results={
            "unsupported_rules": [
                {
                    "rule_id": str(unsupported.id),
                    "rule_name": unsupported.name,
                    "description": UNSUPPORTED_RULE_ERROR_MESSAGE,
                }
            ],
            "not_connected_rules": [
                {
                    "rule_id": str(unsupported.id),
                    "rule_name": unsupported.name,
                    "description": NOT_CONNECTED_RULE_ERROR_MESSAGE,
                },
                {
                    "rule_id": str(not_connected.id),
                    "rule_name": not_connected.name,
                    "description": NOT_CONNECTED_RULE_ERROR_MESSAGE,
                },
            ],
            "disabled_rules": [
                {
                    "rule_id": str(not_connected.id),
                    "rule_name": not_connected.name,
                    "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                    "pipelines": [{"id": "p-1", "name": "Windows"}],
                },
                {
                    "rule_id": str(disabled.id),
                    "rule_name": disabled.name,
                    "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                    "pipelines": [{"id": "p-1", "name": "Windows"}],
                },
            ],
        },
    )

    by_id = {row.id: row.rule_check_results_reason for row in ActiveThreatConverter.to_detail(threat).rules}
    assert by_id[str(unsupported.id)] == UNSUPPORTED_RULE_ERROR_MESSAGE
    assert by_id[str(not_connected.id)] == NOT_CONNECTED_RULE_ERROR_MESSAGE
    assert by_id[str(disabled.id)] == f"{DISABLED_PIPELINE_ERROR_MESSAGE} Windows"
    assert by_id[str(healthy.id)] is None


@pytest.mark.asyncio
async def test_pipeline_rules_endpoint_maps_level_onto_each_row():
    """Drives the real handler: the pipeline screen reads level off the eager-loaded Rule, and
    a pipeline_rule with no rule attached must degrade to None instead of raising."""
    pipeline_id = uuid4()
    with_rule = SimpleNamespace(
        rule_id=uuid4(),
        enabled=True,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        rule=_rule(level="high"),
    )
    without_rule = SimpleNamespace(
        rule_id=uuid4(),
        enabled=False,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        rule=None,
    )

    with (
        patch("apps.routers.pipelines.PipelineDAO") as pipeline_dao,
        patch("apps.routers.pipelines.PipelineRulesDAO") as rules_dao,
        patch("apps.routers.pipelines.MetricsDAO") as metrics_dao,
    ):
        pipeline_dao.return_value.get_by_id = AsyncMock(return_value=SimpleNamespace(id=pipeline_id))
        rules_dao.return_value.get_by_pipeline = AsyncMock(return_value=([with_rule, without_rule], 2))
        metrics_dao.return_value.get_rule_metrics_map = AsyncMock(return_value={})

        # Called directly, so FastAPI's Query defaults are not resolved — pass them explicitly.
        response = await get_pipeline_rules(
            pipeline_id=str(pipeline_id),
            _=SimpleNamespace(),
            page=1,
            limit=10,
            sort=None,
            order="asc",
            search=None,
            tagged_filter="all",
            supported_filter="all",
            db=AsyncMock(),
        )

    assert [row.level for row in response.data] == ["high", None]


def test_rule_list_item_level_defaults_to_none():
    item = RuleListItem(
        id="r1",
        name="Example",
        repository="PRODUCTION Rules",
        repository_id=None,
        created="",
        updated="",
        enabled=True,
        tagged_events=0,
    )

    assert item.level is None


def test_active_threat_response_rules_field_is_linked_rule_detail():
    """AT detail declares a ShortRuleDetailResponse subclass so ``rule_check_results_reason``
    survives serialization without leaking onto GET /rules.
    """
    annotation = str(ActiveThreatResponse.model_fields["rules"].annotation)
    assert "ActiveThreatLinkedRuleResponse" in annotation
    assert issubclass(ActiveThreatLinkedRuleResponse, ShortRuleDetailResponse)


# --- high-order Sigma ---------------------------------------------------------------


def test_hos_row_prefers_tdm_level_over_body():
    row = ActiveThreatsSyncManager._hos_to_row(
        TdmRule(
            id="hos-1",
            name="HOS",
            body="title: HOS\nlevel: low\n",
            level="Critical",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 2, tzinfo=UTC),
        )
    )

    assert row["level"] == "critical"


def test_hos_row_falls_back_to_body_level():
    row = ActiveThreatsSyncManager._hos_to_row(
        TdmRule(
            id="hos-1",
            name="HOS",
            body="title: HOS\nlevel: high\n",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 2, tzinfo=UTC),
        )
    )

    assert row["level"] == "high"


def test_hos_row_level_is_none_when_neither_source_has_one():
    row = ActiveThreatsSyncManager._hos_to_row(
        TdmRule(
            id="hos-1",
            name="HOS",
            body="title: HOS\n",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 2, tzinfo=UTC),
        )
    )

    assert row["level"] is None


async def _run_hos_update(*, stored_level: str | None, name: str | None, body: str | None) -> dict:
    """Invoke HighOrderSigmasManager.update and return the values it wrote."""
    row = SimpleNamespace(id=uuid4(), tdm_id="hos-1", name="HOS", level=stored_level)
    manager = HighOrderSigmasManager.__new__(HighOrderSigmasManager)
    manager.db = AsyncMock()
    manager.dao = AsyncMock()
    manager.dao.get_by_id.return_value = row
    manager.dao.get_tdm_ids_with_non_hidden_at.return_value = set()
    manager.kafka = AsyncMock()

    with patch("apps.managers.high_order_sigma.activity_producer", AsyncMock()):
        await manager.update(
            sigma_id=row.id,
            name=name,
            body=body,
            user=SimpleNamespace(email="a@b.c", full_name="A", id=uuid4()),
        )

    statement = manager.db.execute.await_args.args[0]
    return dict(statement.compile().params)


@pytest.mark.asyncio
async def test_hos_user_edit_rederives_level_from_the_new_body():
    params = await _run_hos_update(stored_level="low", name=None, body="title: h\nlevel: critical\n")

    assert params["level"] == "critical"


@pytest.mark.asyncio
async def test_hos_user_edit_keeps_tdm_level_when_the_body_declares_none():
    """A PATCH that resends an unchanged, level-less body (e.g. a rename) must not discard the
    level TDM reported — sync cannot restore it, because upsert_many skips is_user_edited rows."""
    params = await _run_hos_update(stored_level="critical", name="renamed", body="title: h\n")

    assert params["level"] == "critical"


@pytest.mark.asyncio
async def test_hos_name_only_patch_does_not_touch_level():
    params = await _run_hos_update(stored_level="critical", name="renamed", body=None)

    assert "level" not in params


def test_hos_converters_expose_level():
    hos = SimpleNamespace(
        id=uuid4(),
        tdm_id="hos-1",
        name="HOS",
        body="title: HOS\nlevel: high\n",
        level="high",
        tdm_created=None,
        tdm_updated=None,
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
        is_user_edited=False,
    )

    assert HighOrderSigmaConverter.to_detail(hos).level == "high"
    assert HighOrderSigmaConverter.to_list_item(hos).level == "high"


# --- attack chain matched-rule rows -------------------------------------------------


def _event(
    sigma_rule_id: UUID,
    title: str,
    technique: str = "T1087",
    *,
    rule_id: UUID | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        sigma_rule_id=sigma_rule_id,
        sigma_rule_title=title,
        rule_id=rule_id,
        timestamp=datetime(2026, 2, 4, tzinfo=UTC),
        technique_ids=[technique],
    )


def _manager() -> AttackChainsManager:
    manager = AttackChainsManager.__new__(AttackChainsManager)
    manager.mitre_attack = MagicMock()
    manager.mitre_attack.get_technique_by_id.return_value = None
    return manager


def test_chain_techniques_attach_level_from_sigma_uuid_map():
    """Legacy events (no ``rule_id``) still resolve severity by Sigma-body UUID."""
    manager = _manager()
    known = uuid4()
    unknown = uuid4()

    techniques = manager._prepare_chain_techniques(
        [_event(known, "Known rule"), _event(unknown, "Unknown rule")],
        {str(known): "critical"},
    )

    rules = {rule["rule_name"]: rule for rule in techniques["T1087"]["rules"]}
    assert rules["Known rule"]["level"] == "critical"
    assert rules["Known rule"]["rule_id"] is None
    assert rules["Known rule"]["repository_id"] is None
    # A rule that is not present locally (or declares no level) must degrade to null, not raise.
    assert rules["Unknown rule"]["level"] is None


def test_chain_techniques_pk_hit_uses_name_level_and_repository():
    manager = _manager()
    rule_id = uuid4()
    repo_id = uuid4()
    sigma_id = uuid4()
    rules_by_id = {
        rule_id: RuleTechniqueFields(id=rule_id, repository_id=repo_id, name="DB name", level="high"),
    }

    techniques = manager._prepare_chain_techniques(
        [_event(sigma_id, "Wire title", rule_id=rule_id)],
        {str(sigma_id): "critical"},
        rules_by_id,
    )

    row = techniques["T1087"]["rules"][0]
    assert row["rule_id"] == str(rule_id)
    assert row["repository_id"] == str(repo_id)
    assert row["rule_name"] == "DB name"
    assert row["level"] == "high"
    assert row["sigma_rule_id"] == str(sigma_id)


def test_chain_techniques_pk_miss_falls_back_to_title_and_sigma_uuid_level():
    """Deleted rule: no ``rule_id``/``repository_id``, wire title, level still from the Sigma map."""
    manager = _manager()
    missing_id = uuid4()
    sigma_id = uuid4()

    techniques = manager._prepare_chain_techniques(
        [_event(sigma_id, "Fallback title", rule_id=missing_id)],
        {str(sigma_id): "critical"},
        {},
    )

    row = techniques["T1087"]["rules"][0]
    assert row["rule_id"] is None
    assert row["repository_id"] is None
    assert row["rule_name"] == "Fallback title"
    assert row["level"] == "critical"
    assert row["sigma_rule_id"] == str(sigma_id)


def test_chain_techniques_pk_miss_with_unknown_sigma_has_null_level():
    manager = _manager()
    sigma_id = uuid4()

    techniques = manager._prepare_chain_techniques(
        [_event(sigma_id, "Fallback title", rule_id=uuid4())],
        {},
        {},
    )

    assert techniques["T1087"]["rules"][0]["level"] is None


def test_chain_techniques_two_rule_ids_with_same_sigma_uuid_are_two_rows():
    manager = _manager()
    sigma_id = uuid4()
    first_id = uuid4()
    second_id = uuid4()
    first_repo = uuid4()
    second_repo = uuid4()
    rules_by_id = {
        first_id: RuleTechniqueFields(id=first_id, repository_id=first_repo, name="Copy A", level="high"),
        second_id: RuleTechniqueFields(id=second_id, repository_id=second_repo, name="Copy B", level="low"),
    }

    techniques = manager._prepare_chain_techniques(
        [
            _event(sigma_id, "Same title", rule_id=first_id),
            _event(sigma_id, "Same title", rule_id=second_id),
        ],
        {str(sigma_id): "critical"},
        rules_by_id,
    )

    rows = techniques["T1087"]["rules"]
    assert len(rows) == 2
    by_id = {row["rule_id"]: row for row in rows}
    assert by_id[str(first_id)]["rule_name"] == "Copy A"
    assert by_id[str(first_id)]["level"] == "high"
    assert by_id[str(first_id)]["repository_id"] == str(first_repo)
    assert by_id[str(second_id)]["rule_name"] == "Copy B"
    assert by_id[str(second_id)]["level"] == "low"
    assert by_id[str(first_id)]["sigma_rule_id"] == str(sigma_id)
    assert by_id[str(second_id)]["sigma_rule_id"] == str(sigma_id)


@pytest.mark.asyncio
async def test_get_levels_by_sigma_uuids_keeps_the_first_row_when_copies_disagree():
    """One Sigma UUID can map to several rules rows (same Sigma in multiple repositories), and
    those copies CAN disagree — editing a system-repo rule re-derives only its own level. The
    query orders by ``updated`` desc so the most recent copy wins; here the first row must win.
    """
    sigma_uuid = uuid4()
    session = AsyncMock()
    result = MagicMock()
    result.all.return_value = [(sigma_uuid, "critical"), (sigma_uuid, "high")]
    session.execute.return_value = result

    levels = await RuleDAO(session).get_levels_by_sigma_uuids([sigma_uuid, sigma_uuid])

    assert levels == {str(sigma_uuid): "critical"}


@pytest.mark.asyncio
async def test_get_levels_by_sigma_uuids_query_filters_and_orders():
    """Pin the SQL: without the ORDER BY the winning copy flips between identical requests, and
    without the NOT NULL filter a level-less copy can mask a real severity."""
    session = AsyncMock()
    result = MagicMock()
    result.all.return_value = []
    session.execute.return_value = result

    await RuleDAO(session).get_levels_by_sigma_uuids([uuid4()])

    statement = str(session.execute.await_args.args[0])
    assert "rules.sigma_uuid IN" in statement
    assert "rules.level IS NOT NULL" in statement
    assert "ORDER BY rules.updated DESC" in statement


@pytest.mark.asyncio
async def test_get_levels_by_sigma_uuids_short_circuits_on_empty_input():
    session = AsyncMock()

    assert await RuleDAO(session).get_levels_by_sigma_uuids([]) == {}
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_technique_fields_by_ids_selects_only_id_repo_name_level():
    session = AsyncMock()
    result = MagicMock()
    rule_id = uuid4()
    repo_id = uuid4()
    result.all.return_value = [SimpleNamespace(id=rule_id, repository_id=repo_id, name="R", level="high")]
    session.execute.return_value = result

    rows = await RuleDAO(session).get_technique_fields_by_ids([rule_id])

    assert rows[rule_id] == RuleTechniqueFields(id=rule_id, repository_id=repo_id, name="R", level="high")
    statement = str(session.execute.await_args.args[0])
    assert "rules.id IN" in statement
    assert "rules.repository_id" in statement
    assert "rules.name" in statement
    assert "rules.level" in statement
    assert "rule_content" not in statement
    assert "current_content" not in statement


@pytest.mark.asyncio
async def test_get_technique_fields_by_ids_short_circuits_on_empty_input():
    session = AsyncMock()

    assert await RuleDAO(session).get_technique_fields_by_ids([]) == {}
    session.execute.assert_not_awaited()
