from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from apps.core.models import Rule
from apps.integrations.providers.socprime_tdm import TdmActiveThreat, TdmRule
from apps.managers.active_threats import ActiveThreatsSyncManager
from apps.modules.postgre.high_order_sigma import HighOrderSigmaDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import extract_referenced_sigma_uuids, get_sigma_uuid_from_sigma_body


def test_rule_model_has_sigma_uuid_column():
    assert hasattr(Rule, "sigma_uuid")


def test_parse_additional_fields_extracts_sigma_uuid_from_yaml_id():
    rule = {
        "body": """
title: Example
id: 11111111-2222-3333-4444-555555555555
logsource:
  product: windows
"""
    }

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["sigma_uuid"] == "11111111-2222-3333-4444-555555555555"


def test_parse_additional_fields_sets_sigma_uuid_none_when_missing():
    rule = {"body": "title: Example\nlogsource:\n  product: windows\n"}

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["sigma_uuid"] is None


def test_parse_additional_fields_sets_sigma_uuid_none_when_id_is_not_uuid():
    rule = {"body": "title: Example\nid: custom-rule-id\nlogsource:\n  product: windows\n"}

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["sigma_uuid"] is None


def test_parse_additional_fields_preserves_explicit_folder_path():
    rule = {
        "body": "title: Example\nlogsource:\n  product: windows\n",
        "folder_path": "_product/windows",
    }

    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])

    assert rule["folder_path"] == "_product/windows"


def test_shared_parser_extracts_sigma_uuid_from_yaml_id():
    sigma_uuid = get_sigma_uuid_from_sigma_body(
        """
title: Example
id: "11111111-2222-3333-4444-555555555555"
logsource:
  product: windows
"""
    )

    assert sigma_uuid == "11111111-2222-3333-4444-555555555555"


def test_hos_reference_extraction_removes_hos_own_uuid():
    hos_rule = TdmRule(
        id="hos-case-id",
        name="HOS",
        body="""
title: HOS
id: 11111111-2222-3333-4444-555555555555
detection:
  selection:
    related:
      - 11111111-2222-3333-4444-555555555555
      - 22222222-3333-4444-5555-666666666666
      - 33333333-4444-5555-6666-777777777777
""",
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert extract_referenced_sigma_uuids(hos_rule.body) == [
        "22222222-3333-4444-5555-666666666666",
        "33333333-4444-5555-6666-777777777777",
    ]


def test_build_retention_cutoff_returns_expected_date():
    now = datetime(2026, 5, 7, tzinfo=UTC)
    cutoff = ActiveThreatsSyncManager._build_retention_cutoff(now=now, retention_days=180)
    assert cutoff == now - timedelta(days=180)


def test_build_retention_cutoff_returns_none_for_non_positive_value():
    now = datetime(2026, 5, 7, tzinfo=UTC)
    assert ActiveThreatsSyncManager._build_retention_cutoff(now=now, retention_days=0) is None


def test_filter_retained_threats_excludes_expired_by_created_date():
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    expired = TdmActiveThreat(
        id="at-old",
        name="old",
        created=datetime(2025, 12, 31, tzinfo=UTC),
        updated=datetime(2026, 5, 1, tzinfo=UTC),
        related_high_order_sigma_rules=[],
    )
    retained = TdmActiveThreat(
        id="at-new",
        name="new",
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 5, 1, tzinfo=UTC),
        related_high_order_sigma_rules=[],
    )
    threats = ActiveThreatsSyncManager._filter_retained_threats(
        [expired, retained],
        retention_cutoff=cutoff,
    )

    assert [threat.id for threat in threats] == ["at-new"]


@pytest.mark.asyncio
async def test_get_updated_after_applies_overlap_for_existing_cursor():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    latest = datetime.now(UTC) - timedelta(days=3)  # inside the 90-day TDM window
    manager.dao.get_latest_tdm_updated.return_value = latest

    updated_after = await manager._get_updated_after()

    assert updated_after == latest - timedelta(minutes=5)


@pytest.mark.asyncio
async def test_remove_deleted_active_threats_returns_deleted_ids():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    provider = AsyncMock()

    manager.dao.get_all_tdm_ids.return_value = {"at-1", "at-2", "at-3"}
    manager.dao.get_excluded_pipeline_ids_by_threat_ids.return_value = {}
    provider.check_active_threats_exist.return_value = {"at-1", "at-3"}

    deleted_ids = await manager._remove_deleted_active_threats(provider)

    assert deleted_ids == ["at-2"]
    manager.dao.delete_by_tdm_ids.assert_awaited_once_with(["at-2"])


@pytest.mark.asyncio
async def test_remove_deleted_active_threats_skips_delete_when_all_exist():
    manager = ActiveThreatsSyncManager(AsyncMock())
    manager.dao = AsyncMock()
    provider = AsyncMock()

    manager.dao.get_all_tdm_ids.return_value = {"at-1", "at-2"}
    provider.check_active_threats_exist.return_value = {"at-1", "at-2"}

    deleted_ids = await manager._remove_deleted_active_threats(provider)

    assert deleted_ids == []
    manager.dao.delete_by_tdm_ids.assert_not_awaited()


@pytest.mark.asyncio
async def test_hos_active_threat_mapping_filters_hidden_active_threats():
    session = AsyncMock()
    result = MagicMock()
    result.all.return_value = [
        ("hos-1", "at-visible-2"),
        ("hos-1", "at-visible-1"),
        ("hos-1", "at-visible-1"),
    ]
    session.execute.return_value = result
    dao = HighOrderSigmaDAO(session)

    mapping = await dao.get_active_threat_ids_by_hos_tdm_ids(["hos-1"])

    statement = session.execute.await_args.args[0]
    sql = str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).lower()
    assert "join active_threats" in sql
    assert "active_threats.is_hidden is false" in sql
    assert mapping == {"hos-1": ["at-visible-1", "at-visible-2"]}
