"""Tests for the Microsoft Sentinel ``view_in_siem`` capability (deep-link search).

The deep-link shape is a ``#view/Microsoft_Azure_Monitoring_Logs/LogsBlade`` blade with a JSON
``scope``, a plain URL-encoded ``query``, and the period carried by ``timespan`` (no in-query
time filter).
"""

import uuid
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from urllib.parse import unquote

import pytest

import apps.integrations.providers  # noqa: F401  -- register all providers for registry lookups
from apps.core.exceptions import BadRequestError, NotFoundError, ValidationError
from apps.integrations.base import ChainSearchContext, ViewInSiemIntegration, get_provider_capabilities
from apps.integrations.providers.microsoft_sentinel import (
    _DEFAULT_HOST_NAME_FIELDS,
    DEFAULT_SIGMA_RULE_ID_FIELD,
    MicrosoftSentinelConfig,
    MicrosoftSentinelProvider,
    _kql_host_field_expr,
)
from apps.managers.integrations import IntegrationsManager

# A realistic workspace Logs URL as copied from Workspace Settings > Logs.
SENTINEL_URL = (
    "https://portal.azure.com/#@contoso.onmicrosoft.com/resource/subscriptions/"
    "11111111-1111-1111-1111-111111111111/resourceGroups/rg-soc/providers/"
    "Microsoft.OperationalInsights/workspaces/law-soc-prod/logs"
)
RESOURCE_ID = (
    "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/rg-soc/"
    "providers/Microsoft.OperationalInsights/workspaces/law-soc-prod"
)
START = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
END = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)


def _decode_query(url: str) -> str:
    """Reverse the deep-link ``/query/<...>`` segment back to KQL (plain URL-decode)."""
    return unquote(url.split("/query/", 1)[1].split("/timespan/", 1)[0])


def _decode_timespan(url: str) -> str:
    return unquote(url.split("/timespan/", 1)[1])


def _view_in_siem_config(**overrides) -> dict:
    cfg = {
        "sentinel_url": SENTINEL_URL,
        "table_names": ["SecurityEvent", "Syslog"],
        "host_name_fields": ["Computer", "HostName"],
    }
    cfg.update(overrides)
    return cfg


def _ctx(sigma_rule_ids: list[str], hostname: str | None, start=START, end=END) -> ChainSearchContext:
    return ChainSearchContext(sigma_rule_ids=sigma_rule_ids, hostname=hostname, start=start, end=end)


# ---------------------------------------------------------------------------
# Capability & config schema
# ---------------------------------------------------------------------------


def test_provider_advertises_view_in_siem_capability():
    assert issubclass(MicrosoftSentinelProvider, ViewInSiemIntegration)
    capabilities = get_provider_capabilities(MicrosoftSentinelProvider)
    assert capabilities == ["check_connection", "deploy_rules", "search", "view_in_siem"]


def test_config_defaults_host_name_fields():
    cfg = MicrosoftSentinelConfig()
    assert cfg.host_name_fields == list(_DEFAULT_HOST_NAME_FIELDS)
    assert cfg.sentinel_url == ""
    assert cfg.table_names == []
    assert cfg.remapped_sigma_rule_id_field == ""
    # RELEASE SCOPE: the ARM credential fields are commented out of the config schema (the form
    # renders from it) while the ARM capability group is unreleased — pin that they stay hidden.
    assert {"client_id", "client_secret", "tenant_id", "subscription_id", "resource_group", "workspace"}.isdisjoint(
        MicrosoftSentinelConfig.model_fields
    )


def test_config_host_name_fields_default_is_not_shared():
    a = MicrosoftSentinelConfig()
    a.host_name_fields.append("Extra")
    b = MicrosoftSentinelConfig()
    assert "Extra" not in b.host_name_fields


def test_config_schema_exposes_host_name_fields_default_for_form_prefill():
    # The frontend renders the create form from config_schema; the default MUST be present so the
    # field is pre-filled (default_factory would NOT serialize into the JSON Schema — regression guard).
    prop = MicrosoftSentinelConfig.model_json_schema()["properties"]["host_name_fields"]
    assert prop.get("default") == list(_DEFAULT_HOST_NAME_FIELDS)


# ---------------------------------------------------------------------------
# KQL builder (no in-query time filter — period lives in the URL timespan)
# ---------------------------------------------------------------------------


def test_kql_builder_full_query():
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent", "Syslog"],
        host_name_fields=["Computer", "HostName"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1", "uuid2"],
        hostname="srv-ad-001.example.com",
    )
    assert kql.startswith("union isfuzzy=true")
    assert "    (SecurityEvent)," in kql
    assert "    (Syslog)" in kql
    assert '| extend Host = coalesce(column_ifexists("Computer", ""), column_ifexists("HostName", ""))' in kql
    assert '| where Host == "srv-ad-001.example.com" and SigmaRuleID in ("uuid1", "uuid2")' in kql
    assert kql.rstrip().endswith("| order by TimeGenerated desc")
    # Time is carried by the portal timespan, not the query.
    assert "TimeGenerated between" not in kql
    assert "datetime(" not in kql


def test_kql_builder_single_host_field_has_no_coalesce():
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1"],
        hostname="host-1",
    )
    assert '| extend Host = column_ifexists("Computer", "")' in kql
    assert "coalesce(" not in kql


def test_kql_builder_omits_sigma_filter_when_no_ids():
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=[],
        hostname="host-1",
    )
    assert "SigmaRuleID in" not in kql
    assert '| where Host == "host-1"' in kql


def test_kql_builder_uses_remapped_sigma_field():
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="DetectionRuleId",
        sigma_rule_ids=["uuid1"],
        hostname="host-1",
    )
    assert 'DetectionRuleId == "uuid1"' in kql  # single id → equality


def test_kql_sigma_filter_equals_for_one_in_for_many():
    one = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1"],
        hostname="h",
    )
    many = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1", "uuid2"],
        hostname="h",
    )
    assert 'SigmaRuleID == "uuid1"' in one and "SigmaRuleID in" not in one
    assert 'SigmaRuleID in ("uuid1", "uuid2")' in many


def test_kql_builder_escapes_hostname():
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent"],
        host_name_fields=["Computer"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=[],
        hostname='evil" or 1==1//\\x',
    )
    assert r'Host == "evil\" or 1==1//\\x"' in kql


def test_kql_builder_guards_default_host_fields_for_missing_columns():
    # Full default host fields over SecurityEvent+Syslog must not emit bare references to
    # columns absent from those tables — a bare reference to a column absent from every unioned
    # table is a KQL semantic error (isfuzzy only saves missing tables, not missing columns).
    kql = MicrosoftSentinelProvider._build_view_in_siem_kql(
        tables=["SecurityEvent", "Syslog"],
        host_name_fields=list(_DEFAULT_HOST_NAME_FIELDS),
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1"],
        hostname="h",
    )
    assert 'column_ifexists("Computer", "")' in kql
    assert 'column_ifexists("IPAddress", "")' in kql
    assert 'column_ifexists("CallerIpAddress", "")' in kql
    # Dynamic-indexing expressions: only the root column is guarded.
    assert "tostring(column_ifexists(\"TargetResources\", dynamic(null))[0]['displayName'])" in kql
    assert "tostring(column_ifexists(\"Resources\", dynamic(null))[0]['ARN'])" in kql
    # No unguarded reference to a column missing from the configured tables.
    assert ", IPAddress," not in kql
    assert "TargetResources[0]" not in kql  # always guarded form
    assert "coalesce(Computer," not in kql  # plain Computer is wrapped


def test_host_field_expr_plain_and_dynamic():
    assert _kql_host_field_expr("Computer") == 'column_ifexists("Computer", "")'
    assert (
        _kql_host_field_expr("tostring(TargetResources[0]['displayName'])")
        == "tostring(column_ifexists(\"TargetResources\", dynamic(null))[0]['displayName'])"
    )


def test_host_field_expr_function_wrapped_column_is_guarded():
    # A function-wrapped plain column (no indexing) still gets its column guarded, so it can't
    # blow up the union when the column is absent from a table.
    assert _kql_host_field_expr("tostring(Resources)") == 'tostring(column_ifexists("Resources", dynamic(null)))'


# ---------------------------------------------------------------------------
# resourceId / host extraction & scope param
# ---------------------------------------------------------------------------


def test_extract_resource_id_from_resource_logs_url():
    assert MicrosoftSentinelProvider._extract_workspace_resource_id(SENTINEL_URL) == RESOURCE_ID


def test_extract_resource_id_from_encoded_blade_url():
    url = (
        "https://portal.azure.com/#@tenant/blade/Microsoft_OperationsManagementSuite_Workspace/"
        "Logs.ReactView/resourceId/%2Fsubscriptions%2F2222%2FresourceGroups%2Frg%2Fproviders%2F"
        "Microsoft.OperationalInsights%2Fworkspaces%2Fws1"
    )
    rid = MicrosoftSentinelProvider._extract_workspace_resource_id(url)
    assert rid == "/subscriptions/2222/resourceGroups/rg/providers/Microsoft.OperationalInsights/workspaces/ws1"


def test_extract_resource_id_returns_none_for_garbage():
    assert MicrosoftSentinelProvider._extract_workspace_resource_id("https://example.com/nope") is None
    assert MicrosoftSentinelProvider._extract_workspace_resource_id("") is None


def test_is_defender_portal_url():
    assert MicrosoftSentinelProvider._is_defender_portal_url("https://security.microsoft.com/v2/advanced-hunting")
    assert not MicrosoftSentinelProvider._is_defender_portal_url(SENTINEL_URL)


def test_build_scope_param():
    scope = MicrosoftSentinelProvider._build_scope_param(RESOURCE_ID)
    assert scope.startswith("%7B%22resources%22%3A%5B%7B%22resourceId%22%3A%22%2Fsubscriptions%2F11111111")
    assert unquote(scope) == f'{{"resources":[{{"resourceId":"{RESOURCE_ID}"}}]}}'


# ---------------------------------------------------------------------------
# build_view_in_siem_url (end-to-end provider call) — deep-link URL shape
# ---------------------------------------------------------------------------


async def test_build_view_in_siem_url_success():
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1", "uuid2"], "srv-ad-001.example.com"))
    assert result.status is True
    url = result.url
    assert url is not None
    assert url.startswith("https://portal.azure.com/#view/Microsoft_Azure_Monitoring_Logs/LogsBlade/scope~/")
    scope = MicrosoftSentinelProvider._build_scope_param(RESOURCE_ID)
    assert f"/scope~/{scope}/initiator/DetectFlow/query/" in url
    assert "/timespan/" in url
    # Plain (not gzip) URL-encoded query round-trips and matches the returned preview.
    assert _decode_query(url) == result.query


async def test_build_view_in_siem_url_timespan_is_utc():
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert _decode_timespan(result.url) == "2026-06-01T10:00:00.000Z/2026-06-02T11:30:00.000Z"


async def test_build_view_in_siem_url_timespan_normalizes_non_utc():
    # 12:00 at +02:00 == 10:00Z for both bounds.
    tz_plus2 = timezone(timedelta(hours=2))
    start = datetime(2026, 6, 1, 12, 0, 0, tzinfo=tz_plus2)
    end = datetime(2026, 6, 2, 13, 30, 0, tzinfo=tz_plus2)
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", start=start, end=end))
    assert _decode_timespan(result.url) == "2026-06-01T10:00:00.000Z/2026-06-02T11:30:00.000Z"


async def test_build_view_in_siem_url_timespan_end_rounds_up_to_second():
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    end = datetime(2026, 6, 2, 11, 30, 0, 1, tzinfo=UTC)  # +1us sub-second
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", end=end))
    # End rounded up to the next whole second so the chain's latest event isn't clipped.
    assert _decode_timespan(result.url) == "2026-06-01T10:00:00.000Z/2026-06-02T11:30:01.000Z"


async def test_build_view_in_siem_url_without_hostname_or_rules():
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx([], None))
    assert result.status is True
    assert result.url is not None
    assert "| where Host" not in result.query
    assert "SigmaRuleID in" not in result.query


async def test_build_view_in_siem_url_zero_width_window():
    # A single-instant chain (first_seen == last_seen) yields a valid zero-width inclusive timespan.
    instant = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h", start=instant, end=instant))
    assert result.status is True
    assert _decode_timespan(result.url) == "2026-06-02T11:30:00.000Z/2026-06-02T11:30:00.000Z"


async def test_build_view_in_siem_url_requires_sentinel_url():
    provider = MicrosoftSentinelProvider(_view_in_siem_config(sentinel_url=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    assert "URL is not configured" in result.message


async def test_build_view_in_siem_url_requires_table_names():
    provider = MicrosoftSentinelProvider(_view_in_siem_config(table_names=[]))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "table name" in result.message.lower()


async def test_build_view_in_siem_url_rejects_defender_portal():
    provider = MicrosoftSentinelProvider(
        _view_in_siem_config(sentinel_url="https://security.microsoft.com/v2/advanced-hunting")
    )
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Defender" in result.message
    # Query is still generated for preview even when the URL can't be built.
    assert result.query is not None


async def test_build_view_in_siem_url_defaults_sigma_field():
    provider = MicrosoftSentinelProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert f"{DEFAULT_SIGMA_RULE_ID_FIELD} == " in result.query


async def test_build_view_in_siem_url_strips_userinfo_from_portal_host():
    # Hygiene: pasted user:pass@ credentials must not ride the returned deep link.
    provider = MicrosoftSentinelProvider(
        _view_in_siem_config(sentinel_url=SENTINEL_URL.replace("https://", "https://user:pass@"))
    )
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is True
    assert "user:pass@" not in result.url
    assert result.url.startswith("https://portal.azure.com/#view/")


def test_config_accepts_dotted_sigma_field():
    cfg = MicrosoftSentinelConfig.model_validate(_view_in_siem_config(remapped_sigma_rule_id_field="detection.rule_id"))
    assert cfg.remapped_sigma_rule_id_field == "detection.rule_id"


async def test_build_view_in_siem_url_rejects_injection_in_sigma_field():
    # The field name is interpolated raw into the KQL — the config allowlist is the gate.
    provider = MicrosoftSentinelProvider(_view_in_siem_config(remapped_sigma_rule_id_field='x" or 1==1'))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


# ---------------------------------------------------------------------------
# Response serialization (regression: must not pass a model instance to model_validate)
# ---------------------------------------------------------------------------


def test_response_schema_accepts_result_model_dump():
    from apps.core.schemas import IntegrationViewInSiemResponse
    from apps.integrations.base import ViewInSiemResult

    result = ViewInSiemResult(status=True, url="http://x", query="q")
    resp = IntegrationViewInSiemResponse.model_validate(result.model_dump())
    assert resp.status is True and resp.url == "http://x" and resp.query == "q"


# ---------------------------------------------------------------------------
# Capability-aware check_connection (URL path, no network)
# ---------------------------------------------------------------------------


async def test_check_connection_view_in_siem_only_profile_is_valid():
    provider = MicrosoftSentinelProvider({"sentinel_url": SENTINEL_URL})
    result = await provider.check_connection()
    assert result.success is True
    assert "View in SIEM" in result.message


async def test_check_connection_rejects_defender_url():
    provider = MicrosoftSentinelProvider({"sentinel_url": "https://security.microsoft.com/logs"})
    result = await provider.check_connection()
    assert result.success is False
    assert "Defender" in result.message


async def test_check_connection_empty_profile_gives_guidance():
    provider = MicrosoftSentinelProvider({})
    result = await provider.check_connection()
    assert result.success is False
    assert "View in SIEM" in result.message and "credentials" in result.message.lower()


async def test_check_connection_url_without_workspace_fails():
    provider = MicrosoftSentinelProvider({"sentinel_url": "https://portal.azure.com/#home"})
    result = await provider.check_connection()
    assert result.success is False
    assert "workspace" in result.message.lower()


async def test_check_connection_invalid_config_is_value_free():
    # A secret pasted into a wrong-typed field must not echo back through message or details.
    provider = MicrosoftSentinelProvider({"table_names": "SECRET-SENTINEL"})
    result = await provider.check_connection()
    assert result.success is False
    assert "Invalid config" in result.message
    assert "SECRET-SENTINEL" not in result.message
    assert "SECRET-SENTINEL" not in str(result.details)
    for err in result.details["validation_errors"]:
        assert set(err) == {"loc", "msg", "type"}


# ---------------------------------------------------------------------------
# Manager dispatch (AsyncMock DB/DAO)
# ---------------------------------------------------------------------------


def _manager_with(integration) -> IntegrationsManager:
    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    mgr.dao.get_by_id.return_value = integration
    return mgr


_SENTINEL_MANAGER_CONFIG = {"sentinel_url": SENTINEL_URL, "table_names": ["SecurityEvent"]}


def test_manager_validate_config_message_is_value_free():
    # Create/update path: the domain ValidationError carries loc/msg only, never the raw value.
    with pytest.raises(ValidationError) as exc_info:
        IntegrationsManager._validate_config(MicrosoftSentinelProvider, {"table_names": "SECRET-SENTINEL"})
    assert "Invalid config" in str(exc_info.value)
    assert "SECRET-SENTINEL" not in str(exc_info.value)


async def test_manager_rejects_unsupported_type(fake_integration, activity_log):
    mgr = _manager_with(fake_integration("github", {}))
    with pytest.raises(BadRequestError):
        await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4())


async def test_manager_raises_not_found_for_missing_chain(fake_integration, fake_chains_dao):
    fake_chains_dao.get_chain_link_context.return_value = None
    mgr = _manager_with(fake_integration("microsoft_sentinel", _SENTINEL_MANAGER_CONFIG))
    with pytest.raises(NotFoundError):
        await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4())


async def test_manager_success_with_empty_hostname_and_no_rules(fake_integration, fake_chains_dao, activity_log):
    fake_chains_dao.get_chain_link_context.return_value = (None, START, END)
    mgr = _manager_with(fake_integration("microsoft_sentinel", _SENTINEL_MANAGER_CONFIG))
    result = await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4())
    assert result.status is True
    assert result.url is not None
    assert "| where Host" not in result.query
    activity_log.assert_awaited_once()


async def test_manager_single_rule_narrows_to_one(fake_integration, fake_chains_dao):
    rule = uuid.uuid4()
    other = uuid.uuid4()
    fake_chains_dao.get_distinct_sigma_rule_ids.return_value = [str(rule), str(other)]
    mgr = _manager_with(fake_integration("microsoft_sentinel", _SENTINEL_MANAGER_CONFIG))
    result = await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4(), sigma_rule_id=rule)
    assert result.status is True
    # Only the requested rule is in the query (equality), the other chain rule is excluded.
    assert f'SigmaRuleID == "{rule}"' in result.query
    assert str(other) not in result.query


async def test_manager_rule_not_in_chain_is_404(fake_integration, fake_chains_dao):
    fake_chains_dao.get_distinct_sigma_rule_ids.return_value = [str(uuid.uuid4())]  # some other rule
    mgr = _manager_with(fake_integration("microsoft_sentinel", _SENTINEL_MANAGER_CONFIG))
    with pytest.raises(NotFoundError):
        await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4(), sigma_rule_id=uuid.uuid4())


@pytest.mark.parametrize(
    ("integration_type", "config", "url_prefix", "query_marker"),
    [
        (
            "splunk",
            {"splunk_url": "https://splunk.example.com:8000", "index_names": ["main"]},
            "https://splunk.example.com:8000/en-US/app/search/search?",
            'SigmaRuleID=="{rule}"',
        ),
        (
            "elastic_stack",
            {"kibana_url": "https://kibana.example.com:5601", "index_pattern_id": "idx-1"},
            "https://kibana.example.com:5601/app/discover#/?_g=",
            'SigmaRuleID : "{rule}"',
        ),
        (
            "opensearch",
            {"opensearch_url": "https://opensearch.example.com:5601", "index_pattern_id": "idx-1"},
            "https://opensearch.example.com:5601/app/discover#/?_g=",
            'SigmaRuleID : "{rule}"',
        ),
    ],
)
async def test_manager_dispatches_to_provider(
    integration_type, config, url_prefix, query_marker, fake_integration, fake_chains_dao, activity_log
):
    # Same generic endpoint flow for every View-in-SIEM provider; only type/config/link differ.
    rule = uuid.uuid4()
    fake_chains_dao.get_distinct_sigma_rule_ids.return_value = [str(rule)]
    mgr = _manager_with(fake_integration(integration_type, config))
    result = await mgr.build_view_in_siem_url(uuid.uuid4(), None, uuid.uuid4())
    assert result.status is True
    assert result.url.startswith(url_prefix)
    assert query_marker.format(rule=rule) in result.query
    activity_log.assert_awaited_once()
