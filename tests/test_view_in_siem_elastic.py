"""Tests for the Elastic Stack ``view_in_siem`` capability (Kibana Discover deep links).

The deep-link shape is ``{base}[/s/{space}]/app/discover#/?_g=(...)&_a=(...)`` — rison-encoded
state where ``_g`` carries the absolute UTC window (auto-refresh paused) and ``_a`` targets the
configured data view (``dataSource:(dataViewId:'<id>',type:dataView)``) with a ``language:kuery``
query (no in-query time filter) sorted by ``@timestamp`` descending.
"""

from datetime import UTC, datetime, timedelta, timezone
from urllib.parse import unquote

import apps.integrations.providers  # noqa: F401  -- register all providers for registry lookups
from apps.core.schemas import IntegrationViewInSiemResponse
from apps.integrations._query_escaping import _rison_string
from apps.integrations.base import (
    DEFAULT_SIGMA_RULE_ID_FIELD,
    ChainSearchContext,
    ViewInSiemIntegration,
    get_provider_capabilities,
)
from apps.integrations.providers.elastic_stack import (
    _DEFAULT_HOST_NAME_FIELDS,
    ElasticStackConfig,
    ElasticStackProvider,
    _time_boundary,
)

KIBANA_URL = "https://kibana.example.com:5601"
INDEX_PATTERN_ID = "ff959d40-b880-11e8-a6d9-e546fe2bba5f"
START = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
END = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)
G_STATE = (
    "_g=(filters:!(),refreshInterval:(pause:!t,value:0),"
    "time:(from:'2026-06-01T10:00:00.000Z',mode:absolute,to:'2026-06-02T11:30:00.000Z'))"
)


def _view_in_siem_config(**overrides) -> dict:
    cfg = {
        "kibana_url": KIBANA_URL,
        "index_pattern_id": INDEX_PATTERN_ID,
        "host_name_fields": ["host.name.keyword", "source.hostname"],
    }
    cfg.update(overrides)
    return cfg


def _ctx(sigma_rule_ids: list[str], hostname: str | None, start=START, end=END) -> ChainSearchContext:
    return ChainSearchContext(sigma_rule_ids=sigma_rule_ids, hostname=hostname, start=start, end=end)


# ---------------------------------------------------------------------------
# Capability & config schema
# ---------------------------------------------------------------------------


def test_provider_advertises_view_in_siem_capability():
    assert issubclass(ElasticStackProvider, ViewInSiemIntegration)
    assert get_provider_capabilities(ElasticStackProvider) == ["check_connection", "view_in_siem"]


def test_config_defaults():
    cfg = ElasticStackConfig()
    assert cfg.kibana_url == ""
    assert cfg.kibana_space_id == ""
    assert cfg.index_pattern_id == ""
    assert cfg.remapped_sigma_rule_id_field == ""
    assert cfg.host_name_fields == list(_DEFAULT_HOST_NAME_FIELDS)


def test_config_host_name_fields_default_is_not_shared():
    a = ElasticStackConfig()
    a.host_name_fields.append("extra.field")
    b = ElasticStackConfig()
    assert "extra.field" not in b.host_name_fields


def test_config_schema_exposes_host_name_fields_default_for_form_prefill():
    # The frontend renders the create form from config_schema; the default MUST be present so the
    # field is pre-filled (default_factory would NOT serialize into the JSON Schema — regression guard).
    prop = ElasticStackConfig.model_json_schema()["properties"]["host_name_fields"]
    assert prop.get("default") == list(_DEFAULT_HOST_NAME_FIELDS)


def test_no_sensitive_fields():
    assert ElasticStackProvider.get_sensitive_fields() == set()


# ---------------------------------------------------------------------------
# KQL builder (no in-query time filter — period lives in the URL _g state)
# ---------------------------------------------------------------------------


def test_kql_builder_full_query():
    kql = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword", "source.hostname"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1", "uuid2"],
        hostname="srv-ad-001.example.com",
    )
    assert kql == (
        '(host.name.keyword : "srv-ad-001.example.com" or source.hostname : "srv-ad-001.example.com")'
        ' and SigmaRuleID : ("uuid1" or "uuid2")'
    )


def test_kql_builder_single_host_field_has_no_parens():
    kql = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1"],
        hostname="host-1",
    )
    assert kql == 'host.name.keyword : "host-1" and SigmaRuleID : "uuid1"'


def test_kql_sigma_filter_single_vs_many():
    one = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1"],
        hostname=None,
    )
    many = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=["uuid1", "uuid2"],
        hostname=None,
    )
    assert one == 'SigmaRuleID : "uuid1"'
    assert many == 'SigmaRuleID : ("uuid1" or "uuid2")'


def test_kql_builder_empty_conditions_match_all():
    kql = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=[],
        hostname=None,
    )
    assert kql == ""


def test_kql_builder_uses_remapped_sigma_field():
    kql = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="detection.rule_id",
        sigma_rule_ids=["uuid1"],
        hostname=None,
    )
    assert kql == 'detection.rule_id : "uuid1"'


def test_kql_builder_escapes_hostname():
    kql = ElasticStackProvider._build_view_in_siem_kql(
        host_name_fields=["host.name.keyword"],
        sigma_rule_id_field="SigmaRuleID",
        sigma_rule_ids=[],
        hostname='evil" or 1:1 \\x',
    )
    assert kql == 'host.name.keyword : "evil\\" or 1:1 \\\\x"'


# ---------------------------------------------------------------------------
# rison / time-boundary helpers
# ---------------------------------------------------------------------------


def test_rison_string_escapes_quote_and_bang():
    assert _rison_string("O'Brien!x") == "'O!'Brien!!x'"
    assert _rison_string("plain") == "'plain'"


def test_time_boundary_floors_start_and_ceils_end_to_millisecond():
    value = datetime(2026, 6, 1, 10, 0, 0, 123456, tzinfo=UTC)
    assert _time_boundary(value, ceil=False) == "2026-06-01T10:00:00.123Z"
    assert _time_boundary(value, ceil=True) == "2026-06-01T10:00:00.124Z"
    whole_ms = datetime(2026, 6, 1, 10, 0, 0, 123000, tzinfo=UTC)
    assert _time_boundary(whole_ms, ceil=True) == "2026-06-01T10:00:00.123Z"


def test_time_boundary_normalizes_non_utc():
    tz_plus2 = timezone(timedelta(hours=2))
    value = datetime(2026, 6, 1, 12, 0, 0, tzinfo=tz_plus2)
    assert _time_boundary(value, ceil=False) == "2026-06-01T10:00:00.000Z"


# ---------------------------------------------------------------------------
# Kibana base URL resolution
# ---------------------------------------------------------------------------


def test_resolve_console_base_plain():
    base, error = ElasticStackProvider._resolve_console_base(KIBANA_URL)
    assert error is None and base == KIBANA_URL


def test_resolve_console_base_strips_app_route_and_keeps_proxy_prefix():
    base, error = ElasticStackProvider._resolve_console_base("https://host.example.com/kibana/app/discover")
    assert error is None and base == "https://host.example.com/kibana"


def test_resolve_console_base_strips_pasted_space_segment():
    base, error = ElasticStackProvider._resolve_console_base("https://host.example.com/s/other-space/app/home")
    assert error is None and base == "https://host.example.com"


def test_resolve_console_base_rejects_unparseable():
    base, error = ElasticStackProvider._resolve_console_base("kibana.example.com:5601")
    assert base is None and "http" in error


# ---------------------------------------------------------------------------
# build_view_in_siem_url (end-to-end provider call) — deep-link URL shape
# ---------------------------------------------------------------------------


async def test_build_view_in_siem_url_success():
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1", "uuid2"], "srv-ad-001.example.com"))
    assert result.status is True
    url = result.url
    assert url is not None
    assert url.startswith(f"{KIBANA_URL}/app/discover#/?_g=")
    decoded = unquote(url)
    assert G_STATE in decoded
    assert f"dataSource:(dataViewId:'{INDEX_PATTERN_ID}',type:dataView)" in decoded
    assert f"query:(language:kuery,query:'{result.query}')" in decoded
    assert "columns:!()" in decoded and "sort:!(!('@timestamp',desc))" in decoded


async def test_build_view_in_siem_url_with_space():
    provider = ElasticStackProvider(_view_in_siem_config(kibana_space_id="soc-team"))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert f"{KIBANA_URL}/s/soc-team/app/discover#/?" in result.url


async def test_build_view_in_siem_url_default_space_has_no_prefix():
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert "/s/" not in result.url


async def test_build_view_in_siem_url_time_normalizes_non_utc():
    tz_plus2 = timezone(timedelta(hours=2))
    start = datetime(2026, 6, 1, 12, 0, 0, tzinfo=tz_plus2)
    end = datetime(2026, 6, 2, 13, 30, 0, tzinfo=tz_plus2)
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", start=start, end=end))
    assert G_STATE in unquote(result.url)


async def test_build_view_in_siem_url_end_rounds_up_to_millisecond():
    end = END.replace(microsecond=1)  # sub-millisecond tail
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", end=end))
    assert "to:'2026-06-02T11:30:00.001Z'" in unquote(result.url)


async def test_build_view_in_siem_url_swaps_backwards_window():
    # A reversed window (clock skew / producer bug) must not produce an empty _g range.
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", start=END, end=START))
    assert G_STATE in unquote(result.url)


async def test_build_view_in_siem_url_rison_escapes_query_quotes():
    # KQL double quotes pass through rison unescaped; an apostrophe in the hostname must be
    # rison-escaped (!') inside the single-quoted query string.
    provider = ElasticStackProvider(_view_in_siem_config(host_name_fields=["host.name.keyword"]))
    result = await provider.build_view_in_siem_url(_ctx([], "o'brien-host"))
    decoded = unquote(result.url)
    assert "query:'host.name.keyword : \"o!'brien-host\"'" in decoded
    assert result.query == 'host.name.keyword : "o\'brien-host"'


async def test_build_view_in_siem_url_without_hostname_or_rules():
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx([], None))
    assert result.status is True
    assert result.query == ""
    assert "query:(language:kuery,query:'')" in unquote(result.url)


async def test_build_view_in_siem_url_requires_kibana_url():
    provider = ElasticStackProvider(_view_in_siem_config(kibana_url=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    assert "URL is not configured" in result.message


async def test_build_view_in_siem_url_requires_index_pattern_id():
    provider = ElasticStackProvider(_view_in_siem_config(index_pattern_id=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "index pattern" in result.message.lower()


async def test_build_view_in_siem_url_invalid_url_keeps_query_preview():
    provider = ElasticStackProvider(_view_in_siem_config(kibana_url="kibana.example.com:5601"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    # Query is still generated for preview even when the URL can't be built.
    assert result.query is not None


async def test_build_view_in_siem_url_defaults_sigma_field():
    provider = ElasticStackProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert f'{DEFAULT_SIGMA_RULE_ID_FIELD} : "uuid1"' in result.query


async def test_build_view_in_siem_url_invalid_config_types():
    provider = ElasticStackProvider(_view_in_siem_config(host_name_fields="notalist"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


def test_config_accepts_dotted_sigma_field():
    cfg = ElasticStackConfig.model_validate(_view_in_siem_config(remapped_sigma_rule_id_field="detection.rule_id"))
    assert cfg.remapped_sigma_rule_id_field == "detection.rule_id"


async def test_build_view_in_siem_url_rejects_injection_in_sigma_field():
    # The field name is interpolated raw into the KQL — the config allowlist is the gate.
    provider = ElasticStackProvider(_view_in_siem_config(remapped_sigma_rule_id_field='x" or 1==1'))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


def test_response_schema_accepts_result_model_dump():
    from apps.integrations.base import ViewInSiemResult

    result = ViewInSiemResult(status=True, url="http://x", query="q")
    resp = IntegrationViewInSiemResponse.model_validate(result.model_dump())
    assert resp.status is True and resp.url == "http://x" and resp.query == "q"


# ---------------------------------------------------------------------------
# check_connection (offline URL validation only — the profile carries no credentials)
# ---------------------------------------------------------------------------


async def test_check_connection_valid_url():
    provider = ElasticStackProvider({"kibana_url": KIBANA_URL})
    result = await provider.check_connection()
    assert result.success is True
    assert "View in SIEM" in result.message


async def test_check_connection_empty_profile_gives_guidance():
    provider = ElasticStackProvider({})
    result = await provider.check_connection()
    assert result.success is False
    assert "Kibana URL" in result.message


async def test_check_connection_rejects_unparseable_url():
    provider = ElasticStackProvider({"kibana_url": "kibana.example.com"})
    result = await provider.check_connection()
    assert result.success is False
    assert "http" in result.message


async def test_check_connection_invalid_config_types():
    provider = ElasticStackProvider({"host_name_fields": "notalist"})
    result = await provider.check_connection()
    assert result.success is False
    assert "Invalid config" in result.message
    assert result.details and "validation_errors" in result.details
