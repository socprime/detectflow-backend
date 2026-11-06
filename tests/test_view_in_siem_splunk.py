"""Tests for the Splunk ``view_in_siem`` capability (deep-link search).

The deep-link shape is ``{base}/en-US/app/{app}/search?q=search%20<SPL>&earliest=<epoch>&latest=<epoch>``
— the chain window rides the URL params as UTC epoch seconds (``earliest`` inclusive, ``latest``
exclusive → bumped one second past the window end). The SPL unions indexes as one main search
plus one ``| append [ search index=... ]`` block per additional index, and each block re-asserts
the window as ``| where _time >= <earliest> AND _time < <latest>`` (append subsearches must not
rely on time-picker inheritance).
"""

from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlparse

import pytest

import apps.integrations.providers  # noqa: F401  -- register all providers for registry lookups
from apps.core.schemas import IntegrationViewInSiemResponse
from apps.integrations.base import (
    DEFAULT_SIGMA_RULE_ID_FIELD,
    ChainSearchContext,
    ConnectionResult,
    ViewInSiemIntegration,
    get_provider_capabilities,
)
from apps.integrations.providers.splunk import (
    _DEFAULT_HOST_NAME_FIELDS,
    SplunkConfig,
    SplunkProvider,
    _spl_field_expr,
)

SPLUNK_URL = "https://splunk.example.com:8000"
START = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
END = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)
START_EPOCH = int(START.timestamp())
END_EPOCH = int(END.timestamp()) + 1  # ``latest`` is exclusive — one second past the window end


def _query_params(url: str) -> dict[str, str]:
    return {key: values[0] for key, values in parse_qs(urlparse(url).query).items()}


def _view_in_siem_config(**overrides) -> dict:
    cfg = {
        "splunk_url": SPLUNK_URL,
        "index_names": ["main", "wineventlog"],
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
    assert issubclass(SplunkProvider, ViewInSiemIntegration)
    assert get_provider_capabilities(SplunkProvider) == ["check_connection", "view_in_siem"]


def test_config_defaults():
    cfg = SplunkConfig()
    assert cfg.splunk_url == ""
    assert cfg.custom_search_app == ""
    assert cfg.index_names == []
    assert cfg.remapped_sigma_rule_id_field == ""
    assert cfg.host_name_fields == list(_DEFAULT_HOST_NAME_FIELDS)
    # RELEASE SCOPE: the management REST fields are commented out of the schema so the frontend
    # form (rendered from it) matches the design. Unskip/restore when the REST group ships.
    for rest_field in ("host", "port", "token", "ssl_verify"):
        assert rest_field not in SplunkConfig.model_fields


def test_config_host_name_fields_default_is_not_shared():
    a = SplunkConfig()
    a.host_name_fields.append("Extra")
    b = SplunkConfig()
    assert "Extra" not in b.host_name_fields


def test_config_schema_exposes_host_name_fields_default_for_form_prefill():
    # The frontend renders the create form from config_schema; the default MUST be present so the
    # field is pre-filled (default_factory would NOT serialize into the JSON Schema — regression guard).
    prop = SplunkConfig.model_json_schema()["properties"]["host_name_fields"]
    assert prop.get("default") == list(_DEFAULT_HOST_NAME_FIELDS)


def test_token_remains_sensitive_while_rest_group_commented_out():
    # ``token`` is commented out with the REST group (release scope), but profiles saved before
    # the comment-out may still carry it — the explicit override keeps it masked in GET responses.
    assert SplunkProvider.get_sensitive_fields() == {"token"}


def test_existing_rest_only_profile_still_validates():
    # Pre-View-in-SIEM profiles stored only the management REST fields — they must stay loadable
    # (Pydantic ignores the now-commented-out keys) even though the REST branch is dormant.
    cfg = SplunkConfig.model_validate({"host": "https://splunk.local", "port": 8089, "token": "t", "ssl_verify": False})
    assert cfg.splunk_url == ""
    assert not hasattr(cfg, "host")


# ---------------------------------------------------------------------------
# SPL builder (per-index append blocks; window re-asserted in every block)
# ---------------------------------------------------------------------------

TIME_FILTER = f"| where _time >= {START_EPOCH} AND _time < {END_EPOCH}"


def _spl(**overrides) -> str:
    kwargs = {
        "index_names": ["main"],
        "host_name_fields": ["host"],
        "sigma_rule_id_field": "SigmaRuleID",
        "sigma_rule_ids": ["uuid1"],
        "hostname": "h",
        "start_epoch": START_EPOCH,
        "end_epoch": END_EPOCH,
    }
    kwargs.update(overrides)
    return SplunkProvider._build_view_in_siem_spl(**kwargs)


def test_spl_builder_single_index_full_query():
    spl = _spl(
        host_name_fields=["Computer", "HostName"],
        sigma_rule_ids=["uuid1", "uuid2"],
        hostname="srv-ad-001.example.com",
    )
    assert spl == (
        f'index="main"\n'
        f"{TIME_FILTER}\n"
        f"| eval Host=coalesce(Computer, HostName)\n"
        f'| where Host=="srv-ad-001.example.com" AND SigmaRuleID IN ("uuid1", "uuid2")\n'
        f"| sort 0 - _time"
    )


def test_spl_builder_multi_index_appends_block_per_extra_index():
    spl = _spl(
        index_names=["wineventlog", "syslog", "firewall"],
        host_name_fields=["Computer", "HostName"],
        sigma_rule_ids=["uuid1"],
        hostname="srv-ad-001.example.com",
    )
    block = (
        f"{TIME_FILTER}\n"
        f"| eval Host=coalesce(Computer, HostName)\n"
        f'| where Host=="srv-ad-001.example.com" AND SigmaRuleID=="uuid1"'
    )
    indented = "\n".join(f"    {line}" for line in block.split("\n"))
    assert spl == (
        f'index="wineventlog"\n'
        f"{block}\n"
        f"| append\n"
        f"    [\n"
        f'    search index="syslog"\n'
        f"{indented}\n"
        f"    ]\n"
        f"| append\n"
        f"    [\n"
        f'    search index="firewall"\n'
        f"{indented}\n"
        f"    ]\n"
        f"| sort 0 - _time"
    )
    # Exactly one trailing sort; every block carries its own time bounds.
    assert spl.count("| sort") == 1
    assert spl.count(f"_time >= {START_EPOCH}") == 3


def test_spl_builder_time_bounds_in_every_block_no_search_time_terms():
    spl = _spl(index_names=["a", "b"])
    # The window is asserted per block via where _time, never via earliest=/latest= search terms
    # (those would override the picker the URL params drive).
    assert spl.count(TIME_FILTER) == 2
    assert "earliest" not in spl and "latest" not in spl


def test_spl_builder_single_host_field_has_no_coalesce():
    spl = _spl(hostname="host-1")
    assert "| eval Host=host" in spl
    assert "coalesce(" not in spl


def test_spl_sigma_filter_equals_for_one_in_for_many():
    one = _spl(sigma_rule_ids=["uuid1"])
    many = _spl(sigma_rule_ids=["uuid1", "uuid2"])
    assert 'SigmaRuleID=="uuid1"' in one and "SigmaRuleID IN" not in one
    assert 'SigmaRuleID IN ("uuid1", "uuid2")' in many


def test_spl_builder_keeps_only_time_filter_without_conditions():
    spl = _spl(sigma_rule_ids=[], hostname=None)
    assert spl.count("| where") == 1  # only the time filter
    assert "Host==" not in spl and "SigmaRuleID" not in spl
    assert TIME_FILTER in spl
    assert "| sort 0 - _time" in spl


def test_spl_builder_uses_remapped_sigma_field_and_quotes_dotted_names():
    spl = _spl(sigma_rule_id_field="detection.rule_id", hostname=None)
    # Dotted field names must be single-quoted to act as field references in eval/where.
    assert "'detection.rule_id'==\"uuid1\"" in spl


def test_spl_builder_escapes_hostname_and_index():
    spl = _spl(
        index_names=['my"index', 'other"index'],
        sigma_rule_ids=[],
        hostname='evil" OR 1==1\\x',
    )
    assert spl.startswith('index="my\\"index"')
    assert 'search index="other\\"index"' in spl
    assert 'Host=="evil\\" OR 1==1\\\\x"' in spl


def test_spl_builder_escapes_control_characters():
    # A raw newline would terminate the SPL double-quoted literal and splice stray query text.
    spl = _spl(sigma_rule_ids=[], hostname="evil\n| delete\thost")
    assert "\n| delete" not in spl
    assert 'Host=="evil\\n| delete\\thost"' in spl


def test_spl_field_expr_plain_and_dotted():
    assert _spl_field_expr("host") == "host"
    assert _spl_field_expr("my.field") == "'my.field'"
    assert _spl_field_expr("event id") == "'event id'"


def test_spl_field_expr_escapes_apostrophe_and_backslash():
    # Defense in depth: even called directly, a name can't break out of the quoted reference.
    assert _spl_field_expr("x' | delete") == "'x\\' | delete'"
    assert _spl_field_expr("a\\b c") == "'a\\\\b c'"


# ---------------------------------------------------------------------------
# Field-name allowlist (stored-injection gate on remapped_sigma_rule_id_field)
# ---------------------------------------------------------------------------


def test_config_accepts_dotted_sigma_field_and_strips():
    cfg = SplunkConfig.model_validate(_view_in_siem_config(remapped_sigma_rule_id_field=" detection.rule_id "))
    assert cfg.remapped_sigma_rule_id_field == "detection.rule_id"


async def test_build_view_in_siem_url_rejects_injection_in_sigma_field():
    provider = SplunkProvider(_view_in_siem_config(remapped_sigma_rule_id_field='x" or 1==1'))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


async def test_rejects_apostrophe_breakout_in_sigma_field():
    config = _view_in_siem_config(remapped_sigma_rule_id_field="x' | delete")
    result = await SplunkProvider(config).build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False and "Invalid config" in result.message
    check = await SplunkProvider(config).check_connection()
    assert check.success is False and "Invalid config" in check.message


# ---------------------------------------------------------------------------
# build_view_in_siem_url (end-to-end provider call) — deep-link URL shape
# ---------------------------------------------------------------------------


async def test_build_view_in_siem_url_success():
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1", "uuid2"], "srv-ad-001.example.com"))
    assert result.status is True
    url = result.url
    assert url is not None
    assert url.startswith(f"{SPLUNK_URL}/en-US/app/search/search?q=search%20")
    params = _query_params(url)
    # The q param prepends the literal ``search `` command; the preview query omits it.
    assert params["q"] == f"search {result.query}"
    assert params["earliest"] == str(START_EPOCH)
    assert params["latest"] == str(END_EPOCH)


async def test_build_view_in_siem_url_custom_search_app():
    provider = SplunkProvider(_view_in_siem_config(custom_search_app="soc_app"))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert "/en-US/app/soc_app/search?" in result.url


async def test_build_view_in_siem_url_drops_pasted_path():
    pasted = f"{SPLUNK_URL}/en-US/app/search/search?q=old"
    provider = SplunkProvider(_view_in_siem_config(splunk_url=pasted))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert result.status is True
    assert result.url.startswith(f"{SPLUNK_URL}/en-US/app/search/search?q=search%20")
    assert "q=old" not in result.url


async def test_build_view_in_siem_url_cloud_without_port():
    provider = SplunkProvider(_view_in_siem_config(splunk_url="https://mystack.splunkcloud.com"))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert result.url.startswith("https://mystack.splunkcloud.com/en-US/app/search/search?")


async def test_build_view_in_siem_url_preserves_proxy_mount_prefix():
    # Splunk Web behind web.conf root_endpoint / reverse proxy: the mount prefix must survive.
    pasted = "https://proxy.example.com/splunk/en-US/app/search/search?q=old"
    provider = SplunkProvider(_view_in_siem_config(splunk_url=pasted))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert result.url.startswith("https://proxy.example.com/splunk/en-US/app/search/search?q=search%20")


def test_resolve_splunk_base_drops_non_app_page_route():
    base, error = SplunkProvider._resolve_splunk_base("https://splunk.example.com:8000/en-GB/account/login")
    assert error is None and base == "https://splunk.example.com:8000"


def test_resolve_splunk_base_keeps_plain_mount_prefix():
    base, error = SplunkProvider._resolve_splunk_base("https://proxy.example.com/splunk")
    assert error is None and base == "https://proxy.example.com/splunk"


def test_resolve_splunk_base_strips_userinfo():
    # Hygiene: pasted user:pass@ credentials must not ride the returned deep link.
    base, error = SplunkProvider._resolve_splunk_base("https://user:pass@splunk.example.com:8000/en-US/app/search")
    assert error is None and base == "https://splunk.example.com:8000"


async def test_build_view_in_siem_url_epoch_normalizes_non_utc():
    # 12:00 at +02:00 == 10:00Z — the epoch params must be identical to the UTC window.
    tz_plus2 = timezone(timedelta(hours=2))
    start = datetime(2026, 6, 1, 12, 0, 0, tzinfo=tz_plus2)
    end = datetime(2026, 6, 2, 13, 30, 0, tzinfo=tz_plus2)
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", start=start, end=end))
    params = _query_params(result.url)
    assert params["earliest"] == str(START_EPOCH)
    assert params["latest"] == str(END_EPOCH)


async def test_build_view_in_siem_url_latest_covers_subsecond_end():
    # latest is exclusive — a sub-second end floors then bumps one second, keeping the tail inside.
    end = END.replace(microsecond=500000)
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h", end=end))
    assert _query_params(result.url)["latest"] == str(END_EPOCH)


async def test_build_view_in_siem_url_zero_width_window_is_valid():
    # Splunk rejects latest <= earliest; a single-instant chain still yields a 1-second window.
    instant = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h", start=instant, end=instant))
    params = _query_params(result.url)
    assert int(params["latest"]) == int(params["earliest"]) + 1


async def test_build_view_in_siem_url_swaps_backwards_window():
    # A reversed window (clock skew / producer bug) must not produce latest <= earliest.
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h", start=END, end=START))
    params = _query_params(result.url)
    assert params["earliest"] == str(START_EPOCH)
    assert params["latest"] == str(END_EPOCH)
    # The in-query time filter must carry the same (swapped) bounds as the URL params.
    assert f"| where _time >= {START_EPOCH} AND _time < {END_EPOCH}" in result.query


async def test_build_view_in_siem_url_without_hostname_or_rules():
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx([], None))
    assert result.status is True
    assert result.url is not None
    # No host/sigma conditions — only the per-block time filter remains.
    assert "Host==" not in result.query and "SigmaRuleID" not in result.query
    assert f"| where _time >= {START_EPOCH} AND _time < {END_EPOCH}" in result.query


async def test_build_view_in_siem_url_invalid_config_types():
    provider = SplunkProvider(_view_in_siem_config(index_names="notalist"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


async def test_build_view_in_siem_url_invalid_config_does_not_echo_value():
    # A secret pasted into the wrong-typed field must not be echoed back in the error message.
    provider = SplunkProvider(_view_in_siem_config(index_names="SECRET-SENTINEL"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message
    assert "SECRET-SENTINEL" not in result.message
    assert "index_names" in result.message  # loc/msg are still actionable


async def test_build_view_in_siem_url_requires_splunk_url():
    provider = SplunkProvider(_view_in_siem_config(splunk_url=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    assert "URL is not configured" in result.message


async def test_build_view_in_siem_url_requires_index_names():
    provider = SplunkProvider(_view_in_siem_config(index_names=[]))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "index name" in result.message.lower()


async def test_build_view_in_siem_url_invalid_url_keeps_query_preview():
    provider = SplunkProvider(_view_in_siem_config(splunk_url="splunk.example.com:8000"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    # Query is still generated for preview even when the URL can't be built.
    assert result.query is not None


async def test_build_view_in_siem_url_defaults_sigma_field():
    provider = SplunkProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert f'{DEFAULT_SIGMA_RULE_ID_FIELD}=="uuid1"' in result.query


def test_response_schema_accepts_result_model_dump():
    from apps.integrations.base import ViewInSiemResult

    result = ViewInSiemResult(status=True, url="http://x", query="q")
    resp = IntegrationViewInSiemResponse.model_validate(result.model_dump())
    assert resp.status is True and resp.url == "http://x" and resp.query == "q"


# ---------------------------------------------------------------------------
# Capability-aware check_connection
# ---------------------------------------------------------------------------


async def test_check_connection_view_in_siem_only_profile_is_valid():
    provider = SplunkProvider({"splunk_url": SPLUNK_URL})
    result = await provider.check_connection()
    assert result.success is True
    assert "View in SIEM" in result.message


async def test_check_connection_empty_profile_gives_guidance():
    provider = SplunkProvider({})
    result = await provider.check_connection()
    assert result.success is False
    assert "Splunk URL" in result.message


async def test_check_connection_rejects_unparseable_url():
    provider = SplunkProvider({"splunk_url": "splunk.example.com"})
    result = await provider.check_connection()
    assert result.success is False
    assert "http" in result.message


async def test_check_connection_invalid_config_types():
    provider = SplunkProvider({"index_names": "notalist"})
    result = await provider.check_connection()
    assert result.success is False
    assert "Invalid config" in result.message
    assert result.details and "validation_errors" in result.details


async def test_check_connection_invalid_config_details_are_value_free():
    # Pydantic embeds the submitted value in str(e) and errors()['input'] — neither may reach
    # the response: only loc/msg/type survive sanitization.
    provider = SplunkProvider({"index_names": "SECRET-SENTINEL"})
    result = await provider.check_connection()
    assert result.success is False
    assert "SECRET-SENTINEL" not in result.message
    assert "SECRET-SENTINEL" not in str(result.details)
    for err in result.details["validation_errors"]:
        assert set(err) == {"loc", "msg", "type"}


def test_resolve_management_base_appends_default_port():
    base, error = SplunkProvider._resolve_management_base("https://splunk.example.com", 8089)
    assert error is None and base == "https://splunk.example.com:8089"


def test_resolve_management_base_strips_trailing_slash_and_path():
    # A trailing slash used to land the port in the path (…example.com/:8089/…).
    base, error = SplunkProvider._resolve_management_base("https://splunk.example.com/", 8089)
    assert error is None and base == "https://splunk.example.com:8089"
    base, error = SplunkProvider._resolve_management_base("https://splunk.example.com/en-US/app", 8089)
    assert error is None and base == "https://splunk.example.com:8089"


def test_resolve_management_base_keeps_embedded_port():
    # An embedded port used to double up (…:8089:8089).
    base, error = SplunkProvider._resolve_management_base("https://splunk.example.com:8089", 8089)
    assert error is None and base == "https://splunk.example.com:8089"
    base, error = SplunkProvider._resolve_management_base("https://splunk.example.com:9999", 8089)
    assert error is None and base == "https://splunk.example.com:9999"


def test_resolve_management_base_rejects_missing_scheme():
    # Without a scheme urlparse reads the host as the scheme — the probe would hit nothing.
    base, error = SplunkProvider._resolve_management_base("splunk.example.com", 8089)
    assert base is None and "management host" in error


# With the management REST fields commented out of SplunkConfig (release scope), parsed configs
# never expose host/token, so check_connection always takes the URL branch. Unskip when they ship.
_REST_GROUP_UNRELEASED = pytest.mark.skip(
    reason="Splunk management REST fields are commented out of SplunkConfig for this release"
)


@_REST_GROUP_UNRELEASED
async def test_check_connection_rest_unparseable_host_gives_clear_message():
    provider = SplunkProvider({"host": "splunk.example.com", "token": "t"})
    result = await provider.check_connection()
    assert result.success is False
    assert "management host" in result.message


@_REST_GROUP_UNRELEASED
async def test_check_connection_prefers_rest_probe_when_credentials_set(monkeypatch):
    # host+token wins over splunk_url — the REST probe is the stronger signal.
    probe = AsyncMock(return_value=ConnectionResult(success=True, message="probed"))
    monkeypatch.setattr(SplunkProvider, "_check_connection_rest", probe)
    provider = SplunkProvider({"splunk_url": SPLUNK_URL, "host": "https://splunk.local", "token": "t"})
    result = await provider.check_connection()
    assert result.message == "probed"
    probe.assert_awaited_once()
