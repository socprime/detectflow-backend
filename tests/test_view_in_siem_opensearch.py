"""Tests for the OpenSearch ``view_in_siem`` capability (OpenSearch Dashboards Discover links).

The provider inherits the Kibana deep-link machinery from ``ElasticStackProvider`` and overrides
the console surface: the deep link is the classic/legacy Discover shape
``{base}/app/discover[?security_tenant=<t>]#/?_g=(...)&_a=(...,index:'<id>',...)`` — OSD 1.x reads
it natively and OSD 2.x/3.x auto-migrate it into data-explorer. No Kibana Spaces; the optional
tenant rides the ``security_tenant`` query parameter before the ``#`` fragment.
"""

from datetime import UTC, datetime
from urllib.parse import unquote

import apps.integrations.providers  # noqa: F401  -- register all providers for registry lookups
from apps.integrations.base import (
    DEFAULT_SIGMA_RULE_ID_FIELD,
    ChainSearchContext,
    ViewInSiemIntegration,
    get_provider_capabilities,
)
from apps.integrations.providers.elastic_stack import _DEFAULT_HOST_NAME_FIELDS, ElasticStackProvider
from apps.integrations.providers.opensearch import OpenSearchConfig, OpenSearchProvider

OPENSEARCH_URL = "https://opensearch.example.com:5601"
INDEX_PATTERN_ID = "8a1b2c30-b880-11e8-a6d9-e546fe2bba5f"
START = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
END = datetime(2026, 6, 2, 11, 30, 0, tzinfo=UTC)
G_STATE = (
    "_g=(filters:!(),refreshInterval:(pause:!t,value:0),"
    "time:(from:'2026-06-01T10:00:00.000Z',mode:absolute,to:'2026-06-02T11:30:00.000Z'))"
)


def _view_in_siem_config(**overrides) -> dict:
    cfg = {
        "opensearch_url": OPENSEARCH_URL,
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
    assert issubclass(OpenSearchProvider, ViewInSiemIntegration)
    assert issubclass(OpenSearchProvider, ElasticStackProvider)  # inherits the link machinery
    assert get_provider_capabilities(OpenSearchProvider) == ["check_connection", "view_in_siem"]


def test_provider_identity_is_distinct_from_elastic():
    assert OpenSearchProvider.integration_type == "opensearch"
    assert OpenSearchProvider.display_name == "OpenSearch"
    assert OpenSearchProvider.get_config_schema() is OpenSearchConfig


def test_config_defaults():
    cfg = OpenSearchConfig()
    assert cfg.opensearch_url == ""
    assert cfg.tenant == ""
    assert cfg.index_pattern_id == ""
    assert cfg.remapped_sigma_rule_id_field == ""
    assert cfg.host_name_fields == list(_DEFAULT_HOST_NAME_FIELDS)


def test_config_host_name_fields_default_is_not_shared():
    a = OpenSearchConfig()
    a.host_name_fields.append("extra.field")
    b = OpenSearchConfig()
    assert "extra.field" not in b.host_name_fields


def test_config_schema_exposes_host_name_fields_default_for_form_prefill():
    prop = OpenSearchConfig.model_json_schema()["properties"]["host_name_fields"]
    assert prop.get("default") == list(_DEFAULT_HOST_NAME_FIELDS)


def test_no_sensitive_fields():
    assert OpenSearchProvider.get_sensitive_fields() == set()


# ---------------------------------------------------------------------------
# build_view_in_siem_url — OSD-specific deep-link shape
# ---------------------------------------------------------------------------


async def test_build_view_in_siem_url_success():
    provider = OpenSearchProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1", "uuid2"], "srv-ad-001.example.com"))
    assert result.status is True
    url = result.url
    assert url is not None
    assert url.startswith(f"{OPENSEARCH_URL}/app/discover#/?_g=")
    decoded = unquote(url)
    assert G_STATE in decoded
    # Classic/legacy binding — OSD has no Kibana-8 dataSource key.
    assert f"index:'{INDEX_PATTERN_ID}'" in decoded
    assert "dataSource" not in decoded
    assert f"query:(language:kuery,query:'{result.query}')" in decoded
    assert "sort:!(!('@timestamp',desc))" in decoded


async def test_build_view_in_siem_url_query_is_shared_kql():
    # The query builder is inherited from the Elastic provider unchanged (DQL == KQL).
    provider = OpenSearchProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "host-1"))
    assert result.query == (
        f'(host.name.keyword : "host-1" or source.hostname : "host-1") and {DEFAULT_SIGMA_RULE_ID_FIELD} : "uuid1"'
    )


async def test_build_view_in_siem_url_with_tenant():
    provider = OpenSearchProvider(_view_in_siem_config(tenant="soc team"))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    # The tenant is a query param BEFORE the # fragment, percent-encoded.
    assert f"{OPENSEARCH_URL}/app/discover?security_tenant=soc%20team#/?_g=" in result.url


async def test_build_view_in_siem_url_without_tenant_has_no_param():
    provider = OpenSearchProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert "security_tenant" not in result.url


async def test_build_view_in_siem_url_has_no_space_prefix():
    provider = OpenSearchProvider(_view_in_siem_config())
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert "/s/" not in result.url


async def test_build_view_in_siem_url_preserves_aws_dashboards_base_path():
    # Amazon OpenSearch Service serves Dashboards under /_dashboards.
    pasted = "https://my-domain.eu-west-1.es.amazonaws.com/_dashboards/app/discover"
    provider = OpenSearchProvider(_view_in_siem_config(opensearch_url=pasted))
    result = await provider.build_view_in_siem_url(_ctx(["uuid1"], "h"))
    assert result.url.startswith("https://my-domain.eu-west-1.es.amazonaws.com/_dashboards/app/discover#/?_g=")


async def test_build_view_in_siem_url_requires_opensearch_url():
    provider = OpenSearchProvider(_view_in_siem_config(opensearch_url=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    assert "OpenSearch Dashboards URL is not configured" in result.message


async def test_build_view_in_siem_url_requires_index_pattern_id():
    provider = OpenSearchProvider(_view_in_siem_config(index_pattern_id=""))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "index pattern" in result.message.lower()


async def test_build_view_in_siem_url_invalid_url_keeps_query_preview():
    provider = OpenSearchProvider(_view_in_siem_config(opensearch_url="opensearch.example.com:5601"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert result.url is None
    assert "OpenSearch Dashboards" in result.message
    assert result.query is not None


async def test_build_view_in_siem_url_invalid_config_types():
    provider = OpenSearchProvider(_view_in_siem_config(host_name_fields="notalist"))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


async def test_build_view_in_siem_url_rejects_injection_in_sigma_field():
    provider = OpenSearchProvider(_view_in_siem_config(remapped_sigma_rule_id_field='x" or 1==1'))
    result = await provider.build_view_in_siem_url(_ctx(["x"], "h"))
    assert result.status is False
    assert "Invalid config" in result.message


# ---------------------------------------------------------------------------
# check_connection (offline URL validation only — the profile carries no credentials)
# ---------------------------------------------------------------------------


async def test_check_connection_valid_url():
    provider = OpenSearchProvider({"opensearch_url": OPENSEARCH_URL})
    result = await provider.check_connection()
    assert result.success is True
    assert "OpenSearch Dashboards URL is valid" in result.message


async def test_check_connection_empty_profile_gives_guidance():
    provider = OpenSearchProvider({})
    result = await provider.check_connection()
    assert result.success is False
    assert "OpenSearch Dashboards URL" in result.message


async def test_check_connection_rejects_unparseable_url():
    provider = OpenSearchProvider({"opensearch_url": "opensearch.example.com"})
    result = await provider.check_connection()
    assert result.success is False
    assert "http" in result.message
