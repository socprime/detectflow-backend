"""Tests for integration provider capability discovery."""

from typing import Any

import httpx
import pytest
from pydantic import BaseModel, Field

from apps.core.enums import IntegrationCategory
from apps.integrations.base import (
    BaseIntegrationProvider,
    ConnectionResult,
    DeployableIntegration,
    DeployResult,
    SearchableIntegration,
    SearchResult,
    get_provider_capabilities,
)
from apps.integrations.providers.elastic_stack import ElasticStackProvider
from apps.integrations.providers.microsoft_sentinel import MicrosoftSentinelProvider
from apps.integrations.providers.opensearch import OpenSearchProvider
from apps.integrations.providers.socprime_tdm import SocPrimeTdmProvider
from apps.integrations.providers.splunk import SplunkProvider


class _StubConfig(BaseModel):
    token: str = Field(default="x")


class StubProvider(BaseIntegrationProvider):
    """Provider with only check_connection (no optional mixins)."""

    integration_type = "stub_capabilities_minimal"
    display_name = "Stub"
    category = IntegrationCategory.SIEM
    description = "test"

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return _StubConfig

    async def check_connection(self) -> ConnectionResult:
        return ConnectionResult(success=True, message="ok")


class StubDeployOnly(StubProvider, DeployableIntegration):
    integration_type = "stub_capabilities_deploy"

    async def deploy(
        self,
        rules: list[str | dict[str, Any]],
        *,
        params: dict[str, Any] | None = None,
    ) -> DeployResult:
        return DeployResult(status=True, deployed=len(rules))


class StubFull(
    StubProvider,
    DeployableIntegration,
    SearchableIntegration,
):
    integration_type = "stub_capabilities_full"

    async def deploy(
        self,
        rules: list[str | dict[str, Any]],
        *,
        params: dict[str, Any] | None = None,
    ) -> DeployResult:
        return DeployResult(status=True)

    async def search(
        self,
        query: str | None = None,
        *,
        external_ids: list[str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
        params: dict[str, Any] | None = None,
    ) -> SearchResult:
        return SearchResult(status=True, items=[], next_cursor=None)


def test_get_provider_capabilities_base_only() -> None:
    assert get_provider_capabilities(StubProvider) == [BaseIntegrationProvider.capability_id]


def test_get_provider_capabilities_with_deploy() -> None:
    assert get_provider_capabilities(StubDeployOnly) == [
        BaseIntegrationProvider.capability_id,
        DeployableIntegration.capability_id,
    ]


def test_get_provider_capabilities_all_mixins() -> None:
    assert get_provider_capabilities(StubFull) == [
        BaseIntegrationProvider.capability_id,
        DeployableIntegration.capability_id,
        SearchableIntegration.capability_id,
    ]


def test_socprime_tdm_capabilities() -> None:
    caps = get_provider_capabilities(SocPrimeTdmProvider)
    assert caps == ["check_connection"]


def test_splunk_capabilities() -> None:
    caps = get_provider_capabilities(SplunkProvider)
    assert caps == ["check_connection", "view_in_siem"]


def test_elastic_stack_capabilities() -> None:
    caps = get_provider_capabilities(ElasticStackProvider)
    assert caps == ["check_connection", "view_in_siem"]


def test_opensearch_capabilities() -> None:
    caps = get_provider_capabilities(OpenSearchProvider)
    assert caps == ["check_connection", "view_in_siem"]


def test_socprime_tdm_singleton_profile() -> None:
    assert SocPrimeTdmProvider.singleton_profile is True


def test_merge_config_update_preserves_secret_on_mask_placeholder() -> None:
    merged = SocPrimeTdmProvider.merge_config_update(
        {"api_key": "stored-secret", "base_url": "https://old"},
        {
            "api_key": "pre...fix",
            "base_url": "https://new",
        },
        {"api_key"},
    )
    assert merged["api_key"] == "stored-secret"
    assert merged["base_url"] == "https://new"


def test_merge_config_update_default_provider_preserves_asterisk_mask() -> None:
    merged = SplunkProvider.merge_config_update(
        {"token": "stored-token", "host": "https://old"},
        {
            "token": BaseIntegrationProvider.sensitive_mask,
            "host": "https://new",
        },
        SplunkProvider.get_sensitive_fields(),
    )
    assert merged["token"] == "stored-token"
    assert merged["host"] == "https://new"


def test_merge_config_update_replaces_secret_with_new_value() -> None:
    merged = SocPrimeTdmProvider.merge_config_update(
        {"api_key": "old"},
        {"api_key": "new-secret"},
        {"api_key"},
    )
    assert merged["api_key"] == "new-secret"


def test_socprime_tdm_mask_sensitive_config_long_key() -> None:
    long_key = "prefix-middle-suffix-key"
    out = SocPrimeTdmProvider.mask_sensitive_config(
        {"api_key": long_key, "base_url": "https://api.example"},
        SocPrimeTdmProvider.get_sensitive_fields(),
    )
    assert out["base_url"] == "https://api.example"
    assert out["api_key"].startswith("pre")
    assert out["api_key"].endswith("key")
    assert SocPrimeTdmProvider.sensitive_mask in out["api_key"]


def test_socprime_tdm_mask_sensitive_config_short_or_non_string() -> None:
    short = SocPrimeTdmProvider.mask_sensitive_config({"api_key": "short"}, {"api_key"})
    assert short["api_key"] == SocPrimeTdmProvider.sensitive_mask
    non_str = SocPrimeTdmProvider.mask_sensitive_config({"api_key": 123}, {"api_key"})
    assert non_str["api_key"] == SocPrimeTdmProvider.sensitive_mask


# ---------------------------------------------------------------------------
# Sentinel deploy/search network-failure contract (provider failure → status:false)
# ---------------------------------------------------------------------------

# RELEASE SCOPE: the ARM config fields are commented out of MicrosoftSentinelConfig while the
# ARM capability group (deploy/search) is unreleased, so deploy/search short-circuit at
# credential validation and never reach the network layer. Unskip when the fields ship.
_ARM_GROUP_UNRELEASED = pytest.mark.skip(
    reason="Sentinel ARM config fields are commented out for this release (deploy/search unreleased)"
)

_SENTINEL_ARM_CONFIG = {
    "client_id": "aabbccdd-1111-2222-3333-445566778899",
    "client_secret": "test-secret",
    "tenant_id": "aabbccdd-1111-2222-3333-445566778899",
    "subscription_id": "11111111-1111-1111-1111-111111111111",
    "resource_group": "rg-soc",
    "workspace": "law-soc",
}


def _raise_connect_error(*_args, **_kwargs):
    raise httpx.ConnectError("name resolution failed")


def _raise_timeout(*_args, **_kwargs):
    raise httpx.TimeoutException("timed out")


async def test_sentinel_deploy_reports_missing_arm_credentials() -> None:
    """While the ARM group is unreleased, deploy fails gracefully at credential validation."""
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).deploy([{"displayName": "r1"}])
    assert result.status is False
    assert "missing" in result.message.lower()


async def test_sentinel_search_reports_missing_arm_credentials() -> None:
    """While the ARM group is unreleased, search fails gracefully at credential validation."""
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).search()
    assert result.status is False
    assert "missing" in result.message.lower()


@_ARM_GROUP_UNRELEASED
async def test_sentinel_deploy_network_error_returns_status_false(monkeypatch) -> None:
    monkeypatch.setattr(MicrosoftSentinelProvider, "_fetch_management_token", _raise_connect_error)
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).deploy([{"displayName": "r1"}])
    assert result.status is False
    assert "Network error" in result.message


@_ARM_GROUP_UNRELEASED
async def test_sentinel_deploy_timeout_returns_status_false(monkeypatch) -> None:
    monkeypatch.setattr(MicrosoftSentinelProvider, "_fetch_management_token", _raise_timeout)
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).deploy([{"displayName": "r1"}])
    assert result.status is False
    assert "timed out" in result.message.lower()


@_ARM_GROUP_UNRELEASED
async def test_sentinel_search_network_error_returns_status_false(monkeypatch) -> None:
    monkeypatch.setattr(MicrosoftSentinelProvider, "_fetch_management_token", _raise_connect_error)
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).search()
    assert result.status is False
    assert "Network error" in result.message


@_ARM_GROUP_UNRELEASED
async def test_sentinel_search_timeout_returns_status_false(monkeypatch) -> None:
    monkeypatch.setattr(MicrosoftSentinelProvider, "_fetch_management_token", _raise_timeout)
    result = await MicrosoftSentinelProvider(_SENTINEL_ARM_CONFIG).search()
    assert result.status is False
    assert "timed out" in result.message.lower()
