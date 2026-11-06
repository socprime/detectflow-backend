"""OpenSearch integration — View-in-SIEM deep links into OpenSearch Dashboards Discover.

OpenSearch Dashboards is a Kibana fork, so this provider inherits the entire View-in-SIEM
machinery from :class:`~apps.integrations.providers.elastic_stack.ElasticStackProvider` and
overrides only the console-specific surface:

* **config schema** — OpenSearch wording and keys; an optional ``tenant`` (security-plugin
  multi-tenancy) instead of Kibana Spaces, which OpenSearch Dashboards does not have;
* **data-view binding** — the legacy/classic ``index:'<id>'`` state key. OSD has no Kibana-8
  ``dataSource`` key; OSD 1.x reads the classic shape natively, and OSD 2.x/3.x auto-migrate
  the legacy ``/app/discover`` URL into data-explorer preserving the index pattern, query, and
  time window (``migrateUrlState`` in the OSD ``discover`` plugin);
* **link location** — the tenant rides the ``security_tenant`` query parameter *before* the
  ``#`` fragment. Index patterns are per-tenant saved objects, so without the parameter the
  link opens in the user's last-selected tenant, where the configured index pattern ID may not
  exist.

The generated query is DQL — OpenSearch's rebrand of Kibana KQL; its state token is still
``kuery``, so the inherited query builder and ``language:kuery`` state apply unchanged.
"""

from __future__ import annotations

from typing import ClassVar, Final
from urllib.parse import quote

from pydantic import BaseModel, Field, field_validator

from apps.integrations._query_escaping import _rison_string
from apps.integrations.base import (
    host_name_fields_field,
    validate_field_name,
)
from apps.integrations.providers.elastic_stack import (
    _DEFAULT_HOST_NAME_FIELDS,
    DiscoverLinkProfile,
    ElasticStackProvider,
)
from apps.integrations.registry import IntegrationRegistry

_OPENSEARCH_URL_MESSAGE: Final[str] = (
    "Could not parse the OpenSearch Dashboards URL. Copy your OpenSearch Dashboards web console "
    "address from the browser, including http:// or https:// (and the port number unless the "
    "default 443 is used)."
)


class OpenSearchConfig(BaseModel):
    """Config schema for the OpenSearch integration (View-in-SIEM deep links only).

    All fields are optional at the schema level; the inherited
    :meth:`~apps.integrations.providers.elastic_stack.ElasticStackProvider.build_view_in_siem_url`
    validates the subset it needs at call time (``opensearch_url`` + ``index_pattern_id``).
    """

    opensearch_url: str = Field(
        default="",
        description=(
            'URL of your web console ending with "/discover". Copy it from your browser when on the Discover page.'
        ),
        json_schema_extra={
            "ui_order": 1,
            "placeholder": "https://my-opensearch-server/_dashboards/app/data-explorer/discover",
        },
    )
    tenant: str = Field(
        default="",
        description=(
            "Optional. If multi-tenancy is enabled in your OpenSearch Dashboards, specify the "
            "tenant where the index pattern is stored. If left empty, the link opens in the "
            "user's current tenant."
        ),
        json_schema_extra={"ui_order": 2},
    )
    index_pattern_id: str = Field(
        default="",
        description="Set an ID of the index pattern where events from destination topics are stored",
        json_schema_extra={"ui_order": 3, "placeholder": "ff959d40-b880-11e8-a6d9-e546fe2bba5f"},
    )
    remapped_sigma_rule_id_field: str = Field(
        default="",
        description=(
            "Optional. If you remap the Sigma Rule ID field when ingesting data from destination "
            "topics into AWS OpenSearch, specify the new field name here."
        ),
        json_schema_extra={"ui_order": 4, "placeholder": "sigma_rule_id"},
    )
    host_name_fields: list[str] = host_name_fields_field(_DEFAULT_HOST_NAME_FIELDS, ui_order=5)

    @field_validator("remapped_sigma_rule_id_field")
    @classmethod
    def _field_name_is_safe(cls, value: str) -> str:
        return validate_field_name(value)


@IntegrationRegistry.register
class OpenSearchProvider(ElasticStackProvider):
    integration_type = "opensearch"
    display_name = "OpenSearch"
    description = (
        "Connect to an OpenSearch instance to open attack-chain detections "
        "in OpenSearch Dashboards Discover with a pre-filled query."
    )
    icon = "opensearch"

    _console_label: ClassVar[str] = "OpenSearch Dashboards"
    _console_url_message: ClassVar[str] = _OPENSEARCH_URL_MESSAGE

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return OpenSearchConfig

    @classmethod
    def required_config_fields(cls) -> tuple[str, ...]:
        """View-in-SIEM essentials the config form marks required (asterisk + blocked submit).

        Everything except ``tenant`` (only set under multi-tenancy) and
        ``remapped_sigma_rule_id_field`` (only set when the Sigma Rule ID field is remapped).
        Overrides the parent because the field names differ (``opensearch_url`` vs ``kibana_url``).
        """
        return ("opensearch_url", "index_pattern_id", "host_name_fields")

    @staticmethod
    def _link_profile(parsed: BaseModel) -> DiscoverLinkProfile:
        """Map the OpenSearch config fields into the console-agnostic link profile."""
        return DiscoverLinkProfile(
            console_url=parsed.opensearch_url,
            index_pattern_id=parsed.index_pattern_id,
            remapped_sigma_rule_id_field=parsed.remapped_sigma_rule_id_field,
            host_name_fields=parsed.host_name_fields,
            tenant=parsed.tenant,
        )

    @staticmethod
    def _data_view_state(index_pattern_id: str) -> str:
        """Bind the index pattern via the classic ``index:'<id>'`` key.

        OSD (all versions) understands it: natively on 1.x, and via the built-in legacy-URL
        migration into data-explorer (``_a.metadata.indexPattern``) on 2.x/3.x. The Kibana-8
        ``dataSource`` key the parent emits does not exist in OSD.
        """
        return f"index:{_rison_string(index_pattern_id)}"

    @staticmethod
    def _link_location(profile: DiscoverLinkProfile) -> tuple[str, str]:
        """No Kibana Spaces in OSD — the tenant rides ``?security_tenant=`` before the ``#``.

        Emitted only when a tenant is configured; otherwise the console keeps the user's
        current (cookie-selected) tenant.
        """
        query = f"?security_tenant={quote(profile.tenant, safe='')}" if profile.tenant else ""
        return "", query
