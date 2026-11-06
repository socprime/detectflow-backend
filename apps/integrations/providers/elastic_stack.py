"""Elastic Stack integration — View-in-SIEM deep links into Kibana Discover.

View in SIEM builds a Kibana Discover deep link
(``{base}[/s/{space}]/app/discover#/?_g=(...)&_a=(...)``) from the profile's ``kibana_url``;
no remote call is made. The ``_a`` app state targets the configured data view
(``index_pattern_id`` → ``dataSource:(dataViewId:...,type:dataView)``) with a KQL filter; the
chain window rides the ``_g`` global state as an absolute UTC ISO-8601 range (the query itself
carries no time filter). State is rison-encoded (strings quoted with ``'``, escape char ``!``),
then percent-encoded; Discover auto-runs the search when the link is opened.

KQL is a pure filter language (no ``eval``/``coalesce``), so the host match ORs every configured
``host_name_fields`` entry against the chain hostname — semantically equivalent to the coalesce
used by the Sentinel/Splunk deep links.

:class:`ElasticStackProvider` doubles as the base for other Kibana-fork consoles (see the
``opensearch`` provider): the deep-link machinery is shared, while the console-specific surface —
config schema, operator-facing wording, the data-view state key, and the space/tenant link
location — is overridden via :meth:`ElasticStackProvider.get_config_schema`,
:attr:`ElasticStackProvider._console_label` / :attr:`ElasticStackProvider._console_url_message`,
and the :meth:`ElasticStackProvider._link_profile` / :meth:`ElasticStackProvider._data_view_state`
/ :meth:`ElasticStackProvider._link_location` hooks.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final
from urllib.parse import quote, urlparse

from pydantic import BaseModel, Field, field_validator

from apps.core.enums import IntegrationCategory
from apps.integrations._query_escaping import _kql_string_literal, _rison_string
from apps.integrations.base import (
    DEFAULT_SIGMA_RULE_ID_FIELD,
    BaseIntegrationProvider,
    ChainSearchContext,
    ConnectionResult,
    ViewInSiemIntegration,
    ViewInSiemResult,
    host_name_fields_field,
    validate_field_name,
)
from apps.integrations.registry import IntegrationRegistry
from apps.modules.utils import strip_nonempty_strings

# --- View in SIEM (deep-link search) ----------------------------------------------------------
# Default host-name fields for common Elastic (ECS) data schemas. They pre-fill the
# ``host_name_fields`` config and become the OR-ed host terms of the KQL filter; operators
# override them to match their own schema. ``.keyword`` sub-fields give exact (non-analyzed)
# matching for FQDN hostnames.
_DEFAULT_HOST_NAME_FIELDS: Final[tuple[str, ...]] = ("host.name.keyword", "source.hostname", "host.ip")

# Characters kept literal when percent-encoding the rison ``_g``/``_a`` values — rison's
# structural set, so generated links stay readable and match Kibana's own encoding.
_RISON_SAFE: Final[str] = "()!':,*@._-"

# Trailing ``/s/<space>`` segment of a pasted Kibana URL — dropped from the base because the
# space comes from its own config field.
_SPACE_SUFFIX_RE: Final[re.Pattern[str]] = re.compile(r"/s/[^/]+/?$")

_KIBANA_URL_MESSAGE: Final[str] = (
    "Could not parse the Kibana URL. Copy your Kibana web console address from the browser, "
    "including http:// or https:// (and the port number unless the default 443 is used)."
)


def _time_boundary(value: datetime, *, ceil: bool) -> str:
    """Format a window boundary for the ``_g`` time range as UTC ISO-8601 with milliseconds.

    Kibana's global time filter is inclusive on both ends. Naive values are assumed UTC.
    ``ceil=True`` rounds a sub-millisecond END up to the next whole millisecond so the chain's
    latest event isn't clipped; otherwise the value is floored to the millisecond.
    """
    value = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    sub_ms = value.microsecond % 1000
    if ceil and sub_ms:
        value += timedelta(microseconds=1000 - sub_ms)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


@dataclass(frozen=True)
class DiscoverLinkProfile:
    """Normalized View-in-SIEM config of a Kibana-like console, decoupled from field names.

    Each provider's :meth:`ElasticStackProvider._link_profile` maps its own (frontend-facing)
    config keys into this shape, so the shared deep-link machinery never touches provider
    field names. ``space_id`` is Kibana-only; ``tenant`` is OpenSearch-Dashboards-only — each
    provider fills the one its console supports.
    """

    console_url: str
    index_pattern_id: str
    remapped_sigma_rule_id_field: str
    host_name_fields: list[str]
    space_id: str = ""
    tenant: str = ""


class ElasticStackConfig(BaseModel):
    """Config schema for the Elastic Stack integration (View-in-SIEM deep links only).

    All fields are optional at the schema level; :meth:`ElasticStackProvider.build_view_in_siem_url`
    validates the subset it needs at call time (``kibana_url`` + ``index_pattern_id``).
    """

    kibana_url: str = Field(
        default="",
        description=(
            "URL of your Kibana web console that you can copy from your browser. It should "
            "contain the hostname and port number (unless the default port 443 is used)."
        ),
        json_schema_extra={"ui_order": 1, "placeholder": "https://my-kibana-server:5601"},
    )
    kibana_space_id: str = Field(
        default="",
        description=(
            "If left empty, the default space is used. Fill in this field to use a different "
            "space. The URL identifier is a short text string that becomes part of the Kibana "
            "URL when you are inside that space."
        ),
        json_schema_extra={"ui_order": 2, "placeholder": "default"},
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
            "topics into Elastic, specify the new field name here."
        ),
        json_schema_extra={"ui_order": 4, "placeholder": "sigma_rule_id"},
    )
    host_name_fields: list[str] = host_name_fields_field(_DEFAULT_HOST_NAME_FIELDS, ui_order=5)

    @field_validator("remapped_sigma_rule_id_field")
    @classmethod
    def _field_name_is_safe(cls, value: str) -> str:
        return validate_field_name(value)


@IntegrationRegistry.register
class ElasticStackProvider(BaseIntegrationProvider, ViewInSiemIntegration):
    integration_type = "elastic_stack"
    display_name = "Elastic Stack"
    category = IntegrationCategory.SIEM
    description = (
        "Connect to an Elastic Stack (Kibana) instance to open attack-chain "
        "detections in Discover with a pre-filled query."
    )
    icon = "elastic_stack"

    #: Console name used in operator-facing messages ("Kibana URL is not configured.").
    _console_label: ClassVar[str] = "Kibana"
    #: Error message for an unparseable console URL (product-specific wording).
    _console_url_message: ClassVar[str] = _KIBANA_URL_MESSAGE

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return ElasticStackConfig

    @classmethod
    def required_config_fields(cls) -> tuple[str, ...]:
        """View-in-SIEM essentials the config form marks required (asterisk + blocked submit).

        Everything except ``kibana_space_id`` (defaults to the default space) and
        ``remapped_sigma_rule_id_field`` (only set when the Sigma Rule ID field is remapped).
        Required fields keep their defaults on :class:`ElasticStackConfig` so the form still pre-fills them.
        """
        return ("kibana_url", "index_pattern_id", "host_name_fields")

    @staticmethod
    def _link_profile(parsed: BaseModel) -> DiscoverLinkProfile:
        """Map this provider's config fields into the console-agnostic link profile."""
        return DiscoverLinkProfile(
            console_url=parsed.kibana_url,
            index_pattern_id=parsed.index_pattern_id,
            remapped_sigma_rule_id_field=parsed.remapped_sigma_rule_id_field,
            host_name_fields=parsed.host_name_fields,
            space_id=parsed.kibana_space_id,
        )

    async def check_connection(self) -> ConnectionResult:
        """Validate the console URL is usable for View in SIEM (no remote call).

        The profile carries no credentials, so connectivity can't be probed — the check
        validates that the pasted URL reduces to a console base the deep-link builder can use.
        """
        parsed = self._validate_config_schema(ConnectionResult)
        if isinstance(parsed, ConnectionResult):
            return parsed
        profile = self._link_profile(parsed)
        if not profile.console_url.strip():
            return ConnectionResult(success=False, message=f"Provide the {self._console_label} URL for View in SIEM.")
        _, error = self._resolve_console_base(profile.console_url)
        if error:
            return ConnectionResult(success=False, message=error)
        return ConnectionResult(success=True, message=f"{self._console_label} URL is valid. Ready for View in SIEM.")

    @classmethod
    def _resolve_console_base(cls, console_url: str) -> tuple[str | None, str | None]:
        """Validate a pasted console URL and reduce it to the server base (origin + base path).

        Kibana-like consoles may be served under a reverse-proxy base path (e.g. Amazon
        OpenSearch Service uses ``/_dashboards``), so any path prefix is preserved — only an
        ``/app/...`` route and a trailing ``/s/<space>`` segment (the space comes from its own
        config field) are dropped. Returns ``(base, None)`` when usable, else
        ``(None, error_message)``. Shared by :meth:`check_connection` and
        :meth:`build_view_in_siem_url` so the URL checks and their messages live in one place.
        """
        parsed = urlparse(console_url.strip())
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return None, cls._console_url_message
        path = parsed.path
        if "/app/" in path:
            path = path.split("/app/", 1)[0]
        elif path.endswith("/app"):
            path = path[: -len("/app")]
        path = _SPACE_SUFFIX_RE.sub("", path)
        return f"{parsed.scheme}://{parsed.netloc}{path.rstrip('/')}", None

    # =========================================================================
    # View in SIEM (deep-link search)
    # =========================================================================

    async def build_view_in_siem_url(
        self,
        ctx: ChainSearchContext,
        *,
        params: dict[str, Any] | None = None,
    ) -> ViewInSiemResult:
        """Build a Discover deep link that opens the console running a KQL query for one chain.

        The query ORs the configured ``host_name_fields`` against the chain hostname and filters
        by the matched Sigma rule ids; the chain time window is applied by the ``_g`` global time
        range in the link. The link targets the configured index pattern / data view and is
        synthesized from the configured console URL — no remote call is made.
        """
        del params  # reserved
        parsed = self._validate_config_schema(ViewInSiemResult)
        if isinstance(parsed, ViewInSiemResult):
            return parsed
        profile = self._link_profile(parsed)

        if not profile.console_url.strip():
            return ViewInSiemResult(status=False, message=f"{self._console_label} URL is not configured.")
        index_pattern_id = profile.index_pattern_id.strip()
        if not index_pattern_id:
            return ViewInSiemResult(
                status=False, message="Configure the index pattern ID (Index Pattern ID) for View in SIEM."
            )

        kql = self._build_view_in_siem_kql(
            host_name_fields=[f.strip() for f in profile.host_name_fields if f and f.strip()],
            sigma_rule_id_field=profile.remapped_sigma_rule_id_field.strip() or DEFAULT_SIGMA_RULE_ID_FIELD,
            sigma_rule_ids=ctx.sigma_rule_ids,
            hostname=ctx.hostname,
        )
        # Defensive: a backwards window (clock skew / producer bug) would yield an empty range.
        start, end = (ctx.start, ctx.end) if ctx.start <= ctx.end else (ctx.end, ctx.start)
        base, error = self._resolve_console_base(profile.console_url)
        if error:
            return ViewInSiemResult(status=False, message=error, query=kql)
        url = self._build_discover_deeplink(
            base=base,
            profile=profile,
            index_pattern_id=index_pattern_id,
            kql=kql,
            start=start,
            end=end,
        )
        return ViewInSiemResult(status=True, url=url, query=kql)

    @staticmethod
    def _build_view_in_siem_kql(
        *,
        host_name_fields: list[str],
        sigma_rule_id_field: str,
        sigma_rule_ids: list[str],
        hostname: str | None,
    ) -> str:
        """Render the KQL filter for an attack chain.

        The query carries no time filter — the period is applied by the ``_g`` global time range
        in the deep link. An empty result (no hostname, no rule ids) matches all documents in the
        window, mirroring the other View-in-SIEM providers.
        """
        conditions: list[str] = []
        if hostname and hostname.strip():
            literal = _kql_string_literal(hostname.strip())
            fields = host_name_fields or list(_DEFAULT_HOST_NAME_FIELDS)
            host_terms = [f"{field} : {literal}" for field in fields]
            conditions.append(host_terms[0] if len(host_terms) == 1 else f"({' or '.join(host_terms)})")
        ids = strip_nonempty_strings(sigma_rule_ids)
        if len(ids) == 1:
            conditions.append(f"{sigma_rule_id_field} : {_kql_string_literal(ids[0])}")
        elif ids:
            quoted_ids = " or ".join(_kql_string_literal(rid) for rid in ids)
            conditions.append(f"{sigma_rule_id_field} : ({quoted_ids})")
        return " and ".join(conditions)

    @staticmethod
    def _data_view_state(index_pattern_id: str) -> str:
        """Rison ``_a`` fragment binding the index pattern / data view.

        Kibana 8+ uses the modern ``dataSource`` key (the legacy ``index:'<id>'`` would also
        still load); OpenSearch Dashboards overrides this with the legacy key it understands.
        """
        return f"dataSource:(dataViewId:{_rison_string(index_pattern_id)},type:dataView)"

    @staticmethod
    def _link_location(profile: DiscoverLinkProfile) -> tuple[str, str]:
        """``(path_prefix, query_string)`` placing the link inside the console's tenancy unit.

        Kibana scopes by Space — a ``/s/<space>`` path prefix before ``/app/...`` (default space
        → no prefix). OpenSearch Dashboards overrides with the ``security_tenant`` query param.
        """
        space = f"/s/{quote(profile.space_id, safe='')}" if profile.space_id else ""
        return space, ""

    @classmethod
    def _build_discover_deeplink(
        cls,
        *,
        base: str,
        profile: DiscoverLinkProfile,
        index_pattern_id: str,
        kql: str,
        start: datetime,
        end: datetime,
    ) -> str:
        """Assemble the Discover URL: ``{base}{prefix}/app/discover{query}#/?_g=(...)&_a=(...)``.

        ``_g`` carries the absolute UTC window with auto-refresh paused; ``_a`` binds the index
        pattern via :meth:`_data_view_state` with the KQL query (``language:kuery`` — also the
        state token for OpenSearch DQL) and an explicit ``@timestamp`` descending sort. Both are
        rison-encoded then percent-encoded with rison's structural characters kept literal —
        matching how Kibana itself serializes Discover state.
        """
        g_state = (
            "(filters:!(),refreshInterval:(pause:!t,value:0),"
            "time:(from:"
            f"{_rison_string(_time_boundary(start, ceil=False))}"
            ",mode:absolute,to:"
            f"{_rison_string(_time_boundary(end, ceil=True))}"
            "))"
        )
        a_state = (
            "(columns:!(),"
            f"{cls._data_view_state(index_pattern_id)},"
            "filters:!(),"
            "interval:auto,"
            f"query:(language:kuery,query:{_rison_string(kql)}),"
            "sort:!(!('@timestamp',desc)))"
        )
        path_prefix, query_string = cls._link_location(profile)
        return (
            f"{base}{path_prefix}/app/discover{query_string}"
            f"#/?_g={quote(g_state, safe=_RISON_SAFE)}&_a={quote(a_state, safe=_RISON_SAFE)}"
        )
