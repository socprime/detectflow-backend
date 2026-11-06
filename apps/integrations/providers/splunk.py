"""Splunk integration — View-in-SIEM deep links + optional management-API connectivity check.

View in SIEM builds a Splunk Web search deep link
(``{base}/en-US/app/{app}/search?q=search%20...&earliest=...&latest=...``) from the profile's
``splunk_url``; no remote call is made. The SPL unions the configured indexes: the first index is
the main search and every additional index is its own ``| append [ search index=... ]`` subsearch
block, each block repeating the same time-filter / Host-coalesce / where pipeline. The chain
window rides the ``earliest``/``latest`` URL params as UTC epoch seconds (driving the time
picker) **and** is re-asserted inside every block as ``| where _time >= <earliest> AND
_time < <latest>`` — append subsearches must not depend on time-picker inheritance for
correctness. Search-time ``earliest=``/``latest=`` terms are still avoided (they would override
the picker and make the effective window ambiguous). Splunk treats ``earliest`` as inclusive and
``latest`` as exclusive, so the window end is bumped one second past the chain's last event. The
``q`` value triggers the search to auto-run when the link is opened.

The management REST fields (``host``/``port``/``token``/``ssl_verify``) are **commented out for
this release** (only View in SIEM ships; the frontend renders the config form from this schema).
While they are commented out, :meth:`SplunkProvider.check_connection` always takes the
URL-validation branch; the REST probe stays in code as a dormant branch — uncomment the fields
to re-enable it when a management-API capability (deploy/search) ships.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any, Final
from urllib.parse import quote, urlparse

import httpx
from pydantic import BaseModel, Field, field_validator

from apps.core.enums import IntegrationCategory
from apps.integrations._query_escaping import _spl_string_literal
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

TIMEOUT_CHECK_CONNECTION: Final[float] = 15.0

# --- View in SIEM (deep-link search) ----------------------------------------------------------
# Default host-name fields for common Splunk data schemas. They pre-fill the ``host_name_fields``
# config and become the arguments of the ``coalesce(...)`` that resolves a ``Host`` field;
# operators override them to match their own schema.
_DEFAULT_HOST_NAME_FIELDS: Final[tuple[str, ...]] = ("host", "Computer", "HostName", "ComputerName")

# Splunk Web search-view path pieces. The locale segment is emitted explicitly — without it
# Splunk redirects to the user's locale, which is unreliable for some locales. ``search`` is both
# the stock Search & Reporting app id and the conventional search view name inside any app.
_SPLUNK_LOCALE: Final[str] = "en-US"
_DEFAULT_SEARCH_APP: Final[str] = "search"

# Field references in ``eval``/``where`` must be single-quoted unless they are plain identifiers
# (e.g. dotted names like ``my.field``).
_PLAIN_FIELD_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_]\w*$")

# Locale path segment (``/en-US/...``) of a pasted Splunk Web URL — everything from it onward is
# the page route, not the mount prefix.
_LOCALE_SEGMENT_RE: Final[re.Pattern[str]] = re.compile(r"/[A-Za-z]{2}-[A-Za-z]{2}(?=/|$)")

_SPLUNK_URL_MESSAGE: Final[str] = (
    "Could not parse the Splunk URL. Copy your Splunk web console address from the browser, "
    "including http:// or https:// (add port 8000 for on-prem Splunk; no port for cloud Splunk)."
)

_SPLUNK_MANAGEMENT_HOST_MESSAGE: Final[str] = (
    "Could not parse the Splunk management host. Provide it as https://splunk.example.com — "
    "the management port (default 8089) is appended automatically unless the host already "
    "includes one."
)


def _spl_field_expr(field: str) -> str:
    """Field reference for ``eval``/``where``: bare for plain identifiers, single-quoted otherwise.

    SPL ``eval`` expressions treat unquoted dotted/spaced names as syntax errors; wrapping in
    single quotes makes them field references (e.g. ``'my.field'``). Backslashes and apostrophes
    are escaped inside the quoted branch so the name can't break out of the field reference —
    defense in depth behind the :func:`~apps.integrations.base.validate_field_name` config gate.
    """
    field = field.strip()
    if _PLAIN_FIELD_RE.match(field):
        return field
    escaped = field.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _epoch_boundary(value: datetime, *, exclusive_end: bool) -> int:
    """Convert a window boundary to UTC epoch seconds for the ``earliest``/``latest`` URL params.

    Naive values are assumed UTC. ``earliest`` is inclusive and floors any sub-second part
    (slightly widening the window). ``latest`` is exclusive in Splunk, so ``exclusive_end=True``
    bumps one second past the floored boundary — events at the chain's exact last second
    (including sub-second tails) stay inside the window.
    """
    value = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    epoch = int(value.timestamp())
    return epoch + 1 if exclusive_end else epoch


class SplunkConfig(BaseModel):
    """Config schema for the Splunk integration.

    One profile serves two capability groups; all fields are optional at the schema level so a
    profile can be configured for either purpose, and each capability validates the subset it
    needs at call time:

    * **View in SIEM** (deep-link search, no remote call): ``splunk_url``, ``custom_search_app``,
      ``index_names``, ``remapped_sigma_rule_id_field``, ``host_name_fields``.
    * **Management REST API** (connectivity check): ``host``, ``port``, ``token``, ``ssl_verify``
      — **commented out for this release** (the form renders from this schema and the design
      ships View in SIEM only). ``check_connection`` reads them via ``getattr``, so while they
      are commented out it always falls through to the URL-validation branch.
    """

    # --- View in SIEM (deep-link search) ---
    splunk_url: str = Field(
        default="",
        description=(
            "URL of your Splunk web console that you can copy from your browser. Do not include "
            "the port number for cloud Splunk, and add port 8000 for on-prem Splunk."
        ),
        json_schema_extra={"ui_order": 1, "placeholder": "https://<deployment-name>.splunkcloud.com"},
    )
    custom_search_app: str = Field(
        default="",
        description='Fill in if in Splunk you use a search app different from "default".',
        json_schema_extra={"ui_order": 2, "placeholder": _DEFAULT_SEARCH_APP},
    )
    index_names: list[str] = Field(
        default_factory=list,
        description="The Splunk indexes where tagged events from destination topics are stored.",
        json_schema_extra={"ui_order": 3, "placeholder": "main, security, audit"},
    )
    remapped_sigma_rule_id_field: str = Field(
        default="",
        description=(
            "Optional. If you remap the Sigma Rule ID field when ingesting data from destination "
            "topics into Splunk, specify the new field name here."
        ),
        json_schema_extra={"ui_order": 4, "placeholder": "sigma_rule_id"},
    )
    host_name_fields: list[str] = host_name_fields_field(_DEFAULT_HOST_NAME_FIELDS, ui_order=5)

    # --- Management REST API (connectivity check) — optional ---
    # RELEASE SCOPE: commented out — only View in SIEM ships in this release, and the frontend
    # renders the config form from this schema (the design has no management-API fields). The
    # REST connectivity probe stays in code as a dormant branch. Uncomment to ship.
    # host: str = Field(
    #     default="",
    #     description="Splunk management host URL",
    #     json_schema_extra={"ui_order": 6, "placeholder": "https://splunk.example.com"},
    # )
    # port: int = Field(
    #     default=8089,
    #     description="Splunk management port",
    #     json_schema_extra={"ui_order": 7},
    # )
    # token: str = Field(
    #     default="",
    #     description="Splunk authentication token",
    #     json_schema_extra={"sensitive": True, "ui_order": 8},
    # )
    # ssl_verify: bool = Field(
    #     default=True,
    #     description="Verify SSL certificate (disable for self-signed certs)",
    #     json_schema_extra={"ui_order": 9},
    # )

    @field_validator("remapped_sigma_rule_id_field")
    @classmethod
    def _field_name_is_safe(cls, value: str) -> str:
        return validate_field_name(value)


@IntegrationRegistry.register
class SplunkProvider(BaseIntegrationProvider, ViewInSiemIntegration):
    integration_type = "splunk"
    display_name = "Splunk"
    category = IntegrationCategory.SIEM
    description = (
        "Connect to a Splunk instance to validate your data schema, "
        "stage and deploy detection rules, and manage existing content."
    )
    icon = "splunk"

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return SplunkConfig

    @classmethod
    def required_config_fields(cls) -> tuple[str, ...]:
        """View-in-SIEM essentials the config form marks required (asterisk + blocked submit).

        Everything except ``custom_search_app`` (defaults to the standard search app) and
        ``remapped_sigma_rule_id_field`` (only set when the Sigma Rule ID field is remapped).
        Required fields keep their defaults on :class:`SplunkConfig` so the form still pre-fills them.
        """
        return ("splunk_url", "index_names", "host_name_fields")

    @classmethod
    def get_sensitive_fields(cls) -> set[str]:
        """Keep ``token`` masked while the management REST fields are commented out (release scope).

        Auto-detection only sees fields declared on the schema, but profiles saved before the
        comment-out may still carry a stored token — without this override GET responses would
        echo it in clear text.
        """
        return super().get_sensitive_fields() | {"token"}

    async def check_connection(self) -> ConnectionResult:
        """Capability-aware connection check.

        A single profile may be configured for the management REST API (host + token) and/or for
        View in SIEM (a Splunk web URL only). The check validates whatever the profile is set up
        for:

        * Management host + token set → probe ``/services/server/info`` over REST.
        * Otherwise, a Splunk URL is set → validate it parses to a web base (View-in-SIEM
          readiness), no remote call.
        * Neither → fail with guidance.
        """
        parsed = self._validate_config_schema(ConnectionResult)
        if isinstance(parsed, ConnectionResult):
            return parsed
        # getattr with defaults: the management REST fields are commented out of SplunkConfig for
        # this release, so this branch is dormant until they ship again.
        if getattr(parsed, "host", "").strip() and getattr(parsed, "token", ""):
            return await self._check_connection_rest(parsed)
        if parsed.splunk_url.strip():
            return self._check_connection_view_in_siem(parsed)
        return ConnectionResult(
            success=False,
            message=(
                "Provide a Splunk URL for View in SIEM, or a management host and token for an API connectivity check."
            ),
        )

    @staticmethod
    def _resolve_management_base(host: str, port: int) -> tuple[str | None, str | None]:
        """Normalize the management host to ``scheme://netloc[:port]`` for the REST probe.

        Operator inputs vary: a trailing slash/path would put the port in the path, an embedded
        port would double it, and a missing scheme makes ``urlparse`` read the host as the
        scheme. The configured ``port`` is appended only when the netloc doesn't already carry
        one. Returns ``(base, None)`` when usable, else ``(None, error_message)``.
        """
        parsed = urlparse(host.strip())
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return None, _SPLUNK_MANAGEMENT_HOST_MESSAGE
        try:
            has_port = parsed.port is not None
        except ValueError:  # non-numeric port in the netloc
            return None, _SPLUNK_MANAGEMENT_HOST_MESSAGE
        netloc = parsed.netloc if has_port else f"{parsed.netloc}:{port}"
        return f"{parsed.scheme}://{netloc}", None

    async def _check_connection_rest(self, parsed: SplunkConfig) -> ConnectionResult:
        """Probe the Splunk management REST API (``/services/server/info``) with the token.

        Dormant while the management REST fields are commented out of :class:`SplunkConfig`
        (release scope) — reads them via ``getattr`` so the branch stays importable/callable.
        """
        base, error = self._resolve_management_base(getattr(parsed, "host", ""), getattr(parsed, "port", 8089))
        if error:
            return ConnectionResult(success=False, message=error)
        try:
            url = f"{base}/services/server/info"
            verify = getattr(parsed, "ssl_verify", True)
            async with httpx.AsyncClient(timeout=TIMEOUT_CHECK_CONNECTION, verify=verify) as client:
                response = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {getattr(parsed, 'token', '')}"},
                )
                if response.status_code == 200:
                    return ConnectionResult(success=True, message="Connected to Splunk successfully")
                return ConnectionResult(
                    success=False,
                    message=f"Authentication failed (HTTP {response.status_code})",
                )
        except httpx.TimeoutException:
            return ConnectionResult(success=False, message="Connection timed out")
        except Exception as e:
            return ConnectionResult(success=False, message=f"Connection failed: {str(e)}")

    def _check_connection_view_in_siem(self, parsed: SplunkConfig) -> ConnectionResult:
        """Validate the Splunk URL reduces to a usable web base (no remote call)."""
        _, error = self._resolve_splunk_base(parsed.splunk_url)
        if error:
            return ConnectionResult(success=False, message=error)
        return ConnectionResult(success=True, message="Splunk URL is valid. Ready for View in SIEM.")

    @staticmethod
    def _resolve_splunk_base(splunk_url: str) -> tuple[str | None, str | None]:
        """Validate a pasted Splunk web URL and reduce it to the web base (origin + mount prefix).

        Splunk Web may be served under a path prefix (``web.conf root_endpoint`` or a reverse
        proxy), so a leading mount prefix is preserved — only the Splunk Web route the operator
        had open (the ``/<locale>/...`` segment onward, or a bare ``/app/...`` route) is dropped.
        Returns ``(base, None)`` when usable, else ``(None, error_message)``. Shared by
        :meth:`_check_connection_view_in_siem` and :meth:`build_view_in_siem_url` so the URL
        checks and their messages live in one place.
        """
        parsed = urlparse(splunk_url.strip())
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            return None, _SPLUNK_URL_MESSAGE
        path = parsed.path
        if locale := _LOCALE_SEGMENT_RE.search(path):
            path = path[: locale.start()]
        elif "/app/" in path:
            path = path.split("/app/", 1)[0]
        elif path.endswith("/app"):
            path = path[: -len("/app")]
        # Hygiene: drop any user:pass@ userinfo from the pasted URL so it can't ride the deep link.
        netloc = parsed.netloc.rpartition("@")[2]
        return f"{parsed.scheme}://{netloc}{path.rstrip('/')}", None

    # =========================================================================
    # View in SIEM (deep-link search)
    # =========================================================================

    async def build_view_in_siem_url(
        self,
        ctx: ChainSearchContext,
        *,
        params: dict[str, Any] | None = None,
    ) -> ViewInSiemResult:
        """Build a Splunk Web deep link that opens Splunk running an SPL query for one chain.

        The query searches the first configured index and appends one subsearch block per
        additional index; every block coalesces the ``host_name_fields`` into a ``Host`` field,
        filters by the chain hostname and matched Sigma rule ids, and re-asserts the chain time
        window (``| where _time >= ... AND _time < ...``). The same window also rides the
        ``earliest``/``latest`` URL params (UTC epoch seconds) so the time picker matches. The
        link is synthesized from the configured Splunk URL — no remote call is made.
        """
        del params  # reserved
        parsed = self._validate_config_schema(ViewInSiemResult)
        if isinstance(parsed, ViewInSiemResult):
            return parsed

        if not parsed.splunk_url.strip():
            return ViewInSiemResult(status=False, message="Splunk URL is not configured.")
        indexes = [i.strip() for i in parsed.index_names if i and i.strip()]
        if not indexes:
            return ViewInSiemResult(
                status=False, message="Configure at least one index name (Index Names) for View in SIEM."
            )

        # Defensive: a backwards window (clock skew / producer bug) would make latest <= earliest,
        # which Splunk rejects outright.
        start, end = (ctx.start, ctx.end) if ctx.start <= ctx.end else (ctx.end, ctx.start)
        earliest = _epoch_boundary(start, exclusive_end=False)
        latest = _epoch_boundary(end, exclusive_end=True)
        spl = self._build_view_in_siem_spl(
            index_names=indexes,
            host_name_fields=[f.strip() for f in parsed.host_name_fields if f and f.strip()],
            sigma_rule_id_field=parsed.remapped_sigma_rule_id_field.strip() or DEFAULT_SIGMA_RULE_ID_FIELD,
            sigma_rule_ids=ctx.sigma_rule_ids,
            hostname=ctx.hostname,
            start_epoch=earliest,
            end_epoch=latest,
        )
        base, error = self._resolve_splunk_base(parsed.splunk_url)
        if error:
            return ViewInSiemResult(status=False, message=error, query=spl)
        app = parsed.custom_search_app.strip() or _DEFAULT_SEARCH_APP
        url = (
            f"{base}/{_SPLUNK_LOCALE}/app/{quote(app, safe='')}/search"
            f"?q={quote(f'search {spl}', safe='')}"
            f"&earliest={earliest}"
            f"&latest={latest}"
        )
        return ViewInSiemResult(status=True, url=url, query=spl)

    @staticmethod
    def _build_view_in_siem_spl(
        *,
        index_names: list[str],
        host_name_fields: list[str],
        sigma_rule_id_field: str,
        sigma_rule_ids: list[str],
        hostname: str | None,
        start_epoch: int,
        end_epoch: int,
    ) -> str:
        """Render the per-index union SPL query for an attack chain.

        The first index is the main search; each additional index becomes its own
        ``| append [ search index=... ]`` subsearch block so every index keeps an independent,
        explicit pipeline. Each block repeats the same time-filter / coalesce / where pipeline:
        the chain window is asserted in-block as ``| where _time >= start AND _time < end``
        (epoch seconds, ``end`` exclusive — same values as the deep link's ``earliest``/``latest``
        params) because append subsearches must stay window-bounded without relying on time-picker
        inheritance; search-time ``earliest=``/``latest=`` terms are avoided as they would
        override the picker. The returned text omits the leading ``search `` command — it reads
        like a search-bar query; the deep-link builder prepends the command for ``q``. ``where``
        matching is exact (case-sensitive), mirroring the Sentinel deep link.
        """
        pipeline = [f"| where _time >= {start_epoch} AND _time < {end_epoch}"]

        host_exprs = [_spl_field_expr(f) for f in (host_name_fields or _DEFAULT_HOST_NAME_FIELDS)]
        if len(host_exprs) == 1:
            pipeline.append(f"| eval Host={host_exprs[0]}")
        else:
            pipeline.append(f"| eval Host=coalesce({', '.join(host_exprs)})")

        conditions: list[str] = []
        if hostname and hostname.strip():
            conditions.append(f"Host=={_spl_string_literal(hostname.strip())}")
        ids = strip_nonempty_strings(sigma_rule_ids)
        sigma_field = _spl_field_expr(sigma_rule_id_field)
        if len(ids) == 1:
            conditions.append(f"{sigma_field}=={_spl_string_literal(ids[0])}")
        elif ids:
            quoted_ids = ", ".join(_spl_string_literal(rid) for rid in ids)
            conditions.append(f"{sigma_field} IN ({quoted_ids})")
        if conditions:
            pipeline.append(f"| where {' AND '.join(conditions)}")

        lines = [f"index={_spl_string_literal(index_names[0])}", *pipeline]
        for index in index_names[1:]:
            lines.append("| append")
            lines.append("    [")
            lines.append(f"    search index={_spl_string_literal(index)}")
            lines.extend(f"    {step}" for step in pipeline)
            lines.append("    ]")
        lines.append("| sort 0 - _time")
        return "\n".join(lines)
