"""Microsoft Sentinel integration — aligned with SIEM check-connection service behavior.

OAuth uses the legacy ``/oauth2/token`` endpoint with ``resource=https://management.azure.com``,
then validates workspace access via Security Insights alert rules API and permissions via
role assignments (see ``siem-check-connection-service`` ``microsoft_sentinel`` platform).

Shared config validation for :meth:`MicrosoftSentinelProvider.deploy`,
:meth:`MicrosoftSentinelProvider.search`, and :meth:`MicrosoftSentinelProvider.check_connection`
is centralized in :meth:`MicrosoftSentinelProvider._parse_validated_config`: schema validation
delegates to the base
:meth:`~apps.integrations.base.BaseIntegrationProvider._validate_config_schema` (value-free
errors), then the ARM credential-completeness check layers on top, mapping failures to
:class:`~apps.integrations.base.ConnectionResult` / :class:`~apps.integrations.base.DeployResult`
/ :class:`~apps.integrations.base.SearchResult` via
:meth:`MicrosoftSentinelProvider._config_error_result`.
"""

from __future__ import annotations

import copy
import json
import re
import time
import uuid
from datetime import UTC, datetime, timedelta
from json import JSONDecodeError
from typing import Any, Final, overload
from urllib.parse import quote, unquote

import httpx
from pydantic import BaseModel, Field, field_validator

from apps.core.constants import HTTP_USER_AGENT
from apps.core.enums import IntegrationCategory
from apps.integrations._query_escaping import _kql_string_literal
from apps.integrations.base import (
    DEFAULT_SIGMA_RULE_ID_FIELD,
    BaseIntegrationProvider,
    ChainSearchContext,
    ConnectionResult,
    DeployableIntegration,
    DeployResult,
    SearchableIntegration,
    SearchResult,
    ViewInSiemIntegration,
    ViewInSiemResult,
    host_name_fields_field,
    validate_field_name,
)
from apps.integrations.registry import IntegrationRegistry
from apps.modules.utils import strip_nonempty_strings

TIMEOUT_CHECK_CONNECTION: Final[float] = 8.0
TIMEOUT_ARM_OPERATIONS: Final[float] = 60.0
ALERT_RULES_API_VERSION: Final[str] = "2022-07-01-preview"
SKIP_SEARCH_RULE_KINDS: Final[frozenset[str]] = frozenset({"Anomaly"})

USER_AGENT_VALUE: Final[str] = HTTP_USER_AGENT

BASE_PLATFORM_HEADERS: Final[dict[str, str]] = {
    "User-Agent": USER_AGENT_VALUE,
}

# Workspace resource base (same shape as siem-check-connection-service tools.MicrosoftSentinelMiscRequests)
WORKSPACE_BASE_URL: Final[str] = (
    "https://management.azure.com/subscriptions/{subscription_id}"
    "/resourceGroups/{resource_group}"
    "/providers/Microsoft.OperationalInsights"
    "/workspaces/{workspace}"
)

# --- View in SIEM (deep-link search) ----------------------------------------------------------
# Default host-name fields for common SIEM data schemas. They pre-fill the ``host_name_fields``
# config and become the arguments of the global ``coalesce(...)`` that resolves a ``Host`` column
# across the unioned tables; operators override them to match their own schema.
_DEFAULT_HOST_NAME_FIELDS: Final[tuple[str, ...]] = (
    "Computer",
    "HostName",
    "IPAddress",
    "CallerIpAddress",
    "ClientIP",
    "tostring(TargetResources[0]['displayName'])",
    "tostring(Resources[0]['ARN'])",
)

# Azure Portal Logs (Log Analytics) deep-link contract: a ``#view`` blade carrying a JSON ``scope``
# and a plain URL-encoded query, with the period applied by the portal ``timespan`` (the query
# itself carries no time filter). The query auto-runs when the link is opened.
_LOGS_BLADE_PATH: Final[str] = "Microsoft_Azure_Monitoring_Logs/LogsBlade"
_LOGS_INITIATOR: Final[str] = "DetectFlow"
_DEFAULT_PORTAL_BASE_URL: Final[str] = "https://portal.azure.com"

# Workspace ARM resourceId embedded in a pasted Logs URL (handles plain or %2F-encoded slashes once
# the URL is unquoted). Names captured verbatim; matched case-insensitively.
_WORKSPACE_RESOURCE_ID_RE: Final[re.Pattern[str]] = re.compile(
    r"/subscriptions/[^/?#]+/resourcegroups/[^/?#]+"
    r"/providers/microsoft\.operationalinsights/workspaces/[^/?#]+",
    re.IGNORECASE,
)
_PORTAL_HOST_RE: Final[re.Pattern[str]] = re.compile(r"^(https?://[^/#?]+)", re.IGNORECASE)
# Sentinel is migrating to the Defender portal, which uses a different deep-link surface.
_DEFENDER_HOST_MARKERS: Final[tuple[str, ...]] = ("security.microsoft.com",)

# Shared validation messages for the configured Sentinel Logs URL — used by both check_connection
# and the deep-link builder so the wording can't drift between the two sites.
_DEFENDER_URL_MESSAGE: Final[str] = (
    "Defender portal URLs (security.microsoft.com) are not supported yet. Paste the Azure Portal "
    "Logs URL from Workspace Settings > Logs."
)
_NO_WORKSPACE_MESSAGE: Final[str] = (
    "Could not find a workspace in the Microsoft Sentinel URL. Copy the full Logs page URL (it must "
    "contain subscriptions, resource groups, providers, and workspaces)."
)

# Host-field reference shapes for column_ifexists guarding: a bare column name, a dynamic-indexing
# expression (``Col[...]``), or a function-wrapped column (``func(Col)``).
_PLAIN_COLUMN_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_]\w*$")
_DYNAMIC_ROOT_RE: Final[re.Pattern[str]] = re.compile(r"([A-Za-z_]\w*)\s*\[")
_FUNC_WRAPPED_COL_RE: Final[re.Pattern[str]] = re.compile(r"\(\s*([A-Za-z_]\w*)\s*[).,]")


def _timespan_boundary(value: datetime, *, ceil: bool) -> str:
    """Format a window boundary for the portal ``timespan`` as ``YYYY-MM-DDTHH:MM:SS.000Z`` (UTC).

    Uses second precision (``.000Z``). Naive values are assumed UTC. ``ceil=True`` rounds a
    sub-second END up to the next whole second so the chain's latest event isn't clipped by the
    inclusive range; otherwise the value is floored to the second.
    """
    value = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if ceil and value.microsecond:
        value = (value + timedelta(seconds=1)).replace(microsecond=0)
    else:
        value = value.replace(microsecond=0)
    return value.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _kql_host_field_expr(field: str) -> str:
    """Make a host-field reference tolerant of tables that lack the column.

    A union exposes the union of its tables' columns, and a bare reference to a column absent from
    *every* table is a KQL semantic error (``isfuzzy=true`` only tolerates missing tables). So each
    configured host field is wrapped with ``column_ifexists`` (missing → empty/null, which KQL
    ``coalesce`` skips for strings):

    * plain column ``Computer`` → ``column_ifexists("Computer", "")``
    * indexing expr ``tostring(TargetResources[0]['displayName'])`` → root column guarded
    * function-wrapped column ``tostring(Resources)`` → ``tostring(column_ifexists("Resources", dynamic(null)))``

    A more complex custom expression matching none of these is returned verbatim — the operator owns
    column guarding in that advanced case.
    """
    field = field.strip()
    if _PLAIN_COLUMN_RE.match(field):
        return f'column_ifexists("{field}", "")'
    if _DYNAMIC_ROOT_RE.search(field):
        return _DYNAMIC_ROOT_RE.sub(lambda m: f'column_ifexists("{m.group(1)}", dynamic(null))[', field, count=1)
    if m := _FUNC_WRAPPED_COL_RE.search(field):
        return f'{field[: m.start(1)]}column_ifexists("{m.group(1)}", dynamic(null)){field[m.end(1) :]}'
    return field


class MicrosoftSentinelConfig(BaseModel):
    """Config schema for the Microsoft Sentinel integration.

    One profile serves two capability groups; all fields are optional at the schema level so a
    profile can be configured for either purpose, and each capability validates the subset it
    needs at call time:

    * **View in SIEM** (deep-link search, no remote call): ``sentinel_url``, ``table_names``,
      ``remapped_sigma_rule_id_field``, ``host_name_fields``.
    * **Rule deployment / search via Azure ARM**: ``client_id`` … ``workspace`` (validated by
      :meth:`MicrosoftSentinelProvider._credential_validation_message` when deploy/search run).
      **Commented out for this release** — the ARM capability group is written but not yet
      released, so its fields must not render in the frontend config form (the form is built
      from this model's JSON Schema). Uncomment when the ARM group ships; until then
      ``_credential_validation_message`` always reports missing credentials, so deploy/search
      fail gracefully and ``check_connection`` uses the View-in-SIEM URL branch only.
    """

    # --- View in SIEM (deep-link search) ---
    sentinel_url: str = Field(
        default="",
        description=(
            "URL of your Microsoft Sentinel web console. Open your workspace, select "
            "Workspace Settings > Logs, and copy the full URL of the page. It should contain the "
            "following parameters: subscriptions, resource groups, providers, and workspaces."
        ),
        json_schema_extra={
            "ui_order": 1,
            "placeholder": "https://portal.azure.com/#@<tenant>/resource/subscriptions/.../workspaces/<name>/logs",
        },
    )
    table_names: list[str] = Field(
        default_factory=list,
        description="The Microsoft Sentinel tables where tagged events from destination topics are stored.",
        json_schema_extra={"ui_order": 2, "placeholder": "SecurityEvent, SigninLogs, AuditLogs"},
    )
    remapped_sigma_rule_id_field: str = Field(
        default="",
        description=(
            "Optional. If you remap the Sigma Rule ID field when ingesting data from destination "
            "topics into Microsoft Sentinel, specify the new field name here."
        ),
        json_schema_extra={"ui_order": 3, "placeholder": "sigma_rule_id"},
    )
    host_name_fields: list[str] = host_name_fields_field(_DEFAULT_HOST_NAME_FIELDS, ui_order=4)

    # --- Azure ARM (rule deployment / search) — optional ---
    # RELEASE SCOPE: commented out — the ARM capability group (deploy/search/ARM check) is not in
    # this release, and the frontend renders the config form from this schema. Uncomment to ship.
    # client_id: str = Field(
    #     default="",
    #     description="Azure AD Application (client) ID",
    #     json_schema_extra={
    #         "ui_order": 5,
    #         "placeholder": "aabbccdd-1111-2222-3333-445566778899",
    #     },
    # )
    # client_secret: str = Field(
    #     default="",
    #     description="Azure AD Application client secret",
    #     json_schema_extra={
    #         "sensitive": True,
    #         "ui_order": 6,
    #     },
    # )
    # tenant_id: str = Field(
    #     default="",
    #     description="Azure AD Tenant ID",
    #     json_schema_extra={
    #         "ui_order": 7,
    #         "placeholder": "aabbccdd-1111-2222-3333-445566778899",
    #     },
    # )
    # subscription_id: str = Field(
    #     default="",
    #     description="Azure subscription ID containing the Log Analytics workspace",
    #     json_schema_extra={
    #         "ui_order": 8,
    #         "placeholder": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    #     },
    # )
    # resource_group: str = Field(
    #     default="",
    #     description="Resource group name of the Log Analytics workspace",
    #     json_schema_extra={"ui_order": 9},
    # )
    # workspace: str = Field(
    #     default="",
    #     description="Log Analytics workspace name (Microsoft Sentinel workspace)",
    #     json_schema_extra={"ui_order": 10},
    # )

    @field_validator("remapped_sigma_rule_id_field")
    @classmethod
    def _field_name_is_safe(cls, value: str) -> str:
        return validate_field_name(value)


@IntegrationRegistry.register
class MicrosoftSentinelProvider(
    BaseIntegrationProvider,
    DeployableIntegration,
    SearchableIntegration,
    ViewInSiemIntegration,
):
    integration_type = "microsoft_sentinel"
    display_name = "Microsoft Sentinel"
    category = IntegrationCategory.SIEM
    description = (
        "Connect to a Microsoft Sentinel instance to validate your data schema, "
        "stage and deploy detection rules, and manage existing content."
    )
    icon = "microsoft_sentinel"

    _connection_error: Final[str] = "There's an issue with your credentials: {0}"
    _permission_error: Final[str] = "Your credentials are valid, but there's an issue with permissions: {0}"

    @staticmethod
    def _response_error_snippet(response: httpx.Response, *, max_len: int = 400) -> str:
        """Best-effort extract from Azure / OAuth JSON error bodies or raw text."""
        try:
            data = response.json()
            if isinstance(data, dict):
                if desc := data.get("error_description"):
                    return str(desc)[:max_len]
                err = data.get("error")
                if isinstance(err, dict) and err.get("message"):
                    return str(err["message"])[:max_len]
                if isinstance(err, str):
                    return err[:max_len]
        except (JSONDecodeError, TypeError):
            pass
        text = (response.text or "").strip()
        return text[:max_len] if text else (response.reason or "")

    @staticmethod
    def _workspace_url_params(cfg: MicrosoftSentinelConfig) -> dict[str, str]:
        return {
            "subscription_id": cfg.subscription_id.strip(),
            "resource_group": cfg.resource_group.strip(),
            "workspace": cfg.workspace.strip(),
        }

    @staticmethod
    def _check_config_url(url_params: dict[str, str]) -> str:
        base = WORKSPACE_BASE_URL.format_map(url_params)
        return f"{base}/providers/Microsoft.SecurityInsights/alertRules?api-version=2019-01-01-preview"

    @staticmethod
    def _check_permission_url(url_params: dict[str, str]) -> str:
        return (
            "https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}"
            "/providers/Microsoft.OperationalInsights/workspaces/{workspace}"
            "/providers/Microsoft.Authorization"
            "/roleAssignments?api-version=2020-10-01-preview"
        ).format_map(url_params)

    @classmethod
    def required_config_fields(cls) -> tuple[str, ...]:
        """View-in-SIEM essentials the config form marks required (asterisk + blocked submit).

        Everything except ``remapped_sigma_rule_id_field`` (only set when the Sigma Rule ID field
        is remapped). Required fields keep their defaults on :class:`MicrosoftSentinelConfig` so the
        form still pre-fills them.
        """
        return ("sentinel_url", "table_names", "host_name_fields")

    @classmethod
    def get_sensitive_fields(cls) -> set[str]:
        """Keep ``client_secret`` masked while the ARM fields are commented out (release scope).

        Auto-detection only sees fields declared on the schema, but profiles saved before the
        comment-out may still carry a stored secret — without this override GET responses would
        echo it in clear text.
        """
        return super().get_sensitive_fields() | {"client_secret"}

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return MicrosoftSentinelConfig

    @staticmethod
    def _credential_validation_message(validated: MicrosoftSentinelConfig) -> str | None:
        # getattr with "" defaults: the ARM fields are commented out of MicrosoftSentinelConfig
        # while the ARM capability group is unreleased, so direct attribute access would raise
        # AttributeError on every check_connection (incl. the auto-check on profile create).
        # Restore plain attribute access when the fields are uncommented.
        if not (
            getattr(validated, "client_id", "")
            and getattr(validated, "client_secret", "")
            and getattr(validated, "tenant_id", "")
        ):
            return "Something of App ID, Tenant ID or App Secret is missing"
        if not (
            getattr(validated, "subscription_id", "")
            and getattr(validated, "resource_group", "")
            and getattr(validated, "workspace", "")
        ):
            return "Some of workspace data is missing (subscription, resource group, or workspace name)"
        return None

    @staticmethod
    def _config_error_result(
        error_result_type: type[ConnectionResult] | type[DeployResult] | type[SearchResult],
        message: str,
    ) -> ConnectionResult | DeployResult | SearchResult:
        """Build a failed provider result for credential/workspace completeness errors.

        Schema-validation failures are handled by the base
        :meth:`~apps.integrations.base.BaseIntegrationProvider.invalid_config_result`
        (value-free); this only maps an already-formatted credential message onto the
        caller's result type.

        Raises:
            TypeError: If ``error_result_type`` is not one of the three supported classes
                (exact class identity — subclasses are not matched).
        """
        if error_result_type is ConnectionResult:
            return ConnectionResult(success=False, message=message)
        if error_result_type is DeployResult:
            return DeployResult(status=False, message=message, items=[])
        if error_result_type is SearchResult:
            return SearchResult(status=False, message=message, items=[], next_cursor=None)
        raise TypeError(f"Unsupported error result type: {error_result_type!r}")

    @overload
    def _parse_validated_config(
        self,
        error_result_type: type[ConnectionResult],
    ) -> MicrosoftSentinelConfig | ConnectionResult: ...

    @overload
    def _parse_validated_config(
        self,
        error_result_type: type[DeployResult],
    ) -> MicrosoftSentinelConfig | DeployResult: ...

    @overload
    def _parse_validated_config(
        self,
        error_result_type: type[SearchResult],
    ) -> MicrosoftSentinelConfig | SearchResult: ...

    def _parse_validated_config(
        self,
        error_result_type: type[ConnectionResult] | type[DeployResult] | type[SearchResult],
    ) -> MicrosoftSentinelConfig | ConnectionResult | DeployResult | SearchResult:
        """Validate ``self.config`` against :class:`MicrosoftSentinelConfig` and credential fields.

        Args:
            error_result_type: Model to instantiate on failure (must match the calling
                operation: ``check_connection`` → ``ConnectionResult``, ``deploy`` →
                ``DeployResult``, ``search`` → ``SearchResult``).

        Returns:
            Parsed :class:`MicrosoftSentinelConfig` on success; otherwise the corresponding
            error model from :meth:`_config_error_result`.
        """
        validated = self._validate_config_schema(error_result_type)
        if not isinstance(validated, MicrosoftSentinelConfig):
            return validated
        if cred := self._credential_validation_message(validated):
            return self._config_error_result(error_result_type, self._connection_error.format(cred))
        return validated

    @staticmethod
    def _security_insights_base(url_params: dict[str, str]) -> str:
        """Path up to ``.../Microsoft.SecurityInsights`` (same as SIEM ``get_base_url`` + provider)."""
        return f"{WORKSPACE_BASE_URL.format_map(url_params)}/providers/Microsoft.SecurityInsights"

    @classmethod
    def _alert_rule_put_url(cls, url_params: dict[str, str], rule_id: str) -> str:
        """PUT a single scheduled rule — ``siem-check-connection-service`` ``AzureRuleClient.create_azure_api_url``."""
        safe_id = quote(rule_id, safe="()-._")
        base = cls._security_insights_base(url_params)
        return f"{base}/alertRules/{safe_id}?api-version={ALERT_RULES_API_VERSION}"

    @classmethod
    def _alert_rules_list_url(cls, url_params: dict[str, str]) -> str:
        """First page of alert rules — ``integrations_service`` ``get_content`` (without Log Analytics hits)."""
        base = cls._security_insights_base(url_params)
        return f"{base}/alertRules?api-version={ALERT_RULES_API_VERSION}"

    @staticmethod
    def _change_rule_type_from_string_to_dict(
        rule_dump: dict[str, Any] | str,
    ) -> dict[str, Any] | str:
        """Mirror ``BaseMicrosoftSentinelClient.change_rule_type_from_string_to_dict``."""
        if isinstance(rule_dump, str):
            try:
                parsed = json.loads(rule_dump)
                return parsed if isinstance(parsed, dict) else rule_dump
            except JSONDecodeError:
                return rule_dump
        return rule_dump

    @staticmethod
    def _normalize_rule_dict(rule: dict[str, Any]) -> dict[str, Any]:
        """Flatten ARM-style ``{"properties": {...}}`` for ``parse_rule``."""
        if "properties" in rule and isinstance(rule["properties"], dict):
            merged = {**rule["properties"], **{k: v for k, v in rule.items() if k != "properties"}}
            return merged
        return dict(rule)

    @staticmethod
    def _build_scheduled_alert_rule(
        rule: dict[str, Any],
        *,
        create_incident: bool = True,
    ) -> dict[str, Any]:
        """Build scheduled rule JSON — same logic as ``AzureRuleClient.parse_rule`` (both codebases)."""

        def check_format(rule_time: str) -> str:
            if not rule_time:
                return rule_time
            if re.match(r"(P[0-9]+H)", rule_time):
                return f"PT{rule_time[1:]}"
            if re.match(r"([0-9]+H)", rule_time):
                return f"PT{rule_time}"
            if re.match(r"([0-9]+[mM])", rule_time):
                return f"PT{rule_time.upper()}"
            return rule_time

        if create_incident is None:
            create_incident = True
        new_rule = copy.deepcopy(rule)
        new_rule["queryFrequency"] = check_format(str(rule.get("queryFrequency", "") or ""))
        new_rule["queryPeriod"] = check_format(str(rule.get("queryPeriod", "") or ""))
        new_rule["suppressionDuration"] = check_format(str(rule.get("suppressionDuration", "") or ""))
        new_rule["incidentConfiguration"] = {"createIncident": create_incident}

        tactics = rule.get("tactics", [])
        new_rule["tactics"] = [str(t).replace(" ", "") for t in tactics]

        techniques: list[str] = []
        for item in rule.get("techniques", []):
            if isinstance(item, str):
                techniques.append(item.split(".", 1)[0])
            elif isinstance(item, dict):
                if tid := item.get("id") or "":
                    techniques.append(str(tid).split(".", 1)[0])
        new_rule["techniques"] = list(set(techniques))

        return {"kind": "Scheduled", "type": "Microsoft.SecurityInsights/alertRules", "properties": new_rule}

    async def _fetch_management_token(
        self,
        client: httpx.AsyncClient,
        cfg: MicrosoftSentinelConfig,
    ) -> tuple[str | None, str | None]:
        """OAuth client credentials against legacy token endpoint (same as SIEM service)."""
        token_url = f"https://login.microsoftonline.com/{cfg.tenant_id}/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": cfg.client_id,
            "client_secret": cfg.client_secret,
            "resource": "https://management.azure.com",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded", **BASE_PLATFORM_HEADERS}
        response = await client.post(token_url, data=data, headers=headers)
        if response.status_code != 200:
            snippet = self._response_error_snippet(response)
            return None, (f"Microsoft Sentinel Authentication failed: HTTP {response.status_code} — {snippet}")
        try:
            body = response.json()
        except Exception as exc:
            return None, f"Microsoft Sentinel Authentication failed: invalid token response ({exc})"

        access_token = body.get("access_token")
        expires_on = body.get("expires_on")
        if not access_token:
            return None, "Microsoft Sentinel Authentication failed: missing access_token"
        if expires_on is not None:
            try:
                if int(time.time()) > int(expires_on):
                    return None, ("Microsoft Sentinel Authentication failed: token expired (incorrect expires_on)")
            except (TypeError, ValueError):
                pass
        return access_token, None

    def _management_request_headers(self, access_token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": USER_AGENT_VALUE,
        }

    async def _check_config(
        self,
        client: httpx.AsyncClient,
        url_params: dict[str, str],
        headers: dict[str, str],
    ) -> tuple[bool, str]:
        """GET alert rules — validates workspace + Security Insights access."""
        url = self._check_config_url(url_params)
        response = await client.get(url, headers=headers)
        if response.is_success:
            return True, "Credentials are correct."
        detail = self._response_error_snippet(response)
        return False, f"Import to Azure Sentinel has failed. Azure API returned error: {detail}"

    async def _check_permission(
        self,
        client: httpx.AsyncClient,
        url_params: dict[str, str],
        headers: dict[str, str],
    ) -> tuple[bool, str]:
        """GET role assignments — permission probe (same URL as SIEM service)."""
        url = self._check_permission_url(url_params)
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            return True, "Credentials are valid."
        detail = self._response_error_snippet(response)
        return False, f"Error: {response.status_code} {response.reason} {detail}".strip()

    async def deploy(
        self,
        rules: list[str | dict[str, Any]],
        *,
        params: dict[str, Any] | None = None,
    ) -> DeployResult:
        """Create/update scheduled analytics rules via ARM (``AzureRuleClient.upload_rule_to_sentinel``).

        Config is validated via :meth:`_parse_validated_config` before any HTTP calls.

        Each rule dict should contain Sentinel ``properties`` fields (``queryFrequency``, ``query``,
        ``displayName``, …). Optional ``rule_id`` / ``name`` sets the ARM resource name; default is a
        new UUID. ``params.create_incident`` controls ``incidentConfiguration`` (default ``True``).

        Returns:
            :class:`~apps.integrations.base.DeployResult` with per-rule outcomes in ``items``
            (each dict: ``success``, ``rule_id``, ``message``). Network errors (DNS, timeout,
            connection refused) return ``status=False`` — never raise past the manager.
        """
        opts = params or {}
        parsed = self._parse_validated_config(DeployResult)
        if isinstance(parsed, DeployResult):
            return parsed

        url_params = self._workspace_url_params(parsed)
        per_rule_outcomes: list[dict[str, Any]] = []

        try:
            return await self._deploy_rules(rules, parsed, url_params, per_rule_outcomes, opts)
        except httpx.TimeoutException:
            return DeployResult(status=False, message="Connection timed out", items=per_rule_outcomes)
        except httpx.RequestError as e:
            return DeployResult(status=False, message=f"Network error: {e}", items=per_rule_outcomes)

    async def _deploy_rules(
        self,
        rules: list[str | dict[str, Any]],
        parsed: MicrosoftSentinelConfig,
        url_params: dict[str, str],
        per_rule_outcomes: list[dict[str, Any]],
        opts: dict[str, Any],
    ) -> DeployResult:
        """ARM deploy loop; network errors propagate to :meth:`deploy`'s try/except."""
        async with httpx.AsyncClient(timeout=TIMEOUT_ARM_OPERATIONS) as client:
            token, token_err = await self._fetch_management_token(client, parsed)
            if token_err:
                return DeployResult(status=False, message=token_err, items=[])

            headers = self._management_request_headers(token)
            for raw_rule in rules:
                coerced = self._change_rule_type_from_string_to_dict(raw_rule)
                if not isinstance(coerced, dict):
                    per_rule_outcomes.append(
                        {
                            "success": False,
                            "rule_id": None,
                            "message": "Rule must be a JSON object (or JSON string that parses to one)",
                        }
                    )
                    continue
                rule_body = self._normalize_rule_dict(coerced)
                rule_id = coerced.get("rule_id") or coerced.get("name") or rule_body.get("displayName")
                if not rule_id:
                    rule_id = str(uuid.uuid4())
                try:
                    payload = self._build_scheduled_alert_rule(
                        rule_body, create_incident=opts.get("create_incident", True)
                    )
                except Exception as exc:
                    per_rule_outcomes.append({"success": False, "rule_id": str(rule_id), "message": str(exc)})
                    continue
                response = await client.put(
                    self._alert_rule_put_url(url_params, str(rule_id)),
                    json=payload,
                    headers=headers,
                )
                if response.is_success:
                    per_rule_outcomes.append(
                        {
                            "success": True,
                            "rule_id": str(rule_id),
                            "message": "You can view imported rule in Azure Portal",
                        }
                    )
                else:
                    per_rule_outcomes.append(
                        {
                            "success": False,
                            "rule_id": str(rule_id),
                            "message": self._response_error_snippet(response),
                        }
                    )

        return DeployResult(
            status=all(r.get("success") for r in per_rule_outcomes) if per_rule_outcomes else True,
            items=per_rule_outcomes,
        )

    async def search(
        self,
        query: str | None = None,
        *,
        external_ids: list[str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
        params: dict[str, Any] | None = None,
    ) -> SearchResult:
        """List alert rules (``integrations_service`` ``get_content``) without Log Analytics enrichment.

        Config is validated via :meth:`_parse_validated_config` before any HTTP calls.

        Uses ARM pagination ``nextLink`` as ``next_cursor``. Optional ``query`` filters by
        ``displayName`` (case-insensitive substring). Skips ``kind == Anomaly`` like integrations_service.

        Returns:
            :class:`~apps.integrations.base.SearchResult` with ``items`` and optional
            ``next_cursor``. Network errors return ``status=False`` — never raise past the manager.
        """
        del params  # reserved
        parsed = self._parse_validated_config(SearchResult)
        if isinstance(parsed, SearchResult):
            return parsed
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT_ARM_OPERATIONS) as client:
                token, token_err = await self._fetch_management_token(client, parsed)
                if token_err:
                    return SearchResult(status=False, message=token_err, items=[], next_cursor=None)
                response = await client.get(
                    cursor or self._alert_rules_list_url(self._workspace_url_params(parsed)),
                    headers=self._management_request_headers(token),
                )
                if not response.is_success:
                    return SearchResult(
                        status=False, message=self._response_error_snippet(response), items=[], next_cursor=None
                    )
                try:
                    data = response.json()
                except JSONDecodeError:
                    return SearchResult(
                        status=False, message="Invalid JSON from Azure alertRules list", items=[], next_cursor=None
                    )
        except httpx.TimeoutException:
            return SearchResult(status=False, message="Connection timed out", items=[], next_cursor=None)
        except httpx.RequestError as e:
            return SearchResult(status=False, message=f"Network error: {e}", items=[], next_cursor=None)

        filtered: list[dict[str, Any]] = []
        ext_set = set(external_ids) if external_ids else None
        query_stripped = (query or "").strip().lower()

        for item in data.get("value") or []:
            kind = item.get("kind")
            if kind in SKIP_SEARCH_RULE_KINDS:
                continue
            name = item.get("name") or ""
            if ext_set is not None and name not in ext_set:
                continue
            props = item.get("properties") or {}
            display = str(props.get("displayName") or "")
            if query_stripped and query_stripped not in display.lower():
                continue
            filtered.append({"name": name, "kind": kind, "properties": props})

        if limit > 0 and len(filtered) > limit:
            filtered = filtered[:limit]
        return SearchResult(status=True, items=filtered, next_cursor=data.get("nextLink"))

    async def check_connection(self) -> ConnectionResult:
        """Capability-aware connection check.

        A single profile may be configured for ARM rule deployment/search (full Azure
        credentials) and/or for View in SIEM (a Microsoft Sentinel URL only). The check
        validates whatever the profile is set up for:

        * ARM credentials complete → probe token, workspace API access, and RBAC.
        * Otherwise, a Microsoft Sentinel URL is set → validate it resolves to a workspace
          (View-in-SIEM readiness), no remote call.
        * Neither → fail with guidance.
        """
        parsed = self._validate_config_schema(ConnectionResult)
        if isinstance(parsed, ConnectionResult):
            return parsed
        if self._credential_validation_message(parsed) is None:
            return await self._check_connection_arm(parsed)
        if parsed.sentinel_url.strip():
            return self._check_connection_view_in_siem(parsed)
        return ConnectionResult(
            success=False,
            message=(
                "Provide Azure credentials (App ID, Tenant ID, App Secret, subscription, resource "
                "group, workspace) for rule deployment, or a Microsoft Sentinel URL for View in SIEM."
            ),
        )

    async def _check_connection_arm(self, parsed: MicrosoftSentinelConfig) -> ConnectionResult:
        """Probe Azure ARM: management token → workspace alert-rules API → role assignments (RBAC)."""
        url_params = self._workspace_url_params(parsed)
        try:
            async with httpx.AsyncClient(timeout=TIMEOUT_CHECK_CONNECTION) as client:
                token, token_err = await self._fetch_management_token(client, parsed)
                if token_err:
                    return ConnectionResult(
                        success=False,
                        message=self._connection_error.format(
                            "Check Azure Sentinel rule credentials. Error: Couldn't get valid token."
                        ),
                        details={"detail": token_err},
                    )
                headers = self._management_request_headers(token)
                workspace_api_ok, workspace_api_message = await self._check_config(client, url_params, headers)
                if not workspace_api_ok:
                    return ConnectionResult(success=False, message=self._connection_error.format(workspace_api_message))
                rbac_ok, rbac_message = await self._check_permission(client, url_params, headers)
                if not rbac_ok:
                    return ConnectionResult(success=False, message=self._permission_error.format(rbac_message))
                return ConnectionResult(success=True, message=rbac_message)
        except httpx.TimeoutException:
            return ConnectionResult(success=False, message="Connection timed out")
        except httpx.RequestError as e:
            return ConnectionResult(success=False, message=self._connection_error.format(f"Network error: {e}"))
        except Exception as e:
            return ConnectionResult(
                success=False, message=self._connection_error.format(f"Check credentials to Azure Sentinel. Error: {e}")
            )

    def _check_connection_view_in_siem(self, parsed: MicrosoftSentinelConfig) -> ConnectionResult:
        """Validate the Microsoft Sentinel URL resolves to a Log Analytics workspace (no remote call)."""
        _, error = self._resolve_sentinel_resource_id(parsed.sentinel_url)
        if error:
            return ConnectionResult(success=False, message=error)
        return ConnectionResult(success=True, message="Microsoft Sentinel URL is valid. Ready for View in SIEM.")

    @classmethod
    def _resolve_sentinel_resource_id(cls, sentinel_url: str) -> tuple[str | None, str | None]:
        """Validate a pasted Sentinel Logs URL and extract its workspace ARM resourceId.

        Returns ``(resource_id, None)`` when usable, else ``(None, error_message)``. Shared by
        :meth:`_check_connection_view_in_siem` and :meth:`_build_logs_deeplink` so the URL checks
        and their messages live in one place.
        """
        if cls._is_defender_portal_url(sentinel_url):
            return None, _DEFENDER_URL_MESSAGE
        resource_id = cls._extract_workspace_resource_id(sentinel_url)
        if resource_id is None:
            return None, _NO_WORKSPACE_MESSAGE
        return resource_id, None

    # =========================================================================
    # View in SIEM (deep-link search)
    # =========================================================================

    async def build_view_in_siem_url(
        self,
        ctx: ChainSearchContext,
        *,
        params: dict[str, Any] | None = None,
    ) -> ViewInSiemResult:
        """Build an Azure Portal Logs deep link that opens Sentinel running a KQL query for one chain.

        The query unions the configured ``table_names``, coalesces the ``host_name_fields`` into a
        ``Host`` column, and filters by the chain hostname and matched Sigma rule ids; the chain
        time window is applied by the portal ``timespan`` in the link. The link is synthesized from
        the configured Microsoft Sentinel URL — no remote call is made.
        """
        del params  # reserved
        parsed = self._validate_config_schema(ViewInSiemResult)
        if isinstance(parsed, ViewInSiemResult):
            return parsed

        if not parsed.sentinel_url.strip():
            return ViewInSiemResult(status=False, message="Microsoft Sentinel URL is not configured.")
        tables = [t.strip() for t in parsed.table_names if t and t.strip()]
        if not tables:
            return ViewInSiemResult(
                status=False, message="Configure at least one table name (Table Names) for View in SIEM."
            )

        kql = self._build_view_in_siem_kql(
            tables=tables,
            host_name_fields=[f.strip() for f in parsed.host_name_fields if f and f.strip()],
            sigma_rule_id_field=parsed.remapped_sigma_rule_id_field.strip() or DEFAULT_SIGMA_RULE_ID_FIELD,
            sigma_rule_ids=ctx.sigma_rule_ids,
            hostname=ctx.hostname,
        )
        # Defensive: a backwards window (clock skew / producer bug) would yield an empty timespan.
        start, end = (ctx.start, ctx.end) if ctx.start <= ctx.end else (ctx.end, ctx.start)
        url, error = self._build_logs_deeplink(parsed.sentinel_url, kql, start, end)
        if error:
            return ViewInSiemResult(status=False, message=error, query=kql)
        return ViewInSiemResult(status=True, url=url, query=kql)

    @staticmethod
    def _build_view_in_siem_kql(
        *,
        tables: list[str],
        host_name_fields: list[str],
        sigma_rule_id_field: str,
        sigma_rule_ids: list[str],
        hostname: str | None,
    ) -> str:
        """Render the union / coalesce / filter KQL query for an attack chain.

        The query carries no ``TimeGenerated`` filter — the period is applied by the portal
        ``timespan`` in the deep link. Host fields are wrapped with ``column_ifexists`` so the global
        coalesce stays valid even when a configured table lacks some
        host columns: a union exposes the union of columns, and a bare reference to a column absent
        from *every* table is a KQL error that ``isfuzzy=true`` (only tolerates missing tables) does
        not rescue.
        """
        union_lines = ",\n".join(f"    ({table})" for table in tables)
        lines = [f"union isfuzzy=true\n{union_lines}"]

        host_exprs = [_kql_host_field_expr(f) for f in (host_name_fields or _DEFAULT_HOST_NAME_FIELDS)]
        if len(host_exprs) == 1:
            lines.append(f"| extend Host = {host_exprs[0]}")
        else:
            lines.append(f"| extend Host = coalesce({', '.join(host_exprs)})")

        conditions: list[str] = []
        if hostname and hostname.strip():
            conditions.append(f"Host == {_kql_string_literal(hostname.strip())}")
        ids = strip_nonempty_strings(sigma_rule_ids)
        if len(ids) == 1:
            conditions.append(f"{sigma_rule_id_field} == {_kql_string_literal(ids[0])}")
        elif ids:
            quoted_ids = ", ".join(_kql_string_literal(rid) for rid in ids)
            conditions.append(f"{sigma_rule_id_field} in ({quoted_ids})")
        if conditions:
            lines.append(f"| where {' and '.join(conditions)}")

        lines.append("| order by TimeGenerated desc")
        return "\n".join(lines)

    @staticmethod
    def _is_defender_portal_url(url: str) -> bool:
        """True when the URL points at the Defender portal (different deep-link surface)."""
        lowered = (url or "").lower()
        return any(marker in lowered for marker in _DEFENDER_HOST_MARKERS)

    @staticmethod
    def _extract_workspace_resource_id(url: str) -> str | None:
        """Pull the workspace ARM resourceId out of a pasted Logs URL (plain or %2F-encoded)."""
        if not url or not url.strip():
            return None
        match = _WORKSPACE_RESOURCE_ID_RE.search(unquote(url))
        return match.group(0) if match else None

    @classmethod
    def _build_logs_deeplink(
        cls,
        sentinel_url: str,
        kql: str,
        start: datetime,
        end: datetime,
    ) -> tuple[str | None, str | None]:
        """Build a ``#view`` Logs deep link from the pasted Sentinel URL + generated KQL.

        Extracts the workspace resourceId from the pasted URL (robust to ``/resource/.../logs`` or
        ``/blade/.../resourceId/...``, plain or %2F-encoded), embeds it in the ``scope`` JSON,
        plain-URL-encodes the query, and carries the chain window in ``timespan`` (the query itself
        has no time filter). The query auto-runs on open; the portal host is reused from the pasted
        URL (gov/china clouds preserved).

        Returns ``(url, None)`` on success or ``(None, error_message)`` on failure.
        """
        resource_id, error = cls._resolve_sentinel_resource_id(sentinel_url)
        if error:
            return None, error
        host_match = _PORTAL_HOST_RE.match(sentinel_url.strip())
        host = host_match.group(1) if host_match else _DEFAULT_PORTAL_BASE_URL
        # Hygiene: drop any user:pass@ userinfo from the pasted URL so it can't ride the deep link.
        scheme, _, authority = host.partition("://")
        host = f"{scheme}://{authority.rpartition('@')[2]}"
        timespan = quote(f"{_timespan_boundary(start, ceil=False)}/{_timespan_boundary(end, ceil=True)}", safe="")
        url = (
            f"{host}/#view/{_LOGS_BLADE_PATH}"
            f"/scope~/{cls._build_scope_param(resource_id)}"
            f"/initiator/{_LOGS_INITIATOR}"
            f"/query/{quote(kql, safe='')}"
            f"/timespan/{timespan}"
        )
        return url, None

    @staticmethod
    def _build_scope_param(resource_id: str) -> str:
        """URL-encoded ``{"resources":[{"resourceId": ...}]}`` scope for the Logs blade."""
        scope = {"resources": [{"resourceId": resource_id}]}
        return quote(json.dumps(scope, separators=(",", ":")), safe="")
