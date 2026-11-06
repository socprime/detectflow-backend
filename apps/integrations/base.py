import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar, Final

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from apps.core.enums import IntegrationCategory

# Canonical field carrying the Sigma rule id inside SIEM-ingested events tagged by the match
# node. Every View-in-SIEM provider filters on it by default; operators override it per profile
# via their provider's ``remapped_sigma_rule_id_field`` config.
DEFAULT_SIGMA_RULE_ID_FIELD: Final[str] = "SigmaRuleID"

# Allowlist for single field-name config slots that are interpolated into generated SIEM queries
# (SPL/KQL/DQL). Letters, digits, underscore, dot, hyphen — the realistic SIEM field-name shape.
_SIEM_FIELD_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][\w.\-]*$")


def validate_field_name(value: str) -> str:
    """Allowlist-validate a config field name that is interpolated into generated queries.

    Stored-injection gate: integration configs are writable by any authenticated user, and the
    View-in-SIEM query built from them auto-runs in another analyst's SIEM browser session — so
    a field name like ``x' | delete`` must never persist. Used as the Pydantic field validator
    for ``remapped_sigma_rule_id_field`` in every View-in-SIEM provider config. Empty string
    stays valid (means "use the default ``SigmaRuleID``").

    Returns:
        The stripped value.

    Raises:
        ValueError: If the value is not a plain field name (``^[A-Za-z_][\\w.\\-]*$``).
    """
    value = value.strip()
    if value and not _SIEM_FIELD_NAME_RE.match(value):
        raise ValueError(
            "must be a plain field name (letters, digits, underscore, dot, or hyphen, "
            "starting with a letter or underscore)"
        )
    return value


def host_name_fields_field(defaults: tuple[str, ...], *, ui_order: int) -> Any:
    """``host_name_fields`` config :func:`~pydantic.Field` shared by the View-in-SIEM configs.

    Plain ``default`` (not ``default_factory``) so the value is emitted in the JSON Schema and
    the frontend can pre-fill the form; Pydantic copies the default per instance, so the shared
    list is never mutated across configs. The ``defaults`` stay per-provider — each console's
    data schema differs.
    """
    return Field(
        default=list(defaults),
        description=(
            "Specify the names of the fields that can contain hostnames in your SIEM data schema. "
            "This parameter is pre-filled with default data schema values that may be different from yours."
        ),
        json_schema_extra={"ui_order": ui_order},
    )


#: Config-form key carrying the integration profile's display name. It is **not** a provider
#: config field (it is integration metadata stored in the ``integrations.name`` column) — it is
#: surfaced in the form by :func:`with_profile_name_field` and mapped to/from the name column by
#: ``IntegrationsManager``, so providers never carry it in their Pydantic config models.
PROFILE_NAME_KEY: Final[str] = "profile_name"


def with_profile_name_field(config_schema: dict[str, Any]) -> dict[str, Any]:
    """Prepend the integration-level ``profile_name`` field to a provider's config JSON Schema.

    Every integration profile has a display name. The frontend renders the create/edit form
    purely from this schema, so the name field is injected here as the first field
    (``ui_order`` 0) and marked required — it renders like any other config field — rather than
    duplicated into every provider's Pydantic model (it is integration metadata, not provider
    config). ``IntegrationsManager`` maps ``config[\"profile_name\"]`` to the profile name on write
    and re-injects it on read. Returns a shallow copy; the input schema is not mutated.
    """
    schema = dict(config_schema)
    # No description on purpose: the design renders Profile Name without a tooltip — the
    # placeholder alone explains the field.
    schema["properties"] = {
        PROFILE_NAME_KEY: {
            "type": "string",
            "title": "Profile Name",
            "ui_order": 0,
            "placeholder": "e.g., Production, Staging, Dev",
        },
        **schema.get("properties", {}),
    }
    schema["required"] = [PROFILE_NAME_KEY, *(r for r in schema.get("required", []) if r != PROFILE_NAME_KEY)]
    return schema


def with_required_config_fields(config_schema: dict[str, Any], required_fields: tuple[str, ...]) -> dict[str, Any]:
    """Mark the given config fields required in a provider's config JSON Schema.

    The frontend renders the create/edit form from this schema and shows the required asterisk
    (and blocks submit) for every entry in ``required``. These fields stay declared with defaults
    on the provider's Pydantic model so the form still pre-fills them (e.g. ``host_name_fields``);
    Pydantic only auto-marks no-default fields as required, so the form-level requirement is
    layered on here — the same post-processing approach as the injected ``profile_name`` field
    (see :func:`with_profile_name_field`). Returns a shallow copy; the input schema is not mutated.
    """
    schema = dict(config_schema)
    existing = schema.get("required", [])
    schema["required"] = [*existing, *(f for f in required_fields if f not in existing)]
    return schema


class ConnectionResult(BaseModel):
    """Result of :meth:`BaseIntegrationProvider.check_connection`.

    Check responses use ``success`` and optional ``details`` — they are **not** the same
    shape as deploy/search HTTP bodies (those often expose a ``status`` field via
    :class:`~apps.core.schemas.IntegrationDeployResponse` / :class:`~apps.core.schemas.IntegrationSearchResponse`).
    """

    success: bool
    message: str
    details: dict[str, Any] | None = None


class DeployResult(BaseModel):
    """Typed outcome of :meth:`DeployableIntegration.deploy`.

    Required: ``status`` (overall success). Optional ``message``.
    ``items`` holds per-rule (or per-chunk) outcome dicts when the provider reports
    them — e.g. Microsoft Sentinel fills one entry per rule with ``success``,
    ``rule_id``, ``message``. Other payloads
    may still use ``extra`` fields (see :class:`~apps.core.schemas.IntegrationDeployResponse`).
    """

    model_config = ConfigDict(extra="allow")

    status: bool
    message: str | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)


class SearchResult(BaseModel):
    """Typed outcome of :meth:`SearchableIntegration.search`.

    Required: ``status``. Typical list payloads use ``items`` and ``next_cursor``.
    Extra keys follow the same pattern as :class:`DeployResult` for API-specific fields.
    """

    model_config = ConfigDict(extra="allow")

    status: bool
    message: str | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)
    next_cursor: str | None = None


class ViewInSiemResult(BaseModel):
    """Typed outcome of :meth:`ViewInSiemIntegration.build_view_in_siem_url`.

    Required: ``status``. On success, ``url`` holds the ready-to-open deep link into the
    external SIEM's search UI (pre-filled and auto-running the generated query) and
    ``query`` exposes the raw query text for preview/debugging. Extra keys follow the same
    pattern as :class:`SearchResult` for provider-specific fields.
    """

    model_config = ConfigDict(extra="allow")

    status: bool
    message: str | None = None
    url: str | None = None
    query: str | None = None


def sanitize_validation_error(error: ValidationError) -> tuple[str, list[dict[str, Any]]]:
    """Reduce a Pydantic ``ValidationError`` to a value-free message and error list.

    Pydantic v2 embeds the submitted value in ``str(error)`` (``input_value=...``) and in each
    ``errors()`` entry (``input``/``ctx``); echoing those into HTTP responses leaks secrets
    pasted into the wrong field and internal hostnames/IDs. Only ``loc``/``msg``/``type`` are
    kept — every provider/manager path that surfaces config-validation failures builds from this.

    Returns:
        ``(message, errors)`` where ``message`` is ``"Invalid config: <loc>: <msg>; ..."`` and
        ``errors`` is a list of ``{"loc", "msg", "type"}`` dicts.
    """
    errors = [
        {"loc": list(err["loc"]), "msg": err["msg"], "type": err["type"]}
        for err in error.errors(include_url=False, include_context=False, include_input=False)
    ]
    message = "Invalid config: " + "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in errors)
    return message, errors


class BaseIntegrationProvider(ABC):
    """Abstract base for all integration providers.

    Every provider advertises the **check_connection** capability via
    :attr:`capability_id` (``\"check_connection\"``). Optional behaviors use
    mixins: :class:`DeployableIntegration`, :class:`SearchableIntegration`.

    To create a new provider:
        1. Subclass BaseIntegrationProvider
        2. Set class-level attributes (integration_type, display_name, etc.)
        3. Implement get_config_schema() returning a Pydantic BaseModel subclass
        4. Implement check_connection() returning :class:`ConnectionResult`
        5. Optionally subclass capability mixins (e.g. ``deploy`` / ``search``)
        6. Decorate with @IntegrationRegistry.register

    Capabilities exposed to the API are computed by :func:`get_provider_capabilities`.
    The base :attr:`capability_id` is always ``check_connection``.
    """

    integration_type: ClassVar[str]
    display_name: ClassVar[str]
    category: ClassVar[IntegrationCategory]
    description: ClassVar[str]
    icon: ClassVar[str] = ""
    capability_id: ClassVar[str] = "check_connection"
    #: If True, at most one profile row for :attr:`integration_type` may exist (enforced in DB + manager).
    singleton_profile: ClassVar[bool] = False
    sensitive_mask: ClassVar[str] = "******"

    @classmethod
    @abstractmethod
    def get_config_schema(cls) -> type[BaseModel]:
        """Return the Pydantic model class defining this provider's config fields.

        The model's JSON Schema is sent to the frontend for dynamic form rendering.
        Use json_schema_extra on fields for UI hints:
            - {"sensitive": True} — field contains a secret (masked in responses)
            - {"ui_order": N} — field display order in the form
            - {"placeholder": "..."} — placeholder text

        Hints describe how to *render* the field, not how to validate its contents: the backend
        ships no ``pattern``/``format`` validation hints. Input-shape validation is the frontend's
        concern; the server is the authoritative gate (Pydantic validators run on every write).
        """
        ...

    @classmethod
    def required_config_fields(cls) -> tuple[str, ...]:
        """Config fields the create/edit form must mark required (JSON Schema ``required``).

        Layered onto the config schema by :func:`with_required_config_fields` so the frontend
        shows the required asterisk and blocks submit, while the fields keep their Pydantic
        defaults for form pre-fill. Default: none. Override per provider.
        """
        return ()

    @classmethod
    def immutable_config_fields(cls) -> tuple[str, ...]:
        """Config fields that cannot change after the integration profile is created.

        ``IntegrationsManager.update`` rejects PATCH payloads that alter any of these fields
        (after provider-specific normalization). The list is also exposed on
        ``GET /integrations/types/{type}`` so the edit form can disable them.
        """
        return ()

    @classmethod
    def changed_immutable_config_fields(cls, existing_config: dict, merged_config: dict) -> list[str]:
        """Return immutable field names whose normalized values differ after a config merge."""
        return []

    @classmethod
    def get_sensitive_fields(cls) -> set[str]:
        """Return field names that contain secrets.

        Auto-detects from json_schema_extra={"sensitive": True}.
        """
        schema_cls = cls.get_config_schema()
        sensitive: set[str] = set()
        for field_name, field_info in schema_cls.model_fields.items():
            extra = field_info.json_schema_extra
            if isinstance(extra, dict) and extra.get("sensitive"):
                sensitive.add(field_name)
        return sensitive

    def __init__(self, config: dict[str, Any]):
        """Initialize provider with config from DB."""
        self.config = config

    @abstractmethod
    async def check_connection(self) -> ConnectionResult:
        """Test connectivity with the given config."""
        ...

    def validate_config(self) -> dict[str, Any]:
        """Validate self.config against the provider's schema."""
        schema_cls = self.get_config_schema()
        validated = schema_cls.model_validate(self.config)
        return validated.model_dump()

    @classmethod
    def invalid_config_result(
        cls,
        result_type: type[ConnectionResult] | type[DeployResult] | type[SearchResult] | type[ViewInSiemResult],
        error: ValidationError,
    ) -> ConnectionResult | DeployResult | SearchResult | ViewInSiemResult:
        """Build a provider failure result from a Pydantic ``ValidationError`` (value-free).

        The ``"Invalid config: …"`` message and (for connection checks) the
        ``details["validation_errors"]`` list come from :func:`sanitize_validation_error`,
        so submitted config values never reach the HTTP response.

        Raises:
            TypeError: If ``result_type`` is not one of the four supported result classes
                (exact class identity — subclasses are not matched).
        """
        message, errors = sanitize_validation_error(error)
        if result_type is ConnectionResult:
            return ConnectionResult(success=False, message=message, details={"validation_errors": errors})
        if result_type is DeployResult:
            return DeployResult(status=False, message=message, items=[])
        if result_type is SearchResult:
            return SearchResult(status=False, message=message, items=[], next_cursor=None)
        if result_type is ViewInSiemResult:
            return ViewInSiemResult(status=False, message=message)
        raise TypeError(f"Unsupported error result type: {result_type!r}")

    def _validate_config_schema(
        self,
        result_type: type[ConnectionResult] | type[DeployResult] | type[SearchResult] | type[ViewInSiemResult],
    ) -> BaseModel | ConnectionResult | DeployResult | SearchResult | ViewInSiemResult:
        """Validate ``self.config`` against :meth:`get_config_schema` (schema only).

        Returns the parsed config model on success, or an instance of ``result_type`` built by
        :meth:`invalid_config_result` on failure (``BaseModel | <result_type>`` uniformly —
        callers narrow with ``isinstance`` against ``result_type``).
        """
        try:
            return self.get_config_schema().model_validate(self.config)
        except ValidationError as e:
            return self.invalid_config_result(result_type, e)

    @classmethod
    def mask_sensitive_config(cls, config: dict, sensitive_fields: set[str]) -> dict:
        """Replace sensitive field values with :attr:`sensitive_mask` (default ``******``).

        Used when serializing integration config for ``GET /integrations/{id}``.
        Override per provider if a different masking shape is required (see ``SocPrimeTdmProvider``).
        """
        masked = dict(config)
        for field in sensitive_fields:
            if field in masked and masked[field]:
                masked[field] = cls.sensitive_mask
        return masked

    @classmethod
    def merge_config_update(
        cls,
        existing_config: dict,
        new_config: dict,
        sensitive_fields: set[str],
    ) -> dict:
        """Merge PATCH ``new_config`` onto ``existing_config``.

        For keys listed in ``sensitive_fields``, if ``cls.sensitive_mask`` appears as a
        **substring** of the submitted ``value``, the previous stored value is kept
        (client echoed the masked GET response). Otherwise ``value`` replaces the secret.

        Default ``sensitive_mask`` is ``******`` (substring works when the client sends
        exactly that placeholder). Providers may override ``sensitive_mask`` (e.g. TDM uses
        ``...``); substring semantics still apply.
        """
        merged = dict(existing_config)
        for key, value in new_config.items():
            if key in sensitive_fields and cls.sensitive_mask in value:
                continue  # Keep existing secret
            merged[key] = value
        return merged


class DeployableIntegration(ABC):
    """Mixin for providers that can **push** rules to an external system.

    Advertises capability ``deploy_rules`` (:attr:`capability_id`). Implemented by
    :meth:`deploy`. Invoked from the API via ``POST /api/v1/integrations/{id}/deploy``.
    """

    capability_id: ClassVar[str] = "deploy_rules"

    @abstractmethod
    async def deploy(
        self,
        rules: list[str | dict[str, Any]],
        *,
        params: dict[str, Any] | None = None,
    ) -> DeployResult:
        """Create or update rules in the target system.

        Args:
            rules: Normalized rule payloads for the target API (format is provider-specific).
            params: Optional workspace ids, folders, overwrite flags, etc.

        Returns:
            :class:`DeployResult` with at least ``status``; optional ``message``,
            optional ``items`` (per-rule outcomes), and provider-specific extra fields.
        """
        ...


class SearchableIntegration(ABC):
    """Mixin for providers that support **search** or listing remote content.

    Advertises capability ``search`` (:attr:`capability_id`). Implemented by
    :meth:`search`. Invoked from the API via ``POST /api/v1/integrations/{id}/search``.
    """

    capability_id: ClassVar[str] = "search"

    @abstractmethod
    async def search(
        self,
        query: str | None = None,
        *,
        external_ids: list[str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
        params: dict[str, Any] | None = None,
    ) -> SearchResult:
        """Query or list items in the external system.

        Args:
            query: Free-text search where the backend supports it; may be ignored.
            external_ids: If set, restrict to these external ids when the API supports it.
            cursor: Pagination opaque cursor from a previous response.
            limit: Page size for APIs that paginate.
            params: Provider-specific filters (repo, collection, environment, …).

        Returns:
            :class:`SearchResult` with ``status``; typically ``items`` and ``next_cursor`` for lists.
        """
        ...


class PullableIntegration:
    """Mixin for providers that support on-demand / scheduled rule pulls from Git hosting.

    Advertises capability ``pull_rules`` (:attr:`capability_id`). The manager dispatches to
    ``GitlabSyncManager`` — providers do not implement pull themselves.
    """

    capability_id: ClassVar[str] = "pull_rules"


@dataclass(frozen=True)
class ChainSearchContext:
    """SIEM-agnostic inputs derived from one attack chain for a View-in-SIEM deep link.

    Resolved by the manager from chain data and passed to every :class:`ViewInSiemIntegration`
    provider. Add fields here (e.g. ``username``, ``severity``, ``technique_ids``) as new search
    dimensions are needed — providers read only the fields they use, so new fields stay additive
    and don't churn the contract or any provider signature.

    Attributes:
        sigma_rule_ids: Distinct Sigma rule UUIDs (as strings) that matched within the chain, as
            ingested into the SIEM under the operator's chosen field.
        hostname: Affected host for the chain (``None`` → host filter omitted).
        start: Inclusive window start (the chain's earliest event), timezone-aware.
        end: Inclusive window end (the chain's latest event), timezone-aware.
    """

    sigma_rule_ids: list[str]
    hostname: str | None
    start: datetime
    end: datetime


class ViewInSiemIntegration(ABC):
    """Mixin for providers that can build a **deep link** into the external SIEM's search UI.

    Advertises capability ``view_in_siem`` (:attr:`capability_id`). Implemented by
    :meth:`build_view_in_siem_url`. Invoked from the API via
    ``POST /api/v1/integrations/{id}/view-in-siem``.

    Unlike :class:`SearchableIntegration` (which queries the remote system and returns
    ``items``), this capability performs **no remote call**: it synthesizes a query from the
    matched detections of a local attack chain and returns a URL that opens the SIEM already
    running that query. The manager resolves the chain-derived inputs into a
    :class:`ChainSearchContext` and passes it here; provider-specific shaping (tables, host
    fields, base URL) comes from ``self.config``.
    """

    capability_id: ClassVar[str] = "view_in_siem"

    @abstractmethod
    async def build_view_in_siem_url(
        self,
        ctx: ChainSearchContext,
        *,
        params: dict[str, Any] | None = None,
    ) -> ViewInSiemResult:
        """Build a deep link into the SIEM search UI for one attack chain.

        Args:
            ctx: SIEM-agnostic chain inputs (matched Sigma rule ids, affected hostname, time window).
            params: Optional provider-specific overrides.

        Returns:
            :class:`ViewInSiemResult` with ``status``; on success ``url`` (and ``query``).
        """
        ...


_OPTIONAL_INTEGRATION_MIXINS = (
    DeployableIntegration,
    SearchableIntegration,
    PullableIntegration,
    ViewInSiemIntegration,
)


def get_provider_capabilities(provider_cls: type) -> list[str]:
    """Build the ordered capability string list for a concrete provider class.

    Used for catalog responses and saved-profile detail (``capabilities`` field).

    Returns:
        Always includes :attr:`BaseIntegrationProvider.capability_id` (``check_connection``),
        then each optional mixin’s :attr:`capability_id` when ``provider_cls`` subclasses it
        (``deploy_rules``, ``search``, ``view_in_siem``).
    """
    capabilities: list[str] = [BaseIntegrationProvider.capability_id]
    for mixin_cls in _OPTIONAL_INTEGRATION_MIXINS:
        if issubclass(provider_cls, mixin_cls):
            capabilities.append(mixin_cls.capability_id)
    return capabilities
