"""Integration management orchestrator.

:class:`IntegrationsManager` handles integration profile lifecycle: CRUD,
connection checks (saved and unsaved), and **capability** operations —
deploy/search (``DeployableIntegration`` / ``SearchableIntegration``).
Provider methods return typed Pydantic results
(:class:`~apps.integrations.base.ConnectionResult`, :class:`~apps.integrations.base.DeployResult`,
:class:`~apps.integrations.base.SearchResult`); routers map them to API response schemas.
Activity logging and config masking follow the same patterns as other managers.
"""

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from pydantic import ValidationError as PydanticValidationError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.exceptions import BadRequestError, DuplicateEntityError, NotFoundError, ValidationError
from apps.core.models import Integration, User
from apps.core.schemas import ConnectionCheckRequest, IntegrationCreateRequest, IntegrationUpdateRequest
from apps.integrations.base import (
    PROFILE_NAME_KEY,
    BaseIntegrationProvider,
    ChainSearchContext,
    ConnectionResult,
    DeployableIntegration,
    DeployResult,
    PullableIntegration,
    SearchableIntegration,
    SearchResult,
    ViewInSiemIntegration,
    ViewInSiemResult,
    get_provider_capabilities,
    sanitize_validation_error,
    with_profile_name_field,
    with_required_config_fields,
)
from apps.integrations.providers.gitlab import GitLabProvider
from apps.integrations.providers.socprime_tdm import SocPrimeTdmProvider
from apps.integrations.registry import IntegrationRegistry
from apps.managers.gitlab_sync import GitlabSyncManager
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.attack_chains import AttackChainsDAO
from apps.modules.postgre.integration import IntegrationDAO


class IntegrationsManager:
    """Orchestrates integration catalog, CRUD, ``check_connection``, and capability RPCs.

    Returns from capability endpoints mirror provider result types in
    ``apps/integrations/base.py`` (connection, deploy/search, TDM list/patch/mapping).
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = IntegrationDAO(db)

    # =========================================================================
    # Catalog (static, from registry)
    # =========================================================================

    async def get_available_types(self, category: str | None = None) -> list[dict]:
        """Get all available integration types from registry.

        Enriches each type with connected_count from DB.
        """
        if category:
            providers = IntegrationRegistry.get_by_category(category)
        else:
            providers = IntegrationRegistry.get_all()

        counts = await self.dao.count_by_type()

        result = []
        for type_key, provider_cls in providers.items():
            result.append(
                {
                    "integration_type": provider_cls.integration_type,
                    "display_name": provider_cls.display_name,
                    "category": provider_cls.category.value,
                    "description": provider_cls.description,
                    "icon": provider_cls.icon,
                    "capabilities": get_provider_capabilities(provider_cls),
                    "connected_count": counts.get(type_key, 0),
                    "singleton_profile": provider_cls.singleton_profile,
                }
            )
        return result

    async def get_type_detail(self, integration_type: str) -> dict:
        """Get type metadata + JSON Schema for config form.

        The returned ``config_schema`` is the provider's schema with a required form-only
        ``profile_name`` property prepended (``ui_order: 0``) by
        :func:`~apps.integrations.base.with_profile_name_field`; the manager maps that field to
        the ``integrations.name`` column (it is never stored in the config JSONB).

        Raises:
            NotFoundError: If integration_type not in registry.
        """
        provider_cls = self._get_provider_cls(integration_type)

        counts = await self.dao.count_by_type()
        config_schema = with_profile_name_field(provider_cls.get_config_schema().model_json_schema())
        config_schema = with_required_config_fields(config_schema, provider_cls.required_config_fields())

        return {
            "integration_type": provider_cls.integration_type,
            "display_name": provider_cls.display_name,
            "category": provider_cls.category.value,
            "description": provider_cls.description,
            "icon": provider_cls.icon,
            "capabilities": get_provider_capabilities(provider_cls),
            "connected_count": counts.get(integration_type, 0),
            "singleton_profile": provider_cls.singleton_profile,
            "config_schema": config_schema,
            "sensitive_fields": sorted(provider_cls.get_sensitive_fields()),
            "immutable_config_fields": list(provider_cls.immutable_config_fields()),
        }

    # =========================================================================
    # CRUD
    # =========================================================================

    async def get_all(
        self,
        pagination: dict,
        integration_type: str | None = None,
        category: str | None = None,
    ) -> tuple[list[Integration], int]:
        """Get paginated list of connected integration profiles."""
        filters: dict = {}
        if integration_type:
            filters["integration_type"] = integration_type
        if category:
            filters["category"] = category

        return await self.dao.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            search_fields=["name"],
            sort=pagination["sort"],
            order=pagination["order"],
            filters=filters if filters else None,
        )

    async def get_by_id(self, integration_id: UUID) -> Integration:
        """Get integration profile by ID.

        Raises:
            NotFoundError: If not found.
        """
        integration = await self.dao.get_by_id(integration_id)
        if not integration:
            raise NotFoundError(f"Integration with id {integration_id} not found")
        return integration

    async def create(
        self,
        request: IntegrationCreateRequest,
        user: User,
    ) -> Integration:
        """Create a new integration profile.

        Validates integration_type exists and config matches provider schema.
        Runs a saved-profile connection check immediately so ``status`` /
        ``status_message`` / ``last_checked_at`` reflect the outcome.
        """
        provider_cls = self._get_provider_cls(request.integration_type)
        config, profile_name = self._split_profile_name(request.config)
        name = (request.name or "").strip() or profile_name
        if not name:
            raise BadRequestError("Profile name is required")
        config = self._validate_config(provider_cls, config)

        if provider_cls.singleton_profile:
            existing_n = await self.dao.count_for_integration_type(request.integration_type)
            if existing_n > 0:
                raise DuplicateEntityError(
                    "Integration profile",
                    f"integration_type {provider_cls.integration_type}",
                )

        try:
            integration = await self.dao.create(
                name=name,
                integration_type=request.integration_type,
                category=provider_cls.category.value,
                config=config,
            )
        except IntegrityError:
            await self.db.rollback()
            raise DuplicateEntityError(
                "Integration profile",
                f"integration_type {request.integration_type}",
            ) from None

        if request.integration_type == GitLabProvider.integration_type:
            await GitlabSyncManager.provision_repository(self.db, integration)

        await activity_producer.log_action(
            action="create",
            entity_type="integration",
            entity_id=str(integration.id),
            entity_name=integration.name,
            user=user,
            details=f"Created integration profile '{integration.name}' ({provider_cls.display_name})",
        )

        await self.check_connection(integration.id, user)
        return await self.get_by_id(integration.id)

    async def update(
        self,
        integration_id: UUID,
        request: IntegrationUpdateRequest,
        user: User,
    ) -> Integration:
        """Update an existing integration profile.

        For config updates, merges with existing config (preserving secrets
        when the client sends '******' as value).
        """
        existing = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(existing.integration_type)

        update_data: dict = {}

        if request.config is not None:
            incoming, profile_name = self._split_profile_name(request.config)
            if profile_name:
                update_data["name"] = profile_name
            merged = provider_cls.merge_config_update(
                existing.config or {},
                incoming,
                provider_cls.get_sensitive_fields(),
            )
            changed_immutable = provider_cls.changed_immutable_config_fields(existing.config or {}, merged)
            if changed_immutable:
                fields = ", ".join(changed_immutable)
                raise BadRequestError(
                    f"Cannot change sync settings after creation: {fields}. "
                    "Delete and recreate the integration profile to connect a different repository."
                )
            update_data["config"] = self._validate_config(provider_cls, merged)
            update_data["status"] = "disconnected"

        # An explicit top-level name overrides a name derived from the config form.
        if request.name is not None:
            update_data["name"] = request.name

        updated = await self.dao.update(integration_id, **update_data)

        if existing.integration_type == GitLabProvider.integration_type:
            await GitlabSyncManager.reconcile_owned_repository(self.db, updated)

        await activity_producer.log_action(
            action="update",
            entity_type="integration",
            entity_id=str(updated.id),
            entity_name=updated.name,
            user=user,
            details=f"Updated integration profile '{updated.name}'",
        )

        if request.config is not None:
            await self.check_connection(updated.id, user)
            return await self.get_by_id(updated.id)
        return updated

    async def delete(
        self,
        integration_id: UUID,
        user: User,
    ) -> bool:
        """Delete an integration profile."""
        existing = await self.get_by_id(integration_id)
        integration_name = existing.name

        if existing.integration_type == GitLabProvider.integration_type:
            await GitlabSyncManager.delete_owned_repository(self.db, integration_id)

        deleted = await self.dao.delete(integration_id)
        if not deleted:
            raise NotFoundError(f"Failed to delete integration with id {integration_id}")

        await activity_producer.log_action(
            action="delete",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration_name,
            user=user,
            details=f"Deleted integration profile '{integration_name}'",
        )

        return True

    # =========================================================================
    # Connection check
    # =========================================================================

    async def check_connection(
        self,
        integration_id: UUID,
        user: User,
    ) -> ConnectionResult:
        """Check connection for an existing saved profile.

        Updates status in DB based on result.
        """
        integration = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(integration.integration_type)
        provider = provider_cls(integration.config)

        result = await provider.check_connection()

        new_status = "connected" if result.success else "error"
        await self.dao.update(
            integration_id,
            status=new_status,
            status_message=result.message,
            last_checked_at=datetime.now(UTC),
        )

        await activity_producer.log_action(
            action="update",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration.name,
            user=user,
            details=f"Connection check: {'success' if result.success else 'failed'} - {result.message}",
        )

        return result

    async def check_connection_unsaved(
        self,
        request: ConnectionCheckRequest,
        user: User,
    ) -> ConnectionResult:
        """Check connection without saving (pre-save validation).

        The form-only ``profile_name`` field is dropped before validation — the unsaved form
        config is submitted verbatim, but ``profile_name`` is integration metadata, not
        provider config.
        """
        provider_cls = self._get_provider_cls(request.integration_type)
        request.config, _ = self._split_profile_name(request.config)
        if (
            provider_cls.integration_type == SocPrimeTdmProvider.integration_type
            and request.config.get("api_key") == ""
        ):
            stored = await self.dao.get_first_by_integration_type(provider_cls.integration_type)
            stored_key = (stored.config or {}).get("api_key") if stored else None
            if not stored_key:
                return ConnectionResult(
                    success=False,
                    message="API key is empty and no saved Threat Detection Marketplace profile with a key was found. Save a profile with an API key first, or send the key in the request.",
                )
            request.config = {**request.config, "api_key": stored_key}
        request.config = self._validate_config(provider_cls, request.config)
        provider = provider_cls(request.config)
        result = await provider.check_connection()
        await activity_producer.log_action(
            action="update",
            entity_type="integration",
            entity_id=None,
            entity_name=request.integration_type,
            user=user,
            details=(f"Connection check (unsaved): {'success' if result.success else 'failed'} - {result.message}"),
        )
        return result

    async def pull_integration(self, integration_id: UUID, user: User | None) -> None:
        """Pull rules from a GitLab integration-owned repository."""
        integration = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(integration.integration_type)
        if not issubclass(provider_cls, PullableIntegration):
            raise BadRequestError(f"Integration type '{integration.integration_type}' does not support pull")

        await GitlabSyncManager.sync_integration(self.db, integration_id)

        await activity_producer.log_action(
            action="sync",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration.name,
            user=user,
            details=f"Pulled rules from {provider_cls.display_name}",
        )

    # =========================================================================
    # Deploy rules (providers with DeployableIntegration)
    # =========================================================================

    async def deploy_rules(
        self,
        integration_id: UUID,
        rules: list[str | dict[str, Any]],
        user: User | None,
        params: dict[str, Any] | None = None,
    ) -> DeployResult:
        """Push rules to the external system for a saved integration profile.

        Dispatches to :meth:`apps.integrations.base.DeployableIntegration.deploy`;
        return value is passed through to the HTTP response (``IntegrationDeployResponse``).

        Raises:
            NotFoundError: If integration or type is missing from registry.
            BadRequestError: If the integration type does not support deployment.
        """
        integration = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(integration.integration_type)
        if not issubclass(provider_cls, DeployableIntegration):
            raise BadRequestError(f"Integration type '{integration.integration_type}' does not support rule deployment")
        provider = provider_cls(integration.config or {})
        result = await provider.deploy(rules, params=params)

        await activity_producer.log_action(
            action="update",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration.name,
            user=user,
            details=f"Deploy rules: {len(rules)} payload(s) sent to {provider_cls.display_name}",
        )

        return result

    # =========================================================================
    # Search (providers with SearchableIntegration)
    # =========================================================================

    async def search_integration(
        self,
        integration_id: UUID,
        user: User | None,
        query: str | None = None,
        external_ids: list[str] | None = None,
        cursor: str | None = None,
        limit: int = 100,
        params: dict[str, Any] | None = None,
    ) -> SearchResult:
        """Search or list remote content using a saved integration profile.

        Dispatches to :meth:`apps.integrations.base.SearchableIntegration.search`;
        return value is passed through to the HTTP response (``IntegrationSearchResponse``).

        Raises:
            NotFoundError: If integration or type is missing from registry.
            BadRequestError: If the integration type does not support search.
        """
        integration = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(integration.integration_type)
        if not issubclass(provider_cls, SearchableIntegration):
            raise BadRequestError(f"Integration type '{integration.integration_type}' does not support search")
        provider = provider_cls(integration.config or {})
        result = await provider.search(
            query,
            external_ids=external_ids,
            cursor=cursor,
            limit=limit,
            params=params,
        )

        await activity_producer.log_action(
            action="update",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration.name,
            user=user,
            details=f"Search integration ({provider_cls.display_name}): query={query!r}, ids={len(external_ids or [])}",
        )

        return result

    # =========================================================================
    # View in SIEM
    # =========================================================================

    async def build_view_in_siem_url(
        self,
        integration_id: UUID,
        user: User | None,
        chain_id: UUID,
        sigma_rule_id: UUID | None = None,
        params: dict[str, Any] | None = None,
    ) -> ViewInSiemResult:
        """Build a deep link into the integration's SIEM search UI for an attack chain.

        Resolves the chain-derived inputs (affected hostname, time window, matched Sigma rule UUIDs)
        from the DB and dispatches to
        :meth:`apps.integrations.base.ViewInSiemIntegration.build_view_in_siem_url`. When
        ``sigma_rule_id`` is given, the link narrows to that single rule (per-rule "View in SIEM");
        otherwise it aggregates every rule matched in the chain. Either way the time window is the
        whole chain's ``[first_seen, last_seen]``. The provider synthesizes the URL from its config;
        no remote SIEM call is made.

        Raises:
            NotFoundError: If the integration, its type, or the chain is missing, or if
                ``sigma_rule_id`` did not match within the chain.
            BadRequestError: If the integration type does not support View in SIEM.
        """
        integration = await self.get_by_id(integration_id)
        provider_cls = self._get_provider_cls(integration.integration_type)
        if not issubclass(provider_cls, ViewInSiemIntegration):
            raise BadRequestError(f"Integration type '{integration.integration_type}' does not support view in SIEM")

        chains_dao = AttackChainsDAO(self.db)
        context = await chains_dao.get_chain_link_context(chain_id)
        if context is None:
            raise NotFoundError(f"Attack chain with id {chain_id} not found")
        hostname, first_seen, last_seen = context
        chain_rule_ids = await chains_dao.get_distinct_sigma_rule_ids(chain_id)

        if sigma_rule_id is not None:
            if str(sigma_rule_id) not in chain_rule_ids:
                raise NotFoundError(f"Sigma rule {sigma_rule_id} did not match in chain {chain_id}")
            rule_ids = [str(sigma_rule_id)]
        else:
            rule_ids = chain_rule_ids

        ctx = ChainSearchContext(
            sigma_rule_ids=rule_ids,
            hostname=hostname,
            start=first_seen,
            end=last_seen,
        )
        provider = provider_cls(integration.config or {})
        result = await provider.build_view_in_siem_url(ctx, params=params)

        scope = f"rule {sigma_rule_id}" if sigma_rule_id else "chain"
        await activity_producer.log_action(
            action="read",
            entity_type="integration",
            entity_id=str(integration_id),
            entity_name=integration.name,
            user=user,
            details=f"View in SIEM ({provider_cls.display_name}) for {scope} in chain {chain_id}",
        )

        return result

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _get_provider_cls(integration_type: str) -> type[BaseIntegrationProvider]:
        """Get provider class from registry or raise NotFoundError."""
        try:
            return IntegrationRegistry.get(integration_type)
        except KeyError:
            raise NotFoundError(f"Integration type '{integration_type}' not found")

    @staticmethod
    def _split_profile_name(config: dict | None) -> tuple[dict, str]:
        """Separate the form-only ``profile_name`` from a provider config dict.

        ``profile_name`` is integration metadata surfaced in the config form by
        :func:`~apps.integrations.base.with_profile_name_field`, not provider config — it maps to
        the ``integrations.name`` column and is never stored in the config JSONB (the detail
        endpoint re-injects it from the name column on read). Returns ``(config_without_name,
        stripped_profile_name)``; the input dict is not mutated.
        """
        config = dict(config or {})
        raw = config.pop(PROFILE_NAME_KEY, None)
        return config, raw.strip() if isinstance(raw, str) else ""

    @staticmethod
    def _validate_config(provider_cls: type[BaseIntegrationProvider], config: dict) -> dict:
        """Validate config against provider's schema and return the coerced dict.

        The returned dict — not the raw input — must be what gets persisted: Pydantic's lax
        validation accepts loosely-typed input (e.g. a JSON string ``"false"`` for a ``bool``
        field submitted by a form) and coerces it to the correct Python type, but only the
        validated model carries that coercion. Storing the raw input instead would let
        stringly-typed booleans (or other coercible-but-wrong-typed values) reach providers,
        which read config fields directly (e.g. ``bool(config.get("verify_ssl"))`` truthily
        matches any non-empty string, including ``"false"``).

        Raises:
            ValidationError: With a value-free message (``loc``/``msg`` only) — the submitted
                config values never echo back into the HTTP response.
        """
        try:
            schema_cls = provider_cls.get_config_schema()
            validated = schema_cls.model_validate(config)
        except PydanticValidationError as e:
            message, _ = sanitize_validation_error(e)
            raise ValidationError(message)
        return validated.model_dump(mode="json")
