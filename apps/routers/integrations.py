"""Integration management endpoints.

REST API under ``/api/v1/integrations``: catalog (``/types``), CRUD for saved
profiles, **check-connection** (with and without persistence), and capability
routes (**pull**, **deploy**, **search**) when the provider type advertises them in ``capabilities``.
"""

import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.converters import IntegrationConverter
from apps.core.database import get_db
from apps.core.exceptions import BadRequestError, ConflictError
from apps.core.models import User
from apps.core.openapi import BAD_REQUEST, UNAUTHORIZED, err, errors
from apps.core.pagination import get_pagination_params
from apps.core.schemas import (
    ConnectionCheckRequest,
    ConnectionCheckResponse,
    IntegrationCreateRequest,
    IntegrationDeployRequest,
    IntegrationDeployResponse,
    IntegrationDetailResponse,
    IntegrationListResponse,
    IntegrationResponse,
    IntegrationSearchRequest,
    IntegrationSearchResponse,
    IntegrationTypeDetailResponse,
    IntegrationTypeListResponse,
    IntegrationUpdateRequest,
    IntegrationViewInSiemRequest,
    IntegrationViewInSiemResponse,
    SyncStartedResponse,
)
from apps.integrations.base import PROFILE_NAME_KEY, PullableIntegration
from apps.integrations.registry import IntegrationRegistry
from apps.managers.integrations import IntegrationsManager
from apps.modules.kafka.activity import activity_producer
from apps.services.scheduler import get_scheduler_service

router = APIRouter(prefix="/api/v1/integrations", tags=["Integrations"])


# =============================================================================
# Catalog (Available tab)
# =============================================================================


@router.get(
    "/types",
    response_model=IntegrationTypeListResponse,
    summary="List available integration types",
    responses=UNAUTHORIZED,
)
async def list_integration_types(
    category: str | None = Query(default=None, description="Filter by category (e.g. 'siem', 'git_hosting')."),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """List the integration types available in the catalog grid.

    Returns each type's metadata, the **capabilities** it advertises (which gate the deploy/search/
    view-in-siem routes), and the count of connected profiles. Optionally filtered by category.
    Read-only.
    """
    manager = IntegrationsManager(db)
    types = await manager.get_available_types(category)
    return IntegrationTypeListResponse(data=types)


@router.get(
    "/types/{integration_type}",
    response_model=IntegrationTypeDetailResponse,
    summary="Get integration type detail",
    responses=errors(UNAUTHORIZED, err(404, "Integration type not found")),
)
async def get_integration_type(
    integration_type: str = Path(..., description="Integration type key (e.g. 'splunk')."),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get metadata and the config JSON Schema for a specific integration type.

    ``config_schema`` is a JSON Schema (with a synthetic required ``name`` field for the profile
    label) the frontend uses to render the configuration form dynamically; ``capabilities`` lists
    what this type supports. Read-only.
    """
    manager = IntegrationsManager(db)
    return await manager.get_type_detail(integration_type)


# =============================================================================
# Connection check (unsaved — must be before /{integration_id} routes)
# =============================================================================


@router.post(
    "/check-connection",
    response_model=ConnectionCheckResponse,
    summary="Check connection (unsaved)",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Config failed provider schema validation"),
        err(404, "Integration type not found"),
    ),
)
async def check_connection_unsaved(
    request: ConnectionCheckRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Test a connection before saving an integration profile.

    Validates the submitted config against the provider schema, then attempts to connect — nothing
    is persisted. An unreachable target is reported as ``200`` with ``success=false`` (not an HTTP
    error); a malformed config is ``400``. Logs an ``integration`` connection-check activity event.
    """
    manager = IntegrationsManager(db)
    result = await manager.check_connection_unsaved(request, current_user)
    return ConnectionCheckResponse(
        success=result.success,
        message=result.message,
        details=result.details,
    )


# =============================================================================
# CRUD (Connected tab)
# =============================================================================


@router.get(
    "",
    response_model=IntegrationListResponse,
    summary="List connected integrations",
    responses=UNAUTHORIZED,
)
async def list_integrations(
    integration_type: str | None = Query(default=None, description="Filter by integration type key."),
    category: str | None = Query(default=None, description="Filter by category (e.g. 'siem')."),
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params),
    db: AsyncSession = Depends(get_db),
):
    """List the connected (saved) integration profiles, paginated.

    Supports filtering by ``integration_type`` and ``category`` and name search via the shared
    pagination params. Profiles whose provider type is no longer registered are silently skipped.
    Read-only.
    """
    manager = IntegrationsManager(db)
    integrations, total = await manager.get_all(pagination, integration_type, category)

    data = []
    for integration in integrations:
        try:
            provider_cls = IntegrationRegistry.get(integration.integration_type)
            data.append(IntegrationConverter.to_list_item(integration, provider_cls))
        except KeyError:
            continue  # Skip integrations with unknown type (provider removed)

    return IntegrationListResponse(
        total=total,
        page=pagination["page"],
        limit=pagination["limit"],
        sort=pagination["sort"] or "",
        offset=pagination["offset"],
        order=pagination["order"],
        data=data,
    )


@router.get(
    "/{integration_id}",
    response_model=IntegrationDetailResponse,
    summary="Get integration profile",
    responses=errors(UNAUTHORIZED, err(404, "Integration not found")),
)
async def get_integration(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    _: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Get a single integration profile, including current connection status.

    Sensitive config fields (passwords, tokens, API keys — per the provider's declared sensitive
    fields) are masked as ``******`` in the response and never returned in clear text. Read-only.
    """
    manager = IntegrationsManager(db)
    integration = await manager.get_by_id(integration_id)

    provider_cls = IntegrationRegistry.get(integration.integration_type)
    masked_config = provider_cls.mask_sensitive_config(
        integration.config or {},
        provider_cls.get_sensitive_fields(),
    )
    # Re-inject the form-only profile_name (stored in the name column, not the config JSONB) so
    # the edit form pre-fills it like any other field.
    masked_config = {PROFILE_NAME_KEY: integration.name, **masked_config}
    return IntegrationConverter.to_detail(integration, provider_cls, masked_config)


@router.post(
    "",
    response_model=IntegrationResponse,
    status_code=201,
    summary="Create integration profile",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Config failed provider schema validation"),
        err(404, "Integration type not found"),
        err(409, "A profile of this singleton type already exists"),
    ),
)
async def create_integration(
    request: IntegrationCreateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Create a new integration profile.

    The config is validated against the provider schema (``400`` on failure). For singleton types a
    second profile is rejected with ``409``. After persisting, a connection check runs automatically,
    so the profile's ``status`` / ``last_checked_at`` reflect the outcome — fetch the profile to read
    them. Logs ``create`` (and a follow-up connection-check) ``integration`` activity events.
    """
    manager = IntegrationsManager(db)
    integration = await manager.create(request, current_user)
    return IntegrationResponse(
        id=str(integration.id),
        status=True,
        message="Integration profile created successfully",
    )


@router.patch(
    "/{integration_id}",
    response_model=IntegrationResponse,
    summary="Update integration profile",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Merged config failed provider schema validation"),
        err(404, "Integration not found"),
    ),
)
async def update_integration(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    request: IntegrationUpdateRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Update an existing integration profile.

    Config updates merge over the stored config; for a sensitive field, send ``******`` to keep the
    existing secret unchanged. A config change resets ``status`` to ``disconnected`` and triggers a
    fresh connection check. The merged config is re-validated against the provider schema (``400`` on
    failure). Logs an ``update`` ``integration`` activity event.
    """
    manager = IntegrationsManager(db)
    updated = await manager.update(integration_id, request, current_user)
    return IntegrationResponse(
        id=str(updated.id),
        status=True,
        message="Integration profile updated successfully",
    )


@router.delete(
    "/{integration_id}",
    status_code=204,
    summary="Delete integration profile",
    responses=errors(UNAUTHORIZED, err(404, "Integration not found")),
)
async def delete_integration(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> None:
    """Permanently delete an integration profile.

    Returns ``204 No Content`` on success. Logs a ``delete`` ``integration`` activity event.
    """
    manager = IntegrationsManager(db)
    await manager.delete(integration_id, current_user)


# =============================================================================
# Connection check (saved)
# =============================================================================


@router.post(
    "/{integration_id}/check-connection",
    response_model=ConnectionCheckResponse,
    summary="Check connection (saved)",
    responses=errors(UNAUTHORIZED, err(404, "Integration not found")),
)
async def check_connection(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Re-test the connection for a saved integration profile.

    Persists the outcome onto the profile (``status``, ``status_message``, ``last_checked_at``). An
    unreachable target is reported as ``200`` with ``success=false`` (not an HTTP error). Logs an
    ``integration`` connection-check activity event.
    """
    manager = IntegrationsManager(db)
    result = await manager.check_connection(integration_id, current_user)
    return ConnectionCheckResponse(
        success=result.success,
        message=result.message,
        details=result.details,
    )


# =============================================================================
# Pull rules (saved profile — GitLab)
# =============================================================================


@router.post(
    "/{integration_id}/pull",
    response_model=SyncStartedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Pull rules from GitLab",
    responses={
        202: {"description": "GitLab pull started in background"},
        **errors(
            UNAUTHORIZED,
            BAD_REQUEST,
            err(404, "Integration not found"),
            err(409, "A sync is already running"),
        ),
    },
)
@router.post("/{integration_id}/pull/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def pull_integration_rules(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Pull Sigma rules from GitLab into the integration-owned repository.

    Capability-gated: only profiles whose type advertises ``pull_rules`` are accepted.
    Returns 202 immediately; work runs in the background regardless of ``auto_pull``.
    Poll ``GET /sync-repositories/status`` (key ``gitlab_repos``) only when the pull was
    started via ``POST /sync-repositories``; per-integration pulls do not update that status map.
    """
    manager = IntegrationsManager(db)
    integration = await manager.get_by_id(integration_id)
    provider_cls = IntegrationRegistry.get(integration.integration_type)
    if not issubclass(provider_cls, PullableIntegration):
        raise BadRequestError(f"Integration type '{integration.integration_type}' does not support pull")

    scheduler = get_scheduler_service()
    if scheduler.is_sync_running:
        raise ConflictError("A sync is already running")

    await activity_producer.log_action(
        action="sync_start",
        entity_type="integration",
        entity_id=str(integration_id),
        entity_name=integration.name,
        user=current_user,
        details="GitLab pull triggered",
    )

    asyncio.create_task(scheduler.run_gitlab_pull_in_background(integration_id, current_user))
    return SyncStartedResponse(message="GitLab pull started in background")


# =============================================================================
# Deploy rules (saved profile)
# =============================================================================


@router.post(
    "/{integration_id}/deploy",
    response_model=IntegrationDeployResponse,
    summary="Deploy rules to integration",
    responses=errors(
        UNAUTHORIZED,
        BAD_REQUEST,
        err(404, "Integration not found"),
    ),
)
async def deploy_rules(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    request: IntegrationDeployRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Push rules to the external system using a saved integration profile.

    Availability is capability-gated: only profiles whose type advertises the deploy capability are
    accepted — others return ``400``. Check ``GET /api/v1/integrations/types`` for each type's
    ``capabilities``. The provider returns a :class:`~apps.integrations.base.DeployResult`, validated
    here into ``IntegrationDeployResponse`` (provider-specific extra fields preserved). Logs an
    ``integration`` deploy activity event.
    """
    manager = IntegrationsManager(db)
    result = await manager.deploy_rules(
        integration_id,
        request.rules,
        current_user,
        params=request.params,
    )
    return IntegrationDeployResponse.model_validate(result.model_dump())


# =============================================================================
# Search (saved profile)
# =============================================================================


@router.post(
    "/{integration_id}/search",
    response_model=IntegrationSearchResponse,
    summary="Search or list via integration",
    responses=errors(
        UNAUTHORIZED,
        BAD_REQUEST,
        err(404, "Integration not found"),
    ),
)
async def search_integration(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    request: IntegrationSearchRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Query or list content in the external system using a saved integration profile.

    Availability is capability-gated: only profiles whose type advertises the search capability are
    accepted — others return ``400``. Check ``GET /api/v1/integrations/types`` for each type's
    ``capabilities``. The provider returns a :class:`~apps.integrations.base.SearchResult`, validated
    here as ``IntegrationSearchResponse``. Logs an ``integration`` search activity event.
    """
    manager = IntegrationsManager(db)
    result = await manager.search_integration(
        integration_id,
        current_user,
        query=request.query,
        external_ids=request.external_ids,
        cursor=request.cursor,
        limit=request.limit,
        params=request.params,
    )
    return IntegrationSearchResponse.model_validate(result.model_dump())


# =============================================================================
# View in SIEM (saved profile)
# =============================================================================


@router.post(
    "/{integration_id}/view-in-siem",
    response_model=IntegrationViewInSiemResponse,
    summary="Build a SIEM search deep link for an attack chain",
    responses=errors(
        UNAUTHORIZED,
        BAD_REQUEST,
        err(404, "Integration, attack chain, or sigma rule (within the chain) not found"),
    ),
)
async def view_in_siem(
    integration_id: UUID = Path(..., description="Integration profile UUID."),
    request: IntegrationViewInSiemRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Return a ready-to-open deep link that runs the attack chain's search in the SIEM UI.

    Availability is capability-gated: only profiles whose type advertises the view-in-siem
    capability are accepted — others return ``400``. Check ``GET /api/v1/integrations/types`` for
    each type's ``capabilities``. The manager resolves the chain's affected hostname, ``[first_seen,
    last_seen]`` window, and matched Sigma rule ids (narrowed to a single rule when ``sigma_rule_id``
    is given), then asks the provider to synthesize the URL — no remote SIEM call is made. Returns a
    :class:`~apps.integrations.base.ViewInSiemResult`, validated as ``IntegrationViewInSiemResponse``.
    Logs a ``read`` ``integration`` activity event.
    """
    manager = IntegrationsManager(db)
    result = await manager.build_view_in_siem_url(
        integration_id,
        current_user,
        chain_id=request.chain_id,
        sigma_rule_id=request.sigma_rule_id,
        params=request.params,
    )
    return IntegrationViewInSiemResponse.model_validate(result.model_dump())
