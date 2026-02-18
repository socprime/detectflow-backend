"""Settings endpoints.

This module provides REST API endpoints for application settings:
- Get settings (API key status)
- Set SOCPrime API key
- Get/Set Flink defaults
- Get Flink parameters schema (for UI)
"""

from fastapi import APIRouter, Body, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.tdm_api import TDMAPIClient, TdmApiError, TdmApiUnauthorizedError
from apps.core.auth import get_current_active_user, get_current_admin_user
from apps.core.database import get_db
from apps.core.exceptions import ExternalServiceError
from apps.core.models import User
from apps.core.schemas import (
    ErrorResponse,
    FlinkDefaultsResponse,
    FlinkDefaultsSchemaResponse,
    FlinkDefaultsUpdateRequest,
    RepositoryApiKeyUpdateResponse,
    SettingsResponse,
)
from apps.managers.flink_config import FlinkConfigManager
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.config import ConfigDAO

router = APIRouter(prefix="/api/v1", tags=["Settings"])


@router.get(
    "/settings",
    response_model=SettingsResponse,
    summary="Get application settings",
    responses={
        200: {"description": "Settings retrieved successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/settings/", include_in_schema=False)
async def get_settings(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get application settings.

    Returns current settings including whether SOCPrime API key is configured
    and masked version of the API key (first 3 and last 3 characters).
    """
    config_repo = ConfigDAO(db)
    api_key = await config_repo.get_api_key()
    api_key_configured = api_key is not None

    api_key_mask = None
    if api_key:
        if len(api_key) <= 10:
            api_key_mask = "***"
        else:
            api_key_mask = f"{api_key[:3]}***{api_key[-3:]}"

    return SettingsResponse(
        api_key_configured=api_key_configured,
        api_key_mask=api_key_mask,
    )


@router.put(
    "/socprime-api-key",
    response_model=RepositoryApiKeyUpdateResponse,
    summary="Set SOCPrime API key",
    responses={
        200: {"description": "API key updated successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Invalid API key"},
    },
)
@router.put("/socprime-api-key/", include_in_schema=False)
async def update_socprime_api_key(
    api_key: str = Body(..., description="SOCPrime API key", embed=True, min_length=1, max_length=255),
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create or update the SOCPrime TDM API key.

    The API key is validated by fetching repositories from TDM API.
    """
    config_repo = ConfigDAO(db)
    tdm_api_client = TDMAPIClient(api_key=api_key)

    # Validate API key by fetching repositories
    try:
        await tdm_api_client.get_repositories()
    except TdmApiUnauthorizedError:
        raise  # Global handler returns 401
    except TdmApiError as e:
        raise ExternalServiceError("SOCPrime API", f"Failed to validate API key: {e}") from e

    await config_repo.set_api_key(api_key)

    await activity_producer.log_action(
        action="update",
        entity_type="settings",
        entity_id="socprime_api_key",
        entity_name="SOCPrime API key",
        user=current_user,
        details=f"User {current_user.email} updated SOCPrime API key",
        source="user",
    )

    return RepositoryApiKeyUpdateResponse(message="API key updated successfully")


# =============================================================================
# Flink Defaults Endpoints
# =============================================================================


@router.get(
    "/settings/flink-defaults",
    response_model=FlinkDefaultsResponse,
    summary="Get Flink default configuration",
    responses={
        200: {"description": "Flink defaults retrieved successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/settings/flink-defaults/", include_in_schema=False)
async def get_flink_defaults(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get global Flink default configuration for new pipelines.

    These defaults are used when creating pipelines without explicit resource configuration.
    Individual pipelines can override these values.
    """
    config_dao = ConfigDAO(db)
    return await config_dao.get_flink_defaults()


@router.put(
    "/settings/flink-defaults",
    response_model=FlinkDefaultsResponse,
    summary="Update Flink default configuration",
    responses={
        200: {"description": "Flink defaults updated successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        403: {"model": ErrorResponse, "description": "Admin access required"},
        422: {"description": "Validation error"},
    },
)
@router.put("/settings/flink-defaults/", include_in_schema=False)
async def update_flink_defaults(
    update: FlinkDefaultsUpdateRequest,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update global Flink default configuration.

    **Admin only.** This is a partial update - only provided (non-null) fields are updated.
    Existing pipelines are not affected; only new pipelines use these defaults.
    """
    config_dao = ConfigDAO(db)
    updated = await config_dao.set_flink_defaults(update)

    # Log activity
    await activity_producer.log_action(
        action="update",
        entity_type="settings",
        entity_id="flink_defaults",
        entity_name="Flink defaults",
        user=current_user,
        details=f"User {current_user.email} updated Flink default configuration",
        changes=update.model_dump(exclude_none=True),
        source="user",
    )

    return updated


@router.get(
    "/settings/flink-defaults/schema",
    response_model=FlinkDefaultsSchemaResponse,
    summary="Get Flink configuration schema",
    responses={
        200: {"description": "Schema retrieved successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/settings/flink-defaults/schema/", include_in_schema=False)
async def get_flink_defaults_schema(
    _: User = Depends(get_current_active_user),
):
    """
    Get Flink configuration parameter schema for UI.

    Returns detailed documentation for each configurable parameter including:
    - Data types and valid ranges
    - Descriptions and usage tips
    - Categories for UI grouping
    - Impact information (restart required vs hot-reload)
    """
    return FlinkConfigManager.get_schema()
