"""Settings endpoints.

This module provides REST API endpoints for application settings:
- Get/Set Flink defaults
- Get Flink parameters schema (for UI)

SOCPrime API key management has been migrated to the Integrations module
(POST /api/v1/integrations with integration_type="socprime_tdm").
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, get_current_admin_user
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import FORBIDDEN, UNAUTHORIZED, errors
from apps.core.schemas import (
    FlinkDefaultsResponse,
    FlinkDefaultsSchemaResponse,
    FlinkDefaultsUpdateRequest,
)
from apps.managers.flink_config import FlinkConfigManager
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.config import ConfigDAO

router = APIRouter(prefix="/api/v1", tags=["Settings"])


# =============================================================================
# Flink Defaults Endpoints
# =============================================================================


@router.get(
    "/settings/flink-defaults",
    response_model=FlinkDefaultsResponse,
    summary="Get Flink default configuration",
    responses=errors(UNAUTHORIZED),
)
@router.get("/settings/flink-defaults/", include_in_schema=False)
async def get_flink_defaults(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get the global Flink defaults applied to **newly created** pipelines.

    These resource/processing/autoscaler values seed a pipeline only at creation time when no
    explicit config is supplied; an existing pipeline's own values always take precedence and are
    not affected by reading this. Read-only.
    """
    config_dao = ConfigDAO(db)
    return await config_dao.get_flink_defaults()


@router.put(
    "/settings/flink-defaults",
    response_model=FlinkDefaultsResponse,
    summary="Update Flink default configuration",
    responses=errors(UNAUTHORIZED, FORBIDDEN),
)
@router.put("/settings/flink-defaults/", include_in_schema=False)
async def update_flink_defaults(
    update: FlinkDefaultsUpdateRequest,
    current_user: User = Depends(get_current_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update the global Flink defaults used when creating new pipelines.

    **Admin only** (403 otherwise). Partial update — only provided (non-null) fields change. This
    only changes the seed values for **future** pipelines; it never restarts or reconfigures
    existing pipelines and triggers no Flink redeploy itself. (Whether a given parameter, once set
    on a real pipeline, needs a Flink restart or applies via hot-reload is described by each
    parameter's `impact` in the schema endpoint below.) Writes an `update` entry to the activity log.
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
    responses=errors(UNAUTHORIZED),
)
@router.get("/settings/flink-defaults/schema/", include_in_schema=False)
async def get_flink_defaults_schema(
    _: User = Depends(get_current_active_user),
):
    """
    Get the editable Flink parameter schema for the settings UI.

    Returns, per configurable parameter, its data type and valid range, description and tips, UI
    category, and an `impact` flag indicating whether changing that parameter on a live pipeline
    forces a Flink restart (`restart`) or applies without downtime (`hot_reload`). Static and
    read-only — describes the parameters, not any pipeline's current values.
    """
    return FlinkConfigManager.get_schema()
