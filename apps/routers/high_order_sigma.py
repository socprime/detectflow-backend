"""High-order Sigma catalog API (list, detail, edit)."""

from uuid import UUID

from fastapi import APIRouter, Depends, Path
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.converters import HighOrderSigmaConverter
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import NOT_FOUND, UNAUTHORIZED, errors
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    HighOrderSigmaDetailResponse,
    HighOrderSigmaListResponse,
    HighOrderSigmaUpdateRequest,
)
from apps.managers.high_order_sigma import HighOrderSigmasManager

router = APIRouter(prefix="/api/v1/high_order_sigmas", tags=["High-order Sigmas"])


@router.get(
    "",
    response_model=HighOrderSigmaListResponse,
    summary="List high-order Sigmas",
    responses=errors(UNAUTHORIZED, {200: {"description": "Paginated list"}}),
)
@router.get("/", include_in_schema=False)
async def list_high_order_sigmas(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    db: AsyncSession = Depends(get_db),
):
    """Return the paginated high-order Sigma catalog (name/tdm_id search, sortable).

    Read-only browse of the locally stored HoS rows synced from TDM; each list item carries
    metadata only (full body is on the detail endpoint).
    """
    manager = HighOrderSigmasManager(db)
    rows, total = await manager.get_all(pagination)
    data = [HighOrderSigmaConverter.to_list_item(h) for h in rows]
    return HighOrderSigmaListResponse(
        total=total,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"] or "",
        order=pagination["order"],
        data=data,
    )


@router.get(
    "/{high_order_sigma_id}",
    response_model=HighOrderSigmaDetailResponse,
    summary="Get high-order Sigma by id",
    responses=errors(
        UNAUTHORIZED,
        NOT_FOUND,
        {200: {"description": "Full high-order Sigma row"}},
    ),
)
async def get_high_order_sigma(
    high_order_sigma_id: UUID = Path(..., description="High-order Sigma UUID (`high_order_sigmas.id`)"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return one high-order Sigma row (metadata and body)."""
    manager = HighOrderSigmasManager(db)
    row = await manager.get_by_id(high_order_sigma_id)
    return HighOrderSigmaConverter.to_detail(row)


@router.patch(
    "/{high_order_sigma_id}",
    response_model=HighOrderSigmaDetailResponse,
    summary="Edit high-order Sigma",
    responses=errors(
        UNAUTHORIZED,
        NOT_FOUND,
        {
            200: {"description": "High-order Sigma updated"},
            422: {"description": "Validation error"},
        },
    ),
)
@router.patch("/{high_order_sigma_id}/", include_in_schema=False)
async def update_high_order_sigma(
    body: HighOrderSigmaUpdateRequest,
    high_order_sigma_id: UUID = Path(..., description="High-order Sigma UUID (`high_order_sigmas.id`)"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Update high-order Sigma name/body, pin it as user-edited, and resync it to the correlation runtime.

    Stores the provided user-managed fields and sets ``is_user_edited=true`` so the Active Threat
    sync skips the row and the edit survives subsequent TDM updates. When the body changes, the
    referenced Sigma UUIDs are re-extracted and persisted. The row is then reconciled with the
    hos-rules Kafka topic by its current Active Threat visibility: republished (and un-tombstoned)
    when it still has at least one non-hidden Active Threat membership, otherwise tombstoned on
    Kafka while the user-edited row stays in the DB and is republished automatically once an Active
    Threat becomes visible again. The edit is logged as a user activity event.
    """
    manager = HighOrderSigmasManager(db)
    row = await manager.update(high_order_sigma_id, body.name, body.body, current_user)
    return HighOrderSigmaConverter.to_detail(row)
