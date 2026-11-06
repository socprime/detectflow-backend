"""Attack chains API — chain list/detail/status endpoints."""

import asyncio
import json
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, get_current_active_user_from_cookie, require_write_access
from apps.core.database import get_db
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.pagination import get_attack_chains_list_params
from apps.core.schemas import (
    AttackChainDetailResponse,
    AttackChainGroupBy,
    AttackChainListResponse,
    AttackChainPatchRequest,
    AttackChainsFiltersResponse,
    AttackChainsListSSEMessage,
)
from apps.core.settings import settings
from apps.managers.attack_chains import AttackChainsManager
from apps.modules.kafka.activity import activity_producer
from apps.services.attack_chains_list_invalidation import attack_chains_list_invalidation_service

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/attack_chains", tags=["Attack chains"])


def _attack_chains_list_sse_payload(
    message_type: Literal[
        "connection_established",
        "attack_chains_changed",
        "heartbeat",
        "error",
    ],
    *,
    revision: int | None = None,
    error: str | None = None,
) -> dict:
    return AttackChainsListSSEMessage(
        type=message_type,
        timestamp=datetime.now(UTC),
        revision=revision,
        error=error,
    ).model_dump(mode="json", exclude_none=True)


async def _attack_chains_list_sse_generator(request: Request):
    """SSE: invalidation signals when the attack-chains catalog changes."""
    invalidation = attack_chains_list_invalidation_service
    heartbeat_seconds = settings.attack_chains_list_sse_heartbeat_seconds

    queue = await invalidation.subscribe()
    try:
        try:
            payload = _attack_chains_list_sse_payload(
                "connection_established",
                revision=invalidation.revision,
            )
            yield f"data: {json.dumps(payload)}\n\n"
        except Exception as e:
            logger.error("Failed to send initial attack-chains list SSE", extra={"error": str(e)})
            if ErrorTracker.should_log("attack_chains_list_sse_initial"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="attack_chains",
                    details=f"Failed to send initial attack-chains list SSE: {str(e)}",
                    source="system",
                    severity="error",
                )
        while True:
            if await request.is_disconnected():
                logger.debug("Attack-chains list SSE client disconnected")
                break

            try:
                revision = await asyncio.wait_for(queue.get(), timeout=heartbeat_seconds)
                while not queue.empty():
                    revision = queue.get_nowait()

                payload = _attack_chains_list_sse_payload(
                    "attack_chains_changed",
                    revision=revision,
                )
                yield f"data: {json.dumps(payload)}\n\n"

            except TimeoutError:
                if await request.is_disconnected():
                    break
                payload = _attack_chains_list_sse_payload(
                    "heartbeat",
                    revision=invalidation.revision,
                )
                yield f"data: {json.dumps(payload)}\n\n"

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error generating attack-chains list SSE", extra={"error": str(e)})
                if ErrorTracker.should_log("attack_chains_list_sse_event"):
                    await activity_producer.log_action(
                        action="error",
                        entity_type="attack_chains",
                        details=f"Error generating attack-chains list SSE: {str(e)}",
                        source="system",
                        severity="error",
                    )
                error_event = _attack_chains_list_sse_payload("error", error=str(e))
                yield f"data: {json.dumps(error_event)}\n\n"

    finally:
        await invalidation.unsubscribe(queue)


@router.get(
    "/chains",
    response_model=AttackChainListResponse,
    response_model_exclude_none=True,
    summary="List attack chains",
    responses=errors(UNAUTHORIZED, {200: {"description": "Paginated chains (short rows for tables/filters)"}}),
)
@router.get(
    "/chains/",
    response_model=AttackChainListResponse,
    response_model_exclude_none=True,
    include_in_schema=False,
)
async def list_attack_chains(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_attack_chains_list_params),
    group_by: AttackChainGroupBy = Query(
        default="none",
        description="Grouping mode: none (all chains), hostname, or chain_name",
    ),
    severity: list[str] | None = Query(
        default=None,
        description="Filter by linked active threat severity (case-insensitive). Repeatable: ?severity=high&severity=medium",
    ),
    status: list[str] | None = Query(
        default=None,
        description="Filter by workflow status (case-insensitive). Repeatable: ?status=new&status=in_progress",
    ),
    show_filtered_out: bool = Query(
        default=False,
        description="Include chains with status filtered-out in the returned list",
    ),
    correlation_pipeline_id: UUID | None = Query(
        default=None,
        description=(
            "Scope to one correlation pipeline; omit for platform-wide. Chains whose pipeline was "
            "deleted have a null value and are returned only when this filter is omitted."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """Return a paginated, optionally grouped view of correlation-produced attack chains.

    Read-only. Supports ``search``, ``sort``/``order`` and the repeatable ``severity`` (linked
    active threat) / ``status`` filters. ``search`` also matches hit technique IDs on
    ``attack_chain_events.technique_ids`` (substring, so ``T1053`` also matches ``T1053.005``). Sort options include date fields ``last_seen`` (default),
    ``first_seen``, ``created_at``, ``updated_at``, ``window_start``, plus ``severity``, ``hostname``,
    ``status``, and count/coverage fields. ``group_by`` buckets rows by hostname or chain name.
    Chains in the ``filtered_out`` workflow state are excluded unless ``show_filtered_out=true``;
    ``filtered_out_count`` reports how many were hidden so the UI can offer to reveal them — scoped
    the same way as the list, so it never counts another pipeline's hidden chains.
    ``list_revision`` is the catalog revision captured before the query (aligned with the SSE
    stream); refetch when the stream ``revision`` advances past it.

    Each row carries its producing correlation pipeline both as scalars
    (``correlation_pipeline_id`` / ``correlation_pipeline_name``) and as a **one-element**
    ``correlation_pipelines`` array; the array is empty for a legacy chain or one whose pipeline was
    deleted.
    """
    list_revision = attack_chains_list_invalidation_service.revision
    manager = AttackChainsManager(db)
    groups, total, filtered_out_count = await manager.list_chains(
        pagination,
        group_by,
        severity=severity,
        status=status,
        show_filtered_out=show_filtered_out,
        correlation_pipeline_id=correlation_pipeline_id,
    )
    return AttackChainListResponse(
        list_revision=list_revision,
        total=total,
        filtered_out_count=filtered_out_count,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"] or "",
        order=pagination["order"] or "asc",
        group_by=group_by,
        groups=groups,
    )


@router.get(
    "/chains/stream",
    summary="Stream attack-chains list invalidation (SSE)",
    responses={
        200: {
            "description": (
                "Server-Sent Events: lightweight invalidation signals for the chains table. "
                "Refetch GET /chains after attack_chains_changed."
            ),
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                    "example": (
                        'data: {"type": "attack_chains_changed", "revision": 42, '
                        '"timestamp": "2024-01-01T00:00:00Z"}\n\n'
                    ),
                }
            },
        },
        **UNAUTHORIZED,
    },
)
@router.get("/chains/stream/", include_in_schema=False)
async def stream_attack_chains_list(
    request: Request,
    _: User = Depends(get_current_active_user_from_cookie),
) -> StreamingResponse:
    """Stream invalidation events for the attack-chains list as Server-Sent Events.

    Emits ``connection_established`` immediately, then ``attack_chains_changed`` when the
    catalog revision bumps (Kafka ``chains-analysed`` upsert or user status ``PATCH``), and
    periodic ``heartbeat`` events (``attack_chains_list_sse_heartbeat_seconds``) while idle.
    The stream never includes list rows — clients refetch ``GET /api/v1/attack_chains/chains``
    with their current filters after ``attack_chains_changed``.

    **Authentication:** the `CookieAuth` scheme — the HttpOnly `access_token` cookie set by login,
    not the `Authorization` header (`EventSource` cannot send custom headers).
    """
    logger.info("Attack-chains list SSE client connected")

    return StreamingResponse(
        _attack_chains_list_sse_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/filters",
    response_model=AttackChainsFiltersResponse,
    summary="Get available attack chain filter options",
    responses=errors(UNAUTHORIZED, {200: {"description": "Available filter options"}}),
)
async def get_filter_options(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the distinct values the attack-chains and monitored-threats filter UI can offer.

    Read-only. Aggregates the grouping modes plus the distinct chain severities/statuses and
    active-threat severity/industry/threat-type/region values present in the data, so the
    front end can populate filter dropdowns without enumerating them client-side.
    """
    manager = AttackChainsManager(db)
    return await manager.get_filter_options()


@router.get(
    "/{chain_id}",
    response_model=AttackChainDetailResponse,
    summary="Get attack chain by id",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Chain not found"),
        {
            200: {
                "description": (
                    "Chain with nested HoS (`hos_rule`), threat (`active_threat`), techniques, and chain graph"
                )
            }
        },
    ),
)
async def get_attack_chain(
    chain_id: UUID = Path(..., description="Attack chain id (correlation-job stable id)"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return one attack chain expanded for the detail screen.

    Read-only. Resolves the nested high-order Sigma, linked active threat, per-technique
    breakdown and the MITRE ATT&CK chain graph (hit and unhit techniques merged from the
    chain events and the HoS tags).
    """
    manager = AttackChainsManager(db)
    return await manager.get_chain_detail(chain_id)


@router.patch(
    "/{chain_id}",
    response_model=AttackChainDetailResponse,
    summary="Update attack chain",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Chain not found"),
        {
            200: {"description": "Updated chain (currently status only)"},
            422: {"description": "Validation error"},
        },
    ),
)
async def patch_attack_chain(
    body: AttackChainPatchRequest,
    chain_id: UUID = Path(..., description="Attack chain id"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Advance an attack chain's workflow status and return the refreshed detail.

    Partial update; the only mutable field today is ``status``. When the status actually
    changes the transition is recorded as a user activity event (old/new captured); a no-op
    update (same status) skips logging and returns the current chain unchanged.
    """
    manager = AttackChainsManager(db)
    return await manager.patch_chain(chain_id, body, current_user)
