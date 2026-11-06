"""Active threats API — TDM-backed catalog rows, per-pipeline monitoring, block, and full refresh."""

import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.converters import ActiveThreatConverter
from apps.core.database import get_db
from apps.core.exceptions import ConflictError, ServiceUnavailableError
from apps.core.models import User
from apps.core.openapi import NOT_FOUND, UNAUTHORIZED, err, errors
from apps.core.pagination import get_active_threats_list_params
from apps.core.schemas import (
    ActiveThreatBlockRequest,
    ActiveThreatBlockResponse,
    ActiveThreatBulkIdsRequest,
    ActiveThreatBulkResponse,
    ActiveThreatListResponse,
    ActiveThreatPipelineMonitoringRequest,
    ActiveThreatPipelineMonitoringResponse,
    ActiveThreatResponse,
    SyncStartedResponse,
)
from apps.core.settings import settings
from apps.managers.active_threats import ActiveThreatsManager
from apps.modules.kafka.activity import activity_producer
from apps.services.scheduler import get_scheduler_service

router = APIRouter(prefix="/api/v1/active_threats", tags=["Active threats"])


@router.get(
    "",
    response_model=ActiveThreatListResponse,
    summary="List active threats",
    responses=errors(UNAUTHORIZED, {200: {"description": "Paginated list of active threats"}}),
)
@router.get("/", include_in_schema=False)
async def list_active_threats(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_active_threats_list_params),
    severity: list[str] | None = Query(
        default=None,
        description="Filter by severity (case-insensitive). Repeatable: ?severity=high&severity=medium",
    ),
    industries: list[str] | None = Query(
        default=None,
        description="Filter by industry (any-of). Repeatable. Matches `active_threats.industries`.",
    ),
    threat_type: list[str] | None = Query(
        default=None,
        description="Filter by threat type (any-of). Repeatable. Matches `active_threats.threat_types`.",
    ),
    regions: list[str] | None = Query(
        default=None,
        description="Filter by region (any-of). Repeatable. Matches `active_threats.geographies`.",
    ),
    correlation_pipeline_id: UUID | None = Query(
        default=None,
        description=(
            "Return only the threats this correlation pipeline monitors: not blocked "
            "platform-wide and not excluded from it. Omitted returns the whole catalogue."
        ),
    ),
    db: AsyncSession = Depends(get_db),
):
    """List active threats with pagination, optional case-insensitive ``search``, filters, and sorting.

    ``search`` matches ``name``, ``severity``, and technique IDs on ``active_threats.techniques``
    (substring, so ``T1053`` also matches ``T1053.005``).

    Each row carries ``is_blocked`` (the platform-wide switch) and ``pipelines`` — the correlation
    pipelines currently monitoring the threat, which is every pipeline minus the ones that excluded
    it. Monitoring is opt-out, so a blocked threat lists no pipelines at all.

    Sort options: ``release_date``, ``name``, ``severity``, ``blocked``, and ``enabled`` (kept as
    the inverse of ``blocked`` so the column's meaning is unchanged).
    """
    manager = ActiveThreatsManager(db)
    rows, total = await manager.get_all(
        pagination,
        severity=severity,
        industries=industries,
        threat_types=threat_type,
        geographies=regions,
        correlation_pipeline_id=correlation_pipeline_id,
    )
    data = [ActiveThreatConverter.to_list_item(threat, pipelines) for threat, pipelines in rows]
    return ActiveThreatListResponse(
        total=total,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"] or "",
        order=pagination["order"],
        data=data,
    )


@router.post(
    "/bulk_enabled",
    response_model=ActiveThreatBulkResponse,
    summary="Bulk monitor active threats in correlation pipelines",
    responses=errors(
        UNAUTHORIZED,
        err(400, "One or more correlation pipeline ids do not exist"),
        {200: {"description": "Per-id update outcome"}},
    ),
)
async def bulk_enable_active_threats(
    body: ActiveThreatBulkIdsRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Monitor the given active threats in the selected correlation pipelines.

    Deletes each threat's exclusion row in every requested pipeline and republishes those
    pipelines' chains-config — a hot broadcast change that restarts no Flink job. It does **not**
    touch ``is_blocked`` or the shared active-threats topic; use ``PUT /{threat_id}/blocked`` for
    the platform-wide switch.

    ``correlation_pipeline_ids`` is required (1-200 ids) — a monitoring change always names the
    customers it applies to. The request never fails on unknown threat ids: the response carries a
    per-id ``status`` (``updated``, ``not_found``, or ``blocked`` for a threat blocked platform-wide)
    plus aggregate counts and the pipeline set applied. An unknown correlation pipeline id is a 400.
    """
    manager = ActiveThreatsManager(db)
    return await manager.bulk_set_monitoring(
        body.threat_ids,
        body.correlation_pipeline_ids,
        monitored=True,
        user=current_user,
    )


@router.post(
    "/bulk_disabled",
    response_model=ActiveThreatBulkResponse,
    summary="Bulk stop monitoring active threats in correlation pipelines",
    responses=errors(
        UNAUTHORIZED,
        err(400, "One or more correlation pipeline ids do not exist"),
        {200: {"description": "Per-id update outcome"}},
    ),
)
async def bulk_disable_active_threats(
    body: ActiveThreatBulkIdsRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Stop monitoring the given active threats in the selected correlation pipelines.

    Writes an exclusion row per (threat, pipeline) and republishes those pipelines'
    chains-config — a hot broadcast change that restarts no Flink job. **Behaviour change:** this
    route used to mean "stop correlating everywhere" (i.e. block); it now excludes the threats from
    the selected pipelines only. Blocking is ``PUT /{threat_id}/blocked``, and the shared
    active-threats topic is not written here.

    ``correlation_pipeline_ids`` is required (1-200 ids): omitting it used to mean *every* pipeline,
    so a forgotten field silently stopped monitoring for every customer. Per-id ``status`` is
    ``updated``, ``not_found``, or ``blocked``; an unknown correlation pipeline id is a 400.
    """
    manager = ActiveThreatsManager(db)
    return await manager.bulk_set_monitoring(
        body.threat_ids,
        body.correlation_pipeline_ids,
        monitored=False,
        user=current_user,
    )


@router.post(
    "/refresh",
    response_model=SyncStartedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Full-refresh active threats, HOS, and rules",
    responses={
        202: {"description": "Full refresh started in background"},
        **errors(
            UNAUTHORIZED,
            err(409, "A sync is already running"),
            err(503, "Active Threat sync is disabled"),
        ),
    },
)
@router.post("/refresh/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def refresh_active_threats(
    current_user: User = Depends(require_write_access),
):
    """Trigger a full (non-incremental) re-sync of Active Threats from SOC Prime TDM.

    Unlike the scheduled incremental sync, this ignores the per-entity update cursors and
    re-fetches every Active Threat within the TDM lookback window plus all locally known
    High-Order Sigma bodies and referenced/behavioral Sigma rules. Use it after deploying
    logic changes, or to pick up a HOS/rule that became available in TDM without its Active
    Threat changing.

    Returns 202 immediately; the work runs in an asyncio background task. Emits a `sync_start`
    activity-log entry, rejects with 409 if a sync is already running, and 503 if Active Threat
    sync is disabled. Poll `GET /sync-repositories/status` (key `active_threats_full_refresh`)
    for progress.
    """
    if not settings.sync_active_threats_enabled:
        raise ServiceUnavailableError("Active Threat sync is disabled")

    scheduler = get_scheduler_service()
    if scheduler.is_sync_running:
        raise ConflictError("A sync is already running")

    await activity_producer.log_action(
        action="sync_start",
        entity_type="active_threat",
        user=current_user,
        details="Active Threat full refresh triggered",
        source="user",
    )

    asyncio.create_task(scheduler.run_active_threats_full_refresh_in_background())
    return {"message": "Active Threat full refresh started in background"}


@router.get(
    "/{threat_id}",
    response_model=ActiveThreatResponse,
    summary="Get active threat by TDM id",
    responses=errors(
        UNAUTHORIZED,
        NOT_FOUND,
        {200: {"description": "Full threat with high-order Sigmas and deduped linked rules"}},
    ),
)
async def get_active_threat(
    threat_id: str = Path(..., description="Active threat TDM id (`active_threats.tdm_id`)"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return one active threat with all fields included.

    ``pipelines`` lists EVERY correlation pipeline — monitoring or not — with its runtime state,
    ``first_seen`` / ``last_seen`` over that pipeline's chains for this threat, and an attack-chain
    breakdown across all seven workflow statuses. ``attack_chains_total`` sums all seven, so it
    reconciles with ``GET /attack_chains`` even though the UI's bar renders six.
    ``rule_check_results_count`` is the number of distinct linked rules in ``rule_check_results``.
    """
    manager = ActiveThreatsManager(db)
    threat, pipelines = await manager.get_detail(threat_id)
    return ActiveThreatConverter.to_detail(threat, include_all=True, pipelines=pipelines)


@router.put(
    "/{threat_id}/pipelines/{correlation_pipeline_id}",
    response_model=ActiveThreatPipelineMonitoringResponse,
    summary="Set threat monitoring for one correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        NOT_FOUND,
        err(409, "The threat is blocked for all pipelines; unblock it first"),
        {200: {"description": "Monitoring state after the call"}},
    ),
)
async def set_active_threat_pipeline_monitoring(
    body: ActiveThreatPipelineMonitoringRequest,
    threat_id: str = Path(..., description="Active threat TDM id (`active_threats.tdm_id`)"),
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Monitor or stop monitoring one active threat in one correlation pipeline.

    An idempotent PUT rather than a toggle, so two operators editing the same row cannot swap it
    twice. ``monitored=false`` writes the pipeline's exclusion row, ``true`` removes it; the
    pipeline's chains-config is republished only when a row actually changed, and no Flink job is
    restarted either way. Monitoring a threat that is blocked platform-wide is a 409 — the block is
    the stronger gate and must be lifted first.
    """
    manager = ActiveThreatsManager(db)
    return await manager.set_pipeline_monitoring(threat_id, correlation_pipeline_id, body.monitored, current_user)


@router.put(
    "/{threat_id}/blocked",
    response_model=ActiveThreatBlockResponse,
    summary="Block or unblock a threat for all pipelines",
    responses=errors(
        UNAUTHORIZED,
        NOT_FOUND,
        {200: {"description": "Block state after the call"}},
    ),
)
async def set_active_threat_blocked(
    body: ActiveThreatBlockRequest,
    threat_id: str = Path(..., description="Active threat TDM id (`active_threats.tdm_id`)"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Set the platform-wide block switch for one active threat.

    Blocking removes the threat from monitoring in every correlation pipeline, now and future, and
    republishes it on the shared active-threats topic as ``enabled=false``. Per-pipeline exclusions
    are deliberately preserved, so a mistaken block is reversible without losing analyst intent and
    no chains-config record is written.

    Unblocking is the transition that mutates per-pipeline state: it clears every exclusion of the
    threat and republishes exactly those pipelines' chains-config, which is what "enabled in all
    existing pipelines and all future ones" means. Both directions are written to the activity log.
    """
    manager = ActiveThreatsManager(db)
    return await manager.set_blocked(threat_id, body.blocked, current_user)
