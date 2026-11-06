"""Correlation pipelines API — CRUD, composite run/stop, duplicate.

The N-instance replacement for the singleton ``/api/v1/correlation_job/*`` surface. Thin by
contract: validate, call :class:`~apps.managers.correlation_pipelines.CorrelationPipelinesManager`,
serialize. No Kafka, no Kubernetes and no activity logging here — activity is logged once, in the
manager, for every operation including ``duplicate``.

The platform-wide graph is a sibling resource (``dashboard_router``, prefix
``/api/v1/correlation_dashboard``) so ``dashboard`` is never a path segment next to
``{correlation_pipeline_id}``.

**This is not a tenancy boundary.** The epic isolates the *data plane* (Kafka topics, Flink state,
per-customer config and resources); roles stay platform-wide, so any authenticated viewer can list
every customer's correlation pipelines.
"""

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, get_current_active_user_from_cookie, require_write_access
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.pagination import get_correlation_pipelines_list_params
from apps.core.schemas import (
    CorrelationJobGraphData,
    CorrelationPipelineCreateRequest,
    CorrelationPipelineListResponse,
    CorrelationPipelineResponse,
    CorrelationPipelineRuntimeResponse,
    CorrelationPipelineUpdateRequest,
)
from apps.core.settings import settings
from apps.managers.correlation_dashboard import (
    correlation_dashboard_broadcaster,
    get_correlation_dashboard_snapshot,
)
from apps.managers.correlation_pipelines import CorrelationPipelinesManager

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/correlation_pipelines", tags=["Correlation pipelines"])
dashboard_router = APIRouter(prefix="/api/v1/correlation_dashboard", tags=["Correlation pipelines"])


@router.get(
    "",
    response_model=CorrelationPipelineListResponse,
    summary="List correlation pipelines",
    responses=errors(
        UNAUTHORIZED,
        {200: {"description": "Paginated correlation pipelines with chain counts and observed runtime_state"}},
    ),
)
@router.get("/", response_model=CorrelationPipelineListResponse, include_in_schema=False)
async def list_correlation_pipelines(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_correlation_pipelines_list_params),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineListResponse:
    """Return one page of correlation pipelines, each with members, destination topics, chain count and observed runtime.

    ``jobs_status`` is *desired* state (starting / stopping / stopped). ``runtime_state`` is the
    observed composite of the two Flink jobs, served from a 15s in-memory cache (Kubernetes on a
    cache miss). Stopped pipelines skip the cluster — the same pattern as a disabled detection
    pipeline. The chain counts arrive with the rows from a single grouped aggregate, so a page
    costs a fixed number of statements plus at most one bounded Kubernetes fan-out for rows that
    are actually starting or stopping. List ``overall`` never emits ``partial`` — one failed half
    is ``failed``; the two job tiles still show which half.
    """
    return await CorrelationPipelinesManager(db).list_pipelines(pagination)


@router.post(
    "",
    response_model=CorrelationPipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "A requested member detection pipeline does not exist"),
        err(409, "Name already taken"),
        err(503, "Kafka topic provisioning failed; the pipeline was not created"),
        {201: {"description": "Created and stopped; no Flink deployment exists yet"}},
    ),
)
@router.post(
    "/", response_model=CorrelationPipelineResponse, status_code=status.HTTP_201_CREATED, include_in_schema=False
)
async def create_correlation_pipeline(
    body: CorrelationPipelineCreateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineResponse:
    """Create a correlation pipeline. **Never starts it.**

    Allocates the two deployment names, provisions the pipeline's three Kafka topics, publishes the
    initial chains-config envelope and records a ``create`` activity event. The row is created
    ``stopped`` and no ``FlinkDeployment`` is submitted.

    The UI's **"Create & Run"** split button is a deliberate TWO-CALL flow: this call, then
    ``POST /{id}/run`` with the id from the 201. A ``run_after_create`` flag was rejected because a
    201 body cannot honestly report a start that is still in flight, and folding a failed start into
    the create response would force either a misleading 201 or a rollback of a pipeline the user
    asked to keep. Keeping them apart also keeps the two failure modes separately reportable.
    """
    return await CorrelationPipelinesManager(db).create(body, current_user)


def _correlation_dashboard_sse_event(data: CorrelationJobGraphData | None, error: Exception | None) -> str:
    """Serialize the shared graph snapshot into one SSE ``data:`` frame."""
    if error is not None or data is None:
        event = {
            "type": "error",
            "error": str(error) if error is not None else "no data available",
            "timestamp": datetime.now(UTC).isoformat(),
        }
    else:
        event = {
            "type": "correlation_dashboard_update",
            "data": data.model_dump(mode="json"),
            "timestamp": datetime.now(UTC).isoformat(),
        }
    return f"data: {json.dumps(event)}\n\n"


async def _correlation_dashboard_sse_generator(request: Request):
    """Fan out one shared per-tick graph payload until the client disconnects."""
    interval = settings.dashboard_broadcast_interval_seconds
    await correlation_dashboard_broadcaster.subscribe()
    try:
        data, error = correlation_dashboard_broadcaster.get_cached()
        yield _correlation_dashboard_sse_event(data, error)

        while True:
            if await request.is_disconnected():
                logger.debug("Correlation dashboard SSE client disconnected")
                break
            try:
                await asyncio.sleep(interval)
                if await request.is_disconnected():
                    break
                data, error = correlation_dashboard_broadcaster.get_cached()
                yield _correlation_dashboard_sse_event(data, error)
            except asyncio.CancelledError:
                break
    finally:
        await correlation_dashboard_broadcaster.unsubscribe()


@dashboard_router.get(
    "/snapshot",
    response_model=CorrelationJobGraphData,
    summary="Get the platform-wide correlation dashboard",
    responses=errors(
        UNAUTHORIZED,
        {200: {"description": "Five graph blocks aggregated across every correlation pipeline"}},
    ),
)
@dashboard_router.get("/snapshot/", include_in_schema=False)
async def get_correlation_dashboard(
    _: User = Depends(get_current_active_user),
) -> CorrelationJobGraphData:
    """Return the five Configure-dashboard blocks as a one-time REST snapshot.

    Not scoped to a correlation pipeline: ATTACK CHAINS, AFFECTED HOSTNAMES, TAGGED EVENTS and
    MATCHED RULES count every chain and event, including those whose producing pipeline was
    deleted (``ON DELETE SET NULL``). MONITORED THREATS is the Active Threat catalogue
    (non-hidden / not globally blocked), not a per-pipeline deny-list. ``runtime_state`` lives on
    the list and detail endpoints. When the SSE stream is live this is the current tick, so a
    poll does not re-run the aggregates.
    """
    return await get_correlation_dashboard_snapshot()


@dashboard_router.get(
    "/stream",
    summary="Stream the platform-wide correlation dashboard (SSE)",
    responses={
        200: {
            "description": "Server-Sent Events: UTC-day graph series for every correlation pipeline.",
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                    "example": (
                        'data: {"type": "correlation_dashboard_update", "data": {"blocks": []}, '
                        '"timestamp": "2024-01-01T00:00:00Z"}\n\n'
                    ),
                }
            },
        },
        **UNAUTHORIZED,
    },
)
@dashboard_router.get("/stream/", include_in_schema=False)
async def stream_correlation_dashboard(
    request: Request,
    _: User = Depends(get_current_active_user_from_cookie),
) -> StreamingResponse:
    """Stream the five Configure-dashboard blocks as Server-Sent Events.

    One shared producer computes the payload once per ``DASHBOARD_BROADCAST_INTERVAL_SECONDS``
    for every connected client. Kubernetes is not consulted. An idle process (zero subscribers)
    does no periodic work.

    **Authentication:** the `CookieAuth` scheme — the HttpOnly `access_token` cookie set by login,
    not the `Authorization` header (`EventSource` cannot send custom headers).
    """
    logger.info("Correlation dashboard SSE client connected")
    return StreamingResponse(
        _correlation_dashboard_sse_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get(
    "/{correlation_pipeline_id}",
    response_model=CorrelationPipelineResponse,
    summary="Get a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Correlation pipeline not found"),
        {200: {"description": "Correlation pipeline with members, chain count and observed runtime state"}},
    ),
)
async def get_correlation_pipeline(
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineResponse:
    """Return one correlation pipeline expanded for the Config screen.

    Read-only, but it does observe Kubernetes: ``runtime_state`` carries both Flink jobs' tiles and
    the composite status behind the two Pipeline Runtime panels. ``correlation_window_seconds`` is
    null when the pipeline inherits each high-order Sigma rule's own ``detection.timeframe``, and a
    null resource field means the platform default from settings applies at render time.
    """
    return await CorrelationPipelinesManager(db).get(correlation_pipeline_id)


@router.patch(
    "/{correlation_pipeline_id}",
    response_model=CorrelationPipelineResponse,
    summary="Update a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(400, "An immutable field was sent"),
        err(404, "Correlation pipeline, or a requested member detection pipeline, not found"),
        err(
            409,
            "A restart-class field changed while the jobs are up and confirm_restart is false",
        ),
        {200: {"description": "Updated pipeline; ``warnings`` and ``restarted`` describe the side effects"}},
    ),
)
async def update_correlation_pipeline(
    body: CorrelationPipelineUpdateRequest,
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    confirm_restart: bool = Query(
        default=False,
        description=(
            "Acknowledge that a restart-class change (resources or membership) restarts the running "
            "Flink job(s). Without it such a change on a running pipeline is refused with 409"
        ),
    ),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineResponse:
    """Partially update a correlation pipeline; only the fields present in the body change.

    Two classes of edit. **Hot** fields (threshold, correlation window, auto-add) are published on
    the pipeline's chains-config topic and never restart anything. **Restart** fields (the ten
    resource columns and the member set, which renders ``KAFKA_INPUT_TOPICS``) need a new
    deployment spec — roughly a 30-second correlation gap for that customer — so while the jobs are
    up they are refused with ``409`` unless ``confirm_restart=true``. A stopped pipeline applies
    them silently; the new spec is picked up at the next start.

    Sending ``correlation_window_seconds: null`` explicitly CLEARS the override (each high-order
    Sigma rule's own ``detection.timeframe`` applies again); omitting the field leaves it alone.
    Changing the window re-buckets ``chain_id``, so in-flight chains close under the old window and
    the same attack can appear as two chains across the edit — reported in ``warnings``.
    """
    return await CorrelationPipelinesManager(db).update(
        correlation_pipeline_id, body, current_user, confirm_restart=confirm_restart
    )


@router.delete(
    "/{correlation_pipeline_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Correlation pipelines require FLINK_PROVIDER=kubernetes"),
        err(404, "Correlation pipeline not found"),
        {204: {"description": "Pipeline stopped, topics retired, row deleted"}},
    ),
)
async def delete_correlation_pipeline(
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> Response:
    """Stop the pipeline's two Flink jobs, retire its Kafka topics and delete the row.

    The chains it produced are **kept**: the FK is ``ON DELETE SET NULL``, because a chain carries
    analyst-owned state and outlives the pipeline. Its members' ``tagged-events`` topics belong to
    the detection-pipeline lifecycle and are untouched. Topic retirement follows
    ``KAFKA_TOPIC_DELETE_POLICY``; a downgraded retirement is recorded as a warning activity event
    rather than failing the delete (204 carries no body to return it in).
    """
    await CorrelationPipelinesManager(db).delete(correlation_pipeline_id, current_user)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/{correlation_pipeline_id}/duplicate",
    response_model=CorrelationPipelineResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Duplicate a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(404, "Correlation pipeline not found"),
        err(409, "Fifty copies of this name already exist, or the name was taken concurrently"),
        err(503, "Kafka topic provisioning failed; the copy was not created"),
        {201: {"description": "The new, stopped copy (configuration and members; own topics and identity)"}},
    ),
)
async def duplicate_correlation_pipeline(
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id to copy"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineResponse:
    """Copy a correlation pipeline's configuration into a new, stopped pipeline. No body, no options.

    Returns **201 with the new id**. Copied: threshold, correlation window (**including null**,
    which means "inherit the rule's timeframe" and is a real value), auto-add flag and pattern,
    all ten resource columns — nulls stay null, so the copy keeps inheriting the platform defaults
    instead of freezing today's numbers into a row — member detection pipelines, and the Active
    Threat deny-list. Sharing a detection pipeline across correlation jobs is allowed; each job
    independently consumes that member's tagged-events topic. Reset: id, both deployment names,
    the three Kafka topics (all minted from the NEW id — sharing them would merge two tenants'
    chains), ``jobs_status="stopped"``, chain count, and timestamps. No ``FlinkDeployment`` is
    created and the source pipeline is untouched, still running if it was. The copy does not
    consume until an operator ``POST /{id}/run``.

    The name is ``"<source> (Copy)"``, escalating to ``" (Copy 2)"``… when taken, because
    correlation-pipeline names are unique; after 50 attempts it is a ``409``. To duplicate *and*
    change something, duplicate then ``PATCH`` the copy.
    """
    return await CorrelationPipelinesManager(db).duplicate(correlation_pipeline_id, current_user)


@router.post(
    "/{correlation_pipeline_id}/run",
    response_model=CorrelationPipelineRuntimeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Correlation pipelines require FLINK_PROVIDER=kubernetes"),
        err(404, "Correlation pipeline not found"),
        {
            202: {
                "description": (
                    "Both deployments submitted (``starting``); ``partial`` if one half failed, ``failed`` if both did"
                )
            }
        },
    ),
)
async def run_correlation_pipeline(
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineRuntimeResponse:
    """Submit this pipeline's correlation-job and agent-job FlinkDeployments (correlation first).

    Accepted, not completed — hence 202 rather than the singleton pair's 200: the CRDs are
    submitted and the row flips to ``starting``, and reaching ``running`` is observed later via the
    detail endpoint. ``partial`` means exactly one half failed —
    the healthy half is deliberately left running (no auto-rollback, so a transient image pull on
    the agent side does not tear down a healthy correlation job) — and ``failed`` means both did.
    The current chains-config is published before launch so a cold-started matcher never correlates
    against a stale compacted record. Only the requested pipeline is touched, and the start is
    recorded as an activity event by the manager.
    """
    result = await CorrelationPipelinesManager(db).start(correlation_pipeline_id, current_user)
    return CorrelationPipelineRuntimeResponse(
        status=result.status,
        correlation_job=result.correlation_job,
        agent_job=result.agent_job,
    )


@router.post(
    "/{correlation_pipeline_id}/stop",
    response_model=CorrelationPipelineRuntimeResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Stop a correlation pipeline",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Correlation pipelines require FLINK_PROVIDER=kubernetes"),
        err(404, "Correlation pipeline not found"),
        {202: {"description": "Both deployments torn down; per-job ``cleanup`` status reported"}},
    ),
)
async def stop_correlation_pipeline(
    correlation_pipeline_id: UUID = Path(..., description="Correlation pipeline id"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> CorrelationPipelineRuntimeResponse:
    """Tear down this pipeline's two FlinkDeployments (agent first, so it drains ``chains-raw``).

    202 for the same reason as ``run``: deletion is submitted and the row flips to ``stopping``;
    ``stopped`` is reported once both deployments are actually gone. Each job's checkpoint / HA /
    savepoint state is then wiped so the next start is a cold start — a cleanup failure does not
    fail the stop, and cleanup is skipped (never forced) when a deployment has not confirmed
    termination. Idempotent: stopping an already-stopped pipeline tears nothing down. Other
    tenants' pipelines are untouched.
    """
    result = await CorrelationPipelinesManager(db).stop(correlation_pipeline_id, current_user)
    return CorrelationPipelineRuntimeResponse(
        status=result.status,
        correlation_job=result.correlation_job,
        agent_job=result.agent_job,
    )
