"""DEPRECATED singleton correlation surface, bound to the ``Default`` correlation pipeline.

Two operations survive only to keep the pre-cutover frontend working: ``GET``/``PATCH
/configuration``. Their response shapes and status codes are frozen. Everything behind them now
resolves ``Default`` by name and delegates to the per-pipeline manager. Nothing in this module
reads ``attack_chain_configuration``.

The platform-wide graph lives at ``GET /api/v1/correlation_dashboard/snapshot`` (REST)
and ``GET /api/v1/correlation_dashboard/stream`` (SSE). Runtime
start/stop live on ``/api/v1/attack_chains/runtime/{start,stop}`` and
``POST /api/v1/correlation_pipelines/{id}/{run,stop}``.

``remove-singleton-correlation-endpoints.md`` deletes the module once the frontend MR that stops
calling it has merged.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.database import get_db
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.schemas import AttackChainConfigurationResponse, AttackChainConfigurationUpdateRequest
from apps.managers.attack_chains import AttackChainsManager

router = APIRouter(prefix="/api/v1/correlation_job", tags=["Correlation job"])


@router.get(
    "/configuration",
    response_model=AttackChainConfigurationResponse,
    summary="Get attack chain correlation configuration",
    deprecated=True,
    responses=errors(UNAUTHORIZED, err(404, "No default correlation pipeline")),
)
@router.get("/configuration/", include_in_schema=False)
async def get_attack_chain_configuration(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the `Default` correlation pipeline in the legacy singleton shape.

    **Deprecated.** Superseded by `/api/v1/correlation_pipelines/*`; this route operates on the
    `Default` correlation pipeline only and is removed in a future release.

    Threshold, correlation window, auto-add toggle. Read-only, and nothing is
    created on access any more — a missing `Default` pipeline is a `404`.

    `correlation_window_seconds` renders `300` when the pipeline inherits each HoS rule's
    `detection.timeframe` (its column is nullable; this field is not). `id` stays `1`; the real
    id is on `/api/v1/correlation_pipelines`. Membership is `member_pipeline_ids` on that API,
    not destination-topic toggles.
    """
    manager = AttackChainsManager(db)
    return await manager.get_configuration()


@router.patch(
    "/configuration",
    response_model=AttackChainConfigurationResponse,
    summary="Update attack chain correlation configuration",
    deprecated=True,
    responses=errors(
        UNAUTHORIZED,
        err(404, "No default correlation pipeline"),
    ),
)
@router.patch("/configuration/", include_in_schema=False)
async def patch_attack_chain_configuration(
    body: AttackChainConfigurationUpdateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Partially update the `Default` correlation pipeline through the legacy singleton shape.

    **Deprecated.** Superseded by `/api/v1/correlation_pipelines/*`; this route operates on the
    `Default` correlation pipeline only and is removed in a future release.

    Only the fields present in the body are changed, and the edit goes through the per-pipeline
    manager: hot fields (threshold, window, auto-add) are published to that pipeline's chains-config
    topic with no Flink restart, and the change is recorded as a `correlation_pipeline` activity
    event rather than an `attack_chain_configuration` one.

    Membership is `PATCH /api/v1/correlation_pipelines/{id}` with `member_pipeline_ids`. Sending
    `monitored_topics` here is a 422.
    """
    manager = AttackChainsManager(db)
    return await manager.update(body, current_user)


# /api/v1/correlation_job/{run,stop} removed in favour of the composite
# /api/v1/attack_chains/runtime/{start,stop} — Attack Chains can no longer be
# operated one job at a time (architecture §1 + §9).
