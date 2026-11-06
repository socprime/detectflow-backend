"""Composite Attack Chains runtime lifecycle (correlation-job + agent-job).

Architecture §1 + §9: the two jobs cannot be operated independently —
without agent-job, ``chains-raw`` accumulates with no consumer; without
correlation-job, agent-job has nothing to read. This router replaces the
old per-job ``/api/v1/correlation_job/{run,stop}`` endpoints with a single
composite control surface that starts both (correlation first), stops both
(agent first to drain ``chains-raw``), and reports both submission/deletion results.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import require_write_access
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.schemas import AttackChainsRuntimeResponse
from apps.managers.attack_chains import AttackChainsManager

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/attack_chains/runtime", tags=["Attack chains runtime"])


@router.post(
    "/start",
    response_model=AttackChainsRuntimeResponse,
    summary="Start the Attack Chains runtime",
    deprecated=True,
    responses=errors(
        UNAUTHORIZED,
        err(400, "Attack chains runtime requires FLINK_PROVIDER=kubernetes"),
        err(404, "No default correlation pipeline"),
        {200: {"description": "Both deployments submitted (``starting``) or ``partial`` if agent-job launch failed"}},
    ),
)
@router.post("/start/", include_in_schema=False)
async def start_runtime(
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> AttackChainsRuntimeResponse:
    """Deploy the Attack Chains correlation runtime: submit the correlation-job and agent-job Flink jobs.

    **Deprecated.** Superseded by `/api/v1/correlation_pipelines/*`; this route operates on the
    `Default` correlation pipeline only and is removed in a future release.

    Pushes that pipeline's current monitored-topics selection to its chains-config Kafka topic
    (hot, no restart), flips its ``jobs_status`` to ``starting``, then submits the two
    ``FlinkDeployment`` CRDs in order — correlation-job FIRST (so ``chains-raw`` has a producer
    before its consumer subscribes), agent-job SECOND. The start is logged as a user activity
    event. The response carries the composite runtime state, not just an ack: ``starting`` when
    both submitted, or ``partial`` if the agent half failed — the correlation half is left
    running (no auto-rollback) so a transient agent error doesn't tear down a healthy job.
    Returned at HTTP 200 deliberately because ``partial`` is a successful, actionable outcome.

    Raises ``400`` (BadRequestError) unless ``FLINK_PROVIDER=kubernetes``, and ``404`` when no
    `Default` correlation pipeline exists.
    """
    manager = AttackChainsManager(db)
    return await manager.start_runtime(current_user)


@router.post(
    "/stop",
    response_model=AttackChainsRuntimeResponse,
    summary="Stop the Attack Chains runtime",
    deprecated=True,
    responses=errors(
        UNAUTHORIZED,
        err(400, "Attack chains runtime requires FLINK_PROVIDER=kubernetes"),
        err(404, "No default correlation pipeline"),
        {200: {"description": "Both deployments torn down; per-job cleanup status reported"}},
    ),
)
@router.post("/stop/", include_in_schema=False)
async def stop_runtime(
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> AttackChainsRuntimeResponse:
    """Tear down the Attack Chains correlation runtime: delete the agent-job and correlation-job Flink jobs.

    **Deprecated.** Superseded by `/api/v1/correlation_pipelines/*`; this route operates on the
    `Default` correlation pipeline only and is removed in a future release.

    Flips that pipeline's ``jobs_status`` to ``stopping`` and deletes the two ``FlinkDeployment`` CRDs in
    order — agent-job FIRST so it can drain in-flight ``chains-raw``, correlation-job SECOND —
    then wipes each job's checkpoint/HA/savepoint state on the shared PVCs so the next start is a
    cold start (cleanup failure does NOT fail the stop). The stop is logged as a user activity
    event. The response carries the composite runtime state plus a per-job ``cleanup`` status
    (``ok`` / ``error`` / ``skipped: wait_timeout``); status settles to ``stopped`` once both
    deployments are gone, otherwise ``stopping``. Returned at HTTP 200 deliberately.

    Raises ``400`` (BadRequestError) unless ``FLINK_PROVIDER=kubernetes``, and ``404`` when no
    `Default` correlation pipeline exists.
    """
    manager = AttackChainsManager(db)
    return await manager.stop_runtime(current_user)
