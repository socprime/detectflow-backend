"""Per-customer correlation pipelines: CRUD, composite lifecycle and chains-config publishing.

One ``correlation_pipelines`` row owns a correlation-job + agent-job pair, its member detection
pipelines, its correlation parameters and the compute budget of both jobs. This manager is the
N-instance replacement for the singleton runtime/config half of ``AttackChainsManager``, which
keeps only the chain list/detail and graph responsibilities plus a deprecated shim surface.

The module also owns the composite-runtime mechanics themselves (launch/teardown ordering, K8s
state derivation, health signal, storage cleanup). They are module-level functions rather than
methods so the quarantined singleton path in ``apps.managers.attack_chains`` can call exactly the
same code with its legacy rendering config — a split that duplicated them would let the two
runtimes drift.
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.constants import (
    agent_deployment_name,
    correlation_deployment_name,
    pipeline_name_matches_auto_add_pattern,
)
from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import (
    BadRequestError,
    ConfigurationError,
    ConflictError,
    DuplicateEntityError,
    NotFoundError,
    ServiceUnavailableError,
)
from apps.core.logger import get_logger
from apps.core.models import CorrelationPipeline, User
from apps.core.schemas import (
    AttackChainsJobState,
    AttackChainsRuntimeResponse,
    AttackChainsRuntimeStateResponse,
    CorrelationPipelineCreateRequest,
    CorrelationPipelineJobResources,
    CorrelationPipelineListItem,
    CorrelationPipelineListResponse,
    CorrelationPipelineMember,
    CorrelationPipelineResponse,
    CorrelationPipelineUpdateRequest,
    destination_topics_from_pipeline,
)
from apps.core.settings import settings
from apps.managers.correlation_runtime_status import correlation_runtime_status_cache
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.chains import chains_config_producer
from apps.modules.kafka.topic_lifecycle import (
    TopicProvisioningError,
    chains_analysed_topic_for,
    chains_config_topic_for,
    chains_raw_topic_for,
    topic_lifecycle_service,
)
from apps.modules.postgre.active_threats import ActiveThreatDAO
from apps.modules.postgre.audit import AuditLogDAO
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.providers import CorrelationDeploymentConfig, StorageCleanupOutcome, get_flink_provider
from apps.providers.kubernetes import KubernetesFlinkProvider

logger = get_logger(__name__)

RuntimeStatus = Literal["running", "starting", "stopping", "stopped", "partial", "failed", "missing"]
RuntimeK8sState = tuple[RuntimeStatus, datetime | None]
RestartTarget = Literal["correlation", "agent"]

# Upper bound on how many correlation pipelines one fan-out (SSE tick, auto-add scan) considers.
# Deliberately far above any realistic tenant count: it exists so a corrupt table cannot turn a
# 2-second tick into an unbounded Kubernetes sweep, not as a paging mechanism.
_PIPELINE_FANOUT_LIMIT = 200

# Attempts at " (Copy)", " (Copy 2)", ... before a duplicate gives up. ``name`` is UNIQUE, and the
# migration runbook resolves the Default pipeline by name, so the constraint stays and the suffix
# escalates instead.
_DUPLICATE_SUFFIX_ATTEMPTS = 50
_NAME_MAX_LENGTH = 255

# Names the data migration (``c7e1f04b93aa``) can give the row it creates, in resolution order.
# It inserts ``Default``, or ``Default (migrated singleton)`` when an operator had already created
# a ``Default`` by hand — a collision the runbook's step 1.2 asks to resolve, not a second runtime.
# `name` is UNIQUE and this is the epic's only identity-by-name lookup, which is why the constraint
# was kept rather than dropped to mirror detection pipelines.
DEFAULT_CORRELATION_PIPELINE_NAMES: tuple[str, ...] = ("Default", "Default (migrated singleton)")


# =============================================================================
# Edit-safety matrix
# =============================================================================
#
# Mirrors FLINK_RESTART_FIELDS in apps/managers/pipeline_orchestrator.py. Every settable column of
# `correlation_pipelines`, plus the declared relation pseudo-fields below, MUST appear in exactly
# one of these five sets — tests/test_correlation_pipeline_edit_matrix.py fails the build if a new
# column is added without classifying it.

# Relation pseudo-fields: join-table state that reaches the wire but is not a column on the row.
# `member_pipeline_ids` is edited through this manager's update request; `excluded_active_threat_ids`
# is edited through the Active Threats surface (`ActiveThreatsManager`), which republishes the
# affected pipelines itself — it is classified here so the matrix stays exhaustive over everything
# the envelope carries, not only over what a PATCH can send.
RELATION_FIELDS: frozenset[str] = frozenset({"member_pipeline_ids", "excluded_active_threat_ids"})

# Hot: published on the pipeline's chains-config topic and picked up by the running matcher's
# broadcast state. Never a restart.
CHAINS_CONFIG_HOT_FIELDS: frozenset[str] = frozenset(
    {
        "threshold_pct",  # read hot per event by HosCorrelationFunction
        "correlation_window_seconds",  # §19 override; NULL = inherit each HoS rule's timeframe
        "auto_add_new_pipelines",  # control-plane only (auto-add at pipeline create); republish is a no-op for the matcher
        "excluded_active_threat_ids",  # §20 deny-list; a monitoring edit is never a restart
    }
)

CORRELATION_RESTART_FIELDS: frozenset[str] = frozenset(
    {
        "member_pipeline_ids",  # -> KAFKA_INPUT_TOPICS (§16/D1); ALSO hot, see `update`
        "correlation_parallelism",
        "correlation_taskmanager_memory_mb",
        "correlation_taskmanager_cpu",
        "correlation_jobmanager_memory_mb",
        "correlation_jobmanager_cpu",
    }
)

AGENT_RESTART_FIELDS: frozenset[str] = frozenset(
    {
        "agent_parallelism",
        "agent_taskmanager_memory_mb",
        "agent_taskmanager_cpu",
        "agent_jobmanager_memory_mb",
        "agent_jobmanager_cpu",
    }
)

# Generated, never user-supplied: a deployment name is K8s object identity and `jobs_status` is
# owned by start/stop. Absent from CorrelationPipelineUpdateRequest (extra="forbid" -> 422); the
# manager raises anyway as belt and braces.
IMMUTABLE_FIELDS: frozenset[str] = frozenset({"correlation_deployment_name", "agent_deployment_name", "jobs_status"})

# DB-only: never reach Kubernetes and never reach the wire. `auto_add_pipeline_name_pattern` is
# evaluated control-plane-side in `auto_add_member` and is deliberately absent from the
# chains-config envelope.
FREE_FIELDS: frozenset[str] = frozenset({"name", "auto_add_pipeline_name_pattern"})

# Request-body fields that carry Flink resources, and the column prefix each maps to.
_RESOURCE_GROUPS: dict[str, str] = {"correlation_resources": "correlation", "agent_resources": "agent"}
_RESOURCE_SUFFIXES: tuple[str, ...] = (
    "parallelism",
    "taskmanager_memory_mb",
    "taskmanager_cpu",
    "jobmanager_memory_mb",
    "jobmanager_cpu",
)

_WINDOW_REBUCKET_WARNING = "window change re-buckets chain_id; in-flight chains close under the old window"
# A removal cold-starts the correlation job (`cold_start_correlation_job`), which is a real loss of
# work — never report it as a plain restart.
_MEMBER_REMOVAL_COLD_START_WARNING = (
    "removing a member cold-starts the correlation job: its in-flight chain buffers were wiped and "
    "chains that were mid-window are not emitted"
)
_COLD_START_CLEANUP_WARNING = (
    "correlation state cleanup reported '{cleanup}': the job may have restored the removed member's "
    "Kafka splits and could still be consuming that topic — verify, and stop/start the pipeline if so"
)
# No input topics is not a degraded start, it is no start: the alternative was subscribing the job
# to the platform-wide shared topic, which is every other customer's data.
_NO_INPUT_TOPICS_WARNING = (
    "the correlation job was left DOWN: no member has a dedicated tagged-events topic, so it has "
    "nothing to consume. Assign one (POST /api/v1/pipeline/{pipeline_id}/tagged-events-topic) or "
    "add a member, then start the pipeline"
)


# =============================================================================
# Rendering inputs — one row -> both FlinkDeployments
# =============================================================================


def memory_mb(value: str | None, *, default_mb: int) -> int:
    """Parse a Flink memory setting (``4g`` / ``4096m`` / ``4096``) into whole megabytes.

    The settings carry Flink-style strings while ``correlation_pipelines`` stores integers; ``4g``
    and ``4096m`` size the JVM identically, so the conversion is lossless for the values this repo
    ships. An unparseable value falls back to ``default_mb`` rather than failing a start — the
    deployment templates need a number, and refusing to launch over a typo in an optional override
    is worse than launching on the documented default.
    """
    raw = (value or "").strip().lower()
    if not raw:
        return default_mb
    multipliers = {"g": 1024, "m": 1, "k": 1 / 1024}
    multiplier = multipliers.get(raw[-1], None)
    number = raw[:-1] if multiplier is not None else raw
    try:
        return max(1, int(float(number) * (multiplier if multiplier is not None else 1)))
    except ValueError:
        logger.warning(
            "Unparseable Flink memory setting; using the default",
            extra={"value": value, "default_mb": default_mb},
        )
        return default_mb


def platform_resource_defaults() -> dict[str, Any]:
    """Flink resources a NULL column inherits — the production numbers, resolved at render time.

    Constraint 7 of the manager task: 4g / 3.0 CPU on the correlation side is Pemja/Beam off-heap
    headroom, not a mock default. Keeping the fallback here (rather than as a column default) is
    what lets an operator raise the platform baseline without touching every row.
    """
    return {
        "correlation_parallelism": settings.correlation_job_parallelism,
        "correlation_taskmanager_memory_mb": memory_mb(settings.correlation_job_taskmanager_memory, default_mb=4096),
        "correlation_taskmanager_cpu": settings.correlation_job_taskmanager_cpu,
        "correlation_jobmanager_memory_mb": memory_mb(settings.correlation_job_jobmanager_memory, default_mb=2048),
        "correlation_jobmanager_cpu": settings.correlation_job_jobmanager_cpu,
        "agent_parallelism": settings.agent_job_parallelism,
        "agent_taskmanager_memory_mb": memory_mb(settings.agent_job_taskmanager_memory, default_mb=3072),
        "agent_taskmanager_cpu": settings.agent_job_taskmanager_cpu,
        "agent_jobmanager_memory_mb": memory_mb(settings.agent_job_jobmanager_memory, default_mb=2048),
        "agent_jobmanager_cpu": settings.agent_job_jobmanager_cpu,
    }


def _removed_members(changes: dict[str, dict[str, Any]]) -> list[str]:
    """The member ids an edit DROPS — the one condition that forces a cold start.

    Reads the diff rather than the request: ``_diff`` reports ``member_pipeline_ids`` as sorted
    ``{"old": [...], "new": [...]}`` string ids, so an edit that only ADDS members returns ``[]``
    here and keeps the cheap ``last-state`` roll. An edit that reorders or re-sends the same set
    never reaches this function — ``_diff`` does not report it as a change at all.
    """
    change = changes.get("member_pipeline_ids")
    if not change:
        return []
    return sorted(set(change["old"]) - set(change["new"]))


def unassigned_member_names(row: CorrelationPipeline) -> list[str]:
    """Members with no dedicated tagged-events topic — the ones that contribute no input.

    ``pipelines.tagged_events_topic`` is NULL until the pipeline is explicitly assigned its own
    topic (``POST /api/v1/pipeline/{id}/tagged-events-topic``, the only sanctioned writer). The API
    cannot attach such a member — :meth:`CorrelationPipelinesManager.add_member` refuses it — so the
    reachable sources are the ``Default`` migration, which deliberately creates members before any
    topic exists, and hand-written SQL.
    """
    return sorted(member.name for member in (row.members or []) if not member.tagged_events_topic)


def member_input_topics(row: CorrelationPipeline) -> list[str]:
    """Kafka subscription universe of one correlation pipeline (§16 / D1).

    One ``pipelines.tagged_events_topic`` per member detection pipeline. A member without one
    contributes **nothing**, and a pipeline that resolves to no topics renders an empty list.

    It used to substitute the shared ``settings.kafka_tagged_events_topic`` for a NULL member (and
    for a member-less pipeline), because the data plane refuses to start on an empty
    ``KAFKA_INPUT_TOPICS`` and a crash-loop looked worse. That reasoning answers "must the job
    start" and gets "which topic" wrong: the shared topic carries **every** customer's tagged
    events, so the fallback silently subscribed one customer's correlation job to the whole
    platform — the failure this epic exists to prevent. It also built every chain twice during the
    singleton cutover (the legacy job reads the same topic, and the consumer's lookup is scoped by
    ``correlation_pipeline_id IS NOT DISTINCT FROM``), and pointed at a retired topic afterwards
    while the job still looked healthy.

    An empty result is therefore a real answer — "this pipeline is not startable yet" — and the
    submit paths refuse it (:func:`assert_startable`) rather than render something wrong.
    """
    if unassigned_member_names(row) and ErrorTracker.should_log(f"correlation_pipeline_unassigned_member_{row.id}"):
        logger.warning(
            "Correlation pipeline has members with no dedicated tagged-events topic; they "
            "contribute no input and the pipeline cannot start until they are assigned one",
            extra={
                "correlation_pipeline_id": str(row.id),
                "unassigned_members": unassigned_member_names(row),
            },
        )
    # Deduplicate while preserving order: two members cannot share a topic today
    # (``tagged_events_topic`` is UNIQUE), but the render must not depend on that.
    return list(
        dict.fromkeys(member.tagged_events_topic for member in (row.members or []) if member.tagged_events_topic)
    )


def assert_startable(row: CorrelationPipeline) -> None:
    """Refuse to start a correlation pipeline that has no input topics.

    The guard sits ahead of every CRD submission rather than inside
    :func:`deployment_config_for_row`, which must keep working for the read paths — the runtime
    state, the dashboard snapshot and ``stop`` all render a config for a pipeline that may well be
    unstartable, and a pipeline nobody can stop would be worse than one nobody can start.

    Raises:
        ConfigurationError: 400 naming the members that still need a topic, or the absence of
            members, plus the endpoint that fixes it.
    """
    if member_input_topics(row):
        return
    unassigned = unassigned_member_names(row)
    detail = f"Correlation pipeline '{row.name}' has no input topics: " + (
        f"member pipeline(s) {', '.join(unassigned)} have no dedicated tagged-events topic. "
        "Assign one with POST /api/v1/pipeline/{pipeline_id}/tagged-events-topic (this restarts "
        "that detection pipeline), then start again."
        if unassigned
        else "it has no members. Add at least one before starting."
    )
    raise ConfigurationError(detail)


def deployment_config_for_row(row: CorrelationPipeline) -> CorrelationDeploymentConfig:
    """Map one correlation pipeline to the rendering inputs of its two FlinkDeployments.

    Resource columns are nullable, and NULL means "inherit the platform default from settings" —
    that is what keeps the production correlation numbers (4g / 3.0) in force for a row that never
    overrode them.
    """
    overrides = {
        f"{prefix}_{suffix}": getattr(row, f"{prefix}_{suffix}")
        for prefix in ("correlation", "agent")
        for suffix in _RESOURCE_SUFFIXES
        if getattr(row, f"{prefix}_{suffix}") is not None
    }
    return CorrelationDeploymentConfig(
        correlation_pipeline_id=str(row.id),
        correlation_pipeline_name=row.name,
        # Persisted names win: a K8s object must survive a rename of its pipeline.
        correlation_deployment_name=row.correlation_deployment_name or correlation_deployment_name(row.id),
        agent_deployment_name=row.agent_deployment_name or agent_deployment_name(row.id),
        input_topics=member_input_topics(row),
        chains_config_topic=chains_config_topic_for(row.id),
        chains_raw_topic=chains_raw_topic_for(row.id),
        chains_analysed_topic=chains_analysed_topic_for(row.id),
        threshold_pct=row.threshold_pct,
        correlation_window_seconds=row.correlation_window_seconds,
        **{**platform_resource_defaults(), **overrides},
    )


# =============================================================================
# Composite runtime mechanics (shared with the deprecated singleton shims)
# =============================================================================


def format_runtime_uptime(total_seconds: int) -> str:
    """Human-readable uptime: non-zero days/hours/minutes, then seconds (rolls up through units)."""
    if total_seconds < 0:
        total_seconds = 0
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def require_kubernetes_provider() -> KubernetesFlinkProvider:
    """The correlation runtime is a Kubernetes-only extension surface (there is no CMF parity)."""
    provider = get_flink_provider()
    if not isinstance(provider, KubernetesFlinkProvider):
        raise BadRequestError("Attack chains runtime is supported only when FLINK_PROVIDER=kubernetes")
    return provider


async def read_runtime_k8s_state(
    config: CorrelationDeploymentConfig,
    *,
    error_key: str,
) -> tuple[RuntimeK8sState | None, RuntimeK8sState | None]:
    """Read both jobs' K8s state. ``None`` for a side whose lookup raised (logged once).

    ``error_key`` scopes the ``ErrorTracker`` throttle so one broken pipeline cannot silence the
    warnings of every other one.
    """
    provider = get_flink_provider()
    if not isinstance(provider, KubernetesFlinkProvider):
        return None, None

    corr: RuntimeK8sState | None
    agent: RuntimeK8sState | None
    try:
        corr = await provider.get_correlation_job_deployment_state(config.correlation_deployment_name)
    except Exception as e:
        if ErrorTracker.should_log(f"correlation_job_k8s_sync:{error_key}"):
            logger.warning(
                "Skipping correlation-job Kubernetes sync; using database state",
                extra={"error": str(e), "deployment_name": config.correlation_deployment_name},
            )
        corr = None
    try:
        agent = await provider.get_agent_job_deployment_state(config.agent_deployment_name)
    except Exception as e:
        if ErrorTracker.should_log(f"agent_job_k8s_sync:{error_key}"):
            logger.warning(
                "Skipping agent-job Kubernetes sync; using database state",
                extra={"error": str(e), "deployment_name": config.agent_deployment_name},
            )
        agent = None
    return corr, agent


def job_state_from_k8s(
    deployment_name: str,
    observed: RuntimeK8sState | None,
    *,
    jobs_status: RuntimeStatus,
) -> AttackChainsJobState:
    """Blend the observed FlinkDeployment state with the desired state stored on the row."""
    if observed is None:
        # K8s API was unreachable; preserve transitional desired states so the UI does not show a
        # completed start/stop before observation resumes.
        status = jobs_status if jobs_status != "stopped" else "missing"
        return AttackChainsJobState(deployment_name=deployment_name, status=status)
    state, started_at = observed
    status: RuntimeStatus = state
    if jobs_status == "starting" and state == "missing":
        status = "starting"
    elif jobs_status in {"stopping", "stopped"} and state != "missing":
        status = "stopping"
    return AttackChainsJobState(
        deployment_name=deployment_name,
        status=status,
        started_at=started_at if status == "running" else None,
    )


def derive_overall_status(corr_state: str, agent_state: str, *, jobs_status: RuntimeStatus) -> RuntimeStatus:
    """Composite status of one correlation pipeline's job pair (``CorrelationPipelineStatus``).

    ``partial`` means EXACTLY one half failed — the healthy half is deliberately left running, so
    the operator has something actionable. ``failed`` means both did, and is why the surfaced
    vocabulary has six values rather than the pre-epic five: reporting a total outage as "partial"
    told the UI a half was still working when none was.
    """
    if jobs_status in {"stopping", "stopped"}:
        if corr_state == "missing" and agent_state == "missing":
            return "stopped"
        return "stopping"

    if corr_state == "running" and agent_state == "running":
        return "running"
    failed = [state for state in (corr_state, agent_state) if state == "failed"]
    if not failed:
        return "starting"
    return "failed" if len(failed) == 2 else "partial"


def list_chip_runtime_state(state: AttackChainsRuntimeStateResponse) -> AttackChainsRuntimeStateResponse:
    """List chip: one failed half is ``failed``, not ``partial``.

    ``partial`` stays on detail / dashboard / ``POST /run`` so the operator still sees that the
    healthy job was left up. The list only has room for one badge, and a pipeline that is not
    producing chains should read as failed. Job tiles are unchanged.
    """
    if state.overall != "partial":
        return state
    return state.model_copy(update={"overall": "failed"})


def resolve_runtime_started_at(corr_state: AttackChainsJobState, agent_state: AttackChainsJobState) -> datetime:
    """Uptime is measured from the LATER of the two starts — the runtime is only up when both are."""
    candidates = [ts for ts in (corr_state.started_at, agent_state.started_at) if ts is not None]
    if not candidates:
        return datetime.now(UTC)
    return max(ts.astimezone(UTC) if ts.tzinfo else ts.replace(tzinfo=UTC) for ts in candidates)


async def wait_for_deployment_terminated(provider: KubernetesFlinkProvider, deployment_name: str) -> bool:
    """Block (in a worker thread) until the FlinkDeployment is fully deleted.

    Reuses :meth:`KubernetesFlinkProvider._wait_for_deletion` (60s timeout per existing
    convention). Returns ``True`` when termination is confirmed, ``False`` on timeout/error
    (logged, never raised). A False return must short-circuit cleanup: the TM may still hold the
    PVC mount and ``rm -rf`` against a live mount corrupts checkpoint state.
    """
    try:
        await asyncio.to_thread(provider._wait_for_deletion, deployment_name, 60)
        return True
    except Exception as e:
        logger.warning(
            "FlinkDeployment did not terminate within timeout; skipping cleanup wait",
            extra={"deployment_name": deployment_name, "error": str(e)},
        )
        return False


async def safe_cleanup(
    cleanup_call: Callable[[str], Awaitable[StorageCleanupOutcome]],
    deployment_name: str,
    job_label: str,
) -> str:
    """Run one deployment's storage cleanup and report what actually happened.

    The provider watches the cleanup Job to a terminal state, so a returned status distinguishes
    ``"ok"`` from a Job that **refused** — since §22 the script fails closed, and a refusal means
    the state survived and the next start will restore from checkpoints the operator believes are
    gone. For correlation-job that silently breaks the "no replay of events that landed while the
    job was paused" runtime contract, so the reason travels back to the caller AND to the activity
    log at ``severity="warning"``.

    Never raises: a surviving state directory is recoverable, a failed stop is not. An exception
    (the API refusing the Job object at all) stays ``"error"``, with the verbose K8s detail in the
    structured log rather than in the API response.

    Callers set ``"skipped: wait_timeout"`` themselves when termination wasn't confirmed; the
    wait-skip path never enters this helper.
    """
    try:
        outcome = await cleanup_call(deployment_name)
    except Exception as e:
        logger.warning(
            "Deployment storage cleanup failed; checkpoint state may persist",
            extra={"job": job_label, "deployment_name": deployment_name, "error": str(e)},
        )
        return "error"

    if outcome.ok:
        return "ok"

    logger.warning(
        "Deployment storage cleanup did not complete; checkpoint state still exists",
        extra={
            "job": job_label,
            "deployment_name": deployment_name,
            "status": outcome.status,
            "reason": outcome.reason,
        },
    )
    await activity_producer.log_action(
        action="error",
        entity_type="flink",
        entity_id=deployment_name,
        entity_name=deployment_name,
        details=(
            f"{job_label} state cleanup {outcome.status}: {outcome.reason or 'no reason reported'}. "
            "The state directory still exists, so the next start restores from it instead of "
            "cold-starting."
        ),
        source="system",
        severity="warning",
    )
    return outcome.describe()


async def apply_health_signal(audit_dao: AuditLogDAO, states: Sequence[AttackChainsJobState]) -> None:
    """Set ``health`` / ``last_issue`` on every job state from recent audit activity, in ONE query.

    Looks up warning/error Flink events for all deployment names over the
    ``attack_chains_health_window_seconds`` trailing window and maps the worst severity to a health
    value (``error`` -> ``"error"``, ``warning`` -> ``"degraded"``, none -> ``"healthy"``). The
    window slides with ``now()`` so the signal self-heals once issues stop.

    Batched on purpose: one SSE tick / detail fetch enriches ``2N`` deployment names with one
    ``recent_health`` call, never N calls. The paginated list does not apply this signal (the UI
    reads ``overall``, not ``health``). Best-effort — any failure (DB unavailable, query error) is
    logged and the job states keep their default ``health="healthy"``.
    """
    states_by_name = {state.deployment_name: state for state in states}
    if not states_by_name:
        return
    try:
        summary = await audit_dao.recent_health(
            entity_ids=list(states_by_name.keys()),
            window_seconds=settings.attack_chains_health_window_seconds,
        )
    except Exception as e:
        if ErrorTracker.should_log("attack_chains_health_signal"):
            logger.warning(
                "Skipping attack-chains health signal; leaving health=healthy",
                extra={"error": str(e)},
            )
        return

    for name, job_state in states_by_name.items():
        entry = summary.get(name)
        if not entry:
            continue
        worst = entry.get("worst_severity")
        job_state.health = "error" if worst == "error" else "degraded"
        details = entry.get("last_issue_details")
        job_state.last_issue = str(details) if details is not None else None


def build_runtime_state(
    config: CorrelationDeploymentConfig,
    observed: tuple[RuntimeK8sState | None, RuntimeK8sState | None],
    *,
    jobs_status: RuntimeStatus,
) -> AttackChainsRuntimeStateResponse:
    """Assemble the per-job + composite runtime state (no DB writes, no health enrichment)."""
    corr, agent = observed
    corr_state = job_state_from_k8s(config.correlation_deployment_name, corr, jobs_status=jobs_status)
    agent_state = job_state_from_k8s(config.agent_deployment_name, agent, jobs_status=jobs_status)
    overall = derive_overall_status(corr_state.status, agent_state.status, jobs_status=jobs_status)
    job_uptime: str | None = None
    if overall == "running":
        started = resolve_runtime_started_at(corr_state, agent_state)
        job_uptime = format_runtime_uptime(int((datetime.now(UTC) - started).total_seconds()))
    return AttackChainsRuntimeStateResponse(
        overall=overall,
        correlation_job=corr_state,
        agent_job=agent_state,
        job_uptime=job_uptime,
    )


def synthetic_stopped_runtime_state(config: CorrelationDeploymentConfig) -> AttackChainsRuntimeStateResponse:
    """Observed state of a stopped pipeline without talking to Kubernetes.

    Both jobs are missing and overall is stopped — the same blend ``build_runtime_state`` produces
    when K8s reports the deployments gone. Mirrors detection pipelines returning ``disabled``
    locally when ``enabled=false``.
    """
    missing: RuntimeK8sState = ("missing", None)
    return build_runtime_state(config, (missing, missing), jobs_status="stopped")


async def cold_start_correlation_job(
    provider: KubernetesFlinkProvider,
    config: CorrelationDeploymentConfig,
) -> str:
    """Delete the correlation FlinkDeployment, WIPE its state, and launch it again.

    The one operation that makes a shrunken ``KAFKA_INPUT_TOPICS`` take effect. Re-rendering the
    spec (:func:`CorrelationPipelinesManager._restart_jobs`) is a ``last-state`` roll, and a
    ``KafkaSource`` restores its **assigned splits from the checkpoint**, not from the rendered
    topic list — so a topic dropped from the list comes back with the state and keeps being
    consumed. Measured three times on flink-sql-connector-kafka-4.0.1-2.0: the enumerator logs
    ``Discovered removed partitions`` and the reader ignores it, because FLIP-27 gives a
    ``SplitEnumerator`` no way to take a split back from a live reader.

    The agent half is deliberately untouched: it consumes this pipeline's own chains-raw topic,
    which no membership edit can change, and cold-starting it would either re-pay the LLM for
    chains it already analysed or skip the ones in flight.

    Returns the storage-cleanup status — ``"ok"``, ``"error"`` or ``"skipped: wait_timeout"``. Any
    value other than ``"ok"`` means the state may have survived, i.e. **the job may restore the
    removed topic's splits anyway**; the caller must surface that rather than report success.

    Removing the LAST member leaves nothing to launch. The delete and the wipe still run — that is
    what stops the departed member's events from being consumed — and the job stays down, which is
    the honest state for a pipeline with no inputs.
    """
    await provider.delete_correlation_job_deployment(config.correlation_deployment_name)
    terminated = await wait_for_deployment_terminated(provider, config.correlation_deployment_name)
    cleanup = (
        await safe_cleanup(
            provider.cleanup_correlation_job_storage,
            config.correlation_deployment_name,
            "correlation-job",
        )
        if terminated
        else "skipped: wait_timeout"
    )
    if not config.input_topics:
        return cleanup
    # Relaunch even when the wipe was skipped: leaving a customer's correlation down is worse than
    # a job that may have restored, and the caller warns about exactly that case.
    await provider.launch_correlation_job_deployment(config)
    return cleanup


async def launch_deployment_pair(
    db: AsyncSession,
    provider: KubernetesFlinkProvider,
    config: CorrelationDeploymentConfig,
    status_row: Any,
) -> AttackChainsRuntimeResponse:
    """Start one correlation pipeline: correlation-job FIRST, agent-job SECOND.

    Order matters: chains-raw must have a producer before its consumer subscribes. If the agent
    half fails we do NOT auto-rollback the correlation half — the caller gets ``status="partial"``
    and can investigate (a transient image-pull on agent shouldn't tear down a healthy
    correlation-job).

    ``status_row`` carries the desired state (``jobs_status``); it is flipped to ``starting`` and
    committed before anything is submitted, and re-read between the two launches so a concurrent
    stop wins instead of leaving an orphaned deployment behind.
    """
    status_row.jobs_status = "starting"
    await db.flush()
    await db.commit()

    try:
        corr_result = await provider.launch_correlation_job_deployment(config)
    except Exception:
        status_row.jobs_status = "stopped"
        await db.flush()
        await db.commit()
        raise

    await db.refresh(status_row)
    if status_row.jobs_status != "starting":
        await provider.delete_correlation_job_deployment(config.correlation_deployment_name)
        return AttackChainsRuntimeResponse(
            status=status_row.jobs_status,
            correlation_job={
                "deployment_name": config.correlation_deployment_name,
                "operation": "delete_requested",
                "previous_operation": corr_result.get("operation"),
            },
            agent_job={
                "deployment_name": config.agent_deployment_name,
                "operation": "skipped",
                "reason": "runtime stop requested",
            },
        )

    try:
        agent_result: dict[str, object] = await provider.launch_agent_job_deployment(config)
    except Exception as e:
        logger.warning(
            "Agent-job launch failed during start; correlation-job left running",
            extra={"error": str(e), "deployment_name": config.agent_deployment_name},
        )
        return AttackChainsRuntimeResponse(
            status="partial",
            correlation_job=corr_result,
            agent_job={"error": str(e)},
        )

    await db.refresh(status_row)
    if status_row.jobs_status != "starting":
        await provider.delete_agent_job_deployment(config.agent_deployment_name)
        await provider.delete_correlation_job_deployment(config.correlation_deployment_name)
        return AttackChainsRuntimeResponse(
            status=status_row.jobs_status,
            correlation_job={
                "deployment_name": config.correlation_deployment_name,
                "operation": "delete_requested",
            },
            agent_job={
                "deployment_name": config.agent_deployment_name,
                "operation": "delete_requested",
            },
        )

    return AttackChainsRuntimeResponse(status="starting", correlation_job=corr_result, agent_job=agent_result)


async def teardown_deployment_pair(
    db: AsyncSession,
    provider: KubernetesFlinkProvider,
    config: CorrelationDeploymentConfig,
    status_row: Any,
    *,
    error_key: str,
) -> AttackChainsRuntimeResponse:
    """Stop one correlation pipeline: agent-job FIRST (drains chains-raw), correlation-job SECOND.

    After both FlinkDeployments are torn down, wipe each job's checkpoint / HA / savepoint state on
    the shared PVCs via a busybox cleanup-Job. This forces the next start into a cold start so the
    ``KafkaOffsetsInitializer.latest()`` initial position applies (runtime contract HoS 2026-05-27 —
    "no replay of events that landed while the job was paused"). Cleanup failure does NOT fail the
    stop — Kafka offsets surviving is recoverable, a failed stop is not.

    Per-deployment ``cleanup`` status in the response is one of ``"ok"`` (cleanup-Job dispatched),
    ``"error"`` (cleanup raised; verbose reason in the structured log), or
    ``"skipped: wait_timeout"`` (the FlinkDeployment didn't terminate within the 60s wait, so we
    deliberately do NOT dispatch ``rm -rf`` against a possibly-live PVC mount — the next stop
    retries).
    """
    status_row.jobs_status = "stopping"
    await db.flush()
    await db.commit()

    agent_deleted = await provider.delete_agent_job_deployment(config.agent_deployment_name)
    corr_deleted = await provider.delete_correlation_job_deployment(config.correlation_deployment_name)

    # Wait for both FlinkDeployments to fully terminate before wiping PVC state — cleanup-Job's
    # busybox cannot rm -rf while a TaskManager pod still holds the mount. Run waits in parallel
    # (the two jobs are independent K8s objects). ``return_exceptions=True`` is defensive: the
    # helper swallows internally today, but we don't want a future refactor that re-raises to
    # silently abort the sibling wait.
    wait_targets = [
        (config.correlation_deployment_name, provider.cleanup_correlation_job_storage, "correlation-job"),
        (config.agent_deployment_name, provider.cleanup_agent_job_storage, "agent-job"),
    ]
    wait_results = await asyncio.gather(
        *(wait_for_deployment_terminated(provider, name) for name, _, _ in wait_targets),
        return_exceptions=True,
    )

    # Per-deployment decision: only dispatch cleanup when termination was confirmed. A False
    # (timeout) or an exception (defensive guard above) means the TM may still hold the PVC mount —
    # running rm -rf against a live mount is the corruption vector this whole feature avoids.
    cleanup_coros: list[Awaitable[str]] = []
    statuses: list[str | None] = []
    for (deployment_name, cleanup_call, label), result in zip(wait_targets, wait_results, strict=True):
        terminated = isinstance(result, bool) and result
        if terminated:
            statuses.append(None)  # placeholder; filled after gather
            cleanup_coros.append(safe_cleanup(cleanup_call, deployment_name, label))
        else:
            statuses.append("skipped: wait_timeout")
    cleanup_results = await asyncio.gather(*cleanup_coros) if cleanup_coros else []
    # Stitch cleanup results back into the per-deployment slots.
    cleanup_iter = iter(cleanup_results)
    statuses = [next(cleanup_iter) if s is None else s for s in statuses]
    corr_cleanup, agent_cleanup = statuses

    observed = await read_runtime_k8s_state(config, error_key=error_key)
    state = build_runtime_state(config, observed, jobs_status=status_row.jobs_status)
    if state.overall == "stopped":
        status_row.jobs_status = "stopped"
        await db.flush()
        await db.commit()

    return AttackChainsRuntimeResponse(
        status=state.overall,
        correlation_job={
            "deployment_name": config.correlation_deployment_name,
            "operation": "deleted" if corr_deleted else "not_found",
            "cleanup": corr_cleanup,
        },
        agent_job={
            "deployment_name": config.agent_deployment_name,
            "operation": "deleted" if agent_deleted else "not_found",
            "cleanup": agent_cleanup,
        },
    )


@dataclass(frozen=True)
class PipelineRuntimeSnapshot:
    """One correlation pipeline's observed runtime, as one SSE dashboard tick sees it."""

    correlation_pipeline_id: UUID
    name: str
    jobs_status: str
    runtime_state: AttackChainsRuntimeStateResponse


class CorrelationPipelinesManager:
    """CRUD, composite lifecycle and chains-config publishing for N correlation pipelines.

    Everything is scoped by row: deployment names, Kafka topics, resources and activity entity ids
    all come from ``correlation_pipelines`` rather than from ``settings``. Settings survive only as
    the defaults a NULL column resolves to at render time.

    Args:
        db: Async SQLAlchemy session; the manager owns its transaction boundaries.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = CorrelationPipelineDAO(db)
        self.pipeline_dao = PipelineDAO(db)
        self.audit_dao = AuditLogDAO(db)
        # Owns the per-pipeline deny-list table (§20); read here, written by ActiveThreatsManager.
        self.active_threats_dao = ActiveThreatDAO(db)

    # -------------------------------------------------------------------------
    # CRUD
    # -------------------------------------------------------------------------

    async def list_pipelines(self, pagination: dict) -> CorrelationPipelineListResponse:
        """One page of correlation pipelines with their chain counts and observed runtime.

        The page costs a fixed number of statements regardless of size: the chain counts arrive
        with the rows from ``list_with_chain_counts``'s single ``GROUP BY
        correlation_pipeline_id`` aggregate, never one count query per row.

        Observed ``runtime_state`` is the detection-pipeline list pattern: commit (return the
        pooled connection) then resolve status without holding a DB session. Stopped rows skip
        Kubernetes entirely; starting/stopping rows hit a 15s in-process cache
        (:mod:`apps.managers.correlation_runtime_status`) and observe FlinkDeployments only on
        miss, under ``correlation_dashboard_k8s_concurrency``.
        """
        rows, total = await self.dao.list_with_chain_counts(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            sort=pagination["sort"],
            order=pagination["order"] or "asc",
        )
        page_rows = list(rows)
        # ``expire_on_commit=False``: already-loaded attributes stay readable after the commit.
        await self.db.commit()
        states = await self._runtime_states_for_list([row for row, _ in page_rows])
        return CorrelationPipelineListResponse(
            total=total,
            limit=pagination["limit"],
            offset=pagination["offset"],
            sort=pagination["sort"] or "",
            order=pagination["order"] or "asc",
            data=[
                self._to_list_item(row, chain_count, runtime_state=state)
                for (row, chain_count), state in zip(page_rows, states, strict=True)
            ],
        )

    async def get(self, pipeline_id: UUID) -> CorrelationPipelineResponse:
        """One correlation pipeline with its members, chain count and observed runtime state.

        Live Kubernetes read (then write-through to the 15s list cache). Unlike the list it also
        applies the audit health signal and can flip ``jobs_status`` from ``stopping`` to
        ``stopped`` when both deployments are gone.

        Raises:
            NotFoundError: No such correlation pipeline.
        """
        row = await self._require_row(pipeline_id)
        return await self.to_response(
            row,
            attack_chains_count=await self.dao.count_chains(pipeline_id),
            runtime_state=await self._runtime_state(row),
        )

    async def get_default_pipeline_id(self) -> UUID:
        """The ``Default`` correlation pipeline — the one target of the deprecated singleton API.

        Resolved **by name**, the same rule the data migration used, so the legacy endpoints keep
        addressing the migrated runtime rather than "the only row": a second tenant must not turn
        every legacy call into a 409, and must not silently redirect one either.

        Nothing is cached. The row can be renamed or deleted between two calls, and an id that no
        longer resolves is worse than one extra indexed lookup on a route that is already
        deprecated.

        Raises:
            NotFoundError: No such row — the migration has not run, or the pipeline was renamed or
                deleted. The per-pipeline API is then the only way in.
        """
        for name in DEFAULT_CORRELATION_PIPELINE_NAMES:
            row = await self.dao.get_by_name(name)
            if row is not None:
                return row.id
        raise NotFoundError("No default correlation pipeline; use /api/v1/correlation_pipelines")

    async def create(
        self,
        body: CorrelationPipelineCreateRequest,
        current_user: User,
        *,
        activity_details: str | None = None,
    ) -> CorrelationPipelineResponse:
        """Create a correlation pipeline: allocate names, provision topics, publish initial config.

        Never launches a Flink job. The UI's "Create & Run" split button is a two-call flow — plain
        Create is ``POST /api/v1/correlation_pipelines`` alone (201, ``jobs_status="stopped"``, no
        Flink object), Create & Run is that call followed by ``POST .../{id}/run``. A
        ``run_after_create`` flag was rejected deliberately: it makes the request partially succeed
        (row created, launch failed) with no honest status code, and forces a ~30s Kubernetes
        round-trip into a 201.

        Order matters and is not negotiable:
          1. INSERT the row (``jobs_status="stopped"``) and flush — we need the UUID to derive names.
          2. Persist the generated deployment names (``corr-``/``agent-<uuid>``).
          3. Provision the three per-pipeline topics. ``TopicProvisioningError`` rolls the
             transaction back and raises ``ServiceUnavailableError``: a pipeline whose topics do not
             exist is unstartable.
          4. Publish the initial chains-config envelope — the compacted topic must carry a value
             before correlation-job's broadcast source subscribes.
          5. Activity: ``action="create"``, ``entity_type="correlation_pipeline"``. Logged exactly
             once, here — ``duplicate`` reuses this path and passes ``activity_details`` rather
             than emitting a second event for the same creation.

        Threat monitoring is opt-out (§20): a new pipeline inherits every non-blocked threat by
        writing ZERO exclusion rows. The exclusion table is never seeded and "the current global
        set" is never materialised — absent means monitored, and that is the whole point of the
        deny-list.

        Raises:
            DuplicateEntityError: ``name`` is already taken.
            NotFoundError: A requested member detection pipeline does not exist.
            ServiceUnavailableError: Kafka topic provisioning failed.
        """
        row = await self._insert_row(body)
        await self._assign_members(row, list(body.pipeline_ids), replace=True)
        await self._provision_topics(row)
        await self.db.commit()

        row = await self._require_row(row.id)
        await self._publish_config(row)
        await self._log_activity(
            row,
            action="create",
            user=current_user,
            details=activity_details or f"User {current_user.email} created correlation pipeline '{row.name}'",
        )
        return await self.to_response(row)

    async def duplicate(self, pipeline_id: UUID, current_user: User) -> CorrelationPipelineResponse:
        """Copy a correlation pipeline's configuration into a new, STOPPED pipeline.

        Frontend contract. It does not "work like copying a regular pipeline" —
        no duplicate capability exists for any pipeline type in this control plane — so the
        semantics are defined here:

          * Copied: ``threshold_pct``, ``correlation_window_seconds``, ``auto_add_new_pipelines``,
            ``auto_add_pipeline_name_pattern``, all ten resource columns (NULLs stay NULL, so the
            copy keeps inheriting the platform defaults rather than freezing today's numbers),
            and the member detection pipelines. Sharing a detection pipeline across correlation
            jobs is allowed; each job independently consumes that member's tagged-events topic.
            The copy is created ``stopped``, so it does not start consuming until an operator
            runs it.
          * Copied: the Active Threat deny-list (§20), 1:1. Zero exclusion rows is the rule for a
            plain ``create`` — opt-out inheritance — but a *config copy* that silently monitored
            threats the source had switched off would not be a copy.
          * NOT copied: identity or runtime — new UUID, new deployment names, new topics,
            ``jobs_status="stopped"``.

        Name: source name + ``" (Copy)"``; if taken, ``" (Copy 2)"``, ``" (Copy 3)"``, … up to 50
        attempts, then ``ConflictError``. The base is truncated so the result fits the ``name``
        column. The uniqueness probe is advisory — a concurrent insert surfaces as the unique
        violation and is translated into ``DuplicateEntityError``.

        Implemented as ``create`` with a pre-filled body, so topic provisioning, the initial
        chains-config publish, the rollback-on-failure semantics and the single activity event are
        shared, not re-implemented. The activity row is a plain ``action="create"`` naming both
        pipelines — no ``duplicate`` action type exists, and one creation must not log twice.

        On ``TopicProvisioningError`` nothing is committed: no ghost row, no activity event, the
        source untouched, and a retry is a plain re-POST (which mints a fresh id).
        """
        source = await self._require_row(pipeline_id)
        source_name = source.name
        copy_name = await self._next_copy_name(source_name)
        body = CorrelationPipelineCreateRequest(
            name=copy_name,
            threshold_pct=source.threshold_pct,
            correlation_window_seconds=source.correlation_window_seconds,
            auto_add_new_pipelines=bool(source.auto_add_new_pipelines),
            auto_add_pipeline_name_pattern=source.auto_add_pipeline_name_pattern,
            pipeline_ids=[member.id for member in (source.members or [])],
            correlation_resources=self._resources_of(source, "correlation"),
            agent_resources=self._resources_of(source, "agent"),
        )
        excluded = (await self._excluded_active_threat_ids([source.id])).get(source.id, [])
        response = await self.create(
            body,
            current_user,
            activity_details=(f"Duplicated correlation pipeline '{source_name}' as '{copy_name}' (created stopped)"),
        )
        if excluded:
            # After `create`, so the copy exists: its initial envelope is then republished with the
            # inherited deny-list rather than an empty one.
            await self.active_threats_dao.set_excluded(excluded, [response.id], excluded=True, user_id=current_user.id)
            await self.db.commit()
            await self.republish_chains_config([response.id], reason="correlation_pipeline_duplicated")
        return response

    async def delete(self, pipeline_id: UUID, current_user: User) -> list[str]:
        """Stop the pipeline, retire its topics, drop the row. Returns non-fatal warnings.

        ``attack_chains`` rows keep their history: the FK is ``ON DELETE SET NULL``, so a chain
        outlives the pipeline that produced it and the list view renders a null
        ``correlation_pipeline_id`` as "(deleted)". Chains are never cascade-deleted — analyst-owned
        fields are the point of that table.

        Per-detection-pipeline ``tagged-events`` topics are NOT touched: they belong to the
        detection-pipeline lifecycle, not to this row.
        """
        row = await self._require_row(pipeline_id)
        name = row.name
        await self.stop(pipeline_id, current_user)

        row = await self._require_row(pipeline_id)
        warnings = await self._retire_topics(row)

        await self.db.delete(row)
        await self.db.flush()
        await self.db.commit()

        await activity_producer.log_action(
            action="delete",
            entity_type="correlation_pipeline",
            entity_id=str(pipeline_id),
            entity_name=name,
            user=current_user,
            details=f"User {current_user.email} deleted correlation pipeline '{name}'",
            source="user",
        )
        return warnings

    async def update(
        self,
        pipeline_id: UUID,
        body: CorrelationPipelineUpdateRequest,
        current_user: User,
        *,
        confirm_restart: bool = False,
    ) -> CorrelationPipelineResponse:
        """Apply an edit, honouring the edit-safety matrix.

        Hot fields are published to the row's chains-config topic and never restart anything.
        Resource and membership changes need a new FlinkDeployment spec, which is a ~30s
        correlation gap for a paying customer's tenant, so they require an explicit
        ``confirm_restart`` while the jobs are up. When ``jobs_status == "stopped"`` there is
        nothing to interrupt and the change applies silently — the new spec is picked up at the
        next start.

        ``member_pipeline_ids`` feeds ``KAFKA_INPUT_TOPICS`` (restart). Publishing the hot
        envelope happens first on purpose, so the restarted job reads a config that already
        matches its new input set (threshold / window / AT deny-list).

        Raises:
            ConflictError: A restart-class field changed while the jobs are up and
                ``confirm_restart`` is false.
            BadRequestError: An immutable field reached the manager.
        """
        row = await self._require_row(pipeline_id)
        incoming = self._flatten_update(body)

        immutable = set(incoming) & IMMUTABLE_FIELDS
        if immutable:
            raise BadRequestError(f"{sorted(immutable)[0]} is immutable")

        changes = await self._diff(row, incoming)
        if not changes:
            return await self.to_response(row)

        changed = set(changes)
        restart_needed: dict[RestartTarget, bool] = {
            "correlation": bool(changed & CORRELATION_RESTART_FIELDS),
            "agent": bool(changed & AGENT_RESTART_FIELDS),
        }
        # `starting` is the DB's desired-state value for "the operator wants both jobs up"
        # (models.py CHECK allows only starting/stopping/stopped). The surfaced status enum is
        # expose-correlation-pipelines-rest-api.md's.
        jobs_running = row.jobs_status == "starting"
        # A member REMOVAL cannot be applied by re-rendering the spec: the restored source keeps
        # reading the departed member's topic (see `cold_start_correlation_job`). It costs this
        # pipeline's in-flight chains, so the operator is told before it happens, not after.
        members_removed = _removed_members(changes)
        if jobs_running and any(restart_needed.values()) and not confirm_restart:
            raise ConflictError(
                "Changing "
                + ", ".join(sorted(changed & (CORRELATION_RESTART_FIELDS | AGENT_RESTART_FIELDS)))
                + (
                    " removes a member, which COLD-STARTS the correlation job: its in-flight chain "
                    "buffers are wiped and correlation restarts from the current offsets. A "
                    "last-state roll would leave the removed member's events flowing into this "
                    "pipeline. Re-send with confirm_restart=true."
                    if members_removed
                    else " restarts a running job. Re-send with confirm_restart=true."
                )
            )

        await self._apply_changes(row, incoming, changed, current_user)
        # Reloads ``members`` because the membership write expired the collection
        # (:meth:`_write_members`) — the three derivations below all read it.
        row = await self._require_row(pipeline_id)

        warnings: list[str] = []
        if changed & (CHAINS_CONFIG_HOT_FIELDS | RELATION_FIELDS):
            await self._publish_config(row)
        if "correlation_window_seconds" in changed:
            warnings.append(_WINDOW_REBUCKET_WARNING)

        restarted: list[RestartTarget] = []
        if jobs_running:
            targets = [job for job, needed in restart_needed.items() if needed]
            if members_removed and "correlation" in targets:
                targets.remove("correlation")
                warnings.extend(await self._cold_start_correlation(row, members_removed))
                restarted.append("correlation")
            restarted += await self._restart_jobs(row, targets)

        await self._log_activity(
            row,
            action="update",
            user=current_user,
            details=f"User {current_user.email} updated correlation pipeline '{row.name}'",
            changes=changes,
        )
        response = await self.to_response(row)
        response.warnings = warnings
        response.restarted = restarted
        return response

    # -------------------------------------------------------------------------
    # Runtime lifecycle
    # -------------------------------------------------------------------------

    async def start(self, pipeline_id: UUID, current_user: User | None = None) -> AttackChainsRuntimeResponse:
        """Start this pipeline's job pair: correlation-job FIRST, agent-job SECOND.

        A failed agent launch leaves the correlation half running and reports ``partial``; there is
        no auto-rollback.

        Raises:
            ConfigurationError: The pipeline resolves to no input topics — no member owns a
                tagged-events topic yet. Nothing is submitted (:func:`assert_startable`).
        """
        provider = require_kubernetes_provider()
        row = await self._require_row(pipeline_id)
        assert_startable(row)
        # The running job reads its parameters from the broadcast, so publish before launching:
        # a cold-started matcher must not correlate against a stale compacted record.
        await self._publish_config(row)
        config = deployment_config_for_row(row)
        correlation_runtime_status_cache.invalidate(pipeline_id)

        response = await launch_deployment_pair(self.db, provider, config, row)

        if current_user is not None and response.status in {"starting", "partial"}:
            verb = "started" if response.status == "starting" else "partially started"
            await self._log_activity(
                row,
                action="start",
                user=current_user,
                details=f"User {current_user.email} {verb} correlation pipeline '{row.name}'",
            )
        return response

    async def stop(self, pipeline_id: UUID, current_user: User | None = None) -> AttackChainsRuntimeResponse:
        """Stop this pipeline's job pair: agent-job FIRST, correlation-job SECOND, then cleanup.

        Idempotent — a pipeline that is already stopped tears down nothing and reports ``stopped``,
        which is what makes ``delete`` safe to call unconditionally.
        """
        provider = require_kubernetes_provider()
        row = await self._require_row(pipeline_id)
        config = deployment_config_for_row(row)
        correlation_runtime_status_cache.invalidate(pipeline_id)

        response = await teardown_deployment_pair(self.db, provider, config, row, error_key=str(row.id))

        if current_user is not None:
            await self._log_activity(
                row,
                action="stop",
                user=current_user,
                details=f"User {current_user.email} stopped correlation pipeline '{row.name}'",
            )
        return response

    async def get_runtime_state(self, pipeline_id: UUID) -> AttackChainsRuntimeStateResponse:
        """Per-job K8s state + composite overall + uptime, with the window-based health signal."""
        row = await self._require_row(pipeline_id)
        return await self._runtime_state(row)

    async def get_runtime_snapshot(self, row: CorrelationPipeline) -> dict[str, str] | None:
        """Return ``job_uptime`` (e.g. ``1h 2m 3s``) when both of this row's jobs are RUNNING."""
        if row.jobs_status != "starting":
            return None
        config = deployment_config_for_row(row)
        observed = await read_runtime_k8s_state(config, error_key=str(row.id))
        if observed[0] is None or observed[1] is None:
            return None
        state = build_runtime_state(config, observed, jobs_status="starting")
        if state.overall != "running" or state.job_uptime is None:
            return None
        return {"job_uptime": state.job_uptime}

    async def list_runtime_snapshots(
        self, correlation_pipeline_ids: Sequence[UUID] | None = None
    ) -> list[PipelineRuntimeSnapshot]:
        """Observed runtime of correlation pipelines (Active Threat detail chips, tests).

        The paginated list does not call this: it resolves ``runtime_state`` via the 15s cache
        (:meth:`_runtime_states_for_list`). This path still writes through to that cache so a warm
        observation serves the list without a second Kubernetes round-trip.

        Kubernetes reads run under a bounded semaphore — N pipelines must never mean N API-server
        connections. The caller is responsible for closing its DB session before awaiting this
        (the Kubernetes round-trips must not hold a connection).

        Args:
            correlation_pipeline_ids: Restrict the observation to these pipelines. ``None``
                observes every row (up to the fan-out limit).
        """
        rows = await self.dao.list_with_members(limit=_PIPELINE_FANOUT_LIMIT)
        if correlation_pipeline_ids is not None:
            wanted = set(correlation_pipeline_ids)
            rows = [row for row in rows if row.id in wanted]
        if not rows:
            return []

        semaphore = asyncio.Semaphore(settings.correlation_dashboard_k8s_concurrency)

        async def _observe(row: CorrelationPipeline) -> PipelineRuntimeSnapshot:
            config = deployment_config_for_row(row)
            async with semaphore:
                observed = await read_runtime_k8s_state(config, error_key=str(row.id))
            return PipelineRuntimeSnapshot(
                correlation_pipeline_id=row.id,
                name=row.name,
                jobs_status=row.jobs_status,
                runtime_state=build_runtime_state(config, observed, jobs_status=row.jobs_status),
            )

        snapshots = await asyncio.gather(*(_observe(row) for row in rows))
        # ONE health query for all 2N deployment names, never one per pipeline.
        await apply_health_signal(
            self.audit_dao,
            [
                job_state
                for snapshot in snapshots
                for job_state in (snapshot.runtime_state.correlation_job, snapshot.runtime_state.agent_job)
            ],
        )
        for snapshot in snapshots:
            correlation_runtime_status_cache.put(
                snapshot.correlation_pipeline_id, snapshot.jobs_status, snapshot.runtime_state
            )
        return list(snapshots)

    # -------------------------------------------------------------------------
    # Membership entry points and republish
    # -------------------------------------------------------------------------

    async def get_ids_by_member(self, pipeline_id: UUID) -> list[UUID]:
        """Correlation pipelines containing ``pipeline_id`` — zero or more.

        A destination-topic rename or tagged-events assign must fan out to every owner: each
        independently consumes that member's tagged-events topic.
        """
        return await self.dao.get_correlation_pipeline_ids_for_pipeline(pipeline_id)

    async def restart_if_running(
        self,
        correlation_pipeline_id: UUID,
        user: User | None = None,
        *,
        cold_start: bool = False,
    ) -> None:
        """Re-render the correlation deployment with the row's current member topic list.

        No-op unless ``jobs_status == "starting"``. Never starts a stopped job, never touches the
        agent job (its spec does not depend on membership), never changes the uptime timestamp.

        ``cold_start=True`` when the caller's edit REMOVED a topic from ``KAFKA_INPUT_TOPICS`` —
        a member was deleted, or an existing member was moved off a topic it used to read. A
        re-render cannot apply that: the restored source keeps consuming the departed topic
        (:func:`cold_start_correlation_job`). It is not the default because an addition, a resource
        change and a rename are all correctly served by the cheap roll.
        """
        row = await self.dao.get_with_relations(correlation_pipeline_id)
        if row is None or row.jobs_status != "starting":
            return
        if cold_start:
            await self._cold_start_correlation(row, removed_members=[])
            return
        await self._restart_jobs(row, ["correlation"], user=user)

    async def add_member(self, correlation_pipeline_id: UUID, pipeline_id: UUID, user: User) -> None:
        """Attach a detection pipeline to a correlation pipeline.

        Raises:
            NotFoundError: Either row does not exist.
            BadRequestError: The detection pipeline has no dedicated tagged-events topic yet. One is
                never assigned implicitly — assigning one restarts that customer's own pipeline, and
                that is the detection-pipeline lifecycle's decision to make, not this one's.
        """
        row = await self._require_row(correlation_pipeline_id)
        pipeline = await self.pipeline_dao.get_by_id(pipeline_id)
        if pipeline is None:
            raise NotFoundError(f"Pipeline with id {pipeline_id} not found")
        if not pipeline.tagged_events_topic:
            raise BadRequestError(
                f"Pipeline '{pipeline.name}' has no dedicated tagged-events topic yet — "
                "assign one first (this restarts the pipeline)"
            )

        existing = [member.id for member in (row.members or [])]
        if pipeline_id in existing:
            return
        await self._write_members(row, [*existing, pipeline_id])
        await self.db.commit()

        row = await self._require_row(correlation_pipeline_id)
        await self._publish_config(row)
        await self._restart_jobs(row, ["correlation"], user=user)
        await self._log_activity(
            row,
            action="update",
            user=user,
            details=f"User {user.email} added '{pipeline.name}' to correlation pipeline '{row.name}'",
            changes={"member_pipeline_ids": {"added": str(pipeline_id)}},
        )

    async def auto_add_member(self, pipeline_id: UUID, user: User) -> list[UUID]:
        """Attach a NEW detection pipeline to every correlation pipeline that opted in.

        Selection is ``auto_add_new_pipelines IS TRUE`` AND the pattern matches the detection
        pipeline's **name** — the shared glob matcher in ``apps/core/constants.py``
        (fnmatchcase; NULL/blank pattern matches everything, which is the pre-epic behaviour).
        Destination topics are SIEM output and are not consulted.

        Every matching correlation pipeline is attached: sharing a detection pipeline is allowed,
        and two opted-in jobs both consuming the same tagged-events topic is expected (duplicate
        chains). Each attachment is a membership change — republish + restart-if-running — and is
        EXEMPT from ``confirm_restart`` (there is no interactive operator), but it MUST emit an
        activity event naming the added member. One correlation-side failure is logged and does
        not abort the remaining attachments.

        Returns:
            The correlation pipeline ids whose membership changed.
        """
        pipeline = await self.pipeline_dao.get_by_id(pipeline_id)
        if pipeline is None:
            return []

        candidates = [
            row
            for row in await self.dao.list_with_members(limit=_PIPELINE_FANOUT_LIMIT)
            if bool(row.auto_add_new_pipelines)
            and pipeline_name_matches_auto_add_pattern(row.auto_add_pipeline_name_pattern, pipeline.name)
        ]
        candidates.sort(key=lambda row: (row.created_at, str(row.id)))
        if not candidates:
            return []
        if not pipeline.tagged_events_topic:
            logger.warning(
                "Skipping auto-add: the new detection pipeline has no dedicated tagged-events topic yet",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "correlation_pipeline_ids": [str(row.id) for row in candidates],
                },
            )
            return []

        for target in candidates:
            await self._write_members(target, [*[member.id for member in (target.members or [])], pipeline_id])
        await self.db.commit()

        changed: list[UUID] = []
        for target in candidates:
            try:
                row = await self._require_row(target.id)
                await self._publish_config(row)
                await self._restart_jobs(row, ["correlation"], user=user)
                await self._log_activity(
                    row,
                    action="update",
                    user=user,
                    details=f"Auto-added '{pipeline.name}' to correlation pipeline '{row.name}'",
                    changes={"member_pipeline_ids": {"added": str(pipeline_id)}},
                )
                changed.append(row.id)
            except Exception as error:
                logger.warning(
                    "Failed to finish auto-add on a correlation pipeline (membership already written)",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "correlation_pipeline_id": str(target.id),
                        "error": str(error),
                    },
                )
        return changed

    async def republish_chains_config(self, correlation_pipeline_ids: Iterable[UUID], *, reason: str) -> None:
        """Rebuild and publish the chains-config envelope for the given pipelines. Never restarts.

        Best-effort per pipeline: one failure is logged with ``reason`` and does not abort the rest.

        Call sites are hot — no Flink object is touched:

          * Active Threat monitoring edits (``ActiveThreatsManager``);
          * Active Threat catalogue sync after ``ON DELETE CASCADE`` shrinks a deny-list;
          * duplicate (the copy's first envelope).
        """
        for correlation_pipeline_id in correlation_pipeline_ids:
            try:
                row = await self.dao.get_with_relations(correlation_pipeline_id)
                if row is None:
                    continue
                await self._publish_config(row)
            except Exception as e:
                logger.warning(
                    "Failed to republish chains-config for a correlation pipeline",
                    extra={"correlation_pipeline_id": str(correlation_pipeline_id), "reason": reason, "error": str(e)},
                )

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    async def _require_row(self, pipeline_id: UUID) -> CorrelationPipeline:
        row = await self.dao.get_with_relations(pipeline_id)
        if row is None:
            raise NotFoundError(f"Correlation pipeline with id {pipeline_id} not found")
        return row

    async def _insert_row(self, body: CorrelationPipelineCreateRequest) -> CorrelationPipeline:
        """INSERT + flush + persist the generated deployment names, in one transaction."""
        if await self.dao.get_by_name(body.name) is not None:
            raise DuplicateEntityError("Correlation pipeline", f"name '{body.name}'")

        row = CorrelationPipeline(
            name=body.name,
            jobs_status="stopped",
            threshold_pct=body.threshold_pct,
            correlation_window_seconds=body.correlation_window_seconds,
            auto_add_new_pipelines=bool(body.auto_add_new_pipelines),
            auto_add_pipeline_name_pattern=body.auto_add_pipeline_name_pattern,
            **self._resource_columns(body.correlation_resources, "correlation"),
            **self._resource_columns(body.agent_resources, "agent"),
        )
        self.db.add(row)
        try:
            await self.db.flush()
        except IntegrityError as exc:
            await self.db.rollback()
            raise DuplicateEntityError("Correlation pipeline", f"name '{body.name}'") from exc

        # The UUID exists only after the flush, and a deployment name is derived from it — never
        # from the user-supplied name, which is mutable and not DNS-1123.
        row.correlation_deployment_name = correlation_deployment_name(row.id)
        row.agent_deployment_name = agent_deployment_name(row.id)
        await self.db.flush()
        return row

    async def _assign_members(self, row: CorrelationPipeline, pipeline_ids: list[UUID], *, replace: bool) -> None:
        """Validate and write membership inside the caller's transaction."""
        if not replace and not pipeline_ids:
            return
        unique_ids = list(dict.fromkeys(pipeline_ids))
        if unique_ids:
            found = await self.pipeline_dao.get_by_ids(unique_ids)
            missing = set(unique_ids) - {pipeline.id for pipeline in found}
            if missing:
                raise NotFoundError(f"Pipeline with id {sorted(str(m) for m in missing)[0]} not found")
        await self._write_members(row, unique_ids)

    async def _write_members(self, row: CorrelationPipeline, pipeline_ids: list[UUID]) -> None:
        """The ONE membership write of this manager — the only place that may call ``set_members``.

        ``CorrelationPipelineDAO.set_members`` replaces the association table with **core**
        ``DELETE``/``INSERT`` statements, which the ORM never sees. The session is
        ``expire_on_commit=False`` (``apps/core/database.py``, deliberately) and a second
        ``selectinload`` does NOT overwrite an already-loaded collection: the identity map hands the
        same instance back as it stands. So without this expire, everything derived from
        ``row.members`` after a write is one edit behind — the deployment's ``KAFKA_INPUT_TOPICS``
        and the response's ``members[]``. On a *removal* that is a governance failure (§17): the
        job would be restarted still reading the departed member's topic.

        The invalidation lives at the raw write rather than at the three call sites so the next
        caller cannot forget it.
        """
        await self.dao.set_members(row.id, pipeline_ids)
        # Synchronous on AsyncSession, like ``add``: it only marks the collection for reload.
        self.db.expire(row, ["members"])

    async def _provision_topics(self, row: CorrelationPipeline) -> None:
        """Create the row's three topics; a failure rolls the whole create back.

        The manager never touches Kafka directly — ``KafkaTopicLifecycleService`` owns every
        broker admin call, and ``settings.kafka_topic_delete_policy`` owns the retirement side.
        """
        try:
            await topic_lifecycle_service.ensure_correlation_pipeline_topics(row.id)
        except TopicProvisioningError as e:
            await self.db.rollback()
            # The rolled-back id is discarded forever and a retry mints a new one, so anything
            # ``ensure_*`` already created under it is never re-adopted. chains-raw/-analysed age
            # out on retention, but chains-config is compacted with retention.ms=-1 and would
            # linger: name all three so an operator can reap them. No compensating delete here —
            # kafka_topic_delete_policy owns deletion, and every broker admin call belongs to the
            # topic lifecycle service, never to this manager.
            logger.warning(
                "Rolling back correlation pipeline creation: topic provisioning failed; "
                "any topics created under the discarded id are empty and orphaned",
                extra={
                    "correlation_pipeline_id": str(row.id),
                    "orphaned_topics": [
                        chains_config_topic_for(row.id),
                        chains_raw_topic_for(row.id),
                        chains_analysed_topic_for(row.id),
                    ],
                    "error": str(e),
                },
            )
            raise ServiceUnavailableError(
                "Kafka topics for the correlation pipeline could not be created; the pipeline was not created"
            ) from e

    async def _retire_topics(self, row: CorrelationPipeline) -> list[str]:
        """Retire the row's topics; never blocks the delete.

        The observable outcome is ``settings.kafka_topic_delete_policy``'s decision, not this
        manager's: ``keep`` makes no admin call, ``retire`` (the default) flips the topic to
        ``cleanup.policy=delete`` with the retire retention so the broker ages the data out, and
        ``delete`` really deletes. The consumer-lag drain wait, the downgrade-to-retire rule and the
        shared-topic refusal all live in the service.
        """
        warnings: list[str] = []
        try:
            outcomes = await topic_lifecycle_service.retire_correlation_pipeline_topics(row.id)
        except TopicProvisioningError as e:
            logger.warning(
                "Correlation pipeline topics could not be retired; the row is still deleted",
                extra={"correlation_pipeline_id": str(row.id), "error": str(e)},
            )
            await activity_producer.log_action(
                action="error",
                entity_type="kafka",
                entity_id=str(row.id),
                entity_name=row.name,
                details=f"Failed to retire topics of correlation pipeline '{row.name}': {e}",
                source="system",
                severity="error",
            )
            return [f"Kafka topics were not retired: {e}"]

        for outcome in outcomes:
            if outcome.action != outcome.requested_policy:
                warnings.append(
                    f"{outcome.topic}: {outcome.requested_policy} downgraded to {outcome.action}"
                    + (f" ({outcome.reason})" if outcome.reason else "")
                )
        if warnings:
            await activity_producer.log_action(
                action="delete",
                entity_type="kafka",
                entity_id=str(row.id),
                entity_name=row.name,
                details="; ".join(warnings),
                source="system",
                severity="warning",
            )
        return warnings

    async def _next_copy_name(self, source_name: str) -> str:
        """``<name> (Copy)``, ``(Copy 2)``, … — the first that is free, truncated to fit the column.

        The slice length is computed from the suffix actually being appended, so the result is
        always within the column limit with no separate length check, and the first attempt
        reduces to ``source_name[:248] + " (Copy)"``. Truncation can itself collide (two long
        sources sharing a prefix), which is why the probe runs on the truncated candidate rather
        than on the raw name; ``.rstrip()`` keeps a cut landing on whitespace from producing
        ``"foo  (Copy)"``. The probe is advisory — a concurrent insert still surfaces as the
        ``name`` unique violation and is translated in :meth:`_insert_row`.
        """
        for attempt in range(1, _DUPLICATE_SUFFIX_ATTEMPTS + 1):
            suffix = " (Copy)" if attempt == 1 else f" (Copy {attempt})"
            base = source_name[: _NAME_MAX_LENGTH - len(suffix)].rstrip()
            candidate = f"{base}{suffix}"
            if await self.dao.get_by_name(candidate) is None:
                return candidate
        raise ConflictError(
            f"Could not derive a free name for a copy of '{source_name}' after "
            f"{_DUPLICATE_SUFFIX_ATTEMPTS} attempts; rename the source or delete old copies"
        )

    async def _runtime_states_for_list(
        self, rows: Sequence[CorrelationPipeline]
    ) -> list[AttackChainsRuntimeStateResponse]:
        """Resolve observed runtime for one list page: cache first, Kubernetes only on a miss.

        Stopped rows never leave the process. Starting/stopping rows share
        ``correlation_dashboard_k8s_concurrency`` so a 25-row page cannot burst 50 API-server
        calls. No health query here — the list UI reads ``overall``, not ``health``.
        """
        semaphore = asyncio.Semaphore(settings.correlation_dashboard_k8s_concurrency)

        async def _one(row: CorrelationPipeline) -> AttackChainsRuntimeStateResponse:
            config = deployment_config_for_row(row)
            if row.jobs_status == "stopped":
                return list_chip_runtime_state(synthetic_stopped_runtime_state(config))
            cached = correlation_runtime_status_cache.get(row.id, row.jobs_status)
            if cached is not None:
                return list_chip_runtime_state(cached)
            async with semaphore:
                observed = await read_runtime_k8s_state(config, error_key=str(row.id))
            state = build_runtime_state(config, observed, jobs_status=row.jobs_status)
            correlation_runtime_status_cache.put(row.id, row.jobs_status, state)
            return list_chip_runtime_state(state)

        return list(await asyncio.gather(*(_one(row) for row in rows)))

    def _to_list_item(
        self,
        row: CorrelationPipeline,
        attack_chains_count: int,
        *,
        runtime_state: AttackChainsRuntimeStateResponse | None = None,
    ) -> CorrelationPipelineListItem:
        """Row -> list row. ``runtime_state`` is filled by :meth:`list_pipelines` after the commit."""
        return CorrelationPipelineListItem(
            id=row.id,
            name=row.name,
            jobs_status=row.jobs_status,
            runtime_state=runtime_state,
            members=self._members_of(row),
            destination_topics=self._destination_topics_of(row),
            attack_chains_count=attack_chains_count,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _destination_topics_of(row: CorrelationPipeline) -> list[str]:
        """Unique destination topics of member detection pipelines, sorted for a stable list row."""
        return sorted({topic for member in (row.members or []) for topic in destination_topics_from_pipeline(member)})

    @staticmethod
    def _members_of(row: CorrelationPipeline) -> list[CorrelationPipelineMember]:
        return [
            CorrelationPipelineMember(
                pipeline_id=member.id,
                name=member.name,
                destination_topic=member.destination_topic,
                tagged_events_topic=member.tagged_events_topic,
                enabled=bool(member.enabled),
            )
            for member in sorted(row.members or [], key=lambda member: member.name)
        ]

    @staticmethod
    def _resource_columns(resources: CorrelationPipelineJobResources | None, prefix: str) -> dict[str, Any]:
        """Nested request resources -> row columns. Absent stays NULL so it inherits settings."""
        if resources is None:
            return {}
        return {
            f"{prefix}_{suffix}": getattr(resources, suffix)
            for suffix in _RESOURCE_SUFFIXES
            if getattr(resources, suffix) is not None
        }

    @staticmethod
    def _resources_of(row: CorrelationPipeline, prefix: str) -> CorrelationPipelineJobResources:
        """Row columns -> the nested response/request shape. NULLs stay None (inherit)."""
        return CorrelationPipelineJobResources(
            **{suffix: getattr(row, f"{prefix}_{suffix}") for suffix in _RESOURCE_SUFFIXES}
        )

    @staticmethod
    def _flatten_update(body: CorrelationPipelineUpdateRequest) -> dict[str, Any]:
        """Update request -> ``{matrix field: value}``, only for fields the client actually sent.

        ``exclude_unset`` rather than ``exclude_none``: ``correlation_window_seconds=null`` is a
        meaningful edit (drop the override and inherit each HoS rule's ``detection.timeframe``),
        not an omission.
        """
        sent = body.model_dump(exclude_unset=True)
        flat: dict[str, Any] = {}
        for field, value in sent.items():
            if field in _RESOURCE_GROUPS:
                prefix = _RESOURCE_GROUPS[field]
                for suffix in _RESOURCE_SUFFIXES:
                    if value is not None and suffix in value:
                        flat[f"{prefix}_{suffix}"] = value[suffix]
            elif field == "pipeline_ids":
                flat["member_pipeline_ids"] = list(dict.fromkeys(value or []))
            else:
                flat[field] = value
        return flat

    async def _diff(self, row: CorrelationPipeline, incoming: dict[str, Any]) -> dict[str, dict[str, Any]]:
        """``{field: {"old": …, "new": …}}`` for the fields whose value actually changes."""
        changes: dict[str, dict[str, Any]] = {}
        for field, new in incoming.items():
            if field == "member_pipeline_ids":
                old_ids = sorted(str(member.id) for member in (row.members or []))
                new_ids = sorted(str(pipeline_id) for pipeline_id in new)
                if old_ids != new_ids:
                    changes[field] = {"old": old_ids, "new": new_ids}
                continue
            old = getattr(row, field)
            if field == "auto_add_new_pipelines":
                old, new = bool(old), bool(new)
            if old != new:
                changes[field] = {"old": old, "new": new}
        return changes

    async def _apply_changes(
        self,
        row: CorrelationPipeline,
        incoming: dict[str, Any],
        changed: set[str],
        current_user: User,
    ) -> None:
        for field in changed:
            if field == "member_pipeline_ids":
                await self._assign_members(row, list(incoming[field]), replace=True)
                continue
            setattr(row, field, incoming[field])
        row.updated_by = current_user.id
        await self.db.flush()
        await self.db.commit()

    async def _cold_start_correlation(
        self,
        row: CorrelationPipeline,
        removed_members: list[str],
    ) -> list[str]:
        """Cold-start this row's correlation job after its input topic set shrank.

        Returns the warnings the caller must surface. The first is unconditional — a cold start
        wipes the buffers, so chains that were mid-window when the member left are never emitted —
        and the second only appears when the state wipe did not complete, which means the job may
        have restored the removed member's splits after all.

        No-op unless the jobs are up: a stopped pipeline picks the new member set up at its next
        start, which is a cold start by definition.
        """
        if row.jobs_status != "starting":
            return []
        provider = require_kubernetes_provider()
        config = deployment_config_for_row(row)
        correlation_runtime_status_cache.invalidate(row.id)
        cleanup = await cold_start_correlation_job(provider, config)
        logger.info(
            "Cold-started the correlation job after a member removal",
            extra={
                "correlation_pipeline_id": str(row.id),
                "removed_members": removed_members,
                "cleanup": cleanup,
                "relaunched": bool(config.input_topics),
            },
        )
        warnings = [_MEMBER_REMOVAL_COLD_START_WARNING]
        if cleanup != "ok":
            warnings.append(_COLD_START_CLEANUP_WARNING.format(cleanup=cleanup))
        if not config.input_topics:
            warnings.append(_NO_INPUT_TOPICS_WARNING)
        return warnings

    async def _restart_jobs(
        self,
        row: CorrelationPipeline,
        targets: Sequence[RestartTarget],
        *,
        user: User | None = None,
    ) -> list[RestartTarget]:
        """Re-render the named FlinkDeployments of one row. Only the named side is touched.

        A ``last-state`` roll, which is correct for a resource change or an ADDED member. It is not
        enough for a removal — see :func:`cold_start_correlation_job`.
        """
        if not targets or row.jobs_status != "starting":
            return []
        provider = require_kubernetes_provider()
        config = deployment_config_for_row(row)
        correlation_runtime_status_cache.invalidate(row.id)
        restarted: list[RestartTarget] = []
        for target in targets:
            # An empty KAFKA_INPUT_TOPICS crash-loops the correlation job, so the edit that emptied
            # it must not be pushed to Kubernetes. Raising here is not an option — the edit is
            # already committed — so the old spec is left in place and the caller warns. The agent
            # job has no input topics of its own and is never affected.
            if target == "correlation" and not config.input_topics:
                logger.warning(
                    "Not re-rendering the correlation job: the edit leaves it with no input topics",
                    extra={
                        "correlation_pipeline_id": str(row.id),
                        "unassigned_members": unassigned_member_names(row),
                    },
                )
                continue
            launch = (
                provider.launch_correlation_job_deployment
                if target == "correlation"
                else provider.launch_agent_job_deployment
            )
            await launch(config)
            restarted.append(target)
        if user is not None and restarted:
            logger.info(
                "Re-rendered correlation pipeline deployments",
                extra={"correlation_pipeline_id": str(row.id), "restarted": list(restarted)},
            )
        return restarted

    async def _runtime_state(self, row: CorrelationPipeline) -> AttackChainsRuntimeStateResponse:
        config = deployment_config_for_row(row)
        observed = await read_runtime_k8s_state(config, error_key=str(row.id))
        state = build_runtime_state(config, observed, jobs_status=row.jobs_status)
        if row.jobs_status == "stopping" and state.overall == "stopped":
            row.jobs_status = "stopped"
            await self.db.flush()
            await self.db.commit()
            correlation_runtime_status_cache.invalidate(row.id)
            await apply_health_signal(self.audit_dao, [state.correlation_job, state.agent_job])
            return state
        await apply_health_signal(self.audit_dao, [state.correlation_job, state.agent_job])
        correlation_runtime_status_cache.put(row.id, row.jobs_status, state)
        return state

    async def excluded_active_threat_ids(self, correlation_pipeline_ids: Sequence[UUID]) -> dict[UUID, list[str]]:
        """Public read of the per-pipeline Active Threat deny-list (§20), batched over N pipelines.

        The scoped MONITORED THREATS dashboard block subtracts this from the global catalogue.
        Thin on purpose: the lookup itself stays in the seam
        ``expose-per-pipeline-monitored-threats.md`` fills in.
        """
        return await self._excluded_active_threat_ids(correlation_pipeline_ids)

    async def _excluded_active_threat_ids(self, correlation_pipeline_ids: Sequence[UUID]) -> dict[UUID, list[str]]:
        """Per-pipeline Active Threat deny-list (§20), in one grouped query for all of them.

        A pipeline with no exclusion rows is absent from the result, and that is not a "not
        configured yet" state: monitoring is opt-out, so an empty deny-list means it monitors every
        non-blocked threat. The other half of the rule (``NOT is_blocked``) never travels here — it
        rides the shared active-threats topic.
        """
        return await self.active_threats_dao.get_excluded_threat_ids_by_pipeline_ids(correlation_pipeline_ids)

    async def _publish_config(self, row: CorrelationPipeline) -> None:
        """Publish this row's chains-config envelope to ITS topic (one producer, N topics)."""
        excluded = (await self._excluded_active_threat_ids([row.id])).get(row.id, [])
        envelope = chains_config_producer.build_envelope(
            row,
            excluded_active_threat_ids=excluded,
        )
        await chains_config_producer.publish(envelope, topic=chains_config_topic_for(row.id))

    async def _log_activity(
        self,
        row: CorrelationPipeline,
        *,
        action: str,
        user: User | None,
        details: str,
        changes: dict[str, Any] | None = None,
    ) -> None:
        if user is None:
            return
        await activity_producer.log_action(
            action=action,
            entity_type="correlation_pipeline",
            entity_id=str(row.id),
            entity_name=row.name,
            user=user,
            details=details,
            changes=changes,
            source="user",
        )

    async def to_response(
        self,
        row: CorrelationPipeline,
        *,
        attack_chains_count: int = 0,
        runtime_state: AttackChainsRuntimeStateResponse | None = None,
    ) -> CorrelationPipelineResponse:
        """Row -> detail response. Chain count and runtime state are supplied by the caller."""
        return CorrelationPipelineResponse(
            id=row.id,
            name=row.name,
            jobs_status=row.jobs_status,
            runtime_state=runtime_state,
            members=self._members_of(row),
            destination_topics=self._destination_topics_of(row),
            attack_chains_count=attack_chains_count,
            created_at=row.created_at,
            updated_at=row.updated_at,
            threshold_pct=row.threshold_pct,
            correlation_window_seconds=row.correlation_window_seconds,
            auto_add_new_pipelines=bool(row.auto_add_new_pipelines),
            auto_add_pipeline_name_pattern=row.auto_add_pipeline_name_pattern,
            correlation_resources=self._resources_of(row, "correlation"),
            agent_resources=self._resources_of(row, "agent"),
            correlation_deployment_name=row.correlation_deployment_name,
            agent_deployment_name=row.agent_deployment_name,
            updated_by=row.updated_by,
        )
