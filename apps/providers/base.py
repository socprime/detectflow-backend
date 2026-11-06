"""Base interface for Flink deployment providers.

This module defines the abstract base class and data structures for
Flink deployment providers. Implementations include:
- KubernetesFlinkProvider: Uses Flink Kubernetes Operator (FlinkDeployment CRDs)
- CMFFlinkProvider: Uses Confluent Manager for Apache Flink (REST API)

The ``FlinkProvider`` ABC covers **detection pipelines** only. The correlation and agent
FlinkDeployments of a correlation pipeline are a Kubernetes-provider-only extension surface:
``AttackChainsManager._require_kubernetes_provider`` rejects every other provider before any
correlation code runs, so those methods live on ``KubernetesFlinkProvider`` and are deliberately
absent from the ABC (CMF would only be able to stub them). ``CorrelationDeploymentConfig`` and
``CorrelationDeploymentRef`` below are their shared vocabulary.
"""

import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal

from apps.core.exceptions import ConfigurationError
from apps.core.settings import settings

# Kubernetes labels every correlation-pipeline FlinkDeployment carries. The id label is what
# ``KubernetesFlinkProvider.list_correlation_deployments`` selects on (by existence, not value),
# so the runtime view can diff the cluster against the database and spot orphans.
CORRELATION_PIPELINE_ID_LABEL = "detectflow.io/correlation-pipeline-id"
CORRELATION_JOB_ROLE_LABEL = "detectflow.io/job-role"

# A Kubernetes Job name may not exceed 63 characters. The storage-cleanup Job is named
# ``cleanup-{deployment_name}``, which makes it the longest name derived from a deployment name
# and therefore the binding constraint on the generated names in ``apps/core/constants.py``.
K8S_JOB_NAME_MAX_LEN = 63
CLEANUP_JOB_NAME_PREFIX = "cleanup-"

# One DNS-1123 label: lowercase alphanumeric and '-', 1-63 chars, no separator.
_STATE_SUBPATH_RE = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")


def assert_state_subpath(state_subpath: str) -> str:
    """Validate the single path segment that isolates one deployment's state.

    Rendered into ``subPath:`` in all three FlinkDeployment templates and used as the
    cleanup Job's ``rm -rf`` target, so it must be exactly one DNS-1123 label: no
    separator, no traversal, no empty string. Raises (``ConfigurationError``, 400)
    instead of returning a bool — a bad key here is another customer's checkpoints.

    Raises:
        ConfigurationError: 400 — the key is not a single DNS-1123 label.
    """
    if not _STATE_SUBPATH_RE.fullmatch(state_subpath):
        raise ConfigurationError(
            f"Invalid state subPath '{state_subpath}': expected a single DNS-1123 label "
            "(lowercase alphanumeric and '-', 1-63 chars, no '/')"
        )
    return state_subpath


class FlinkQuotaExceededError(Exception):
    """Raised when resource quota is exceeded.

    This is a provider-agnostic error that allows orchestrator to handle
    quota issues without knowing provider-specific exception types.
    """

    def __init__(self, message: str, details: str | None = None):
        self.message = message
        self.details = details
        super().__init__(message)


def get_default_jobmanager_config() -> dict:
    """Get default JobManager resource configuration from settings.

    Returns:
        Dict with jobmanager_memory and jobmanager_cpu keys.
        Used by both KubernetesFlinkProvider and CMFFlinkProvider.
    """
    return {
        "jobmanager_memory": f"{int(settings.flink_jobmanager_memory_gb * 1024)}m",
        "jobmanager_cpu": settings.flink_jobmanager_cpu,
    }


@dataclass
class FlinkDeploymentConfig:
    """Configuration for creating/updating a Flink deployment.

    Contains all parameters needed to deploy a Flink job for a pipeline.
    Provider implementations translate this to their native format
    (FlinkDeployment YAML for K8s, JSON manifest for CMF).
    """

    # Pipeline identification
    pipeline_id: str
    pipeline_name: str

    # Kafka topics
    input_topics: list[str]
    output_topic: str  # First destination (KAFKA_OUTPUT_TOPIC shim for older match nodes)
    # Full output fan-out → KAFKA_OUTPUT_DESTINATIONS JSON
    output_destinations: list[dict[str, Any]] = field(default_factory=list)
    rules_topic: str | None = None
    metrics_topic: str | None = None
    # Per-pipeline tagged-events side-output topic (epic Decision 1).
    # None ⇒ legacy shared settings.kafka_tagged_events_topic (pre-epic pipelines).
    tagged_events_topic: str | None = None

    # Processing configuration (output_mode / apply_parser mirror the first destination)
    parallelism: int = 2
    output_mode: str = "matched_only"
    apply_parser_to_output_events: bool = False

    # Resource configuration (per TaskManager; each TM = one parallelism unit, 1 slot)
    taskmanager_memory_mb: int = 2048
    taskmanager_cpu: float = 1.5
    kafka_starting_offset: str = "latest"
    window_size_sec: int = 30
    checkpoint_interval_sec: int = 60

    # Autoscaler configuration
    autoscaler_enabled: bool = False
    autoscaler_min_parallelism: int = 1
    autoscaler_max_parallelism: int = 24

    def kafka_output_destinations_json(self) -> str:
        """Compact JSON for the match node's ``KAFKA_OUTPUT_DESTINATIONS`` env var.

        Shape: ``[{"topic", "output_mode", "apply_parser_to_output_events"}, ...]`` — the
        match node writes each event to every destination whose mode accepts it. Falls back
        to the singular fields so a config built without destinations still deploys.
        """
        destinations = self.output_destinations or [
            {
                "topic": self.output_topic,
                "output_mode": self.output_mode,
                "apply_parser_to_output_events": self.apply_parser_to_output_events,
            }
        ]
        return json.dumps(destinations, separators=(",", ":"))


@dataclass
class CorrelationDeploymentConfig:
    """Rendering inputs for one correlation pipeline's pair of FlinkDeployments.

    One instance drives BOTH the correlation job and the agent job of a single
    correlation pipeline. Deployment names are pre-generated (stored on
    ``correlation_pipelines.correlation_deployment_name`` / ``.agent_deployment_name``)
    so K8s object identity survives a rename of the pipeline.

    Kafka contract (epic D1 + D2): ``input_topics`` -> KAFKA_INPUT_TOPICS,
    comma-joined, one ``detectflow.tagged-events.<pipeline_id>`` per member detection
    pipeline; ``chains_config_topic`` compacted, 1 partition, key "global";
    ``chains_raw_topic`` correlation-job -> agent-job; ``chains_analysed_topic``
    agent-job -> one pattern-subscribed control-plane consumer.

    ``correlation_pipeline_id`` is empty only for the legacy single-instance deployment that
    predates the epic: it owns no ``correlation_pipelines`` row until the migration creates one,
    and the data plane reads an absent pipeline id as "keep the pre-epic chain_id seed".
    """

    correlation_pipeline_id: str
    correlation_pipeline_name: str
    correlation_deployment_name: str
    agent_deployment_name: str

    input_topics: list[str]
    chains_config_topic: str
    chains_raw_topic: str
    chains_analysed_topic: str

    # Hot-reloaded via chains-config; carried here only as a cold-start seed.
    # 60 is the settled epic default and matches both the DB column default and
    # the data plane's own `chain_coverage_threshold_pct` default.
    threshold_pct: int = 60
    correlation_window_seconds: int | None = None

    # Correlation Flink job (defaults = today's production numbers)
    correlation_parallelism: int = 1
    correlation_taskmanager_memory_mb: int = 4096
    correlation_taskmanager_cpu: float = 3.0
    correlation_jobmanager_memory_mb: int = 2048
    correlation_jobmanager_cpu: float = 1.0

    # Agentic Flink job
    agent_parallelism: int = 1
    agent_taskmanager_memory_mb: int = 3072
    agent_taskmanager_cpu: float = 1.0
    agent_jobmanager_memory_mb: int = 2048
    agent_jobmanager_cpu: float = 1.0


@dataclass
class CorrelationDeploymentRef:
    """One correlation-pipeline FlinkDeployment as the cluster currently reports it.

    Returned by ``KubernetesFlinkProvider.list_correlation_deployments``; the runtime view diffs
    these against ``correlation_pipelines`` to flag orphans, so ``correlation_pipeline_id`` is
    whatever the label says — it may name a row that no longer exists.
    """

    deployment_name: str
    correlation_pipeline_id: str
    role: Literal["correlation", "agent"]
    state: Literal["running", "starting", "failed", "missing"]


@dataclass(frozen=True)
class StorageCleanupOutcome:
    """What the storage-cleanup Job actually did — not merely that it was created.

    The Job used to end in ``|| true`` and could not fail, so "the API accepted the object" was a
    fair proxy for "the state is gone". §22 made the script **fail closed**: a state key that is not
    a single path segment, a symlinked target or a target that is not a directory each exit
    non-zero. A refusal means the state SURVIVED and the next start will restore from checkpoints
    the operator believes were deleted — for correlation-job that silently breaks the "no replay of
    events that landed while the job was paused" runtime contract. So the outcome is reported.

    ``reason`` carries the Job's terminal message (the script prints its refusal to stderr) so an
    operator does not need ``kubectl logs`` to learn why.
    """

    state_subpath: str
    status: Literal["succeeded", "failed", "timed_out"]
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "succeeded"

    def describe(self) -> str:
        """One-line status for an API response or an activity entry."""
        return self.status if self.ok or not self.reason else f"{self.status}: {self.reason}"


@dataclass
class FlinkDeploymentResult:
    """Result of a deployment operation (create/update).

    Returned by create_deployment and update_deployment methods.
    """

    deployment_name: str
    success: bool
    error: str | None = None
    # Raw response from API (provider-specific)
    raw_response: dict | None = None


@dataclass
class FlinkDeploymentStatus:
    """Status of a Flink deployment.

    Normalized status representation from any provider.
    """

    # Normalized state (running, failed, disabled, deploying, reconciling, etc.)
    state: str

    # Flink job ID if available
    job_id: str | None = None

    # Provider-specific lifecycle state
    lifecycle_state: str | None = None

    # Error message if any
    error: str | None = None

    # Additional diagnostics (provider-specific)
    job_manager_status: str | None = None
    warnings: list[str] = field(default_factory=list)


class FlinkProvider(ABC):
    """Abstract base class for Flink deployment providers.

    Defines the interface that all Flink providers must implement.
    This enables switching between different Flink deployment mechanisms
    (Kubernetes Operator, CMF, etc.) without changing business logic.

    Usage:
        from apps.providers.factory import get_flink_provider

        provider = get_flink_provider()
        result = await provider.create_deployment(config)
    """

    @abstractmethod
    async def create_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Create a new Flink deployment for a pipeline.

        Args:
            config: Deployment configuration

        Returns:
            FlinkDeploymentResult with deployment name and status

        Raises:
            Exception: If deployment creation fails
        """
        pass

    @abstractmethod
    async def update_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Update an existing Flink deployment.

        Args:
            config: Updated deployment configuration

        Returns:
            FlinkDeploymentResult with deployment name and status

        Raises:
            Exception: If deployment update fails
        """
        pass

    @abstractmethod
    async def delete_deployment(self, pipeline_id: str, wait: bool = True) -> bool:
        """Delete a Flink deployment.

        Args:
            pipeline_id: Pipeline UUID to delete deployment for
            wait: If True, wait for deletion to complete

        Returns:
            True if deleted successfully or doesn't exist
        """
        pass

    @abstractmethod
    async def suspend_deployment(self, pipeline_id: str) -> bool:
        """Suspend a running deployment (preserves state).

        Args:
            pipeline_id: Pipeline UUID to suspend

        Returns:
            True if suspended successfully
        """
        pass

    @abstractmethod
    async def resume_deployment(self, pipeline_id: str) -> bool:
        """Resume a suspended deployment.

        Args:
            pipeline_id: Pipeline UUID to resume

        Returns:
            True if resumed successfully
        """
        pass

    @abstractmethod
    async def get_deployment_status(self, pipeline_id: str) -> FlinkDeploymentStatus | None:
        """Get the current status of a deployment.

        Args:
            pipeline_id: Pipeline UUID to get status for

        Returns:
            FlinkDeploymentStatus or None if not found
        """
        pass

    @abstractmethod
    async def find_deployment(self, pipeline_id: str) -> str | None:
        """Find deployment name by pipeline ID.

        Args:
            pipeline_id: Pipeline UUID to search for

        Returns:
            Deployment name if found, None otherwise
        """
        pass

    @abstractmethod
    async def cleanup_storage(self, pipeline_id: str) -> "StorageCleanupOutcome":
        """Clean up storage (checkpoints, savepoints) for a pipeline.

        Implementations report what the cleanup DID, not that they asked for it: the Kubernetes
        one fails closed and a refusal means the state survived. Never raises for a refusal — a
        surviving state directory is recoverable, a failed stop is not.

        Args:
            pipeline_id: Pipeline UUID to clean up

        Returns:
            The outcome: ``succeeded`` / ``failed`` / ``timed_out``, with the failure reason.
        """
        pass

    @abstractmethod
    def get_pod_warnings(self, pipeline_id: str, max_events: int = 10) -> list[str]:
        """Get warning events related to a deployment.

        Args:
            pipeline_id: Pipeline UUID
            max_events: Maximum number of warnings to return

        Returns:
            List of warning messages
        """
        pass

    @abstractmethod
    def get_rest_url(self, pipeline_id: str) -> str:
        """Get Flink REST API URL for a pipeline.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            Full URL to Flink REST API (e.g., http://pipeline-xxx-rest:8081)
        """
        pass
