"""Base interface for Flink deployment providers.

This module defines the abstract base class and data structures for
Flink deployment providers. Implementations include:
- KubernetesFlinkProvider: Uses Flink Kubernetes Operator (FlinkDeployment CRDs)
- CMFFlinkProvider: Uses Confluent Manager for Apache Flink (REST API)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from apps.core.settings import settings


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
    output_topic: str
    rules_topic: str | None = None
    metrics_topic: str | None = None

    # Processing configuration
    parallelism: int = 2
    output_mode: str = "matched_only"
    apply_parser_to_output_events: bool = False

    # Resource configuration (per TaskManager; each TM = one parallelism unit, 1 slot)
    taskmanager_memory_mb: int = 2048
    taskmanager_cpu: float = 1.0
    kafka_starting_offset: str = "latest"
    window_size_sec: int = 30
    checkpoint_interval_sec: int = 60

    # Autoscaler configuration
    autoscaler_enabled: bool = False
    autoscaler_min_parallelism: int = 1
    autoscaler_max_parallelism: int = 24


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
    async def cleanup_storage(self, pipeline_id: str) -> bool:
        """Clean up storage (checkpoints, savepoints) for a pipeline.

        Args:
            pipeline_id: Pipeline UUID to clean up

        Returns:
            True if cleanup was successful or not needed
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
