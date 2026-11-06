"""Kubernetes Flink Operator provider for managing Flink deployments.

This module provides KubernetesFlinkProvider that implements FlinkProvider
interface using Kubernetes Flink Operator (FlinkDeployment CRDs).
"""

import asyncio
import time
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Literal

import yaml
from jinja2 import Environment, FileSystemLoader
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import ConfigurationError
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer
from apps.providers.base import (
    CLEANUP_JOB_NAME_PREFIX,
    CORRELATION_JOB_ROLE_LABEL,
    CORRELATION_PIPELINE_ID_LABEL,
    K8S_JOB_NAME_MAX_LEN,
    CorrelationDeploymentConfig,
    CorrelationDeploymentRef,
    FlinkDeploymentConfig,
    FlinkDeploymentResult,
    FlinkDeploymentStatus,
    FlinkProvider,
    FlinkQuotaExceededError,
    StorageCleanupOutcome,
    assert_state_subpath,
    get_default_jobmanager_config,
)
from apps.providers.matcher_runtime_tuning import resolve_matcher_runtime

logger = get_logger(__name__)

# How often the cleanup-Job watch polls. The Job removes three directories, so it either finishes
# in the first second or is stuck waiting to schedule.
_CLEANUP_POLL_INTERVAL_SECONDS = 2

# Body of the storage-cleanup Job. A module-level constant so a test can execute it under a real
# ``sh``. The state key arrives as ``$STATE_SUBPATH`` and is NEVER interpolated into the shell:
# this script is the last thing standing between a name-derivation bug and another customer's
# checkpoints, so it validates the target instead of trusting it, and exits non-zero on refusal
# (the ``|| true`` this replaces made every failure look like a success).
_CLEANUP_SCRIPT = """set -eu
: "${STATE_SUBPATH:?STATE_SUBPATH is required}"
# Dot segments and separators never reach `rm`: `dirname "$base/.."` is `$base`, so the
# one-level check below would wave them through and leave the outcome to whether this image's
# `rm` refuses to remove '.' / '..' -- which is the PVC root's contents.
case "$STATE_SUBPATH" in
    "" | "." | ".." | */*)
        echo "refusing: '$STATE_SUBPATH' is not a single path segment" >&2
        exit 1
        ;;
esac
for base in /mnt/checkpoints /mnt/ha /mnt/savepoints; do
    target="$base/$STATE_SUBPATH"
    # Exactly one level below the mount, or refuse: a name-derivation bug that got past the
    # checks above must not be able to reach a sibling's state -- or the PVC root.
    if [ "$(dirname "$target")" != "$base" ]; then
        echo "refusing: '$target' is not exactly one level below '$base'" >&2
        exit 1
    fi
    if [ -L "$target" ]; then
        echo "refusing: '$target' is a symlink" >&2
        exit 1
    fi
    if [ ! -e "$target" ]; then
        echo "nothing to delete at '$target'"
        continue
    fi
    if [ ! -d "$target" ]; then
        echo "refusing: '$target' is not a directory" >&2
        exit 1
    fi
    rm -rf "$target"
    echo "deleted '$target'"
done
"""


def _assert_k8s_names_fit(config: CorrelationDeploymentConfig) -> None:
    """Refuse to deploy names whose derived Kubernetes Job name would be rejected.

    ``cleanup-{deployment_name}`` is the longest object name this repo derives from a deployment
    name, so checking it covers every shorter one Flink adds (``-rest``, ``-taskmanager-N-N``,
    ``-cluster-config-map``). Raises before the first API call so a bad name surfaces as a 400,
    not as a half-created runtime.

    Raises:
        ConfigurationError: 400 — a generated deployment name is too long.
    """
    for name in (config.correlation_deployment_name, config.agent_deployment_name):
        if len(CLEANUP_JOB_NAME_PREFIX + name) > K8S_JOB_NAME_MAX_LEN:
            raise ConfigurationError(
                f"Generated deployment name '{name}' is too long: "
                f"'{CLEANUP_JOB_NAME_PREFIX}{name}' exceeds the {K8S_JOB_NAME_MAX_LEN}-char "
                "Kubernetes Job limit"
            )


class KubernetesFlinkProvider(FlinkProvider):
    """Flink provider using Kubernetes Flink Operator.

    Manages FlinkDeployment CRDs for ETL pipelines. Uses Jinja2 templates
    to generate deployment manifests and interacts with the Kubernetes API.

    Attributes:
        namespace: Kubernetes namespace for deployments.
        flink_image: Docker image for Flink jobs.
        image_pull_policy: Image pull policy (IfNotPresent, Always, Never).
        api: Kubernetes CustomObjectsApi client.
        core_api: Kubernetes CoreV1Api client.
        jinja_env: Jinja2 template environment.
        default_config: Default resource configuration for deployments.
    """

    # Kubernetes API constants
    FLINK_API_GROUP = "flink.apache.org"
    FLINK_API_VERSION = "v1beta1"
    FLINK_PLURAL = "flinkdeployments"

    def __init__(
        self,
        namespace: str | None = None,
        flink_image: str | None = None,
        image_pull_policy: str | None = None,
    ):
        """Initialize Kubernetes client and configuration.

        Args:
            namespace: Kubernetes namespace for deployments.
                       Defaults to settings.kubernetes_namespace.
            flink_image: Docker image for Flink jobs.
            image_pull_policy: Image pull policy (IfNotPresent, Always, Never).
        """
        self.namespace = namespace or settings.kubernetes_namespace
        self.flink_image = flink_image or settings.flink_image
        raw_pull_policy = image_pull_policy or settings.image_pull_policy
        self.image_pull_policy = self._normalize_image_pull_policy(raw_pull_policy)

        # Load kubeconfig
        try:
            # Try in-cluster config (if running inside a pod)
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            # Fallback to ~/.kube/config
            config.load_kube_config()
            logger.info("Loaded kubeconfig from ~/.kube/config")

        # API clients for Kubernetes resources
        self.api = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()

        # Jinja2 template environment
        template_dir = Path(__file__).parent.parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Default resource configuration from settings (JobManager; TaskManagers scale with parallelism)
        self.default_config = get_default_jobmanager_config()

        logger.info(
            "KubernetesFlinkProvider initialized",
            extra={
                "namespace": namespace,
                "flink_image": flink_image,
                "image_pull_policy": self.image_pull_policy,
            },
        )

    @staticmethod
    def _calculate_autoscaler_quotas(
        taskmanager_memory_mb: int,
        autoscaler_max_parallelism: int,
    ) -> tuple[str, str]:
        """Calculate autoscaler quotas for a one-slot-per-TaskManager layout."""
        if settings.autoscaler_quota_cpu is not None:
            quota_cpu = str(settings.autoscaler_quota_cpu)
        else:
            quota_cpu = str(float(autoscaler_max_parallelism))

        if settings.autoscaler_quota_memory_gb is not None:
            quota_memory = f"{int(settings.autoscaler_quota_memory_gb)}g"
        else:
            quota_memory_mb = autoscaler_max_parallelism * taskmanager_memory_mb
            quota_memory = f"{quota_memory_mb}m"

        return quota_cpu, quota_memory

    @staticmethod
    def _native_headroom_min_mb(python_execution_mode: str | None = None) -> int:
        """Python/off-heap reserve outside ``process.size`` (mode-specific)."""
        mode = python_execution_mode or settings.matcher_python_execution_mode
        if mode == "thread":
            return settings.flink_taskmanager_native_headroom_min_mb
        return settings.flink_taskmanager_process_native_headroom_min_mb

    @classmethod
    def _calculate_taskmanager_process_memory_mb(
        cls,
        taskmanager_memory_mb: int,
        python_execution_mode: str | None = None,
    ) -> int:
        """JVM/Flink process budget carved out of the configured pod total.

        Process mode (default): at or below ``flink_taskmanager_process_memory_base_mb`` the
        configured value is the starting point; above it the JVM keeps the base plus
        ``flink_taskmanager_process_memory_growth_fraction`` of the extra. Prefer leaving
        ``process_native_headroom_min_mb`` for Python workers; if that does not fit, the
        JVM is floored at ``process_memory_min_mb`` and Python gets whatever remains
        inside the configured total (headroom is compressed — the pod is never grown).

        Thread mode: Python shares the JVM process, so nearly all configured memory goes to
        ``process.size``; only ``native_headroom_min_mb`` stays outside for off-heap
        (jemalloc / Polars) within the same cgroup.
        """
        mode = python_execution_mode or settings.matcher_python_execution_mode
        headroom_mb = cls._native_headroom_min_mb(mode)
        process_min_mb = settings.flink_taskmanager_process_memory_min_mb

        if mode == "thread":
            process_memory_mb = taskmanager_memory_mb - headroom_mb
            return max(process_memory_mb, process_min_mb)

        base_mb = settings.flink_taskmanager_process_memory_base_mb
        if taskmanager_memory_mb <= base_mb:
            process_memory_mb = taskmanager_memory_mb
        else:
            excess_mb = taskmanager_memory_mb - base_mb
            process_memory_mb = base_mb + round(excess_mb * settings.flink_taskmanager_process_memory_growth_fraction)

        # Prefer the configured Python reserve when it still leaves room for the JVM floor.
        # Otherwise pin the JVM to the floor and give the rest of the configured pod to Python
        # (compress headroom — do not grow the pod).
        max_for_headroom = taskmanager_memory_mb - headroom_mb
        if max_for_headroom >= process_min_mb:
            process_memory_mb = min(process_memory_mb, max_for_headroom)
        else:
            process_memory_mb = process_min_mb
        process_memory_mb = max(process_memory_mb, process_min_mb)
        return min(process_memory_mb, taskmanager_memory_mb)

    @classmethod
    def _calculate_taskmanager_pod_memory_mb(
        cls,
        taskmanager_memory_mb: int,
        python_execution_mode: str | None = None,
    ) -> int:
        """Pod memory limit: exactly the configured total.

        Grow only when the configured value is below the JVM floor (so the memory request
        cannot exceed the limit). Do not inflate for Python headroom — users expect the
        TaskManager memory they set to be the pod size.
        """
        process_memory_mb = cls._calculate_taskmanager_process_memory_mb(taskmanager_memory_mb, python_execution_mode)
        return max(taskmanager_memory_mb, process_memory_mb)

    @classmethod
    def _calculate_taskmanager_memory_limit_factor(
        cls,
        taskmanager_memory_mb: int,
        python_execution_mode: str | None = None,
    ) -> str:
        """Return the K8s memory limit multiplier for PyFlink TaskManager pods (pod / JVM)."""
        process_memory_mb = cls._calculate_taskmanager_process_memory_mb(taskmanager_memory_mb, python_execution_mode)
        pod_memory_mb = cls._calculate_taskmanager_pod_memory_mb(taskmanager_memory_mb, python_execution_mode)
        limit_factor = pod_memory_mb / process_memory_mb
        if limit_factor.is_integer():
            return f"{limit_factor:.1f}"
        return f"{limit_factor:.3f}".rstrip("0").rstrip(".")

    # =========================================================================
    # FlinkProvider Interface Implementation (async with to_thread for K8s API)
    # =========================================================================

    async def create_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Create a FlinkDeployment CRD for a pipeline.

        Raises:
            FlinkQuotaExceededError: If Kubernetes ResourceQuota is exceeded
        """
        try:
            result = await asyncio.to_thread(self._create_flink_deployment, config)
            deployment_name = result.get("metadata", {}).get("name", f"flink-{config.pipeline_id}")
            return FlinkDeploymentResult(
                deployment_name=deployment_name,
                success=True,
                raw_response=result,
            )
        except ApiException as e:
            # Check for quota exceeded error
            error_body = str(e.body) if e.body else ""
            if e.status == 403 and "exceeded quota" in error_body.lower():
                raise FlinkQuotaExceededError(
                    message="Kubernetes ResourceQuota exceeded",
                    details=error_body[:500],
                ) from e
            error_msg = f"Failed to create FlinkDeployment: {e}"
            if ErrorTracker.should_log(f"k8s_create_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=f"flink-{config.pipeline_id}",
                success=False,
                error=str(e),
            )
        except Exception as e:
            error_msg = f"Failed to create FlinkDeployment: {e}"
            if ErrorTracker.should_log(f"k8s_create_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=f"flink-{config.pipeline_id}",
                success=False,
                error=str(e),
            )

    async def update_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Update an existing FlinkDeployment spec.

        Raises:
            FlinkQuotaExceededError: If Kubernetes ResourceQuota is exceeded
        """
        try:
            result = await asyncio.to_thread(self._update_flink_deployment, config)
            deployment_name = result.get("metadata", {}).get("name", f"flink-{config.pipeline_id}")
            return FlinkDeploymentResult(
                deployment_name=deployment_name,
                success=True,
                raw_response=result,
            )
        except ApiException as e:
            # Check for quota exceeded error
            error_body = str(e.body) if e.body else ""
            if e.status == 403 and "exceeded quota" in error_body.lower():
                raise FlinkQuotaExceededError(
                    message="Kubernetes ResourceQuota exceeded",
                    details=error_body[:500],
                ) from e
            error_msg = f"Failed to update FlinkDeployment: {e}"
            if ErrorTracker.should_log(f"k8s_update_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=f"flink-{config.pipeline_id}",
                success=False,
                error=str(e),
            )
        except Exception as e:
            error_msg = f"Failed to update FlinkDeployment: {e}"
            if ErrorTracker.should_log(f"k8s_update_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=f"flink-{config.pipeline_id}",
                success=False,
                error=str(e),
            )

    async def delete_deployment(self, pipeline_id: str, wait: bool = True) -> bool:
        """Delete FlinkDeployment by pipeline ID."""
        return await asyncio.to_thread(self.delete_flink_deployment_by_pipeline_id, pipeline_id, wait)

    async def suspend_deployment(self, pipeline_id: str) -> bool:
        """Suspend a FlinkDeployment by pipeline ID."""
        deployment_name = await asyncio.to_thread(self.find_deployment_by_pipeline_id, pipeline_id)
        if not deployment_name:
            return True  # Nothing to suspend
        return await asyncio.to_thread(self._suspend_deployment, deployment_name)

    async def resume_deployment(self, pipeline_id: str) -> bool:
        """Resume a suspended FlinkDeployment by pipeline ID."""
        deployment_name = await asyncio.to_thread(self.find_deployment_by_pipeline_id, pipeline_id)
        if not deployment_name:
            return False  # Nothing to resume
        return await asyncio.to_thread(self._resume_deployment, deployment_name)

    async def get_deployment_status(self, pipeline_id: str) -> FlinkDeploymentStatus | None:
        """Get FlinkDeployment status by pipeline ID."""
        deployment_name = await asyncio.to_thread(self.find_deployment_by_pipeline_id, pipeline_id)
        if not deployment_name:
            return None
        return await asyncio.to_thread(self._get_deployment_status, deployment_name)

    async def find_deployment(self, pipeline_id: str) -> str | None:
        """Find FlinkDeployment by pipeline-id label."""
        return await asyncio.to_thread(self.find_deployment_by_pipeline_id, pipeline_id)

    async def cleanup_storage(self, pipeline_id: str) -> StorageCleanupOutcome:
        """Clean up savepoints, checkpoints, and HA storage for a pipeline, and report the outcome."""
        return await asyncio.to_thread(self.cleanup_pipeline_storage, pipeline_id)

    def get_pod_warnings(self, pipeline_id: str, max_events: int = 10) -> list[str]:
        """Get warning events for pods related to a FlinkDeployment.

        Note: This is sync because it's called from sync contexts.
        For async contexts, wrap with asyncio.to_thread().
        """
        deployment_name = self.find_deployment_by_pipeline_id(pipeline_id)
        if not deployment_name:
            return []
        return self._get_pod_warnings(deployment_name, max_events)

    def get_rest_url(self, pipeline_id: str) -> str:
        """Get Flink REST API URL for a pipeline."""
        deployment_name = self._generate_deployment_name(pipeline_id)
        return f"http://{deployment_name}-rest.{self.namespace}.svc.cluster.local:8081"

    async def launch_correlation_job_deployment(self, config: CorrelationDeploymentConfig) -> dict[str, object]:
        """Create-or-update one correlation pipeline's correlation-job FlinkDeployment."""
        return await asyncio.to_thread(self._launch_correlation_job_deployment, config)

    async def delete_correlation_job_deployment(self, deployment_name: str) -> bool:
        """Delete one correlation-job FlinkDeployment by name."""
        return await asyncio.to_thread(self._delete_correlation_job_deployment, deployment_name)

    async def get_correlation_job_deployment_state(
        self,
        deployment_name: str,
    ) -> tuple[Literal["running", "starting", "failed", "missing"], datetime | None]:
        """Return normalized correlation-job FlinkDeployment state and its start time."""
        return await asyncio.to_thread(self._get_deployment_runtime_state, deployment_name)

    async def cleanup_correlation_job_storage(self, deployment_name: str) -> StorageCleanupOutcome:
        """Wipe one correlation-job's checkpoint/HA/savepoint state on the shared PVCs."""
        return await asyncio.to_thread(self._cleanup_deployment_storage, deployment_name)

    async def launch_agent_job_deployment(self, config: CorrelationDeploymentConfig) -> dict[str, object]:
        """Create-or-update one correlation pipeline's agent-job FlinkDeployment."""
        return await asyncio.to_thread(self._launch_agent_job_deployment, config)

    async def delete_agent_job_deployment(self, deployment_name: str) -> bool:
        """Delete one agent-job FlinkDeployment by name."""
        return await asyncio.to_thread(self._delete_agent_job_deployment, deployment_name)

    async def get_agent_job_deployment_state(
        self,
        deployment_name: str,
    ) -> tuple[Literal["running", "starting", "failed", "missing"], datetime | None]:
        """Return normalized agent-job FlinkDeployment state and its start time."""
        return await asyncio.to_thread(self._get_deployment_runtime_state, deployment_name)

    async def cleanup_agent_job_storage(self, deployment_name: str) -> StorageCleanupOutcome:
        """Wipe one agent-job's checkpoint/HA/savepoint state on the shared PVCs."""
        return await asyncio.to_thread(self._cleanup_deployment_storage, deployment_name)

    async def list_correlation_deployments(self) -> list[CorrelationDeploymentRef]:
        """List every correlation/agent FlinkDeployment the cluster currently carries.

        Selects on the **existence** of ``detectflow.io/correlation-pipeline-id``, not on a
        value, so a deployment whose ``correlation_pipelines`` row was deleted still shows up —
        that is exactly what the runtime view diffs against the database to flag orphans.

        The legacy single-instance deployment carries no such label (it owns no row until the
        migration creates one), so it is deliberately not listed here.
        """
        return await asyncio.to_thread(self._list_correlation_deployments)

    # =========================================================================
    # Internal Synchronous Methods
    # =========================================================================

    def find_deployment_by_pipeline_id(self, pipeline_id: str) -> str | None:
        """Find FlinkDeployment by pipeline-id label."""
        logger.debug(
            "Searching for FlinkDeployment by pipeline ID",
            extra={"pipeline_id": pipeline_id},
        )

        try:
            response = self.api.list_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                label_selector=f"pipeline-id={pipeline_id}",
            )

            items = response.get("items", [])
            if items:
                deployment_name = items[0]["metadata"]["name"]
                logger.debug(
                    "Found FlinkDeployment",
                    extra={
                        "pipeline_id": pipeline_id,
                        "deployment_name": deployment_name,
                    },
                )
                return deployment_name

            logger.debug(
                "No FlinkDeployment found",
                extra={"pipeline_id": pipeline_id},
            )
            return None

        except ApiException as e:
            if e.status == 404:
                return None

            error_msg = f"Failed to find FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": pipeline_id, "status_code": e.status},
            )
            raise Exception(error_msg) from e

    def delete_flink_deployment(self, deployment_name: str, wait: bool = True, timeout: int = 60) -> bool:
        """Delete a FlinkDeployment CRD."""
        logger.info(
            "Deleting FlinkDeployment",
            extra={"deployment_name": deployment_name, "wait": wait},
        )

        try:
            self.api.delete_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )

            logger.info(
                "FlinkDeployment delete request accepted",
                extra={"deployment_name": deployment_name},
            )

            if wait:
                self._wait_for_deletion(deployment_name, timeout)

            logger.info(
                "FlinkDeployment deleted successfully",
                extra={"deployment_name": deployment_name},
            )
            return True

        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    "FlinkDeployment not found (already deleted)",
                    extra={"deployment_name": deployment_name},
                )
                return True

            error_msg = f"Failed to delete FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={
                    "deployment_name": deployment_name,
                    "status_code": e.status,
                },
            )
            raise Exception(error_msg) from e

    def delete_flink_deployment_by_pipeline_id(self, pipeline_id: str, wait: bool = True) -> bool:
        """Find and delete FlinkDeployment by pipeline ID."""
        logger.info(
            "Deleting FlinkDeployment by pipeline ID",
            extra={"pipeline_id": pipeline_id},
        )

        deployment_name = self.find_deployment_by_pipeline_id(pipeline_id)
        if deployment_name:
            return self.delete_flink_deployment(deployment_name, wait=wait)

        logger.info(
            "No FlinkDeployment found to delete",
            extra={"pipeline_id": pipeline_id},
        )
        return True

    def cleanup_pipeline_storage(self, pipeline_id: str) -> StorageCleanupOutcome:
        """Clean up savepoints, checkpoints, and HA storage for a detection pipeline.

        The detection pipeline's state key is the **bare** ``pipeline_id`` — the directory
        segment `flink-deployment.yaml.j2` has always written under, and the one its mounts are
        now scoped to. It is deliberately not ``flink-{pipeline_id}``: that would name a
        directory no pipeline owns.
        """
        return self._cleanup_deployment_storage(str(pipeline_id))

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _upsert_flink_deployment_by_name(
        self,
        *,
        template_name: str,
        deployment_name: str,
        template_vars: dict,
    ) -> dict[str, object]:
        """Create or replace a FlinkDeployment CR rendered from a Jinja template.

        Used for the correlation-pipeline FlinkDeployments (correlation-job, agent-job), whose
        name is generated once per pipeline and then fixed, so ``last-state`` recovery survives
        spec edits. Keyed by name only — ``resourceVersion`` is carried forward from the live
        object so a replace never clobbers a concurrent write.
        """
        manifest = yaml.safe_load(self.jinja_env.get_template(template_name).render(**template_vars))
        try:
            existing = self.api.get_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )
            manifest["metadata"]["resourceVersion"] = existing["metadata"]["resourceVersion"]
            self.api.replace_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
                body=manifest,
            )
            operation = "updated"
        except ApiException as e:
            if e.status != 404:
                raise
            self.api.create_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                body=manifest,
            )
            operation = "created"

        logger.info(
            "FlinkDeployment upserted",
            extra={"deployment_name": deployment_name, "operation": operation},
        )
        return {"deployment_name": deployment_name, "operation": operation}

    def _correlation_job_template_vars(self, config: CorrelationDeploymentConfig) -> dict:
        """Template variables for correlation-job-deployment.yaml.j2.

        Everything per-instance comes from ``config``; everything cluster-wide (namespace,
        images, node selector, PVCs, shared ConfigMap/Secret, Kafka auth, checkpoint interval)
        comes from settings.
        """
        return {
            "deployment_name": config.correlation_deployment_name,
            # State key for the three subPath mounts (§22). Equals the deployment name, which is
            # the directory segment this job's state already lives under on the shared PVCs — so
            # switching to subPath moves no bytes.
            "state_subpath": assert_state_subpath(config.correlation_deployment_name),
            "correlation_pipeline_id": config.correlation_pipeline_id,
            "correlation_pipeline_name": config.correlation_pipeline_name,
            "namespace": self.namespace,
            "correlation_image": settings.correlation_job_image,
            "image_pull_policy": self.image_pull_policy,
            "parallelism": config.correlation_parallelism,
            "taskmanager_replicas": config.correlation_parallelism,
            "taskmanager_memory": f"{config.correlation_taskmanager_memory_mb}m",
            "taskmanager_cpu": config.correlation_taskmanager_cpu,
            "jobmanager_memory": f"{config.correlation_jobmanager_memory_mb}m",
            "jobmanager_cpu": config.correlation_jobmanager_cpu,
            "checkpoint_interval_sec": settings.detectflow_checkpoint_interval_sec,
            "node_selector_key": settings.flink_node_selector_key,
            "node_selector_value": settings.flink_node_selector_value,
            "checkpoints_pvc": settings.flink_checkpoints_pvc,
            "ha_pvc": settings.flink_ha_pvc,
            "savepoints_pvc": settings.flink_savepoints_pvc,
            # Shared cluster infra (KAFKA_BOOTSTRAP_SERVERS, KAFKA_AUTH_METHOD, log
            # defaults) — same ConfigMap + Secret reused by sigma-detector.
            "flink_config_configmap": settings.flink_config_configmap_name,
            "flink_sigma_config_secret": settings.flink_sigma_config_secret_name,
            # Per-instance Kafka wiring (epic D1 + D2). The AT / HoS catalogues stay shared:
            # vendor data, not customer data.
            "input_topics": config.input_topics,
            "active_threats_topic": settings.kafka_active_threats_topic,
            "hos_rules_topic": settings.kafka_hos_rules_topic,
            "chains_config_topic": config.chains_config_topic,
            "chains_raw_topic": config.chains_raw_topic,
            "activity_topic": settings.kafka_activity_topic,
            "kafka_auth_method": settings.kafka_auth_method,
            # Cold-start seed only; the live value rides the chains-config broadcast.
            "threshold_pct": config.threshold_pct,
        }

    def _launch_correlation_job_deployment(self, config: CorrelationDeploymentConfig) -> dict[str, object]:
        """Create-or-update one correlation pipeline's correlation-job FlinkDeployment."""
        _assert_k8s_names_fit(config)
        return self._upsert_flink_deployment_by_name(
            template_name="correlation-job-deployment.yaml.j2",
            deployment_name=config.correlation_deployment_name,
            template_vars=self._correlation_job_template_vars(config),
        )

    @staticmethod
    def _parse_k8s_timestamp(value: object) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None

    @classmethod
    def _runtime_state_from_flinkdeployment(
        cls,
        response: dict,
    ) -> tuple[Literal["running", "starting", "failed", "missing"], datetime | None]:
        status = response.get("status") or {}
        job_state = (status.get("jobStatus") or {}).get("state")
        started_at = cls._parse_k8s_timestamp((response.get("metadata") or {}).get("creationTimestamp"))
        if job_state == "RUNNING":
            return "running", started_at
        if job_state == "FAILED":
            return "failed", None
        return "starting", None

    def _get_deployment_runtime_state(
        self,
        deployment_name: str,
    ) -> tuple[Literal["running", "starting", "failed", "missing"], datetime | None]:
        """Read one correlation/agent FlinkDeployment CR and return normalized runtime state."""
        try:
            response = self.api.get_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )
        except ApiException as e:
            if e.status == 404:
                return "missing", None
            raise

        return self._runtime_state_from_flinkdeployment(response)

    def _list_correlation_deployments(self) -> list[CorrelationDeploymentRef]:
        """List FlinkDeployments carrying the correlation-pipeline-id label (any value)."""
        response = self.api.list_namespaced_custom_object(
            group=self.FLINK_API_GROUP,
            version=self.FLINK_API_VERSION,
            namespace=self.namespace,
            plural=self.FLINK_PLURAL,
            label_selector=CORRELATION_PIPELINE_ID_LABEL,
        )

        refs: list[CorrelationDeploymentRef] = []
        for item in response.get("items", []):
            metadata = item.get("metadata") or {}
            labels = metadata.get("labels") or {}
            role = labels.get(CORRELATION_JOB_ROLE_LABEL)
            if role not in ("correlation", "agent"):
                logger.warning(
                    "Skipping correlation FlinkDeployment with unknown job role",
                    extra={"deployment_name": metadata.get("name"), "role": role},
                )
                continue
            state, _ = self._runtime_state_from_flinkdeployment(item)
            refs.append(
                CorrelationDeploymentRef(
                    deployment_name=str(metadata.get("name") or ""),
                    correlation_pipeline_id=str(labels.get(CORRELATION_PIPELINE_ID_LABEL) or ""),
                    role=role,
                    state=state,
                )
            )
        return refs

    def _delete_correlation_job_deployment(self, deployment_name: str) -> bool:
        """Delete one correlation-job FlinkDeployment by name."""
        try:
            self.api.delete_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )
            logger.info(
                "Correlation-job FlinkDeployment delete requested",
                extra={"deployment_name": deployment_name},
            )
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info(
                    "Correlation-job FlinkDeployment not found on delete",
                    extra={"deployment_name": deployment_name},
                )
                return False
            raise

    @staticmethod
    def _derive_agent_ca_mount(cert_path: str | None) -> tuple[str, str] | None:
        """Split an ``SSL_CERT_FILE`` path into ``(mount_dir, file_name)`` for the CA volume.

        The path is a *container* path, so it is parsed as POSIX regardless of
        the host the backend runs on. Returns ``None`` — no CA volume rendered —
        for anything that cannot be mounted safely: unset, relative, or a file
        directly under ``/`` (a Secret volume mounted on the container root
        would shadow the whole filesystem).
        """
        if not cert_path or not cert_path.strip():
            return None

        path = PurePosixPath(cert_path.strip())
        if not path.is_absolute() or not path.name:
            logger.warning(
                "Agent CA cert path is not an absolute file path — skipping CA mount",
                extra={"ssl_cert_file": cert_path},
            )
            return None

        mount_dir = str(path.parent)
        if mount_dir == "/":
            logger.warning(
                "Agent CA cert path sits directly under / — refusing to mount a Secret on the container root",
                extra={"ssl_cert_file": cert_path},
            )
            return None

        return mount_dir, path.name

    def _read_agent_ssl_cert_file(self) -> tuple[bool, str | None]:
        """Read ``SSL_CERT_FILE`` from the agent ConfigMap.

        Returns ``(readable, value)``. ``readable=False`` means the backend could
        not look at the ConfigMap at all — reading configmaps in the Flink
        namespace is a permission the backend is usually NOT granted (it lives in
        its own namespace and gets `flinkdeployments` there), so this is the
        common case, not the exotic one. The distinction matters: an unreadable
        ConfigMap says nothing about whether a CA is configured, while a readable
        one without the key is a definitive "no CA".
        """
        configmap_name = settings.detectflow_agent_configmap_name
        try:
            config_map = self.core_api.read_namespaced_config_map(
                name=configmap_name,
                namespace=self.namespace,
            )
        except ApiException as e:
            logger.warning(
                f"Could not read agent ConfigMap for SSL_CERT_FILE (HTTP {e.status}) — falling back to "
                f"the conventional CA path; grant the backend ServiceAccount `get` on configmaps in "
                f"this namespace, or set DETECTFLOW_AGENT_SSL_CERT_FILE, to resolve it exactly",
                extra={"configmap": configmap_name, "namespace": self.namespace, "status": e.status},
            )
            return False, None
        except Exception as e:
            # A CA lookup must never take the whole deployment down.
            logger.warning(
                "Agent ConfigMap read failed for SSL_CERT_FILE — falling back to the conventional CA path",
                extra={"configmap": configmap_name, "namespace": self.namespace, "error": str(e)},
            )
            return False, None

        return True, ((config_map.data or {}).get("SSL_CERT_FILE") or "").strip() or None

    def _agent_ca_mount(self) -> tuple[str, str, bool] | None:
        """Resolve the LLM CA volume as ``(mount_dir, secret_key, authoritative)``.

        Three sources, in order:

        1. ``DETECTFLOW_AGENT_SSL_CERT_FILE`` on the backend — an explicit operator
           decision, so it wins.
        2. ``SSL_CERT_FILE`` in the agent ConfigMap — the very value the container
           reads via ``envFrom``, so mountPath and the path httpx opens cannot drift.
        3. ``detectflow_agent_ssl_cert_file_default`` — used only when (2) could not
           be read. Blind, hence ``authoritative=False``: the caller mounts the key
           **optionally** and does NOT inline ``SSL_CERT_FILE``, so a cluster with no
           private CA at all just gets an empty directory nobody opens, while the
           convention-following one gets its bundle without any extra RBAC.

        ``None`` means no volume at all: a readable ConfigMap that has no
        ``SSL_CERT_FILE`` is a definitive "this LLM does not need a private CA".
        """
        override = (settings.detectflow_agent_ssl_cert_file or "").strip()
        if override:
            derived = self._derive_agent_ca_mount(override)
            return (*derived, True) if derived else None

        readable, cert_path = self._read_agent_ssl_cert_file()
        if readable:
            derived = self._derive_agent_ca_mount(cert_path)
            return (*derived, True) if derived else None

        derived = self._derive_agent_ca_mount(settings.detectflow_agent_ssl_cert_file_default)
        return (*derived, False) if derived else None

    def _agent_job_template_vars(self, config: CorrelationDeploymentConfig) -> dict:
        """Template variables for agent-job-deployment.yaml.j2.

        Mirrors :meth:`_correlation_job_template_vars` but feeds the agent
        image / parallelism / resources and exposes the optional dedicated
        node selector (architecture §1 + §9 — thread-mode TMs may need a
        pool of their own; default is to inherit the global flink pool).

        Also resolves the optional private-CA mount for the LLM endpoint:
        ``SSL_CERT_FILE`` (agent ConfigMap or backend override) is split into
        mountPath + Secret key so nothing about the cert location is hardcoded
        in the template.
        """
        ca_mount = self._agent_ca_mount()
        ssl_cert_mount_dir, ssl_cert_file_name, ca_mount_authoritative = ca_mount or (None, None, False)
        if ca_mount:
            logger.info(
                "Agent-job gets the LLM CA mount"
                if ca_mount_authoritative
                else "Agent-job gets the LLM CA mount on the conventional path (ConfigMap unreadable)",
                extra={
                    "mount_path": ssl_cert_mount_dir,
                    "secret": settings.detectflow_agent_secret_name,
                    "secret_key": ssl_cert_file_name,
                    "authoritative": ca_mount_authoritative,
                },
            )
        return {
            "deployment_name": config.agent_deployment_name,
            # See :meth:`_correlation_job_template_vars` — same rule, agent's own directory.
            "state_subpath": assert_state_subpath(config.agent_deployment_name),
            "correlation_pipeline_id": config.correlation_pipeline_id,
            "correlation_pipeline_name": config.correlation_pipeline_name,
            "namespace": self.namespace,
            "agent_image": settings.agent_job_image,
            "image_pull_policy": self.image_pull_policy,
            "parallelism": config.agent_parallelism,
            "taskmanager_replicas": config.agent_parallelism,
            "taskmanager_memory": f"{config.agent_taskmanager_memory_mb}m",
            "taskmanager_cpu": config.agent_taskmanager_cpu,
            "jobmanager_memory": f"{config.agent_jobmanager_memory_mb}m",
            "jobmanager_cpu": config.agent_jobmanager_cpu,
            "checkpoint_interval_sec": settings.detectflow_checkpoint_interval_sec,
            "agent_node_selector_key": settings.agent_node_selector_key,
            "agent_node_selector_value": settings.agent_node_selector_value,
            "flink_node_selector_key": settings.flink_node_selector_key,
            "flink_node_selector_value": settings.flink_node_selector_value,
            "checkpoints_pvc": settings.flink_checkpoints_pvc,
            "ha_pvc": settings.flink_ha_pvc,
            "savepoints_pvc": settings.flink_savepoints_pvc,
            # Shared cluster infra (KAFKA_BOOTSTRAP_SERVERS, KAFKA_AUTH_METHOD, log
            # defaults) — same ConfigMap + Secret reused by sigma-detector.
            "flink_config_configmap": settings.flink_config_configmap_name,
            "flink_sigma_config_secret": settings.flink_sigma_config_secret_name,
            # Agent-only LLM ConfigMap + Secret.
            "agent_configmap": settings.detectflow_agent_configmap_name,
            "agent_secret": settings.detectflow_agent_secret_name,
            "ssl_cert_file": (
                f"{ssl_cert_mount_dir}/{ssl_cert_file_name}" if ca_mount and ca_mount_authoritative else None
            ),
            "ssl_cert_mount_dir": ssl_cert_mount_dir,
            "ssl_cert_file_name": ssl_cert_file_name,
            "ssl_cert_optional": not ca_mount_authoritative,
            # Per-instance Kafka wiring (epic D2): agent reads chains-raw, writes chains-analysed.
            "chains_raw_topic": config.chains_raw_topic,
            "chains_analysed_topic": config.chains_analysed_topic,
            "activity_topic": settings.kafka_activity_topic,
            "kafka_auth_method": settings.kafka_auth_method,
        }

    def _launch_agent_job_deployment(self, config: CorrelationDeploymentConfig) -> dict[str, object]:
        """Create-or-update one correlation pipeline's agent-job FlinkDeployment."""
        _assert_k8s_names_fit(config)
        return self._upsert_flink_deployment_by_name(
            template_name="agent-job-deployment.yaml.j2",
            deployment_name=config.agent_deployment_name,
            template_vars=self._agent_job_template_vars(config),
        )

    def _delete_agent_job_deployment(self, deployment_name: str) -> bool:
        """Delete one agent-job FlinkDeployment by name."""
        try:
            self.api.delete_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )
            logger.info(
                "Agent-job FlinkDeployment delete requested",
                extra={"deployment_name": deployment_name},
            )
            return True
        except ApiException as e:
            if e.status == 404:
                logger.info(
                    "Agent-job FlinkDeployment not found on delete",
                    extra={"deployment_name": deployment_name},
                )
                return False
            raise

    def _cleanup_deployment_storage(self, state_subpath: str) -> bool:
        """Delete one deployment's checkpoint/HA/savepoint directories on the shared PVCs.

        Mounts the three PVCs at PARENT level on purpose: the directory itself must go, and a
        ``subPath`` mount can only empty it. Isolation therefore comes from the guard, not the
        mount — ``assert_state_subpath`` rejects anything that is not a single DNS-1123 label
        before the Job exists, the key travels as ``STATE_SUBPATH`` rather than being
        interpolated into the shell, and the script refuses any target that is not exactly one
        level below its mount. Callers must confirm the deployment is terminated first: a live
        TaskManager still holds the mount (see ``AttackChainsManager.stop_runtime``).

        Forces the next start into a cold start so the ``KafkaOffsetsInitializer.latest()``
        initial position applies (runtime contract: no replay of events that landed while the
        job was paused), and for agent-job so stale checkpoints cannot resume the LLM-fed
        pipeline mid-stream and re-analyse chains that were already paid for.

        The state key is the deployment name for correlation/agent jobs and the bare
        ``pipeline_id`` for detection pipelines — in both cases the directory segment that
        deployment's mounts are scoped to. Job name is ``cleanup-{state_subpath}`` (must be
        DNS-1123 and ≤ ``K8S_JOB_NAME_MAX_LEN``, which is what ``_assert_k8s_names_fit``
        guards); ``ttlSecondsAfterFinished=300`` garbage-collects finished Jobs after 5 min. A
        409 from ``create_namespaced_job`` (back-to-back stops within the TTL window) is logged
        and swallowed — the prior Job has either completed or is about to.

        Raises:
            ConfigurationError: 400 — the state key is not a single DNS-1123 label. Raised
                before any Job object is created.
        """
        assert_state_subpath(state_subpath)
        logger.info(
            "Creating deployment storage cleanup job",
            extra={"state_subpath": state_subpath},
        )

        cleanup_job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": f"{CLEANUP_JOB_NAME_PREFIX}{state_subpath}",
                "namespace": self.namespace,
            },
            "spec": {
                "ttlSecondsAfterFinished": 300,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "nodeSelector": {
                            settings.flink_node_selector_key: settings.flink_node_selector_value,
                        },
                        "containers": [
                            {
                                "name": "cleanup",
                                "image": "busybox:latest",
                                "command": ["sh", "-c", _CLEANUP_SCRIPT],
                                "env": [{"name": "STATE_SUBPATH", "value": state_subpath}],
                                "resources": {
                                    "requests": {"cpu": "100m", "memory": "128Mi"},
                                    "limits": {"cpu": "200m", "memory": "256Mi"},
                                },
                                # Parent-level, no subPath: a scoped mount could only empty the
                                # directory, and the directory itself has to go.
                                "volumeMounts": [
                                    {"name": "checkpoints", "mountPath": "/mnt/checkpoints"},
                                    {"name": "ha", "mountPath": "/mnt/ha"},
                                    {"name": "savepoints", "mountPath": "/mnt/savepoints"},
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "checkpoints",
                                "persistentVolumeClaim": {"claimName": settings.flink_checkpoints_pvc},
                            },
                            {
                                "name": "ha",
                                "persistentVolumeClaim": {"claimName": settings.flink_ha_pvc},
                            },
                            {
                                "name": "savepoints",
                                "persistentVolumeClaim": {"claimName": settings.flink_savepoints_pvc},
                            },
                        ],
                    }
                },
            },
        }

        job_name = f"{CLEANUP_JOB_NAME_PREFIX}{state_subpath}"
        batch_api = client.BatchV1Api()
        try:
            batch_api.create_namespaced_job(namespace=self.namespace, body=cleanup_job)
            logger.info(
                "Deployment storage cleanup job created",
                extra={"state_subpath": state_subpath},
            )
        except ApiException as e:
            if e.status == 409:
                # Back-to-back stops inside ttlSecondsAfterFinished hit the previous Job. Keep
                # swallowing the conflict — but watch THAT Job rather than assuming it succeeded.
                logger.warning(
                    "Deployment cleanup job already exists (within ttlSecondsAfterFinished window); "
                    "watching the existing Job",
                    extra={"state_subpath": state_subpath},
                )
            else:
                error_msg = f"Failed to create deployment cleanup job: {e}"
                logger.error(
                    error_msg,
                    extra={"state_subpath": state_subpath, "status_code": e.status},
                )
                raise Exception(error_msg) from e

        return self._watch_cleanup_job(batch_api, job_name, state_subpath)

    def _watch_cleanup_job(
        self,
        batch_api: "client.BatchV1Api",
        job_name: str,
        state_subpath: str,
    ) -> StorageCleanupOutcome:
        """Poll the cleanup Job to a terminal state, bounded by settings.

        Returns rather than raises: a surviving state directory is recoverable and a failed stop is
        not, so every caller treats this as *visible*, never fatal.
        """
        deadline = time.monotonic() + settings.flink_cleanup_job_timeout_seconds
        while time.monotonic() < deadline:
            try:
                status = batch_api.read_namespaced_job_status(name=job_name, namespace=self.namespace).status
            except ApiException as e:
                if e.status == 404:
                    # ttlSecondsAfterFinished reaped it between create and read: it ran to
                    # completion, because a failed Job is only GC'd after it finishes too — but we
                    # can no longer prove which. Report it as succeeded, the same assumption the
                    # pre-watch code made unconditionally.
                    logger.info(
                        "Cleanup job already garbage-collected; assuming it completed",
                        extra={"state_subpath": state_subpath, "job": job_name},
                    )
                    return StorageCleanupOutcome(state_subpath, "succeeded")
                logger.warning(
                    "Could not read cleanup job status",
                    extra={"state_subpath": state_subpath, "job": job_name, "error": str(e)},
                )
                return StorageCleanupOutcome(state_subpath, "timed_out", f"status unreadable: {e.reason}")

            if status and status.succeeded:
                logger.info("Deployment storage cleaned", extra={"state_subpath": state_subpath})
                return StorageCleanupOutcome(state_subpath, "succeeded")
            if status and status.failed:
                reason = self._cleanup_failure_reason(job_name, status)
                logger.warning(
                    "Storage cleanup job FAILED; the state directory still exists",
                    extra={"state_subpath": state_subpath, "job": job_name, "reason": reason},
                )
                return StorageCleanupOutcome(state_subpath, "failed", reason)
            time.sleep(_CLEANUP_POLL_INTERVAL_SECONDS)

        logger.warning(
            "Storage cleanup job did not finish in time; state may still exist",
            extra={
                "state_subpath": state_subpath,
                "job": job_name,
                "timeout": settings.flink_cleanup_job_timeout_seconds,
            },
        )
        return StorageCleanupOutcome(
            state_subpath,
            "timed_out",
            f"still running after {settings.flink_cleanup_job_timeout_seconds}s",
        )

    def _cleanup_failure_reason(self, job_name: str, status: object) -> str:
        """The refusal the script printed, or the Job's own condition message as a fallback."""
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={job_name}",
            )
            for pod in pods.items:
                log = self.core_api.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=self.namespace,
                    tail_lines=5,
                )
                if log and log.strip():
                    return log.strip().splitlines()[-1][:500]
        except Exception as e:  # noqa: BLE001 — diagnostics must never mask the outcome
            logger.debug("Could not read cleanup job logs", extra={"job": job_name, "error": str(e)})

        for condition in getattr(status, "conditions", None) or []:
            if condition.type == "Failed" and condition.message:
                return str(condition.message)[:500]
        return "cleanup job failed without a message"

    def _create_flink_deployment(self, config: FlinkDeploymentConfig) -> dict:
        """Create FlinkDeployment resource in Kubernetes."""
        deployment_name = self._generate_deployment_name(config.pipeline_id)

        quota_cpu, quota_memory = self._calculate_autoscaler_quotas(
            config.taskmanager_memory_mb,
            config.autoscaler_max_parallelism,
        )
        runtime = resolve_matcher_runtime(
            python_execution_mode=settings.matcher_python_execution_mode,
            state_backend=settings.matcher_state_backend,
        )
        taskmanager_process_memory = f"{self._calculate_taskmanager_process_memory_mb(config.taskmanager_memory_mb, runtime.python_execution_mode)}m"
        taskmanager_memory_limit_factor = self._calculate_taskmanager_memory_limit_factor(
            config.taskmanager_memory_mb, runtime.python_execution_mode
        )

        logger.info(
            "Creating FlinkDeployment",
            extra={
                "deployment_name": deployment_name,
                "pipeline_id": config.pipeline_id,
                "input_topics": config.input_topics,
                "input_topics_count": len(config.input_topics),
                "output_topic": config.output_topic,
                "initial_parallelism": config.parallelism,
                "taskmanager_replicas": config.parallelism,
                "autoscaler_enabled": config.autoscaler_enabled,
                "python_execution_mode": runtime.python_execution_mode,
                "state_backend": runtime.state_backend,
            },
        )

        # Render Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(config.pipeline_id),
            # State key for the three subPath mounts (§22): the BARE pipeline_id, not
            # ``flink-{pipeline_id}``. That is the directory segment this template has always
            # written under, so scoping the mounts to it moves no bytes; keying on the
            # deployment name instead would orphan every existing pipeline's checkpoints.
            state_subpath=assert_state_subpath(str(config.pipeline_id)),
            job_id=str(config.pipeline_id),
            input_topics=config.input_topics,
            output_topic=config.output_topic,
            kafka_output_destinations=config.kafka_output_destinations_json(),
            output_mode=config.output_mode,
            apply_parser_to_output_events=config.apply_parser_to_output_events,
            parallelism=config.parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            taskmanager_process_memory=taskmanager_process_memory,
            taskmanager_memory_limit_factor=taskmanager_memory_limit_factor,
            taskmanager_cpu=config.taskmanager_cpu,
            kafka_starting_offset=config.kafka_starting_offset,
            window_size_sec=config.window_size_sec,
            checkpoint_interval_sec=config.checkpoint_interval_sec,
            autoscaler_enabled=config.autoscaler_enabled,
            autoscaler_min_parallelism=config.autoscaler_min_parallelism,
            autoscaler_max_parallelism=config.autoscaler_max_parallelism,
            rules_topic=config.rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=config.metrics_topic or settings.kafka_metrics_topic,
            tagged_events_topic=config.tagged_events_topic or settings.kafka_tagged_events_topic,
            jobmanager_memory=self.default_config["jobmanager_memory"],
            jobmanager_cpu=self.default_config["jobmanager_cpu"],
            taskmanager_replicas=config.parallelism,
            autoscaler_quota_cpu=quota_cpu,
            autoscaler_quota_memory=quota_memory,
            kafka_auth_method=settings.kafka_auth_method,
            node_selector_key=settings.flink_node_selector_key,
            node_selector_value=settings.flink_node_selector_value,
            checkpoints_pvc=settings.flink_checkpoints_pvc,
            ha_pvc=settings.flink_ha_pvc,
            savepoints_pvc=settings.flink_savepoints_pvc,
            runtime=runtime,
        )

        manifest = yaml.safe_load(manifest_yaml)

        try:
            response = self.api.create_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                body=manifest,
            )

            logger.info(
                "FlinkDeployment created successfully",
                extra={
                    "deployment_name": deployment_name,
                    "pipeline_id": config.pipeline_id,
                },
            )

            return response

        except ApiException as e:
            if e.status == 409:
                error_msg = f"FlinkDeployment {deployment_name} already exists"
                logger.error(error_msg, extra={"pipeline_id": config.pipeline_id})
                raise Exception(error_msg) from e

            error_msg = f"Failed to create FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": config.pipeline_id, "status_code": e.status},
            )
            raise Exception(error_msg) from e

    def _update_flink_deployment(self, config: FlinkDeploymentConfig) -> dict:
        """Update FlinkDeployment resource in Kubernetes."""
        deployment_name = self._generate_deployment_name(config.pipeline_id)

        quota_cpu, quota_memory = self._calculate_autoscaler_quotas(
            config.taskmanager_memory_mb,
            config.autoscaler_max_parallelism,
        )
        runtime = resolve_matcher_runtime(
            python_execution_mode=settings.matcher_python_execution_mode,
            state_backend=settings.matcher_state_backend,
        )
        taskmanager_process_memory = f"{self._calculate_taskmanager_process_memory_mb(config.taskmanager_memory_mb, runtime.python_execution_mode)}m"
        taskmanager_memory_limit_factor = self._calculate_taskmanager_memory_limit_factor(
            config.taskmanager_memory_mb, runtime.python_execution_mode
        )

        logger.info(
            "Updating FlinkDeployment spec",
            extra={
                "deployment_name": deployment_name,
                "pipeline_id": config.pipeline_id,
                "parallelism": config.parallelism,
                "taskmanager_replicas": config.parallelism,
                "autoscaler_enabled": config.autoscaler_enabled,
                "python_execution_mode": runtime.python_execution_mode,
                "state_backend": runtime.state_backend,
            },
        )

        # Render new spec using Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(config.pipeline_id),
            # State key for the three subPath mounts (§22): the BARE pipeline_id, not
            # ``flink-{pipeline_id}``. That is the directory segment this template has always
            # written under, so scoping the mounts to it moves no bytes; keying on the
            # deployment name instead would orphan every existing pipeline's checkpoints.
            state_subpath=assert_state_subpath(str(config.pipeline_id)),
            job_id=str(config.pipeline_id),
            input_topics=config.input_topics,
            output_topic=config.output_topic,
            kafka_output_destinations=config.kafka_output_destinations_json(),
            output_mode=config.output_mode,
            apply_parser_to_output_events=config.apply_parser_to_output_events,
            parallelism=config.parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            taskmanager_process_memory=taskmanager_process_memory,
            taskmanager_memory_limit_factor=taskmanager_memory_limit_factor,
            taskmanager_cpu=config.taskmanager_cpu,
            kafka_starting_offset=config.kafka_starting_offset,
            window_size_sec=config.window_size_sec,
            checkpoint_interval_sec=config.checkpoint_interval_sec,
            autoscaler_enabled=config.autoscaler_enabled,
            autoscaler_min_parallelism=config.autoscaler_min_parallelism,
            autoscaler_max_parallelism=config.autoscaler_max_parallelism,
            rules_topic=config.rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=config.metrics_topic or settings.kafka_metrics_topic,
            tagged_events_topic=config.tagged_events_topic or settings.kafka_tagged_events_topic,
            jobmanager_memory=self.default_config["jobmanager_memory"],
            jobmanager_cpu=self.default_config["jobmanager_cpu"],
            taskmanager_replicas=config.parallelism,
            autoscaler_quota_cpu=quota_cpu,
            autoscaler_quota_memory=quota_memory,
            kafka_auth_method=settings.kafka_auth_method,
            node_selector_key=settings.flink_node_selector_key,
            node_selector_value=settings.flink_node_selector_value,
            checkpoints_pvc=settings.flink_checkpoints_pvc,
            ha_pvc=settings.flink_ha_pvc,
            savepoints_pvc=settings.flink_savepoints_pvc,
            runtime=runtime,
        )

        manifest = yaml.safe_load(manifest_yaml)

        try:
            response = self.api.patch_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
                body=manifest,
            )

            logger.info(
                "FlinkDeployment updated successfully",
                extra={
                    "deployment_name": deployment_name,
                    "pipeline_id": config.pipeline_id,
                },
            )

            return response

        except ApiException as e:
            if e.status == 404:
                error_msg = f"FlinkDeployment {deployment_name} not found"
                logger.error(error_msg, extra={"pipeline_id": config.pipeline_id})
                raise Exception(error_msg) from e

            error_msg = f"Failed to update FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": config.pipeline_id, "status_code": e.status},
            )
            raise Exception(error_msg) from e

    def _suspend_deployment(self, deployment_name: str) -> bool:
        """Set deployment state to suspended."""
        logger.info(
            "Suspending FlinkDeployment",
            extra={"deployment_name": deployment_name},
        )

        try:
            patch = {"spec": {"job": {"state": "suspended"}}}

            self.api.patch_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
                body=patch,
            )

            logger.info(
                "FlinkDeployment suspended successfully",
                extra={"deployment_name": deployment_name},
            )
            return True

        except ApiException as e:
            error_msg = f"Failed to suspend FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={
                    "deployment_name": deployment_name,
                    "status_code": e.status,
                },
            )
            raise Exception(error_msg) from e

    def _resume_deployment(self, deployment_name: str) -> bool:
        """Set deployment state to running and refresh image to current version.

        The patch includes spec.image and spec.imagePullPolicy from current settings
        so that disable/enable cycles pick up newly released matchnode images. With
        upgradeMode=last-state the operator restores from HA metadata / last
        checkpoint and the job ID is preserved.
        """
        logger.info(
            "Resuming FlinkDeployment with current image",
            extra={
                "deployment_name": deployment_name,
                "flink_image": self.flink_image,
                "image_pull_policy": self.image_pull_policy,
            },
        )

        try:
            patch = {
                "spec": {
                    "image": self.flink_image,
                    "imagePullPolicy": self.image_pull_policy,
                    "job": {"state": "running"},
                }
            }

            self.api.patch_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
                body=patch,
            )

            logger.info(
                "FlinkDeployment resumed successfully",
                extra={
                    "deployment_name": deployment_name,
                    "flink_image": self.flink_image,
                },
            )
            return True

        except ApiException as e:
            error_msg = f"Failed to resume FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={
                    "deployment_name": deployment_name,
                    "status_code": e.status,
                },
            )
            raise Exception(error_msg) from e

    def _get_deployment_status(self, deployment_name: str) -> FlinkDeploymentStatus | None:
        """Fetch deployment status from Kubernetes."""
        logger.debug(
            "Fetching FlinkDeployment status",
            extra={"deployment_name": deployment_name},
        )

        try:
            response = self.api.get_namespaced_custom_object(
                group=self.FLINK_API_GROUP,
                version=self.FLINK_API_VERSION,
                namespace=self.namespace,
                plural=self.FLINK_PLURAL,
                name=deployment_name,
            )

            status = response.get("status", {})
            job_state = status.get("jobStatus", {}).get("state", "unknown")
            lifecycle_state = status.get("lifecycleState", "unknown")

            # Determine normalized state
            # For streaming pipelines, CANCELED/FINISHED are error states
            if job_state == "RUNNING" and lifecycle_state == "STABLE":
                state = "running"
            elif status.get("error") or job_state in ("CANCELED", "FINISHED"):
                state = "failed"
            else:
                state = job_state.lower() if job_state != "unknown" else lifecycle_state.lower()

            result = FlinkDeploymentStatus(
                state=state,
                job_id=status.get("jobStatus", {}).get("jobId"),
                lifecycle_state=lifecycle_state,
                error=status.get("error"),
                job_manager_status=status.get("jobManagerDeploymentStatus", "unknown"),
            )

            logger.debug(
                "Retrieved FlinkDeployment status",
                extra={"deployment_name": deployment_name, "state": result.state},
            )

            return result

        except ApiException as e:
            if e.status == 404:
                logger.warning(
                    "FlinkDeployment not found",
                    extra={"deployment_name": deployment_name},
                )
                return None

            error_msg = f"Failed to get FlinkDeployment status: {e}"
            logger.error(
                error_msg,
                extra={
                    "deployment_name": deployment_name,
                    "status_code": e.status,
                },
            )
            raise Exception(error_msg) from e

    def _get_pod_warnings(self, deployment_name: str, max_events: int = 10) -> list[str]:
        """Fetch warning events for deployment pods."""
        warnings = []
        try:
            events = self.core_api.list_namespaced_event(
                namespace=self.namespace,
                field_selector="type=Warning",
            )

            for event in events.items:
                involved_name = event.involved_object.name or ""
                if not involved_name.startswith(deployment_name):
                    continue

                message = event.message or ""
                if message and message not in warnings:
                    warnings.append(message)

                    if len(warnings) >= max_events:
                        break

            logger.debug(
                "Retrieved pod warnings",
                extra={
                    "deployment_name": deployment_name,
                    "warnings_count": len(warnings),
                },
            )

        except ApiException as e:
            logger.warning(
                "Failed to get pod events",
                extra={"deployment_name": deployment_name, "error": str(e)},
            )

        return warnings

    def _wait_for_deletion(self, deployment_name: str, timeout: int = 60) -> None:
        """Wait for FlinkDeployment to be fully deleted."""
        poll_interval = 2
        elapsed = 0

        logger.info(
            "Waiting for FlinkDeployment to be fully deleted",
            extra={"deployment_name": deployment_name, "timeout": timeout},
        )

        while elapsed < timeout:
            try:
                self.api.get_namespaced_custom_object(
                    group=self.FLINK_API_GROUP,
                    version=self.FLINK_API_VERSION,
                    namespace=self.namespace,
                    plural=self.FLINK_PLURAL,
                    name=deployment_name,
                )
                time.sleep(poll_interval)
                elapsed += poll_interval
                logger.debug(
                    "FlinkDeployment still terminating",
                    extra={"deployment_name": deployment_name, "elapsed": elapsed},
                )
            except ApiException as e:
                if e.status == 404:
                    logger.info(
                        "FlinkDeployment fully deleted",
                        extra={"deployment_name": deployment_name, "elapsed": elapsed},
                    )
                    return
                raise

        error_msg = f"Timeout waiting for FlinkDeployment {deployment_name} to be deleted after {timeout}s"
        logger.error(error_msg, extra={"deployment_name": deployment_name})
        raise Exception(error_msg)

    def _normalize_image_pull_policy(self, policy: str) -> str:
        """Normalize and validate image pull policy value."""
        if not policy:
            return "Always"

        policy_lower = policy.lower()
        valid_policies = {
            "always": "Always",
            "ifnotpresent": "IfNotPresent",
            "never": "Never",
        }

        normalized = valid_policies.get(policy_lower)
        if normalized:
            return normalized

        logger.warning(
            f"Invalid imagePullPolicy value: '{policy}'. "
            "Valid values are: Always, IfNotPresent, Never. Using default: Always",
            extra={"invalid_policy": policy},
        )
        return "Always"

    def _generate_deployment_name(self, pipeline_id: str) -> str:
        """Generate Kubernetes-compatible deployment name from pipeline ID."""
        return f"flink-{str(pipeline_id).lower()}"
