"""Kubernetes service for managing Flink deployments.

This module provides a service layer for interacting with Kubernetes API
to manage FlinkDeployment custom resources for ETL pipelines.
"""

import asyncio
import time
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer

logger = get_logger(__name__)


def _log_flink_error_async(
    pipeline_id: str,
    details: str,
    entity_type: str = "flink",
    severity: str = "error",
) -> None:
    """Log Flink/K8s error to activity producer from sync context.

    Uses asyncio.create_task if event loop is running, otherwise skips.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(
            activity_producer.log_action(
                action="error",
                entity_type=entity_type,
                entity_id=pipeline_id,
                details=details,
                source="system",
                severity=severity,
            )
        )
    except RuntimeError:
        # No event loop running - just log to file
        logger.debug("Cannot log to activity producer: no event loop running")


class KubernetesService:
    """Service for managing Flink deployments via Kubernetes API.

    This service handles the lifecycle of FlinkDeployment CRDs including
    creation, deletion, updates, and status monitoring. It uses Jinja2
    templates to generate deployment manifests and interacts with the
    Kubernetes API to manage resources.

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

        # Default resource configuration from settings
        jobmanager_cpu = settings.flink_jobmanager_cpu
        jobmanager_memory_gb = settings.flink_jobmanager_memory_gb

        self.default_config = {
            "jobmanager_memory": f"{int(jobmanager_memory_gb * 1024)}m",
            "jobmanager_cpu": jobmanager_cpu,
            "taskmanager_replicas": 1,
            "initial_parallelism": settings.flink_taskmanager_slots,  # 1 replica × slots
        }

        logger.info(
            "KubernetesService initialized",
            extra={
                "namespace": namespace,
                "flink_image": flink_image,
                "image_pull_policy": self.image_pull_policy,
            },
        )

    def create_flink_deployment(
        self,
        pipeline_id: str,
        pipeline_name: str,
        input_topics: list[str],
        output_topic: str,
        parallelism: int | None = None,
        output_mode: str = "matched_only",
        apply_parser_to_output_events: bool = False,
        # Resource configuration (optional - uses defaults if not provided)
        taskmanager_memory_mb: int | None = None,
        taskmanager_cpu: float | None = None,
        window_size_sec: int | None = None,
        checkpoint_interval_sec: int | None = None,
        autoscaler_enabled: bool | None = None,
        autoscaler_min_parallelism: int | None = None,
        autoscaler_max_parallelism: int | None = None,
        rules_topic: str | None = None,
        metrics_topic: str | None = None,
    ) -> dict:
        """Create a FlinkDeployment CRD for a pipeline.

        Generates a FlinkDeployment manifest from a Jinja2 template and
        creates it in Kubernetes. The deployment uses the pipeline ID for
        resource isolation and labeling.

        Args:
            pipeline_id: UUID of the pipeline (used as job_id).
            pipeline_name: Name of the pipeline.
            input_topics: List of Kafka input topic names (will be unioned in Flink).
            output_topic: Kafka output topic name.
            parallelism: Initial job parallelism (default: 4 for autoscaling).
                Note: With autoscaling enabled, Flink will automatically adjust
                this value based on workload. The value here is just the starting point.
            output_mode: Output mode (matched_only or all_events).
            apply_parser_to_output_events: Apply parser to output events (default: False).
            taskmanager_memory_mb: Memory per TaskManager in MB (default: 2048).
            taskmanager_cpu: CPU cores per TaskManager (default: 1.0).
            window_size_sec: Window size in seconds (default: 30).
            checkpoint_interval_sec: Checkpoint interval in seconds (default: 60).
            autoscaler_enabled: Enable Flink autoscaler (default: False).
            autoscaler_min_parallelism: Min parallelism for autoscaler (default: 1).
            autoscaler_max_parallelism: Max parallelism for autoscaler (default: 24).
            rules_topic: Kafka topic for Sigma rules (default: from settings).
            metrics_topic: Kafka topic for per-rule metrics (default: from settings).

        Returns:
            dict: Created FlinkDeployment resource from Kubernetes API.

        Raises:
            Exception: If deployment already exists or creation fails.

        Example:
            >>> service = KubernetesService()
            >>> result = service.create_flink_deployment(
            ...     pipeline_id="uuid-here",
            ...     pipeline_name="My Pipeline",
            ...     input_topics=["raw-events", "windows-events"],
            ...     output_topic="detections",
            ...     parallelism=2
            ... )
        """
        deployment_name = self._generate_deployment_name(pipeline_id)

        # Use default initial_parallelism if not provided
        # For autoscaling: start with 4 (1 replica × 4 slots), autoscaler will adjust
        if parallelism is None:
            parallelism = self.default_config["initial_parallelism"]

        # Apply defaults for optional resource parameters
        # These defaults come from FlinkDefaultsResponse system defaults
        tm_memory_mb = taskmanager_memory_mb if taskmanager_memory_mb is not None else 2048
        tm_cpu = taskmanager_cpu if taskmanager_cpu is not None else 1.0
        window_sec = window_size_sec if window_size_sec is not None else 30
        checkpoint_sec = checkpoint_interval_sec if checkpoint_interval_sec is not None else 60
        autoscaler_on = autoscaler_enabled if autoscaler_enabled is not None else False
        autoscaler_min = autoscaler_min_parallelism if autoscaler_min_parallelism is not None else 1
        autoscaler_max = autoscaler_max_parallelism if autoscaler_max_parallelism is not None else 24

        # Calculate autoscaler resource quotas per-pipeline
        # CPU quota = max parallelism (1 core per parallel slot)
        # Memory quota = number of TaskManagers needed at max scale × memory per TM
        if settings.autoscaler_quota_cpu is not None:
            quota_cpu = str(settings.autoscaler_quota_cpu)
        else:
            quota_cpu = str(float(autoscaler_max))

        if settings.autoscaler_quota_memory_gb is not None:
            quota_memory = f"{int(settings.autoscaler_quota_memory_gb)}g"
        else:
            tm_count = -(-autoscaler_max // parallelism)  # ceil division
            quota_memory_mb = tm_count * tm_memory_mb
            quota_memory = f"{quota_memory_mb}m"

        logger.info(
            "Creating FlinkDeployment",
            extra={
                "deployment_name": deployment_name,
                "pipeline_id": pipeline_id,
                "input_topics": input_topics,
                "input_topics_count": len(input_topics),
                "output_topic": output_topic,
                "initial_parallelism": parallelism,
                "autoscaler_max_parallelism": autoscaler_max,
                "taskmanager_memory_mb": tm_memory_mb,
                "taskmanager_cpu": tm_cpu,
                "autoscaler_enabled": autoscaler_on,
                "checkpoint_interval_sec": checkpoint_sec,
                "autoscaler_quota_cpu": quota_cpu,
                "autoscaler_quota_memory": quota_memory,
            },
        )

        # Render Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(pipeline_id),
            job_id=str(pipeline_id),
            input_topics=input_topics,
            output_topic=output_topic,
            output_mode=output_mode,
            apply_parser_to_output_events=apply_parser_to_output_events,
            parallelism=parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            # Resource configuration
            taskmanager_memory=f"{tm_memory_mb}m",
            taskmanager_cpu=tm_cpu,
            window_size_sec=window_sec,
            checkpoint_interval_sec=checkpoint_sec,
            autoscaler_enabled=autoscaler_on,
            autoscaler_min_parallelism=autoscaler_min,
            autoscaler_max_parallelism=autoscaler_max,
            # Kafka topics for rules and metrics
            rules_topic=rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=metrics_topic or settings.kafka_metrics_topic,
            # JobManager and replicas config
            jobmanager_memory=self.default_config["jobmanager_memory"],
            jobmanager_cpu=self.default_config["jobmanager_cpu"],
            taskmanager_replicas=self.default_config["taskmanager_replicas"],
            # Autoscaler quotas (dynamic per-pipeline)
            autoscaler_quota_cpu=quota_cpu,
            autoscaler_quota_memory=quota_memory,
            # Kafka authentication (for SSL certificate volumes)
            kafka_auth_method=settings.kafka_auth_method,
            # Node selector for pod scheduling
            node_selector_key=settings.flink_node_selector_key,
            node_selector_value=settings.flink_node_selector_value,
            # PVC names for state storage
            checkpoints_pvc=settings.flink_checkpoints_pvc,
            ha_pvc=settings.flink_ha_pvc,
            savepoints_pvc=settings.flink_savepoints_pvc,
        )

        # Parse YAML to dict
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
                    "pipeline_id": pipeline_id,
                },
            )

            return response

        except ApiException as e:
            if e.status == 409:
                error_msg = f"FlinkDeployment {deployment_name} already exists"
                logger.error(error_msg, extra={"pipeline_id": pipeline_id})
                if ErrorTracker.should_log(f"flink_create_conflict_{pipeline_id}"):
                    _log_flink_error_async(pipeline_id, error_msg)
                raise Exception(error_msg) from e

            error_msg = f"Failed to create FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": pipeline_id, "status_code": e.status},
            )
            if ErrorTracker.should_log(f"flink_create_{pipeline_id}"):
                _log_flink_error_async(pipeline_id, error_msg)
            raise Exception(error_msg) from e

    def update_flink_deployment(
        self,
        pipeline_id: str,
        pipeline_name: str,
        input_topics: list[str],
        output_topic: str,
        parallelism: int | None = None,
        output_mode: str = "matched_only",
        apply_parser_to_output_events: bool = False,
        taskmanager_memory_mb: int | None = None,
        taskmanager_cpu: float | None = None,
        window_size_sec: int | None = None,
        checkpoint_interval_sec: int | None = None,
        autoscaler_enabled: bool | None = None,
        autoscaler_min_parallelism: int | None = None,
        autoscaler_max_parallelism: int | None = None,
        rules_topic: str | None = None,
        metrics_topic: str | None = None,
    ) -> dict:
        """Update an existing FlinkDeployment spec.

        Patches the FlinkDeployment with new configuration. The Flink Operator
        will automatically handle the upgrade using the configured upgradeMode
        (last-state), preserving checkpoint state.

        Args:
            Same as create_flink_deployment.

        Returns:
            dict: Updated FlinkDeployment resource from Kubernetes API.

        Raises:
            Exception: If deployment doesn't exist or update fails.
        """
        deployment_name = self._generate_deployment_name(pipeline_id)

        if parallelism is None:
            parallelism = self.default_config["initial_parallelism"]

        tm_memory_mb = taskmanager_memory_mb if taskmanager_memory_mb is not None else 2048
        tm_cpu = taskmanager_cpu if taskmanager_cpu is not None else 1.0
        window_sec = window_size_sec if window_size_sec is not None else 30
        checkpoint_sec = checkpoint_interval_sec if checkpoint_interval_sec is not None else 60
        autoscaler_on = autoscaler_enabled if autoscaler_enabled is not None else False
        autoscaler_min = autoscaler_min_parallelism if autoscaler_min_parallelism is not None else 1
        autoscaler_max = autoscaler_max_parallelism if autoscaler_max_parallelism is not None else 24

        # Calculate autoscaler resource quotas per-pipeline
        if settings.autoscaler_quota_cpu is not None:
            quota_cpu = str(settings.autoscaler_quota_cpu)
        else:
            quota_cpu = str(float(autoscaler_max))

        if settings.autoscaler_quota_memory_gb is not None:
            quota_memory = f"{int(settings.autoscaler_quota_memory_gb)}g"
        else:
            tm_count = -(-autoscaler_max // parallelism)  # ceil division
            quota_memory_mb = tm_count * tm_memory_mb
            quota_memory = f"{quota_memory_mb}m"

        logger.info(
            "Updating FlinkDeployment spec",
            extra={
                "deployment_name": deployment_name,
                "pipeline_id": pipeline_id,
                "parallelism": parallelism,
                "autoscaler_enabled": autoscaler_on,
                "autoscaler_quota_cpu": quota_cpu,
                "autoscaler_quota_memory": quota_memory,
            },
        )

        # Render new spec using Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(pipeline_id),
            job_id=str(pipeline_id),
            input_topics=input_topics,
            output_topic=output_topic,
            output_mode=output_mode,
            apply_parser_to_output_events=apply_parser_to_output_events,
            parallelism=parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            taskmanager_memory=f"{tm_memory_mb}m",
            taskmanager_cpu=tm_cpu,
            window_size_sec=window_sec,
            checkpoint_interval_sec=checkpoint_sec,
            autoscaler_enabled=autoscaler_on,
            autoscaler_min_parallelism=autoscaler_min,
            autoscaler_max_parallelism=autoscaler_max,
            rules_topic=rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=metrics_topic or settings.kafka_metrics_topic,
            jobmanager_memory=self.default_config["jobmanager_memory"],
            jobmanager_cpu=self.default_config["jobmanager_cpu"],
            taskmanager_replicas=self.default_config["taskmanager_replicas"],
            autoscaler_quota_cpu=quota_cpu,
            autoscaler_quota_memory=quota_memory,
            # Kafka authentication (for SSL certificate volumes)
            kafka_auth_method=settings.kafka_auth_method,
            # Node selector for pod scheduling
            node_selector_key=settings.flink_node_selector_key,
            node_selector_value=settings.flink_node_selector_value,
            # PVC names for state storage
            checkpoints_pvc=settings.flink_checkpoints_pvc,
            ha_pvc=settings.flink_ha_pvc,
            savepoints_pvc=settings.flink_savepoints_pvc,
        )

        manifest = yaml.safe_load(manifest_yaml)

        try:
            # Use strategic merge patch to update the deployment
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
                    "pipeline_id": pipeline_id,
                },
            )

            return response

        except ApiException as e:
            if e.status == 404:
                error_msg = f"FlinkDeployment {deployment_name} not found"
                logger.error(error_msg, extra={"pipeline_id": pipeline_id})
                if ErrorTracker.should_log(f"flink_update_notfound_{pipeline_id}"):
                    _log_flink_error_async(pipeline_id, error_msg)
                raise Exception(error_msg) from e

            error_msg = f"Failed to update FlinkDeployment: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": pipeline_id, "status_code": e.status},
            )
            if ErrorTracker.should_log(f"flink_update_{pipeline_id}"):
                _log_flink_error_async(pipeline_id, error_msg)
            raise Exception(error_msg) from e

    def find_deployment_by_pipeline_id(self, pipeline_id: str) -> str | None:
        """Find FlinkDeployment by pipeline-id label.

        Searches for a FlinkDeployment with the matching pipeline-id label.
        Only one deployment should exist per pipeline.

        Args:
            pipeline_id: UUID of the pipeline.

        Returns:
            str: FlinkDeployment name if found, None otherwise.

        Raises:
            Exception: If API call fails (except 404).

        Example:
            >>> service = KubernetesService()
            >>> name = service.find_deployment_by_pipeline_id("uuid-here")
            >>> print(name)  # "flink-uuid-here" or None
        """
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
            if ErrorTracker.should_log(f"flink_find_{pipeline_id}"):
                _log_flink_error_async(pipeline_id, error_msg)
            raise Exception(error_msg) from e

    def delete_flink_deployment(self, deployment_name: str, wait: bool = True, timeout: int = 60) -> bool:
        """Delete a FlinkDeployment CRD.

        Deletes the specified FlinkDeployment. The Flink Operator will
        automatically create a savepoint before stopping the job.

        Args:
            deployment_name: Name of the FlinkDeployment to delete.
            wait: If True, wait for the resource to be fully deleted.
            timeout: Maximum seconds to wait for deletion (default: 60).

        Returns:
            bool: True if deleted successfully or already doesn't exist.

        Raises:
            Exception: If deletion fails (except 404) or timeout waiting.

        Example:
            >>> service = KubernetesService()
            >>> success = service.delete_flink_deployment("flink-uuid-here")
        """
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

            # Wait for resource to be fully deleted
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
            if ErrorTracker.should_log(f"flink_delete_{deployment_name}"):
                _log_flink_error_async(deployment_name, error_msg)
            raise Exception(error_msg) from e

    def _wait_for_deletion(self, deployment_name: str, timeout: int = 60) -> None:
        """Wait for FlinkDeployment to be fully deleted.

        Polls the Kubernetes API until the resource returns 404 or timeout.

        Args:
            deployment_name: Name of the FlinkDeployment.
            timeout: Maximum seconds to wait.

        Raises:
            Exception: If timeout exceeded waiting for deletion.
        """
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
                # Resource still exists, wait and retry
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
        if ErrorTracker.should_log(f"flink_delete_timeout_{deployment_name}"):
            _log_flink_error_async(deployment_name, error_msg)
        raise Exception(error_msg)

    def delete_flink_deployment_by_pipeline_id(self, pipeline_id: str) -> bool:
        """Find and delete FlinkDeployment by pipeline ID.

        Combines find and delete operations to remove a FlinkDeployment
        using the pipeline-id label.

        Args:
            pipeline_id: UUID of the pipeline.

        Returns:
            bool: True if deleted successfully or doesn't exist.

        Raises:
            Exception: If deletion fails.

        Example:
            >>> service = KubernetesService()
            >>> success = service.delete_flink_deployment_by_pipeline_id("uuid")
        """
        logger.info(
            "Deleting FlinkDeployment by pipeline ID",
            extra={"pipeline_id": pipeline_id},
        )

        deployment_name = self.find_deployment_by_pipeline_id(pipeline_id)
        if deployment_name:
            return self.delete_flink_deployment(deployment_name)

        logger.info(
            "No FlinkDeployment found to delete",
            extra={"pipeline_id": pipeline_id},
        )
        return True

    def get_deployment_status(self, deployment_name: str) -> dict | None:
        """Get FlinkDeployment status information.

        Retrieves the current status of a FlinkDeployment including job state,
        lifecycle state, job ID, and any errors.

        Args:
            deployment_name: Name of the FlinkDeployment.

        Returns:
            dict: Status information with keys:
                - state: Job state (RUNNING, FAILED, etc.)
                - lifecycle_state: Lifecycle state (STABLE, DEPLOYING, etc.)
                - job_id: Flink job ID
                - error: Error message if any
            None if deployment doesn't exist.

        Raises:
            Exception: If API call fails (except 404).

        Example:
            >>> service = KubernetesService()
            >>> status = service.get_deployment_status("flink-uuid-here")
            >>> print(status["state"])  # "RUNNING"
        """
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
            result = {
                "state": status.get("jobStatus", {}).get("state", "unknown"),
                "lifecycle_state": status.get("lifecycleState", "unknown"),
                "job_id": status.get("jobStatus", {}).get("jobId"),
                "error": status.get("error"),
                "job_manager_status": status.get("jobManagerDeploymentStatus", "unknown"),
            }

            logger.debug(
                "Retrieved FlinkDeployment status",
                extra={"deployment_name": deployment_name, **result},
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
            if ErrorTracker.should_log(f"flink_status_{deployment_name}"):
                _log_flink_error_async(deployment_name, error_msg)
            raise Exception(error_msg) from e

    def suspend_deployment(self, deployment_name: str) -> bool:
        """Suspend a FlinkDeployment (savepoint + stop).

        Patches the deployment to set job state to "suspended", which triggers
        the Flink Operator to create a savepoint and stop the job gracefully.

        Args:
            deployment_name: Name of the FlinkDeployment to suspend.

        Returns:
            bool: True if suspended successfully.

        Raises:
            Exception: If suspend operation fails.

        Example:
            >>> service = KubernetesService()
            >>> success = service.suspend_deployment("flink-uuid-here")
        """
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
            if ErrorTracker.should_log(f"flink_suspend_{deployment_name}"):
                _log_flink_error_async(deployment_name, error_msg)
            raise Exception(error_msg) from e

    def resume_deployment(self, deployment_name: str) -> bool:
        """Resume a suspended FlinkDeployment (restore from savepoint).

        Patches the deployment to set job state to "running", which triggers
        the Flink Operator to restore from the latest savepoint and start the job.

        Args:
            deployment_name: Name of the FlinkDeployment to resume.

        Returns:
            bool: True if resumed successfully.

        Raises:
            Exception: If resume operation fails.

        Example:
            >>> service = KubernetesService()
            >>> success = service.resume_deployment("flink-uuid-here")
        """
        logger.info(
            "Resuming FlinkDeployment",
            extra={"deployment_name": deployment_name},
        )

        try:
            patch = {"spec": {"job": {"state": "running"}}}

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
                extra={"deployment_name": deployment_name},
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
            if ErrorTracker.should_log(f"flink_resume_{deployment_name}"):
                _log_flink_error_async(deployment_name, error_msg)
            raise Exception(error_msg) from e

    def get_pod_warnings(self, deployment_name: str, max_events: int = 10) -> list[str]:
        """Get warning events for pods related to a FlinkDeployment.

        Fetches Kubernetes events filtered by deployment name and Warning type.
        Useful for detecting scheduling issues like node affinity mismatches.

        Args:
            deployment_name: Name of the FlinkDeployment.
            max_events: Maximum number of warning events to return.

        Returns:
            list[str]: List of warning messages from recent events.

        Example:
            >>> service = KubernetesService()
            >>> warnings = service.get_pod_warnings("flink-uuid-here")
            >>> print(warnings)
            ["0/3 nodes available: node affinity mismatch"]
        """
        warnings = []
        try:
            # Get events for all pods with this deployment name prefix
            events = self.core_api.list_namespaced_event(
                namespace=self.namespace,
                field_selector="type=Warning",
            )

            for event in events.items:
                # Filter by deployment name (pods are named {deployment_name}-*)
                involved_name = event.involved_object.name or ""
                if not involved_name.startswith(deployment_name):
                    continue

                # Get unique warning messages
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

    def cleanup_pipeline_storage(self, pipeline_id: str) -> bool:
        """Clean up savepoints, checkpoints, and HA storage for a pipeline.

        Creates a Kubernetes Job that removes all storage associated with
        a pipeline, including savepoints, checkpoints, and HA data.

        Note:
            This performs aggressive cleanup (Variant 1) - all storage is
            deleted immediately.

        Todo:
            Implement Variant 2 with soft delete and retention policy:
            1. Add deleted_at field to Pipeline model
            2. Create CronJob for cleanup after N days
            3. Allow recovery of accidentally deleted pipelines

        Args:
            pipeline_id: UUID of the pipeline.

        Returns:
            bool: True if cleanup job created successfully.

        Raises:
            Exception: If cleanup job creation fails (except 409 conflict).

        Example:
            >>> service = KubernetesService()
            >>> success = service.cleanup_pipeline_storage("uuid-here")
        """
        logger.info(
            "Creating storage cleanup job",
            extra={"pipeline_id": pipeline_id},
        )

        try:
            cleanup_job = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {
                    "name": f"cleanup-{pipeline_id}",
                    "namespace": self.namespace,
                },
                "spec": {
                    "ttlSecondsAfterFinished": 300,  # Auto-delete job after 5 min
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
                                    "command": [
                                        "sh",
                                        "-c",
                                        f"rm -rf /mnt/checkpoints/{pipeline_id} "
                                        f"/mnt/ha/{pipeline_id} "
                                        f"/mnt/savepoints/{pipeline_id} || true",
                                    ],
                                    "resources": {
                                        "requests": {"cpu": "100m", "memory": "128Mi"},
                                        "limits": {"cpu": "200m", "memory": "256Mi"},
                                    },
                                    "volumeMounts": [
                                        {
                                            "name": "checkpoints",
                                            "mountPath": "/mnt/checkpoints",
                                        },
                                        {
                                            "name": "ha",
                                            "mountPath": "/mnt/ha",
                                        },
                                        {
                                            "name": "savepoints",
                                            "mountPath": "/mnt/savepoints",
                                        },
                                    ],
                                }
                            ],
                            "volumes": [
                                {
                                    "name": "checkpoints",
                                    "persistentVolumeClaim": {"claimName": "flink-checkpoints-pvc"},
                                },
                                {
                                    "name": "ha",
                                    "persistentVolumeClaim": {"claimName": "flink-ha-pvc"},
                                },
                                {
                                    "name": "savepoints",
                                    "persistentVolumeClaim": {"claimName": "flink-savepoints-pvc"},
                                },
                            ],
                        }
                    },
                },
            }

            # Create cleanup job
            batch_api = client.BatchV1Api()
            batch_api.create_namespaced_job(
                namespace=self.namespace,
                body=cleanup_job,
            )

            logger.info(
                "Storage cleanup job created successfully",
                extra={"pipeline_id": pipeline_id},
            )
            return True

        except ApiException as e:
            if e.status == 409:
                # Job already exists, that's ok
                logger.warning(
                    "Cleanup job already exists",
                    extra={"pipeline_id": pipeline_id},
                )
                return True

            error_msg = f"Failed to create cleanup job: {e}"
            logger.error(
                error_msg,
                extra={"pipeline_id": pipeline_id, "status_code": e.status},
            )
            if ErrorTracker.should_log(f"k8s_cleanup_{pipeline_id}"):
                _log_flink_error_async(pipeline_id, error_msg, entity_type="kubernetes")
            raise Exception(error_msg) from e

    def _normalize_image_pull_policy(self, policy: str) -> str:
        """Normalize and validate image pull policy value.

        Kubernetes imagePullPolicy values are case-sensitive. This method
        normalizes common case variations and validates the value.
        """
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
        """Generate Kubernetes-compatible deployment name from pipeline ID.

        Creates a valid Kubernetes resource name following naming conventions:
        - Lowercase alphanumeric characters and hyphens only
        - Maximum 253 characters
        - Must start and end with alphanumeric character

        Args:
            pipeline_id: UUID of the pipeline.

        Returns:
            str: Kubernetes-compatible name (e.g., "flink-550e8400-e29b-41d4-...").

        Example:
            >>> service = KubernetesService()
            >>> name = service._generate_deployment_name("550e8400-e29b-41d4")
            >>> print(name)  # "flink-550e8400-e29b-41d4"
        """
        return f"flink-{str(pipeline_id).lower()}"

    def check_resource_quota_available(
        self, requested_cpu: float, requested_memory_gb: float
    ) -> tuple[bool, str | None]:
        """Check if ResourceQuota has enough capacity for new pipeline.

        Queries the namespace ResourceQuota and calculates if requested resources
        would exceed the hard limits. This provides fast-fail feedback before
        attempting to create pods.

        Note:
            This is a best-effort check and not 100% race-condition free.
            Kubernetes ResourceQuota remains the source of truth and will
            reject pod creation if limits are exceeded at scheduling time.

        Args:
            requested_cpu: CPU cores requested (e.g., 3.0 for JM+TM).
            requested_memory_gb: Memory in GB requested (e.g., 5 for JM+TM).

        Returns:
            tuple[bool, Optional[str]]: (can_create, error_message)
                - can_create: True if resources appear available
                - error_message: User-friendly error if insufficient resources

        Example:
            >>> service = KubernetesService()
            >>> can_create, error = service.check_resource_quota_available(3.0, 5.0)
            >>> if not can_create:
            ...     print(error)  # "Insufficient memory: 7GB used + 5GB requested > 8GB limit"
        """
        try:
            core_api = client.CoreV1Api()

            # Get ResourceQuota (assumes single quota named flink-pipelines-quota)
            quota_name = "flink-pipelines-quota"
            quota = core_api.read_namespaced_resource_quota(name=quota_name, namespace=self.namespace)

            if not quota.status or not quota.status.hard or not quota.status.used:
                # No quota configured - allow creation
                logger.warning(
                    "ResourceQuota not found or not initialized",
                    extra={"quota_name": quota_name, "namespace": self.namespace},
                )
                return True, None

            hard = quota.status.hard
            used = quota.status.used

            # Parse CPU (format: "6" or "6000m")
            def parse_cpu(value: str) -> float:
                if value.endswith("m"):
                    return float(value[:-1]) / 1000
                return float(value)

            # Parse memory (format: "8Gi" or "8192Mi")
            def parse_memory_gb(value: str) -> float:
                if value.endswith("Gi"):
                    return float(value[:-2])
                elif value.endswith("Mi"):
                    return float(value[:-2]) / 1024
                elif value.endswith("G"):
                    return float(value[:-1])
                elif value.endswith("M"):
                    return float(value[:-1]) / 1024
                return float(value) / (1024**3)  # Assume bytes

            # Get CPU limits
            cpu_hard = parse_cpu(hard.get("requests.cpu", "0"))
            cpu_used = parse_cpu(used.get("requests.cpu", "0"))
            cpu_available = cpu_hard - cpu_used

            # Get memory limits
            memory_hard_gb = parse_memory_gb(hard.get("requests.memory", "0"))
            memory_used_gb = parse_memory_gb(used.get("requests.memory", "0"))
            memory_available_gb = memory_hard_gb - memory_used_gb

            logger.info(
                "ResourceQuota check",
                extra={
                    "cpu_used": cpu_used,
                    "cpu_available": cpu_available,
                    "cpu_hard": cpu_hard,
                    "cpu_requested": requested_cpu,
                    "memory_used_gb": round(memory_used_gb, 2),
                    "memory_available_gb": round(memory_available_gb, 2),
                    "memory_hard_gb": memory_hard_gb,
                    "memory_requested_gb": requested_memory_gb,
                },
            )

            # Check if resources available (with small buffer for rounding)
            if requested_cpu > cpu_available + 0.1:
                return False, (
                    f"Insufficient CPU quota: {cpu_used:.1f} CPU used + "
                    f"{requested_cpu:.1f} CPU requested > {cpu_hard:.1f} CPU limit. "
                    f"Please disable another pipeline or increase cluster capacity."
                )

            if requested_memory_gb > memory_available_gb + 0.1:
                return False, (
                    f"Insufficient memory quota: {memory_used_gb:.1f}GB used + "
                    f"{requested_memory_gb:.1f}GB requested > {memory_hard_gb:.1f}GB limit. "
                    f"Please disable another pipeline or increase cluster capacity."
                )

            return True, None

        except ApiException as e:
            if e.status == 404:
                # ResourceQuota not found - allow creation (no limits configured)
                logger.warning(
                    "ResourceQuota not found - allowing creation",
                    extra={"namespace": self.namespace},
                )
                return True, None

            # Other API errors - log but allow (fail open for availability)
            logger.error(
                "Failed to check ResourceQuota",
                extra={"error": str(e), "status": e.status},
            )
            if ErrorTracker.should_log("k8s_quota_check"):
                _log_flink_error_async("quota", f"Failed to check ResourceQuota: {str(e)}", entity_type="kubernetes")
            return True, None  # Fail open - let Kubernetes enforce at pod creation

        except Exception as e:
            # Parsing errors or other issues - fail open
            logger.error(
                "Unexpected error checking ResourceQuota",
                extra={"error": str(e)},
            )
            if ErrorTracker.should_log("k8s_quota_check_unexpected"):
                _log_flink_error_async(
                    "quota", f"Unexpected error checking ResourceQuota: {str(e)}", entity_type="kubernetes"
                )
            return True, None
