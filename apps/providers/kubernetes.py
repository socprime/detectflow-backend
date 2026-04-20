"""Kubernetes Flink Operator provider for managing Flink deployments.

This module provides KubernetesFlinkProvider that implements FlinkProvider
interface using Kubernetes Flink Operator (FlinkDeployment CRDs).
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
from apps.providers.base import (
    FlinkDeploymentConfig,
    FlinkDeploymentResult,
    FlinkDeploymentStatus,
    FlinkProvider,
    FlinkQuotaExceededError,
    get_default_jobmanager_config,
)

logger = get_logger(__name__)


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

    async def cleanup_storage(self, pipeline_id: str) -> bool:
        """Clean up savepoints, checkpoints, and HA storage for a pipeline."""
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

    def cleanup_pipeline_storage(self, pipeline_id: str) -> bool:
        """Clean up savepoints, checkpoints, and HA storage for a pipeline."""
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
            raise Exception(error_msg) from e

    # =========================================================================
    # Private Methods
    # =========================================================================

    def _create_flink_deployment(self, config: FlinkDeploymentConfig) -> dict:
        """Create FlinkDeployment resource in Kubernetes."""
        deployment_name = self._generate_deployment_name(config.pipeline_id)

        quota_cpu, quota_memory = self._calculate_autoscaler_quotas(
            config.taskmanager_memory_mb,
            config.autoscaler_max_parallelism,
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
            },
        )

        # Render Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(config.pipeline_id),
            job_id=str(config.pipeline_id),
            input_topics=config.input_topics,
            output_topic=config.output_topic,
            output_mode=config.output_mode,
            apply_parser_to_output_events=config.apply_parser_to_output_events,
            parallelism=config.parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            taskmanager_memory=f"{config.taskmanager_memory_mb}m",
            taskmanager_cpu=config.taskmanager_cpu,
            kafka_starting_offset=config.kafka_starting_offset,
            window_size_sec=config.window_size_sec,
            checkpoint_interval_sec=config.checkpoint_interval_sec,
            autoscaler_enabled=config.autoscaler_enabled,
            autoscaler_min_parallelism=config.autoscaler_min_parallelism,
            autoscaler_max_parallelism=config.autoscaler_max_parallelism,
            rules_topic=config.rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=config.metrics_topic or settings.kafka_metrics_topic,
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

        logger.info(
            "Updating FlinkDeployment spec",
            extra={
                "deployment_name": deployment_name,
                "pipeline_id": config.pipeline_id,
                "parallelism": config.parallelism,
                "taskmanager_replicas": config.parallelism,
                "autoscaler_enabled": config.autoscaler_enabled,
            },
        )

        # Render new spec using Jinja2 template
        template = self.jinja_env.get_template("flink-deployment.yaml.j2")
        manifest_yaml = template.render(
            deployment_name=deployment_name,
            namespace=self.namespace,
            pipeline_id=str(config.pipeline_id),
            job_id=str(config.pipeline_id),
            input_topics=config.input_topics,
            output_topic=config.output_topic,
            output_mode=config.output_mode,
            apply_parser_to_output_events=config.apply_parser_to_output_events,
            parallelism=config.parallelism,
            flink_image=self.flink_image,
            image_pull_policy=self.image_pull_policy,
            taskmanager_memory=f"{config.taskmanager_memory_mb}m",
            taskmanager_cpu=config.taskmanager_cpu,
            kafka_starting_offset=config.kafka_starting_offset,
            window_size_sec=config.window_size_sec,
            checkpoint_interval_sec=config.checkpoint_interval_sec,
            autoscaler_enabled=config.autoscaler_enabled,
            autoscaler_min_parallelism=config.autoscaler_min_parallelism,
            autoscaler_max_parallelism=config.autoscaler_max_parallelism,
            rules_topic=config.rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=config.metrics_topic or settings.kafka_metrics_topic,
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
