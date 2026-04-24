"""Confluent Manager for Apache Flink (CMF) provider.

This module provides CMFFlinkProvider that implements FlinkProvider
interface using CMF REST API for Flink application management.
"""

import json
from pathlib import Path

import httpx
from jinja2 import Environment, FileSystemLoader

from apps.clients.cmf_api import CMFApiClient
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer
from apps.providers.base import (
    FlinkDeploymentConfig,
    FlinkDeploymentResult,
    FlinkDeploymentStatus,
    FlinkProvider,
    get_default_jobmanager_config,
)

logger = get_logger(__name__)


# CMF status mapping to normalized states (matches K8s provider output)
# For streaming pipelines, CANCELED/FINISHED are error states
CMF_STATUS_MAP = {
    "RUNNING": "running",
    "SUSPENDED": "disabled",
    "STARTING": "deploying",
    "STOPPING": "reconciling",
    "FAILED": "failed",
    "CANCELED": "failed",
    "FINISHED": "failed",
    "UNKNOWN": "unknown",
}

# Mapping CMF status to K8s-compatible job_manager_status
# This makes status_details output consistent between providers
CMF_JOB_MANAGER_STATUS_MAP = {
    "RUNNING": "READY",
    "SUSPENDED": "READY",
    "STARTING": "DEPLOYING",
    "STOPPING": "READY",
    "FAILED": "ERROR",
    "CANCELED": "ERROR",
    "FINISHED": "ERROR",
    "UNKNOWN": "MISSING",
}

# Mapping CMF status to K8s-compatible lifecycle_state
CMF_LIFECYCLE_STATE_MAP = {
    "RUNNING": "STABLE",
    "SUSPENDED": "SUSPENDED",
    "STARTING": "DEPLOYING",
    "STOPPING": "STOPPING",
    "FAILED": "FAILED",
    "CANCELED": "FAILED",
    "FINISHED": "FAILED",
    "UNKNOWN": "UNKNOWN",
}


class CMFFlinkProvider(FlinkProvider):
    """Flink provider using Confluent Manager for Apache Flink.

    Manages Flink applications through CMF REST API with mTLS authentication.

    Attributes:
        client: CMF API client instance
        environment: CMF environment name
        jinja_env: Jinja2 template environment
    """

    def __init__(self):
        """Initialize CMF provider with configuration from settings."""
        self.client = CMFApiClient(
            base_url=settings.cmf_url,
            cert_path=settings.cmf_client_cert_path,
            key_path=settings.cmf_client_key_path,
            ca_path=settings.cmf_ca_cert_path,
        )
        self.environment = settings.cmf_environment
        self.namespace = settings.cmf_namespace
        # Use CMF-specific image if configured, otherwise fall back to default
        self.flink_image = settings.flink_image_cmf or settings.flink_image

        # Jinja2 template environment
        template_dir = Path(__file__).parent.parent / "templates"
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Default resource configuration from settings
        self.default_config = get_default_jobmanager_config()

        logger.info(
            "CMFFlinkProvider initialized",
            extra={
                "cmf_url": settings.cmf_url,
                "environment": self.environment,
            },
        )

    def _generate_app_name(self, pipeline_id: str) -> str:
        """Generate CMF application name from pipeline ID."""
        return f"pipeline-{str(pipeline_id).lower()}"

    def _render_manifest(self, config: FlinkDeploymentConfig) -> dict:
        """Render CMF application manifest from template.

        Args:
            config: Deployment configuration

        Returns:
            Parsed JSON manifest as dict
        """
        app_name = self._generate_app_name(config.pipeline_id)

        template = self.jinja_env.get_template("cmf-application.json.j2")
        manifest_json = template.render(
            app_name=app_name,
            pipeline_id=str(config.pipeline_id),
            job_id=str(config.pipeline_id),
            input_topics=config.input_topics,
            output_topic=config.output_topic,
            output_mode=config.output_mode,
            apply_parser_to_output_events=config.apply_parser_to_output_events,
            parallelism=config.parallelism,
            flink_image=self.flink_image,
            image_pull_policy=settings.image_pull_policy,
            taskmanager_memory=f"{config.taskmanager_memory_mb}m",
            taskmanager_cpu=config.taskmanager_cpu,
            kafka_starting_offset=config.kafka_starting_offset,
            window_size_sec=config.window_size_sec,
            checkpoint_interval_sec=config.checkpoint_interval_sec,
            rules_topic=config.rules_topic or settings.kafka_sigma_rules_topic,
            metrics_topic=config.metrics_topic or settings.kafka_metrics_topic,
            jobmanager_memory=self.default_config["jobmanager_memory"],
            jobmanager_cpu=self.default_config["jobmanager_cpu"],
            kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
            kafka_auth_method=settings.kafka_auth_method,
        )

        return json.loads(manifest_json)

    # =========================================================================
    # FlinkProvider Interface Implementation
    # =========================================================================

    async def create_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Create a Flink application in CMF."""
        app_name = self._generate_app_name(config.pipeline_id)

        try:
            manifest = self._render_manifest(config)

            logger.info(
                "Creating CMF Flink application",
                extra={
                    "app_name": app_name,
                    "pipeline_id": config.pipeline_id,
                    "environment": self.environment,
                },
            )

            result = await self.client.create_application(self.environment, manifest)

            return FlinkDeploymentResult(
                deployment_name=app_name,
                success=True,
                raw_response=result,
            )

        except httpx.TimeoutException as e:
            error_msg = f"CMF API timeout after {self.client.timeout}s - check CMF_URL and connectivity"
            logger.error(
                error_msg,
                extra={
                    "app_name": app_name,
                    "pipeline_id": config.pipeline_id,
                    "cmf_url": settings.cmf_url,
                    "exception": str(e),
                },
            )
        except Exception as e:
            error_msg = f"Failed to create CMF application: {e}"
            logger.error(
                error_msg,
                extra={
                    "app_name": app_name,
                    "pipeline_id": config.pipeline_id,
                },
            )
            if ErrorTracker.should_log(f"cmf_create_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="cmf",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=app_name,
                success=False,
                error=str(e),
            )

    async def update_deployment(self, config: FlinkDeploymentConfig) -> FlinkDeploymentResult:
        """Update an existing Flink application in CMF."""
        app_name = self._generate_app_name(config.pipeline_id)

        try:
            manifest = self._render_manifest(config)

            logger.info(
                "Updating CMF Flink application",
                extra={
                    "app_name": app_name,
                    "pipeline_id": config.pipeline_id,
                    "environment": self.environment,
                },
            )

            result = await self.client.update_application(self.environment, app_name, manifest)

            return FlinkDeploymentResult(
                deployment_name=app_name,
                success=True,
                raw_response=result,
            )

        except Exception as e:
            error_msg = f"Failed to update CMF application: {e}"
            logger.error(
                error_msg,
                extra={
                    "app_name": app_name,
                    "pipeline_id": config.pipeline_id,
                },
            )
            if ErrorTracker.should_log(f"cmf_update_{config.pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="cmf",
                    entity_id=config.pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return FlinkDeploymentResult(
                deployment_name=app_name,
                success=False,
                error=str(e),
            )

    async def delete_deployment(self, pipeline_id: str, wait: bool = True) -> bool:  # noqa: ARG002
        """Delete a Flink application from CMF.

        Note: wait parameter is ignored for CMF - deletion is synchronous.
        """
        app_name = self._generate_app_name(pipeline_id)

        try:
            return await self.client.delete_application(self.environment, app_name)
        except Exception as e:
            error_msg = f"Failed to delete CMF application: {e}"
            logger.error(
                error_msg,
                extra={
                    "app_name": app_name,
                    "pipeline_id": pipeline_id,
                },
            )
            if ErrorTracker.should_log(f"cmf_delete_{pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="cmf",
                    entity_id=pipeline_id,
                    details=error_msg,
                    source="system",
                    severity="error",
                )
            return False

    async def suspend_deployment(self, pipeline_id: str) -> bool:
        """Suspend a running Flink application."""
        app_name = self._generate_app_name(pipeline_id)

        try:
            return await self.client.suspend_application(self.environment, app_name)
        except Exception as e:
            logger.error(
                "Failed to suspend CMF application",
                extra={
                    "app_name": app_name,
                    "pipeline_id": pipeline_id,
                    "error": str(e),
                },
            )
            return False

    async def resume_deployment(self, pipeline_id: str) -> bool:
        """Resume a suspended Flink application with current image version.

        Before starting, the manifest is refreshed with the current image and
        imagePullPolicy from settings so that disable/enable cycles pick up
        newly released matchnode images. CMF stores the image at create time,
        so plain /start would otherwise restart with the cached tag.
        """
        app_name = self._generate_app_name(pipeline_id)

        try:
            existing = await self.client.get_application(self.environment, app_name)
            if existing is None:
                logger.warning(
                    "CMF application not found on resume",
                    extra={"app_name": app_name, "pipeline_id": pipeline_id},
                )
                return False

            spec = existing.setdefault("spec", {})
            current_image = spec.get("image")
            if current_image != self.flink_image or spec.get("imagePullPolicy") != settings.image_pull_policy:
                spec["image"] = self.flink_image
                spec["imagePullPolicy"] = settings.image_pull_policy

                logger.info(
                    "Refreshing CMF application image before resume",
                    extra={
                        "app_name": app_name,
                        "pipeline_id": pipeline_id,
                        "previous_image": current_image,
                        "new_image": self.flink_image,
                    },
                )

                # Strip server-managed fields that PUT must not echo back
                existing.pop("status", None)
                metadata = existing.get("metadata") or {}
                for field in ("resourceVersion", "uid", "creationTimestamp", "generation", "managedFields"):
                    metadata.pop(field, None)

                await self.client.update_application(self.environment, app_name, existing)

            return await self.client.start_application(self.environment, app_name)
        except Exception as e:
            logger.error(
                "Failed to start CMF application",
                extra={
                    "app_name": app_name,
                    "pipeline_id": pipeline_id,
                    "error": str(e),
                },
            )
            return False

    async def get_deployment_status(self, pipeline_id: str) -> FlinkDeploymentStatus | None:
        """Get the status of a Flink application."""
        app_name = self._generate_app_name(pipeline_id)

        try:
            status = await self.client.get_application_status(self.environment, app_name)
            if not status:
                return None

            # Map CMF status to normalized state
            cmf_state = status.state.upper()
            normalized_state = CMF_STATUS_MAP.get(cmf_state, status.state.lower())

            # Map to K8s-compatible job_manager_status and lifecycle_state
            # This ensures consistent status_details output between K8s and CMF providers
            job_manager_status = CMF_JOB_MANAGER_STATUS_MAP.get(cmf_state, "MISSING")
            lifecycle_state = CMF_LIFECYCLE_STATE_MAP.get(cmf_state, cmf_state)

            return FlinkDeploymentStatus(
                state=normalized_state,
                job_id=status.job_id,
                lifecycle_state=lifecycle_state,
                error=status.error,
                job_manager_status=job_manager_status,
            )

        except Exception as e:
            logger.error(
                "Failed to get CMF application status",
                extra={
                    "app_name": app_name,
                    "pipeline_id": pipeline_id,
                    "error": str(e),
                },
            )
            return None

    async def find_deployment(self, pipeline_id: str) -> str | None:
        """Find application name by pipeline ID.

        In CMF, the app name is deterministic based on pipeline ID.
        We verify it exists by checking the API.
        """
        app_name = self._generate_app_name(pipeline_id)

        try:
            app = await self.client.get_application(self.environment, app_name)
            return app_name if app else None
        except Exception as e:
            logger.warning(
                "Failed to find CMF application",
                extra={
                    "app_name": app_name,
                    "pipeline_id": pipeline_id,
                    "error": str(e),
                },
            )
            return None

    async def cleanup_storage(self, pipeline_id: str) -> bool:
        """Clean up storage for a pipeline.

        Note: CMF manages its own storage. This is a no-op for CMF provider,
        but we return True to indicate "cleanup successful".
        """
        logger.info(
            "Storage cleanup requested for CMF application (no-op)",
            extra={"pipeline_id": pipeline_id},
        )
        return True

    def get_pod_warnings(self, pipeline_id: str, max_events: int = 10) -> list[str]:  # noqa: ARG002
        """Get warning events for a deployment.

        Note: CMF doesn't expose Kubernetes events directly.
        This returns an empty list for CMF provider.
        """
        _ = pipeline_id, max_events  # Unused - CMF doesn't expose K8s events
        return []

    def get_rest_url(self, pipeline_id: str) -> str:
        """Get Flink REST API URL for a pipeline."""
        app_name = self._generate_app_name(pipeline_id)
        return f"http://{app_name}-rest.{self.namespace}.svc.cluster.local:8081"
