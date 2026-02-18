"""Pipeline orchestrator for managing ETL pipeline lifecycle.

This module provides the PipelineOrchestrator class that handles all business logic
for pipeline operations including CRUD, Flink deployment management, and Kafka sync.
"""

from dataclasses import dataclass
from uuid import UUID

from fastapi import HTTPException, status
from kubernetes.client.rest import ApiException
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.logger import get_logger
from apps.core.models import Pipeline, User
from apps.core.schemas import (
    PipelineCreateRequest,
    PipelineDetailResponse,
    PipelineListItem,
    PipelineListResponse,
    PipelineResponse,
    PipelineUpdateRequest,
)
from apps.core.settings import settings
from apps.managers.custom_fields import CustomFieldsOrchestrator
from apps.managers.dashboard import dashboard_service
from apps.managers.filter import FiltersOrchestrator
from apps.managers.flink_config import FlinkConfigManager
from apps.managers.parser_sync import ParsersOrchestrator
from apps.managers.pipeline import KubernetesService
from apps.managers.pipeline_status import pipeline_status_manager
from apps.managers.rule import RulesOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.filter import FilterDAO
from apps.modules.postgre.log_source import LogSourceDAO
from apps.modules.postgre.metrics import MetricsDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO
from apps.modules.postgre.repository import RepositoryDAO

logger = get_logger(__name__)

# Fields that require Flink restart when changed on enabled pipeline
# Other fields (name, filters, log_source_id, repository_ids) are hot-reloadable via Kafka
FLINK_RESTART_FIELDS = {
    "source_topics",  # Different Kafka source topics
    "destination_topic",  # Different output topic
    "parallelism",  # Flink parallelism change requires restart
    "save_untagged",  # Changes output behavior
    "apply_parser_to_output_events",  # Changes output behavior
    # Resource configuration fields (all require restart)
    "taskmanager_memory_mb",
    "taskmanager_cpu",
    "window_size_sec",
    "checkpoint_interval_sec",
    "autoscaler_enabled",
    "autoscaler_min_parallelism",
    "autoscaler_max_parallelism",
}

# Fields that trigger storage cleanup when changed (subset of FLINK_RESTART_FIELDS)
# (cleanup = delete checkpoints/savepoints, effectively reset consumer offsets)
CLEANUP_TRIGGER_FIELDS = {
    "source_topics",  # New sources = need fresh start, old offsets are invalid
}


@dataclass
class PipelineChanges:
    """Result of comparing pipeline update request with existing pipeline."""

    update_data: dict  # Fields to update in DB
    flink_restart_needed: bool  # Need to recreate FlinkDeployment
    cleanup_needed: bool  # Need to reset offsets (delete checkpoints)
    filters_changed: bool  # Filters array changed (hot-reloadable)
    log_source_changed: bool  # LogSource changed (hot-reloadable)
    old_log_source_id: UUID | None  # Previous log_source_id
    new_log_source_id: UUID | None  # New log_source_id
    custom_fields_changed: bool  # Custom fields changed (hot-reloadable)
    old_custom_fields: str | None  # Previous custom_fields
    new_custom_fields: str | None  # New custom_fields


def detect_pipeline_changes(pipeline: PipelineUpdateRequest, existing: Pipeline) -> PipelineChanges:
    """Detect all changes between update request and existing pipeline.

    Args:
        pipeline: Update request with new values
        existing: Current pipeline from database

    Returns:
        PipelineChanges with all detected changes and flags
    """
    update_data = {}
    changed_fields = set()

    # Check each field for changes
    if pipeline.name is not None and pipeline.name != existing.name:
        update_data["name"] = pipeline.name
        changed_fields.add("name")

    # Compare source_topics as sets (order-independent)
    if pipeline.source_topics is not None:
        existing_topics = set(existing.source_topics) if existing.source_topics else set()
        new_topics = set(pipeline.source_topics)
        if new_topics != existing_topics:
            update_data["source_topics"] = pipeline.source_topics
            changed_fields.add("source_topics")

    if pipeline.destination_topic is not None and pipeline.destination_topic != existing.destination_topic:
        update_data["destination_topic"] = pipeline.destination_topic
        changed_fields.add("destination_topic")

    if pipeline.save_untagged is not None and pipeline.save_untagged != existing.save_untagged:
        update_data["save_untagged"] = pipeline.save_untagged
        changed_fields.add("save_untagged")

    if (
        pipeline.apply_parser_to_output_events is not None
        and pipeline.apply_parser_to_output_events != existing.apply_parser_to_output_events
    ):
        update_data["apply_parser_to_output_events"] = pipeline.apply_parser_to_output_events
        changed_fields.add("apply_parser_to_output_events")

    # NOTE: resources field removed from PipelineUpdateRequest - changing parallelism/memory
    # after pipeline creation causes Flink checkpoint incompatibility (maxParallelism mismatch).
    # To change resources, delete and recreate the pipeline.

    # Custom fields (hot-reloadable via Kafka)
    custom_fields_changed = False
    old_custom_fields = existing.custom_fields
    new_custom_fields = old_custom_fields

    if pipeline.custom_fields is not None:
        # Empty string means clear custom_fields, otherwise update if changed
        new_custom_fields = pipeline.custom_fields if pipeline.custom_fields else None
        if new_custom_fields != existing.custom_fields:
            update_data["custom_fields"] = new_custom_fields
            custom_fields_changed = True

    # Handle enabled separately (special case)
    if pipeline.enabled is not None:
        update_data["enabled"] = pipeline.enabled

    # Filters (hot-reloadable via Kafka)
    filters_changed = False
    if pipeline.filters is not None:
        existing_filters = {str(f) for f in existing.filters} if existing.filters else set()
        new_filters = set(pipeline.filters)
        if new_filters != existing_filters:
            update_data["filters"] = [UUID(f) for f in pipeline.filters]
            filters_changed = True

    # LogSource (hot-reloadable via Kafka - parser + mapping)
    log_source_changed = False
    old_log_source_id = existing.log_source_id
    new_log_source_id = old_log_source_id

    if pipeline.log_source_id is not None:
        new_log_source_id = UUID(pipeline.log_source_id)
        if new_log_source_id != existing.log_source_id:
            update_data["log_source_id"] = new_log_source_id
            log_source_changed = True

    return PipelineChanges(
        update_data=update_data,
        flink_restart_needed=bool(changed_fields & FLINK_RESTART_FIELDS),
        cleanup_needed=bool(changed_fields & CLEANUP_TRIGGER_FIELDS),
        filters_changed=filters_changed,
        log_source_changed=log_source_changed,
        old_log_source_id=old_log_source_id,
        new_log_source_id=new_log_source_id,
        custom_fields_changed=custom_fields_changed,
        old_custom_fields=old_custom_fields,
        new_custom_fields=new_custom_fields,
    )


class PipelineNotFoundError(Exception):
    """Raised when pipeline is not found."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        super().__init__(f"Pipeline with id {pipeline_id} not found")


class ResourceNotFoundError(Exception):
    """Raised when a required resource is not found."""

    def __init__(self, resource_type: str, resource_id: str):
        self.resource_type = resource_type
        self.resource_id = resource_id
        super().__init__(f"{resource_type} with id {resource_id} not found")


class PipelineOrchestrator:
    """Orchestrator for pipeline lifecycle management.

    Coordinates between Kubernetes, Kafka, and PostgreSQL to manage
    ETL pipeline operations including create, update, delete, enable/disable.
    """

    def __init__(self, db: AsyncSession):
        """Initialize orchestrator with database session.

        Args:
            db: SQLAlchemy async session for database operations
        """
        self.db = db
        self.pipeline_repo = PipelineDAO(db)
        self.pipeline_rules_repo = PipelineRulesDAO(db)
        self.log_source_repo = LogSourceDAO(db)
        self.filter_repo = FilterDAO(db)
        self.repository_repo = RepositoryDAO(db)
        self.config_dao = ConfigDAO(db)
        self.k8s = KubernetesService()

    def _get_rules_orchestrator(self) -> RulesOrchestrator:
        """Get RulesOrchestrator instance."""
        return RulesOrchestrator(self.db)

    def _get_filters_orchestrator(self) -> FiltersOrchestrator:
        """Get FiltersOrchestrator instance."""
        return FiltersOrchestrator(self.db)

    def _get_parsers_orchestrator(self) -> ParsersOrchestrator:
        """Get ParsersOrchestrator instance."""
        return ParsersOrchestrator(self.db)

    def _get_custom_fields_orchestrator(self) -> CustomFieldsOrchestrator:
        """Get CustomFieldsOrchestrator instance."""
        return CustomFieldsOrchestrator(self.db)

    async def _validate_resources(
        self,
        log_source_id: str,
        filter_ids: list[str] | None = None,
        repository_ids: list[str] | None = None,
    ) -> None:
        """Validate that all referenced resources exist.

        Args:
            log_source_id: UUID string of log source
            filter_ids: Optional list of filter UUID strings
            repository_ids: Optional list of repository UUID strings

        Raises:
            ResourceNotFoundError: If any resource is not found
        """
        # Validate log_source exists
        log_source_uuid = UUID(log_source_id)
        log_source = await self.log_source_repo.get_by_id(log_source_uuid)
        if not log_source:
            raise ResourceNotFoundError("Log source", log_source_id)

        # Validate filters exist
        if filter_ids:
            for filter_id in filter_ids:
                filter_uuid = UUID(filter_id)
                filter_obj = await self.filter_repo.get_by_id(filter_uuid)
                if not filter_obj:
                    raise ResourceNotFoundError("Filter", filter_id)

        # Validate repositories exist
        if repository_ids:
            for repo_id in repository_ids:
                repo_uuid = UUID(repo_id)
                repository = await self.repository_repo.get_by_id(repo_uuid)
                if not repository:
                    raise ResourceNotFoundError("Repository", repo_id)

    async def create_pipeline(
        self,
        request: PipelineCreateRequest,
        user: User,
    ) -> PipelineResponse:
        """Create a new ETL pipeline.

        Creates pipeline in database and optionally deploys Flink job if enabled.

        Args:
            request: Pipeline creation request with configuration
            user: Current user for activity logging

        Returns:
            PipelineResponse with created pipeline ID

        Raises:
            ResourceNotFoundError: If log source, filter, or repository not found
            HTTPException: If Flink deployment fails
        """
        logger.info(f"Creating pipeline: {request.name}")

        # Validate resources exist
        await self._validate_resources(
            log_source_id=request.log_source_id,
            filter_ids=request.filters,
            repository_ids=request.repository_ids,
        )

        # Resolve Flink resource configuration
        # Priority: request values → global defaults → system defaults
        flink_defaults = await self.config_dao.get_flink_defaults()
        resolved_config = FlinkConfigManager.resolve_config(
            request_config=request.resources,
            pipeline=None,  # New pipeline, no existing values
            defaults=flink_defaults,
        )

        # Prepare create data
        create_data = {
            "name": request.name,
            "source_topics": request.source_topics,
            "destination_topic": request.destination_topic,
            "log_source_id": UUID(request.log_source_id),
            "enabled": request.enabled,
            "save_untagged": request.save_untagged,
            "apply_parser_to_output_events": request.apply_parser_to_output_events,
            "filters": request.filters or [],
            "custom_fields": request.custom_fields,
            # Kafka topics from settings
            "rules_topic": settings.kafka_sigma_rules_topic,
            "metrics_topic": settings.kafka_metrics_topic,
            "namespace": settings.kubernetes_namespace,
            # Flink resource configuration (store resolved values)
            "parallelism": resolved_config.parallelism,
            "taskmanager_memory_mb": resolved_config.taskmanager_memory_mb,
            "taskmanager_cpu": resolved_config.taskmanager_cpu,
            "window_size_sec": resolved_config.window_size_sec,
            "checkpoint_interval_sec": resolved_config.checkpoint_interval_sec,
            "autoscaler_enabled": resolved_config.autoscaler_enabled,
            "autoscaler_min_parallelism": resolved_config.autoscaler_min_parallelism,
            "autoscaler_max_parallelism": resolved_config.autoscaler_max_parallelism,
        }

        # Create pipeline in database
        new_pipeline = await self.pipeline_repo.create(**create_data)

        # Add repositories and rules
        await self._get_rules_orchestrator().handle_pipeline_creation(
            pipeline_id=new_pipeline.id,
            repository_ids=[UUID(repo_id) for repo_id in request.repository_ids] if request.repository_ids else [],
        )

        # Send filters to Kafka (if pipeline is enabled and has filters)
        if request.enabled and request.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_creation(
                    pipeline_id=new_pipeline.id,
                    filter_ids=[UUID(f) for f in request.filters],
                )
            except Exception as e:
                logger.warning(
                    "Failed to send filters to Kafka (non-critical)",
                    extra={"pipeline_id": str(new_pipeline.id), "error": str(e)},
                )

        # Send custom_fields to Kafka (if pipeline is enabled and has custom_fields)
        if request.enabled and request.custom_fields:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_creation(
                    pipeline_id=new_pipeline.id,
                    custom_fields=request.custom_fields,
                )
            except Exception as e:
                logger.warning(
                    "Failed to send custom_fields to Kafka (non-critical)",
                    extra={"pipeline_id": str(new_pipeline.id), "error": str(e)},
                )

        # Create FlinkDeployment if pipeline is enabled
        if request.enabled:
            await self._create_flink_deployment(new_pipeline)

        # Refresh to ensure response has latest state
        await self.db.refresh(new_pipeline)

        # Log activity
        source_topics_str = ", ".join(new_pipeline.source_topics) if new_pipeline.source_topics else ""
        await activity_producer.log_action(
            action="create",
            entity_type="pipeline",
            entity_id=str(new_pipeline.id),
            entity_name=new_pipeline.name,
            user=user,
            details=f"Created pipeline from [{source_topics_str}] to {new_pipeline.destination_topic}",
        )

        return PipelineResponse(
            id=str(new_pipeline.id),
            status=True,
            message="Pipeline created successfully",
        )

    async def _create_flink_deployment(self, pipeline: Pipeline) -> None:
        """Create Flink deployment for enabled pipeline.

        Args:
            pipeline: Pipeline model instance

        Raises:
            HTTPException: If Flink deployment fails
        """
        flink_created = False

        try:
            result = self.k8s.create_flink_deployment(
                pipeline_id=str(pipeline.id),
                pipeline_name=pipeline.name,
                input_topics=pipeline.source_topics,
                output_topic=pipeline.destination_topic,
                parallelism=pipeline.parallelism,
                output_mode="all_events" if pipeline.save_untagged else "matched_only",
                apply_parser_to_output_events=pipeline.apply_parser_to_output_events,
                # Resource configuration
                taskmanager_memory_mb=pipeline.taskmanager_memory_mb,
                taskmanager_cpu=pipeline.taskmanager_cpu,
                window_size_sec=pipeline.window_size_sec,
                checkpoint_interval_sec=pipeline.checkpoint_interval_sec,
                autoscaler_enabled=pipeline.autoscaler_enabled,
                autoscaler_min_parallelism=pipeline.autoscaler_min_parallelism,
                autoscaler_max_parallelism=pipeline.autoscaler_max_parallelism,
                rules_topic=pipeline.rules_topic,
                metrics_topic=pipeline.metrics_topic,
            )
            flink_created = True

            # Update deployment_name in database
            deployment_name = result.get("metadata", {}).get("name", f"flink-{pipeline.id}")
            await self.pipeline_repo.update(pipeline.id, deployment_name=deployment_name)

            logger.info(
                "FlinkDeployment created successfully",
                extra={
                    "pipeline_id": str(pipeline.id),
                    "deployment_name": deployment_name,
                },
            )

            # Send parser to Kafka
            try:
                await self._get_parsers_orchestrator().handle_pipeline_creation(pipeline.id)
            except Exception as parser_err:
                logger.warning(
                    "Failed to send parser to Kafka (non-critical)",
                    extra={"pipeline_id": str(pipeline.id), "error": str(parser_err)},
                )

        except Exception as e:
            logger.error(
                "Failed to create FlinkDeployment or update DB",
                extra={"pipeline_id": str(pipeline.id), "error": str(e)},
            )

            # Compensating transaction: cleanup created resources
            if flink_created:
                try:
                    logger.info(
                        "Rolling back: Deleting FlinkDeployment",
                        extra={"pipeline_id": str(pipeline.id)},
                    )
                    self.k8s.delete_flink_deployment_by_pipeline_id(str(pipeline.id))
                except Exception as rollback_error:
                    logger.error(
                        "Failed to delete FlinkDeployment during rollback",
                        extra={
                            "pipeline_id": str(pipeline.id),
                            "rollback_error": str(rollback_error),
                        },
                    )

            if isinstance(e, HTTPException):
                raise
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create FlinkDeployment: {str(e)}",
            ) from e

    async def update_pipeline(
        self,
        pipeline_id: UUID,
        request: PipelineUpdateRequest,
        user: User,
    ) -> PipelineResponse:
        """Update an existing pipeline.

        Handles 5 scenarios based on enabled state and config changes:
        1. Disabling pipeline (true → false)
        2. Enabling pipeline (false → true)
        3. Pipeline stays disabled
        4. Pipeline stays enabled + config changed (restart needed)
        5. Pipeline stays enabled + hot-reload only

        Args:
            pipeline_id: UUID of pipeline to update
            request: Update request with new values
            user: Current user for activity logging

        Returns:
            PipelineResponse with updated pipeline ID

        Raises:
            PipelineNotFoundError: If pipeline not found
            HTTPException: If Flink operations fail
        """
        logger.info("Updating pipeline", extra={"pipeline_id": str(pipeline_id)})

        existing = await self.pipeline_repo.get_with_relations(pipeline_id)
        if not existing:
            logger.warning("Pipeline not found for update", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        old_enabled = existing.enabled
        old_filter_ids = list(existing.filters) if existing.filters else []
        old_repo_ids = [str(repo.id) for repo in existing.repositories]

        # Detect all changes
        changes = detect_pipeline_changes(request, existing)

        # Update pipeline in database
        updated = await self.pipeline_repo.update(pipeline_id, **changes.update_data)

        # Handle repository_ids changes
        if request.repository_ids is not None:
            await self._handle_repository_changes(
                pipeline_id=pipeline_id,
                old_repo_ids=old_repo_ids,
                new_repo_ids=request.repository_ids,
            )

        # Handle filter changes
        if request.filters is not None and updated.enabled:
            await self._handle_filter_changes(
                pipeline_id=pipeline_id,
                old_filter_ids=old_filter_ids,
                new_filter_ids=request.filters,
            )

        new_enabled = updated.enabled

        try:
            # Execute appropriate scenario
            if old_enabled and not new_enabled:
                await self._handle_disable_scenario(pipeline_id)
            elif not old_enabled and new_enabled:
                await self._handle_enable_scenario(pipeline_id, updated)
            elif not old_enabled and not new_enabled:
                await self._handle_stays_disabled_scenario(pipeline_id)
            elif old_enabled and new_enabled and changes.flink_restart_needed:
                await self._handle_restart_scenario(pipeline_id, updated, changes)
            else:
                await self._handle_hot_reload_scenario(pipeline_id, changes)

        except Exception as e:
            logger.error(
                "Failed to update FlinkDeployment or DB",
                extra={"pipeline_id": str(pipeline_id), "error": str(e)},
            )

            if isinstance(e, HTTPException):
                raise

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update FlinkDeployment: {str(e)}",
            ) from e

        # Refresh to get final state
        updated = await self.pipeline_repo.get_by_id(pipeline_id)

        # Log activity
        await self._log_update_activity(user, updated, old_enabled, new_enabled)

        return PipelineResponse(id=str(updated.id), status=True, message="Pipeline updated successfully")

    async def _handle_repository_changes(
        self,
        pipeline_id: UUID,
        old_repo_ids: list[str],
        new_repo_ids: list[str],
    ) -> None:
        """Handle repository changes for pipeline."""
        current_repo_ids = set(old_repo_ids)
        new_repo_ids_set = set(new_repo_ids)

        added_repo_ids = new_repo_ids_set - current_repo_ids
        removed_repo_ids = current_repo_ids - new_repo_ids_set

        await self._get_rules_orchestrator().handle_pipeline_update(
            pipeline_id=pipeline_id,
            added_repo_ids=[UUID(repo_id) for repo_id in added_repo_ids],
            removed_repo_ids=[UUID(repo_id) for repo_id in removed_repo_ids],
        )

    async def _handle_filter_changes(
        self,
        pipeline_id: UUID,
        old_filter_ids: list,
        new_filter_ids: list[str],
    ) -> None:
        """Handle filter changes for enabled pipeline."""
        current_filter_ids = {str(f) for f in old_filter_ids}
        new_filter_ids_set = set(new_filter_ids) if new_filter_ids else set()

        added_filter_ids = new_filter_ids_set - current_filter_ids
        removed_filter_ids = current_filter_ids - new_filter_ids_set

        logger.info(
            "Filter diff calculated",
            extra={
                "pipeline_id": str(pipeline_id),
                "old_filter_ids": [str(f) for f in old_filter_ids],
                "new_filter_ids": list(new_filter_ids_set),
                "added": list(added_filter_ids),
                "removed": list(removed_filter_ids),
            },
        )

        if added_filter_ids or removed_filter_ids:
            try:
                await self._get_filters_orchestrator().handle_pipeline_update(
                    pipeline_id=pipeline_id,
                    added_filter_ids=[UUID(fid) for fid in added_filter_ids],
                    removed_filter_ids=[UUID(fid) for fid in removed_filter_ids],
                )
            except Exception as filter_err:
                logger.warning(
                    "Failed to sync filters on update (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(filter_err)},
                )

    async def _handle_disable_scenario(self, pipeline_id: UUID) -> None:
        """Handle scenario 1: Disabling pipeline (true → false).

        Uses suspend instead of delete to preserve job ID and checkpoint history.
        The Flink Operator will create a savepoint and stop the job gracefully.
        """
        logger.info(
            "Disabling pipeline - suspending FlinkDeployment (preserving state and job ID)",
            extra={"pipeline_id": str(pipeline_id)},
        )

        deployment_name = self.k8s.find_deployment_by_pipeline_id(str(pipeline_id))
        if deployment_name:
            try:
                self.k8s.suspend_deployment(deployment_name)
                logger.info(
                    "Pipeline suspended (state preserved for checkpoint restore)",
                    extra={"pipeline_id": str(pipeline_id), "deployment_name": deployment_name},
                )
            except Exception as suspend_error:
                logger.warning(
                    "Failed to suspend deployment, attempting delete as fallback",
                    extra={"pipeline_id": str(pipeline_id), "error": str(suspend_error)},
                )
                # Fallback to delete if suspend fails
                self.k8s.delete_flink_deployment_by_pipeline_id(str(pipeline_id))
        else:
            logger.info(
                "No deployment found to suspend",
                extra={"pipeline_id": str(pipeline_id)},
            )

    async def _handle_enable_scenario(self, pipeline_id: UUID, updated: Pipeline) -> None:
        """Handle scenario 2: Enabling pipeline (false → true).

        Attempts to resume from suspended state first (preserves checkpoint).
        If no suspended deployment exists, creates a new one.
        """
        # Check if deployment exists (was suspended)
        deployment_name = self.k8s.find_deployment_by_pipeline_id(str(pipeline_id))

        if deployment_name:
            # Deployment exists - resume it (restores from checkpoint)
            logger.info(
                "Enabling pipeline - resuming suspended FlinkDeployment (restoring from checkpoint)",
                extra={"pipeline_id": str(pipeline_id), "deployment_name": deployment_name},
            )

            try:
                self.k8s.resume_deployment(deployment_name)

                # Sync data to Kafka (rules, filters, parser, custom_fields)
                await self._sync_kafka_data_on_enable(pipeline_id, updated)

                logger.info(
                    "Pipeline resumed successfully (checkpoint restored)",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "deployment_name": deployment_name,
                        "note": "Restored from last checkpoint, no data loss",
                    },
                )
                return

            except Exception as resume_error:
                logger.warning(
                    "Failed to resume deployment, will try creating new one",
                    extra={"pipeline_id": str(pipeline_id), "error": str(resume_error)},
                )
                # Fall through to create new deployment

        # No deployment or resume failed - create new one
        logger.info(
            "Enabling pipeline - creating new FlinkDeployment",
            extra={"pipeline_id": str(pipeline_id)},
        )

        flink_created = False
        try:
            result = self.k8s.create_flink_deployment(
                pipeline_id=str(updated.id),
                pipeline_name=updated.name,
                input_topics=updated.source_topics,
                output_topic=updated.destination_topic,
                parallelism=updated.parallelism,
                output_mode="all_events" if updated.save_untagged else "matched_only",
                apply_parser_to_output_events=updated.apply_parser_to_output_events,
                taskmanager_memory_mb=updated.taskmanager_memory_mb,
                taskmanager_cpu=updated.taskmanager_cpu,
                window_size_sec=updated.window_size_sec,
                checkpoint_interval_sec=updated.checkpoint_interval_sec,
                autoscaler_enabled=updated.autoscaler_enabled,
                autoscaler_min_parallelism=updated.autoscaler_min_parallelism,
                autoscaler_max_parallelism=updated.autoscaler_max_parallelism,
                rules_topic=updated.rules_topic,
                metrics_topic=updated.metrics_topic,
            )
            flink_created = True

            new_deployment_name = result.get("metadata", {}).get("name", f"flink-{updated.id}")
            await self.pipeline_repo.update(pipeline_id, deployment_name=new_deployment_name)

            # Sync data to Kafka
            await self._sync_kafka_data_on_enable(pipeline_id, updated)

            logger.info(
                "Pipeline enabled successfully (fresh start)",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "deployment_name": new_deployment_name,
                },
            )

        except Exception as flink_error:
            # Check if this is a Kubernetes ResourceQuota error
            if isinstance(flink_error, ApiException):
                error_body = str(flink_error.body) if flink_error.body else ""

                if flink_error.status == 403 and "exceeded quota" in error_body.lower():
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "Insufficient cluster resources",
                            "reason": "Kubernetes ResourceQuota exceeded",
                            "details": "The namespace has reached its resource limits (CPU/Memory/Pods)",
                            "solution": [
                                "1. Disable another pipeline to free resources",
                                "2. Scale up the cluster (add nodes or increase quotas)",
                                "3. Check quota usage: kubectl describe resourcequota -n security",
                            ],
                            "kubernetes_message": error_body[:500],
                        },
                    ) from flink_error

            # Compensating transaction
            if flink_created:
                try:
                    self.k8s.delete_flink_deployment_by_pipeline_id(str(updated.id))
                except Exception as rb_error:
                    logger.error("Rollback failed", extra={"error": str(rb_error)})

            raise

    async def _sync_kafka_data_on_enable(self, pipeline_id: UUID, updated: Pipeline) -> None:
        """Sync rules, filters, parser, and custom_fields to Kafka on enable/resume."""
        await self._get_rules_orchestrator().handle_pipeline_enable(pipeline_id)

        if updated.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_enable(
                    pipeline_id=pipeline_id,
                    filter_ids=[UUID(f) for f in updated.filters],
                )
            except Exception as filter_err:
                logger.warning(
                    "Failed to send filters on enable (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(filter_err)},
                )

        try:
            await self._get_parsers_orchestrator().handle_pipeline_enable(pipeline_id)
        except Exception as parser_err:
            logger.warning(
                "Failed to send parser to Kafka (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(parser_err)},
            )

        if updated.custom_fields:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_enable(
                    pipeline_id=pipeline_id,
                    custom_fields=updated.custom_fields,
                )
            except Exception as custom_fields_err:
                logger.warning(
                    "Failed to send custom_fields on enable (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(custom_fields_err)},
                )

    async def _handle_stays_disabled_scenario(self, pipeline_id: UUID) -> None:
        """Handle scenario 3: Pipeline stays disabled."""
        logger.info(
            "Pipeline remains disabled, config updated in database only",
            extra={"pipeline_id": str(pipeline_id)},
        )

    async def _handle_restart_scenario(
        self,
        pipeline_id: UUID,
        updated: Pipeline,
        changes: PipelineChanges,
    ) -> None:
        """Handle scenario 4: Pipeline stays enabled but config changed - update deployment.

        Uses update_flink_deployment to patch the spec. The Flink Operator with
        upgradeMode: last-state will automatically suspend, update, and resume
        from the latest checkpoint.
        """
        logger.info(
            "Config changed on enabled pipeline - updating FlinkDeployment spec",
            extra={
                "pipeline_id": str(pipeline_id),
                "changes.cleanup_needed": changes.cleanup_needed,
                "note": "Operator will handle upgrade with checkpoint preservation",
            },
        )

        # Cleanup storage only if source_topics changed (old offsets are invalid)
        if changes.cleanup_needed:
            try:
                self.k8s.cleanup_pipeline_storage(str(updated.id))
                logger.info(
                    "Pipeline storage cleaned up (source_topics changed)",
                    extra={"pipeline_id": str(pipeline_id)},
                )
            except Exception as cleanup_error:
                logger.warning(
                    "Failed to cleanup storage (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(cleanup_error)},
                )

        try:
            # Update deployment spec - Flink Operator handles the upgrade
            result = self.k8s.update_flink_deployment(
                pipeline_id=str(updated.id),
                pipeline_name=updated.name,
                input_topics=updated.source_topics,
                output_topic=updated.destination_topic,
                parallelism=updated.parallelism,
                output_mode="all_events" if updated.save_untagged else "matched_only",
                apply_parser_to_output_events=updated.apply_parser_to_output_events,
                taskmanager_memory_mb=updated.taskmanager_memory_mb,
                taskmanager_cpu=updated.taskmanager_cpu,
                window_size_sec=updated.window_size_sec,
                checkpoint_interval_sec=updated.checkpoint_interval_sec,
                autoscaler_enabled=updated.autoscaler_enabled,
                autoscaler_min_parallelism=updated.autoscaler_min_parallelism,
                autoscaler_max_parallelism=updated.autoscaler_max_parallelism,
                rules_topic=updated.rules_topic,
                metrics_topic=updated.metrics_topic,
            )

            deployment_name = result.get("metadata", {}).get("name", f"flink-{updated.id}")

            logger.info(
                "FlinkDeployment spec updated (operator will perform rolling upgrade)",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "deployment_name": deployment_name,
                },
            )

            # Resync data to Kafka
            await self._sync_kafka_data_on_enable(pipeline_id, updated)

        except Exception as flink_error:
            logger.error(
                "Failed to update FlinkDeployment",
                extra={"pipeline_id": str(pipeline_id), "error": str(flink_error)},
            )

            # Try to disable pipeline in DB as fallback
            try:
                await self.db.rollback()
                await self.pipeline_repo.update(pipeline_id, enabled=False)
                await self.db.commit()
                logger.info(
                    "Rolled back: disabled pipeline after failed update",
                    extra={"pipeline_id": str(pipeline_id)},
                )
            except Exception as rollback_error:
                logger.error(
                    "Rollback failed",
                    extra={"pipeline_id": str(pipeline_id), "error": str(rollback_error)},
                )

            raise

    # _resync_kafka_data is replaced by _sync_kafka_data_on_enable above

    # Legacy method for backward compatibility (remove after verification)
    async def _resync_kafka_data(self, pipeline_id: UUID, updated: Pipeline) -> None:
        """Deprecated: Use _sync_kafka_data_on_enable instead."""
        await self._sync_kafka_data_on_enable(pipeline_id, updated)

    async def _handle_hot_reload_scenario(self, pipeline_id: UUID, changes: PipelineChanges) -> None:
        """Handle scenario 5: Pipeline stays enabled, hot-reload changes only."""
        logger.info(
            "Pipeline remains enabled, syncing hot-reload changes to Kafka",
            extra={
                "pipeline_id": str(pipeline_id),
                "log_source_changed": changes.log_source_changed,
                "filters_changed": changes.filters_changed,
                "custom_fields_changed": changes.custom_fields_changed,
            },
        )

        # Sync parser+mapping if log_source changed
        if changes.log_source_changed:
            try:
                await self._get_parsers_orchestrator().handle_pipeline_update(
                    pipeline_id,
                    changes.old_log_source_id,
                    changes.new_log_source_id,
                )
            except Exception as parser_err:
                logger.warning(
                    "Failed to sync parser to Kafka (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(parser_err)},
                )

        # Sync custom_fields if changed
        if changes.custom_fields_changed:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_update(
                    pipeline_id,
                    changes.old_custom_fields,
                    changes.new_custom_fields,
                )
            except Exception as custom_fields_err:
                logger.warning(
                    "Failed to sync custom_fields to Kafka (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(custom_fields_err)},
                )

    async def _log_update_activity(
        self,
        user: User,
        updated: Pipeline,
        old_enabled: bool,
        new_enabled: bool,
    ) -> None:
        """Log activity for pipeline update."""
        if old_enabled and not new_enabled:
            action = "toggle"
            details = f"Disabled pipeline {updated.name}"
        elif not old_enabled and new_enabled:
            action = "toggle"
            details = f"Enabled pipeline {updated.name}"
        else:
            action = "update"
            details = f"Updated pipeline {updated.name}"

        await activity_producer.log_action(
            action=action,
            entity_type="pipeline",
            entity_id=str(updated.id),
            entity_name=updated.name,
            user=user,
            details=details,
        )

    async def delete_pipeline(self, pipeline_id: UUID, user: User) -> PipelineResponse:
        """Permanently delete a pipeline.

        Deletes Flink deployment, cleans storage, removes from Kafka and database.

        Args:
            pipeline_id: UUID of pipeline to delete
            user: Current user for activity logging

        Returns:
            PipelineResponse confirming deletion

        Raises:
            PipelineNotFoundError: If pipeline not found
        """
        logger.info("Deleting pipeline permanently", extra={"pipeline_id": str(pipeline_id)})

        existing = await self.pipeline_repo.get_by_id(pipeline_id)
        if not existing:
            logger.warning("Pipeline not found for deletion", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        pipeline_name = existing.name

        # Delete FlinkDeployment from Kubernetes
        try:
            self.k8s.delete_flink_deployment_by_pipeline_id(str(pipeline_id))
            logger.info("FlinkDeployment deleted successfully", extra={"pipeline_id": str(pipeline_id)})
        except Exception as e:
            logger.warning(
                "Failed to delete FlinkDeployment (may not exist)",
                extra={"pipeline_id": str(pipeline_id), "error": str(e)},
            )

        # Cleanup storage
        try:
            self.k8s.cleanup_pipeline_storage(str(pipeline_id))
            logger.info("Storage cleaned up successfully", extra={"pipeline_id": str(pipeline_id)})
        except Exception as cleanup_error:
            logger.warning(
                "Failed to cleanup storage (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(cleanup_error)},
            )

        # Delete from Kafka
        await self._delete_from_kafka(pipeline_id, existing)

        # Delete pipeline from database
        deleted = await self.pipeline_repo.delete(pipeline_id)
        if not deleted:
            logger.error("Failed to delete pipeline from database", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        await self.db.commit()

        # Invalidate caches
        dashboard_service.invalidate_pipeline_cache(pipeline_id)

        logger.info("Pipeline deleted successfully", extra={"pipeline_id": str(pipeline_id)})

        # Log activity
        await activity_producer.log_action(
            action="delete",
            entity_type="pipeline",
            entity_id=str(pipeline_id),
            entity_name=pipeline_name,
            user=user,
            details=f"Deleted pipeline {pipeline_name}",
        )

        return PipelineResponse(id=str(pipeline_id), status=True, message="Pipeline deleted successfully")

    async def _delete_from_kafka(self, pipeline_id: UUID, existing: Pipeline) -> None:
        """Delete pipeline data from Kafka topics."""
        # Delete parser
        try:
            await self._get_parsers_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as parser_err:
            logger.warning(
                "Failed to delete parser from Kafka (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(parser_err)},
            )

        # Delete rules
        try:
            await self._get_rules_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as rules_err:
            logger.warning(
                "Failed to delete rules from Kafka (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(rules_err)},
            )

        # Delete filters
        if existing.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_deletion(
                    pipeline_id=pipeline_id,
                    filter_ids=[UUID(f) for f in existing.filters],
                )
            except Exception as filter_err:
                logger.warning(
                    "Failed to delete filters from Kafka (non-critical)",
                    extra={"pipeline_id": str(pipeline_id), "error": str(filter_err)},
                )

        # Delete custom_fields
        try:
            await self._get_custom_fields_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as custom_fields_err:
            logger.warning(
                "Failed to delete custom_fields from Kafka (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(custom_fields_err)},
            )

    async def get_pipeline_details(self, pipeline_id: UUID) -> PipelineDetailResponse:
        """Get detailed information about a pipeline.

        Args:
            pipeline_id: UUID of pipeline

        Returns:
            PipelineDetailResponse with full configuration and status

        Raises:
            PipelineNotFoundError: If pipeline not found
        """
        logger.info("Fetching pipeline details", extra={"pipeline_id": str(pipeline_id)})

        pipeline = await self.pipeline_repo.get_with_relations(pipeline_id)
        if not pipeline:
            logger.warning("Pipeline not found", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        # Get repository IDs
        repository_ids = [str(repo.id) for repo in pipeline.repositories] if pipeline.repositories else []

        # Get rules statistics
        enabled_rule_ids = await self.pipeline_rules_repo.get_enabled_rules_ids(pipeline_id)
        active_rules = len(enabled_rule_ids)

        # Get matched_rules count from pipeline_rule_metrics table
        # (rules that have at least one match)
        metrics_dao = MetricsDAO(self.db)
        rule_metrics_map = await metrics_dao.get_rule_metrics_map(str(pipeline_id))
        matched_rules = sum(1 for count in rule_metrics_map.values() if count > 0)

        # Get Flink status
        flink_status, status_details = await pipeline_status_manager.get_status_details(
            pipeline_id=str(pipeline.id),
            deployment_name=pipeline.deployment_name,
            enabled=pipeline.enabled,
        )

        return PipelineDetailResponse(
            id=str(pipeline.id),
            name=pipeline.name,
            source_topics=pipeline.source_topics or [],
            destination_topic=pipeline.destination_topic,
            save_untagged=pipeline.save_untagged,
            apply_parser_to_output_events=pipeline.apply_parser_to_output_events or False,
            filters=[str(f) for f in pipeline.filters] if pipeline.filters else [],
            log_source_id=str(pipeline.log_source_id),
            log_source_name=pipeline.log_source.name if pipeline.log_source else None,
            enabled=pipeline.enabled,
            repository_ids=repository_ids,
            custom_fields=pipeline.custom_fields,
            events_tagged=pipeline.events_tagged or 0,
            events_untagged=pipeline.events_untagged or 0,
            active_rules=active_rules,
            matched_rules=matched_rules,
            rules_topic=pipeline.rules_topic,
            metrics_topic=pipeline.metrics_topic,
            parallelism=pipeline.parallelism,
            window_size_sec=pipeline.window_size_sec,
            status=flink_status,
            status_details=status_details,
            deployment_name=pipeline.deployment_name,
            namespace=pipeline.namespace,
            last_sync_at=pipeline.last_sync_at.isoformat() if pipeline.last_sync_at else None,
            # Flink resource configuration
            taskmanager_memory_mb=pipeline.taskmanager_memory_mb,
            taskmanager_cpu=pipeline.taskmanager_cpu,
            checkpoint_interval_sec=pipeline.checkpoint_interval_sec,
            autoscaler_enabled=pipeline.autoscaler_enabled,
            autoscaler_min_parallelism=pipeline.autoscaler_min_parallelism,
            autoscaler_max_parallelism=pipeline.autoscaler_max_parallelism,
        )

    async def get_pipelines_list(
        self,
        pagination: dict,
    ) -> PipelineListResponse:
        """Get paginated list of pipelines.

        Args:
            pagination: Pagination parameters (page, limit, skip, search, sort, order)

        Returns:
            PipelineListResponse with list of pipelines and total count
        """
        logger.info(
            "Fetching pipeline list",
            extra={
                "page": pagination["page"],
                "limit": pagination["limit"],
                "search": pagination["search"],
            },
        )

        pipelines, total = await self.pipeline_repo.get_list_with_relations(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            sort=pagination["sort"],
            order=pagination["order"],
        )

        data = []
        for pipeline in pipelines:
            filters_count = len(pipeline.filters) if pipeline.filters else 0
            rules_count = len(pipeline.pipeline_rules) if hasattr(pipeline, "pipeline_rules") else 0

            repositories = (
                [{"name": repo.name, "type": repo.type} for repo in pipeline.repositories]
                if pipeline.repositories
                else []
            )

            logsources = (
                [
                    {
                        "name": pipeline.log_source.name,
                        "parser": pipeline.log_source.name if pipeline.log_source.parsing_config else "",
                    }
                ]
                if pipeline.log_source
                else []
            )

            flink_status, status_details = await pipeline_status_manager.get_status_details(
                pipeline_id=str(pipeline.id),
                deployment_name=pipeline.deployment_name,
                enabled=pipeline.enabled,
            )

            data.append(
                PipelineListItem(
                    id=str(pipeline.id),
                    enabled=pipeline.enabled,
                    name=pipeline.name,
                    source_topics=pipeline.source_topics or [],
                    status=flink_status,
                    status_details=status_details,
                    destination_topic=pipeline.destination_topic,
                    repositories=repositories,
                    log_source=logsources,
                    filters=filters_count,
                    rules=rules_count,
                    events_tagged=pipeline.events_tagged or 0,
                    events_untagged=pipeline.events_untagged or 0,
                    created=pipeline.created.isoformat(),
                    updated=pipeline.updated.isoformat(),
                )
            )

        logger.info(f"Retrieved {len(data)} pipelines out of {total} total")

        return PipelineListResponse(
            total=total,
            page=pagination["page"],
            limit=pagination["limit"],
            sort=pagination["sort"] or "",
            offset=pagination["offset"],
            order=pagination["order"],
            data=data,
        )
