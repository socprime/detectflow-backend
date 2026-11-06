"""Pipeline orchestrator for managing ETL pipeline lifecycle.

This module provides the PipelineOrchestrator class that handles all business logic
for pipeline operations including CRUD, Flink deployment management, and Kafka sync.
"""

import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import BadRequestError, ServiceUnavailableError
from apps.core.logger import get_logger
from apps.core.models import Pipeline, User
from apps.core.schemas import (
    CorrelationPipelineRef,
    PipelineCreateRequest,
    PipelineDetailResponse,
    PipelineListItem,
    PipelineListResponse,
    PipelineOutputDestination,
    PipelineRepositorySelection,
    PipelineResponse,
    PipelineUpdateRequest,
    TaggedEventsTopicAssignRequest,
    destinations_from_legacy,
    legacy_fields_from_destinations,
    normalize_destinations,
    pipeline_destinations,
)
from apps.core.settings import settings
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.managers.custom_fields import CustomFieldsOrchestrator
from apps.managers.dashboard import dashboard_service
from apps.managers.filter import FiltersOrchestrator
from apps.managers.flink_config import FlinkConfigManager
from apps.managers.parser_sync import ParsersOrchestrator
from apps.managers.pipeline_status import pipeline_status_manager
from apps.managers.rule import RulesOrchestrator
from apps.managers.sigma_validation_service import SigmaValidationService
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.topic_lifecycle import TopicProvisioningError, topic_lifecycle_service
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.filter import FilterDAO
from apps.modules.postgre.log_source import LogSourceDAO
from apps.modules.postgre.metrics import MetricsDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO
from apps.modules.postgre.repository import RepositoryDAO
from apps.providers import FlinkDeploymentConfig, FlinkQuotaExceededError, get_flink_provider

logger = get_logger(__name__)


# Note: detectflow_matchnode_version is now set via Kafka metrics from Flink,
# not extracted from Docker image tag. See apps/modules/kafka/metrics.py


# Resource / processing / autoscaler columns on Pipeline (nested under request.resources)
FLINK_RESOURCE_FIELDS = (
    "parallelism",
    "taskmanager_memory_mb",
    "taskmanager_cpu",
    "window_size_sec",
    "checkpoint_interval_sec",
    "autoscaler_enabled",
    "autoscaler_min_parallelism",
    "autoscaler_max_parallelism",
)

# Fields that require Flink restart when changed on enabled pipeline
# Other fields (name, filters, log_source_id, repository_ids) are hot-reloadable via Kafka
FLINK_RESTART_FIELDS = {
    "source_topics",  # Different Kafka source topics
    "destinations",  # Output topics / modes / parser flags (legacy singular fields fold into this)
    "tagged_events_topic",  # rendered as KAFKA_TAGGED_EVENTS_TOPIC env on the matcher deployment
    *FLINK_RESOURCE_FIELDS,
}

# Fields that trigger storage cleanup when changed (subset of FLINK_RESTART_FIELDS)
# (cleanup = delete checkpoints/savepoints, effectively reset consumer offsets)
# ``tagged_events_topic`` is deliberately absent: only the matcher's side-output SINK moves, its
# input offsets stay valid.
CLEANUP_TRIGGER_FIELDS = {
    "source_topics",  # New sources = need fresh start, old offsets are invalid
}

# Suffix appended to a duplicated pipeline's name. pipelines.name is not unique, so copies of
# copies are allowed; the base name is truncated to keep the result within String(255).
DUPLICATE_NAME_SUFFIX = " (Copy)"
PIPELINE_NAME_MAX_LENGTH = 255


@dataclass
class PipelineChanges:
    """Result of comparing pipeline update request with existing pipeline."""

    update_data: dict  # Fields to update in DB
    flink_restart_needed: bool  # Need to patch FlinkDeployment spec (last-state upgrade)
    cleanup_needed: bool  # Need to reset offsets (delete checkpoints)
    filters_changed: bool  # Filters array changed (hot-reloadable)
    log_source_changed: bool  # LogSource changed (hot-reloadable)
    old_log_source_id: UUID | None  # Previous log_source_id
    new_log_source_id: UUID | None  # New log_source_id
    custom_fields_changed: bool  # Custom fields changed (hot-reloadable)
    old_custom_fields: str | None  # Previous custom_fields
    new_custom_fields: str | None  # New custom_fields


def _resolve_updated_destinations(pipeline: PipelineUpdateRequest, existing: Pipeline) -> list[dict[str, Any]] | None:
    """Merge a pipeline update's destination fields with the stored destinations.

    ``destinations`` replaces the whole list. The legacy singular fields are a partial
    update of the FIRST destination only, so a legacy client changing ``save_untagged`` or
    the primary topic never drops the extra destinations it doesn't know about.

    Returns:
        The new destinations list, or None when this update leaves them unchanged.

    Raises:
        ValueError / PydanticCustomError: If the result is invalid — including a source
            topic change that collides with an untouched destination.
    """
    stored = pipeline_destinations(existing)
    legacy_update = (
        pipeline.destination_topic is not None
        or pipeline.save_untagged is not None
        or pipeline.apply_parser_to_output_events is not None
    )

    if pipeline.destinations is not None:
        merged: list[dict[str, Any]] = list(pipeline.destinations)
    elif legacy_update and stored:
        first = dict(stored[0])
        if pipeline.destination_topic is not None:
            first["topic"] = pipeline.destination_topic
        if pipeline.save_untagged is not None:
            # A legacy client can only express the two-mode world; sending this field on an
            # `unmatched_only` destination deliberately collapses it to all_events/matched_only.
            first["output_mode"] = "all_events" if pipeline.save_untagged else "matched_only"
        if pipeline.apply_parser_to_output_events is not None:
            first["apply_parser_to_output_events"] = pipeline.apply_parser_to_output_events
        merged = [first, *stored[1:]]
    elif legacy_update:
        merged = destinations_from_legacy(
            pipeline.destination_topic or existing.destination_topic,
            bool(pipeline.save_untagged if pipeline.save_untagged is not None else existing.save_untagged),
            bool(
                pipeline.apply_parser_to_output_events
                if pipeline.apply_parser_to_output_events is not None
                else existing.apply_parser_to_output_events
            ),
        )
    elif pipeline.source_topics is not None and stored:
        merged = stored  # untouched, but must not collide with the new source topics
    else:
        return None

    sources = pipeline.source_topics if pipeline.source_topics is not None else (existing.source_topics or [])
    normalized = normalize_destinations(merged, sources)
    return normalized if normalized != stored else None


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

    new_destinations = _resolve_updated_destinations(pipeline, existing)
    if new_destinations is not None:
        update_data["destinations"] = new_destinations
        # Keep the singular columns in sync — old clients and destination_topic sorting read them
        (
            update_data["destination_topic"],
            update_data["save_untagged"],
            update_data["apply_parser_to_output_events"],
        ) = legacy_fields_from_destinations(new_destinations)
        changed_fields.add("destinations")

    # Resource / processing / autoscaler (last-state Flink upgrade; omitted/null = unchanged)
    if pipeline.resources is not None:
        for field_name in FLINK_RESOURCE_FIELDS:
            new_value = getattr(pipeline.resources, field_name)
            if new_value is None:
                continue
            if new_value != getattr(existing, field_name):
                update_data[field_name] = new_value
                changed_fields.add(field_name)

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


def correlation_pipeline_refs(pipeline: Any) -> list[CorrelationPipelineRef]:
    """Membership chips for a detection pipeline, sorted by name then id.

    Expects ``pipeline.correlation_pipelines`` already loaded (``selectinload``).
    The relationship is ``noload``, so an unloaded collection is empty, not lazy.
    """
    return [
        CorrelationPipelineRef(id=row.id, name=row.name)
        for row in sorted(pipeline.correlation_pipelines or [], key=lambda row: (row.name, str(row.id)))
    ]


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
        self._flink = get_flink_provider()

    async def _log_kafka_sync_warning(
        self,
        pipeline_id: UUID,
        operation: str,
        component: str,
        error: Exception,
    ) -> None:
        """Log a non-critical Kafka sync warning with anti-flood protection.

        Args:
            pipeline_id: Pipeline UUID
            operation: Operation that failed (e.g., "create", "enable", "update")
            component: Component that failed (e.g., "filters", "parser", "custom_fields")
            error: The exception that occurred
        """
        error_key = f"kafka_sync_{component}_{pipeline_id}"

        logger.warning(
            f"Failed to send {component} to Kafka (non-critical)",
            extra={"pipeline_id": str(pipeline_id), "error": str(error)},
        )

        if ErrorTracker.should_log(error_key):
            await activity_producer.log_action(
                action="warning",
                entity_type="kafka_sync",
                entity_id=str(pipeline_id),
                details=f"Failed to send {component} to Kafka on {operation} (non-critical): {str(error)}",
                source="system",
                severity="warning",
            )

    async def _log_k8s_warning(
        self,
        pipeline_id: UUID,
        operation: str,
        component: str,
        error: Exception,
    ) -> None:
        """Log a non-critical Kubernetes warning with anti-flood protection.

        Args:
            pipeline_id: Pipeline UUID
            operation: Operation that failed (e.g., "restart", "delete")
            component: Component that failed (e.g., "storage_cleanup", "deployment")
            error: The exception that occurred
        """
        error_key = f"k8s_{component}_{pipeline_id}"

        logger.warning(
            f"Failed to {component.replace('_', ' ')} (non-critical)",
            extra={"pipeline_id": str(pipeline_id), "error": str(error)},
        )

        if ErrorTracker.should_log(error_key):
            await activity_producer.log_action(
                action="warning",
                entity_type="kubernetes",
                entity_id=str(pipeline_id),
                details=f"Failed to {component.replace('_', ' ')} on {operation} (non-critical): {str(error)}",
                source="system",
                severity="warning",
            )

    async def _log_flink_warning(
        self,
        pipeline_id: UUID,
        operation: str,
        error: Exception,
    ) -> None:
        """Log a non-critical Flink warning with anti-flood protection.

        Args:
            pipeline_id: Pipeline UUID
            operation: Operation that failed (e.g., "delete", "rollback")
            error: The exception that occurred
        """
        error_key = f"flink_{operation}_{pipeline_id}"

        logger.warning(
            f"Failed to {operation} FlinkDeployment (non-critical)",
            extra={"pipeline_id": str(pipeline_id), "error": str(error)},
        )

        if ErrorTracker.should_log(error_key):
            await activity_producer.log_action(
                action="warning",
                entity_type="flink",
                entity_id=str(pipeline_id),
                details=f"Failed to {operation} FlinkDeployment (non-critical): {str(error)}",
                source="system",
                severity="warning",
            )

    async def _sync_correlation_pipelines_non_critical(
        self,
        user: User,
        pipeline_id: UUID,
        reason: str,
        *,
        cold_start: bool = False,
    ) -> None:
        """Restart every correlation pipeline that contains ``pipeline_id``, if it is running.

        Destination-topic edits are SIEM output and do not republish or restart. Callers that only
        need a chains-config refresh (threshold / window / AT deny-list) go through
        ``CorrelationPipelinesManager.republish_chains_config`` directly.

        Every owning correlation job is re-rendered with its current member topic list; the
        detection pipeline itself is never restarted (hard constraint 4). ``cold_start=True`` turns
        that re-render into a stop→start with the state wiped, which is mandatory whenever the edit
        takes a topic OUT of ``KAFKA_INPUT_TOPICS`` — a re-rendered job keeps consuming it from its
        restored splits.

        Non-critical by contract (hard constraint 3): a correlation-side failure must never fail the
        detection-pipeline operation that triggered it, and one correlation pipeline failing must not
        stop the others from being refreshed.
        """
        context = {"pipeline_id": str(pipeline_id), "reason": reason}
        try:
            manager = CorrelationPipelinesManager(self.db)
            owners = await manager.get_ids_by_member(pipeline_id)
            for correlation_pipeline_id in owners:
                try:
                    await manager.restart_if_running(correlation_pipeline_id, user, cold_start=cold_start)
                except Exception as error:
                    logger.warning(
                        "Failed to refresh correlation pipeline (non-critical)",
                        extra={
                            **context,
                            "correlation_pipeline_id": str(correlation_pipeline_id),
                            "error": str(error),
                        },
                    )
        except Exception as error:
            logger.warning(
                "Failed to resolve correlation-pipeline membership (non-critical)",
                extra={**context, "error": str(error)},
            )

    async def _auto_add_to_correlation_pipelines_non_critical(self, user: User, pipeline_id: UUID) -> None:
        """Attach a brand-new detection pipeline to every correlation pipeline that opted in.

        Selection, pattern matching and the restart-if-running are the manager's
        (``auto_add_member``). Auto-add matches the detection pipeline's name and is about
        membership, not about the matcher running, so this is NOT gated on ``enabled`` — and it
        must never raise into a pipeline create or duplicate request.
        """
        try:
            await CorrelationPipelinesManager(self.db).auto_add_member(pipeline_id, user)
        except Exception as error:
            logger.warning(
                "Failed to auto-add the new pipeline to correlation pipelines (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(error)},
            )

    async def _provision_tagged_events_topic(self, pipeline: Pipeline) -> None:
        """Create this pipeline's dedicated tagged-events topic and pin it on the row.

        The name is fixed for the pipeline's lifetime (epic Decision 1): it is rendered into
        KAFKA_TAGGED_EVENTS_TOPIC on the matcher deployment and into every owning correlation
        pipeline's KAFKA_INPUT_TOPICS, and the correlation job's single main ``KafkaSource``
        restores its per-topic split offsets by topic name — renaming restores nothing.

        Fail-fast: the topic must exist before the matcher starts producing, otherwise the run
        depends on broker auto-create (wrong partitions/retention) or drops the side output.
        Raising is the rollback: ``create_pipeline`` / ``duplicate_pipeline`` commit exactly once,
        at the end, and ``get_db`` rolls the session back on any exception, so the un-committed
        INSERT disappears with the failed request. A topic created just before an unrelated later
        failure is harmless: it is empty, retention-bound, and ``ensure_pipeline_topics`` is
        idempotent, so a retry re-adopts it.

        Raises:
            ServiceUnavailableError: 503 — the broker could not provision the topic.
        """
        try:
            topic = await topic_lifecycle_service.ensure_pipeline_topics(pipeline.id)
        except TopicProvisioningError as e:
            logger.error(
                "Rolling back pipeline creation: tagged-events topic provisioning failed",
                extra={"pipeline_id": str(pipeline.id), "error": str(e)},
            )
            raise ServiceUnavailableError(
                f"Could not provision tagged-events topic for pipeline '{pipeline.name}'"
            ) from e
        await self.pipeline_repo.update(pipeline.id, tagged_events_topic=topic)
        pipeline.tagged_events_topic = topic  # the caller holds the pre-update instance

    def _build_deployment_config(self, pipeline: Pipeline) -> FlinkDeploymentConfig:
        """Build FlinkDeploymentConfig from Pipeline model.

        ``output_destinations`` carries the full fan-out; ``output_topic`` / ``output_mode`` /
        ``apply_parser_to_output_events`` mirror the first destination for match-node images
        that predate ``KAFKA_OUTPUT_DESTINATIONS``.
        """
        destinations = pipeline_destinations(pipeline)
        first = destinations[0] if destinations else {}
        return FlinkDeploymentConfig(
            pipeline_id=str(pipeline.id),
            pipeline_name=pipeline.name,
            input_topics=pipeline.source_topics,
            output_topic=first.get("topic", ""),
            output_destinations=destinations,
            parallelism=pipeline.parallelism,
            output_mode=first.get("output_mode", "matched_only"),
            apply_parser_to_output_events=bool(first.get("apply_parser_to_output_events", False)),
            taskmanager_memory_mb=pipeline.taskmanager_memory_mb,
            taskmanager_cpu=pipeline.taskmanager_cpu,
            window_size_sec=pipeline.window_size_sec,
            checkpoint_interval_sec=pipeline.checkpoint_interval_sec,
            autoscaler_enabled=pipeline.autoscaler_enabled,
            autoscaler_min_parallelism=pipeline.autoscaler_min_parallelism,
            autoscaler_max_parallelism=pipeline.autoscaler_max_parallelism,
            rules_topic=pipeline.rules_topic,
            metrics_topic=pipeline.metrics_topic,
            tagged_events_topic=pipeline.tagged_events_topic,
        )

    @staticmethod
    def _quota_exceeded_http_exception(quota_error: FlinkQuotaExceededError) -> HTTPException:
        """Map a provider quota failure to HTTP 409 so the pipeline is not disabled."""
        return HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "Insufficient cluster resources",
                "reason": quota_error.message,
                "details": "The namespace has reached its resource limits (CPU/Memory/Pods)",
                "solution": [
                    "1. Disable another pipeline to free resources",
                    "2. Scale up the cluster (add nodes or increase quotas)",
                    "3. Check quota usage: kubectl describe resourcequota -n security",
                ],
                "provider_message": quota_error.details or "",
            },
        )

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

    @staticmethod
    def _repository_selections_to_dict(
        repository_ids: list[PipelineRepositorySelection] | None,
    ) -> dict[UUID, list[str]]:
        if not repository_ids:
            return {}
        return {UUID(selection.id): selection.folders for selection in repository_ids}

    async def _validate_resources(
        self,
        log_source_id: str,
        filter_ids: list[str] | None = None,
        repository_ids: list[PipelineRepositorySelection] | None = None,
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
            for selection in repository_ids:
                repo_uuid = UUID(selection.id)
                repository = await self.repository_repo.get_by_id(repo_uuid)
                if not repository:
                    raise ResourceNotFoundError("Repository", selection.id)

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

        # Prepare create data. PipelineCreateRequest already resolved destinations from the
        # legacy singular fields; the fallback only covers non-HTTP callers.
        destinations = normalize_destinations(
            request.destinations
            or destinations_from_legacy(
                request.destination_topic,
                bool(request.save_untagged),
                bool(request.apply_parser_to_output_events),
            )
        )
        dest_topic, save_untagged, apply_parser = legacy_fields_from_destinations(destinations)
        create_data = {
            "name": request.name,
            "source_topics": request.source_topics,
            "destinations": destinations,
            "destination_topic": dest_topic,
            "log_source_id": UUID(request.log_source_id),
            "enabled": request.enabled,
            "save_untagged": save_untagged,
            "apply_parser_to_output_events": apply_parser,
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

        # Provision the dedicated tagged-events topic before any other side effect, so a broker
        # failure rolls back with the fewest of them emitted. Raising here is the rollback.
        await self._provision_tagged_events_topic(new_pipeline)

        # Add repositories and rules
        await self._get_rules_orchestrator().handle_pipeline_creation(
            pipeline_id=new_pipeline.id,
            repository_selections=self._repository_selections_to_dict(request.repository_ids),
        )

        # Send filters to Kafka (if pipeline is enabled and has filters)
        if request.enabled and request.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_creation(
                    pipeline_id=new_pipeline.id,
                    filter_ids=[UUID(f) for f in request.filters],
                )
            except Exception as e:
                await self._log_kafka_sync_warning(new_pipeline.id, "create", "filters", e)

        # Send custom_fields to Kafka (if pipeline is enabled and has custom_fields)
        if request.enabled and request.custom_fields:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_creation(
                    pipeline_id=new_pipeline.id,
                    custom_fields=request.custom_fields,
                )
            except Exception as e:
                await self._log_kafka_sync_warning(new_pipeline.id, "create", "custom_fields", e)

        # Create FlinkDeployment if pipeline is enabled
        if request.enabled:
            await self._create_flink_deployment(
                new_pipeline,
                kafka_starting_offset_earliest=request.kafka_starting_offset_earliest,
            )

        # Auto-add matches the detection pipeline's name, not whether the matcher is running —
        # hence outside the ``enabled`` branch above.
        await self._auto_add_to_correlation_pipelines_non_critical(user, new_pipeline.id)

        # Refresh to ensure response has latest state
        await self.db.refresh(new_pipeline)

        # Commit BEFORE invalidating the dashboard topology cache (same pattern as delete):
        # if the SSE refresher reloaded between an early invalidation and the request-end
        # commit, it would cache pre-CRUD state for a full TTL.
        await self.db.commit()
        dashboard_service.invalidate_pipeline_cache(new_pipeline.id)

        # Log activity
        source_topics_str = ", ".join(new_pipeline.source_topics) if new_pipeline.source_topics else ""
        dest_topics_str = ", ".join(d["topic"] for d in destinations)
        await activity_producer.log_action(
            action="create",
            entity_type="pipeline",
            entity_id=str(new_pipeline.id),
            entity_name=new_pipeline.name,
            user=user,
            details=f"Created pipeline from [{source_topics_str}] to [{dest_topics_str}]",
        )

        return PipelineResponse(
            id=str(new_pipeline.id),
            status=True,
            message="Pipeline created successfully",
        )

    async def _create_flink_deployment(
        self,
        pipeline: Pipeline,
        kafka_starting_offset_earliest: bool = False,
    ) -> None:
        """Create Flink deployment for enabled pipeline.

        Args:
            pipeline: Pipeline model instance
            kafka_starting_offset_earliest: If True, consumer starts from earliest offset; not stored

        Raises:
            HTTPException: If Flink deployment fails
        """
        flink_created = False

        try:
            config = self._build_deployment_config(pipeline)
            config.kafka_starting_offset = "earliest" if kafka_starting_offset_earliest else "latest"
            result = await self._flink.create_deployment(config)

            if not result.success:
                raise Exception(result.error or "Failed to create deployment")

            flink_created = True

            # Update deployment_name in database
            await self.pipeline_repo.update(pipeline.id, deployment_name=result.deployment_name)

            logger.info(
                "FlinkDeployment created successfully",
                extra={
                    "pipeline_id": str(pipeline.id),
                    "deployment_name": result.deployment_name,
                },
            )

            # Send parser to Kafka
            try:
                await self._get_parsers_orchestrator().handle_pipeline_creation(pipeline.id)
            except Exception as parser_err:
                await self._log_kafka_sync_warning(pipeline.id, "create", "parser", parser_err)

        except Exception as e:
            logger.error(
                "Failed to create FlinkDeployment or update DB",
                extra={"pipeline_id": str(pipeline.id), "error": str(e)},
            )
            await activity_producer.log_action(
                action="error",
                entity_type="flink",
                entity_id=str(pipeline.id),
                details=f"Failed to create FlinkDeployment or update DB: {str(e)}",
                source="system",
                severity="error",
            )

            # Compensating transaction: cleanup created resources
            if flink_created:
                try:
                    logger.info(
                        "Rolling back: Deleting FlinkDeployment",
                        extra={"pipeline_id": str(pipeline.id)},
                    )
                    await self._flink.delete_deployment(str(pipeline.id))
                except Exception as rollback_error:
                    logger.error(
                        "Failed to delete FlinkDeployment during rollback",
                        extra={
                            "pipeline_id": str(pipeline.id),
                            "rollback_error": str(rollback_error),
                        },
                    )
                    await activity_producer.log_action(
                        action="error",
                        entity_type="flink",
                        entity_id=str(pipeline.id),
                        details=f"Failed to delete FlinkDeployment during rollback: {str(rollback_error)}",
                        source="system",
                        severity="error",
                    )

            if isinstance(e, HTTPException):
                raise
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create FlinkDeployment: {str(e)}",
            ) from e

    @staticmethod
    def _duplicate_name(source_name: str) -> str:
        """Build a copy's name, truncating the base so the result still fits ``pipelines.name``."""
        base = source_name[: PIPELINE_NAME_MAX_LENGTH - len(DUPLICATE_NAME_SUFFIX)]
        return f"{base}{DUPLICATE_NAME_SUFFIX}"

    async def duplicate_pipeline(self, pipeline_id: UUID, user: User) -> PipelineResponse:
        """Create a disabled copy of an existing pipeline.

        The copy keeps the source configuration (source topics, every output destination, log
        source, filters, custom fields, Flink resources), its repository selections with folder
        paths, and its per-rule
        ``pipeline_rules.enabled`` flags. Infrastructure-owned fields (rules/metrics topic,
        namespace) come from settings, not from the source row. It is always created with
        ``enabled=False``: no FlinkDeployment is created and no filters/custom fields/parser are
        published to Kafka. Same as ``create_pipeline``, the copy gets its own tagged-events topic
        (fail-fast: a broker failure rolls the request back) and is auto-added to every matching
        correlation pipeline — auto-add matches the copy's name, not whether the matcher is running.
        Its rules are published under the new pipeline id — see
        ``RulesOrchestrator.handle_pipeline_duplication``.

        Args:
            pipeline_id: UUID of the pipeline to copy
            user: Current user for activity logging

        Returns:
            PipelineResponse with the created copy's ID

        Raises:
            PipelineNotFoundError: If the source pipeline is not found
            ServiceUnavailableError: 503 — the broker could not provision the tagged-events topic
        """
        logger.info("Duplicating pipeline", extra={"pipeline_id": str(pipeline_id)})

        source = await self.pipeline_repo.get_by_id(pipeline_id)
        if not source:
            logger.warning("Pipeline not found for duplication", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        source_name = source.name
        destinations = pipeline_destinations(source)
        dest_topic, save_untagged, apply_parser = legacy_fields_from_destinations(destinations)

        # Runtime state (status, deployment_name, metrics, last_sync_at, matchnode version) is
        # deliberately not copied — the copy starts as a fresh, never-deployed pipeline.
        new_pipeline = await self.pipeline_repo.create(
            name=self._duplicate_name(source_name),
            enabled=False,  # a copy never runs on creation
            source_topics=list(source.source_topics or []),
            destinations=destinations,
            destination_topic=dest_topic,
            log_source_id=source.log_source_id,
            save_untagged=save_untagged,
            apply_parser_to_output_events=apply_parser,
            filters=list(source.filters or []),
            custom_fields=source.custom_fields,
            # Infrastructure-owned, not user config: resolved from settings exactly like
            # create_pipeline, so a copy is never wired to a topic the backend no longer publishes to.
            rules_topic=settings.kafka_sigma_rules_topic,
            metrics_topic=settings.kafka_metrics_topic,
            namespace=settings.kubernetes_namespace,
            parallelism=source.parallelism,
            taskmanager_memory_mb=source.taskmanager_memory_mb,
            taskmanager_cpu=source.taskmanager_cpu,
            window_size_sec=source.window_size_sec,
            checkpoint_interval_sec=source.checkpoint_interval_sec,
            autoscaler_enabled=source.autoscaler_enabled,
            autoscaler_min_parallelism=source.autoscaler_min_parallelism,
            autoscaler_max_parallelism=source.autoscaler_max_parallelism,
        )

        # Same order as create_pipeline: the topic must exist before any other side effect, and
        # auto-add matches the copy's name — not gated on enabled, so a disabled copy still
        # joins matching correlation pipelines.
        await self._provision_tagged_events_topic(new_pipeline)

        # Repositories + rules (with their enabled flags) and the Kafka rule sync
        await self._get_rules_orchestrator().handle_pipeline_duplication(pipeline_id, new_pipeline.id)

        await self._auto_add_to_correlation_pipelines_non_critical(user, new_pipeline.id)

        await self.db.refresh(new_pipeline)

        # Commit BEFORE invalidating the dashboard topology cache (same pattern as create/delete)
        await self.db.commit()
        dashboard_service.invalidate_pipeline_cache(new_pipeline.id)

        await activity_producer.log_action(
            action="create",
            entity_type="pipeline",
            entity_id=str(new_pipeline.id),
            entity_name=new_pipeline.name,
            user=user,
            details=f"Duplicated pipeline '{source_name}' as '{new_pipeline.name}' (created disabled)",
        )

        return PipelineResponse(
            id=str(new_pipeline.id),
            status=True,
            message="Pipeline duplicated successfully",
        )

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
        old_repository_selections = await self.pipeline_repo.get_repository_selections(pipeline_id)

        # Detect all changes
        changes = detect_pipeline_changes(request, existing)

        # Update pipeline in database
        updated = await self.pipeline_repo.update(pipeline_id, **changes.update_data)

        # Handle repository_ids changes
        if request.repository_ids is not None:
            await self._handle_repository_changes(
                pipeline_id=pipeline_id,
                old_repository_selections=old_repository_selections,
                new_repository_selections=self._repository_selections_to_dict(request.repository_ids),
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

            # Enabling/disabling a pipeline no longer touches any correlation job: the input list
            # is derived from membership and a member's tagged-events topic exists from creation,
            # so a disabled pipeline simply stops producing into a topic that still exists.
            if "tagged_events_topic" in changes.update_data:
                # The correlation job's KAFKA_INPUT_TOPICS really changes, and it changes by
                # SUBTRACTION: this member stops feeding the topic it fed before (usually the shared
                # legacy one, which carries other customers' events). A re-render would leave the
                # old topic in the restored source's splits, so this is a cold start.
                await self._sync_correlation_pipelines_non_critical(
                    user, pipeline_id, reason="pipeline_tagged_topic_assigned", cold_start=True
                )

        except Exception as e:
            logger.error(
                "Failed to update FlinkDeployment or DB",
                extra={"pipeline_id": str(pipeline_id), "error": str(e)},
            )
            await activity_producer.log_action(
                action="error",
                entity_type="flink",
                entity_id=str(pipeline_id),
                details=f"Failed to update FlinkDeployment or DB: {str(e)}",
                source="system",
                severity="error",
            )

            if isinstance(e, HTTPException):
                raise

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update FlinkDeployment: {str(e)}",
            ) from e

        # Refresh to get final state
        updated = await self.pipeline_repo.get_by_id(pipeline_id)

        # Commit BEFORE invalidating the dashboard topology cache (same pattern as delete)
        # so the next SSE tick reloads post-update state, not the pre-commit rows.
        await self.db.commit()
        dashboard_service.invalidate_pipeline_cache(pipeline_id)

        # Log activity
        await self._log_update_activity(user, updated, old_enabled, new_enabled)

        return PipelineResponse(id=str(updated.id), status=True, message="Pipeline updated successfully")

    async def _handle_repository_changes(
        self,
        pipeline_id: UUID,
        old_repository_selections: dict[UUID, list[str]],
        new_repository_selections: dict[UUID, list[str]],
    ) -> None:
        """Handle repository changes for pipeline."""
        if old_repository_selections == new_repository_selections:
            return

        await self._get_rules_orchestrator().handle_pipeline_update(
            pipeline_id=pipeline_id,
            repository_selections=new_repository_selections,
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
                await self._log_kafka_sync_warning(pipeline_id, "update", "filters", filter_err)

    async def _handle_disable_scenario(self, pipeline_id: UUID) -> None:
        """Handle scenario 1: Disabling pipeline (true → false).

        Uses suspend instead of delete to preserve job ID and checkpoint history.
        The Flink Operator will create a savepoint and stop the job gracefully.
        """
        logger.info(
            "Disabling pipeline - suspending FlinkDeployment (preserving state and job ID)",
            extra={"pipeline_id": str(pipeline_id)},
        )

        deployment_name = await self._flink.find_deployment(str(pipeline_id))
        if deployment_name:
            try:
                await self._flink.suspend_deployment(str(pipeline_id))
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
                await self._flink.delete_deployment(str(pipeline_id))
        else:
            logger.info(
                "No deployment found to suspend",
                extra={"pipeline_id": str(pipeline_id)},
            )

    async def _handle_enable_scenario(self, pipeline_id: UUID, updated: Pipeline) -> None:
        """Handle scenario 2: Enabling pipeline (false → true).

        If a suspended deployment exists, patch its spec from the current DB row (so CPU/RAM/
        parallelism/window/checkpoint edits made while disabled take effect) and start it.
        The template sets job.state=running; upgradeMode last-state restores from checkpoint.
        If none exists or the patch fails for a non-quota reason, create a new deployment.
        """
        deployment_name = await self._flink.find_deployment(str(pipeline_id))

        if deployment_name:
            logger.info(
                "Enabling pipeline - applying current spec and resuming from checkpoint",
                extra={"pipeline_id": str(pipeline_id), "deployment_name": deployment_name},
            )

            try:
                config = self._build_deployment_config(updated)
                result = await self._flink.update_deployment(config)

                if not result.success:
                    raise Exception(result.error or "Failed to update deployment")

                await self._sync_kafka_data_on_enable(pipeline_id, updated)

                logger.info(
                    "Pipeline enabled (spec applied, checkpoint restored)",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "deployment_name": result.deployment_name,
                        "note": "Restored from last checkpoint, no data loss",
                    },
                )
                return

            except FlinkQuotaExceededError as quota_error:
                raise self._quota_exceeded_http_exception(quota_error) from quota_error

            except Exception as update_error:
                logger.warning(
                    "Failed to update existing deployment, will try creating new one",
                    extra={"pipeline_id": str(pipeline_id), "error": str(update_error)},
                )

        # No deployment or update failed - create new one
        logger.info(
            "Enabling pipeline - creating new FlinkDeployment",
            extra={"pipeline_id": str(pipeline_id)},
        )

        flink_created = False
        try:
            config = self._build_deployment_config(updated)
            result = await self._flink.create_deployment(config)

            if not result.success:
                raise Exception(result.error or "Failed to create deployment")

            flink_created = True

            await self.pipeline_repo.update(pipeline_id, deployment_name=result.deployment_name)

            # Sync data to Kafka
            await self._sync_kafka_data_on_enable(pipeline_id, updated)

            logger.info(
                "Pipeline enabled successfully (fresh start)",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "deployment_name": result.deployment_name,
                },
            )

        except FlinkQuotaExceededError as quota_error:
            raise self._quota_exceeded_http_exception(quota_error) from quota_error

        except Exception:
            # Compensating transaction
            if flink_created:
                try:
                    await self._flink.delete_deployment(str(updated.id))
                except Exception as rb_error:
                    logger.error("Rollback failed", extra={"error": str(rb_error)})
                    await activity_producer.log_action(
                        action="error",
                        entity_type="flink",
                        entity_id=str(updated.id),
                        details=f"Rollback failed on enable: {str(rb_error)}",
                        source="system",
                        severity="error",
                    )

            raise

    async def _sync_kafka_data_on_enable(self, pipeline_id: UUID, updated: Pipeline) -> None:
        """Sync rules, filters, parser, and custom_fields to Kafka on enable/resume."""
        # Check if re-validation needed
        needs_revalidation = updated.needs_restart or False

        await self._get_rules_orchestrator().handle_pipeline_enable(pipeline_id, needs_revalidation=needs_revalidation)

        if updated.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_enable(
                    pipeline_id=pipeline_id,
                    filter_ids=[UUID(f) for f in updated.filters],
                )
            except Exception as filter_err:
                await self._log_kafka_sync_warning(pipeline_id, "enable", "filters", filter_err)

        try:
            await self._get_parsers_orchestrator().handle_pipeline_enable(pipeline_id)
        except Exception as parser_err:
            await self._log_kafka_sync_warning(pipeline_id, "enable", "parser", parser_err)

        if updated.custom_fields:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_enable(
                    pipeline_id=pipeline_id,
                    custom_fields=updated.custom_fields,
                )
            except Exception as custom_fields_err:
                await self._log_kafka_sync_warning(pipeline_id, "enable", "custom_fields", custom_fields_err)

        # Clear needs_restart flag AFTER all sync operations (ISSUE #3 fix)
        if needs_revalidation:
            validation_service = SigmaValidationService(self.db)
            await validation_service.clear_pipeline_restart_flag(pipeline_id)
            logger.info(f"Cleared needs_restart flag for pipeline {pipeline_id}")

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

        # `source_topics` changed, so the checkpointed offsets are invalid and the state has to go.
        # That means a COLD START — delete, wait for the pods to be gone, wipe, recreate — not a
        # wipe under the running deployment, which is what this used to do. Two reasons:
        #
        #   * the busybox cleanup Job would `rm -rf` the state directory the live TaskManager has
        #     bind-mounted through `subPath` (§22), orphaning the inode silently;
        #   * a `last-state` upgrade restores the source's ASSIGNED SPLITS from the checkpoint, not
        #     from the rendered topic list, so a topic dropped from `source_topics` keeps being
        #     consumed until the state is really gone (measured on the correlation job's identical
        #     connector). Wiping state mid-upgrade got neither: the reset did not take and the mount
        #     was corrupted.
        #
        # Deferring the wipe to the next stop was the alternative and is worse: it leaves the
        # pipeline reading a topic the operator removed, with nothing saying so.
        cold_start = changes.cleanup_needed
        if cold_start:
            outcome = await self._terminate_then_cleanup(pipeline_id, operation="restart")
            # ONLY a skipped teardown falls back: the CR is then still there, so patching it is
            # both possible and the least-bad outcome. Every other status means the deployment WAS
            # deleted — a patch would 404 — so we recreate even when the state wipe itself refused.
            # The refusal is already recorded; the new job simply restores state we meant to drop.
            if outcome.startswith("skipped"):
                cold_start = False

        try:
            config = self._build_deployment_config(updated)
            # A cold start deleted the CR, so `update_deployment` would 404 — recreate it. The new
            # job starts from `kafka_starting_offset` with no state, which IS the offset reset.
            result = (
                await self._flink.create_deployment(config)
                if cold_start
                else await self._flink.update_deployment(config)
            )

            if not result.success:
                raise Exception(result.error or "Failed to update deployment")

            # Note: detectflow_matchnode_version is updated via Kafka metrics from Flink

            logger.info(
                "FlinkDeployment spec updated (operator will perform rolling upgrade)",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "deployment_name": result.deployment_name,
                },
            )

            await self._sync_kafka_data_on_enable(pipeline_id, updated)

        except FlinkQuotaExceededError as quota_error:
            raise self._quota_exceeded_http_exception(quota_error) from quota_error

        except Exception as flink_error:
            logger.error(
                "Failed to update FlinkDeployment",
                extra={"pipeline_id": str(pipeline_id), "error": str(flink_error)},
            )
            await activity_producer.log_action(
                action="error",
                entity_type="flink",
                entity_id=str(pipeline_id),
                details=f"Failed to update FlinkDeployment on restart: {str(flink_error)}",
                source="system",
                severity="error",
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
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink",
                    entity_id=str(pipeline_id),
                    details=f"Rollback failed on restart: {str(rollback_error)}",
                    source="system",
                    severity="error",
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
                await self._log_kafka_sync_warning(pipeline_id, "hot-reload", "parser", parser_err)

        # Sync custom_fields if changed
        if changes.custom_fields_changed:
            try:
                await self._get_custom_fields_orchestrator().handle_pipeline_update(
                    pipeline_id,
                    changes.old_custom_fields,
                    changes.new_custom_fields,
                )
            except Exception as custom_fields_err:
                await self._log_kafka_sync_warning(pipeline_id, "hot-reload", "custom_fields", custom_fields_err)

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

    async def assign_tagged_events_topic(
        self,
        pipeline_id: UUID,
        request: TaggedEventsTopicAssignRequest,
        user: User,
    ) -> PipelineResponse:
        """Move a pre-epic pipeline onto its own tagged-events topic.

        ⚠️ RESTARTS THE DETECTION PIPELINE (~30 s). This is the ONLY operation in the
        per-customer-correlation epic that restarts a customer's matcher, and the only sanctioned
        writer of ``pipelines.tagged_events_topic``. It changes KAFKA_TAGGED_EVENTS_TOPIC on the
        matcher deployment, so the Flink Operator upgrades the job from its last checkpoint.
        Idempotent: a pipeline that already has a topic returns without provisioning or restarting.

        ``tagged_events_topic IS NULL`` is load-bearing, not "not filled in yet": it keeps the row
        on the legacy shared topic and on the legacy singleton correlation path. Nothing migrates a
        pipeline implicitly — not membership, not auto-add, not the migration runbook's PATCH.

        Args:
            pipeline_id: Detection pipeline to migrate.
            request: Must carry ``confirm_restart=true``.
            user: Current user for activity logging.

        Returns:
            PipelineResponse for the assigned pipeline.

        Raises:
            BadRequestError: 400 — ``confirm_restart`` was not ``True``.
            PipelineNotFoundError: The pipeline does not exist.
            ServiceUnavailableError: 503 — the broker could not provision the topic.
        """
        if request.confirm_restart is not True:
            raise BadRequestError(
                "Assigning a dedicated tagged-events topic restarts this detection pipeline (~30s). "
                "Re-send with confirm_restart=true."
            )

        existing = await self.pipeline_repo.get_by_id(pipeline_id)
        if not existing:
            logger.warning("Pipeline not found for topic assignment", extra={"pipeline_id": str(pipeline_id)})
            raise PipelineNotFoundError(str(pipeline_id))

        if existing.tagged_events_topic:
            logger.info(
                "Pipeline already has a dedicated tagged-events topic; nothing to do",
                extra={"pipeline_id": str(pipeline_id), "topic": existing.tagged_events_topic},
            )
            return PipelineResponse(
                id=str(pipeline_id),
                status=True,
                message="Pipeline already has a dedicated tagged-events topic",
            )

        await self._provision_tagged_events_topic(existing)
        topic = existing.tagged_events_topic

        if existing.enabled:
            # One restart implementation for the whole orchestrator: build the changes the update
            # path would have built and route through the same scenario handler. ``existing`` is the
            # identity-mapped row the DAO just refreshed, so it already carries the new topic.
            await self._handle_restart_scenario(
                pipeline_id,
                existing,
                PipelineChanges(
                    update_data={"tagged_events_topic": topic},
                    flink_restart_needed=True,
                    cleanup_needed=False,
                    filters_changed=False,
                    log_source_changed=False,
                    old_log_source_id=existing.log_source_id,
                    new_log_source_id=existing.log_source_id,
                    custom_fields_changed=False,
                    old_custom_fields=existing.custom_fields,
                    new_custom_fields=existing.custom_fields,
                ),
            )

        await self.db.commit()
        dashboard_service.invalidate_pipeline_cache(pipeline_id)

        # The owning correlation job's KAFKA_INPUT_TOPICS now really changes — and by subtraction:
        # the topic this member fed before (the shared legacy one) leaves the list, so the job must
        # be cold-started or it keeps reading it from its restored splits.
        await self._sync_correlation_pipelines_non_critical(
            user, pipeline_id, reason="pipeline_tagged_topic_assigned", cold_start=True
        )

        await activity_producer.log_action(
            action="update",
            entity_type="pipeline",
            entity_id=str(pipeline_id),
            entity_name=existing.name,
            user=user,
            details=f"Assigned dedicated tagged-events topic {topic} to pipeline {existing.name}",
            changes={"tagged_events_topic": {"old": None, "new": topic}},
        )

        return PipelineResponse(id=str(pipeline_id), status=True, message="Tagged-events topic assigned")

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
        tagged_events_topic = existing.tagged_events_topic

        # Resolve membership BEFORE the row is deleted — the FK cascade destroys the join rows.
        correlation_pipeline_ids = await self._correlation_owners_before_delete(pipeline_id)

        # Delete the FlinkDeployment and wipe its state — in that order, and only if the first
        # actually finished. Never `rm -rf` under a live TaskManager mount.
        await self._terminate_then_cleanup(pipeline_id, operation="delete")

        # Delete from Kafka
        await self._delete_from_kafka(pipeline_id, existing)

        # Delete pipeline from database
        deleted = await self.pipeline_repo.delete(pipeline_id)
        if not deleted:
            logger.error("Failed to delete pipeline from database", extra={"pipeline_id": str(pipeline_id)})
            await activity_producer.log_action(
                action="error",
                entity_type="database",
                entity_id=str(pipeline_id),
                details="Failed to delete pipeline from database",
                source="system",
                severity="error",
            )
            raise PipelineNotFoundError(str(pipeline_id))

        await self.db.commit()

        # Invalidate caches
        dashboard_service.invalidate_pipeline_cache(pipeline_id)

        # Re-render the correlation job WITHOUT the departing topic first; only then retire it.
        # Retiring while a running job still subscribes produces UNKNOWN_TOPIC_OR_PARTITION churn.
        await self._detach_from_correlation_pipelines(user, correlation_pipeline_ids, pipeline_id, pipeline_name)
        await self._retire_tagged_events_topic(pipeline_id, tagged_events_topic)

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

    async def _correlation_owners_before_delete(self, pipeline_id: UUID) -> list[UUID]:
        """Correlation pipelines owning ``pipeline_id``, captured before the FK cascade wipes them.

        Non-critical (hard constraint 3): an unresolvable membership degrades to "no owners" and the
        pipeline is still deleted.
        """
        try:
            return await CorrelationPipelinesManager(self.db).get_ids_by_member(pipeline_id)
        except Exception as error:
            logger.warning(
                "Failed to resolve correlation-pipeline membership before delete (non-critical)",
                extra={"pipeline_id": str(pipeline_id), "error": str(error)},
            )
            return []

    async def _terminate_then_cleanup(self, pipeline_id: UUID, *, operation: str) -> str:
        """Delete this pipeline's FlinkDeployment, then wipe its state — only if it really went.

        The handshake the correlation runtime already uses (``teardown_deployment_pair`` /
        ``cold_start_correlation_job`` in ``apps/managers/correlation_pipelines.py``), mirrored here
        rather than forked: ``delete_deployment(wait=True)`` reuses the provider's single
        ``_wait_for_deletion`` poll loop, so there is no second copy of it.

        Why the order is load-bearing: the cleanup is a busybox Job that ``rm -rf``s the pipeline's
        state directory, and since §22 the Flink pods bind-mount **that directory itself** through
        `subPath`, not its parent. Deleting it under a live mount leaves the TaskManager writing
        into an orphaned inode — no error, no data, and nothing visible until the pod restarts into
        an empty state root. Before ``subPath`` the pod would at least have recreated it under the
        parent mount. A cleanup that never runs only costs a replay of stale checkpoints, so when
        the wait does not confirm termination we skip it and say so.

        Returns ``"ok"``, ``"error"`` or ``"skipped: wait_timeout"``.
        """
        try:
            await self._flink.delete_deployment(str(pipeline_id))
            logger.info("FlinkDeployment deleted successfully", extra={"pipeline_id": str(pipeline_id)})
        except Exception as e:
            # Includes the wait timing out: the CR may still exist with its pods up.
            await self._log_flink_warning(pipeline_id, operation, e)
            logger.warning(
                "Skipping pipeline storage cleanup: the FlinkDeployment did not terminate",
                extra={"pipeline_id": str(pipeline_id), "operation": operation},
            )
            await activity_producer.log_action(
                action="error",
                entity_type="flink",
                entity_id=str(pipeline_id),
                details=(
                    "Storage cleanup skipped: the FlinkDeployment did not terminate in time, and "
                    "wiping state under a live TaskManager mount corrupts it. Stale checkpoints "
                    "remain; they are reclaimed by the next successful stop or delete."
                ),
                source="system",
                severity="warning",
            )
            return "skipped: wait_timeout"

        try:
            outcome = await self._flink.cleanup_storage(str(pipeline_id))
        except Exception as cleanup_error:
            await self._log_k8s_warning(pipeline_id, operation, "storage_cleanup", cleanup_error)
            return "error"

        if outcome.ok:
            logger.info("Storage cleaned up successfully", extra={"pipeline_id": str(pipeline_id)})
            return "ok"

        # The Job ran and REFUSED (or never finished). Since §22 the script fails closed, so this
        # means the state directory still exists and the next start restores from it.
        logger.warning(
            "Pipeline storage cleanup did not complete; state still exists",
            extra={
                "pipeline_id": str(pipeline_id),
                "operation": operation,
                "status": outcome.status,
                "reason": outcome.reason,
            },
        )
        await activity_producer.log_action(
            action="error",
            entity_type="flink",
            entity_id=str(pipeline_id),
            details=(
                f"Pipeline state cleanup {outcome.status}: {outcome.reason or 'no reason reported'}. "
                "The state directory still exists, so the job will restore from it instead of "
                "starting from the configured offsets."
            ),
            source="system",
            severity="warning",
        )
        return outcome.describe()

    async def _detach_from_correlation_pipelines(
        self,
        user: User,
        correlation_pipeline_ids: list[UUID],
        pipeline_id: UUID,
        pipeline_name: str,
    ) -> None:
        """Re-render every correlation job that just lost this member, and say so in the activity log.

        Membership is cascade-removed rather than blocking the delete with ``EntityInUseError``: a
        detection pipeline is the customer's asset, a correlation pipeline is a *view* over a set of
        them (``pipeline_repositories`` sets the same ``ON DELETE CASCADE`` precedent). A correlation
        pipeline left with zero members is not auto-stopped — it reports as idle.

        ``cold_start=True``: this is the removal case by definition — the deleted pipeline's
        tagged-events topic leaves ``KAFKA_INPUT_TOPICS``, and a re-render would leave the
        correlation job consuming it from its restored splits.
        """
        manager = CorrelationPipelinesManager(self.db)
        for correlation_pipeline_id in correlation_pipeline_ids:
            try:
                await manager.restart_if_running(correlation_pipeline_id, user, cold_start=True)
                await activity_producer.log_action(
                    action="update",
                    entity_type="correlation_pipeline",
                    entity_id=str(correlation_pipeline_id),
                    user=user,
                    details=f"Removed member pipeline {pipeline_name} (deleted)",
                )
            except Exception as error:
                logger.warning(
                    "Failed to refresh correlation pipeline after member delete (non-critical)",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "correlation_pipeline_id": str(correlation_pipeline_id),
                        "error": str(error),
                    },
                )

    async def _retire_tagged_events_topic(self, pipeline_id: UUID, tagged_events_topic: str | None) -> None:
        """Retire the pipeline's dedicated tagged-events topic — best effort, never fatal.

        A NULL ``tagged_events_topic`` means the row never owned one (it rode the legacy shared
        topic, which is never retired), so there is nothing to do.

        The manager never deletes a topic itself and never branches on
        ``settings.kafka_topic_delete_policy``: the lifecycle service decides (default ``retire``)
        and may downgrade a ``delete`` when a consumer group still has lag or live members.
        """
        if not tagged_events_topic:
            return

        try:
            outcomes = await topic_lifecycle_service.retire_pipeline_topics(pipeline_id)
        except Exception as error:
            await self._log_kafka_sync_warning(pipeline_id, "delete", "tagged_events_topic", error)
            return

        for outcome in outcomes:
            if outcome.action != outcome.requested_policy:
                logger.warning(
                    "Tagged-events topic retirement downgraded",
                    extra={
                        "pipeline_id": str(pipeline_id),
                        "topic": outcome.topic,
                        "requested_policy": outcome.requested_policy,
                        "action": outcome.action,
                        "reason": outcome.reason,
                    },
                )
                await activity_producer.log_action(
                    action="warning",
                    entity_type="kafka",
                    entity_id=str(pipeline_id),
                    details=(
                        f"{outcome.topic}: {outcome.requested_policy} downgraded to {outcome.action}"
                        + (f" ({outcome.reason})" if outcome.reason else "")
                    ),
                    source="system",
                    severity="warning",
                )

    async def _delete_from_kafka(self, pipeline_id: UUID, existing: Pipeline) -> None:
        """Delete pipeline data from Kafka topics."""
        # Delete parser
        try:
            await self._get_parsers_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as parser_err:
            await self._log_kafka_sync_warning(pipeline_id, "delete", "parser", parser_err)

        # Delete rules
        try:
            await self._get_rules_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as rules_err:
            await self._log_kafka_sync_warning(pipeline_id, "delete", "rules", rules_err)

        # Delete filters
        if existing.filters:
            try:
                await self._get_filters_orchestrator().handle_pipeline_deletion(
                    pipeline_id=pipeline_id,
                    filter_ids=[UUID(f) for f in existing.filters],
                )
            except Exception as filter_err:
                await self._log_kafka_sync_warning(pipeline_id, "delete", "filters", filter_err)

        # Delete custom_fields
        try:
            await self._get_custom_fields_orchestrator().handle_pipeline_deletion(pipeline_id)
        except Exception as custom_fields_err:
            await self._log_kafka_sync_warning(pipeline_id, "delete", "custom_fields", custom_fields_err)

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

        # Get repository selections
        repository_selections = await self.pipeline_repo.get_repository_selections(pipeline_id)
        repository_ids = [
            PipelineRepositorySelection(id=str(repository_id), folders=folder_paths)
            for repository_id, folder_paths in repository_selections.items()
        ]

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

        # Get rule validation counts
        validation_service = SigmaValidationService(self.db)
        supported_rules_count, total_rules_count = await validation_service.get_supported_rules_count(pipeline_id)

        # Build warnings list
        warnings: list[str] = []
        if total_rules_count > 0 and supported_rules_count == 0:
            warnings.append("All rules in this pipeline are unsupported. No events will be tagged.")
        elif total_rules_count > 0 and supported_rules_count < total_rules_count:
            unsupported_count = total_rules_count - supported_rules_count
            warnings.append(f"{unsupported_count} rule(s) are unsupported and will not be used for tagging.")
        if pipeline.needs_restart:
            warnings.append("Pipeline needs restart due to rule loader module update.")

        destinations = pipeline_destinations(pipeline)
        dest_topic, save_untagged, apply_parser = legacy_fields_from_destinations(destinations)

        return PipelineDetailResponse(
            id=str(pipeline.id),
            name=pipeline.name,
            source_topics=pipeline.source_topics or [],
            destinations=[PipelineOutputDestination.model_validate(d) for d in destinations],
            destination_topic=dest_topic,
            tagged_events_topic=pipeline.tagged_events_topic,
            save_untagged=save_untagged,
            apply_parser_to_output_events=apply_parser,
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
            detectflow_matchnode_version=pipeline.detectflow_matchnode_version,
            warnings=warnings,
            supported_rules_count=supported_rules_count,
            total_rules_count=total_rules_count,
            needs_restart=pipeline.needs_restart,
            correlation_pipelines=correlation_pipeline_refs(pipeline),
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

        # Rule totals via ONE GROUP BY aggregate — replaces hydrating every
        # pipeline_rules/rules row just to compute two integers per pipeline.
        rule_counts = await self.pipeline_repo.get_rule_counts([p.id for p in pipelines])

        # Phase 1 (DB): extract every field needed into plain data while the ORM objects are
        # still attached, then end the transaction so the pooled connection is returned. The
        # per-pipeline status lookup below hits Flink REST -> K8s (up to ~5s each on an
        # unreachable Flink); holding the request's DB connection across those HTTP calls is
        # what drained the pool. ``AsyncSessionLocal`` uses ``expire_on_commit=False``, so the
        # already-loaded attributes stay readable after the commit.
        pipeline_rows: list[dict] = []
        for pipeline in pipelines:
            destinations = pipeline_destinations(pipeline)
            pipeline_rows.append(
                {
                    "id": str(pipeline.id),
                    "enabled": pipeline.enabled,
                    "name": pipeline.name,
                    "deployment_name": pipeline.deployment_name,
                    "source_topics": pipeline.source_topics or [],
                    "destination_topic": legacy_fields_from_destinations(destinations)[0],
                    "destinations": destinations,
                    "tagged_events_topic": pipeline.tagged_events_topic,
                    "repositories": (
                        [{"name": repo.name, "type": repo.type} for repo in pipeline.repositories]
                        if pipeline.repositories
                        else []
                    ),
                    "log_source": (
                        [
                            {
                                "name": pipeline.log_source.name,
                                "parser": pipeline.log_source.name if pipeline.log_source.parsing_config else "",
                            }
                        ]
                        if pipeline.log_source
                        else []
                    ),
                    "filters": len(pipeline.filters) if pipeline.filters else 0,
                    "rules": rule_counts.get(pipeline.id, (0, 0))[0],
                    "supported_rules": rule_counts.get(pipeline.id, (0, 0))[1],
                    "events_tagged": pipeline.events_tagged or 0,
                    "events_untagged": pipeline.events_untagged or 0,
                    "detectflow_matchnode_version": pipeline.detectflow_matchnode_version,
                    "needs_restart": pipeline.needs_restart or False,
                    "correlation_pipelines": correlation_pipeline_refs(pipeline),
                    "created": pipeline.created.isoformat(),
                    "updated": pipeline.updated.isoformat(),
                }
            )

        # Release the connection before the external status HTTP calls below.
        await self.db.commit()

        # Phase 2 (HTTP, no DB connection held): resolve Flink/K8s status for the
        # whole page concurrently. Sequentially this was up to page_size × the
        # Flink timeout on a cold cache. Bounded by a semaphore so a 100-row page
        # doesn't burst 100 sockets at Flink/K8s (and their to_thread calls don't
        # exhaust the shared default executor).
        status_semaphore = asyncio.Semaphore(15)

        async def _bounded_status(row: dict):
            async with status_semaphore:
                return await pipeline_status_manager.get_status_details(
                    pipeline_id=row["id"],
                    deployment_name=row["deployment_name"],
                    enabled=row["enabled"],
                )

        status_results = await asyncio.gather(*(_bounded_status(row) for row in pipeline_rows))

        data = []
        for row, (flink_status, status_details) in zip(pipeline_rows, status_results, strict=True):
            data.append(
                PipelineListItem(
                    id=row["id"],
                    enabled=row["enabled"],
                    name=row["name"],
                    source_topics=row["source_topics"],
                    status=flink_status,
                    status_details=status_details,
                    destination_topic=row["destination_topic"],
                    destinations=[PipelineOutputDestination.model_validate(d) for d in row["destinations"]],
                    tagged_events_topic=row["tagged_events_topic"],
                    repositories=row["repositories"],
                    log_source=row["log_source"],
                    filters=row["filters"],
                    rules=row["rules"],
                    supported_rules=row["supported_rules"],
                    events_tagged=row["events_tagged"],
                    events_untagged=row["events_untagged"],
                    detectflow_matchnode_version=row["detectflow_matchnode_version"],
                    needs_restart=row["needs_restart"],
                    correlation_pipelines=row["correlation_pipelines"],
                    created=row["created"],
                    updated=row["updated"],
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
