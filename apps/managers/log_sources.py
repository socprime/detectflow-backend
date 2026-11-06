"""Log source management orchestrator.

This module provides the LogSourcesManager class for handling
log source lifecycle operations with activity logging.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import BadRequestError, ConflictError, NotFoundError
from apps.core.logger import get_logger
from apps.core.models import LogSource, User
from apps.core.schemas import LogSourceCreateRequest, LogSourceUpdateRequest
from apps.managers.parser import parser_manager
from apps.managers.parser_sync import ParsersOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.log_source import LogSourceDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.utils import normalize_field_mapping_yaml

logger = get_logger(__name__)


def _normalize_mapping_or_bad_request(mapping: str | None) -> str | None:
    """Quote unsafe YAML scalars and validate mapping shape; raise 400 on failure."""
    try:
        return normalize_field_mapping_yaml(mapping)
    except ValueError as e:
        raise BadRequestError(str(e)) from e


class LogSourcesManager:
    """Manager for log source lifecycle operations."""

    def __init__(self, db: AsyncSession):
        """Initialize manager with database session."""
        self.db = db
        self.log_source_repo = LogSourceDAO(db)

    async def get_all(
        self,
        pagination: dict,
    ) -> tuple[list[LogSource], int]:
        """Get paginated list of log sources."""
        return await self.log_source_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            sort=pagination["sort"],
            order=pagination["order"],
            search=pagination["search"],
            search_fields=["name"],
        )

    async def get_by_id(self, log_source_id: UUID) -> LogSource:
        """Get log source by ID.

        Raises:
            NotFoundError: If log source not found
        """
        log_source = await self.log_source_repo.get_by_id(log_source_id)
        if not log_source:
            raise NotFoundError(f"Log source with id {log_source_id} not found")
        return log_source

    async def create(
        self,
        request: LogSourceCreateRequest,
        user: User,
    ) -> LogSource:
        """Create a new log source.

        Args:
            request: Log source creation request
            user: Current user for activity logging

        Returns:
            Created LogSource instance

        Raises:
            BadRequestError: If mapping YAML is invalid or not a field-mapping dictionary
        """
        create_data = {"name": request.name}

        if request.parsing_script:
            create_data["parsing_script"] = request.parsing_script
            create_data["parsing_config"] = await parser_manager.create_parser_config(request.parsing_script)

        if request.mapping:
            create_data["mapping"] = _normalize_mapping_or_bad_request(request.mapping)

        if request.test_topics is not None:
            create_data["test_topics"] = request.test_topics

        if request.test_repository_ids is not None:
            create_data["test_repository_ids"] = [selection.model_dump() for selection in request.test_repository_ids]

        new_log_source = await self.log_source_repo.create(**create_data)

        await activity_producer.log_action(
            action="create",
            entity_type="log_source",
            entity_id=str(new_log_source.id),
            entity_name=new_log_source.name,
            user=user,
            details=f"Created log source {new_log_source.name}",
        )

        return new_log_source

    async def update(
        self,
        log_source_id: UUID,
        request: LogSourceUpdateRequest,
        user: User,
    ) -> LogSource:
        """Update an existing log source.

        Args:
            log_source_id: UUID of log source to update
            request: Update request with new values
            user: Current user for activity logging

        Returns:
            Updated LogSource instance

        Raises:
            NotFoundError: If log source not found
            BadRequestError: If mapping YAML is invalid or not a field-mapping dictionary
        """
        existing = await self.log_source_repo.get_by_id(log_source_id)
        if not existing:
            raise NotFoundError(f"Log source with id {log_source_id} not found")

        old_sync_values = {
            "parsing_script": existing.parsing_script,
            "mapping": existing.mapping,
        }

        update_data = {}
        if request.name is not None:
            update_data["name"] = request.name

        if request.parsing_script is not None:
            update_data["parsing_script"] = request.parsing_script
            update_data["parsing_config"] = await parser_manager.create_parser_config(request.parsing_script)

        if request.mapping is not None:
            update_data["mapping"] = _normalize_mapping_or_bad_request(request.mapping)

        if request.test_topics is not None:
            update_data["test_topics"] = request.test_topics

        if request.test_repository_ids is not None:
            update_data["test_repository_ids"] = [selection.model_dump() for selection in request.test_repository_ids]

        updated = await self.log_source_repo.update(log_source_id, **update_data)

        # Sync parser metadata to Kafka when pipeline runtime payload changes.
        sync_fields = {"parsing_script", "mapping"}
        should_sync = any(
            field in update_data and old_sync_values[field] != update_data[field] for field in sync_fields
        )
        if should_sync:
            try:
                await ParsersOrchestrator(self.db).handle_log_source_parser_update(log_source_id)
                logger.info("Log source parser metadata synced to Kafka", extra={"log_source_id": str(log_source_id)})
            except Exception as e:
                logger.warning(
                    "Failed to sync log source parser metadata to Kafka (non-critical)",
                    extra={"log_source_id": str(log_source_id), "error": str(e)},
                )
                if ErrorTracker.should_log(f"kafka_sync_parser_{log_source_id}"):
                    await activity_producer.log_action(
                        action="warning",
                        entity_type="kafka_sync",
                        entity_id=str(log_source_id),
                        details=f"Failed to sync log source parser metadata to Kafka (non-critical): {str(e)}",
                        source="system",
                        severity="warning",
                    )

        # Activity logging (was missing!)
        await activity_producer.log_action(
            action="update",
            entity_type="log_source",
            entity_id=str(updated.id),
            entity_name=updated.name,
            user=user,
            details=f"Updated log source {updated.name}",
        )

        return updated

    async def delete(
        self,
        log_source_id: UUID,
        user: User,
    ) -> bool:
        """Delete a log source.

        Args:
            log_source_id: UUID of log source to delete
            user: Current user for activity logging

        Returns:
            True if deleted successfully

        Raises:
            NotFoundError: If log source not found
            ConflictError: If log source is in use by pipelines
        """
        existing = await self.log_source_repo.get_by_id(log_source_id)
        if not existing:
            raise NotFoundError(f"Log source with id {log_source_id} not found")

        log_source_name = existing.name

        # Check if log source is used by any pipelines
        pipeline_dao = PipelineDAO(self.db)
        pipelines = await pipeline_dao.get_by_log_source_id(log_source_id)
        if pipelines:
            pipeline_names = ", ".join(p.name for p in pipelines)
            raise ConflictError(
                f"Cannot delete log source '{log_source_name}': it is used by pipeline(s): {pipeline_names}"
            )

        deleted = await self.log_source_repo.delete(log_source_id)
        if not deleted:
            raise NotFoundError(f"Failed to delete log source with id {log_source_id}")

        # Activity logging (was missing!)
        await activity_producer.log_action(
            action="delete",
            entity_type="log_source",
            entity_id=str(log_source_id),
            entity_name=log_source_name,
            user=user,
            details=f"Deleted log source {log_source_name}",
        )

        return True
