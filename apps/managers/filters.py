"""Filter management orchestrator.

This module provides the FiltersManager class for handling
filter lifecycle operations with activity logging and Kafka sync.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.error_tracker import ErrorTracker
from apps.core.exceptions import ConflictError, NotFoundError
from apps.core.logger import get_logger
from apps.core.models import Filter, User
from apps.core.schemas import FilterCreateRequest, FilterUpdateRequest
from apps.managers.filter import FiltersOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.filter import FilterDAO
from apps.modules.postgre.pipeline import PipelineDAO

logger = get_logger(__name__)


class FiltersManager:
    """Manager for filter lifecycle operations."""

    def __init__(self, db: AsyncSession):
        """Initialize manager with database session."""
        self.db = db
        self.filter_repo = FilterDAO(db)

    async def get_all(
        self,
        pagination: dict,
    ) -> tuple[list[Filter], int]:
        """Get paginated list of filters."""
        return await self.filter_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            search_fields=["name"],
            sort=pagination["sort"],
            order=pagination["order"],
        )

    async def get_active(self) -> list[Filter]:
        """Get filters currently assigned to pipelines."""
        return await self.filter_repo.get_active()

    async def get_by_id(self, filter_id: UUID) -> Filter:
        """Get filter by ID.

        Raises:
            NotFoundError: If filter not found
        """
        filter_obj = await self.filter_repo.get_by_id(filter_id)
        if not filter_obj:
            raise NotFoundError(f"Filter with id {filter_id} not found")
        return filter_obj

    async def create(
        self,
        request: FilterCreateRequest,
        user: User,
    ) -> Filter:
        """Create a new filter.

        Args:
            request: Filter creation request
            user: Current user for activity logging

        Returns:
            Created Filter instance
        """
        new_filter = await self.filter_repo.create(
            name=request.name,
            body=request.body,
            active=True,
        )

        await activity_producer.log_action(
            action="create",
            entity_type="filter",
            entity_id=str(new_filter.id),
            entity_name=new_filter.name,
            user=user,
            details=f"Created filter {new_filter.name}",
        )

        return new_filter

    async def update(
        self,
        filter_id: UUID,
        request: FilterUpdateRequest,
        user: User,
    ) -> Filter:
        """Update an existing filter.

        Args:
            filter_id: UUID of filter to update
            request: Update request with new values
            user: Current user for activity logging

        Returns:
            Updated Filter instance

        Raises:
            NotFoundError: If filter not found
        """
        existing = await self.filter_repo.get_by_id(filter_id)
        if not existing:
            raise NotFoundError(f"Filter with id {filter_id} not found")

        update_data = {}
        if request.name is not None:
            update_data["name"] = request.name
        if request.body is not None:
            update_data["body"] = request.body

        updated = await self.filter_repo.update(filter_id, **update_data)

        # Sync updated filter to Kafka for all pipelines using it
        try:
            await FiltersOrchestrator(self.db).handle_filter_update(filter_id)
            logger.info("Filter synced to Kafka", extra={"filter_id": str(filter_id)})
        except Exception as e:
            logger.warning(
                "Failed to sync filter to Kafka (non-critical)",
                extra={"filter_id": str(filter_id), "error": str(e)},
            )
            if ErrorTracker.should_log(f"kafka_filter_sync_{filter_id}"):
                await activity_producer.log_action(
                    action="warning",
                    entity_type="kafka_sync",
                    entity_id=str(filter_id),
                    details=f"Failed to sync filter to Kafka on update (non-critical): {str(e)}",
                    source="system",
                    severity="warning",
                )

        await activity_producer.log_action(
            action="update",
            entity_type="filter",
            entity_id=str(updated.id),
            entity_name=updated.name,
            user=user,
            details=f"Updated filter {updated.name}",
        )

        return updated

    async def delete(
        self,
        filter_id: UUID,
        user: User,
    ) -> bool:
        """Delete a filter.

        Args:
            filter_id: UUID of filter to delete
            user: Current user for activity logging

        Returns:
            True if deleted successfully

        Raises:
            NotFoundError: If filter not found
        """
        existing = await self.filter_repo.get_by_id(filter_id)
        if not existing:
            raise NotFoundError(f"Filter with id {filter_id} not found")

        filter_name = existing.name

        # Check if filter is used by any pipelines
        pipeline_dao = PipelineDAO(self.db)
        pipelines = await pipeline_dao.get_by_filter_id(filter_id)
        if pipelines:
            pipeline_names = ", ".join(p.name for p in pipelines)
            raise ConflictError(f"Cannot delete filter '{filter_name}': it is used by pipeline(s): {pipeline_names}")

        # Delete filter from Kafka BEFORE removing from DB (need pipeline associations)
        try:
            await FiltersOrchestrator(self.db).handle_filter_deletion(filter_id)
            logger.info("Filter deleted from Kafka", extra={"filter_id": str(filter_id)})
        except Exception as e:
            logger.warning(
                "Failed to delete filter from Kafka (non-critical)",
                extra={"filter_id": str(filter_id), "error": str(e)},
            )
            if ErrorTracker.should_log(f"kafka_filter_delete_{filter_id}"):
                await activity_producer.log_action(
                    action="warning",
                    entity_type="kafka_sync",
                    entity_id=str(filter_id),
                    details=f"Failed to delete filter from Kafka (non-critical): {str(e)}",
                    source="system",
                    severity="warning",
                )

        # Remove filter from all pipelines that reference it (cleanup stale references)
        await self.filter_repo.remove_from_pipelines(filter_id)

        deleted = await self.filter_repo.delete(filter_id)
        if not deleted:
            raise NotFoundError(f"Failed to delete filter with id {filter_id}")

        await activity_producer.log_action(
            action="delete",
            entity_type="filter",
            entity_id=str(filter_id),
            entity_name=filter_name,
            user=user,
            details=f"Deleted filter {filter_name}",
        )

        return True
