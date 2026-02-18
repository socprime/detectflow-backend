"""Filters Orchestrator.

Orchestrates filter synchronization between database and Kafka.
Handles filter lifecycle events for pipelines.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.logger import get_logger
from apps.core.models import Filter
from apps.modules.kafka.filters import KafkaFiltersSyncService
from apps.modules.postgre.filter import FilterDAO
from apps.modules.postgre.pipeline import PipelineDAO

logger = get_logger(__name__)


class FilterNotFoundError(Exception):
    """Raised when a filter is not found."""

    pass


class FiltersOrchestrator:
    """Orchestrates filter synchronization with Kafka.

    Handles:
    - Pipeline creation: Send filters to Kafka
    - Pipeline update: Sync added/removed filters
    - Pipeline deletion: Remove filters from Kafka
    - Pipeline enable: Send all filters to Kafka
    - Filter update: Resync to all pipelines using it
    - Filter delete: Remove from all pipelines in Kafka
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.kafka = KafkaFiltersSyncService()
        self.filter_repo = FilterDAO(db)
        self.pipeline_repo = PipelineDAO(db)

    # ==========================================
    # PIPELINE LIFECYCLE
    # ==========================================

    async def handle_pipeline_creation(self, pipeline_id: UUID, filter_ids: list[UUID]) -> None:
        """Handle pipeline creation - send filters to Kafka.

        Args:
            pipeline_id: UUID of the created pipeline
            filter_ids: List of filter UUIDs assigned to pipeline
        """
        if not filter_ids:
            logger.debug(f"Pipeline {pipeline_id} created without filters")
            return

        filters = await self._get_filters_by_ids(filter_ids)
        if filters:
            logger.info(
                f"Sending {len(filters)} filters to Kafka for pipeline {pipeline_id}",
                extra={"pipeline_id": str(pipeline_id), "filter_count": len(filters)},
            )
            await self.kafka.send_filters(str(pipeline_id), filters)

    async def handle_pipeline_deletion(self, pipeline_id: UUID, filter_ids: list[UUID]) -> None:
        """Handle pipeline deletion - remove filters from Kafka.

        Args:
            pipeline_id: UUID of the pipeline being deleted
            filter_ids: List of filter UUIDs to remove
        """
        if not filter_ids:
            logger.debug(f"Pipeline {pipeline_id} has no filters to delete")
            return

        filter_id_strs = [str(fid) for fid in filter_ids]
        logger.info(
            f"Deleting {len(filter_id_strs)} filters from Kafka for pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id), "filter_count": len(filter_id_strs)},
        )
        await self.kafka.delete_filters(str(pipeline_id), filter_id_strs)

    async def handle_pipeline_enable(self, pipeline_id: UUID, filter_ids: list[UUID]) -> None:
        """Handle pipeline enable - send all filters to Kafka.

        Called when pipeline is enabled (false → true).

        Args:
            pipeline_id: UUID of the enabled pipeline
            filter_ids: List of filter UUIDs assigned to pipeline
        """
        if not filter_ids:
            logger.debug(f"Pipeline {pipeline_id} enabled without filters")
            return

        filters = await self._get_filters_by_ids(filter_ids)
        if filters:
            logger.info(
                f"Sending {len(filters)} filters to Kafka for enabled pipeline {pipeline_id}",
                extra={"pipeline_id": str(pipeline_id), "filter_count": len(filters)},
            )
            await self.kafka.send_filters(str(pipeline_id), filters)

    async def handle_pipeline_update(
        self,
        pipeline_id: UUID,
        added_filter_ids: list[UUID],
        removed_filter_ids: list[UUID],
    ) -> None:
        """Handle pipeline update - sync added/removed filters.

        Args:
            pipeline_id: UUID of the updated pipeline
            added_filter_ids: Filter UUIDs that were added
            removed_filter_ids: Filter UUIDs that were removed
        """
        logger.info(
            "FiltersOrchestrator.handle_pipeline_update called",
            extra={
                "pipeline_id": str(pipeline_id),
                "added_filter_ids": [str(fid) for fid in added_filter_ids],
                "removed_filter_ids": [str(fid) for fid in removed_filter_ids],
            },
        )

        # Send new filters to Kafka
        if added_filter_ids:
            filters = await self._get_filters_by_ids(added_filter_ids)
            logger.info(
                f"Fetched {len(filters)} active filters from DB for {len(added_filter_ids)} requested",
                extra={
                    "pipeline_id": str(pipeline_id),
                    "requested_ids": [str(fid) for fid in added_filter_ids],
                    "fetched_count": len(filters),
                },
            )
            if filters:
                logger.info(
                    f"Sending {len(filters)} added filters to Kafka for pipeline {pipeline_id}",
                    extra={"pipeline_id": str(pipeline_id), "filter_count": len(filters)},
                )
                await self.kafka.send_filters(str(pipeline_id), filters)

        # Delete removed filters from Kafka
        if removed_filter_ids:
            filter_id_strs = [str(fid) for fid in removed_filter_ids]
            logger.info(
                f"Deleting {len(filter_id_strs)} removed filters from Kafka for pipeline {pipeline_id}",
                extra={"pipeline_id": str(pipeline_id), "filter_count": len(filter_id_strs)},
            )
            await self.kafka.delete_filters(str(pipeline_id), filter_id_strs)

    # ==========================================
    # FILTER LIFECYCLE
    # ==========================================

    async def handle_filter_update(self, filter_id: UUID) -> None:
        """Handle filter update - resync to all pipelines using it.

        Args:
            filter_id: UUID of the updated filter
        """
        filter_obj = await self.filter_repo.get_by_id(filter_id)
        if not filter_obj:
            raise FilterNotFoundError(f"Filter {filter_id} not found")

        # Get all pipelines using this filter
        pipelines = await self.pipeline_repo.get_by_filter_id(filter_id)

        if not pipelines:
            logger.debug(f"Filter {filter_id} is not used by any pipeline")
            return

        logger.info(
            f"Resyncing filter {filter_id} to {len(pipelines)} pipelines",
            extra={"filter_id": str(filter_id), "pipeline_count": len(pipelines)},
        )

        # Send updated filter to each pipeline
        for pipeline in pipelines:
            if pipeline.enabled:
                await self.kafka.send_filter(str(pipeline.id), filter_obj)

    async def handle_filter_deletion(self, filter_id: UUID) -> None:
        """Handle filter deletion - remove from all pipelines in Kafka.

        Args:
            filter_id: UUID of the filter being deleted
        """
        # Get all pipelines using this filter BEFORE it's removed
        pipelines = await self.pipeline_repo.get_by_filter_id(filter_id)

        if not pipelines:
            logger.debug(f"Filter {filter_id} is not used by any pipeline")
            return

        logger.info(
            f"Deleting filter {filter_id} from {len(pipelines)} pipelines in Kafka",
            extra={"filter_id": str(filter_id), "pipeline_count": len(pipelines)},
        )

        # Delete filter from each pipeline
        for pipeline in pipelines:
            await self.kafka.delete_filter(str(pipeline.id), str(filter_id))

    # ==========================================
    # HELPER METHODS
    # ==========================================

    async def _get_filters_by_ids(self, filter_ids: list[UUID]) -> list[Filter]:
        """Get filter objects by their IDs.

        Args:
            filter_ids: List of filter UUIDs

        Returns:
            List of Filter objects
        """
        filters = []
        for filter_id in filter_ids:
            filter_obj = await self.filter_repo.get_by_id(filter_id)
            logger.info(
                f"Fetching filter {filter_id}",
                extra={
                    "filter_id": str(filter_id),
                    "found": filter_obj is not None,
                    "active": filter_obj.active if filter_obj else None,
                },
            )
            if filter_obj and filter_obj.active:
                filters.append(filter_obj)
        return filters
