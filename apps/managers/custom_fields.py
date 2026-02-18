"""Custom Fields Orchestrator.

Orchestrates custom fields synchronization between database and Kafka.
Handles custom fields lifecycle events for pipelines.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.logger import get_logger
from apps.modules.kafka.custom_fields import KafkaCustomFieldsSyncService
from apps.modules.postgre.pipeline import PipelineDAO

logger = get_logger(__name__)


class CustomFieldsOrchestrator:
    """Orchestrates custom fields synchronization with Kafka.

    Handles:
    - Pipeline creation: Send custom_fields to Kafka (if present)
    - Pipeline update: Sync custom_fields changes
    - Pipeline deletion: Remove custom_fields from Kafka
    - Pipeline enable: Send custom_fields to Kafka (if present)
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.kafka = KafkaCustomFieldsSyncService()
        self.pipeline_repo = PipelineDAO(db)

    async def handle_pipeline_creation(self, pipeline_id: UUID, custom_fields: str | None) -> None:
        """Handle pipeline creation - send custom_fields to Kafka.

        Args:
            pipeline_id: UUID of the created pipeline
            custom_fields: YAML string with custom fields (can be None)
        """
        if not custom_fields:
            logger.debug(f"Pipeline {pipeline_id} created without custom_fields")
            return

        logger.info(
            f"Sending custom_fields to Kafka for pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id)},
        )
        await self.kafka.send_custom_fields(str(pipeline_id), custom_fields)

    async def handle_pipeline_deletion(self, pipeline_id: UUID) -> None:
        """Handle pipeline deletion - remove custom_fields from Kafka.

        Args:
            pipeline_id: UUID of the pipeline being deleted
        """
        logger.info(
            f"Deleting custom_fields from Kafka for pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id)},
        )
        await self.kafka.delete_custom_fields(str(pipeline_id))

    async def handle_pipeline_enable(self, pipeline_id: UUID, custom_fields: str | None) -> None:
        """Handle pipeline enable - send custom_fields to Kafka.

        Called when pipeline is enabled (false → true).

        Args:
            pipeline_id: UUID of the enabled pipeline
            custom_fields: YAML string with custom fields (can be None)
        """
        if not custom_fields:
            logger.debug(f"Pipeline {pipeline_id} enabled without custom_fields")
            return

        logger.info(
            f"Sending custom_fields to Kafka for enabled pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id)},
        )
        await self.kafka.send_custom_fields(str(pipeline_id), custom_fields)

    async def handle_pipeline_update(
        self,
        pipeline_id: UUID,
        old_custom_fields: str | None,
        new_custom_fields: str | None,
    ) -> None:
        """Handle pipeline update - sync custom_fields changes.

        Args:
            pipeline_id: UUID of the updated pipeline
            old_custom_fields: Previous custom_fields value
            new_custom_fields: New custom_fields value
        """
        # No change
        if old_custom_fields == new_custom_fields:
            return

        # Custom fields removed (was set, now None/empty)
        if old_custom_fields and not new_custom_fields:
            logger.info(
                f"Deleting custom_fields from Kafka for pipeline {pipeline_id}",
                extra={"pipeline_id": str(pipeline_id)},
            )
            await self.kafka.delete_custom_fields(str(pipeline_id))
            return

        # Custom fields added or updated
        if new_custom_fields:
            logger.info(
                f"Updating custom_fields in Kafka for pipeline {pipeline_id}",
                extra={"pipeline_id": str(pipeline_id)},
            )
            await self.kafka.send_custom_fields(str(pipeline_id), new_custom_fields)
