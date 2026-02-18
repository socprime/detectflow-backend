"""Parsers Orchestrator.

Orchestrates parser synchronization between database and Kafka.
Handles parser lifecycle events for pipelines.

Parser config is embedded directly in LogSource:
Pipeline → log_source_id → LogSource (parsing_script, parsing_config, mapping)
"""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.core.logger import get_logger
from apps.core.models import LogSource, Pipeline
from apps.modules.kafka.parsers import KafkaParserPublisher

logger = get_logger(__name__)


class ParsersOrchestrator:
    """Orchestrates parser synchronization with Kafka.

    Handles:
    - Pipeline creation: Send parser to Kafka
    - Pipeline enable: Send parser to Kafka
    - Pipeline update (log_source changed): Resync parser
    - Pipeline deletion: Delete parser from Kafka
    - LogSource parser update: Resync to all pipelines using it
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.kafka = KafkaParserPublisher()

    async def handle_pipeline_creation(self, pipeline_id: UUID) -> None:
        """Handle pipeline creation - send parser and mapping to Kafka.

        Args:
            pipeline_id: UUID of the created pipeline
        """
        log_source = await self._get_log_source_for_pipeline(pipeline_id)
        if not log_source or not log_source.parsing_config:
            logger.debug(f"Pipeline {pipeline_id} has no parser configured")
            return

        logger.info(
            f"Sending parser to Kafka for pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id), "log_source_id": str(log_source.id)},
        )
        await self.kafka.send_parser_config(
            pipeline_id=str(pipeline_id),
            log_source_id=str(log_source.id),
            log_source_name=log_source.name,
            parsing_config=log_source.parsing_config,
            mapping=log_source.mapping,
        )

    async def handle_pipeline_enable(self, pipeline_id: UUID) -> None:
        """Handle pipeline enable - send parser and mapping to Kafka.

        Args:
            pipeline_id: UUID of the enabled pipeline
        """
        log_source = await self._get_log_source_for_pipeline(pipeline_id)
        if not log_source or not log_source.parsing_config:
            logger.debug(f"Pipeline {pipeline_id} has no parser configured")
            return

        logger.info(
            f"Sending parser to Kafka for enabled pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id), "log_source_id": str(log_source.id)},
        )
        await self.kafka.send_parser_config(
            pipeline_id=str(pipeline_id),
            log_source_id=str(log_source.id),
            log_source_name=log_source.name,
            parsing_config=log_source.parsing_config,
            mapping=log_source.mapping,
        )

    async def handle_pipeline_update(
        self,
        pipeline_id: UUID,
        old_log_source_id: UUID | None,
        new_log_source_id: UUID | None,
    ) -> None:
        """Handle pipeline update - resync parser if log_source changed.

        Args:
            pipeline_id: UUID of the updated pipeline
            old_log_source_id: Previous log source ID (may be None)
            new_log_source_id: New log source ID (may be None)
        """
        if old_log_source_id == new_log_source_id:
            return

        logger.info(
            f"Log source changed for pipeline {pipeline_id}, resyncing parser",
            extra={
                "pipeline_id": str(pipeline_id),
                "old_log_source_id": str(old_log_source_id) if old_log_source_id else None,
                "new_log_source_id": str(new_log_source_id) if new_log_source_id else None,
            },
        )

        if new_log_source_id:
            log_source = await self._get_log_source_for_pipeline(pipeline_id)
            if log_source and log_source.parsing_config:
                await self.kafka.send_parser_config(
                    pipeline_id=str(pipeline_id),
                    log_source_id=str(log_source.id),
                    log_source_name=log_source.name,
                    parsing_config=log_source.parsing_config,
                    mapping=log_source.mapping,
                )
            else:
                # New log source has no parser - delete old parser
                await self.kafka.delete_parser(str(pipeline_id))
        else:
            # Log source removed - delete parser
            await self.kafka.delete_parser(str(pipeline_id))

    async def handle_pipeline_deletion(self, pipeline_id: UUID) -> None:
        """Handle pipeline deletion - delete parser from Kafka.

        Args:
            pipeline_id: UUID of the pipeline being deleted
        """
        logger.info(
            f"Deleting parser from Kafka for pipeline {pipeline_id}",
            extra={"pipeline_id": str(pipeline_id)},
        )
        await self.kafka.delete_parser(str(pipeline_id))

    async def handle_log_source_parser_update(self, log_source_id: UUID) -> None:
        """Handle log source parser update - resync to all pipelines using it.

        Args:
            log_source_id: UUID of the updated log source
        """
        logger.info(
            f"Parser changed for log source {log_source_id}, resyncing pipelines",
            extra={"log_source_id": str(log_source_id)},
        )

        pipelines = await self._get_pipelines_by_log_source_id(log_source_id)
        if not pipelines:
            logger.debug(f"Log source {log_source_id} is not used by any pipeline")
            return

        log_source = await self._get_log_source_by_id(log_source_id)
        if not log_source:
            logger.warning(f"Log source {log_source_id} not found")
            return

        for pipeline in pipelines:
            if not pipeline.enabled:
                continue

            if log_source.parsing_config:
                await self.kafka.send_parser_config(
                    pipeline_id=str(pipeline.id),
                    log_source_id=str(log_source.id),
                    log_source_name=log_source.name,
                    parsing_config=log_source.parsing_config,
                    mapping=log_source.mapping,
                )
            else:
                # Parser removed - delete from Kafka
                await self.kafka.delete_parser(str(pipeline.id))

    async def _get_log_source_for_pipeline(self, pipeline_id: UUID) -> LogSource | None:
        """Get log source for a pipeline.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            LogSource model or None
        """
        result = await self.db.execute(
            select(Pipeline).options(selectinload(Pipeline.log_source)).where(Pipeline.id == pipeline_id)
        )
        pipeline = result.scalar_one_or_none()

        if not pipeline or not pipeline.log_source:
            return None

        return pipeline.log_source

    async def _get_log_source_by_id(self, log_source_id: UUID) -> LogSource | None:
        """Get log source by ID.

        Args:
            log_source_id: LogSource UUID

        Returns:
            LogSource model or None
        """
        result = await self.db.execute(select(LogSource).where(LogSource.id == log_source_id))
        return result.scalar_one_or_none()

    async def _get_pipelines_by_log_source_id(self, log_source_id: UUID) -> list[Pipeline]:
        """Get all pipelines using a specific log source.

        Args:
            log_source_id: LogSource UUID

        Returns:
            List of Pipeline models
        """
        result = await self.db.execute(select(Pipeline).where(Pipeline.log_source_id == log_source_id))
        return list(result.scalars().all())
