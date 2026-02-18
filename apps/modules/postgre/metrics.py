"""Pipeline metrics repository with append-only pattern.

This module provides database operations for pipeline metrics using:
- INSERT for time-series history (append-only, never UPDATE)
- Atomic UPDATE for pipeline totals (events_tagged/events_untagged)
- UPSERT for per-rule match counters
- Batch queries to avoid N+1 problems
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.logger import get_logger
from apps.core.models import Pipeline, PipelineMetric, PipelineRuleMetric
from apps.core.schemas import KafkaMetricMessage

logger = get_logger(__name__)


class MetricsDAO:
    """Repository for pipeline metrics with append-only writes.

    Key design decisions:
    - History uses INSERT only (append-only, no updates)
    - Totals use atomic UPDATE with increment (not read-modify-write)
    - Batch queries for list endpoints
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def append_metric(self, pipeline_id: UUID, metric: KafkaMetricMessage) -> None:
        """Append a new metric record and update pipeline totals atomically.

        Args:
            pipeline_id: Pipeline UUID
            metric: Metric data from Kafka
        """
        tagged = metric.window_matched_events
        untagged = metric.window_total_events - metric.window_matched_events

        # 1. Insert into history (append-only)
        history_record = PipelineMetric(
            pipeline_id=pipeline_id,
            window_total_events=metric.window_total_events,
            window_matched_events=metric.window_matched_events,
            window_duration_seconds=metric.window_duration_seconds,
            throughput_eps=metric.window_throughput_eps,
            event_parsing_errors=metric.errors.event_parsing_errors,
            rule_parsing_errors=metric.errors.rule_parsing_errors,
            matching_errors=metric.errors.matching_errors,
        )
        self.session.add(history_record)

        # 2. Update pipeline totals atomically (single UPDATE, no read needed)
        await self.session.execute(
            update(Pipeline)
            .where(Pipeline.id == pipeline_id)
            .values(
                events_tagged=Pipeline.events_tagged + tagged,
                events_untagged=Pipeline.events_untagged + untagged,
            )
        )

        # Note: commit is handled by caller to ensure proper transaction management

        # 3. Upsert per-rule match counters if present
        if metric.rules and metric.rules.matched_rules:
            await self._upsert_rule_metrics(pipeline_id, metric)

        logger.debug(
            "Appended metric",
            extra={
                "pipeline_id": str(pipeline_id),
                "tagged": tagged,
                "untagged": untagged,
            },
        )

    async def _upsert_rule_metrics(self, pipeline_id: UUID, metric: KafkaMetricMessage) -> None:
        """Upsert per-rule match counters using PostgreSQL ON CONFLICT.

        Args:
            pipeline_id: Pipeline UUID
            metric: Metric data containing rule matches
        """
        if not metric.rules or not metric.rules.matched_rules:
            return

        for rule_match in metric.rules.matched_rules:
            try:
                rule_uuid = UUID(rule_match.rule_id)
            except (ValueError, TypeError):
                logger.warning(f"Invalid rule_id in metrics: {rule_match.rule_id}")
                continue

            # PostgreSQL UPSERT: INSERT ... ON CONFLICT DO UPDATE
            stmt = insert(PipelineRuleMetric).values(
                pipeline_id=pipeline_id,
                rule_id=rule_uuid,
                total_matches=rule_match.window_matches,
            )

            stmt = stmt.on_conflict_do_update(
                constraint="uq_pipeline_rule_metric",
                set_={
                    "total_matches": PipelineRuleMetric.total_matches + rule_match.window_matches,
                    "updated": datetime.now(UTC),
                },
            )

            await self.session.execute(stmt)

        logger.debug(
            "Upserted rule metrics",
            extra={
                "pipeline_id": str(pipeline_id),
                "rules_count": len(metric.rules.matched_rules),
            },
        )

    async def upsert_rule_metrics_only(self, pipeline_id: UUID, metric: KafkaMetricMessage) -> None:
        """Upsert ONLY per-rule match counters (public method for Kafka consumer).

        This is the same as _upsert_rule_metrics but exposed as public method.
        Used by MetricsConsumerService to update per-rule metrics WITHOUT
        updating pipeline totals (which are handled by FlinkMetricsPoller).

        Args:
            pipeline_id: Pipeline UUID
            metric: Metric data containing rule matches
        """
        await self._upsert_rule_metrics(pipeline_id, metric)

    async def get_rule_metrics(self, pipeline_id: str) -> list[dict]:
        """Get per-rule match totals for a pipeline.

        Args:
            pipeline_id: Pipeline UUID string

        Returns:
            List of dicts with rule_id and total_matches
        """
        result = await self.session.execute(
            select(PipelineRuleMetric.rule_id, PipelineRuleMetric.total_matches)
            .where(PipelineRuleMetric.pipeline_id == UUID(pipeline_id))
            .order_by(PipelineRuleMetric.total_matches.desc())
        )

        return [{"rule_id": str(row.rule_id), "total_matches": row.total_matches} for row in result.all()]

    async def get_rule_metrics_map(self, pipeline_id: str) -> dict[str, int]:
        """Get per-rule match totals as a map for quick lookup.

        Args:
            pipeline_id: Pipeline UUID string

        Returns:
            Dict mapping rule_id (str) to total_matches (int)
        """
        result = await self.session.execute(
            select(PipelineRuleMetric.rule_id, PipelineRuleMetric.total_matches).where(
                PipelineRuleMetric.pipeline_id == UUID(pipeline_id)
            )
        )

        return {str(row.rule_id): row.total_matches or 0 for row in result.all()}

    async def get_totals(self, pipeline_id: str) -> dict:
        """Get accumulated totals for a single pipeline.

        Args:
            pipeline_id: Pipeline UUID string

        Returns:
            Dict with events_tagged and events_untagged
        """
        result = await self.session.execute(
            select(Pipeline.events_tagged, Pipeline.events_untagged).where(Pipeline.id == UUID(pipeline_id))
        )
        row = result.first()
        if row:
            return {"events_tagged": row.events_tagged or 0, "events_untagged": row.events_untagged or 0}
        return {"events_tagged": 0, "events_untagged": 0}

    async def get_totals_batch(self, pipeline_ids: list[str]) -> dict[str, dict]:
        """Get totals for multiple pipelines in a single query.

        Args:
            pipeline_ids: List of pipeline UUID strings

        Returns:
            Dict mapping pipeline_id to totals dict
        """
        if not pipeline_ids:
            return {}

        uuids = [UUID(pid) for pid in pipeline_ids]
        result = await self.session.execute(
            select(Pipeline.id, Pipeline.events_tagged, Pipeline.events_untagged).where(Pipeline.id.in_(uuids))
        )

        return {
            str(row.id): {"events_tagged": row.events_tagged or 0, "events_untagged": row.events_untagged or 0}
            for row in result.all()
        }

    async def get_history(
        self,
        pipeline_id: str,
        hours: int = 24,
        limit: int = 1000,
    ) -> list[PipelineMetric]:
        """Get metrics history for a pipeline.

        Args:
            pipeline_id: Pipeline UUID string
            hours: How many hours of history to fetch
            limit: Maximum records to return

        Returns:
            List of PipelineMetric records ordered by timestamp desc
        """
        cutoff = datetime.now(UTC) - timedelta(hours=hours)

        result = await self.session.execute(
            select(PipelineMetric)
            .where(PipelineMetric.pipeline_id == UUID(pipeline_id))
            .where(PipelineMetric.timestamp >= cutoff)
            .order_by(PipelineMetric.timestamp.desc())
            .limit(limit)
        )

        return list(result.scalars().all())

    async def cleanup_old_metrics(self, retention_hours: int = 48) -> int:
        """Delete metrics older than retention period.

        Args:
            retention_hours: Hours to keep (default 48)

        Returns:
            Number of deleted records
        """
        cutoff = datetime.now(UTC) - timedelta(hours=retention_hours)

        result = await self.session.execute(delete(PipelineMetric).where(PipelineMetric.timestamp < cutoff))
        # Note: commit is handled by caller

        deleted = result.rowcount
        if deleted > 0:
            logger.info(
                "Cleaned up old metrics",
                extra={"deleted": deleted, "retention_hours": retention_hours},
            )

        return deleted

    async def delete_pipeline_metrics(self, pipeline_id: UUID) -> int:
        """Delete all metrics for a pipeline (called when pipeline is deleted).

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            Number of deleted records
        """
        result = await self.session.execute(delete(PipelineMetric).where(PipelineMetric.pipeline_id == pipeline_id))
        # Note: commit is handled by caller

        return result.rowcount
