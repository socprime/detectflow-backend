"""Dashboard data aggregation service.

This module provides the main service for aggregating dashboard data
from multiple sources (PostgreSQL, Flink REST API).

Metrics are fetched from Flink REST API via FlinkMetricsPoller, replacing
the previous Kafka consumer approach.
"""

from collections import defaultdict
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.clients.flink_rest import FlinkJobMetrics
from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.models import Pipeline, Repository, Rule, pipeline_repositories
from apps.core.schemas import (
    DashboardData,
    DashboardGraphData,
    DashboardSnapshotResponse,
    DestinationTopicStats,
    PipelineStatsItem,
    RepositoryStats,
    SourceTopicStats,
)
from apps.managers.activity import activity_service
from apps.managers.pipeline_status import pipeline_status_manager

logger = get_logger(__name__)


class DashboardService:
    """Aggregates dashboard data from multiple sources.

    This service:
    1. Uses FlinkMetricsPoller to get real-time metrics from Flink REST API
    2. Aggregates data from PostgreSQL (topics, repos, events)
    3. Provides data for SSE and REST endpoints
    """

    def __init__(self):
        """Initialize the dashboard service."""
        # Cache of pipeline info for quick lookups
        # Key: pipeline_id, Value: dict with name, source_topics (list), destination_topic
        self._pipeline_cache: dict[UUID, dict[str, Any]] = {}
        self._is_running: bool = False

    async def start(self) -> None:
        """Start the dashboard service."""
        if self._is_running:
            return

        self._is_running = True

        # Preload pipeline cache
        await self._refresh_pipeline_cache()

        logger.info("DashboardService started")

    async def stop(self) -> None:
        """Stop the dashboard service."""
        self._is_running = False
        logger.info("DashboardService stopped")

    def _get_metrics_for_pipeline(self, pipeline_id: UUID) -> FlinkJobMetrics | None:
        """Get metrics for a pipeline from the poller cache.

        Args:
            pipeline_id: Pipeline UUID

        Returns:
            FlinkJobMetrics or None if not available
        """
        from apps.services.flink_metrics_poller import flink_metrics_poller

        return flink_metrics_poller.get_metrics(pipeline_id)

    async def get_dashboard_data(self) -> DashboardData:
        """Get complete dashboard data for SSE/REST.

        Returns:
            DashboardData with graph, pipeline stats, and recent activity
        """
        async with AsyncSessionLocal() as db:
            # Execute sequentially - AsyncSession doesn't support concurrent operations
            graph_data = await self._get_graph_data(db)
            pipeline_stats = await self._get_pipeline_stats(db)

            # Get recent activity from ActivityService
            recent_activity = await activity_service.get_recent()

            return DashboardData(
                graph=graph_data,
                pipelines_stats=pipeline_stats,
                recent_activity=recent_activity,
            )

    async def get_snapshot(self) -> DashboardSnapshotResponse:
        """Get dashboard snapshot for REST endpoint.

        Returns:
            Complete snapshot with health status
        """
        from apps.services.flink_metrics_poller import flink_metrics_poller

        data = await self.get_dashboard_data()

        return DashboardSnapshotResponse(
            graph=data.graph,
            pipelines_stats=data.pipelines_stats,
            recent_activity=data.recent_activity,
            last_kafka_update=flink_metrics_poller.last_poll_time,
            consumer_healthy=flink_metrics_poller.is_healthy,
        )

    async def _get_graph_data(self, db: AsyncSession) -> DashboardGraphData:
        """Get graph visualization data.

        Args:
            db: Database session

        Returns:
            DashboardGraphData for visualization
        """
        from apps.services.flink_metrics_poller import flink_metrics_poller

        # Get unique source topics from enabled pipelines only (using UNNEST for array)
        source_topics_query = await db.execute(
            select(func.unnest(Pipeline.source_topics).label("topic")).where(Pipeline.enabled.is_(True)).distinct()
        )
        source_topics = [row[0] for row in source_topics_query.fetchall() if row[0]]

        # Get unique destination topics from enabled pipelines only
        dest_topics_query = await db.execute(
            select(Pipeline.destination_topic).where(Pipeline.enabled.is_(True)).distinct()
        )
        dest_topics = [row[0] for row in dest_topics_query.fetchall() if row[0]]

        # Get repositories that are connected to enabled pipelines only
        repos_query = await db.execute(
            select(Repository)
            .join(pipeline_repositories, Repository.id == pipeline_repositories.c.repository_id)
            .join(Pipeline, Pipeline.id == pipeline_repositories.c.pipeline_id)
            .where(Pipeline.enabled.is_(True))
            .distinct()
        )
        repositories = repos_query.scalars().all()

        # Calculate rules count per repository
        repo_rules_counts = {}
        for repo in repositories:
            count_query = await db.execute(select(func.count(Rule.id)).where(Rule.repository_id == repo.id))
            repo_rules_counts[repo.id] = count_query.scalar() or 0

        # Get active pipelines count
        pipelines_query = await db.execute(select(func.count(Pipeline.id)).where(Pipeline.enabled.is_(True)))
        pipelines_count = pipelines_query.scalar() or 0

        # Get enabled pipelines for metrics lookup
        enabled_pipelines_query = await db.execute(select(Pipeline).where(Pipeline.enabled.is_(True)))
        enabled_pipelines = enabled_pipelines_query.scalars().all()

        # Aggregate metrics from Flink poller cache
        source_topic_eps = defaultdict(float)
        dest_topic_tagged_eps = defaultdict(float)
        dest_topic_untagged_eps = defaultdict(float)
        total_events_eps = 0.0
        total_tagged_eps = 0.0

        for pipeline in enabled_pipelines:
            cache_entry = flink_metrics_poller.get_cache_entry(pipeline.id)
            if not cache_entry or not cache_entry.metrics:
                continue

            metrics = cache_entry.metrics
            pipeline_info = self._pipeline_cache.get(pipeline.id, {})
            source_topics_list = pipeline_info.get("source_topics") or pipeline.source_topics or []
            dest_topic_name = pipeline_info.get("destination_topic") or pipeline.destination_topic

            # Distribute input EPS evenly across source topics (approximation)
            # NOTE: source_topics.eps uses IOMetrics (input_eps), while tagged_eps/untagged_eps
            # use custom Flink gauges. These are measured at different points in the pipeline,
            # so there may be slight inconsistencies. Ideally:
            #   source_topics.eps ≈ tagged_eps + untagged_eps
            # For full consistency, consider using: total_events_eps = tagged_eps + untagged_eps
            if source_topics_list and metrics.input_eps > 0:
                eps_per_topic = metrics.input_eps / len(source_topics_list)
                for source_topic_name in source_topics_list:
                    source_topic_eps[source_topic_name] += eps_per_topic
                total_events_eps += metrics.input_eps

            if dest_topic_name:
                # Use accurate tagged_eps/untagged_eps calculated from custom Flink gauges
                # These represent actual matched/unmatched events per second
                tagged_eps = cache_entry.tagged_eps
                untagged_eps = cache_entry.untagged_eps

                dest_topic_tagged_eps[dest_topic_name] += tagged_eps
                dest_topic_untagged_eps[dest_topic_name] += untagged_eps
                total_tagged_eps += tagged_eps

        # Build response
        source_topics_stats = [
            SourceTopicStats(
                id=topic_name,
                name=topic_name,
                eps=source_topic_eps.get(topic_name, 0.0),
            )
            for topic_name in source_topics
        ]

        dest_topics_stats = [
            DestinationTopicStats(
                id=topic_name,
                name=topic_name,
                tagged_eps=dest_topic_tagged_eps.get(topic_name, 0.0),
                untagged_eps=dest_topic_untagged_eps.get(topic_name, 0.0),
            )
            for topic_name in dest_topics
        ]

        repo_stats = [
            RepositoryStats(
                id=str(r.id),
                name=r.name,
                rules_count=repo_rules_counts.get(r.id, 0),
            )
            for r in repositories
        ]

        total_rules = sum(repo_rules_counts.values())

        return DashboardGraphData(
            source_topics=source_topics_stats,
            repositories=repo_stats,
            destination_topics=dest_topics_stats,
            pipelines_count=pipelines_count,
            total_events_eps=round(total_events_eps, 2),
            total_tagged_eps=round(total_tagged_eps, 2),
            total_rules=total_rules,
        )

    async def _get_pipeline_stats(self, db: AsyncSession) -> list[PipelineStatsItem]:
        """Get per-pipeline statistics.

        Args:
            db: Database session

        Returns:
            List of pipeline stats for the modal table
        """
        from apps.services.flink_metrics_poller import flink_metrics_poller

        # Get all enabled pipelines with repositories eagerly loaded
        pipelines_query = await db.execute(
            select(Pipeline).where(Pipeline.enabled.is_(True)).options(selectinload(Pipeline.repositories))
        )
        pipelines = pipelines_query.scalars().all()

        stats = []
        for pipeline in pipelines:
            # Get metrics from Flink poller cache
            metrics = flink_metrics_poller.get_metrics(pipeline.id)

            # Get EPS from cached metrics
            input_eps = metrics.input_eps if metrics else 0.0
            output_eps = metrics.output_eps if metrics else 0.0

            # Get topic lag from cached metrics (pending_records)
            topic_lag = metrics.pending_records if metrics and metrics.pending_records else 0

            # Get topic names from cache or fallback to DB
            cached_info = self._pipeline_cache.get(pipeline.id, {})
            source_topics = cached_info.get("source_topics") or pipeline.source_topics or []
            destination_topic = cached_info.get("destination_topic") or pipeline.destination_topic or ""

            # Get real-time Flink deployment status (Flink REST -> K8s fallback)
            flink_status, status_details = await pipeline_status_manager.get_status_details(
                pipeline_id=str(pipeline.id),
                deployment_name=pipeline.deployment_name,
                enabled=pipeline.enabled,
            )

            # Get repository IDs from cache or pipeline relationship
            repository_ids = cached_info.get("repository_ids") or [str(r.id) for r in pipeline.repositories] or []

            stats.append(
                PipelineStatsItem(
                    id=str(pipeline.id),
                    name=pipeline.name,
                    source_topics=source_topics,
                    destination_topic=destination_topic,
                    repository_ids=repository_ids,
                    input_eps=round(input_eps, 2),
                    output_eps=round(output_eps, 2),
                    topic_lag=topic_lag,
                    status=flink_status,
                    status_details=status_details,
                )
            )

        return stats

    async def _refresh_pipeline_cache(self) -> None:
        """Refresh the pipeline info cache from database."""
        try:
            async with AsyncSessionLocal() as db:
                pipelines_query = await db.execute(select(Pipeline))
                pipelines = pipelines_query.scalars().all()

                self._pipeline_cache = {
                    p.id: {
                        "name": p.name,
                        "source_topics": p.source_topics or [],
                        "destination_topic": p.destination_topic,
                        "enabled": p.enabled,
                    }
                    for p in pipelines
                }

                logger.debug(
                    "Pipeline cache refreshed",
                    extra={"count": len(self._pipeline_cache)},
                )
        except Exception as e:
            logger.exception(f"Failed to refresh pipeline cache: {e}")

    def invalidate_pipeline_cache(self, pipeline_id: UUID | None = None) -> None:
        """Invalidate pipeline cache.

        Args:
            pipeline_id: Specific pipeline to invalidate, or None for all
        """
        from apps.services.flink_metrics_poller import flink_metrics_poller

        if pipeline_id:
            self._pipeline_cache.pop(pipeline_id, None)
            flink_metrics_poller.clear_cache(pipeline_id)
        else:
            self._pipeline_cache.clear()
            flink_metrics_poller.clear_cache()


# Singleton instance
dashboard_service = DashboardService()
