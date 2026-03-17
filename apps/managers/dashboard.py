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
from apps.core.error_tracker import ErrorTracker
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
from apps.modules.kafka.activity import activity_producer
from apps.services.flink_metrics_poller import flink_metrics_poller

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
        return flink_metrics_poller.get_metrics(pipeline_id)

    async def get_dashboard_data(self) -> DashboardData:
        """Get complete dashboard data for SSE/REST.

        IMPORTANT: DB session is closed BEFORE making HTTP calls to Flink/K8s
        to avoid holding connections during slow external API calls.
        This prevents pool exhaustion with multiple SSE clients.

        Returns:
            DashboardData with graph, pipeline stats, and recent activity
        """
        # Phase 1: Fetch all data from DB (short-lived session)
        async with AsyncSessionLocal() as db:
            graph_data = await self._get_graph_data(db)
            # Get pipelines data only - status will be fetched outside DB session
            pipelines_data = await self._get_pipelines_for_stats(db)

        # Phase 2: Fetch Flink/K8s status OUTSIDE DB session (HTTP calls)
        # This is the slow part - can take 3-30 seconds for 50+ pipelines
        pipeline_stats = await self._build_pipeline_stats(pipelines_data)

        # Phase 3: Get recent activity (separate short-lived session inside)
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
        total_untagged_eps = 0.0

        for pipeline in enabled_pipelines:
            cache_entry = flink_metrics_poller.get_cache_entry(pipeline.id)
            if not cache_entry:
                continue

            pipeline_info = self._pipeline_cache.get(pipeline.id, {})
            source_topics_list = pipeline_info.get("source_topics") or pipeline.source_topics or []
            dest_topic_name = pipeline_info.get("destination_topic") or pipeline.destination_topic

            # Use input_eps from cache (custom Flink gauge)
            input_eps = cache_entry.input_eps
            if source_topics_list and input_eps > 0:
                eps_per_topic = input_eps / len(source_topics_list)
                for source_topic_name in source_topics_list:
                    source_topic_eps[source_topic_name] += round(eps_per_topic, 2)
                total_events_eps += input_eps

            if dest_topic_name:
                # Use tagged_eps/untagged_eps from cache (custom Flink gauges)
                tagged_eps = cache_entry.tagged_eps
                untagged_eps = cache_entry.untagged_eps

                dest_topic_tagged_eps[dest_topic_name] += round(tagged_eps, 2)
                dest_topic_untagged_eps[dest_topic_name] += round(untagged_eps, 2)
                total_tagged_eps += tagged_eps
                total_untagged_eps += untagged_eps

        # Build response
        source_topics_stats = [
            SourceTopicStats(
                id=topic_name,
                name=topic_name,
                eps=round(source_topic_eps.get(topic_name, 0.0), 2),
            )
            for topic_name in source_topics
        ]

        dest_topics_stats = [
            DestinationTopicStats(
                id=topic_name,
                name=topic_name,
                tagged_eps=round(dest_topic_tagged_eps.get(topic_name, 0.0), 2),
                untagged_eps=round(dest_topic_untagged_eps.get(topic_name, 0.0), 2),
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
            total_untagged_eps=round(total_untagged_eps, 2),
            total_rules=total_rules,
        )

    async def _get_pipelines_for_stats(self, db: AsyncSession) -> list[dict]:
        """Get pipeline data from DB for stats calculation.

        This method only fetches data from DB - no HTTP calls.
        HTTP calls for status are made separately outside the DB session.

        Args:
            db: Database session

        Returns:
            List of pipeline data dicts with all needed fields
        """
        # Get all enabled pipelines with repositories eagerly loaded
        pipelines_query = await db.execute(
            select(Pipeline).where(Pipeline.enabled.is_(True)).options(selectinload(Pipeline.repositories))
        )
        pipelines = pipelines_query.scalars().all()

        pipelines_data = []
        for pipeline in pipelines:
            # Get topic names from cache or fallback to DB
            cached_info = self._pipeline_cache.get(pipeline.id, {})
            source_topics = cached_info.get("source_topics") or pipeline.source_topics or []
            destination_topic = cached_info.get("destination_topic") or pipeline.destination_topic or ""
            repository_ids = cached_info.get("repository_ids") or [str(r.id) for r in pipeline.repositories] or []

            pipelines_data.append(
                {
                    "id": pipeline.id,
                    "name": pipeline.name,
                    "deployment_name": pipeline.deployment_name,
                    "enabled": pipeline.enabled,
                    "source_topics": source_topics,
                    "destination_topic": destination_topic,
                    "save_untagged": pipeline.save_untagged or False,
                    "repository_ids": repository_ids,
                }
            )

        return pipelines_data

    async def _build_pipeline_stats(self, pipelines_data: list[dict]) -> list[PipelineStatsItem]:
        """Build pipeline stats from pre-fetched data.

        This method makes HTTP calls to Flink/K8s for status.
        It runs OUTSIDE the DB session to avoid holding connections.

        Args:
            pipelines_data: List of pipeline data dicts from _get_pipelines_for_stats

        Returns:
            List of pipeline stats for the modal table
        """
        stats = []
        for pipeline_data in pipelines_data:
            pipeline_id = pipeline_data["id"]

            # Get cache entry from Flink poller (includes EPS from custom gauges)
            cache_entry = flink_metrics_poller.get_cache_entry(pipeline_id)
            metrics = cache_entry.metrics if cache_entry else None

            # Get EPS from cache entry (custom Flink gauges)
            input_eps = cache_entry.input_eps if cache_entry else 0.0
            # output_eps = tagged + untagged (total output events per second)
            output_eps = (cache_entry.tagged_eps + cache_entry.untagged_eps) if cache_entry else 0.0

            # Get topic lag from cached metrics (pending_records)
            topic_lag = metrics.pending_records if metrics and metrics.pending_records else 0

            # Get real-time Flink deployment status (Flink REST -> K8s fallback)
            # This is the HTTP call that was blocking the DB session before
            flink_status, status_details = await pipeline_status_manager.get_status_details(
                pipeline_id=str(pipeline_id),
                deployment_name=pipeline_data["deployment_name"],
                enabled=pipeline_data["enabled"],
            )

            stats.append(
                PipelineStatsItem(
                    id=str(pipeline_id),
                    name=pipeline_data["name"],
                    source_topics=pipeline_data["source_topics"],
                    destination_topic=pipeline_data["destination_topic"],
                    save_untagged=pipeline_data["save_untagged"],
                    repository_ids=pipeline_data["repository_ids"],
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
            if ErrorTracker.should_log("dashboard_cache_refresh"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="dashboard",
                    details=f"Failed to refresh pipeline cache: {str(e)}",
                    source="system",
                    severity="error",
                )

    def invalidate_pipeline_cache(self, pipeline_id: UUID | None = None) -> None:
        """Invalidate pipeline cache.

        Args:
            pipeline_id: Specific pipeline to invalidate, or None for all
        """
        if pipeline_id:
            self._pipeline_cache.pop(pipeline_id, None)
            flink_metrics_poller.clear_cache(pipeline_id)
        else:
            self._pipeline_cache.clear()
            flink_metrics_poller.clear_cache()


# Singleton instance
dashboard_service = DashboardService()
