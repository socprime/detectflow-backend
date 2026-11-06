"""Dashboard data aggregation service.

This module provides the main service for aggregating dashboard data
from multiple sources (PostgreSQL, Flink REST API).

Metrics are fetched from Flink REST API via FlinkMetricsPoller, replacing
the previous Kafka consumer approach.

The DB-derived part of the payload ("topology": topics, repositories + rule counts,
pipeline rows) only changes on CRUD/sync, so it is cached on the service singleton with a
TTL (``dashboard_topology_ttl_seconds``) — on warm SSE ticks the topology and metrics
phases perform no DB access (recent activity is buffer-served with a one-off post-restart
DB fallback; see ``ActivityService.get_recent``).
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from apps.clients.flink_rest import FlinkJobMetrics
from apps.core.database import AsyncSessionLocal
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import Pipeline, Repository, pipeline_repositories
from apps.core.schemas import (
    DashboardData,
    DashboardGraphData,
    DashboardSnapshotResponse,
    DestinationTopicStats,
    PipelineOutputDestination,
    PipelineStatsItem,
    RepositoryStats,
    SourceTopicStats,
    destination_accepts,
    legacy_fields_from_destinations,
    pipeline_destinations,
)
from apps.core.settings import settings
from apps.managers.activity import activity_service
from apps.managers.pipeline_status import pipeline_status_manager
from apps.managers.sse_broadcaster import SSEBroadcaster
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.rule import RuleDAO
from apps.services.flink_metrics_poller import flink_metrics_poller

logger = get_logger(__name__)


@dataclass
class DashboardTopology:
    """Point-in-time snapshot of the dashboard's DB-derived data.

    Everything here only changes on pipeline/repository/rule CRUD or repo sync, so it is
    loaded in one short DB session and reused across SSE ticks until the TTL expires or
    ``DashboardService.invalidate_pipeline_cache`` drops it. The per-tick-fresh values (EPS,
    lag, status) intentionally live OUTSIDE this snapshot — they come from
    ``flink_metrics_poller`` / ``pipeline_status_manager`` in-memory caches, not the DB.
    """

    source_topics: list[str]
    dest_topics: list[str]
    repositories: list[RepositoryStats]  # includes per-repo rules_count
    total_rules: int
    pipelines_count: int
    # Enabled pipelines for EPS aggregation: id, source_topics, destinations.
    enabled_pipelines: list[dict[str, Any]]
    # Pre-extracted rows for _build_pipeline_stats (id, name, deployment_name, ...).
    pipelines_data: list[dict[str, Any]]
    fetched_at: float  # time.monotonic()


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
        # Key: pipeline_id, Value: dict with name, source_topics (list), enabled
        self._pipeline_cache: dict[UUID, dict[str, Any]] = {}
        self._is_running: bool = False
        # Cached DB topology snapshot (see DashboardTopology). Recomputed only on TTL expiry
        # or invalidation; the lock stops concurrent callers (refresher tick + REST snapshot)
        # from both recomputing. The generation counter makes invalidation win over an
        # in-flight reload: a snapshot loaded before the invalidation is never cached.
        self._topology: DashboardTopology | None = None
        self._topology_lock = asyncio.Lock()
        self._topology_generation = 0
        # Single-producer broadcaster: computes the SSE payload once per tick and fans it out
        # to all connected clients (O(1) instead of O(clients) DB/Flink/K8s load). The
        # refresher only runs while at least one SSE client is subscribed.
        self._broadcaster: SSEBroadcaster[DashboardData] = SSEBroadcaster(
            name="dashboard",
            compute=self.get_dashboard_data,
            interval=settings.dashboard_broadcast_interval_seconds,
            on_error=self._on_broadcast_error,
        )

    @property
    def broadcaster(self) -> SSEBroadcaster[DashboardData]:
        """The shared SSE payload broadcaster (subscribe/unsubscribe/get_cached)."""
        return self._broadcaster

    async def _on_broadcast_error(self, exc: Exception) -> None:
        """Throttled activity logging for a failed dashboard refresh tick (once per tick)."""
        if ErrorTracker.should_log("sse_event"):
            await activity_producer.log_action(
                action="error",
                entity_type="dashboard",
                details=f"Error generating SSE event: {str(exc)}",
                source="system",
                severity="error",
            )

    async def start(self) -> None:
        """Start the dashboard service."""
        if self._is_running:
            return

        self._is_running = True

        # Preload pipeline cache
        await self._refresh_pipeline_cache()

        logger.info("DashboardService started")

    async def stop(self) -> None:
        """Stop the dashboard service and cancel the SSE broadcaster refresher."""
        self._is_running = False
        await self._broadcaster.stop()
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

        IMPORTANT: no DB session is held during the Flink/K8s HTTP status phase, and on a
        warm topology cache (see :class:`DashboardTopology`) the topology and metrics phases
        perform no DB access — the payload is assembled from the cached topology plus fresh
        in-memory poller metrics. This keeps the 2s SSE refresher from hammering PostgreSQL
        with static topology re-reads. The recent-activity phase is served from the activity
        consumer's in-memory buffer and may run one DB fallback query right after a restart
        (``ActivityService.get_recent`` caches that fallback).

        Returns:
            DashboardData with graph, pipeline stats, and recent activity
        """
        # Phase 1: DB topology (served from cache within the TTL; one short session on miss)
        topology = await self._get_topology()

        # Phase 2: per-tick assembly — fresh EPS from the poller's in-memory cache, no DB
        graph_data = self._assemble_graph_data(topology)

        # Phase 3: Fetch Flink/K8s status OUTSIDE any DB session (HTTP calls + 15s cache)
        pipeline_stats = await self._build_pipeline_stats(topology.pipelines_data)

        # Phase 4: recent activity (in-memory buffer fed by the activity consumer; DB only
        # as a post-restart fallback)
        recent_activity = await activity_service.get_recent()

        return DashboardData(
            graph=graph_data,
            pipelines_stats=pipeline_stats,
            recent_activity=recent_activity,
        )

    async def _get_topology(self) -> DashboardTopology:
        """Return the cached topology snapshot, reloading it on TTL expiry or invalidation.

        The lock guards the recompute so concurrent callers (SSE refresher tick + REST
        ``get_snapshot`` requests) trigger exactly one reload; waiters re-check under the
        lock and reuse the winner's snapshot.

        The generation check keeps an ``invalidate_pipeline_cache()`` that arrives while
        ``_load_topology()`` is in flight from being swallowed: the just-loaded (pre-CRUD)
        snapshot is returned to the current caller but NOT cached, so the next caller
        reloads post-CRUD state instead of serving stale topology for a full TTL.
        """
        topology = self._topology
        if topology is not None and (time.monotonic() - topology.fetched_at) < settings.dashboard_topology_ttl_seconds:
            return topology

        async with self._topology_lock:
            topology = self._topology  # re-check: another caller may have just reloaded
            if (
                topology is not None
                and (time.monotonic() - topology.fetched_at) < settings.dashboard_topology_ttl_seconds
            ):
                return topology

            generation = self._topology_generation
            topology = await self._load_topology()
            if generation == self._topology_generation:
                self._topology = topology
            return topology

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

    async def _load_topology(self) -> DashboardTopology:
        """Load the full DB topology snapshot in one short-lived session.

        These are the only dashboard DB queries; everything per-tick-fresh (EPS, lag,
        status) is read from in-memory caches at assembly time. Rule counts use one
        GROUP BY aggregate (``RuleDAO.get_rule_counts_by_repositories``) instead of a
        per-repository count loop.
        """
        async with AsyncSessionLocal() as db:
            # Unique source topics from enabled pipelines only (using UNNEST for array)
            source_topics_query = await db.execute(
                select(func.unnest(Pipeline.source_topics).label("topic")).where(Pipeline.enabled.is_(True)).distinct()
            )
            source_topics = [row[0] for row in source_topics_query.fetchall() if row[0]]

            # Repositories that are connected to enabled pipelines only
            repos_query = await db.execute(
                select(Repository)
                .join(pipeline_repositories, Repository.id == pipeline_repositories.c.repository_id)
                .join(Pipeline, Pipeline.id == pipeline_repositories.c.pipeline_id)
                .where(Pipeline.enabled.is_(True))
                .distinct()
            )
            repositories = repos_query.scalars().all()

            # Rule counts per repository — one aggregate query, not one query per repo
            repo_rules_counts = await RuleDAO(db).get_rule_counts_by_repositories([r.id for r in repositories])

            # Active pipelines count
            pipelines_query = await db.execute(select(func.count(Pipeline.id)).where(Pipeline.enabled.is_(True)))
            pipelines_count = pipelines_query.scalar() or 0

            # Enabled pipelines (repositories eagerly loaded) — one query serves both the
            # EPS-aggregation list and the pipeline-stats rows
            enabled_pipelines_query = await db.execute(
                select(Pipeline).where(Pipeline.enabled.is_(True)).options(selectinload(Pipeline.repositories))
            )
            enabled_pipelines = enabled_pipelines_query.scalars().all()

            dest_topics_set: set[str] = set()
            eps_pipelines: list[dict[str, Any]] = []
            pipelines_data: list[dict[str, Any]] = []
            for pipeline in enabled_pipelines:
                # Destinations come straight from the row: the pipeline cache predates
                # multi-destination and carries no output_mode, so it cannot serve them.
                destinations = pipeline_destinations(pipeline)
                dest_topics_set.update(d["topic"] for d in destinations)
                primary_topic, save_untagged, _ = legacy_fields_from_destinations(destinations)

                cached_info = self._pipeline_cache.get(pipeline.id, {})
                cached_source_topics = cached_info.get("source_topics") or pipeline.source_topics or []
                repository_ids = cached_info.get("repository_ids") or [str(r.id) for r in pipeline.repositories] or []

                eps_pipelines.append(
                    {
                        "id": pipeline.id,
                        "source_topics": pipeline.source_topics or [],
                        "destinations": destinations,
                    }
                )
                pipelines_data.append(
                    {
                        "id": pipeline.id,
                        "name": pipeline.name,
                        "deployment_name": pipeline.deployment_name,
                        "enabled": pipeline.enabled,
                        "source_topics": cached_source_topics,
                        "destination_topic": primary_topic,
                        "destinations": destinations,
                        "save_untagged": save_untagged,
                        "repository_ids": repository_ids,
                    }
                )
            dest_topics = sorted(dest_topics_set)

        repo_stats = [
            RepositoryStats(
                id=str(r.id),
                name=r.name,
                rules_count=repo_rules_counts.get(r.id, 0),
            )
            for r in repositories
        ]

        logger.debug(
            "Dashboard topology snapshot refreshed",
            extra={"pipelines": len(pipelines_data), "repositories": len(repo_stats)},
        )

        return DashboardTopology(
            source_topics=source_topics,
            dest_topics=dest_topics,
            repositories=repo_stats,
            total_rules=sum(repo_rules_counts.values()),
            pipelines_count=pipelines_count,
            enabled_pipelines=eps_pipelines,
            pipelines_data=pipelines_data,
            fetched_at=time.monotonic(),
        )

    def _assemble_graph_data(self, topology: DashboardTopology) -> DashboardGraphData:
        """Combine the cached topology with FRESH poller metrics — no DB access.

        EPS values are read from ``flink_metrics_poller``'s in-memory cache on every call,
        so they stay tick-fresh even when the topology snapshot is minutes old.

        Destination EPS attribution: the Flink gauges are per-pipeline, not per-sink, so each
        destination is credited the pipeline's ``tagged_eps`` / ``untagged_eps`` according to
        what its ``output_mode`` accepts (``destination_accepts``) — ``matched_only`` gets no
        untagged, ``unmatched_only`` gets no tagged, ``all_events`` gets both. That is what
        those topics actually receive. The ``total_*`` values stay per-pipeline (events
        produced, not sink writes), so with multiple destinations the per-node values sum to
        more than the totals by design.
        """
        source_topic_eps = defaultdict(float)
        dest_topic_tagged_eps = defaultdict(float)
        dest_topic_untagged_eps = defaultdict(float)
        total_events_eps = 0.0
        total_tagged_eps = 0.0
        total_untagged_eps = 0.0

        for pipeline in topology.enabled_pipelines:
            cache_entry = flink_metrics_poller.get_cache_entry(pipeline["id"])
            if not cache_entry:
                continue

            pipeline_info = self._pipeline_cache.get(pipeline["id"], {})
            source_topics_list = pipeline_info.get("source_topics") or pipeline["source_topics"] or []
            destinations = pipeline["destinations"]

            # Use input_eps from cache (custom Flink gauge)
            input_eps = cache_entry.input_eps
            if source_topics_list and input_eps > 0:
                eps_per_topic = input_eps / len(source_topics_list)
                for source_topic_name in source_topics_list:
                    source_topic_eps[source_topic_name] += round(eps_per_topic, 2)
                total_events_eps += input_eps

            if destinations:
                # tagged_eps/untagged_eps from cache (custom Flink gauges), fanned out per
                # destination according to what its output mode accepts
                tagged_eps = cache_entry.tagged_eps
                untagged_eps = cache_entry.untagged_eps

                for destination in destinations:
                    output_mode = destination.get("output_mode", "matched_only")
                    if destination_accepts(output_mode, matched=True):
                        dest_topic_tagged_eps[destination["topic"]] += round(tagged_eps, 2)
                    if destination_accepts(output_mode, matched=False):
                        dest_topic_untagged_eps[destination["topic"]] += round(untagged_eps, 2)

                total_tagged_eps += tagged_eps
                total_untagged_eps += untagged_eps

        source_topics_stats = [
            SourceTopicStats(
                id=topic_name,
                name=topic_name,
                eps=round(source_topic_eps.get(topic_name, 0.0), 2),
            )
            for topic_name in topology.source_topics
        ]

        dest_topics_stats = [
            DestinationTopicStats(
                id=topic_name,
                name=topic_name,
                tagged_eps=round(dest_topic_tagged_eps.get(topic_name, 0.0), 2),
                untagged_eps=round(dest_topic_untagged_eps.get(topic_name, 0.0), 2),
            )
            for topic_name in topology.dest_topics
        ]

        return DashboardGraphData(
            source_topics=source_topics_stats,
            repositories=topology.repositories,
            destination_topics=dest_topics_stats,
            pipelines_count=topology.pipelines_count,
            total_events_eps=round(total_events_eps, 2),
            total_tagged_eps=round(total_tagged_eps, 2),
            total_untagged_eps=round(total_untagged_eps, 2),
            total_rules=topology.total_rules,
        )

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
                    destinations=[
                        PipelineOutputDestination.model_validate(d) for d in pipeline_data.get("destinations") or []
                    ],
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
        """Invalidate pipeline cache and the topology snapshot.

        Called on pipeline CRUD (create/update/delete, after commit). Dropping the topology
        snapshot makes the next SSE tick / snapshot request reload it from the DB, so user
        actions reflect immediately instead of waiting out ``dashboard_topology_ttl_seconds``.
        Bumping the generation ensures a ``_load_topology()`` already in flight cannot cache
        its (pre-CRUD) result over this invalidation (see ``_get_topology``).

        Args:
            pipeline_id: Specific pipeline to invalidate, or None for all
        """
        if pipeline_id:
            self._pipeline_cache.pop(pipeline_id, None)
            flink_metrics_poller.clear_cache(pipeline_id)
        else:
            self._pipeline_cache.clear()
            flink_metrics_poller.clear_cache()
        self._topology = None
        self._topology_generation += 1


# Singleton instance
dashboard_service = DashboardService()
