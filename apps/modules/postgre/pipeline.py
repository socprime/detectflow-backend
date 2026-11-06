"""Pipeline repository for database operations.

This module provides the repository layer for Pipeline model with support
for eager loading of relationships, filtering, pagination, and statistics.
"""

from typing import Any
from uuid import UUID

from sqlalchemy import delete, func, insert, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.core.enums import PipelineSortField
from apps.core.models import LogSource, Pipeline, PipelineRule, Rule, pipeline_repositories
from apps.modules.postgre.base import BaseDAO


class PipelineDAO(BaseDAO[Pipeline]):
    """Repository for Pipeline model with relationship handling.

    Provides database operations for pipelines including CRUD operations,
    list retrieval with pagination and filtering, and statistics aggregation.
    All list operations use eager loading to avoid N+1 query problems.

    Args:
        session: Async SQLAlchemy session for database operations.
    """

    # Sort field configuration: maps enum value to (model, attribute) or special handler
    # Direct Pipeline fields use (Pipeline, "field_name")
    # Related/computed fields use special handling in _apply_sorting()
    _SORT_CONFIG: dict[str, tuple[type, str] | str] = {
        PipelineSortField.NAME.value: (Pipeline, "name"),
        PipelineSortField.DESTINATION_TOPIC.value: (Pipeline, "destination_topic"),
        PipelineSortField.EVENTS_TAGGED.value: (Pipeline, "events_tagged"),
        PipelineSortField.EVENTS_UNTAGGED.value: (Pipeline, "events_untagged"),
        PipelineSortField.CREATED.value: (Pipeline, "created"),
        PipelineSortField.ENABLED.value: (Pipeline, "enabled"),
        PipelineSortField.STATUS.value: (Pipeline, "status"),
        # Special handlers for computed/related fields
        PipelineSortField.SOURCE_TOPICS.value: "source_topics",  # Sort by first element
        PipelineSortField.LOG_SOURCE.value: "log_source",  # Needs join
        PipelineSortField.FILTERS.value: "filters",  # Count of array
        PipelineSortField.RULES.value: "rules",  # Count of relationship
    }

    # Legacy whitelist for backward compatibility
    ALLOWED_SORT_FIELDS = {e.value for e in PipelineSortField}

    def __init__(self, session: AsyncSession):
        """Initialize pipeline repository.

        Args:
            session: Async SQLAlchemy session.
        """
        super().__init__(Pipeline, session)

    async def get_with_relations(self, id: UUID) -> Pipeline | None:
        """Get pipeline by ID with all relationships eagerly loaded.

        Loads a single pipeline with its related entities including log source,
        pipeline rules, repositories, and correlation-pipeline membership to avoid
        lazy loading issues.

        Note: loads PipelineRule but does NOT load Rule or its current_content —
        rule.body is unavailable.

        Args:
            id: UUID of the pipeline.

        Returns:
            Pipeline: Pipeline object with relationships loaded, or None if not found.

        Example:
            >>> repo = PipelineRepository(session)
            >>> pipeline = await repo.get_with_relations(uuid)
            >>> print(pipeline.source_topics)  # Already loaded list of topics
        """
        result = await self.session.execute(
            select(Pipeline)
            .options(
                selectinload(Pipeline.log_source),
                selectinload(Pipeline.pipeline_rules),
                selectinload(Pipeline.repositories),
                selectinload(Pipeline.correlation_pipelines),
            )
            .where(Pipeline.id == id)
        )
        return result.scalar_one_or_none()

    async def get_by_source_topic(self, topic_name: str) -> list[Pipeline]:
        """Get pipelines that use a specific source topic.

        Uses PostgreSQL array containment operator to check if topic_name
        is in the source_topics array.

        Args:
            topic_name: Name of the Kafka topic to search for.

        Returns:
            List of pipelines that include this topic in source_topics.
        """
        result = await self.session.execute(select(Pipeline).where(Pipeline.source_topics.any(topic_name)))
        return list(result.scalars().all())

    async def get_list_with_relations(
        self,
        skip: int = 0,
        limit: int = 100,
        search: str | None = None,
        sort: str | None = None,
        order: str = "asc",
    ) -> tuple[list[Pipeline], int]:
        """Get paginated list of pipelines with relationships.

        Retrieves a list of pipelines with pagination, filtering, and sorting support.
        Small relationships (log_source, repositories, correlation_pipelines) are eagerly loaded.

        Note: does NOT load ``pipeline_rules`` — pipelines can reference thousands
        of rules, and the list view only needs the per-pipeline totals. Use
        ``get_rule_counts`` for those (one GROUP BY instead of hydrating ~MBs of
        rule rows into ORM objects per page).

        Args:
            skip: Number of records to skip (for pagination).
            limit: Maximum number of records to return.
            search: Search query to filter by pipeline name (case-insensitive).
            sort: Field name to sort by (must be a Pipeline model attribute).
            order: Sort order, either 'asc' or 'desc'.

        Returns:
            tuple[list[Pipeline], int]: Tuple of (list of pipelines, total count).

        Example:
            >>> repo = PipelineRepository(session)
            >>> pipelines, total = await repo.get_list_with_relations(
            ...     skip=0, limit=10, search="prod", sort="created", order="desc"
            ... )
            >>> print(f"Found {total} pipelines, showing {len(pipelines)}")
        """
        query = select(Pipeline).options(
            selectinload(Pipeline.log_source),
            selectinload(Pipeline.repositories),
            selectinload(Pipeline.correlation_pipelines),
        )

        # Apply search filter
        if search:
            query = query.where(or_(Pipeline.name.ilike(f"%{search}%")))

        # Count total matching records
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # Apply sorting
        if sort and sort in self._SORT_CONFIG:
            query = self._apply_sorting(query, sort, order)

        # Apply pagination
        query = query.offset(skip).limit(limit)

        # Execute query
        result = await self.session.execute(query)
        items = result.scalars().all()

        return list(items), total

    async def get_rule_counts(self, pipeline_ids: list[UUID]) -> dict[UUID, tuple[int, int]]:
        """Per-pipeline rule totals for the list view, in ONE aggregate query.

        Replaces the old ``selectinload(pipeline_rules → rule)`` pattern that
        hydrated every joined rule row just to compute two integers per
        pipeline — on rule-heavy pipelines that meant megabytes of rows and
        thousands of ORM objects per page.

        Args:
            pipeline_ids: IDs of the pipelines on the current page.

        Returns:
            Mapping ``pipeline_id -> (total_rules, supported_rules)``. Pipelines
            with no rules are absent — callers should default to ``(0, 0)``.
        """
        if not pipeline_ids:
            return {}
        result = await self.session.execute(
            select(
                PipelineRule.pipeline_id,
                func.count(PipelineRule.id),
                func.count(PipelineRule.id).filter(Rule.is_supported.is_(True)),
            )
            # OUTER join: pipeline_rules.rule_id is nullable — link rows without
            # a rule must still count toward the total (matches the previous
            # len(pipeline.pipeline_rules) semantics).
            .outerjoin(Rule, Rule.id == PipelineRule.rule_id)
            .where(PipelineRule.pipeline_id.in_(pipeline_ids))
            .group_by(PipelineRule.pipeline_id)
        )
        return {row[0]: (row[1], row[2]) for row in result.all()}

    def _apply_sorting(self, query, sort: str, order: str):
        """Apply sorting to query based on sort field.

        Handles both direct Pipeline fields and computed/related fields.
        """
        config = self._SORT_CONFIG.get(sort)
        is_desc = order.lower() == "desc"

        # Direct Pipeline field
        if isinstance(config, tuple):
            model, attr = config
            sort_column = getattr(model, attr)
            return query.order_by(sort_column.desc() if is_desc else sort_column.asc())

        # Special handlers for computed/related fields
        if config == "source_topics":
            # Sort by first element of array (NULL-safe)
            sort_column = Pipeline.source_topics[1]  # PostgreSQL arrays are 1-indexed
            return query.order_by(sort_column.desc() if is_desc else sort_column.asc())

        if config == "log_source":
            # Sort by log source name using scalar subquery (avoids join duplicates)
            log_source_name = (
                select(LogSource.name)
                .where(LogSource.id == Pipeline.log_source_id)
                .correlate(Pipeline)
                .scalar_subquery()
            )
            sort_column = func.coalesce(log_source_name, "")
            return query.order_by(sort_column.desc() if is_desc else sort_column.asc())

        if config == "filters":
            # Sort by count of filters array
            sort_column = func.coalesce(func.cardinality(Pipeline.filters), 0)
            return query.order_by(sort_column.desc() if is_desc else sort_column.asc())

        if config == "rules":
            # Sort by count of pipeline_rules relationship
            rules_count = (
                select(func.count(PipelineRule.id))
                .where(PipelineRule.pipeline_id == Pipeline.id)
                .correlate(Pipeline)
                .scalar_subquery()
            )
            return query.order_by(rules_count.desc() if is_desc else rules_count.asc())

        return query

    async def get_statistics(self) -> dict[str, Any]:
        """Get aggregated statistics across all pipelines.

        Computes various statistics including unique topic counts and placeholder
        values for networks, rules, and events.

        Returns:
            dict[str, Any]: Dictionary with statistics structure:
                - topics: Dict with 'source' and 'destination' counts
                - networks: Dict with 'nodes' and 'clusters' counts (placeholder)
                - rules: Dict with 'active' and 'matched' counts (placeholder)
                - events: Dict with 'tagged' and 'untagged' counts (placeholder)

        Note:
            The networks, rules, and events statistics currently return placeholder
            values (0). These should be implemented when the corresponding functionality
            is added to track these metrics in the database.

        Example:
            >>> repo = PipelineRepository(session)
            >>> stats = await repo.get_statistics()
            >>> print(f"Total source topics: {stats['topics']['source']}")
        """
        # Get unique source topics count (using UNNEST for array column)
        source_count_query = await self.session.execute(
            select(func.count(func.distinct(func.unnest(Pipeline.source_topics))))
        )
        source_count = source_count_query.scalar() or 0

        # Get unique destination topics count (every destinations[].topic across pipelines)
        dest_count = len(await self._distinct_destination_topics())

        return {
            "topics": {
                "source": source_count,
                "destination": dest_count,
            },
            "networks": {
                "nodes": 0,  # TODO: Implement when network tracking is added
                "clusters": 0,  # TODO: Implement when network tracking is added
            },
            "rules": {
                "active": 0,  # TODO: Implement when rule tracking is added
                "matched": 0,  # TODO: Implement when rule tracking is added
            },
            "events": {
                "tagged": 0,  # TODO: Implement when event tracking is added
                "untagged": 0,  # TODO: Implement when event tracking is added
            },
        }

    async def count_enabled(self) -> int:
        """
        Count number of currently enabled pipelines.

        Used for cluster capacity checks before enabling new pipelines.

        Returns:
            int: Number of pipelines with enabled=True

        Example:
            >>> repo = PipelineRepository(session)
            >>> count = await repo.count_enabled()
            >>> print(f"Active pipelines: {count}")
        """
        # NOTE: `Pipeline.enabled is True` (Python identity check on a Column)
        # always evaluates to False and produced `WHERE false` — the count was
        # permanently 0. `.is_(True)` renders the intended SQL `enabled IS TRUE`.
        result = await self.session.execute(
            select(func.count()).select_from(Pipeline).where(Pipeline.enabled.is_(True))
        )
        return result.scalar() or 0

    async def _distinct_destination_topics(self) -> set[str]:
        """Every destination topic used by pipelines (the full output fan-out).

        Reads ``destinations[].topic``, falling back to the legacy singular column for rows
        whose JSONB list is still empty.
        """
        result = await self.session.execute(select(Pipeline.destinations, Pipeline.destination_topic))

        topics: set[str] = set()
        for destinations, destination_topic in result.fetchall():
            row_topics = {item["topic"] for item in destinations or [] if item.get("topic")}
            topics |= row_topics or ({destination_topic} if destination_topic else set())
        return topics

    async def get_by_ids(self, pipeline_ids: list[UUID]) -> list[Pipeline]:
        """Fetch many pipelines by id in ONE query.

        Used to validate an incoming correlation-pipeline member set: the caller diffs the
        returned ids against what was asked for, so an unknown id is a ``NotFoundError`` rather
        than a silently dropped membership row.

        Args:
            pipeline_ids: Pipeline ids to fetch; duplicates are harmless.

        Returns:
            The rows that exist, in arbitrary order (missing ids are simply absent).
        """
        if not pipeline_ids:
            return []
        result = await self.session.execute(select(Pipeline).where(Pipeline.id.in_(pipeline_ids)))
        return list(result.scalars().all())

    async def get_tagged_events_topics(self) -> dict[UUID, str]:
        """Pipelines that own a per-pipeline tagged-events topic, id → stored topic name.

        A ``NULL`` ``tagged_events_topic`` means "still on the legacy shared topic" and is excluded:
        those rows own no topic, so the startup topic reconcile must not provision one for them.

        Returns:
            Mapping of ``pipelines.id`` to its ``tagged_events_topic``.
        """
        result = await self.session.execute(
            select(Pipeline.id, Pipeline.tagged_events_topic).where(Pipeline.tagged_events_topic.is_not(None))
        )
        return {row[0]: row[1] for row in result.fetchall()}

    async def get_by_repository_id(self, repository_id: UUID) -> list[Pipeline]:
        """Get pipelines connected to a specific repository."""
        result = await self.session.execute(
            select(Pipeline)
            .join(pipeline_repositories, Pipeline.id == pipeline_repositories.c.pipeline_id)
            .where(pipeline_repositories.c.repository_id == repository_id)
        )
        return list(result.scalars().all())

    async def get_by_repository_ids(self, repository_ids: list[UUID]) -> dict[UUID, list[Pipeline]]:
        """Get pipelines connected to multiple repositories in a single query.

        Args:
            repository_ids: List of repository UUIDs to fetch pipelines for.

        Returns:
            Dictionary mapping repository_id to list of connected pipelines.
        """
        if not repository_ids:
            return {}

        result = await self.session.execute(
            select(
                pipeline_repositories.c.repository_id,
                Pipeline,
            )
            .join(Pipeline, Pipeline.id == pipeline_repositories.c.pipeline_id)
            .where(pipeline_repositories.c.repository_id.in_(repository_ids))
        )

        # Group pipelines by repository_id
        repo_pipelines_map: dict[UUID, list[Pipeline]] = {}
        for row in result.all():
            repo_id = row.repository_id
            pipeline = row.Pipeline
            if repo_id not in repo_pipelines_map:
                repo_pipelines_map[repo_id] = []
            repo_pipelines_map[repo_id].append(pipeline)

        return repo_pipelines_map

    async def get_repository_selections(self, pipeline_id: UUID) -> dict[UUID, list[str]]:
        """Get selected repository folders for a pipeline."""
        result = await self.session.execute(
            select(
                pipeline_repositories.c.repository_id,
                pipeline_repositories.c.folder_paths,
            ).where(pipeline_repositories.c.pipeline_id == pipeline_id)
        )
        return {row.repository_id: list(row.folder_paths or []) for row in result.all()}

    async def get_repository_selections_by_repository_id(self, repository_id: UUID) -> dict[UUID, list[str]]:
        """Get pipeline folder selections for one repository."""
        result = await self.session.execute(
            select(
                pipeline_repositories.c.pipeline_id,
                pipeline_repositories.c.folder_paths,
            ).where(pipeline_repositories.c.repository_id == repository_id)
        )
        return {row.pipeline_id: list(row.folder_paths or []) for row in result.all()}

    async def set_repository_selections(self, pipeline_id: UUID, selections: dict[UUID, list[str]]) -> None:
        """Replace repository folder selections for a pipeline."""
        await self.remove_all_repositories(pipeline_id)
        await self.add_repositories(pipeline_id, selections)

    async def add_repositories(self, pipeline_id: UUID, selections: dict[UUID, list[str]] | list[UUID]) -> None:
        """Add repositories to a pipeline with optional folder selections."""
        if not selections:
            return

        if isinstance(selections, dict):
            values = [
                {"pipeline_id": pipeline_id, "repository_id": repo_id, "folder_paths": folders}
                for repo_id, folders in selections.items()
            ]
        else:
            values = [
                {"pipeline_id": pipeline_id, "repository_id": repo_id, "folder_paths": []} for repo_id in selections
            ]
        await self.session.execute(insert(pipeline_repositories).values(values))
        await self.session.flush()

    async def remove_repositories(self, pipeline_id: UUID, repository_ids: list[UUID]) -> None:
        """Remove repositories from a pipeline."""
        if not repository_ids:
            return

        await self.session.execute(
            delete(pipeline_repositories).where(
                pipeline_repositories.c.pipeline_id == pipeline_id,
                pipeline_repositories.c.repository_id.in_(repository_ids),
            )
        )
        await self.session.flush()

    async def remove_all_repositories(self, pipeline_id: UUID) -> None:
        """Remove all repositories from a pipeline."""
        await self.session.execute(
            delete(pipeline_repositories).where(pipeline_repositories.c.pipeline_id == pipeline_id)
        )
        await self.session.flush()

    async def get_by_log_source_id(self, log_source_id: UUID) -> list[Pipeline]:
        """Get pipelines that use a specific log source.

        Args:
            log_source_id: UUID of the log source to search for.

        Returns:
            List of pipelines that reference this log source.
        """
        result = await self.session.execute(select(Pipeline).where(Pipeline.log_source_id == log_source_id))
        return list(result.scalars().all())

    async def get_by_filter_id(self, filter_id: UUID) -> list[Pipeline]:
        """Get pipelines that use a specific filter.

        Filters are stored as ARRAY(UUID) in Pipeline.filters column.
        Uses PostgreSQL ANY operator to check if filter_id is in the array.

        Args:
            filter_id: UUID of the filter to search for.

        Returns:
            List of pipelines that contain this filter.
        """
        result = await self.session.execute(select(Pipeline).where(Pipeline.filters.any(filter_id)))
        return list(result.scalars().all())
