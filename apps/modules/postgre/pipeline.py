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
from apps.core.models import LogSource, Pipeline, PipelineRule, pipeline_repositories
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
        pipeline rules, and repositories to avoid lazy loading issues.

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
        All relationships are eagerly loaded to prevent N+1 queries.

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
            selectinload(Pipeline.pipeline_rules),
            selectinload(Pipeline.repositories),
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
            # Sort by log source name (needs outerjoin)
            query = query.outerjoin(LogSource, Pipeline.log_source_id == LogSource.id)
            sort_column = func.coalesce(LogSource.name, "")
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

        # Get unique destination topics count
        dest_count_query = await self.session.execute(select(func.count(func.distinct(Pipeline.destination_topic))))
        dest_count = dest_count_query.scalar() or 0

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
        result = await self.session.execute(select(func.count()).select_from(Pipeline).where(Pipeline.enabled is True))
        return result.scalar() or 0

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

    async def add_repositories(self, pipeline_id: UUID, repository_ids: list[UUID]) -> None:
        """Add repositories to a pipeline."""
        if not repository_ids:
            return

        values = [{"pipeline_id": pipeline_id, "repository_id": repo_id} for repo_id in repository_ids]
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
