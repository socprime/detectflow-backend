from typing import Literal
from uuid import UUID

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.core.enums import PipelineRuleSortField
from apps.core.models import PipelineRule, PipelineRuleMetric, Repository, Rule
from apps.modules.postgre.base import BaseDAO


class PipelineRulesDAO(BaseDAO[PipelineRule]):
    """Repository for PipelineRule model with relationship handling.

    Provides database operations for pipeline rules including CRUD operations
    and list retrieval with pagination and filtering.

    Args:
        session: Async SQLAlchemy session for database operations.
    """

    # Sort field configuration: maps field name to (model, attribute_name)
    # Uses enum values as single source of truth
    _SORT_CONFIG: dict[str, tuple[type, str]] = {
        PipelineRuleSortField.ENABLED.value: (PipelineRule, "enabled"),
        PipelineRuleSortField.TAGGED_EVENTS.value: (PipelineRuleMetric, "total_matches"),
        PipelineRuleSortField.CREATED.value: (PipelineRule, "created"),
        PipelineRuleSortField.UPDATED.value: (PipelineRule, "updated"),
        PipelineRuleSortField.NAME.value: (Rule, "name"),
        PipelineRuleSortField.REPOSITORY.value: (Repository, "name"),
    }

    def __init__(self, session: AsyncSession):
        super().__init__(PipelineRule, session)

    async def get_by_pipeline(
        self,
        pipeline_id: UUID,
        skip: int = 0,
        limit: int = 100,
        search: str | None = None,
        sort: str | None = None,
        order: str = "asc",
        tagged_filter: Literal["all", "tagged", "untagged"] = "all",
        supported_filter: Literal["all", "supported", "unsupported"] = "all",
    ) -> tuple[list[PipelineRule], int]:
        query = (
            select(PipelineRule)
            .options(selectinload(PipelineRule.rule).selectinload(Rule.repository))
            .where(PipelineRule.pipeline_id == pipeline_id)
        )

        # Track joins to avoid duplicates
        has_rule_join = False
        has_metrics_join = False

        if search:
            query = query.join(Rule, PipelineRule.rule_id == Rule.id).where(Rule.name.ilike(f"%{search}%"))
            has_rule_join = True

        # Apply supported/unsupported filter 
        if supported_filter != "all":
            if not has_rule_join:
                query = query.join(Rule, PipelineRule.rule_id == Rule.id)
                has_rule_join = True

            if supported_filter == "supported":
                query = query.where(Rule.is_supported.is_(True))
            elif supported_filter == "unsupported":
                query = query.where(Rule.is_supported.is_(False))

        # Apply tagged/untagged filter using pipeline_rule_metrics
        if tagged_filter != "all":
            if not has_metrics_join:
                query = query.outerjoin(
                    PipelineRuleMetric,
                    and_(
                        PipelineRuleMetric.pipeline_id == PipelineRule.pipeline_id,
                        PipelineRuleMetric.rule_id == PipelineRule.rule_id,
                    ),
                )
                has_metrics_join = True

            if tagged_filter == "tagged":
                # Rules that have at least one match
                query = query.where(PipelineRuleMetric.total_matches > 0)
            elif tagged_filter == "untagged":
                # Rules that have no matches (NULL or 0)
                query = query.where(
                    (PipelineRuleMetric.total_matches.is_(None)) | (PipelineRuleMetric.total_matches == 0)
                )

        # Apply sorting using config mapping
        if sort and sort in self._SORT_CONFIG:
            model, attr = self._SORT_CONFIG[sort]

            # Add required joins for related models
            if model == Rule and not has_rule_join:
                query = query.join(Rule, PipelineRule.rule_id == Rule.id)
                has_rule_join = True
            elif model == Repository:
                if not has_rule_join:
                    query = query.join(Rule, PipelineRule.rule_id == Rule.id)
                query = query.outerjoin(Repository, Rule.repository_id == Repository.id)
            elif model == PipelineRuleMetric and not has_metrics_join:
                query = query.outerjoin(
                    PipelineRuleMetric,
                    and_(
                        PipelineRuleMetric.pipeline_id == PipelineRule.pipeline_id,
                        PipelineRuleMetric.rule_id == PipelineRule.rule_id,
                    ),
                )
                has_metrics_join = True

            # Apply order (use COALESCE for nullable metrics columns)
            sort_column = getattr(model, attr)
            if model == PipelineRuleMetric:
                # NULL values should be treated as 0 for metrics sorting
                sort_column = func.coalesce(sort_column, 0)
            query = query.order_by(sort_column.desc() if order.lower() == "desc" else sort_column.asc())

        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        query = query.offset(skip).limit(limit)
        result = await self.session.execute(query)
        items = result.scalars().all()
        return list(items), total

    async def update_rule_status(self, pipeline_id: UUID, rule_id: UUID, enabled: bool) -> int:
        result = await self.session.execute(
            update(PipelineRule)
            .where(
                and_(
                    PipelineRule.pipeline_id == pipeline_id,
                    PipelineRule.rule_id == rule_id,
                )
            )
            .values(enabled=enabled)
        )
        await self.session.flush()
        return result.rowcount

    async def update_rule_status_by_pipeline_and_repo(
        self, pipeline_id: UUID, repository_ids: list[UUID], enabled: bool
    ) -> None:
        rule_subquery = select(Rule.id).where(Rule.repository_id.in_(repository_ids))
        await self.session.execute(
            update(PipelineRule)
            .where(and_(PipelineRule.pipeline_id == pipeline_id, PipelineRule.rule_id.in_(rule_subquery)))
            .values(enabled=enabled)
        )
        await self.session.flush()

    async def get_pipelines_by_rule_id(self, rule_id: UUID) -> list[UUID]:
        result = await self.session.execute(
            select(PipelineRule.pipeline_id).where(PipelineRule.rule_id == rule_id).distinct()
        )
        return [row[0] for row in result.all()]

    async def get_pipelines_by_repository_id(self, repository_id: UUID) -> list[UUID]:
        result = await self.session.execute(
            select(PipelineRule.pipeline_id)
            .join(Rule, PipelineRule.rule_id == Rule.id)
            .where(Rule.repository_id == repository_id)
            .distinct()
        )
        return [row[0] for row in result.all()]

    async def create_many(self, data: list[dict]) -> list[PipelineRule]:
        instances = [self.model(**item) for item in data]
        self.session.add_all(instances)
        await self.session.flush()
        # Refresh all instances to get their IDs and timestamps
        for instance in instances:
            await self.session.refresh(instance)
            # Встановити updated = created при створенні, якщо обидва поля існують
            if hasattr(instance, "created") and hasattr(instance, "updated"):
                if instance.created and not instance.updated:
                    instance.updated = instance.created
        await self.session.flush()
        return instances

    async def get_by_rule_id(self, rule_id: UUID) -> list[PipelineRule]:
        """Get all pipeline rule entries for a specific rule."""
        result = await self.session.execute(select(PipelineRule).where(PipelineRule.rule_id == rule_id))
        return list(result.scalars().all())

    async def get_enabled_rules_ids(self, pipeline_id: UUID) -> list[UUID]:
        result = await self.session.execute(
            select(PipelineRule.rule_id).where(PipelineRule.pipeline_id == pipeline_id, PipelineRule.enabled)
        )
        return list(result.scalars().all())

    async def delete_by_pipeline_and_repo(self, pipeline_id: UUID, repository_ids: list[UUID]) -> int:
        """Delete pipeline rules for a specific pipeline and list of repositories."""
        if not repository_ids:
            return 0
        rule_subquery = select(Rule.id).where(Rule.repository_id.in_(repository_ids))
        query = delete(PipelineRule).where(
            and_(PipelineRule.pipeline_id == pipeline_id, PipelineRule.rule_id.in_(rule_subquery))
        )
        result = await self.session.execute(query)
        await self.session.flush()
        return result.rowcount

    async def delete_by_pipeline(self, pipeline_id: UUID) -> int:
        """Delete all pipeline rules for a specific pipeline."""
        result = await self.session.execute(delete(PipelineRule).where(PipelineRule.pipeline_id == pipeline_id))
        await self.session.flush()
        return result.rowcount

    async def get_rules_by_pipeline(self, pipeline_id: UUID) -> list[Rule]:
        """Get all rules (enabled or not) for a pipeline."""
        query = (
            select(PipelineRule).options(selectinload(PipelineRule.rule)).where(PipelineRule.pipeline_id == pipeline_id)
        )
        result = await self.session.execute(query)
        pipeline_rules = result.unique().scalars().all()
        return [pr.rule for pr in pipeline_rules if pr.rule is not None]

    async def get_by_pipeline_and_repo(self, pipeline_id: UUID, repository_ids: list[UUID]) -> list[PipelineRule]:
        """Get all pipeline rules for a specific pipeline and list of repositories."""
        rule_subquery = select(Rule.id).where(Rule.repository_id.in_(repository_ids))
        if not repository_ids:
            return []
        query = (
            select(PipelineRule)
            .options(selectinload(PipelineRule.rule))
            .where(PipelineRule.pipeline_id == pipeline_id, PipelineRule.rule_id.in_(rule_subquery))
        )
        result = await self.session.execute(query)
        return result.unique().scalars().all()
