from typing import Any
from uuid import UUID

import yaml
from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only

from apps.core.models import Rule
from apps.modules.postgre.base import BaseDAO


class RuleDAO(BaseDAO[Rule]):
    def __init__(self, session: AsyncSession):
        super().__init__(Rule, session)

    async def get_by_id(self, id: UUID) -> Rule | None:
        """Get rule by ID with repository loaded via left join."""
        query = select(Rule).options(joinedload(Rule.repository)).where(Rule.id == id)
        result = await self.session.execute(query)
        return result.unique().scalar_one_or_none()

    async def get_by_ids(self, ids: list[UUID]) -> list[Rule]:
        """Get rules by list of IDs."""
        if not ids:
            return []
        query = select(Rule).where(Rule.id.in_(ids))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 1000,
        filters: dict[str, Any] | None = None,
        search: str | None = None,
        search_fields: list[str] | None = None,
        sort: str | None = None,
        order: str = "asc",
    ) -> tuple[list[Rule], int]:
        """Get all rules with repository loaded via left join.
        Only loads needed columns (excludes body for performance).
        """
        query = select(Rule).options(
            load_only(
                Rule.id,
                Rule.name,
                Rule.repository_id,
                Rule.created,
                Rule.updated,
                Rule.product,
                Rule.service,
                Rule.category,
                Rule.is_supported,
                Rule.unsupported_reason,
            ),
            joinedload(Rule.repository),
        )

        # Apply filters
        if filters:
            conditions = []
            for key, value in filters.items():
                if hasattr(Rule, key):
                    conditions.append(getattr(Rule, key) == value)
            if conditions:
                query = query.where(and_(*conditions))

        # Apply search
        if search and search_fields:
            search_conditions = []
            for field in search_fields:
                if hasattr(Rule, field):
                    search_conditions.append(getattr(Rule, field).ilike(f"%{search}%"))
            if search_conditions:
                query = query.where(or_(*search_conditions))

        # Get total count (without join for performance)
        # Build count query with same filters but without join
        count_query = select(func.count()).select_from(Rule)
        if query.whereclause is not None:
            count_query = count_query.where(query.whereclause)
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # Apply sorting
        if sort and hasattr(Rule, sort):
            sort_column = getattr(Rule, sort)
            if order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(Rule.id.asc())

        # Apply pagination
        query = query.offset(skip).limit(limit)

        result = await self.session.execute(query)
        items = result.unique().scalars().all()
        return list(items), total

    async def create_many(self, rules: list[dict[str, Any]]) -> list[Rule]:
        for rule in rules:
            self._parse_additional_fields(rule)
        instances = [self.model(**rule_data) for rule_data in rules]
        self.session.add_all(instances)
        await self.session.flush()
        # Refresh all instances to get their IDs and timestamps
        for instance in instances:
            await self.session.refresh(instance)
            # Set updated = created on insert when both fields exist
            if hasattr(instance, "created") and hasattr(instance, "updated"):
                if instance.created and not instance.updated:
                    instance.updated = instance.created
        await self.session.flush()
        return instances

    async def upsert_many(self, rules: list[dict[str, Any]]) -> None:
        """Upsert multiple rules. Updates existing rules if id matches, otherwise inserts new ones."""
        if not rules:
            return

        for rule in rules:
            self._parse_additional_fields(rule)

        rule_ids = [UUID(rule["id"]) for rule in rules if "id" in rule]

        # Query existing IDs from database
        existing_ids = set()
        if rule_ids:
            result = await self.session.execute(select(Rule.id).where(Rule.id.in_(rule_ids)))
            existing_ids = {row[0] for row in result.all()}

        # Split rules into existing (to update) and new (to create)
        rules_to_update = [rule for rule in rules if UUID(rule["id"]) in existing_ids]
        rules_to_create = [rule for rule in rules if UUID(rule["id"]) not in existing_ids]

        # Update existing rules
        for rule_data in rules_to_update:
            rule_id = UUID(rule_data["id"])
            update_data = {k: v for k, v in rule_data.items() if k != "id"}
            await self.session.execute(update(Rule).where(Rule.id == rule_id).values(**update_data))

        # Create new rules
        if rules_to_create:
            await self.create_many(rules_to_create)

        await self.session.flush()

    async def get_rule_count_by_repository_id(self, repository_id: UUID) -> int:
        result = await self.session.execute(
            select(func.count()).select_from(Rule).where(Rule.repository_id == repository_id)
        )
        return result.scalar() or 0

    async def get_all_rule_ids_with_repository_id(self) -> list[tuple[str, str]]:
        """Get all rule ids with repository id. Returns list of tuples (rule_id, repository_id)."""
        result = await self.session.execute(select(Rule.id, Rule.repository_id))
        return [(str(rule_id), str(repository_id)) for rule_id, repository_id in result.all()]

    async def create(self, **kwargs) -> Rule:
        self._parse_additional_fields(kwargs)
        instance = self.model(**kwargs)
        self.session.add(instance)
        await self.session.flush()
        await self.session.refresh(instance)
        # Set updated = created on insert when both fields exist
        if hasattr(instance, "created") and hasattr(instance, "updated"):
            if instance.created and not instance.updated:
                instance.updated = instance.created
                await self.session.flush()
        # Use get_by_id to ensure repository relationship is loaded (consistent with other methods)
        rule = await self.get_by_id(instance.id)
        return rule if rule else instance

    async def delete_by_repository_id(self, repository_ids: list[UUID]) -> int:
        """Delete all rules for a given list of repositories."""
        if not repository_ids:
            return 0
        result = await self.session.execute(delete(Rule).where(Rule.repository_id.in_(repository_ids)))
        await self.session.flush()
        return result.rowcount

    async def update(self, id: UUID, **kwargs) -> Rule | None:
        """Update rule and return it with repository loaded via left join."""
        self._parse_additional_fields(kwargs)

        # Load the instance into session (or get from identity map if already loaded)
        rule = await self.get_by_id(id)
        if not rule:
            return None

        # Update attributes directly - SQLAlchemy tracks changes automatically
        # This updates both the Python object AND will update the database on flush
        for key, value in kwargs.items():
            if hasattr(rule, key):
                setattr(rule, key, value)

        await self.session.flush()
        # Object is already in session with updated values, no need to refetch
        return rule

    async def get_all_by_repository(self, repository_ids: list[UUID]) -> list[Rule]:
        """Get all rules for a list of repositories."""
        if not repository_ids:
            return []
        query = select(Rule).where(Rule.repository_id.in_(repository_ids))
        result = await self.session.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _parse_additional_fields(rule: dict) -> None:
        """Parse additional fields from rule."""
        rule_body = rule.get("body")
        if rule_body is None:
            return  # Body not provided, skip parsing additional fields
        try:
            sigma_dict = yaml.safe_load(rule_body)
        except yaml.YAMLError:
            sigma_dict = {}
        sigma_dict = sigma_dict if isinstance(sigma_dict, dict) else {}
        logsource = sigma_dict.get("logsource")
        logsource = logsource if isinstance(logsource, dict) else {}
        product = logsource.get("product")
        service = logsource.get("service")
        category = logsource.get("category")
        rule["product"] = product if isinstance(product, str) else None
        rule["service"] = service if isinstance(service, str) else None
        rule["category"] = category if isinstance(category, str) else None
