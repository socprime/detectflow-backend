import re
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any, NamedTuple
from uuid import UUID

import yaml
from sqlalchemy import Text, and_, cast, delete, false, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only
from sqlalchemy.sql.elements import ColumnElement

from apps.core.enums import RuleSearchField
from apps.core.models import Rule
from apps.modules.postgre.base import BaseDAO
from apps.modules.postgre.rule_content import CHANGE_SOURCE_SYSTEM, RuleContentDAO, compute_content_hash
from apps.modules.utils import compute_logsource_folder_path, normalize_sigma_level, normalize_sigma_uuid

# A uuid rendered as text is only hex digits and dashes, so a term holding any other character
# can never be a fragment of one — such terms get no fragment predicate at all.
_SIGMA_UUID_FRAGMENT = re.compile(r"^[0-9a-fA-F-]+$")


def sigma_uuid_search_condition(search: str, *, allow_fragment: bool = True) -> ColumnElement[bool] | None:
    """Build the ``rules.sigma_uuid`` predicate for a free-text search term ("By ID" search).

    ``Rule.sigma_uuid`` is a native PostgreSQL ``uuid``; there is no ``uuid ~~* text`` operator, so
    it must never reach ``.ilike()`` directly (that raises and surfaces as a 500). A whole UUID —
    dashed, undashed, upper-case or URN/brace-wrapped — is matched by equality and uses
    ``ix_rules_sigma_uuid``. A shorter fragment is matched against the dash-stripped text rendering
    of the column, so a fragment matches whether or not it straddles a dash.

    Args:
        search: Raw search term from the request.
        allow_fragment: Whether a partial id may match. True for an explicit "By ID" search box.
            False where the same term also feeds a name search (the pipeline details page): a short
            hex fragment such as "4" or "a" occurs in nearly every UUID, so a mixed box must react
            to whole ids only or it stops narrowing while the user types an ordinary name.

    Returns:
        A SQL predicate, or ``None`` when the term cannot match any UUID — callers decide whether
        that means "skip this leg of an OR" or "match nothing".
    """
    term = search.strip()
    if not term:
        return None
    normalized = normalize_sigma_uuid(term)
    if normalized is not None:
        return Rule.sigma_uuid == UUID(normalized)
    if not allow_fragment or not _SIGMA_UUID_FRAGMENT.match(term):
        return None
    hex_term = term.replace("-", "")
    if not hex_term:
        return None
    return func.replace(cast(Rule.sigma_uuid, Text), "-", "").ilike(f"%{hex_term}%")


# Searchable rule fields -> predicate builder. An explicit allow-list, because the search fields
# come straight from the query string: reflection over the model would let a caller build an
# ILIKE on a uuid/boolean column or on a Python property and take the endpoint down with a 500.
RULE_SEARCH_CONDITIONS: dict[str, Callable[[str], ColumnElement[bool] | None]] = {
    RuleSearchField.NAME.value: lambda term: Rule.name.ilike(f"%{term}%"),
    RuleSearchField.PRODUCT.value: lambda term: Rule.product.ilike(f"%{term}%"),
    RuleSearchField.SERVICE.value: lambda term: Rule.service.ilike(f"%{term}%"),
    RuleSearchField.CATEGORY.value: lambda term: Rule.category.ilike(f"%{term}%"),
    RuleSearchField.SIGMA_UUID.value: sigma_uuid_search_condition,
}


class RuleTechniqueFields(NamedTuple):
    """Lightweight ``rules`` projection for attack-chain technique rows (no body)."""

    id: UUID
    repository_id: UUID | None
    name: str
    level: str | None


class RuleDAO(BaseDAO[Rule]):
    def __init__(self, session: AsyncSession):
        super().__init__(Rule, session)
        self.content_dao = RuleContentDAO(session)

    async def get_by_id(self, id: UUID) -> Rule | None:
        """Get rule by ID with repository loaded via left join."""
        query = (
            select(Rule)
            .options(joinedload(Rule.repository), joinedload(Rule.current_content))
            .where(Rule.id == id)
            .execution_options(populate_existing=True)
        )
        result = await self.session.execute(query)
        return result.unique().scalar_one_or_none()

    async def get_by_ids(self, ids: list[UUID]) -> list[Rule]:
        """Get rules by list of IDs. Loads current_content — rule.body is available."""
        if not ids:
            return []
        query = select(Rule).options(joinedload(Rule.current_content)).where(Rule.id.in_(ids))
        result = await self.session.execute(query)
        return list(result.unique().scalars().all())

    async def _get_rule_for_update(self, id: UUID) -> Rule | None:
        """Lock a single rule row for update within the current transaction.

        Note: does NOT load current_content — rule.body is unavailable.
        """
        result = await self.session.execute(select(Rule).where(Rule.id == id).with_for_update())
        return result.scalar_one_or_none()

    async def _get_rules_for_update(self, ids: list[UUID]) -> dict[UUID, Rule]:
        """Lock existing rules with SELECT ... FOR UPDATE, ordered by id ASC.

        Consistent ordering prevents deadlocks when concurrent transactions
        (e.g. parallel syncs) lock overlapping rule sets. Any new code path
        that locks multiple rules MUST use the same ascending id order.

        Note: does NOT load current_content — rule.body is unavailable.
        """
        if not ids:
            return {}

        result = await self.session.execute(
            select(Rule).where(Rule.id.in_(ids)).order_by(Rule.id.asc()).with_for_update()
        )
        rules = result.scalars().all()
        return {rule.id: rule for rule in rules}

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
        """Get all rules with repository loaded (metadata only).

        Note: does NOT load current_content — rule.body is unavailable.
        """
        query = select(Rule).options(
            load_only(
                Rule.id,
                Rule.name,
                Rule.version,
                Rule.repository_id,
                Rule.created,
                Rule.updated,
                Rule.product,
                Rule.service,
                Rule.category,
                Rule.folder_path,
                Rule.sigma_uuid,
                Rule.level,
                Rule.external_id,
                Rule.is_supported,
                Rule.unsupported_reason,
                Rule.is_user_edited_system_rule,
            ),
            joinedload(Rule.repository),
        )

        if filters:
            conditions = []
            for key, value in filters.items():
                if key == "folder_path":
                    folder_path_column = func.lower(Rule.folder_path)
                    folder_path = str(value).strip().lower()
                    conditions.append(
                        or_(folder_path_column == folder_path, folder_path_column.like(f"{folder_path}/%"))
                    )
                    continue
                if hasattr(Rule, key):
                    conditions.append(getattr(Rule, key) == value)
            if conditions:
                query = query.where(and_(*conditions))

        if search and search_fields:
            builders = [
                RULE_SEARCH_CONDITIONS[str(field)] for field in search_fields if str(field) in RULE_SEARCH_CONDITIONS
            ]
            if builders:
                search_conditions = [condition for build in builders if (condition := build(search)) is not None]
                # Every requested field was a real one, so a term none of them can match means
                # "no results" — falling through unfiltered would present the whole catalog as hits.
                query = query.where(or_(*search_conditions) if search_conditions else false())

        count_query = select(func.count()).select_from(Rule)
        if query.whereclause is not None:
            count_query = count_query.where(query.whereclause)
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        if sort and hasattr(Rule, sort):
            sort_column = getattr(Rule, sort)
            if order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(Rule.id.asc())

        query = query.offset(skip).limit(limit)

        result = await self.session.execute(query)
        items = result.unique().scalars().all()
        return list(items), total

    async def create_many(
        self,
        rules: list[dict[str, Any]],
        change_source: str | None = None,
        changed_by: str | None = None,
    ) -> list[Rule]:
        """Batch-insert rules with their first content versions.

        Each dict in ``rules`` MUST contain ``"id"`` (pre-generated UUID).
        Raises ValueError if any rule dict is missing ``"id"``.

        Note: returned Rule instances do NOT have current_content loaded —
        rule.body is unavailable. Use get_by_id/get_by_ids if you need it.
        """
        bodies: list[str | None] = []
        instances: list[Rule] = []

        for raw in rules:
            if not raw.get("id"):
                raise ValueError("create_many requires 'id' in every rule dict")

            rule_data = raw.copy()
            body = rule_data.pop("body", None)
            bodies.append(body)
            self._parse_additional_fields_from_body(rule_data, body)
            rule_data["version"] = 1
            instances.append(self.model(**rule_data))

        self.session.add_all(instances)
        await self.session.flush()
        await self._create_initial_content_versions(instances, bodies, change_source, changed_by)

        for instance in instances:
            self.session.expire(instance)

        return instances

    async def upsert_many(
        self,
        rules: list[dict[str, Any]],
        change_source: str | None = None,
        changed_by: str | None = None,
    ) -> None:
        """Upsert multiple rules with versioned body content."""
        if not rules:
            return

        # ``rule["id"]`` may be a plain string (GitHub/TDM sync) or a UUID-like value read back
        # from the DB (e.g. asyncpg's native ``asyncpg.pgproto.pgproto.UUID``, which is not a
        # ``uuid.UUID`` instance and breaks ``UUID(value)``); ``str()`` normalizes both.
        rule_ids = [UUID(str(rule["id"])) for rule in rules if "id" in rule]
        locked_rules = await self._get_rules_for_update(rule_ids)
        existing_ids = set(locked_rules.keys())

        rules_to_update = [rule for rule in rules if UUID(str(rule["id"])) in existing_ids]
        rules_to_create = [rule for rule in rules if UUID(str(rule["id"])) not in existing_ids]

        for raw in rules_to_update:
            rule_id = UUID(str(raw["id"]))
            locked_rule = locked_rules[rule_id]
            rule_data = raw.copy()
            body = rule_data.pop("body", None)
            self._parse_additional_fields_from_body(rule_data, body)
            update_data = {k: v for k, v in rule_data.items() if k != "id"}

            await self._apply_versioned_update(locked_rule, update_data, body, change_source, changed_by)

        if rules_to_update:
            await self.session.flush()
            for rule in locked_rules.values():
                self.session.expire(rule)

        if rules_to_create:
            await self.create_many(rules_to_create, change_source=change_source, changed_by=changed_by)

    async def get_rule_count_by_repository_id(self, repository_id: UUID) -> int:
        """Return rule count for a repository. No Rule objects returned."""
        result = await self.session.execute(
            select(func.count()).select_from(Rule).where(Rule.repository_id == repository_id)
        )
        return result.scalar() or 0

    async def get_rule_counts_by_repositories(self, repository_ids: list[UUID]) -> dict[UUID, int]:
        """Rule counts per repository in one GROUP BY query.

        Replaces per-repository ``SELECT count(*)`` loops (the dashboard refresher ran one per
        repo per 2s tick). Repositories with zero rules are absent from the result — callers
        should ``.get(repo_id, 0)``.
        """
        if not repository_ids:
            return {}

        result = await self.session.execute(
            select(Rule.repository_id, func.count(Rule.id))
            .where(Rule.repository_id.in_(repository_ids))
            .group_by(Rule.repository_id)
        )
        return dict(result.all())

    async def get_folder_counts_by_repositories(self, repository_ids: list[UUID]) -> dict[UUID, dict[str, int]]:
        """Get exact folder rule counts grouped by repository."""
        if not repository_ids:
            return {}

        folder_path = func.lower(Rule.folder_path).label("folder_path")
        result = await self.session.execute(
            select(Rule.repository_id, folder_path, func.count(Rule.id))
            .where(Rule.repository_id.in_(repository_ids), Rule.folder_path.is_not(None))
            .group_by(Rule.repository_id, folder_path)
        )

        counts: dict[UUID, dict[str, int]] = defaultdict(dict)
        for repository_id, folder_path, count in result.all():
            counts[repository_id][folder_path] = count
        return dict(counts)

    async def get_all_rule_ids_with_repository_id(self) -> list[tuple[str, str]]:
        """Get all rule ids with repository id. Returns list of tuples (rule_id, repository_id)."""
        result = await self.session.execute(select(Rule.id, Rule.repository_id))
        return [(str(rule_id), str(repository_id)) for rule_id, repository_id in result.all()]

    async def create(
        self,
        change_source: str | None = None,
        changed_by: str | None = None,
        **kwargs,
    ) -> Rule:
        """Create a rule with initial content version. Returns rule via get_by_id — rule.body is available."""
        body = kwargs.pop("body", None)
        self._parse_additional_fields_from_body(kwargs, body)
        kwargs["version"] = 1

        instance = self.model(**kwargs)
        self.session.add(instance)
        await self.session.flush()
        await self.session.refresh(instance)
        if hasattr(instance, "created") and hasattr(instance, "updated"):
            if instance.created and not instance.updated:
                instance.updated = instance.created
                await self.session.flush()

        if body is not None:
            content_hash = compute_content_hash(body)
            await self.content_dao.create_version(
                rule_id=instance.id,
                version=1,
                body=body,
                content_hash=content_hash,
                change_source=change_source,
                changed_by=changed_by,
            )

        rule_id = instance.id
        self.session.expire(instance)
        rule = await self.get_by_id(rule_id)
        return rule if rule else instance

    async def delete_by_repository_id(self, repository_ids: list[UUID]) -> int:
        """Delete all rules for a given list of repositories."""
        if not repository_ids:
            return 0
        result = await self.session.execute(delete(Rule).where(Rule.repository_id.in_(repository_ids)))
        await self.session.flush()
        return result.rowcount

    async def _apply_versioned_update(
        self,
        rule: Rule,
        update_data: dict[str, Any],
        body: str | None,
        change_source: str | None,
        changed_by: str | None,
    ) -> None:
        """Compare body hash and create a new content version if changed, then apply attributes.

        Mutates ``rule`` in-place via setattr — SQLAlchemy dirty-tracks
        the changes and will emit UPDATE on next flush.
        If body is unchanged (same SHA-256 hash), no new version is created.
        """
        current_content = await self.content_dao.get_version(rule.id, rule.version)
        current_hash = current_content.content_hash if current_content else ""

        if body is not None:
            new_hash = compute_content_hash(body)

            if new_hash != current_hash:
                new_version = rule.version + 1
                await self.content_dao.create_version(
                    rule_id=rule.id,
                    version=new_version,
                    body=body,
                    content_hash=new_hash,
                    change_source=change_source,
                    changed_by=changed_by,
                )
                update_data["version"] = new_version

        for key, value in update_data.items():
            if hasattr(rule, key):
                setattr(rule, key, value)

    async def update(
        self,
        id: UUID,
        change_source: str | None = None,
        changed_by: str | None = None,
        **kwargs,
    ) -> Rule | None:
        """Update rule. If body changed and hash differs, creates a new content version.

        Returns rule via get_by_id — rule.body is available and generated columns
        such as folder_path are fresh.
        """
        body = kwargs.pop("body", None)
        self._parse_additional_fields_from_body(kwargs, body)

        rule = await self._get_rule_for_update(id)
        if not rule:
            return None

        await self._apply_versioned_update(rule, kwargs, body, change_source, changed_by)
        await self.session.flush()
        # Expire so get_by_id re-reads generated columns (folder_path) and joins current_content.
        self.session.expire(rule)
        return await self.get_by_id(id)

    async def get_all_by_repository(self, repository_ids: list[UUID]) -> list[Rule]:
        """Get all rules for a list of repositories. Loads current_content — rule.body is available."""
        if not repository_ids:
            return []
        query = select(Rule).options(joinedload(Rule.current_content)).where(Rule.repository_id.in_(repository_ids))
        result = await self.session.execute(query)
        return list(result.unique().scalars().all())

    async def get_all_by_repository_selections(self, selections: dict[UUID, list[str]]) -> list[Rule]:
        """Get rules that match repository selections and optional folder subtrees.

        Loads ``current_content`` — ``rule.body`` is available (needed by Kafka sync callers).
        """
        if not selections:
            return []

        conditions = []
        for repository_id, folder_paths in selections.items():
            if not folder_paths:
                conditions.append(Rule.repository_id == repository_id)
                continue

            folder_conditions = [
                or_(Rule.folder_path == folder_path, Rule.folder_path.like(f"{folder_path}/%"))
                for folder_path in folder_paths
            ]
            conditions.append(and_(Rule.repository_id == repository_id, or_(*folder_conditions)))

        query = select(Rule).options(joinedload(Rule.current_content)).where(or_(*conditions))
        result = await self.session.execute(query)
        return list(result.unique().scalars().all())

    async def get_latest_updated_by_repository_id(
        self,
        repository_id: UUID,
        *,
        exclude_user_edited_system_rules: bool = False,
    ) -> datetime | None:
        query = select(func.max(Rule.updated)).where(Rule.repository_id == repository_id)
        if exclude_user_edited_system_rules:
            query = query.where(Rule.is_user_edited_system_rule.is_(False))
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_sigma_uuids_by_repository_id(self, repository_id: UUID) -> set[str]:
        result = await self.session.execute(
            select(Rule.sigma_uuid).where(Rule.repository_id == repository_id, Rule.sigma_uuid.is_not(None))
        )
        return {str(row[0]) for row in result.all()}

    async def get_technique_fields_by_ids(self, ids: Iterable[UUID]) -> dict[UUID, RuleTechniqueFields]:
        """Map ``rules.id`` -> id/repository_id/name/level without loading ``current_content``.

        Used by the attack-chain detail view for events that carry ``attack_chain_events.rule_id``.
        ``get_by_ids`` joined-loads the body and is the wrong tool for this hot path.
        """
        normalized = {value for value in ids if value is not None}
        if not normalized:
            return {}
        result = await self.session.execute(
            select(Rule.id, Rule.repository_id, Rule.name, Rule.level).where(Rule.id.in_(normalized))
        )
        return {
            row.id: RuleTechniqueFields(
                id=row.id,
                repository_id=row.repository_id,
                name=row.name,
                level=row.level,
            )
            for row in result.all()
        }

    async def get_levels_by_sigma_uuids(self, sigma_uuids: Iterable[UUID | str]) -> dict[str, str]:
        """Map Sigma UUID -> severity for the given UUIDs, skipping rules with no level.

        Used by the attack-chain detail view for **legacy** events that have no
        ``attack_chain_events.rule_id``. Those rows only carry ``sigma_rule_id`` (a Sigma-body
        UUID, not ``rules.id``) and are therefore not joined to ``rules`` anywhere else.

        ``rules.sigma_uuid`` is neither unique nor NOT NULL — the same Sigma legitimately exists
        in several repositories (e.g. a synced API repo and the Active Threat system repo), so the
        lookup is 1:N and must not assume a single row. Those copies can genuinely disagree: editing
        a system-repo rule sets ``is_user_edited_system_rule`` and re-derives its level while the
        synced copy keeps the old one. The most recently written copy therefore wins, and the order
        is explicit so the same request cannot return a different severity each time.
        """
        normalized = {str(value) for value in sigma_uuids if value is not None}
        if not normalized:
            return {}
        result = await self.session.execute(
            select(Rule.sigma_uuid, Rule.level)
            .where(
                Rule.sigma_uuid.in_([UUID(value) for value in normalized]),
                Rule.level.is_not(None),
            )
            .order_by(Rule.updated.desc(), Rule.id.asc())
        )
        levels: dict[str, str] = {}
        for sigma_uuid, level in result.all():
            levels.setdefault(str(sigma_uuid), level)
        return levels

    async def get_external_ids_by_repository_id(self, repository_id: UUID) -> set[str]:
        result = await self.session.execute(
            select(Rule.external_id).where(Rule.repository_id == repository_id, Rule.external_id.is_not(None))
        )
        return {row[0] for row in result.all()}

    async def upsert_system_repository_rules(
        self, repository_id: UUID, rules: list[dict[str, Any]]
    ) -> tuple[list[UUID], list[UUID]]:
        """Upsert TDM-backed system rules without using TDM ids as local primary keys.

        Returns ``(new_rule_ids, updated_rule_ids)`` so callers (Active Threat sync)
        can propagate the changes to connected pipelines and Kafka.
        """
        if not rules:
            return [], []

        prepared = self._prepare_system_repository_rules(repository_id, rules)
        by_external_id, by_sigma_uuid = await self._load_system_repository_rule_indexes(repository_id, prepared)
        new_instances, new_bodies, rules_to_update = self._partition_system_repository_upserts(
            prepared, by_external_id, by_sigma_uuid
        )

        await self.session.flush()
        await self._create_initial_content_versions(new_instances, new_bodies, CHANGE_SOURCE_SYSTEM, "system")
        updated_ids = await self._update_system_repository_rules(rules_to_update)

        return [instance.id for instance in new_instances], updated_ids

    def _prepare_system_repository_rules(
        self, repository_id: UUID, rules: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for rule in rules:
            rule_data = rule.copy()
            rule_data["repository_id"] = repository_id
            self._parse_additional_fields_from_body(rule_data, rule_data.get("body"))
            prepared.append(rule_data)
        return prepared

    async def _load_system_repository_rule_indexes(
        self, repository_id: UUID, prepared_rules: list[dict[str, Any]]
    ) -> tuple[dict[str, Rule], dict[str, Rule]]:
        external_ids = [rule["external_id"] for rule in prepared_rules if rule.get("external_id")]
        sigma_uuids = [rule["sigma_uuid"] for rule in prepared_rules if rule.get("sigma_uuid")]
        conditions = []
        if external_ids:
            conditions.append(Rule.external_id.in_(external_ids))
        if sigma_uuids:
            conditions.append(Rule.sigma_uuid.in_(sigma_uuids))
        if not conditions:
            return {}, {}

        result = await self.session.execute(select(Rule).where(Rule.repository_id == repository_id, or_(*conditions)))
        existing_rules = list(result.scalars().all())
        by_external_id = {rule.external_id: rule for rule in existing_rules if rule.external_id}
        by_sigma_uuid = {str(rule.sigma_uuid): rule for rule in existing_rules if rule.sigma_uuid}
        return by_external_id, by_sigma_uuid

    def _partition_system_repository_upserts(
        self,
        prepared_rules: list[dict[str, Any]],
        by_external_id: dict[str, Rule],
        by_sigma_uuid: dict[str, Rule],
    ) -> tuple[list[Rule], list[str | None], list[tuple[UUID, dict[str, Any]]]]:
        new_instances: list[Rule] = []
        new_bodies: list[str | None] = []
        rules_to_update: list[tuple[UUID, dict[str, Any]]] = []

        for rule_data in prepared_rules:
            existing = self._find_existing_system_rule(rule_data, by_external_id, by_sigma_uuid)
            if existing is None:
                create_data, body = self._split_rule_body(rule_data)
                create_data["version"] = 1
                instance = self.model(**create_data)
                self.session.add(instance)
                new_instances.append(instance)
                new_bodies.append(body)
                continue

            if existing.is_user_edited_system_rule:
                continue
            rules_to_update.append((existing.id, rule_data))

        return new_instances, new_bodies, rules_to_update

    async def _update_system_repository_rules(self, rules_to_update: list[tuple[UUID, dict[str, Any]]]) -> list[UUID]:
        if not rules_to_update:
            return []

        locked_rules = await self._get_rules_for_update([rule_id for rule_id, _ in rules_to_update])
        updated_ids: list[UUID] = []
        for rule_id, rule_data in rules_to_update:
            update_data, body = self._split_rule_body(rule_data)
            await self._apply_versioned_update(locked_rules[rule_id], update_data, body, CHANGE_SOURCE_SYSTEM, "system")
            updated_ids.append(rule_id)

        await self.session.flush()
        return updated_ids

    async def _create_initial_content_versions(
        self,
        instances: list[Rule],
        bodies: list[str | None],
        change_source: str | None,
        changed_by: str | None,
    ) -> None:
        content_data = [
            {
                "rule_id": instance.id,
                "version": 1,
                "body": body,
                "content_hash": compute_content_hash(body),
                "change_source": change_source,
                "changed_by": changed_by,
            }
            for instance, body in zip(instances, bodies, strict=True)
            if body is not None
        ]
        await self.content_dao.create_versions_bulk(content_data)

    @staticmethod
    def _find_existing_system_rule(
        rule_data: dict[str, Any],
        by_external_id: dict[str, Rule],
        by_sigma_uuid: dict[str, Rule],
    ) -> Rule | None:
        external_id = rule_data.get("external_id")
        if external_id and external_id in by_external_id:
            return by_external_id[external_id]

        sigma_uuid = rule_data.get("sigma_uuid")
        if sigma_uuid:
            return by_sigma_uuid.get(str(sigma_uuid))
        return None

    @staticmethod
    def _split_rule_body(rule_data: dict[str, Any]) -> tuple[dict[str, Any], str | None]:
        data = rule_data.copy()
        body = data.pop("body", None)
        data.pop("id", None)
        return data, body

    async def delete_unreferenced_by_repository_id(
        self,
        repository_id: UUID,
        referenced_sigma_uuids: set[str],
        protected_external_ids: set[str] | None = None,
    ) -> list[UUID]:
        """Delete rules from a system repo that are no longer referenced.

        A rule is kept when its ``sigma_uuid`` is in ``referenced_sigma_uuids``
        (HOS-referenced rules) OR its ``external_id`` is in ``protected_external_ids``
        (behavioral-rule memberships, keyed by TDM case id — protects rows even when
        ``sigma_uuid`` is NULL).

        Returns the list of deleted rule ids so callers can emit Kafka
        deletes for connected pipelines.
        """
        normalized_uuids = {UUID(value) for value in referenced_sigma_uuids}
        protected_external_ids = protected_external_ids or set()
        select_query = select(Rule.id).where(
            Rule.repository_id == repository_id,
            Rule.is_user_edited_system_rule.is_(False),
        )
        if normalized_uuids:
            select_query = select_query.where(or_(Rule.sigma_uuid.is_(None), Rule.sigma_uuid.not_in(normalized_uuids)))
        if protected_external_ids:
            select_query = select_query.where(
                or_(Rule.external_id.is_(None), Rule.external_id.not_in(protected_external_ids))
            )
        select_result = await self.session.execute(select_query)
        deleted_ids = [row[0] for row in select_result.all()]
        if not deleted_ids:
            return []

        await self.session.execute(delete(Rule).where(Rule.id.in_(deleted_ids)))
        await self.session.flush()
        return deleted_ids

    @staticmethod
    def _parse_additional_fields_from_body(rule: dict, body: str | None) -> None:
        """Parse product/service/category/sigma_uuid/level from Sigma YAML body into the rule dict.

        This is the single derivation point for body-derived rule columns: every write path
        (``create``, ``create_many``, ``upsert_many``, ``update``, ``upsert_system_repository_rules``)
        routes through it, so a field set here is set for manual CRUD, bulk create, rollback,
        TDM sync, GitHub sync, GitLab sync and Active Threat system-repo sync alike.

        A caller that explicitly provides ``folder_path`` (even as ``None``) owns the value; it is
        never overwritten with a logsource-derived path. GitLab-synced rules living in the project
        root pass ``folder_path=None`` so they stay at the repository root instead of being placed
        into a virtual product/category/service folder.
        """
        has_explicit_folder_path = "folder_path" in rule
        explicit_folder_path = rule.get("folder_path")
        if body is None:
            if not has_explicit_folder_path and any(rule.get(field) for field in ("product", "service", "category")):
                rule["folder_path"] = compute_logsource_folder_path(
                    rule.get("product"),
                    rule.get("service"),
                    rule.get("category"),
                )
            return
        try:
            sigma_dict = yaml.safe_load(body)
        except yaml.YAMLError:
            sigma_dict = {}
        sigma_dict = sigma_dict if isinstance(sigma_dict, dict) else {}
        logsource = sigma_dict.get("logsource")
        logsource = logsource if isinstance(logsource, dict) else {}
        product = logsource.get("product")
        service = logsource.get("service")
        category = logsource.get("category")
        sigma_uuid = sigma_dict.get("id")
        rule["product"] = product if isinstance(product, str) else None
        rule["service"] = service if isinstance(service, str) else None
        rule["category"] = category if isinstance(category, str) else None
        rule["sigma_uuid"] = normalize_sigma_uuid(sigma_uuid)
        rule["level"] = normalize_sigma_level(sigma_dict.get("level"))
        if has_explicit_folder_path:
            rule["folder_path"] = explicit_folder_path
        else:
            rule["folder_path"] = compute_logsource_folder_path(
                rule.get("product"),
                rule.get("service"),
                rule.get("category"),
            )
