from collections.abc import Iterable
from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import and_, delete, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from apps.core.models import (
    ActiveThreat,
    ActiveThreatBehavioralRule,
    CorrelationPipelineExcludedActiveThreat,
    HighOrderSigma,
    Repository,
    Rule,
)
from apps.modules.postgre.high_order_sigma_rule_checks import HighOrderSigmaRuleCheckFailureDAO
from apps.modules.postgre.repository import SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY

# Sort keys whose SQL column carries the opposite polarity of the key's meaning. The frontend still
# sorts by "enabled"; the column behind it is now ``is_blocked``, so the direction is flipped to
# keep "asc" meaning the same rows in the same order as before D8.
_INVERTED_SORT_KEYS = frozenset({"enabled"})


class ActiveThreatDAO:
    """Data access for active threats (TDM-backed catalog rows)."""

    @staticmethod
    def _resolve_sort_column(sort: str | None):
        if sort == "release_date":
            return func.coalesce(ActiveThreat.tdm_created, ActiveThreat.created)
        # ``enabled`` is gone as a column; the key survives for the frontend and maps to its
        # inverse. ``_sort_is_inverted`` supplies the matching direction flip.
        if sort == "enabled":
            return ActiveThreat.is_blocked
        if sort == "blocked":
            return ActiveThreat.is_blocked
        if sort and hasattr(ActiveThreat, sort):
            return getattr(ActiveThreat, sort)
        return None

    @staticmethod
    def _sort_is_inverted(sort: str | None) -> bool:
        """True when the resolved column must be sorted opposite to the requested direction."""
        return sort in _INVERTED_SORT_KEYS

    @staticmethod
    def _search_clause(search: str, search_fields: Iterable[str] | None):
        """OR of ILIKE on scalar ``search_fields`` plus technique-id substring match.

        ``techniques`` is a string array, so it cannot use ``.ilike()``. ``array_to_string``
        lets ``?search=T1053`` hit both ``T1053`` and ``T1053.005``.
        """
        conditions = []
        for field in search_fields or ():
            column = getattr(ActiveThreat, field, None)
            if column is not None:
                conditions.append(column.ilike(f"%{search}%"))
        conditions.append(func.array_to_string(ActiveThreat.techniques, " ").ilike(f"%{search}%"))
        return or_(*conditions)

    @staticmethod
    def monitored_clause(correlation_pipeline_id: UUID):
        """SQL for D8: monitored = NOT blocked AND no exclusion row for this pipeline.

        The single definition of the effective rule — every read path that asks "does this
        pipeline monitor this threat?" composes this clause instead of re-deriving it.
        """
        excluded = select(CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id).where(
            CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id == correlation_pipeline_id
        )
        return and_(ActiveThreat.is_blocked.is_(False), ActiveThreat.tdm_id.not_in(excluded))

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_tdm_id(self, tdm_id: str) -> ActiveThreat | None:
        query = (
            select(ActiveThreat)
            .options(
                selectinload(ActiveThreat.high_order_sigmas).selectinload(HighOrderSigma.rule_refs),
            )
            .where(ActiveThreat.tdm_id == tdm_id)
        )
        result = await self.session.execute(query)
        threat = result.scalar_one_or_none()
        if threat:
            await self._hydrate_linked_rules(threat)
            await self._hydrate_rule_check_results([threat])
        return threat

    async def get_graph_daily_series(self, exclude_ids: Iterable[str] | None = None) -> list[tuple[date, int]]:
        """Return daily UTC aggregates for correlation graph data.

        Hidden Active Threats are excluded so the dashboard reflects only the
        threats currently surfaced to the correlation job.

        Args:
            exclude_ids: Active Threat ``tdm_id`` values to leave out — one correlation pipeline's
                deny-list (§20), so a scoped dashboard shows that tenant's monitored set.

        Tuple shape:
            (day_date, monitored_threats_count)
        """
        day_col = func.date(func.timezone("UTC", ActiveThreat.tdm_updated)).label("day")
        stmt = (
            select(day_col, func.count(ActiveThreat.tdm_id).label("count"))
            .where(
                ActiveThreat.tdm_updated.is_not(None),
                ActiveThreat.is_hidden.is_(False),
            )
            .group_by(day_col)
            .order_by(day_col.asc())
        )
        stmt = self._apply_exclusions(stmt, exclude_ids)
        result = await self.session.execute(stmt)
        return list(result.all())

    @staticmethod
    def _apply_exclusions(stmt, exclude_ids: Iterable[str] | None):
        """Drop a correlation pipeline's excluded Active Threats (§20 deny-list) from an aggregate."""
        ids = list(exclude_ids or [])
        return stmt.where(ActiveThreat.tdm_id.not_in(ids)) if ids else stmt

    async def get_monitored_threats_count(self) -> int:
        result = await self.session.execute(
            select(func.count(ActiveThreat.tdm_id)).where(ActiveThreat.is_hidden.is_(False))
        )
        return int(result.scalar() or 0)

    async def get_unblocked_threats_count(self, exclude_ids: Iterable[str] | None = None) -> int:
        """Threats actively monitored: not globally blocked, not hidden, minus ``exclude_ids``.

        ``exclude_ids`` is one correlation pipeline's deny-list, so passing it turns the global
        count into that pipeline's monitored count — the two conjuncts of D8 evaluated together.
        """
        stmt = select(func.count(ActiveThreat.tdm_id)).where(
            ActiveThreat.is_blocked.is_(False),
            ActiveThreat.is_hidden.is_(False),
        )
        result = await self.session.execute(self._apply_exclusions(stmt, exclude_ids))
        return int(result.scalar() or 0)

    async def get_latest_tdm_updated(self) -> datetime | None:
        """Return the most recent TDM update timestamp stored locally."""
        result = await self.session.execute(select(func.max(ActiveThreat.tdm_updated)))
        return result.scalar_one_or_none()

    async def get_earliest_tdm_updated(self) -> datetime | None:
        """Return the oldest TDM update timestamp stored locally (None when empty)."""
        result = await self.session.execute(select(func.min(ActiveThreat.tdm_updated)))
        return result.scalar_one_or_none()

    async def get_distinct_severities(self) -> list[str]:
        result = await self.session.execute(
            select(ActiveThreat.severity)
            .distinct()
            .where(ActiveThreat.severity.is_not(None))
            .order_by(ActiveThreat.severity)
        )
        return [row[0] for row in result.fetchall()]

    async def get_distinct_array_values(self, column: Any) -> list[str]:
        value = func.unnest(column).label("value")
        subquery = select(value).where(column.is_not(None)).subquery()
        result = await self.session.execute(
            select(subquery.c.value).distinct().where(subquery.c.value.is_not(None)).order_by(subquery.c.value)
        )
        return [row[0] for row in result.fetchall()]

    async def get_by_tdm_ids(self, tdm_ids: list[str]) -> list[ActiveThreat]:
        if not tdm_ids:
            return []
        result = await self.session.execute(select(ActiveThreat).where(ActiveThreat.tdm_id.in_(tdm_ids)))
        return list(result.scalars().all())

    async def _hydrate_linked_rules(self, threat: ActiveThreat) -> None:
        """Attach ``linked_rules`` on ``threat``: HOS Sigma UUID refs plus behavioral-rule memberships.

        HOS-derived rules come first (ordered by their Sigma UUID refs), then
        behavioral rules are appended; the combined list is deduplicated by ``Rule.id``.
        """
        linked_rules: list[Rule] = []
        seen_rule_ids: set[Any] = set()

        sigmas = list(threat.high_order_sigmas or [])
        ref_uuids = [ref.sigma_uuid for h in sigmas for ref in (h.rule_refs or [])]
        ordered_unique = list(dict.fromkeys(ref_uuids))
        if ordered_unique:
            res = await self.session.execute(
                select(Rule).options(joinedload(Rule.repository)).where(Rule.sigma_uuid.in_(ordered_unique))
            )
            by_sigma_uuid: dict[Any, list[Rule]] = {}
            for rule in res.unique().scalars().all():
                by_sigma_uuid.setdefault(rule.sigma_uuid, []).append(rule)
            for sigma_uuid in ordered_unique:
                for rule in by_sigma_uuid.get(sigma_uuid, []):
                    if rule.id in seen_rule_ids:
                        continue
                    seen_rule_ids.add(rule.id)
                    linked_rules.append(rule)

        for rule in await self._get_behavioral_rules(threat.tdm_id):
            if rule.id in seen_rule_ids:
                continue
            seen_rule_ids.add(rule.id)
            linked_rules.append(rule)

        threat.linked_rules = linked_rules

    async def _get_behavioral_rules(self, active_threat_id: str) -> list[Rule]:
        """Return behavioral Sigma rules linked to an Active Threat (with repository loaded).

        Memberships store TDM case ids (``rules.external_id``); the join is scoped to the
        Active Threat system repository because ``external_id`` is not unique across repos.
        """
        result = await self.session.execute(
            select(Rule)
            .options(joinedload(Rule.repository))
            .join(ActiveThreatBehavioralRule, ActiveThreatBehavioralRule.external_id == Rule.external_id)
            .where(
                ActiveThreatBehavioralRule.active_threat_id == active_threat_id,
                Rule.repository.has(Repository.system_key == SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY),
            )
            .order_by(Rule.name.asc())
        )
        return list(result.unique().scalars().all())

    async def get_all_behavioral_external_ids(self) -> set[str]:
        """Return every TDM case id (``external_id``) referenced by any behavioral membership."""
        result = await self.session.execute(select(ActiveThreatBehavioralRule.external_id).distinct())
        return {row[0] for row in result.all()}

    async def replace_behavioral_memberships_for_active_threats(self, memberships: dict[str, set[str]]) -> None:
        """Replace AT->behavioral rule memberships (by TDM case id) for the given Active Threats.

        For each Active Threat in ``memberships`` we delete its existing rows in
        ``active_threat_behavioral_rules`` and insert the provided external-id set.
        Active Threats not in the dict are left alone. Mirrors
        :meth:`HighOrderSigmaDAO.replace_memberships_for_active_threats`.
        """
        if not memberships:
            return
        active_threat_ids = list(memberships.keys())
        await self.session.execute(
            delete(ActiveThreatBehavioralRule).where(ActiveThreatBehavioralRule.active_threat_id.in_(active_threat_ids))
        )
        for active_threat_id, external_ids in memberships.items():
            for external_id in external_ids:
                self.session.add(ActiveThreatBehavioralRule(active_threat_id=active_threat_id, external_id=external_id))
        await self.session.flush()

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 100,
        search: str | None = None,
        search_fields: Iterable[str] | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
        severity: list[str] | None = None,
        industries: list[str] | None = None,
        threat_types: list[str] | None = None,
        geographies: list[str] | None = None,
        correlation_pipeline_id: UUID | None = None,
    ) -> tuple[list[ActiveThreat], int]:
        """One page of threats. ``correlation_pipeline_id`` narrows it to that pipeline's
        monitored set via :meth:`monitored_clause` (D8), leaving every other filter untouched."""
        query = select(ActiveThreat).where(ActiveThreat.is_hidden.is_(False))
        if correlation_pipeline_id is not None:
            query = query.where(self.monitored_clause(correlation_pipeline_id))

        if search:
            search_clause = self._search_clause(search, search_fields)
            if search_clause is not None:
                query = query.where(search_clause)

        if severity:
            query = query.where(func.lower(ActiveThreat.severity).in_([s.lower() for s in severity]))
        if industries:
            query = query.where(ActiveThreat.industries.op("&&")(list(industries)))
        if threat_types:
            query = query.where(ActiveThreat.threat_types.op("&&")(list(threat_types)))
        if geographies:
            query = query.where(ActiveThreat.geographies.op("&&")(list(geographies)))

        count_query = select(func.count()).select_from(ActiveThreat)
        if query.whereclause is not None:
            count_query = count_query.where(query.whereclause)
        total_result = await self.session.execute(count_query)

        sort_column = self._resolve_sort_column(sort)
        if sort_column is not None:
            descending = order.lower() == "desc"
            if self._sort_is_inverted(sort):
                descending = not descending
            query = query.order_by(sort_column.desc() if descending else sort_column.asc())
        else:
            query = query.order_by(ActiveThreat.tdm_id.asc())

        query = query.offset(skip).limit(limit)
        result = await self.session.execute(query)
        threats = list(result.scalars().all())
        await self._hydrate_rule_check_results(threats)
        return threats, total_result.scalar() or 0

    async def _hydrate_rule_check_results(self, threats: list[ActiveThreat]) -> None:
        active_threat_ids = [threat.tdm_id for threat in threats]
        if not active_threat_ids:
            return
        grouped = await HighOrderSigmaRuleCheckFailureDAO(self.session).get_grouped_results_by_active_threat_ids(
            active_threat_ids
        )
        for threat in threats:
            threat.rule_check_results = grouped.get(
                threat.tdm_id,
                {
                    "unsupported_rules": [],
                    "not_connected_rules": [],
                    "disabled_rules": [],
                },
            )

    async def get_visible_blocked_state(self, threat_ids: list[str]) -> dict[str, bool]:
        """``{tdm_id: is_blocked}`` for the non-hidden rows among ``threat_ids``.

        One query serving the bulk routes' three-way per-item outcome: absent = ``not_found``,
        ``True`` = ``blocked`` (skipped), ``False`` = eligible for an exclusion change.
        """
        if not threat_ids:
            return {}
        result = await self.session.execute(
            select(ActiveThreat.tdm_id, ActiveThreat.is_blocked).where(
                ActiveThreat.tdm_id.in_(threat_ids),
                ActiveThreat.is_hidden.is_(False),
            )
        )
        return {row[0]: bool(row[1]) for row in result.all()}

    async def set_blocked(self, threat_id: str, blocked: bool) -> bool:
        """Set the global block flag. Returns ``False`` when the row is already in that state."""
        result = await self.session.execute(
            update(ActiveThreat)
            .where(
                ActiveThreat.tdm_id == threat_id,
                ActiveThreat.is_blocked.is_not(blocked),
            )
            .values(is_blocked=blocked, updated=func.now())
        )
        await self.session.flush()
        return bool(result.rowcount)

    # -------------------------------------------------------------------------
    # Per-pipeline exclusions (D8 deny-list)
    # -------------------------------------------------------------------------

    async def get_excluded_pipeline_ids_by_threat_ids(self, threat_ids: list[str]) -> dict[str, set[UUID]]:
        """``{tdm_id: {correlation_pipeline_id, ...}}`` for a whole page, in ONE grouped query.

        The reverse lookup behind the list view's ``Pipelines`` column and behind the cascade
        capture in the catalogue sync. Threats with no exclusion row are absent from the mapping.
        """
        if not threat_ids:
            return {}
        result = await self.session.execute(
            select(
                CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id,
                CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id,
            ).where(CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id.in_(threat_ids))
        )
        grouped: dict[str, set[UUID]] = {}
        for threat_id, correlation_pipeline_id in result.all():
            grouped.setdefault(threat_id, set()).add(correlation_pipeline_id)
        return grouped

    async def get_excluded_threat_ids_by_pipeline_ids(
        self, correlation_pipeline_ids: Iterable[UUID]
    ) -> dict[UUID, list[str]]:
        """``{correlation_pipeline_id: [tdm_id, ...]}`` — the deny-list as it goes on the wire.

        One grouped query for N pipelines: this feeds ``excluded_active_threat_ids`` on each
        pipeline's compacted chains-config envelope, and the SSE tick republishes many at once.
        Pipelines with no exclusions are absent from the mapping (absent means monitored).
        """
        ids = list(correlation_pipeline_ids)
        if not ids:
            return {}
        result = await self.session.execute(
            select(
                CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id,
                CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id,
            )
            .where(CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id.in_(ids))
            .order_by(CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id.asc())
        )
        grouped: dict[UUID, list[str]] = {}
        for correlation_pipeline_id, threat_id in result.all():
            grouped.setdefault(correlation_pipeline_id, []).append(threat_id)
        return grouped

    async def get_pipeline_ids_with_exclusion(self, threat_id: str) -> list[UUID]:
        """Correlation pipelines that currently exclude one threat (sorted, for stable output)."""
        grouped = await self.get_excluded_pipeline_ids_by_threat_ids([threat_id])
        return sorted(grouped.get(threat_id, set()), key=str)

    async def set_excluded(
        self,
        threat_ids: list[str],
        correlation_pipeline_ids: list[UUID],
        excluded: bool,
        user_id: UUID | None,
    ) -> int:
        """Add or remove exclusion rows for the full (threat × pipeline) cross product.

        One bulk statement either way — never a loop. Insert is ``ON CONFLICT DO NOTHING`` so
        re-excluding an already-excluded threat is a no-op rather than an error.

        Returns:
            Number of rows actually inserted or deleted; 0 means nothing changed, which is what
            the callers use to decide whether a chains-config republish is owed.
        """
        if not threat_ids or not correlation_pipeline_ids:
            return 0
        if excluded:
            stmt = (
                pg_insert(CorrelationPipelineExcludedActiveThreat)
                .values(
                    [
                        {
                            "correlation_pipeline_id": correlation_pipeline_id,
                            "active_threat_tdm_id": threat_id,
                            "excluded_by": user_id,
                        }
                        for correlation_pipeline_id in correlation_pipeline_ids
                        for threat_id in threat_ids
                    ]
                )
                .on_conflict_do_nothing()
            )
        else:
            stmt = delete(CorrelationPipelineExcludedActiveThreat).where(
                CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id.in_(correlation_pipeline_ids),
                CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id.in_(threat_ids),
            )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount or 0

    async def get_changed_pipeline_ids(
        self, threat_ids: list[str], correlation_pipeline_ids: list[UUID], excluded: bool
    ) -> list[UUID]:
        """Which of ``correlation_pipeline_ids`` this exclusion change would actually move.

        Call BEFORE :meth:`set_excluded`: it is the republish set (hard constraint 5 — republish
        exactly the affected pipelines, not all and not none). Excluding: pipelines missing at
        least one of the rows. Un-excluding: pipelines holding at least one of them.
        """
        if not threat_ids or not correlation_pipeline_ids:
            return []
        stmt = select(CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id).where(
            CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id.in_(correlation_pipeline_ids),
            CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id.in_(threat_ids),
        )
        if excluded:
            # Unchanged only when the pipeline already excludes EVERY requested threat.
            stmt = stmt.group_by(CorrelationPipelineExcludedActiveThreat.correlation_pipeline_id).having(
                func.count(func.distinct(CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id))
                == len(set(threat_ids))
            )
        else:
            stmt = stmt.distinct()
        rows = {row[0] for row in (await self.session.execute(stmt)).all()}
        if excluded:
            return [pid for pid in correlation_pipeline_ids if pid not in rows]
        return [pid for pid in correlation_pipeline_ids if pid in rows]

    async def clear_exclusions_for_threat(self, threat_id: str) -> list[UUID]:
        """Drop every exclusion of one threat and return the pipelines that held one.

        That list *is* the republish set for an unblock: those pipelines' deny-lists just shrank.
        """
        affected = await self.get_pipeline_ids_with_exclusion(threat_id)
        if not affected:
            return []
        await self.session.execute(
            delete(CorrelationPipelineExcludedActiveThreat).where(
                CorrelationPipelineExcludedActiveThreat.active_threat_tdm_id == threat_id
            )
        )
        await self.session.flush()
        return affected

    async def get_existing_tdm_ids(self, tdm_ids: list[str]) -> set[str]:
        """Return which of the given TDM ids already exist in ``active_threats``."""
        if not tdm_ids:
            return set()
        result = await self.session.execute(select(ActiveThreat.tdm_id).where(ActiveThreat.tdm_id.in_(tdm_ids)))
        return {row[0] for row in result.all()}

    async def get_tdm_updated_by_ids(self, tdm_ids: list[str]) -> dict[str, datetime | None]:
        """Return ``{tdm_id: tdm_updated}`` for the given ids that exist locally."""
        if not tdm_ids:
            return {}
        result = await self.session.execute(
            select(ActiveThreat.tdm_id, ActiveThreat.tdm_updated).where(ActiveThreat.tdm_id.in_(tdm_ids))
        )
        return dict(result.tuples().all())

    async def get_all_tdm_ids(self) -> set[str]:
        """Return all Active Threat ids currently stored in ``active_threats``."""
        result = await self.session.execute(select(ActiveThreat.tdm_id))
        return {row[0] for row in result.all()}

    async def upsert_many(self, rows: list[dict[str, Any]]) -> list[str]:
        """Upsert ActiveThreat rows by ``tdm_id``. Rows must include ``tdm_id``.

        Every row reaching this method comes from a successful TDM read, so the
        AT is by definition not missing from TDM right now. To make reappearance
        unhide automatic, both the update and insert branches force
        ``is_hidden=False`` regardless of the row payload (any
        ``is_hidden`` value in ``rows`` is overwritten). This is the only writer
        that flips ``is_hidden`` back to ``False``; ``set_hidden_by_tdm_ids`` is
        the only writer that flips it to ``True``.

        Returns the list of TDM ids that transitioned from ``is_hidden=True`` to
        ``is_hidden=False`` (i.e. were hidden locally but reappeared in TDM). Callers
        can use this to republish dependent objects (e.g. HOS rows) whose Kafka
        visibility depends on the AT becoming non-hidden again.
        """
        if not rows:
            return []
        tdm_ids = [row["tdm_id"] for row in rows if row.get("tdm_id")]
        if not tdm_ids:
            return []
        hidden_before_result = await self.session.execute(
            select(ActiveThreat.tdm_id).where(
                ActiveThreat.tdm_id.in_(tdm_ids),
                ActiveThreat.is_hidden.is_(True),
            )
        )
        hidden_before_ids = {row[0] for row in hidden_before_result.all()}
        existing_ids = await self.get_existing_tdm_ids(tdm_ids)
        for row in rows:
            tid = row.get("tdm_id")
            if not tid:
                continue
            if tid in existing_ids:
                update_data = {k: v for k, v in row.items() if k != "tdm_id"}
                update_data["is_hidden"] = False
                if update_data:
                    await self.session.execute(
                        update(ActiveThreat).where(ActiveThreat.tdm_id == tid).values(**update_data)
                    )
            else:
                # No monitoring default to force: D8 is opt-out, so a new threat is monitored
                # everywhere by virtue of ``is_blocked`` defaulting to false and owning no
                # exclusion rows. The pre-D8 ``enabled = True`` special case is gone.
                insert_data = dict(row)
                insert_data["is_hidden"] = False
                self.session.add(ActiveThreat(**insert_data))
        await self.session.flush()
        return sorted(hidden_before_ids)

    async def set_hidden_by_tdm_ids(self, tdm_ids: list[str], hidden: bool) -> list[str]:
        """Mark Active Threat rows hidden/unhidden and return ids that were updated."""
        if not tdm_ids:
            return []
        result = await self.session.execute(
            select(ActiveThreat.tdm_id).where(
                ActiveThreat.tdm_id.in_(tdm_ids),
                ActiveThreat.is_hidden.is_not(hidden),
            )
        )
        changed_ids = [row[0] for row in result.all()]
        if not changed_ids:
            return []
        await self.session.execute(
            update(ActiveThreat)
            .where(ActiveThreat.tdm_id.in_(changed_ids))
            .values(is_hidden=hidden, updated=func.now())
        )
        await self.session.flush()
        return changed_ids

    async def delete_by_tdm_ids(self, tdm_ids: list[str]) -> int:
        if not tdm_ids:
            return 0
        result = await self.session.execute(delete(ActiveThreat).where(ActiveThreat.tdm_id.in_(tdm_ids)))
        await self.session.flush()
        return result.rowcount or 0

    async def get_ids_older_than_tdm_created(self, cutoff: datetime) -> list[str]:
        """Active Threat ids retention is about to delete, without deleting them.

        Split out so the caller can capture what the ``ON DELETE CASCADE`` will take with them
        (per-pipeline monitoring exclusions) before the rows are gone.
        """
        result = await self.session.execute(
            select(ActiveThreat.tdm_id).where(
                ActiveThreat.tdm_created.is_not(None),
                ActiveThreat.tdm_created < cutoff,
            )
        )
        return [row[0] for row in result.all()]
