"""DAO for attack chains: correlation configuration singleton, chain rows and parked poison records."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import delete, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.core.exceptions import ChainDependencyMissingError
from apps.core.models import (
    ActiveThreat,
    AttackChain,
    AttackChainConfiguration,
    AttackChainEvent,
    ChainsAnalysedDeadLetter,
    CorrelationPipeline,
    HighOrderSigma,
    HighOrderSigmaRuleRef,
)
from apps.core.schemas import ChainsAnalysedEvent, ChainsAnalysedMessage

_FILTERED_OUT_LLM_VERDICTS = frozenset({"FALSE_POSITIVE", "INSUFFICIENT_CONTEXT"})
_STICKY_CHAIN_STATUSES = frozenset({"new", "investigating"})
_ChainsAnalysedUpdateMode = Literal["full", "skip"]


@dataclass(frozen=True)
class _ChainsAnalysedUpdatePlan:
    mode: _ChainsAnalysedUpdateMode
    status: str | None = None


_CHAIN_LIST_SORTABLE = frozenset(
    {
        "last_seen",
        "first_seen",
        "created_at",
        "updated_at",
        "hostname",
        "status",
        "event_count",
        "rule_count",
        "coverage",
        "window_start",
        "chain_type",
        "revision",
    }
)


class AttackChainsDAO:
    """Data access for attack chain data: ``attack_chain_configuration`` singleton and ``attack_chains`` rows."""

    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def _calculate_match_percent(seqs: list[Any]) -> int:
        if not seqs:
            return 0

        hit_count = sum(1 for seq in seqs if isinstance(seq, dict) and seq.get("hit") is True)
        return int((Decimal(hit_count * 100) / Decimal(len(seqs))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    @staticmethod
    def _coverage_from_seqs(seqs: list[Any]) -> Decimal:
        """Coverage as percent 0–100 — same scale as Kafka ``coverage`` and ``match``."""
        return Decimal(AttackChainsDAO._calculate_match_percent(seqs))

    @staticmethod
    def _union_seq_hits(stored: list[Any] | None, incoming: list[Any] | None) -> list[Any]:
        """Union ``hit: true`` across stored and incoming seqs, preserving first-seen order."""
        merged: dict[Any, dict[str, Any]] = {}
        order: list[Any] = []
        for source in (stored or [], incoming or []):
            for item in source:
                if not isinstance(item, dict):
                    continue
                key = item.get("seq")
                existing = merged.get(key)
                if existing is None:
                    merged[key] = dict(item)
                    order.append(key)
                elif item.get("hit") is True:
                    existing["hit"] = True
        return [merged[key] for key in order]

    @staticmethod
    def _resolve_initial_status(status: str | None, llm_verdict: str | None) -> str:
        if llm_verdict is not None and llm_verdict.strip().upper() in _FILTERED_OUT_LLM_VERDICTS:
            return "filtered-out"
        return status if status is not None else "new"

    @staticmethod
    def _resolve_incoming_status(msg: ChainsAnalysedMessage) -> str:
        return AttackChainsDAO._resolve_initial_status(msg.status, msg.llm_verdict)

    @staticmethod
    def _resolve_update_plan(stored_status: str, incoming_status: str) -> _ChainsAnalysedUpdatePlan:
        stored = stored_status.lower()
        incoming = incoming_status.lower()

        if stored in {"new", "investigating"} and incoming == "filtered-out":
            return _ChainsAnalysedUpdatePlan(mode="skip")

        if stored == "filtered-out" and incoming == "new":
            return _ChainsAnalysedUpdatePlan(mode="full", status="new")
        if stored == "filtered-out" and incoming == "filtered-out":
            return _ChainsAnalysedUpdatePlan(mode="full", status="filtered-out")
        if stored == "new" and incoming == "new":
            return _ChainsAnalysedUpdatePlan(mode="full", status="new")
        if stored == "investigating" and incoming == "new":
            return _ChainsAnalysedUpdatePlan(mode="full", status="investigating")
        if stored == "investigating" and incoming == "investigating":
            return _ChainsAnalysedUpdatePlan(mode="full", status="investigating")

        return _ChainsAnalysedUpdatePlan(mode="full", status=stored)

    @staticmethod
    def _merge_max_int(stored: int, incoming: int) -> int:
        return max(stored, incoming)

    @staticmethod
    def _merge_min_datetime(stored: datetime, incoming: datetime) -> datetime:
        return min(stored, incoming)

    @staticmethod
    def _merge_max_datetime(stored: datetime, incoming: datetime) -> datetime:
        return max(stored, incoming)

    @staticmethod
    def _dedupe_chain_events(events: list[ChainsAnalysedEvent]) -> dict[str, ChainsAnalysedEvent]:
        # Last-write-wins: Postgres ON CONFLICT cannot affect the same row twice in one INSERT.
        return {event.event_id: event for event in events}

    async def get_configuration_singleton(self) -> AttackChainConfiguration | None:
        """DEPRECATED — **no reader is left in the application**; kept for the rollback window.

        The runtime stopped reading this row when the ``Default`` correlation pipeline migration
        landed, and ``GET`` / ``PATCH /correlation_job/configuration`` — its last two readers —
        were rebound to that pipeline. The table survives one release as the only record of the
        pre-migration ``correlation_window_seconds`` / ``monitored_topics`` and as the cheapest
        rollback source; these two accessors survive with it and go when it is dropped.
        """
        result = await self.session.execute(select(AttackChainConfiguration).where(AttackChainConfiguration.id == 1))
        return result.scalar_one_or_none()

    async def ensure_configuration_singleton(self) -> AttackChainConfiguration:
        """DEPRECATED — see :meth:`get_configuration_singleton`. No caller writes this row now."""
        row = await self.get_configuration_singleton()
        if row is not None:
            return row
        row = AttackChainConfiguration(
            id=1,
            threshold_pct=60,
            correlation_window_seconds=300,
            monitored_topics=[],
            auto_add_new_pipelines=False,
            jobs_status="stopped",
        )
        self.session.add(row)
        await self.session.flush()
        await self.session.refresh(row)
        return row

    async def get_graph_daily_series(self) -> list[tuple[date, int]]:
        """Return daily UTC aggregates for correlation graph data.

        Tuple shape:
            (day_date, attack_chains_count)
        """
        day_col = func.date(func.timezone("UTC", AttackChain.created_at)).label("day")
        stmt = (
            select(
                day_col,
                func.count(AttackChain.id).label("attack_chains"),
            )
            .group_by(day_col)
            .order_by(day_col.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.all())

    async def get_graph_affected_hostnames_daily_series(self) -> list[tuple[date, int]]:
        """Return daily UTC aggregates for correlation graph data.

        Tuple shape:
            (day_date, affected_hostnames_count)
        """
        day_col = func.date(func.timezone("UTC", AttackChain.created_at)).label("day")
        stmt = (
            select(day_col, func.count(func.distinct(AttackChain.hostname)).label("count"))
            .group_by(day_col)
            .order_by(day_col.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.all())

    async def get_graph_tagged_events_daily_series(self) -> list[tuple[date, int]]:
        """Return daily UTC tagged event counts from attack-chain events."""
        day_col = func.date(func.timezone("UTC", AttackChainEvent.timestamp)).label("day")
        stmt = select(day_col, func.count(AttackChainEvent.id).label("count")).group_by(day_col).order_by(day_col.asc())
        result = await self.session.execute(stmt)
        return list(result.all())

    async def get_graph_matched_rules_daily_series(self) -> list[tuple[date, int]]:
        """Return daily UTC counts of unique matched Sigma rules from attack-chain events."""
        day_col = func.date(func.timezone("UTC", AttackChainEvent.timestamp)).label("day")
        stmt = (
            select(day_col, func.count(func.distinct(AttackChainEvent.sigma_rule_id)).label("count"))
            .group_by(day_col)
            .order_by(day_col.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.all())

    async def get_attack_chains_count(self) -> int:
        result = await self.session.execute(select(func.count(AttackChain.id)))
        return int(result.scalar() or 0)

    async def get_affected_hostnames_count(self) -> int:
        result = await self.session.execute(select(func.count(func.distinct(AttackChain.hostname))))
        return int(result.scalar() or 0)

    async def get_tagged_events_count(self) -> int:
        result = await self.session.execute(select(func.count(AttackChainEvent.id)))
        return int(result.scalar() or 0)

    async def get_matched_rules_counts(self) -> tuple[int, int]:
        matched_result = await self.session.execute(select(func.count(func.distinct(AttackChainEvent.sigma_rule_id))))
        total_result = await self.session.execute(select(func.count(func.distinct(HighOrderSigmaRuleRef.sigma_uuid))))
        return int(matched_result.scalar() or 0), int(total_result.scalar() or 0)

    async def get_chain_status_counts_by_active_threat_and_pipeline(
        self, active_threat_ids: list[str]
    ) -> dict[tuple[str, UUID], dict[str, int]]:
        """Chains grouped by ``(active_threat_id, correlation_pipeline_id, status)``. One query.

        Backs the threat detail's per-pipeline chain-status bar. One statement for every threat and
        every pipeline at once — the per-pipeline breakdown must never become a query per row.
        Served by ``ix_attack_chains_at_cp_status``.

        Chains with a NULL ``correlation_pipeline_id`` (pre-epic rows, or rows orphaned by
        ``ON DELETE SET NULL``) are excluded — they belong to no pipeline column.
        """
        if not active_threat_ids:
            return {}
        stmt = (
            select(
                AttackChain.active_threat_id,
                AttackChain.correlation_pipeline_id,
                AttackChain.status,
                func.count(AttackChain.id).label("count"),
            )
            .where(
                AttackChain.active_threat_id.in_(active_threat_ids),
                AttackChain.correlation_pipeline_id.is_not(None),
            )
            .group_by(AttackChain.active_threat_id, AttackChain.correlation_pipeline_id, AttackChain.status)
        )
        counts: dict[tuple[str, UUID], dict[str, int]] = {}
        for active_threat_id, correlation_pipeline_id, status, count in (await self.session.execute(stmt)).all():
            counts.setdefault((active_threat_id, correlation_pipeline_id), {})[str(status)] = int(count)
        return counts

    async def get_chain_seen_window_by_active_threat_and_pipeline(
        self, active_threat_ids: list[str]
    ) -> dict[tuple[str, UUID], tuple[datetime, datetime]]:
        """``MIN(first_seen)`` / ``MAX(last_seen)`` of this threat's chains in each pipeline.

        Same scope as the status-bar aggregate: unattributed (NULL ``correlation_pipeline_id``)
        chains are excluded. A pipeline with no chains for the threat is absent from the mapping
        — the caller treats that as null timestamps.
        """
        if not active_threat_ids:
            return {}
        stmt = (
            select(
                AttackChain.active_threat_id,
                AttackChain.correlation_pipeline_id,
                func.min(AttackChain.first_seen),
                func.max(AttackChain.last_seen),
            )
            .where(
                AttackChain.active_threat_id.in_(active_threat_ids),
                AttackChain.correlation_pipeline_id.is_not(None),
            )
            .group_by(AttackChain.active_threat_id, AttackChain.correlation_pipeline_id)
        )
        windows: dict[tuple[str, UUID], tuple[datetime, datetime]] = {}
        for active_threat_id, correlation_pipeline_id, first_seen, last_seen in (
            await self.session.execute(stmt)
        ).all():
            windows[(active_threat_id, correlation_pipeline_id)] = (first_seen, last_seen)
        return windows

    async def get_distinct_chain_severities(self) -> list[str]:
        result = await self.session.execute(
            select(ActiveThreat.severity)
            .join(AttackChain, AttackChain.active_threat_id == ActiveThreat.tdm_id)
            .distinct()
            .where(ActiveThreat.severity.is_not(None))
            .order_by(ActiveThreat.severity)
        )
        return [row[0] for row in result.fetchall()]

    async def get_distinct_chain_statuses(self) -> list[str]:
        result = await self.session.execute(
            select(AttackChain.status).distinct().where(AttackChain.status.is_not(None)).order_by(AttackChain.status)
        )
        return [row[0] for row in result.fetchall()]

    def _chains_list_select(self):
        """Row shape: ``(chain, hos_name, threat_name, threat_severity, correlation_pipeline_name)``.

        The correlation pipeline is OUTER-joined and its name selected here on purpose: the list
        converter renders it (and the one-element ``correlation_pipelines`` array) per row, and
        ``AttackChainConverter.to_list_item`` receives a tuple — a lazy relationship would be one
        extra query per row.
        """
        return (
            select(
                AttackChain,
                HighOrderSigma.name,
                ActiveThreat.name,
                ActiveThreat.severity,
                CorrelationPipeline.name,
            )
            .join(HighOrderSigma, AttackChain.hos_rule_id == HighOrderSigma.id)
            .join(ActiveThreat, AttackChain.active_threat_id == ActiveThreat.tdm_id)
            .outerjoin(CorrelationPipeline, AttackChain.correlation_pipeline_id == CorrelationPipeline.id)
        )

    def _apply_chain_list_search(self, stmt, search: str | None):
        if not search or not search.strip():
            return stmt
        term = f"%{search.strip()}%"
        # Hit techniques live on events, not the chain row. EXISTS avoids duplicating list rows.
        technique_hit = exists(
            select(1).where(
                AttackChainEvent.chain_id == AttackChain.id,
                func.array_to_string(AttackChainEvent.technique_ids, " ").ilike(term),
            )
        )
        return stmt.where(
            or_(
                AttackChain.hostname.ilike(term),
                AttackChain.username.ilike(term),
                AttackChain.status.ilike(term),
                AttackChain.active_threat_id.ilike(term),
                AttackChain.chain_type.ilike(term),
                HighOrderSigma.name.ilike(term),
                ActiveThreat.name.ilike(term),
                ActiveThreat.severity.ilike(term),
                technique_hit,
            )
        )

    def _apply_chain_list_filters(
        self,
        stmt,
        severity: list[str] | None,
        status: list[str] | None,
        show_filtered_out: bool = True,
        correlation_pipeline_id: UUID | None = None,
    ):
        if severity:
            stmt = stmt.where(func.lower(ActiveThreat.severity).in_([s.lower() for s in severity]))
        if status:
            stmt = stmt.where(func.lower(AttackChain.status).in_([s.lower() for s in status]))
        if not show_filtered_out:
            stmt = stmt.where(func.lower(AttackChain.status) != "filtered-out")
        if correlation_pipeline_id is not None:
            # NULL (deleted pipeline / legacy chain) is deliberately excluded by the scoped view:
            # an unattributed chain belongs to no tenant and is only listed platform-wide.
            stmt = stmt.where(AttackChain.correlation_pipeline_id == correlation_pipeline_id)
        return stmt

    async def count_filtered_out_chains(self, correlation_pipeline_id: UUID | None = None) -> int:
        """Hidden ``filtered-out`` chains, scoped exactly like the list they are hidden from.

        Scoping it matters: an unscoped count would tell one tenant how many chains ANOTHER
        tenant's pipeline hid.
        """
        stmt = select(func.count(AttackChain.id)).where(func.lower(AttackChain.status) == "filtered-out")
        if correlation_pipeline_id is not None:
            stmt = stmt.where(AttackChain.correlation_pipeline_id == correlation_pipeline_id)
        result = await self.session.execute(stmt)
        return int(result.scalar() or 0)

    def _resolve_chain_group_key_column(self, group_by: Literal["hostname", "chain_name"]):
        if group_by == "hostname":
            return AttackChain.hostname
        return HighOrderSigma.name

    @staticmethod
    def _resolve_chain_sort_column(sort: str | None):
        if sort == "severity":
            return ActiveThreat.severity
        order_col = AttackChain.last_seen
        if sort and sort in _CHAIN_LIST_SORTABLE:
            order_col = getattr(AttackChain, sort)
        return order_col

    async def list_chains(
        self,
        skip: int,
        limit: int,
        search: str | None,
        sort: str | None,
        order: str,
        severity: list[str] | None = None,
        status: list[str] | None = None,
        show_filtered_out: bool = False,
        correlation_pipeline_id: UUID | None = None,
    ) -> tuple[list[tuple[AttackChain, str, str, str | None, str | None]], int]:
        base = self._chains_list_select()
        base = self._apply_chain_list_search(base, search)
        base = self._apply_chain_list_filters(base, severity, status, show_filtered_out, correlation_pipeline_id)
        count_stmt = select(func.count()).select_from(base.subquery())
        total_result = await self.session.execute(count_stmt)
        total = int(total_result.scalar() or 0)

        order_col = self._resolve_chain_sort_column(sort)
        if order.lower() == "desc":
            base = base.order_by(order_col.desc())
        else:
            base = base.order_by(order_col.asc())

        base = base.offset(skip).limit(limit)
        result = await self.session.execute(base)
        return result.all(), total

    async def list_chains_grouped(
        self,
        skip: int,
        limit: int,
        search: str | None,
        sort: str | None,
        order: str,
        group_by: Literal["hostname", "chain_name"],
        severity: list[str] | None = None,
        status: list[str] | None = None,
        show_filtered_out: bool = False,
        correlation_pipeline_id: UUID | None = None,
    ) -> tuple[dict[str, list[tuple[AttackChain, str, str, str | None, str | None]]], int]:
        key_col = self._resolve_chain_group_key_column(group_by)
        order_col = self._resolve_chain_sort_column(sort)
        key_label = key_col.label("group_key")

        base = self._chains_list_select()
        base = self._apply_chain_list_search(base, search)
        base = self._apply_chain_list_filters(base, severity, status, show_filtered_out, correlation_pipeline_id)

        keys_subquery = base.with_only_columns(key_label).distinct().subquery()
        paged_keys_stmt = select(
            keys_subquery.c.group_key,
            func.count().over().label("total_groups"),
        )

        if order.lower() == "desc":
            paged_keys_stmt = paged_keys_stmt.order_by(keys_subquery.c.group_key.desc())
            key_order_clause = key_col.desc()
            row_order_clause = order_col.desc()
        else:
            paged_keys_stmt = paged_keys_stmt.order_by(keys_subquery.c.group_key.asc())
            key_order_clause = key_col.asc()
            row_order_clause = order_col.asc()

        paged_keys_stmt = paged_keys_stmt.offset(skip).limit(limit)
        paged_keys_result = await self.session.execute(paged_keys_stmt)
        paged_keys = paged_keys_result.fetchall()
        if not paged_keys:
            total_stmt = select(func.count()).select_from(keys_subquery)
            total_result = await self.session.execute(total_stmt)
            return {}, int(total_result.scalar() or 0)

        selected_keys = [row[0] for row in paged_keys]
        total_groups = int(paged_keys[0][1] or 0)

        rows_stmt = base.where(key_col.in_(selected_keys)).order_by(key_order_clause, row_order_clause)
        rows_result = await self.session.execute(rows_stmt)

        grouped: dict[str, list[tuple[AttackChain, str, str, str | None, str | None]]] = {
            str(k): [] for k in selected_keys
        }
        for row in rows_result.all():
            chain, hos_rule_name, _threat_name, _threat_severity, _correlation_pipeline_name = row
            key_value = chain.hostname if group_by == "hostname" else hos_rule_name
            key = str(key_value)
            grouped[key].append(row)

        return grouped, total_groups

    async def get_chain_by_id(self, chain_id: UUID) -> AttackChain | None:
        stmt = (
            select(AttackChain)
            .where(AttackChain.id == chain_id)
            .options(
                selectinload(AttackChain.hos_rule),
                selectinload(AttackChain.active_threat),
                selectinload(AttackChain.events),
                # The detail row carries the producing correlation pipeline (id + name); eager-load
                # it so the converter never triggers a lazy fetch.
                selectinload(AttackChain.correlation_pipeline),
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_chain_link_context(self, chain_id: UUID) -> tuple[str, datetime, datetime] | None:
        """Lightweight ``(hostname, first_seen, last_seen)`` fetch for SIEM deep-link building.

        Avoids loading event rows / ``raw_event`` JSONB. Returns ``None`` when the chain is missing.
        """
        stmt = select(AttackChain.hostname, AttackChain.first_seen, AttackChain.last_seen).where(
            AttackChain.id == chain_id
        )
        row = (await self.session.execute(stmt)).first()
        return (row[0], row[1], row[2]) if row else None

    async def get_distinct_sigma_rule_ids(self, chain_id: UUID) -> list[str]:
        """Distinct matched Sigma rule UUIDs across a chain's events (sorted, as strings)."""
        stmt = select(AttackChainEvent.sigma_rule_id).where(AttackChainEvent.chain_id == chain_id).distinct()
        rows = (await self.session.execute(stmt)).scalars().all()
        return sorted(str(rid) for rid in rows)

    async def _resolve_hos_rule_fk(self, hos_rule_id: str) -> UUID | None:
        """Map Kafka ``hos_rule_id`` to ``high_order_sigmas.id``.

        Producers may send either the DB primary key or the TDM HoS identifier stored in
        ``high_order_sigmas.tdm_id``.
        """
        key = str(hos_rule_id).lower()
        conditions = [func.lower(HighOrderSigma.tdm_id) == key]
        try:
            conditions.append(HighOrderSigma.id == UUID(str(hos_rule_id)))
        except ValueError:
            pass

        stmt = select(HighOrderSigma.id).where(or_(*conditions)).limit(1)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update_chain_status(self, chain_id: UUID, status: str) -> AttackChain | None:
        result = await self.session.execute(select(AttackChain).where(AttackChain.id == chain_id))
        row = result.scalar_one_or_none()
        if row is None:
            return None
        row.status = status
        await self.session.flush()
        return await self.get_chain_by_id(chain_id)

    async def get_sticky_chain(
        self,
        hos_rule_id: UUID,
        hostname: str,
        correlation_pipeline_id: UUID | None,
    ) -> AttackChain | None:
        """Return the sticky-open chain for ``(pipeline, hos_rule_id, hostname)``, if any.

        Sticky statuses are ``new`` and ``investigating`` only. ``filtered-out`` / ``closed`` /
        analyst-terminal statuses are not sticky: a later window INSERTs a new row rather than
        resurrecting a hidden or closed one.

        ``window_start`` is Flink wire identity only and is not part of this lookup. Isolation
        stays per ``correlation_pipeline_id`` so two customers sharing a hostname and a catalogue
        HoS still get two rows.

        ``is_not_distinct_from`` renders ``IS NOT DISTINCT FROM``, so a legacy/unattributed row
        (NULL pipeline) matches only other unattributed records and never an attributed pipeline.

        When multiple sticky rows exist (legacy duplicates), the row with the latest ``last_seen``
        wins.
        """
        stmt = (
            select(AttackChain)
            .where(
                AttackChain.hos_rule_id == hos_rule_id,
                AttackChain.hostname == hostname,
                AttackChain.correlation_pipeline_id.is_not_distinct_from(correlation_pipeline_id),
                func.lower(AttackChain.status).in_(_STICKY_CHAIN_STATUSES),
            )
            .order_by(AttackChain.last_seen.desc())
            .limit(1)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def _resolve_insert_chain_id(self, proposed_id: UUID) -> UUID:
        """Return ``proposed_id`` for INSERT unless that PK already exists.

        When only a closed (or otherwise inactive) row holds ``proposed_id``, a fresh UUID is
        returned so the new active chain does not collide on ``attack_chains.id``.
        """
        existing = await self.session.get(AttackChain, proposed_id)
        if existing is None:
            return proposed_id
        return uuid4()

    def _prepare_message_fields(self, msg: ChainsAnalysedMessage) -> dict[str, Any]:
        seqs = msg.seqs if msg.seqs else []
        return {
            "coverage": Decimal(str(msg.coverage)),
            "llm_conf": Decimal(str(msg.llm_confidence)) if msg.llm_confidence is not None else None,
            "seqs": seqs,
            "match": self._calculate_match_percent(seqs),
            "hos_tags": list(msg.hos_tags) if msg.hos_tags else None,
            "pipelines": list(msg.pipeline_names) if msg.pipeline_names else [],
        }

    @staticmethod
    def _is_same_wire(row: AttackChain, msg: ChainsAnalysedMessage) -> bool:
        """True when this message is another revision of the last applied wire chain_id.

        ``last_wire_chain_id IS NULL`` is not equal to ``msg.chain_id``, so a post-migrate
        cross-window envelope still merges.
        """
        return row.last_wire_chain_id is not None and row.last_wire_chain_id == msg.chain_id

    def _stamp_applied_wire(self, row: AttackChain, msg: ChainsAnalysedMessage, fields: dict[str, Any]) -> None:
        row.first_seen = self._merge_min_datetime(row.first_seen, msg.first_seen)
        row.last_seen = self._merge_max_datetime(row.last_seen, msg.last_seen)
        row.match = self._merge_max_int(row.match, fields["match"])
        row.revision = msg.revision
        row.analysed_at = msg.analysed_at
        row.last_wire_chain_id = msg.chain_id
        # Never downgrade an attributed row to NULL because one lagging producer omitted the stamp.
        if msg.correlation_pipeline_id is not None:
            row.correlation_pipeline_id = msg.correlation_pipeline_id

    def _apply_unioned_seqs(self, row: AttackChain, fields: dict[str, Any]) -> None:
        unioned = self._union_seq_hits(row.seqs, fields["seqs"])
        row.seqs = unioned
        union_percent = self._calculate_match_percent(unioned)
        row.match = self._merge_max_int(row.match, union_percent)
        row.coverage = max(row.coverage, fields["coverage"], self._coverage_from_seqs(unioned))

    def _apply_full_update(
        self,
        row: AttackChain,
        msg: ChainsAnalysedMessage,
        resolved_hos_id: UUID,
        fields: dict[str, Any],
        new_status: str,
        *,
        same_wire: bool,
    ) -> None:
        self._stamp_applied_wire(row, msg, fields)
        row.status = new_status
        if same_wire:
            row.hos_rule_id = resolved_hos_id
            row.active_threat_id = msg.active_threat_id
            row.chain_type = msg.chain_type
            row.hostname = msg.hostname
            row.username = msg.username
            row.seqs = fields["seqs"]
            row.hos_tags = fields["hos_tags"]
            row.coverage = fields["coverage"]
            row.pipeline_names = fields["pipelines"]
            row.ai_summary = msg.ai_summary
            row.llm_verdict = msg.llm_verdict
            row.llm_confidence = fields["llm_conf"]
            row.llm_review = msg.llm_review
        else:
            self._apply_unioned_seqs(row, fields)

    def _apply_skip_update(
        self,
        row: AttackChain,
        msg: ChainsAnalysedMessage,
        fields: dict[str, Any],
    ) -> None:
        self._stamp_applied_wire(row, msg, fields)
        self._apply_unioned_seqs(row, fields)

    async def apply_chains_analysed(self, msg: ChainsAnalysedMessage) -> bool:
        """Merge enriched chain payload from Kafka.

        Lookup is sticky-open: ``(correlation_pipeline_id, hos_rule_id, hostname)`` among
        ``new`` / ``investigating`` only. ``window_start`` is Flink wire identity
        (``uuid5(NS, "{pipeline}|{hos}|{host}|{window_start}")``, §21) and is not part of the
        lookup — a later tumbling window for the same open investigation merges onto that row
        instead of inserting. ``filtered-out``, ``closed``, and other analyst-terminal statuses
        are not sticky: when no sticky row exists, INSERT uses ``msg.chain_id`` if that PK is
        free, otherwise a new UUID.

        Idempotency is ``(last_wire_chain_id, revision)``, not bare ``revision``. ``revision`` is
        monotonic **per wire chain_id** and restarts at ``1`` every window, so a later window
        with ``revision=1`` must still merge onto an investigating row whose stored revision is
        already ``7``. Skip only when ``row.last_wire_chain_id == msg.chain_id`` **and**
        ``row.revision >= msg.revision``. ``last_wire_chain_id IS NULL`` is not equal to the
        incoming id, so a post-migrate cross-window message still merges.

        ``correlation_pipeline_id`` is stored as resolved by the consumer (envelope stamp, else the
        topic name, else NULL for legacy/unattributed records) and is never downgraded to NULL by a
        later message that omits it. ``window_start`` on UPDATE keeps the first window's value.

        Active-chain updates follow a status matrix (FULL vs partial SKIP). SKIP applies when the
        stored chain is ``new``/``investigating`` and the incoming Kafka status resolves to
        ``filtered-out``: enrichment/LLM fields and ``status`` are preserved, while timestamps,
        ``match`` (max), coverage/seqs/revision/analysed_at/last_wire_chain_id, and events are
        merged onto the sticky row (no sibling ``filtered-out`` INSERT). Same-window FULL (same
        ``last_wire_chain_id``, higher revision) keeps today's overwrite of LLM fields and
        seqs/coverage; cross-window FULL unions ``hit: true`` across seqs instead of replacing.

        Per-event rows in ``attack_chain_events`` are upserted on ``(chain_id, event_id)`` against the
        sticky chain id (not necessarily ``msg.chain_id``). ``event_count`` and ``rule_count`` are
        always recomputed from stored events after upsert (incoming Kafka counts are batch-scoped).

        Returns:
            ``True`` if a row was inserted or updated, ``False`` if skipped as a duplicate delivery.

        Raises:
            ChainDependencyMissingError: no ``high_order_sigmas`` row matches ``msg.hos_rule_id``
                (classified DEFERRABLE by the consumer — catalogue sync may still store it).

        ``msg.hos_rule_id`` may be either ``high_order_sigmas.id`` or ``high_order_sigmas.tdm_id``;
        it is resolved to ``id`` before insert/update.
        """
        resolved_hos_id = await self._resolve_hos_rule_fk(msg.hos_rule_id)
        if resolved_hos_id is None:
            raise ChainDependencyMissingError("high_order_sigmas", f"hos_rule_id={msg.hos_rule_id}")

        fields = self._prepare_message_fields(msg)
        row = await self.get_sticky_chain(resolved_hos_id, msg.hostname, msg.correlation_pipeline_id)

        if row is None:
            chain_id = await self._resolve_insert_chain_id(msg.chain_id)
            inserted = AttackChain(
                id=chain_id,
                hos_rule_id=resolved_hos_id,
                correlation_pipeline_id=msg.correlation_pipeline_id,
                active_threat_id=msg.active_threat_id,
                chain_type=msg.chain_type,
                hostname=msg.hostname,
                username=msg.username,
                status=self._resolve_initial_status(msg.status, msg.llm_verdict),
                first_seen=msg.first_seen,
                last_seen=msg.last_seen,
                window_start=msg.window_start,
                event_count=0,
                rule_count=0,
                seqs=fields["seqs"],
                match=fields["match"],
                hos_tags=fields["hos_tags"],
                coverage=fields["coverage"],
                pipeline_names=fields["pipelines"],
                ai_summary=msg.ai_summary,
                llm_verdict=msg.llm_verdict,
                llm_confidence=fields["llm_conf"],
                llm_review=msg.llm_review,
                revision=msg.revision,
                last_wire_chain_id=msg.chain_id,
                analysed_at=msg.analysed_at,
            )
            self.session.add(inserted)
            await self.session.flush()
            await self._upsert_chain_events(chain_id, msg.events)
            await self._refresh_chain_counts(chain_id, inserted)
            return True

        if row.last_wire_chain_id == msg.chain_id and row.revision >= msg.revision:
            return False

        incoming_status = self._resolve_incoming_status(msg)
        plan = self._resolve_update_plan(row.status, incoming_status)
        same_wire = self._is_same_wire(row, msg)

        if plan.mode == "skip":
            self._apply_skip_update(row, msg, fields)
        else:
            assert plan.status is not None
            self._apply_full_update(row, msg, resolved_hos_id, fields, plan.status, same_wire=same_wire)

        await self.session.flush()
        await self._upsert_chain_events(row.id, msg.events)
        await self._refresh_chain_counts(row.id, row)
        return True

    async def _refresh_chain_counts(self, chain_id: UUID, row: AttackChain | None = None) -> None:
        """Recompute ``event_count`` / ``rule_count`` on a chain from ``attack_chain_events``."""
        stmt = select(
            func.count(AttackChainEvent.id).label("event_count"),
            func.count(func.distinct(AttackChainEvent.sigma_rule_id)).label("rule_count"),
        ).where(AttackChainEvent.chain_id == chain_id)
        counts = (await self.session.execute(stmt)).one()
        event_count = int(counts.event_count or 0)
        rule_count = int(counts.rule_count or 0)

        target = row if row is not None else await self.session.get(AttackChain, chain_id)
        if target is not None:
            target.event_count = event_count
            target.rule_count = rule_count

    async def _upsert_chain_events(self, chain_id: UUID, events: list[ChainsAnalysedEvent]) -> None:
        if not events:
            return
        # Dedup by event_id (last-write-wins): Postgres ON CONFLICT cannot affect the same row
        # twice in one INSERT, so duplicates inside a single Kafka message must be collapsed here.
        deduped: dict[str, ChainsAnalysedEvent] = {}
        for ev in events:
            deduped[ev.event_id] = ev
        rows = [
            {
                "chain_id": chain_id,
                "event_id": ev.event_id,
                "sigma_rule_id": UUID(str(ev.sigma_rule_id)),
                "sigma_rule_title": ev.sigma_rule_title,
                "rule_id": ev.rule_id,
                "seq": ev.seq,
                "timestamp": ev.timestamp,
                "technique_ids": ev.technique_ids,
                "raw_event": ev.raw_event,
            }
            for ev in deduped.values()
        ]
        stmt = pg_insert(AttackChainEvent).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_attack_chain_events_chain_event",
            set_={
                "sigma_rule_id": stmt.excluded.sigma_rule_id,
                "sigma_rule_title": stmt.excluded.sigma_rule_title,
                # Sticky: a later revision that omits ``rule_db_id`` (or sends a malformed one)
                # must not erase a PK we already know for this event.
                "rule_id": func.coalesce(stmt.excluded.rule_id, AttackChainEvent.rule_id),
                "seq": stmt.excluded.seq,
                "timestamp": stmt.excluded.timestamp,
                "technique_ids": stmt.excluded.technique_ids,
                "raw_event": stmt.excluded.raw_event,
            },
        )
        await self.session.execute(stmt)


def truncate_dead_letter_payload(payload: str | None, max_bytes: int) -> tuple[str | None, bool]:
    """Strip NUL bytes (PostgreSQL ``text`` rejects ``\\x00``) and cap the stored payload."""
    if payload is None:
        return None, False
    cleaned = payload.replace("\x00", "")
    encoded = cleaned.encode("utf-8")
    if len(encoded) <= max_bytes:
        return cleaned, False
    return encoded[:max_bytes].decode("utf-8", errors="ignore"), True


class ChainsAnalysedDeadLetterDAO:
    """Data access for ``chains_analysed_dead_letters`` — poison records parked before being skipped.

    The caller owns the transaction: every method flushes, none commits, so parking can share (or
    deliberately not share) a session with whatever else the caller is doing.
    """

    def __init__(self, session: AsyncSession):
        self.session = session

    async def park(
        self,
        *,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
        error_class: str,
        error_type: str,
        error_message: str,
        attempts: int = 1,
        chain_id: UUID | None = None,
        hos_rule_id: str | None = None,
        payload: str | None = None,
        payload_truncated: bool = False,
    ) -> None:
        """Upsert one parked record on ``(topic, partition, offset)``.

        Redelivery of the same offset (rebalance, or a restart before the commit landed) must not
        create a second row: it adds to ``attempts`` and refreshes the error and payload instead.
        """
        stmt = pg_insert(ChainsAnalysedDeadLetter).values(
            kafka_topic=kafka_topic,
            kafka_partition=kafka_partition,
            kafka_offset=kafka_offset,
            chain_id=chain_id,
            hos_rule_id=hos_rule_id,
            error_class=error_class,
            error_type=error_type,
            error_message=error_message,
            attempts=attempts,
            payload=payload,
            payload_truncated=payload_truncated,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_chains_dlq_tp_offset",
            set_={
                "attempts": ChainsAnalysedDeadLetter.attempts + attempts,
                "last_failed_at": func.now(),
                "error_class": stmt.excluded.error_class,
                "error_type": stmt.excluded.error_type,
                "error_message": stmt.excluded.error_message,
                "chain_id": stmt.excluded.chain_id,
                "hos_rule_id": stmt.excluded.hos_rule_id,
                "payload": stmt.excluded.payload,
                "payload_truncated": stmt.excluded.payload_truncated,
            },
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def count_unreplayed(self, since: datetime | None = None) -> int:
        """Count parked records not yet marked replayed, optionally only those parked after ``since``."""
        stmt = select(func.count(ChainsAnalysedDeadLetter.id)).where(ChainsAnalysedDeadLetter.replayed_at.is_(None))
        if since is not None:
            stmt = stmt.where(ChainsAnalysedDeadLetter.last_failed_at >= since)
        return int((await self.session.execute(stmt)).scalar() or 0)

    async def prune(self, older_than_days: int) -> int:
        """Delete parked records older than the retention window. Returns the number deleted."""
        cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
        result = await self.session.execute(
            delete(ChainsAnalysedDeadLetter).where(ChainsAnalysedDeadLetter.created_at < cutoff)
        )
        await self.session.flush()
        return result.rowcount or 0

    async def mark_replayed(self, row_id: UUID) -> bool:
        """Stamp ``replayed_at`` on one parked record. Returns ``False`` when the id is unknown."""
        result = await self.session.execute(
            update(ChainsAnalysedDeadLetter).where(ChainsAnalysedDeadLetter.id == row_id).values(replayed_at=func.now())
        )
        await self.session.flush()
        return bool(result.rowcount)
