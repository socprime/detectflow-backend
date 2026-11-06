from collections.abc import Iterable
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import (
    ActiveThreat,
    ActiveThreatHighOrderSigma,
    HighOrderSigma,
    HighOrderSigmaRuleRef,
)

_HOS_SYNC_FIELDS = frozenset({"name", "body", "level", "tdm_created", "tdm_updated"})


class HighOrderSigmaDAO:
    """Data access for high-order Sigma rows."""

    @staticmethod
    def _hos_sync_row_changed(existing: HighOrderSigma, row: dict[str, Any]) -> bool:
        """Return True when incoming TDM row differs from the stored HOS sync fields."""
        return any(getattr(existing, field) != row.get(field) for field in _HOS_SYNC_FIELDS)

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, sigma_id: UUID) -> HighOrderSigma | None:
        query = select(HighOrderSigma).where(HighOrderSigma.id == sigma_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_tdm_id(self, tdm_id: str) -> HighOrderSigma | None:
        query = select(HighOrderSigma).where(HighOrderSigma.tdm_id == tdm_id)
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_tdm_ids(self, tdm_ids: Iterable[str]) -> list[HighOrderSigma]:
        ids = list(dict.fromkeys(tdm_ids))
        if not ids:
            return []
        result = await self.session.execute(select(HighOrderSigma).where(HighOrderSigma.tdm_id.in_(ids)))
        return list(result.scalars().all())

    async def get_latest_tdm_updated(self) -> datetime | None:
        """Return the most recent TDM update timestamp stored for HOS rows."""
        result = await self.session.execute(select(func.max(HighOrderSigma.tdm_updated)))
        return result.scalar_one_or_none()

    async def get_all_tdm_ids(self) -> set[str]:
        result = await self.session.execute(select(HighOrderSigma.tdm_id))
        return {row[0] for row in result.all()}

    async def get_existing_tdm_ids(self, tdm_ids: Iterable[str]) -> set[str]:
        ids = list(dict.fromkeys(tdm_ids))
        if not ids:
            return set()
        result = await self.session.execute(select(HighOrderSigma.tdm_id).where(HighOrderSigma.tdm_id.in_(ids)))
        return {row[0] for row in result.all()}

    async def get_ids_by_tdm_ids(self, tdm_ids: Iterable[str]) -> dict[str, UUID]:
        ids = list(dict.fromkeys(tdm_ids))
        if not ids:
            return {}
        result = await self.session.execute(
            select(HighOrderSigma.tdm_id, HighOrderSigma.id).where(HighOrderSigma.tdm_id.in_(ids))
        )
        return dict(result.all())

    async def replace_memberships_for_active_threats(self, memberships: dict[str, set[UUID]]) -> None:
        """Replace AT→HOS memberships for the given Active Threats.

        For each Active Threat in ``memberships`` we delete its existing rows in
        ``active_threat_high_order_sigmas`` and insert the provided HOS set.
        Active Threats not in the dict are left alone.
        """
        if not memberships:
            return
        active_threat_ids = list(memberships.keys())
        await self.session.execute(
            delete(ActiveThreatHighOrderSigma).where(ActiveThreatHighOrderSigma.active_threat_id.in_(active_threat_ids))
        )
        for active_threat_id, hos_ids in memberships.items():
            for hos_id in hos_ids:
                self.session.add(
                    ActiveThreatHighOrderSigma(
                        active_threat_id=active_threat_id,
                        high_order_sigma_id=hos_id,
                    )
                )
        await self.session.flush()

    async def delete_orphan_hos(self) -> list[str]:
        """Delete HOS rows that are no longer referenced by any Active Threat and return deleted TDM ids."""
        membership_subq = select(ActiveThreatHighOrderSigma.high_order_sigma_id).where(
            ActiveThreatHighOrderSigma.high_order_sigma_id == HighOrderSigma.id
        )
        ids_result = await self.session.execute(
            select(HighOrderSigma.tdm_id).where(
                ~membership_subq.exists(),
                HighOrderSigma.is_user_edited.is_(False),
            )
        )
        deleted_tdm_ids = [row[0] for row in ids_result.all()]
        if not deleted_tdm_ids:
            return []
        await self.session.execute(delete(HighOrderSigma).where(HighOrderSigma.tdm_id.in_(deleted_tdm_ids)))
        await self.session.flush()
        return deleted_tdm_ids

    async def get_all(
        self,
        skip: int = 0,
        limit: int = 100,
        search: str | None = None,
        search_fields: Iterable[str] | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> tuple[list[HighOrderSigma], int]:
        query = select(HighOrderSigma)

        if search and search_fields:
            search_conditions = []
            for field in search_fields:
                if hasattr(HighOrderSigma, field):
                    search_conditions.append(getattr(HighOrderSigma, field).ilike(f"%{search}%"))
            if search_conditions:
                query = query.where(or_(*search_conditions))

        count_query = select(func.count()).select_from(HighOrderSigma)
        if query.whereclause is not None:
            count_query = count_query.where(query.whereclause)
        total_result = await self.session.execute(count_query)

        if sort and hasattr(HighOrderSigma, sort):
            sort_column = getattr(HighOrderSigma, sort)
            if order.lower() == "desc":
                query = query.order_by(sort_column.desc())
            else:
                query = query.order_by(sort_column.asc())
        else:
            query = query.order_by(HighOrderSigma.name.asc())

        query = query.offset(skip).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all()), total_result.scalar() or 0

    async def upsert_many(self, rows: list[dict[str, Any]]) -> set[str]:
        """Upsert HighOrderSigma rows by ``tdm_id``. Rows must include ``tdm_id``.

        ``is_user_edited=True`` rows are skipped (user edits are not overwritten by sync).
        Unchanged existing rows are left untouched so steady-state sync does not reset
        ``is_kafka_tombstoned`` or trigger needless Kafka republishes.

        Returns:
            TDM ids that were inserted or had sync-field changes applied.
        """
        if not rows:
            return set()
        tdm_ids = [row["tdm_id"] for row in rows if row.get("tdm_id")]
        if not tdm_ids:
            return set()
        result = await self.session.execute(select(HighOrderSigma).where(HighOrderSigma.tdm_id.in_(tdm_ids)))
        existing_by_tdm_id = {row.tdm_id: row for row in result.scalars().all()}
        changed_tdm_ids: set[str] = set()
        for row in rows:
            tid = row.get("tdm_id")
            if not tid:
                continue
            existing = existing_by_tdm_id.get(tid)
            if existing is not None:
                if existing.is_user_edited:
                    continue
                if not self._hos_sync_row_changed(existing, row):
                    continue
                update_data = {k: v for k, v in row.items() if k != "tdm_id"}
                update_data["is_kafka_tombstoned"] = False
                await self.session.execute(
                    update(HighOrderSigma).where(HighOrderSigma.tdm_id == tid).values(**update_data)
                )
                changed_tdm_ids.add(tid)
            else:
                self.session.add(HighOrderSigma(**row))
                changed_tdm_ids.add(tid)
        await self.session.flush()
        return changed_tdm_ids

    async def get_user_edited_tdm_ids(self, tdm_ids: Iterable[str]) -> set[str]:
        ids = list(dict.fromkeys(tdm_ids))
        if not ids:
            return set()
        result = await self.session.execute(
            select(HighOrderSigma.tdm_id).where(
                HighOrderSigma.tdm_id.in_(ids),
                HighOrderSigma.is_user_edited.is_(True),
            )
        )
        return {row[0] for row in result.all()}

    async def delete_by_tdm_ids(self, tdm_ids: list[str]) -> int:
        if not tdm_ids:
            return 0
        result = await self.session.execute(delete(HighOrderSigma).where(HighOrderSigma.tdm_id.in_(tdm_ids)))
        await self.session.flush()
        return result.rowcount or 0

    async def replace_rule_refs(self, high_order_sigma_id: UUID, sigma_uuids: Iterable[str | UUID]) -> None:
        """Replace HOS→Sigma UUID relations for one HOS. Invalid UUID strings are ignored."""
        await self.session.execute(
            delete(HighOrderSigmaRuleRef).where(HighOrderSigmaRuleRef.high_order_sigma_id == high_order_sigma_id)
        )
        normalized: list[UUID] = []
        for value in sigma_uuids:
            if isinstance(value, UUID):
                normalized.append(value)
                continue
            if not isinstance(value, str):
                continue
            try:
                normalized.append(UUID(value))
            except (TypeError, ValueError):
                continue
        unique = list(dict.fromkeys(normalized))
        for sigma_uuid in unique:
            self.session.add(
                HighOrderSigmaRuleRef(
                    high_order_sigma_id=high_order_sigma_id,
                    sigma_uuid=sigma_uuid,
                )
            )
        await self.session.flush()

    async def get_by_sigma_uuids(self, sigma_uuids: Iterable[str | UUID]) -> list[HighOrderSigma]:
        """Return all HOS rows that reference any of the given Sigma UUIDs (de-duplicated)."""
        normalized: list[UUID] = []
        for value in sigma_uuids:
            if isinstance(value, UUID):
                normalized.append(value)
                continue
            if not isinstance(value, str):
                continue
            try:
                normalized.append(UUID(value))
            except (TypeError, ValueError):
                continue
        if not normalized:
            return []
        query = (
            select(HighOrderSigma)
            .join(HighOrderSigmaRuleRef, HighOrderSigmaRuleRef.high_order_sigma_id == HighOrderSigma.id)
            .where(HighOrderSigmaRuleRef.sigma_uuid.in_(normalized))
            .distinct()
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_all_referenced_sigma_uuids(self) -> set[str]:
        result = await self.session.execute(select(HighOrderSigmaRuleRef.sigma_uuid).distinct())
        return {str(row[0]) for row in result.all()}

    async def get_tdm_ids_with_non_hidden_at(self, tdm_ids: Iterable[str] | None = None) -> set[str]:
        """Return HOS TDM ids that have at least one non-hidden Active Threat membership.

        Pass ``tdm_ids`` to constrain the result to a known HOS set (used by the AT
        sync to publish only currently-visible HOS to Kafka).
        """
        query = (
            select(HighOrderSigma.tdm_id)
            .join(
                ActiveThreatHighOrderSigma,
                ActiveThreatHighOrderSigma.high_order_sigma_id == HighOrderSigma.id,
            )
            .join(ActiveThreat, ActiveThreat.tdm_id == ActiveThreatHighOrderSigma.active_threat_id)
            .where(ActiveThreat.is_hidden.is_(False))
            .distinct()
        )
        if tdm_ids is not None:
            ids = list(dict.fromkeys(tdm_ids))
            if not ids:
                return set()
            query = query.where(HighOrderSigma.tdm_id.in_(ids))
        result = await self.session.execute(query)
        return {row[0] for row in result.all()}

    async def get_pending_kafka_tombstone_tdm_ids(self) -> set[str]:
        """Return HOS TDM ids that need a Kafka tombstone but haven't received one yet.

        A HOS qualifies when both conditions hold:
          * It has no non-hidden Active Threat membership — the correlation job has no
            use for it (a HOS only makes sense in the context of an AT).
          * Its last recorded Kafka write was not a tombstone (``is_kafka_tombstoned=false``),
            i.e. either it is currently published or it has never been written.

        ``is_user_edited=true`` rows are NOT excluded: the user-edited body stays in the
        DB and is republished automatically once an AT becomes visible again, but while
        no AT references it the correlation job has nothing to do with the row.
        """
        visible_hos_ids_subq = (
            select(ActiveThreatHighOrderSigma.high_order_sigma_id)
            .join(ActiveThreat, ActiveThreat.tdm_id == ActiveThreatHighOrderSigma.active_threat_id)
            .where(ActiveThreat.is_hidden.is_(False))
        )
        query = select(HighOrderSigma.tdm_id).where(
            HighOrderSigma.id.not_in(visible_hos_ids_subq),
            HighOrderSigma.is_kafka_tombstoned.is_(False),
        )
        result = await self.session.execute(query)
        return {row[0] for row in result.all()}

    async def get_pending_kafka_publish_tdm_ids(self) -> set[str]:
        """Return HOS TDM ids that should be on Kafka but were last tombstoned.

        A HOS qualifies when both conditions hold:
          * It has at least one non-hidden Active Threat membership — i.e. the
            correlation job needs it.
          * Its last recorded Kafka write was a tombstone (``is_kafka_tombstoned=true``).

        This catches HOS that regained visibility through an AT-membership change
        without TDM updating the HOS body (so the row wasn't part of the regular
        ``upserted_hos_ids`` publish batch).
        """
        visible_hos_ids_subq = (
            select(ActiveThreatHighOrderSigma.high_order_sigma_id)
            .join(ActiveThreat, ActiveThreat.tdm_id == ActiveThreatHighOrderSigma.active_threat_id)
            .where(ActiveThreat.is_hidden.is_(False))
        )
        query = select(HighOrderSigma.tdm_id).where(
            HighOrderSigma.id.in_(visible_hos_ids_subq),
            HighOrderSigma.is_kafka_tombstoned.is_(True),
        )
        result = await self.session.execute(query)
        return {row[0] for row in result.all()}

    async def set_kafka_tombstoned(self, tdm_ids: Iterable[str], tombstoned: bool) -> None:
        """Update ``is_kafka_tombstoned`` for the given TDM ids; missing rows are silently skipped.

        The flag mirrors the most recent Kafka write performed for the row, so callers
        must invoke this immediately after a successful ``send_hos`` (``tombstoned=False``)
        or ``delete_hos`` (``tombstoned=True``). Orphan-deleted HOS rows are gone from
        the DB so they are skipped automatically.
        """
        ids = list(dict.fromkeys(tdm_ids))
        if not ids:
            return
        await self.session.execute(
            update(HighOrderSigma).where(HighOrderSigma.tdm_id.in_(ids)).values(is_kafka_tombstoned=tombstoned)
        )
        await self.session.flush()

    async def get_active_threat_ids_by_hos_tdm_ids(self, hos_tdm_ids: Iterable[str]) -> dict[str, list[str]]:
        ids = list(dict.fromkeys(hos_tdm_ids))
        if not ids:
            return {}
        result = await self.session.execute(
            select(HighOrderSigma.tdm_id, ActiveThreatHighOrderSigma.active_threat_id)
            .join(
                ActiveThreatHighOrderSigma,
                ActiveThreatHighOrderSigma.high_order_sigma_id == HighOrderSigma.id,
            )
            .join(ActiveThreat, ActiveThreat.tdm_id == ActiveThreatHighOrderSigma.active_threat_id)
            .where(
                HighOrderSigma.tdm_id.in_(ids),
                ActiveThreat.is_hidden.is_(False),
            )
        )
        mapping: dict[str, list[str]] = {}
        for hos_tdm_id, active_threat_id in result.all():
            mapping.setdefault(hos_tdm_id, []).append(active_threat_id)
        for hos_tdm_id in mapping:
            mapping[hos_tdm_id] = sorted(set(mapping[hos_tdm_id]))
        return mapping
