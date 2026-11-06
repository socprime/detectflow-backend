from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import HighOrderSigmaRuleRef, PipelineRule, Rule
from apps.modules.postgre.high_order_sigma_rule_checks import HighOrderSigmaRuleCheckFailureDAO


class HighOrderSigmaRuleCheckService:
    """Refresh failure-only snapshots for rules referenced by High Order Sigmas."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = HighOrderSigmaRuleCheckFailureDAO(db)

    async def refresh_for_hos_ids(self, high_order_sigma_ids: Iterable[UUID]) -> None:
        ids = self._unique_uuids(high_order_sigma_ids)
        if not ids:
            return
        await self.dao.delete_by_hos_ids(ids)
        await self._insert_current_failures(high_order_sigma_ids=ids)

    async def refresh_for_rule_ids(self, rule_ids: Iterable[UUID]) -> None:
        ids = self._unique_uuids(rule_ids)
        if not ids:
            return
        await self.dao.delete_by_rule_ids(ids)
        await self._insert_current_failures(rule_ids=ids)

    async def _insert_current_failures(
        self,
        *,
        high_order_sigma_ids: list[UUID] | None = None,
        rule_ids: list[UUID] | None = None,
    ) -> None:
        pairs_stmt = (
            select(
                HighOrderSigmaRuleRef.high_order_sigma_id,
                Rule.id.label("rule_id"),
                Rule.is_supported,
            )
            .join(Rule, Rule.sigma_uuid == HighOrderSigmaRuleRef.sigma_uuid)
            .where(Rule.sigma_uuid.is_not(None))
        )
        if high_order_sigma_ids is not None:
            pairs_stmt = pairs_stmt.where(HighOrderSigmaRuleRef.high_order_sigma_id.in_(high_order_sigma_ids))
        if rule_ids is not None:
            pairs_stmt = pairs_stmt.where(Rule.id.in_(rule_ids))

        pairs_result = await self.db.execute(pairs_stmt)
        pairs = pairs_result.all()
        if not pairs:
            return

        affected_rule_ids = list({row.rule_id for row in pairs})
        pipeline_result = await self.db.execute(
            select(PipelineRule.rule_id, PipelineRule.pipeline_id, PipelineRule.enabled).where(
                PipelineRule.rule_id.in_(affected_rule_ids),
                PipelineRule.pipeline_id.is_not(None),
            )
        )
        connected_pipeline_ids: dict[UUID, set[UUID]] = {}
        disabled_pipeline_ids: dict[UUID, list[UUID]] = {}
        for rule_id, pipeline_id, enabled in pipeline_result.all():
            connected_pipeline_ids.setdefault(rule_id, set()).add(pipeline_id)
            if not enabled:
                disabled_pipeline_ids.setdefault(rule_id, []).append(pipeline_id)

        rows: list[dict] = []
        for pair in pairs:
            disabled_ids = sorted(set(disabled_pipeline_ids.get(pair.rule_id, [])), key=str)
            is_not_supported = not bool(pair.is_supported)
            is_not_connected = not connected_pipeline_ids.get(pair.rule_id)
            if not (is_not_supported or is_not_connected or disabled_ids):
                continue
            rows.append(
                {
                    "high_order_sigma_id": pair.high_order_sigma_id,
                    "rule_id": pair.rule_id,
                    "is_not_supported": is_not_supported,
                    "is_not_connected": is_not_connected,
                    "disabled_pipeline_ids": disabled_ids,
                }
            )

        await self.dao.insert_many(rows)

    @staticmethod
    def _unique_uuids(values: Iterable[UUID | str]) -> list[UUID]:
        normalized: list[UUID] = []
        for value in values:
            if isinstance(value, UUID):
                normalized.append(value)
                continue
            try:
                normalized.append(UUID(str(value)))
            except (TypeError, ValueError):
                continue
        return list(dict.fromkeys(normalized))
