from collections.abc import Iterable
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import ActiveThreatHighOrderSigma, HighOrderSigmaRuleCheckFailure, Pipeline, Rule

UNSUPPORTED_RULE_ERROR_MESSAGE = "The syntax of this Sigma rule is not supported in DetectFlow yet."
NOT_CONNECTED_RULE_ERROR_MESSAGE = "This detection is not connected to any pipeline. Add the directory with the rule to a pipeline with data from the corresponding log source."
DISABLED_PIPELINE_ERROR_MESSAGE = "This detection has been disabled in the following pipelines:"


class HighOrderSigmaRuleCheckFailureDAO:
    """Failure-only snapshot access for HOS referenced rule checks."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def delete_by_hos_ids(self, high_order_sigma_ids: Iterable[UUID]) -> None:
        ids = list(dict.fromkeys(high_order_sigma_ids))
        if not ids:
            return
        await self.session.execute(
            delete(HighOrderSigmaRuleCheckFailure).where(HighOrderSigmaRuleCheckFailure.high_order_sigma_id.in_(ids))
        )
        await self.session.flush()

    async def delete_by_rule_ids(self, rule_ids: Iterable[UUID]) -> None:
        ids = list(dict.fromkeys(rule_ids))
        if not ids:
            return
        await self.session.execute(
            delete(HighOrderSigmaRuleCheckFailure).where(HighOrderSigmaRuleCheckFailure.rule_id.in_(ids))
        )
        await self.session.flush()

    async def insert_many(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.session.add_all([HighOrderSigmaRuleCheckFailure(**row) for row in rows])
        await self.session.flush()

    async def get_grouped_results_by_active_threat_ids(
        self, active_threat_ids: Iterable[str]
    ) -> dict[str, dict[str, list]]:
        """Grouped check results for a page of active threats — in TWO queries total.

        Previously this looped per threat (2 round-trips × N page rows → 50
        round-trips at limit=25, up to 1 000 at limit=500), holding the pooled
        connection the whole time. Now: one joined SELECT for all threats plus
        one pipeline-name lookup.
        """
        ids = list(dict.fromkeys(active_threat_ids))
        if not ids:
            return {}

        result = await self.session.execute(
            select(
                ActiveThreatHighOrderSigma.active_threat_id,
                HighOrderSigmaRuleCheckFailure.rule_id,
                HighOrderSigmaRuleCheckFailure.is_not_supported,
                HighOrderSigmaRuleCheckFailure.is_not_connected,
                HighOrderSigmaRuleCheckFailure.disabled_pipeline_ids,
                Rule.name,
                Rule.sigma_uuid,
            )
            .select_from(ActiveThreatHighOrderSigma)
            .join(
                HighOrderSigmaRuleCheckFailure,
                HighOrderSigmaRuleCheckFailure.high_order_sigma_id == ActiveThreatHighOrderSigma.high_order_sigma_id,
            )
            .join(Rule, Rule.id == HighOrderSigmaRuleCheckFailure.rule_id)
            .where(ActiveThreatHighOrderSigma.active_threat_id.in_(ids))
            .order_by(Rule.name.asc(), HighOrderSigmaRuleCheckFailure.rule_id.asc())
        )
        rows = result.all()

        pipeline_names = await self._collect_pipeline_names(
            {pipeline_id for row in rows for pipeline_id in (row.disabled_pipeline_ids or [])}
        )

        rows_by_threat: dict[str, list[Any]] = {}
        for row in rows:
            rows_by_threat.setdefault(row.active_threat_id, []).append(row)

        return {
            threat_id: self._build_grouped_payload(rows_by_threat.get(threat_id, []), pipeline_names)
            for threat_id in ids
        }

    async def _get_rows_by_active_threat(self, active_threat_id: str) -> list[Any]:
        result = await self.session.execute(
            select(
                HighOrderSigmaRuleCheckFailure.rule_id,
                HighOrderSigmaRuleCheckFailure.is_not_supported,
                HighOrderSigmaRuleCheckFailure.is_not_connected,
                HighOrderSigmaRuleCheckFailure.disabled_pipeline_ids,
                Rule.name,
                Rule.sigma_uuid,
            )
            .select_from(ActiveThreatHighOrderSigma)
            .join(
                HighOrderSigmaRuleCheckFailure,
                HighOrderSigmaRuleCheckFailure.high_order_sigma_id == ActiveThreatHighOrderSigma.high_order_sigma_id,
            )
            .join(Rule, Rule.id == HighOrderSigmaRuleCheckFailure.rule_id)
            .where(ActiveThreatHighOrderSigma.active_threat_id == active_threat_id)
            .order_by(Rule.name.asc(), HighOrderSigmaRuleCheckFailure.rule_id.asc())
        )
        return result.all()

    async def get_grouped_results_by_active_threat(self, active_threat_id: str) -> dict[str, list]:
        rows = await self._get_rows_by_active_threat(active_threat_id)

        pipeline_names = await self._collect_pipeline_names(
            {pipeline_id for row in rows for pipeline_id in (row.disabled_pipeline_ids or [])}
        )

        return self._build_grouped_payload(rows, pipeline_names)

    @staticmethod
    def _build_grouped_payload(rows: list[Any], pipeline_names: dict[UUID, str]) -> dict[str, list]:
        """Group failure rows into the unsupported/not-connected/disabled payload."""
        payload: dict[str, list] = {
            "unsupported_rules": [],
            "not_connected_rules": [],
            "disabled_rules": [],
        }
        seen: dict[str, set[UUID]] = {
            "unsupported_rules": set(),
            "not_connected_rules": set(),
        }
        disabled_entries: dict[UUID, dict[str, Any]] = {}
        disabled_pipeline_seen: dict[UUID, set[UUID]] = {}
        for row in rows:
            rule_key = row.sigma_uuid or row.rule_id
            rule_payload = {
                "rule_id": str(row.rule_id),
                "rule_name": row.name,
            }
            if row.is_not_supported and rule_key not in seen["unsupported_rules"]:
                payload["unsupported_rules"].append({**rule_payload, "description": UNSUPPORTED_RULE_ERROR_MESSAGE})
                seen["unsupported_rules"].add(rule_key)
            if row.is_not_connected and rule_key not in seen["not_connected_rules"]:
                payload["not_connected_rules"].append({**rule_payload, "description": NOT_CONNECTED_RULE_ERROR_MESSAGE})
                seen["not_connected_rules"].add(rule_key)
            if row.disabled_pipeline_ids:
                if rule_key not in disabled_entries:
                    disabled_entries[rule_key] = {
                        **rule_payload,
                        "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                        "pipelines": [],
                    }
                    payload["disabled_rules"].append(disabled_entries[rule_key])
                seen_pipeline_ids = disabled_pipeline_seen.setdefault(rule_key, set())
                new_pipeline_ids = [
                    pipeline_id for pipeline_id in row.disabled_pipeline_ids if pipeline_id not in seen_pipeline_ids
                ]
                disabled_entries[rule_key]["pipelines"].extend(
                    [
                        {
                            "id": str(pipeline_id),
                            "name": pipeline_names.get(pipeline_id, str(pipeline_id)),
                        }
                        for pipeline_id in new_pipeline_ids
                    ]
                )
                seen_pipeline_ids.update(new_pipeline_ids)

        return payload

    async def _collect_pipeline_names(self, pipeline_ids: Iterable[UUID]) -> dict[UUID, str]:
        pipeline_names: dict[UUID, str] = {}
        if pipeline_ids:
            result = await self.session.execute(select(Pipeline.id, Pipeline.name).where(Pipeline.id.in_(pipeline_ids)))
            pipeline_names = dict(result.all())
        return pipeline_names
