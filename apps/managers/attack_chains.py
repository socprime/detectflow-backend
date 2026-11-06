"""Business logic for attack chains: chain list/detail, the Configure graph, and legacy shims.

The per-customer correlation runtime moved to :mod:`apps.managers.correlation_pipelines`. What
remains here is the chains-and-graph manager plus a **quarantined** singleton surface which now
operates entirely on the ``Default`` correlation pipeline: every shim resolves it through
``CorrelationPipelinesManager.get_default_pipeline_id()`` and delegates. Nothing here reads or
writes ``attack_chain_configuration`` any more — that table survives one release as a rollback
record only.

Two things the projection back into the legacy shape cannot express honestly, both kept because
the response shape must not change under the frontend before its cutover MR merges:

* ``correlation_window_seconds`` is non-null in the legacy response while the pipeline column is
  nullable, and NULL means "inherit each HoS rule's ``detection.timeframe``". The projection
  renders :data:`LEGACY_WINDOW_PROJECTION_SECONDS`, the deprecated column's own server default —
  the number the legacy dialog showed before the migration. A legacy ``PATCH`` that echoes it back
  therefore *creates* an override where there was none; the per-pipeline API is the only surface
  that can say "inherit".
* Membership is ``member_pipeline_ids`` on ``PATCH /correlation_pipelines/{id}``. This legacy
  Configure surface no longer projects or writes destination-topic toggles.

Everything quarantined is marked ``DEPRECATED`` at its definition and is removed wholesale by
``remove-singleton-correlation-endpoints.md`` once nothing calls it.
"""

from collections import OrderedDict
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.converters import AttackChainConverter
from apps.core.exceptions import NotFoundError
from apps.core.logger import get_logger
from apps.core.models import (
    ActiveThreat,
    AttackChain,
    AttackChainEvent,
    CorrelationPipeline,
    User,
)
from apps.core.schemas import (
    AttackChainConfigurationResponse,
    AttackChainConfigurationUpdateRequest,
    AttackChainDetailResponse,
    AttackChainGroupBy,
    AttackChainPatchRequest,
    AttackChainResponse,
    AttackChainsFiltersResponse,
    AttackChainsFiltersSectionResponse,
    AttackChainsRuntimeResponse,
    AttackChainsRuntimeStateResponse,
    CorrelationJobControlResponse,
    CorrelationJobGraphBlock,
    CorrelationJobGraphData,
    CorrelationJobGraphPoint,
    CorrelationPipelineUpdateRequest,
    FilterOptionResponse,
    MonitoredThreatsFiltersSectionResponse,
)
from apps.managers.correlation_pipelines import (
    CorrelationPipelinesManager,
    deployment_config_for_row,
)
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.active_threats import ActiveThreatDAO
from apps.modules.postgre.attack_chains import AttackChainsDAO
from apps.modules.postgre.audit import AuditLogDAO
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO
from apps.modules.postgre.rule import RuleDAO, RuleTechniqueFields
from apps.services.attack_chains_list_invalidation import attack_chains_list_invalidation_service
from apps.services.mitre_attack import mitre_attack

logger = get_logger(__name__)

# DEPRECATED. The legacy response's ``id`` is documented as "always 1 (singleton)" and is an
# ``int``, so it cannot carry the pipeline's UUID; the id of the row actually being addressed is
# on ``/api/v1/correlation_pipelines``.
LEGACY_SINGLETON_ID = 1

# DEPRECATED. What a NULL ``correlation_pipelines.correlation_window_seconds`` is rendered as in
# the legacy shape, whose field is a non-null ``int``. 300 is ``attack_chain_configuration``'s own
# server default — i.e. what the Configure dialog displayed for a deployment that never tuned the
# slider, which is exactly the state the migration carried across as NULL. Echoing this value back
# in a PATCH turns "inherit each HoS rule's detection.timeframe" into a real override; only the
# per-pipeline API can express the difference.
LEGACY_WINDOW_PROJECTION_SECONDS = 300


def _format_graph_day(value: date) -> str:
    return f"{value.day} {value.strftime('%b %Y')}"


class AttackChainsManager:
    """Attack chains: chain list/detail, the Configure graph, and the quarantined singleton surface.

    The per-customer correlation runtime lives in
    :class:`apps.managers.correlation_pipelines.CorrelationPipelinesManager`. Everything here that
    is marked ``DEPRECATED`` keeps the pre-epic ``/correlation_job`` and ``/attack_chains/runtime``
    endpoints answering while the frontend cuts over — all of them now bound to the ``Default``
    correlation pipeline, none of them reading or writing ``attack_chain_configuration``.
    ``remove-singleton-correlation-endpoints.md`` removes the surface once nothing calls it.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = AttackChainsDAO(db)
        self.active_threats_dao = ActiveThreatDAO(db)
        self.rule_dao = RuleDAO(db)
        self.audit_dao = AuditLogDAO(db)
        self.correlation_pipeline_dao = CorrelationPipelineDAO(db)
        self.mitre_attack = mitre_attack

    async def _default_pipeline_id(self) -> UUID:
        """DEPRECATED — the ``Default`` correlation pipeline every legacy operation is bound to.

        Resolution (by name, never "the only row") and its ``NotFoundError`` live in
        :meth:`CorrelationPipelinesManager.get_default_pipeline_id`; this is only the shim's way in.
        """
        return await CorrelationPipelinesManager(self.db).get_default_pipeline_id()

    async def _default_row(self) -> CorrelationPipeline:
        """DEPRECATED — the ``Default`` row with its members loaded (the config projection needs them).

        Raises:
            NotFoundError: The row was deleted between the name lookup and this read.
        """
        pipeline_id = await self._default_pipeline_id()
        row = await self.correlation_pipeline_dao.get_with_relations(pipeline_id)
        if row is None:
            raise NotFoundError("No default correlation pipeline; use /api/v1/correlation_pipelines")
        return row

    async def get_correlation_job_runtime_snapshot(self) -> dict[str, str] | None:
        """DEPRECATED shim — ``job_uptime`` of the ``Default`` correlation pipeline's runtime."""
        return await CorrelationPipelinesManager(self.db).get_runtime_snapshot(await self._default_row())

    async def _row_to_response(self, row: CorrelationPipeline) -> AttackChainConfigurationResponse:
        """DEPRECATED — one correlation pipeline projected into the legacy singleton response shape.

        Destination-topic toggles are gone: membership is ``member_pipeline_ids`` on the
        per-pipeline API. This shape keeps threshold, window, and auto-add only.
        """
        return AttackChainConfigurationResponse(
            id=LEGACY_SINGLETON_ID,
            threshold_pct=row.threshold_pct,
            correlation_window_seconds=row.correlation_window_seconds or LEGACY_WINDOW_PROJECTION_SECONDS,
            auto_add_new_pipelines=bool(row.auto_add_new_pipelines),
            updated_at=row.updated_at,
            updated_by=row.updated_by,
        )

    async def start_runtime(self, current_user: User | None = None) -> AttackChainsRuntimeResponse:
        """DEPRECATED shim — start the ``Default`` correlation pipeline's job pair."""
        return await CorrelationPipelinesManager(self.db).start(await self._default_pipeline_id(), current_user)

    async def stop_runtime(self, current_user: User | None = None) -> AttackChainsRuntimeResponse:
        """DEPRECATED shim — stop the ``Default`` correlation pipeline's job pair."""
        return await CorrelationPipelinesManager(self.db).stop(await self._default_pipeline_id(), current_user)

    async def get_runtime_state(self) -> AttackChainsRuntimeStateResponse:
        """DEPRECATED shim — composite runtime state of the ``Default`` correlation pipeline."""
        return await CorrelationPipelinesManager(self.db).get_runtime_state(await self._default_pipeline_id())

    async def restart_correlation_job_if_running(
        self, current_user: User | None = None
    ) -> CorrelationJobControlResponse | None:
        """DEPRECATED shim — re-render the ``Default`` pipeline's correlation-job spec when it is up.

        No production caller is left: the pipeline orchestrator's "subscription drift" path is now
        per-membership (``_sync_correlation_pipelines_non_critical`` →
        ``CorrelationPipelinesManager.restart_if_running``), which is its per-pipeline replacement.
        The agent-job is not touched — its spec doesn't depend on pipeline state.
        """
        row = await self._default_row()
        await CorrelationPipelinesManager(self.db).restart_if_running(row.id, current_user)
        # Read both fields off the rendering config rather than the row: ``correlation_deployment_name``
        # is NULL until the pipeline has been started once, and reporting "" for a deployment that
        # does have a generated name is a lie the caller cannot tell from an error.
        config = deployment_config_for_row(row)
        return CorrelationJobControlResponse(
            action="run",
            operation="updated",
            deployment_name=config.correlation_deployment_name,
            input_topics=config.input_topics,
        )

    async def get_configuration(self) -> AttackChainConfigurationResponse:
        """DEPRECATED — the ``Default`` correlation pipeline in the legacy singleton shape."""
        return await self._row_to_response(await self._default_row())

    async def get_graph_data(self) -> CorrelationJobGraphData:
        """The five Configure-dashboard blocks across every correlation pipeline.

        ATTACK CHAINS / AFFECTED HOSTNAMES / TAGGED EVENTS / MATCHED RULES are unscoped table
        aggregates, so chains whose producing pipeline was deleted (``ON DELETE SET NULL``) still
        count. MONITORED THREATS is the Active Threat catalogue (non-hidden / not globally
        blocked), not a per-pipeline deny-list union.
        """
        return CorrelationJobGraphData(
            blocks=[
                await self._build_attack_chains_block(),
                await self._build_monitored_threats_block(),
                await self._build_affected_hostnames_block(),
                await self._build_tagged_events_block(),
                await self._build_matched_rules_block(),
            ]
        )

    async def _build_attack_chains_block(self) -> CorrelationJobGraphBlock:
        attack_chains_rows = await self.dao.get_graph_daily_series()
        attack_chains: list[CorrelationJobGraphPoint] = []
        for day, count in attack_chains_rows:
            attack_chains.append(CorrelationJobGraphPoint(name=_format_graph_day(day), value=int(count)))
        count_all = await self.dao.get_attack_chains_count()
        return CorrelationJobGraphBlock(
            name="ATTACK CHAINS",
            count_all=count_all,
            count_active=None,
            points=attack_chains,
        )

    async def _build_monitored_threats_block(self) -> CorrelationJobGraphBlock:
        count_all = await self.active_threats_dao.get_monitored_threats_count()
        count_active = await self.active_threats_dao.get_unblocked_threats_count()
        monitored_threats_rows = await self.active_threats_dao.get_graph_daily_series()
        monitored_threats: list[CorrelationJobGraphPoint] = []
        for day, count in monitored_threats_rows:
            monitored_threats.append(CorrelationJobGraphPoint(name=_format_graph_day(day), value=int(count)))
        return CorrelationJobGraphBlock(
            name="MONITORED THREATS",
            count_all=count_all,
            count_active=count_active,
            points=monitored_threats,
        )

    async def _build_affected_hostnames_block(self) -> CorrelationJobGraphBlock:
        count_all = await self.dao.get_affected_hostnames_count()
        affected_hostnames_rows = await self.dao.get_graph_affected_hostnames_daily_series()
        affected_hostnames: list[CorrelationJobGraphPoint] = []
        for day, count in affected_hostnames_rows:
            affected_hostnames.append(CorrelationJobGraphPoint(name=_format_graph_day(day), value=int(count)))
        return CorrelationJobGraphBlock(
            name="AFFECTED HOSTNAMES",
            count_all=count_all,
            count_active=None,
            points=affected_hostnames,
        )

    async def _build_tagged_events_block(self) -> CorrelationJobGraphBlock:
        count_all = await self.dao.get_tagged_events_count()
        tagged_events_rows = await self.dao.get_graph_tagged_events_daily_series()
        tagged_events: list[CorrelationJobGraphPoint] = []
        for day, count in tagged_events_rows:
            tagged_events.append(CorrelationJobGraphPoint(name=_format_graph_day(day), value=int(count)))
        return CorrelationJobGraphBlock(
            name="TAGGED EVENTS",
            count_all=count_all,
            count_active=None,
            points=tagged_events,
        )

    async def _build_matched_rules_block(self) -> CorrelationJobGraphBlock:
        count_active, count_all = await self.dao.get_matched_rules_counts()
        matched_rules_rows = await self.dao.get_graph_matched_rules_daily_series()
        matched_rules: list[CorrelationJobGraphPoint] = []
        for day, count in matched_rules_rows:
            matched_rules.append(CorrelationJobGraphPoint(name=_format_graph_day(day), value=int(count)))
        return CorrelationJobGraphBlock(
            name="MATCHED RULES",
            count_all=count_all,
            count_active=count_active,
            points=matched_rules,
        )

    async def update(
        self,
        body: AttackChainConfigurationUpdateRequest,
        current_user: User,
    ) -> AttackChainConfigurationResponse:
        """DEPRECATED — the singleton Configure dialog, applied to the ``Default`` pipeline.

        Delegates to ``CorrelationPipelinesManager.update``, which owns the edit-safety matrix, the
        chains-config publish and the activity event; nothing is written to
        ``attack_chain_configuration`` any more.

        ``confirm_restart=True`` is passed deliberately. The legacy request schema has no such flag
        and this dialog never prompted, so refusing an edit with a 409 the client cannot answer
        would be a regression — an operator editing through this route can restart a running job
        without being asked. The per-pipeline API is where that confirmation lives.

        Raises:
            NotFoundError: No ``Default`` correlation pipeline.
        """
        pipeline_id = await self._default_pipeline_id()
        await CorrelationPipelinesManager(self.db).update(
            pipeline_id,
            CorrelationPipelineUpdateRequest(**await self._to_pipeline_update(body)),
            current_user,
            confirm_restart=True,
        )
        return await self._row_to_response(await self._default_row())

    async def _to_pipeline_update(self, body: AttackChainConfigurationUpdateRequest) -> dict[str, Any]:
        """DEPRECATED — legacy Configure body -> the fields of a ``CorrelationPipelineUpdateRequest``.

        Only the keys the client actually sent are returned: the per-pipeline manager reads its
        request with ``exclude_unset``, so an absent key must stay absent rather than arrive as an
        explicit ``None`` that would clear a column. ``exclude_none`` matches the legacy schema,
        where every field is optional-and-nullable and a null has always meant "not sent" — which
        is also why this route cannot clear the window override.
        """
        data = body.model_dump(exclude_none=True)
        return {
            field: data[field]
            for field in ("threshold_pct", "correlation_window_seconds", "auto_add_new_pipelines")
            if field in data
        }

    async def get_filter_options(self) -> AttackChainsFiltersResponse:
        chain_severities = await self.dao.get_distinct_chain_severities()
        chain_statuses = await self.dao.get_distinct_chain_statuses()
        threat_severities = await self.active_threats_dao.get_distinct_severities()
        industries = await self.active_threats_dao.get_distinct_array_values(ActiveThreat.industries)
        threat_types = await self.active_threats_dao.get_distinct_array_values(ActiveThreat.threat_types)
        geographies = await self.active_threats_dao.get_distinct_array_values(ActiveThreat.geographies)

        return AttackChainsFiltersResponse(
            attack_chains=AttackChainsFiltersSectionResponse(
                group=[
                    FilterOptionResponse(label="No Grouping", value="none"),
                    FilterOptionResponse(label="Hostname", value="hostname"),
                    FilterOptionResponse(label="Chain Name", value="chain_name"),
                ],
                severity=chain_severities,
                status=chain_statuses,
            ),
            monitored_threats=MonitoredThreatsFiltersSectionResponse(
                severity=threat_severities,
                industry=industries,
                threat_type=threat_types,
                region=geographies,
            ),
        )

    async def list_chains(
        self,
        pagination: dict,
        group_by: AttackChainGroupBy,
        *,
        severity: list[str] | None = None,
        status: list[str] | None = None,
        show_filtered_out: bool = False,
        correlation_pipeline_id: UUID | None = None,
    ) -> tuple[dict[str, list[AttackChainResponse]], int, int | None]:
        """List chains, optionally scoped to one correlation pipeline.

        ``correlation_pipeline_id=None`` is platform-wide and includes chains whose pipeline was
        deleted (the FK is ``ON DELETE SET NULL``); a scoped call returns only that pipeline's
        chains — including its ``filtered_out_count``, which would otherwise leak how many chains
        other tenants hid.
        """
        sort = pagination["sort"]
        order = pagination["order"] or "asc"
        filtered_out_count = (
            None
            if show_filtered_out
            else await self.dao.count_filtered_out_chains(correlation_pipeline_id=correlation_pipeline_id)
        )
        if group_by == "none":
            rows, total = await self.dao.list_chains(
                skip=pagination["skip"],
                limit=pagination["limit"],
                search=pagination["search"],
                sort=sort,
                order=order,
                severity=severity,
                status=status,
                show_filtered_out=show_filtered_out,
                correlation_pipeline_id=correlation_pipeline_id,
            )
            return {"default": [AttackChainConverter.to_list_item(row) for row in rows]}, total, filtered_out_count

        grouped_rows, total = await self.dao.list_chains_grouped(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination["search"],
            sort=sort,
            order=order,
            group_by=group_by,
            severity=severity,
            status=status,
            show_filtered_out=show_filtered_out,
            correlation_pipeline_id=correlation_pipeline_id,
        )
        groups: dict[str, list[AttackChainResponse]] = OrderedDict()
        for key, rows in grouped_rows.items():
            groups[key] = [AttackChainConverter.to_list_item(row) for row in rows]
        return groups, total, filtered_out_count

    async def get_chain_detail(self, chain_id: UUID) -> AttackChainDetailResponse:
        row = await self.dao.get_chain_by_id(chain_id)
        if row is None:
            raise NotFoundError(f"Attack chain with id {chain_id} not found")
        return await self._build_chain_detail_response(row)

    async def _build_chain_detail_response(self, chain: AttackChain) -> AttackChainDetailResponse:
        # Stored ``attack_chain_events`` rows are only correlation hits (seq hit = true); no per-seq map needed.
        events = list(chain.events or [])
        # Prefer ``rules.id`` (Kafka ``rule_db_id``) for name/level/repo. The Sigma-UUID lookup
        # covers the rows the PK cannot resolve: legacy events that predate
        # ``attack_chain_events.rule_id``, and events whose rule has since been deleted.
        rule_ids = {event.rule_id for event in events if event.rule_id is not None}
        rules_by_id = await self.rule_dao.get_technique_fields_by_ids(rule_ids)
        unresolved_sigma_ids = {
            event.sigma_rule_id for event in events if event.rule_id is None or event.rule_id not in rules_by_id
        }
        levels_by_sigma_uuid = await self.rule_dao.get_levels_by_sigma_uuids(unresolved_sigma_ids)
        all_techniques = self._prepare_chain_techniques(events, levels_by_sigma_uuid, rules_by_id)
        chain_graph: list[dict[str, Any]] = []
        for technique in all_techniques.values():
            chain_graph.append(
                {
                    "name": technique["name"],
                    "description": technique["description"],
                    "code": technique["code"],
                    "hit": True,
                }
            )
        for tag in chain.hos_tags or []:
            if tag not in all_techniques and tag.startswith("attack"):
                if data := self.mitre_attack.get_technique_by_id(tag):
                    chain_graph.append(
                        {
                            "name": data.name,
                            "description": data.description,
                            "code": tag,
                            "hit": False,
                        }
                    )
        chain_graph.sort(key=lambda item: item["name"].lower())
        threat = await self.active_threats_dao.get_by_tdm_id(chain.active_threat_id)
        return AttackChainConverter.to_detail(chain, list(all_techniques.values()), chain_graph, threat)

    def _prepare_chain_techniques(
        self,
        events: Sequence[AttackChainEvent],
        levels_by_sigma_uuid: Mapping[str, str],
        rules_by_id: Mapping[UUID, RuleTechniqueFields] | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Aggregate per-technique metadata and per-rule event counts.

        Callers only persist events that already matched a correlation hit (``seq`` hit = true),
        so every row here is treated as a hit for ``chain_graph`` / ``techniques``.

        Two local copies of the same Sigma (different ``rules.id``, same ``sigma_rule_id``) stay
        as two rows. Events without ``rule_id`` still collapse by ``sigma_rule_title`` (legacy).

        Args:
            events: correlation-hit rows for one chain.
            levels_by_sigma_uuid: Sigma UUID -> severity, from ``RuleDAO.get_levels_by_sigma_uuids``.
                Consulted whenever the event's ``rule_id`` resolves to nothing.
            rules_by_id: ``rules.id`` -> id/repository_id/name/level, from
                ``RuleDAO.get_technique_fields_by_ids``. A PK that is missing from the map
                (rule deleted) gets ``rule_id``/``repository_id`` null, keeps the wire
                ``event.sigma_rule_title`` as display name, and takes its level from the map above.
        """
        rules_by_id = rules_by_id or {}
        all_techniques: dict[str, dict[str, Any]] = {}
        tagged_events: dict[tuple[object, ...], dict[str, Any]] = {}

        for event in events:
            row_key = self._technique_rule_key(event)
            entry = tagged_events.get(row_key)
            if entry is None:
                tagged_events[row_key] = self._technique_rule_row(event, levels_by_sigma_uuid, rules_by_id)
            else:
                entry["first_seen"] = min(entry["first_seen"], event.timestamp)
                entry["last_seen"] = max(entry["last_seen"], event.timestamp)
                entry["tagged_events"] += 1
            for technique_id in list(event.technique_ids or []):
                existing = all_techniques.get(technique_id)
                if existing is None:
                    ap = self.mitre_attack.get_technique_by_id(technique_id)
                    all_techniques[technique_id] = {
                        "name": ap.name if ap else technique_id,
                        "description": ap.description if ap else None,
                        "code": technique_id,
                        "rules": [row_key],
                    }
                else:
                    if row_key not in existing["rules"]:
                        existing["rules"].append(row_key)
        for technique in all_techniques.values():
            rules_resolved = [tagged_events[key] for key in technique["rules"]]
            technique["rules"] = rules_resolved
            technique["matched_rules"] = len(rules_resolved)
            if rules_resolved:
                technique["first_seen"] = min(r["first_seen"] for r in rules_resolved)
                technique["last_seen"] = max(r["last_seen"] for r in rules_resolved)
        return all_techniques

    @staticmethod
    def _technique_rule_key(event: AttackChainEvent) -> tuple[str, str | None]:
        if event.rule_id is not None:
            return ("pk", str(event.rule_id))
        return ("legacy", event.sigma_rule_title)

    @staticmethod
    def _technique_rule_row(
        event: AttackChainEvent,
        levels_by_sigma_uuid: Mapping[str, str],
        rules_by_id: Mapping[UUID, RuleTechniqueFields],
    ) -> dict[str, Any]:
        rule = rules_by_id.get(event.rule_id) if event.rule_id is not None else None
        return {
            "rule_id": str(rule.id) if rule else None,
            "repository_id": str(rule.repository_id) if rule and rule.repository_id else None,
            "rule_name": rule.name if rule else event.sigma_rule_title,
            # ``sigma_rule_id`` lets the UI drive per-rule "View in SIEM" for this row.
            "sigma_rule_id": str(event.sigma_rule_id),
            "level": rule.level if rule else levels_by_sigma_uuid.get(str(event.sigma_rule_id)),
            "first_seen": event.timestamp,
            "last_seen": event.timestamp,
            "tagged_events": 1,
        }

    async def patch_chain(
        self,
        chain_id: UUID,
        body: AttackChainPatchRequest,
        current_user: User,
    ) -> AttackChainDetailResponse:
        row = await self.dao.get_chain_by_id(chain_id)
        if row is None:
            raise NotFoundError(f"Attack chain with id {chain_id} not found")
        if row.status == body.status:
            return await self._build_chain_detail_response(row)

        previous_status = row.status
        updated = await self.dao.update_chain_status(chain_id, body.status)
        if updated is None:
            raise NotFoundError(f"Attack chain with id {chain_id} not found")

        await activity_producer.log_action(
            action="update",
            entity_type="attack_chain",
            entity_id=str(chain_id),
            entity_name=f"Attack chain {chain_id}",
            user=current_user,
            details=f"User {current_user.email} updated attack chain status",
            changes={"status": {"old": previous_status, "new": body.status}},
            source="user",
        )

        await self.db.commit()
        attack_chains_list_invalidation_service.notify_changed()

        return await self._build_chain_detail_response(updated)
