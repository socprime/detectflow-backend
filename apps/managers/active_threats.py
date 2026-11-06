from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.exceptions import BadRequestError, ConflictError, NotFoundError
from apps.core.logger import get_logger
from apps.core.models import ActiveThreat, User
from apps.core.schemas import (
    ActiveThreatBlockResponse,
    ActiveThreatBulkItemResult,
    ActiveThreatBulkResponse,
    ActiveThreatChainStatusCounts,
    ActiveThreatPipelineMonitoringItem,
    ActiveThreatPipelineMonitoringResponse,
    CorrelationPipelineRef,
)
from apps.core.settings import settings
from apps.integrations.providers.socprime_tdm import SocPrimeTdmProvider, TdmActiveThreat, TdmRule
from apps.managers.correlation_pipelines import CorrelationPipelinesManager
from apps.managers.high_order_sigma_rule_checks import HighOrderSigmaRuleCheckService
from apps.managers.rule import RulesOrchestrator
from apps.managers.sigma_validation_service import SigmaValidationService
from apps.modules.kafka.active_threats import get_kafka_active_threats_service
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.active_threats import ActiveThreatDAO
from apps.modules.postgre.attack_chains import AttackChainsDAO
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO
from apps.modules.postgre.high_order_sigma import HighOrderSigmaDAO
from apps.modules.postgre.repository import SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY, RepositoryDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import extract_referenced_sigma_uuids, get_level_from_sigma_body, normalize_sigma_level

# Scalar string/text columns for ``?search=``. Technique IDs live on the ARRAY
# column ``techniques`` and are matched separately in ``ActiveThreatDAO._search_clause``.
_SEARCH_FIELDS = frozenset({"name", "severity"})
ACTIVE_THREAT_INITIAL_LOOKBACK_DAYS = 30
# Hard floor for the full-refresh ``updated_after`` cursor. A full refresh normally reaches
# back to the oldest stored AT; this clamps that floor since TDM's ``GET /v1/active-threats``
# rejects ``updated_after`` older than 90 days. We stay a day inside that limit so request
# latency / clock skew can't push the timestamp past the boundary (TDM returns
# 400 "updated_after cannot be older than ..." at exactly 90 days).
ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS = 89
SYNC_CURSOR_OVERLAP_MINUTES = 5

logger = get_logger(__name__)


def _chain_status_counts(counts: dict[str, int]) -> ActiveThreatChainStatusCounts:
    """``{"false-positive": 3, ...}`` -> the zero-filled seven-field breakdown.

    The wire statuses are hyphenated (``false-positive``, ``filtered-out``); the response fields are
    not. Every status is always present so the UI's bar never has to guess a missing segment.
    """
    fields = ActiveThreatChainStatusCounts.model_fields
    return ActiveThreatChainStatusCounts(
        **{
            status.replace("-", "_"): int(count)
            for status, count in counts.items()
            if status.replace("-", "_") in fields
        }
    )


def _bulk_response(
    rows: list[tuple[str, Literal["updated", "not_found", "blocked"]]],
    correlation_pipeline_ids: list[UUID],
) -> ActiveThreatBulkResponse:
    """Per-threat outcomes plus the pipeline set they were applied to.

    Results stay per THREAT, never per (threat, pipeline): 500 ids × N pipelines would be a
    response nobody can read.
    """
    results = [ActiveThreatBulkItemResult(threat_id=threat_id, status=status) for threat_id, status in rows]
    return ActiveThreatBulkResponse(
        results=results,
        updated=sum(1 for _, status in rows if status == "updated"),
        not_found=sum(1 for _, status in rows if status == "not_found"),
        blocked=sum(1 for _, status in rows if status == "blocked"),
        correlation_pipeline_ids=correlation_pipeline_ids,
        pipelines_applied=len(correlation_pipeline_ids),
    )


@dataclass(frozen=True)
class ActiveThreatSyncResult:
    """Summary for one Active Threat metadata sync run."""

    configured: bool
    updated_after: datetime | None
    fetched: int = 0
    upserted: int = 0
    retention_deleted: int = 0
    hos_fetched: int = 0
    hos_upserted: int = 0
    hos_deleted: int = 0
    referenced_rules_fetched: int = 0
    referenced_rules_upserted: int = 0
    referenced_rules_deleted: int = 0
    behavioral_rules_fetched: int = 0
    behavioral_rules_linked: int = 0
    hidden_in_tdm: int = 0


@dataclass(frozen=True)
class HighOrderSigmaSyncResult:
    fetched: int
    upserted: int
    deleted_tdm_ids: list[str]
    synced_tdm_ids: list[str]


@dataclass(frozen=True)
class ReferencedSystemRulesSyncResult:
    fetched: int
    upserted: int
    deleted: int
    behavioral_fetched: int = 0
    behavioral_linked: int = 0


class ActiveThreatsManager:
    """Catalogue reads plus the whole D8 monitoring surface: per-pipeline deny-list and the block.

    Monitoring is **opt-out**: ``monitored = NOT at.is_blocked AND at_id NOT IN pipeline.excluded``.
    The two conjuncts travel on two different channels and must not be confused:

    * ``NOT is_blocked`` is global vendor state and rides the SHARED ``detectflow.active-threats``
      topic through the frozen ``enabled`` key (``enabled = NOT is_blocked``);
    * the exclusions ride each pipeline's compacted ``detectflow.chains-config.<id>`` envelope as
      ``excluded_active_threat_ids``.

    No monitoring change ever restarts a Flink job — both channels are broadcast state.
    """

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = ActiveThreatDAO(db)
        self.chains_dao = AttackChainsDAO(db)
        self.correlation_pipeline_dao = CorrelationPipelineDAO(db)
        self.correlation_pipelines = CorrelationPipelinesManager(db)
        self.kafka = get_kafka_active_threats_service()

    # -------------------------------------------------------------------------
    # Reads
    # -------------------------------------------------------------------------

    async def get_all(
        self,
        pagination: dict,
        *,
        severity: list[str] | None = None,
        industries: list[str] | None = None,
        threat_types: list[str] | None = None,
        geographies: list[str] | None = None,
        correlation_pipeline_id: UUID | None = None,
    ) -> tuple[list[tuple[ActiveThreat, list[CorrelationPipelineRef]]], int]:
        """One page of threats, each with the correlation pipelines currently monitoring it.

        ``correlation_pipeline_id`` narrows the page to that pipeline's monitored set (D8).

        The ``pipelines`` column costs exactly two statements for the whole page regardless of its
        size — one bounded read of the correlation pipelines, one grouped exclusion lookup — and
        the set difference happens in Python. It must never become a per-row query.
        """
        threats, total = await self.dao.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination.get("search"),
            search_fields=_SEARCH_FIELDS,
            sort=pagination.get("sort"),
            order=pagination.get("order") or "asc",
            severity=severity,
            industries=industries,
            threat_types=threat_types,
            geographies=geographies,
            correlation_pipeline_id=correlation_pipeline_id,
        )
        return await self._hydrate_pipelines(threats), total

    async def _hydrate_pipelines(
        self, threats: list[ActiveThreat]
    ) -> list[tuple[ActiveThreat, list[CorrelationPipelineRef]]]:
        """Attach each threat's monitoring pipelines: all pipelines minus its exclusions."""
        if not threats:
            return []
        refs = await self.correlation_pipeline_dao.list_refs()
        if not refs:
            return [(threat, []) for threat in threats]
        excluded = await self.dao.get_excluded_pipeline_ids_by_threat_ids([threat.tdm_id for threat in threats])
        hydrated: list[tuple[ActiveThreat, list[CorrelationPipelineRef]]] = []
        for threat in threats:
            if threat.is_blocked:
                hydrated.append((threat, []))
                continue
            denied = excluded.get(threat.tdm_id, set())
            hydrated.append(
                (
                    threat,
                    [
                        CorrelationPipelineRef(id=pipeline_id, name=name)
                        for pipeline_id, name in refs
                        if pipeline_id not in denied
                    ],
                )
            )
        return hydrated

    async def get_by_tdm_id(self, tdm_id: str) -> ActiveThreat:
        threat = await self.dao.get_by_tdm_id(tdm_id)
        if not threat:
            raise NotFoundError(f"Active threat with id {tdm_id} not found")
        return threat

    async def get_detail(self, tdm_id: str) -> tuple[ActiveThreat, list[ActiveThreatPipelineMonitoringItem]]:
        """One threat plus EVERY correlation pipeline's monitoring state and chain breakdown."""
        threat = await self.get_by_tdm_id(tdm_id)
        return threat, await self._pipeline_monitoring_items(threat)

    async def _pipeline_monitoring_items(self, threat: ActiveThreat) -> list[ActiveThreatPipelineMonitoringItem]:
        """Every correlation pipeline, monitoring or not, with its chain-status breakdown.

        Four reads for N pipelines: the batched runtime snapshot (one bounded Kubernetes fan-out
        plus one health query), this threat's exclusion rows, ONE
        ``GROUP BY (active_threat_id, correlation_pipeline_id, status)`` aggregate, and ONE
        ``MIN(first_seen)`` / ``MAX(last_seen)`` window over the same grouping without status.
        """
        snapshots = await self.correlation_pipelines.list_runtime_snapshots()
        if not snapshots:
            return []
        denied = set(await self.dao.get_pipeline_ids_with_exclusion(threat.tdm_id))
        counts = await self.chains_dao.get_chain_status_counts_by_active_threat_and_pipeline([threat.tdm_id])
        windows = await self.chains_dao.get_chain_seen_window_by_active_threat_and_pipeline([threat.tdm_id])
        items: list[ActiveThreatPipelineMonitoringItem] = []
        for snapshot in snapshots:
            chain_counts = _chain_status_counts(counts.get((threat.tdm_id, snapshot.correlation_pipeline_id), {}))
            seen = windows.get((threat.tdm_id, snapshot.correlation_pipeline_id))
            items.append(
                ActiveThreatPipelineMonitoringItem(
                    correlation_pipeline_id=snapshot.correlation_pipeline_id,
                    name=snapshot.name,
                    jobs_status=snapshot.jobs_status,
                    runtime_state=snapshot.runtime_state,
                    monitored=not threat.is_blocked and snapshot.correlation_pipeline_id not in denied,
                    attack_chains_total=sum(chain_counts.model_dump().values()),
                    attack_chain_counts=chain_counts,
                    first_seen=seen[0] if seen else None,
                    last_seen=seen[1] if seen else None,
                )
            )
        return items

    # -------------------------------------------------------------------------
    # Monitoring changes (never restart a Flink job — hot broadcast state only)
    # -------------------------------------------------------------------------

    async def set_pipeline_monitoring(
        self,
        threat_id: str,
        correlation_pipeline_id: UUID,
        monitored: bool,
        user: User,
    ) -> ActiveThreatPipelineMonitoringResponse:
        """Set one threat's monitoring state in one correlation pipeline (idempotent).

        Writes or deletes a single exclusion row and republishes that pipeline's chains-config —
        only when a row actually changed, and never a restart.

        Raises:
            NotFoundError: Unknown threat or correlation pipeline.
            ConflictError: ``monitored=True`` on a threat blocked platform-wide; the block is the
                stronger gate and must be lifted first.
        """
        blocked = await self._require_visible_threat(threat_id)
        pipeline = await self.correlation_pipeline_dao.get_by_id(correlation_pipeline_id)
        if pipeline is None:
            raise NotFoundError(f"Correlation pipeline with id {correlation_pipeline_id} not found")
        if monitored and blocked:
            raise ConflictError(
                f"Active threat {threat_id} is blocked for all pipelines; unblock it before monitoring it here"
            )

        changed = bool(
            await self.dao.set_excluded([threat_id], [correlation_pipeline_id], excluded=not monitored, user_id=user.id)
        )
        if changed:
            await self.db.commit()
            await self._republish([correlation_pipeline_id])
            await activity_producer.log_action(
                action="update",
                entity_type="active_threat",
                entity_id=threat_id,
                user=user,
                details=(
                    f"User {user.email} {'started' if monitored else 'stopped'} monitoring active threat "
                    f"{threat_id} in correlation pipeline '{pipeline.name}'"
                ),
                changes={"monitored": {"old": not monitored, "new": monitored}},
                source="user",
            )
        return ActiveThreatPipelineMonitoringResponse(
            threat_id=threat_id,
            correlation_pipeline_id=correlation_pipeline_id,
            monitored=monitored,
            changed=changed,
        )

    async def bulk_set_monitoring(
        self,
        threat_ids: list[str],
        correlation_pipeline_ids: list[UUID],
        *,
        monitored: bool,
        user: User,
    ) -> ActiveThreatBulkResponse:
        """Apply one monitoring change to many threats across the selected correlation pipelines.

        One bulk statement, one activity entry, and a republish of exactly the pipelines whose
        stored deny-list moved. Threats blocked platform-wide are skipped and reported as
        ``blocked`` — this operation never touches ``is_blocked``.

        Raises:
            BadRequestError: An unknown correlation pipeline id was supplied.
        """
        pipeline_ids = await self._resolve_pipeline_ids(correlation_pipeline_ids)
        state = await self.dao.get_visible_blocked_state(threat_ids)

        rows: list[tuple[str, Literal["updated", "not_found", "blocked"]]] = []
        eligible: list[str] = []
        for threat_id in threat_ids:
            if threat_id not in state:
                rows.append((threat_id, "not_found"))
            elif state[threat_id]:
                rows.append((threat_id, "blocked"))
            else:
                rows.append((threat_id, "updated"))
                eligible.append(threat_id)

        affected: list[UUID] = []
        if eligible and pipeline_ids:
            affected = await self.dao.get_changed_pipeline_ids(eligible, pipeline_ids, excluded=not monitored)
            await self.dao.set_excluded(eligible, pipeline_ids, excluded=not monitored, user_id=user.id)
            await self.db.commit()
            await self._republish(affected)

        await activity_producer.log_action(
            action="update",
            entity_type="active_threat",
            user=user,
            details=(
                f"User {user.email} {'started' if monitored else 'stopped'} monitoring {len(eligible)} "
                f"active threat(s) in {len(pipeline_ids)} correlation pipeline(s); "
                f"{len(affected)} pipeline(s) republished"
            ),
            source="user",
        )
        return _bulk_response(rows, pipeline_ids)

    async def set_blocked(self, threat_id: str, blocked: bool, user: User) -> ActiveThreatBlockResponse:
        """Block or unblock a threat platform-wide (the "Block for all pipelines" switch).

        The two directions are deliberately asymmetric:

        * **block** flips the flag and republishes the threat on the shared active-threats topic
          only. Per-pipeline exclusions are PRESERVED, so a mistaken block is reversible without
          losing analyst intent, and no chains-config record is written;
        * **unblock** is what the ticket defines as "enabled in all existing pipelines and all
          future ones", so it is the transition that mutates per-pipeline state: every exclusion
          row for the threat is cleared and exactly those pipelines' chains-config is republished.

        Both directions are activity-logged. Raises ``NotFoundError`` for an unknown threat.
        """
        await self._require_visible_threat(threat_id)
        changed = await self.dao.set_blocked(threat_id, blocked)
        republished: list[UUID] = []
        if changed and not blocked:
            republished = await self.dao.clear_exclusions_for_threat(threat_id)
        if changed:
            await self.db.commit()
            # The shared catalogue topic carries this half of the rule as `enabled = NOT is_blocked`.
            await self.kafka.send_active_threats(await self.dao.get_by_tdm_ids([threat_id]))
            await self._republish(republished)
            await activity_producer.log_action(
                action="update",
                entity_type="active_threat",
                entity_id=threat_id,
                user=user,
                details=(
                    f"User {user.email} {'blocked' if blocked else 'unblocked'} active threat {threat_id} "
                    f"for all correlation pipelines"
                    + (f"; cleared exclusions in {len(republished)} pipeline(s)" if republished else "")
                ),
                changes={"is_blocked": {"old": not blocked, "new": blocked}},
                source="user",
            )
        return ActiveThreatBlockResponse(
            threat_id=threat_id,
            is_blocked=blocked,
            changed=changed,
            pipelines_republished=len(republished),
        )

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    async def _require_visible_threat(self, threat_id: str) -> bool:
        """Return the threat's ``is_blocked`` state, raising ``NotFoundError`` when it is unknown."""
        state = await self.dao.get_visible_blocked_state([threat_id])
        if threat_id not in state:
            raise NotFoundError(f"Active threat with id {threat_id} not found")
        return state[threat_id]

    async def _resolve_pipeline_ids(self, requested: list[UUID]) -> list[UUID]:
        """Validate the requested correlation pipelines and deduplicate them.

        There is no "all pipelines" fallback: the field is required (``min_length=1``), so a caller
        always states which customers' monitoring it is changing. It was optional for one release,
        while the frontend could not send a field the backend did not yet accept
        (``ActiveThreatBulkIdsRequest`` is ``extra="forbid"``); an omitted field then meant *every*
        pipeline, which is the mistake this signature makes unrepresentable.

        Raises:
            BadRequestError: An id does not name a correlation pipeline.
        """
        known = dict(await self.correlation_pipeline_dao.list_refs())
        unknown = sorted({str(pipeline_id) for pipeline_id in requested if pipeline_id not in known})
        if unknown:
            raise BadRequestError(f"Unknown correlation pipeline ids: {', '.join(unknown)}")
        return list(dict.fromkeys(requested))

    async def _republish(self, correlation_pipeline_ids: Iterable[UUID]) -> None:
        """Publish the new deny-list to exactly these pipelines' chains-config topics.

        Hot broadcast state: no Flink job is restarted, and a publish failure is logged by
        ``republish_chains_config`` rather than failing the operator's request.
        """
        ids = list(correlation_pipeline_ids)
        if not ids:
            return
        await self.correlation_pipelines.republish_chains_config(ids, reason="active_threat_monitoring_changed")


class ActiveThreatsSyncManager:
    """Synchronize Active Threat metadata, HOS rows, and referenced Sigma rules from TDM."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = ActiveThreatDAO(db)
        self.config_dao = ConfigDAO(db)
        self.hos_dao = HighOrderSigmaDAO(db)
        self.repository_dao = RepositoryDAO(db)
        self.rule_dao = RuleDAO(db)
        self.correlation_pipeline_dao = CorrelationPipelineDAO(db)
        self.rule_check_service = HighOrderSigmaRuleCheckService(db)
        self.kafka = get_kafka_active_threats_service()
        # ``{correlation_pipeline_id: {active threat tdm_id, ...}}`` captured before each delete
        # path cascades its exclusion rows away. One sync run per manager instance.
        self._dropped_exclusions: dict[UUID, set[str]] = {}

    async def sync_active_threats(
        self, full_refresh: bool = False, provider: SocPrimeTdmProvider | None = None
    ) -> ActiveThreatSyncResult:
        """Fetch created/updated Active Threats and upsert them locally.

        Args:
            full_refresh: When ``True``, ignore the per-entity incremental update cursors
                and re-fetch everything: Active Threats across the full TDM lookback window
                (:data:`ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS`) and *all* locally known HOS
                bodies and referenced/behavioral rules regardless of their TDM update date.
                Use after deploying logic changes or to pick up a HOS/rule that became
                available in TDM without its Active Threat changing. The default (``False``)
                runs the cheap incremental sync used by the scheduler.
            provider: Content source; defaults to the configured TDM integration. The bundled
                content snapshot (``apps.modules.postgre.init_content``) passes its own here.
        """
        if provider is None:
            provider_config = await self.config_dao.get_socprime_tdm_provider_config()
            if not provider_config:
                logger.info("Skipping Active Threat sync: socprime_tdm integration is not configured")
                return ActiveThreatSyncResult(configured=False, updated_after=None)
            provider = SocPrimeTdmProvider(provider_config)

        updated_after = await self._get_updated_after(full_refresh=full_refresh)
        last_hos_updated = None if full_refresh else self._with_overlap(await self.hos_dao.get_latest_tdm_updated())
        system_repo = await self.repository_dao.get_system_repository(SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY)
        if full_refresh or not system_repo:
            last_system_rule_updated = None
        else:
            last_system_rule_updated = self._with_overlap(
                await self.rule_dao.get_latest_updated_by_repository_id(
                    system_repo.id,
                    exclude_user_edited_system_rules=True,
                )
            )

        threats = await provider.get_active_threats(updated_after=updated_after)
        rows = [self._to_row(threat) for threat in threats]
        unhidden_at_tdm_ids = await self.dao.upsert_many(rows)
        hidden_in_tdm_ids: list[str] = []
        if settings.sync_delete_at_missing_in_tdm:
            deleted_in_tdm_ids = await self._remove_deleted_active_threats(provider)
        else:
            deleted_in_tdm_ids = []
            hidden_in_tdm_ids = await self._hide_missing_active_threats(provider)
        retention_cutoff = self._build_retention_cutoff(
            now=datetime.now(UTC),
            retention_days=settings.sync_active_threats_retention_days,
        )
        retention_deleted_ids = await self._apply_retention(retention_cutoff)
        retention_deleted = len(retention_deleted_ids)
        retained_threats = self._filter_retained_threats(threats, retention_cutoff)
        hos_sync = await self._sync_high_order_sigmas(
            provider=provider,
            threats=retained_threats,
            existing_hos_updated_after=last_hos_updated,
        )
        hos_deleted = len(hos_sync.deleted_tdm_ids)
        behavioral_rules_by_active_threat_id = self._collect_behavioral_rules_by_active_threat_id(retained_threats)
        referenced_rules_sync = await self._sync_referenced_system_rules(
            provider=provider,
            existing_rule_updated_after=last_system_rule_updated,
            behavioral_rules_by_active_threat_id=behavioral_rules_by_active_threat_id,
        )
        deleted_active_threat_ids = sorted(set(deleted_in_tdm_ids) | set(retention_deleted_ids))
        await self._sync_active_threats_to_kafka([threat.id for threat in threats], deleted_active_threat_ids)
        await self._republish_chains_config_for_deleted_threats(deleted_active_threat_ids)
        # HOS bodies whose ATs just transitioned from hidden→visible may not appear in
        # ``hos_sync.synced_tdm_ids`` (their TDM body could be unchanged since the cursor),
        # so explicitly include them so the correlation job sees the freshly-visible HOS.
        unhidden_hos_tdm_ids = self._collect_hos_for_active_threats(threats, unhidden_at_tdm_ids)
        hos_to_publish = sorted(set(hos_sync.synced_tdm_ids) | unhidden_hos_tdm_ids)
        await self._sync_hos_to_kafka(hos_to_publish, hos_sync.deleted_tdm_ids)

        logger.info(
            (
                "Active Threat sync completed (full_refresh=%s): fetched=%s, upserted=%s, hos_fetched=%s, "
                "hos_upserted=%s, hos_deleted=%s, retention_deleted=%s, deleted_in_tdm=%s, hidden_in_tdm=%s, "
                "referenced_rules_fetched=%s, referenced_rules_upserted=%s, "
                "referenced_rules_deleted=%s, behavioral_rules_fetched=%s, "
                "behavioral_rules_linked=%s, updated_after=%s"
            ),
            full_refresh,
            len(threats),
            len(rows),
            hos_sync.fetched,
            hos_sync.upserted,
            hos_deleted,
            retention_deleted,
            len(deleted_in_tdm_ids),
            len(hidden_in_tdm_ids),
            referenced_rules_sync.fetched,
            referenced_rules_sync.upserted,
            referenced_rules_sync.deleted,
            referenced_rules_sync.behavioral_fetched,
            referenced_rules_sync.behavioral_linked,
            updated_after.isoformat(),
        )
        return ActiveThreatSyncResult(
            configured=True,
            updated_after=updated_after,
            fetched=len(threats),
            upserted=len(rows),
            retention_deleted=retention_deleted,
            hos_fetched=hos_sync.fetched,
            hos_upserted=hos_sync.upserted,
            hos_deleted=hos_deleted,
            referenced_rules_fetched=referenced_rules_sync.fetched,
            referenced_rules_upserted=referenced_rules_sync.upserted,
            referenced_rules_deleted=referenced_rules_sync.deleted,
            behavioral_rules_fetched=referenced_rules_sync.behavioral_fetched,
            behavioral_rules_linked=referenced_rules_sync.behavioral_linked,
            hidden_in_tdm=len(hidden_in_tdm_ids),
        )

    async def _republish_chains_config_for_deleted_threats(self, deleted_tdm_ids: list[str]) -> None:
        """Refresh every affected pipeline's deny-list after the catalogue sync erased threat rows.

        ``_remove_deleted_active_threats`` and ``_apply_retention`` are the only two paths that
        DELETE ``active_threats`` rows (``_hide_missing_active_threats`` only flips a flag and must
        NOT land here). Their ``ON DELETE CASCADE`` takes the per-pipeline exclusion rows with them,
        so a pipeline's ``excluded_active_threat_ids`` silently shrinks while its published envelope
        still names the vanished threat.

        Restart-free by construction — the envelope is broadcast state. Best-effort: a sync must not
        fail because a config republish did.
        """
        if not deleted_tdm_ids:
            return
        manager = CorrelationPipelinesManager(self.db)
        affected = await self._correlation_pipelines_for_threats(deleted_tdm_ids)
        if not affected:
            return
        await manager.republish_chains_config(affected, reason="active_threat_catalogue_deleted")

    async def _correlation_pipelines_for_threats(self, deleted_tdm_ids: list[str]) -> list[UUID]:
        """Correlation pipelines whose exclusion list referenced any of these threats.

        Reads the map captured by :meth:`_capture_dropped_exclusions` rather than the table: by the
        time this runs the rows have already cascaded away, so a live query would always be empty.
        """
        doomed = set(deleted_tdm_ids)
        return [
            correlation_pipeline_id
            for correlation_pipeline_id, threat_ids in self._dropped_exclusions.items()
            if threat_ids & doomed
        ]

    async def _capture_dropped_exclusions(self, doomed_tdm_ids: list[str]) -> None:
        """Record and announce the exclusions a catalogue delete is about to cascade away.

        Must run BEFORE the DELETE. ``ON DELETE CASCADE`` on ``active_threat_tdm_id`` erases
        analyst intent with no trace, and if the threat later reappears in TDM it comes back
        MONITORED in a pipeline that had deliberately excluded it. CASCADE stays (RESTRICT would
        make retention fail, a soft delete would leak rows) — instead the loss is made visible with
        one warning activity entry per affected correlation pipeline, and the caller republishes
        those pipelines afterwards because their deny-list genuinely shrank.
        """
        if not doomed_tdm_ids:
            return
        by_threat = await self.dao.get_excluded_pipeline_ids_by_threat_ids(doomed_tdm_ids)
        if not by_threat:
            return
        dropped: dict[UUID, set[str]] = {}
        for threat_id, correlation_pipeline_ids in by_threat.items():
            for correlation_pipeline_id in correlation_pipeline_ids:
                dropped.setdefault(correlation_pipeline_id, set()).add(threat_id)
        names = dict(await self.correlation_pipeline_dao.list_refs())
        for correlation_pipeline_id, threat_ids in dropped.items():
            self._dropped_exclusions.setdefault(correlation_pipeline_id, set()).update(threat_ids)
            await activity_producer.log_action(
                action="update",
                entity_type="correlation_pipeline",
                entity_id=str(correlation_pipeline_id),
                entity_name=names.get(correlation_pipeline_id),
                details=(
                    "Active Threat catalogue deletion dropped monitoring exclusions for "
                    f"{', '.join(sorted(threat_ids))}; those threats are monitored again if they reappear"
                ),
                source="system",
                severity="warning",
            )

    async def _remove_deleted_active_threats(self, provider: SocPrimeTdmProvider) -> list[str]:
        """Delete local Active Threats missing from TDM and return deleted ids."""
        local_ids = await self.dao.get_all_tdm_ids()
        if not local_ids:
            return []

        existing_ids = await provider.check_active_threats_exist(sorted(local_ids))
        deleted_ids = sorted(local_ids - existing_ids)
        if not deleted_ids:
            return []

        await self._capture_dropped_exclusions(deleted_ids)
        await self.dao.delete_by_tdm_ids(deleted_ids)
        logger.info("Deleted %s Active Threat rows missing in TDM", len(deleted_ids))
        return deleted_ids

    async def _hide_missing_active_threats(self, provider: SocPrimeTdmProvider) -> list[str]:
        """Mark local Active Threats missing from TDM hidden and return changed ids."""
        local_ids = await self.dao.get_all_tdm_ids()
        if not local_ids:
            return []

        existing_ids = await provider.check_active_threats_exist(sorted(local_ids))
        missing_ids = sorted(local_ids - existing_ids)
        if not missing_ids:
            return []

        hidden_ids = await self.dao.set_hidden_by_tdm_ids(missing_ids, hidden=True)
        if hidden_ids:
            logger.info("Marked %s Active Threat rows missing in TDM as hidden", len(hidden_ids))
        return hidden_ids

    async def _apply_retention(self, retention_cutoff: datetime | None) -> list[str]:
        """Delete Active Threats past the retention window; capture their exclusions first."""
        if retention_cutoff is None:
            return []
        doomed_ids = await self.dao.get_ids_older_than_tdm_created(retention_cutoff)
        if not doomed_ids:
            return []
        await self._capture_dropped_exclusions(doomed_ids)
        deleted = await self.dao.delete_by_tdm_ids(doomed_ids)
        logger.info(
            "Active Threat retention cleanup removed %s rows older than %s",
            deleted,
            retention_cutoff.isoformat(),
        )
        return doomed_ids

    @staticmethod
    def _build_retention_cutoff(now: datetime, retention_days: int) -> datetime | None:
        if retention_days <= 0:
            logger.warning(
                "Active Threat retention disabled: SYNC_ACTIVE_THREATS_RETENTION_DAYS=%s",
                retention_days,
            )
            return None
        return now - timedelta(days=retention_days)

    @staticmethod
    def _filter_retained_threats(
        threats: list[TdmActiveThreat],
        retention_cutoff: datetime | None,
    ) -> list[TdmActiveThreat]:
        if retention_cutoff is None:
            return threats
        retained: list[TdmActiveThreat] = []
        for threat in threats:
            if threat.created is None:
                retained.append(threat)
                continue
            threat_created = ActiveThreatsSyncManager._ensure_utc(threat.created)
            if threat_created >= retention_cutoff:
                retained.append(threat)
        return retained

    async def _get_updated_after(self, full_refresh: bool = False) -> datetime:
        if full_refresh:
            return await self._get_full_refresh_updated_after()
        now = datetime.now(UTC)
        latest = await self.dao.get_latest_tdm_updated()
        if latest is not None and latest < now:
            # A sync paused for 3+ months (or a catalog seeded from an old content snapshot)
            # resumes from TDM's 90-day limit instead of a 400: older changes are skipped.
            return max(self._with_overlap(latest), now - timedelta(days=ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS))
        return now - timedelta(days=ACTIVE_THREAT_INITIAL_LOOKBACK_DAYS)

    async def _get_full_refresh_updated_after(self) -> datetime:
        """Lower bound for a full re-sync: re-fetch exactly the ATs already in the catalog.

        Uses the oldest stored ``tdm_updated`` (minus the cursor overlap) so the full refresh
        covers every locally known Active Threat without reaching further back to pull in ATs
        that were never fetched (e.g. updated before the initial seed window). The result is
        clamped to ``ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS`` so we never send TDM an
        ``updated_after`` past its hard 90-day limit. With an empty catalog there is nothing to
        refresh, so it falls back to the initial seed window.
        """
        now = datetime.now(UTC)
        tdm_limit = now - timedelta(days=ACTIVE_THREAT_FULL_REFRESH_LOOKBACK_DAYS)
        earliest = await self.dao.get_earliest_tdm_updated()
        if earliest is None:
            return now - timedelta(days=ACTIVE_THREAT_INITIAL_LOOKBACK_DAYS)
        return max(self._with_overlap(earliest), tdm_limit)

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _with_overlap(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return cls._ensure_utc(value) - timedelta(minutes=SYNC_CURSOR_OVERLAP_MINUTES)

    async def _sync_high_order_sigmas(
        self,
        provider: SocPrimeTdmProvider,
        threats: list[TdmActiveThreat],
        existing_hos_updated_after: datetime | None,
    ) -> HighOrderSigmaSyncResult:
        """Fetch HOS bodies, refresh AT↔HOS memberships, rebuild Sigma UUID refs, drop orphans.

        ``existing_hos_updated_after`` lets us refresh already known HOS only if
        TDM changed them after our latest local HOS update, instead of fetching
        every stored HOS on every sync.
        """
        related_hos_by_active_threat_id = self._collect_related_hos_by_active_threat_id(threats)
        all_related_hos_tdm_ids = {hos for ids in related_hos_by_active_threat_id.values() for hos in ids}
        local_hos_ids = await self.hos_dao.get_all_tdm_ids()
        user_edited_hos_ids = await self.hos_dao.get_user_edited_tdm_ids(local_hos_ids)
        new_hos_ids = all_related_hos_tdm_ids - local_hos_ids
        syncable_existing_hos_ids = local_hos_ids - user_edited_hos_ids

        hos_rules = await self._fetch_hos_rules(
            provider=provider,
            new_hos_ids=new_hos_ids,
            existing_hos_ids=syncable_existing_hos_ids,
            existing_hos_updated_after=existing_hos_updated_after,
        )

        hos_rules_by_id = {rule.id: rule for rule in hos_rules}
        rows = [self._hos_to_row(hos_rule) for hos_rule in hos_rules_by_id.values()]
        changed_hos_tdm_ids = await self.hos_dao.upsert_many(rows)

        hos_ids_by_tdm_id = await self.hos_dao.get_ids_by_tdm_ids(all_related_hos_tdm_ids | set(hos_rules_by_id))

        memberships: dict[str, set[UUID]] = {}
        for active_threat_id, hos_tdm_ids in related_hos_by_active_threat_id.items():
            memberships[active_threat_id] = {
                hos_ids_by_tdm_id[tdm_id] for tdm_id in hos_tdm_ids if tdm_id in hos_ids_by_tdm_id
            }
        await self.hos_dao.replace_memberships_for_active_threats(memberships)

        touched_hos_ids: list[UUID] = []
        for hos_rule in hos_rules_by_id.values():
            if hos_rule.id in user_edited_hos_ids:
                continue
            hos_id = hos_ids_by_tdm_id.get(hos_rule.id)
            if not hos_id:
                continue
            await self.hos_dao.replace_rule_refs(hos_id, extract_referenced_sigma_uuids(hos_rule.body))

        deleted_hos_tdm_ids = []
        if settings.sync_delete_orphan_hos:
            deleted_hos_tdm_ids = await self.hos_dao.delete_orphan_hos()
        await self.rule_check_service.refresh_for_hos_ids(touched_hos_ids)
        return HighOrderSigmaSyncResult(
            fetched=len(hos_rules_by_id),
            upserted=len(changed_hos_tdm_ids),
            deleted_tdm_ids=deleted_hos_tdm_ids,
            synced_tdm_ids=sorted(changed_hos_tdm_ids),
        )

    async def _sync_active_threats_to_kafka(self, upserted_ids: list[str], deleted_ids: list[str]) -> None:
        if upserted_ids:
            active_threats = await self.dao.get_by_tdm_ids(upserted_ids)
            await self.kafka.send_active_threats(active_threats)
        if deleted_ids:
            await self.kafka.delete_active_threats(deleted_ids)

    async def _sync_hos_to_kafka(self, upserted_hos_ids: list[str], deleted_hos_ids: list[str]) -> None:
        """Publish/tombstone only the HOS delta against the recorded Kafka state.

        Each HOS row carries ``is_kafka_tombstoned`` reflecting its last Kafka write
        (``True`` after a tombstone, ``False`` after a publish or before any write).
        The publish/tombstone candidate sets are computed from that state plus
        current AT visibility, so steady-state runs do not re-emit data that's
        already in the correct shape on the compacted Kafka topic.

        Publish set:
          * Caller-provided ``upserted_hos_ids`` (HOS just refreshed from TDM or whose
            ATs just transitioned hidden→visible), filtered to those currently visible.
          * HOS that are currently visible but marked ``is_kafka_tombstoned=True``
            (became visible via AT-membership changes outside the upserted batch).

        Tombstone set:
          * Caller-provided ``deleted_hos_ids`` (orphan-deleted rows — DB row is gone
            so no flag update is possible; tombstone is sent unconditionally).
          * HOS that are currently invisible but marked ``is_kafka_tombstoned=False``
            (lost their last non-hidden AT since the previous run, or have never
            been written).

        DB rows are never deleted here; user-edited HOS bodies stay in the DB so they
        are republished automatically the next time an AT becomes visible for them.
        """
        publish_candidates = set(upserted_hos_ids) | await self.hos_dao.get_pending_kafka_publish_tdm_ids()
        if publish_candidates:
            visible_tdm_ids = await self.hos_dao.get_tdm_ids_with_non_hidden_at(publish_candidates)
            visible_ids_sorted = sorted(visible_tdm_ids)
            if visible_ids_sorted:
                hos_rows = await self.hos_dao.get_by_tdm_ids(visible_ids_sorted)
                active_threat_ids_by_hos_tdm_id = await self.hos_dao.get_active_threat_ids_by_hos_tdm_ids(
                    visible_ids_sorted,
                )
                await self.kafka.send_hos(hos_rows, active_threat_ids_by_hos_tdm_id)
                await self.hos_dao.set_kafka_tombstoned(visible_ids_sorted, tombstoned=False)

        pending_invisible = await self.hos_dao.get_pending_kafka_tombstone_tdm_ids()
        kafka_delete_ids = sorted(set(deleted_hos_ids) | pending_invisible)
        if kafka_delete_ids:
            await self.kafka.delete_hos(kafka_delete_ids)
            if pending_invisible:
                # ``deleted_hos_ids`` rows are gone from the DB so set_kafka_tombstoned
                # silently skips them; only update the flag for rows still present.
                await self.hos_dao.set_kafka_tombstoned(sorted(pending_invisible), tombstoned=True)

    async def _fetch_hos_rules(
        self,
        provider: SocPrimeTdmProvider,
        new_hos_ids: set[str],
        existing_hos_ids: set[str],
        existing_hos_updated_after: datetime | None,
    ) -> list[TdmRule]:
        rules: list[TdmRule] = []
        if new_hos_ids:
            rules.extend(await provider.search_rules_by_case_ids(sorted(new_hos_ids), siem_type="hos"))
        if existing_hos_ids:
            rules.extend(
                await provider.search_rules_by_case_ids(
                    sorted(existing_hos_ids),
                    siem_type="hos",
                    updated_after=existing_hos_updated_after,
                )
            )
        return rules

    async def _sync_referenced_system_rules(
        self,
        provider: SocPrimeTdmProvider,
        existing_rule_updated_after: datetime | None,
        behavioral_rules_by_active_threat_id: dict[str, set[str]] | None = None,
    ) -> ReferencedSystemRulesSyncResult:
        """Sync Sigma rules referenced by HOS and by AT behavioral relations into the system repo.

        Both HOS-referenced rules (by Sigma UUID) and behavioral rules (by TDM case id)
        live in the same system repository and are upserted together so duplicates collapse
        onto a single row (the upsert matches existing rows by ``external_id`` or ``sigma_uuid``).

        ``existing_rule_updated_after`` lets us refresh already stored system rules only if
        TDM changed them after our latest local rule update, while newly referenced content
        (new Sigma UUIDs / new case ids) is fetched without a date filter.

        Cleanup deletes rules no longer referenced by any HOS Sigma UUID and not protected by
        a behavioral membership, so behavioral rows survive even when their ``sigma_uuid`` is NULL.
        """
        behavioral_rules_by_active_threat_id = behavioral_rules_by_active_threat_id or {}
        system_repo = await self.repository_dao.get_system_repository(SYSTEM_ACTIVE_THREAT_SIGMA_REPOSITORY_KEY)
        if not system_repo:
            logger.warning("Skipping referenced Sigma sync: active threat system repository is missing")
            return ReferencedSystemRulesSyncResult(fetched=0, upserted=0, deleted=0)

        referenced_sigma_uuids = await self.hos_dao.get_all_referenced_sigma_uuids()
        behavioral_case_ids = {cid for case_ids in behavioral_rules_by_active_threat_id.values() for cid in case_ids}

        existing_sigma_uuids = await self.rule_dao.get_sigma_uuids_by_repository_id(system_repo.id)
        new_sigma_uuids = referenced_sigma_uuids - existing_sigma_uuids
        existing_referenced_sigma_uuids = referenced_sigma_uuids & existing_sigma_uuids

        existing_external_ids = await self.rule_dao.get_external_ids_by_repository_id(system_repo.id)

        rules: list[TdmRule] = []
        if new_sigma_uuids:
            rules.extend(await provider.search_rules_by_sigma_uuids(sorted(new_sigma_uuids)))
        if existing_referenced_sigma_uuids:
            rules.extend(
                await provider.search_rules_by_sigma_uuids(
                    sorted(existing_referenced_sigma_uuids),
                    updated_after=existing_rule_updated_after,
                )
            )
        behavioral_rules = await self._fetch_behavioral_rules(
            provider=provider,
            behavioral_case_ids=behavioral_case_ids,
            existing_external_ids=existing_external_ids,
            existing_rule_updated_after=existing_rule_updated_after,
        )
        rules.extend(behavioral_rules)

        rules_by_id = {rule.id: rule for rule in rules}
        rows = [self._system_rule_to_row(rule) for rule in rules_by_id.values()]
        new_rule_ids, updated_rule_ids = await self.rule_dao.upsert_system_repository_rules(system_repo.id, rows)

        behavioral_linked = await self._sync_behavioral_memberships(
            behavioral_rules_by_active_threat_id=behavioral_rules_by_active_threat_id,
        )

        deleted_rule_ids = []
        if settings.sync_delete_unreferenced_system_rules:
            protected_external_ids = await self.dao.get_all_behavioral_external_ids()
            deleted_rule_ids = await self.rule_dao.delete_unreferenced_by_repository_id(
                system_repo.id, referenced_sigma_uuids, protected_external_ids
            )

        await self._validate_system_rules(new_rule_ids + updated_rule_ids)
        await self._propagate_system_rule_changes_to_pipelines(
            repository_id=system_repo.id,
            new_rule_ids=new_rule_ids,
            updated_rule_ids=updated_rule_ids,
            deleted_rule_ids=deleted_rule_ids,
        )

        return ReferencedSystemRulesSyncResult(
            fetched=len(rules_by_id),
            upserted=len(new_rule_ids) + len(updated_rule_ids),
            deleted=len(deleted_rule_ids),
            behavioral_fetched=len({rule.id for rule in behavioral_rules}),
            behavioral_linked=behavioral_linked,
        )

    async def _fetch_behavioral_rules(
        self,
        provider: SocPrimeTdmProvider,
        behavioral_case_ids: set[str],
        existing_external_ids: set[str],
        existing_rule_updated_after: datetime | None,
    ) -> list[TdmRule]:
        """Fetch behavioral Sigma rules by TDM case id (new without date filter, existing incrementally)."""
        if not behavioral_case_ids:
            return []
        new_case_ids = behavioral_case_ids - existing_external_ids
        existing_case_ids = behavioral_case_ids & existing_external_ids
        rules: list[TdmRule] = []
        if new_case_ids:
            rules.extend(await provider.search_rules_by_case_ids(sorted(new_case_ids), siem_type="sigma"))
        if existing_case_ids:
            rules.extend(
                await provider.search_rules_by_case_ids(
                    sorted(existing_case_ids),
                    siem_type="sigma",
                    updated_after=existing_rule_updated_after,
                )
            )
        return rules

    async def _sync_behavioral_memberships(
        self,
        behavioral_rules_by_active_threat_id: dict[str, set[str]],
    ) -> int:
        """Replace AT->behavioral memberships with TDM case ids verbatim.

        The relation is stored as ``(active_threat_id, external_id)`` directly from TDM —
        no resolution to a local rule id — so it is recorded even before the referenced rule
        lands in the system repo (the read path resolves case id -> rule against that repo).

        Returns the total number of membership rows written across all Active Threats.
        """
        if not behavioral_rules_by_active_threat_id:
            return 0
        await self.dao.replace_behavioral_memberships_for_active_threats(behavioral_rules_by_active_threat_id)
        return sum(len(case_ids) for case_ids in behavioral_rules_by_active_threat_id.values())

    async def _validate_system_rules(self, rule_ids: list[UUID]) -> None:
        """Validate upserted/updated system rules so ``is_supported`` reflects their current state."""
        if not rule_ids:
            return
        db_rules = await self.rule_dao.get_by_ids(rule_ids)
        if not db_rules:
            return
        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rules_batch(db_rules, force=True)

    async def _propagate_system_rule_changes_to_pipelines(
        self,
        repository_id: UUID,
        new_rule_ids: list[UUID],
        updated_rule_ids: list[UUID],
        deleted_rule_ids: list[UUID],
    ) -> None:
        """Mirror system repo rule changes to ``pipeline_rules`` and Kafka for connected pipelines."""
        if not (new_rule_ids or updated_rule_ids or deleted_rule_ids):
            return
        rules_orchestrator = RulesOrchestrator(self.db)
        await rules_orchestrator.sync_system_repository_changes(
            repository_id=repository_id,
            new_rule_ids=new_rule_ids,
            updated_rule_ids=updated_rule_ids,
            deleted_rule_ids=deleted_rule_ids,
        )

    @staticmethod
    def _collect_related_hos_by_active_threat_id(threats: list[TdmActiveThreat]) -> dict[str, set[str]]:
        related: dict[str, set[str]] = {}
        for threat in threats:
            related[threat.id] = {hos_id for hos_id in threat.related_high_order_sigma_rules if hos_id}
        return related

    @staticmethod
    def _collect_behavioral_rules_by_active_threat_id(threats: list[TdmActiveThreat]) -> dict[str, set[str]]:
        """Map each Active Threat id to its set of behavioral rule TDM case ids (empty set allowed).

        Empty sets are kept so ``_sync_behavioral_memberships`` clears stale memberships for
        threats that no longer reference any behavioral rule.
        """
        related: dict[str, set[str]] = {}
        for threat in threats:
            related[threat.id] = {case_id for case_id in threat.related_behavioral_rules if case_id}
        return related

    @staticmethod
    def _collect_hos_for_active_threats(
        threats: list[TdmActiveThreat],
        active_threat_ids: list[str],
    ) -> set[str]:
        if not active_threat_ids:
            return set()
        target_ids = set(active_threat_ids)
        hos_ids: set[str] = set()
        for threat in threats:
            if threat.id in target_ids:
                hos_ids.update(hos_id for hos_id in threat.related_high_order_sigma_rules if hos_id)
        return hos_ids

    @staticmethod
    def _to_row(threat: TdmActiveThreat) -> dict[str, Any]:
        return {
            "tdm_id": threat.id,
            "name": threat.name,
            "summary": threat.summary,
            "severity": threat.severity,
            "investigation": threat.investigation,
            "response": threat.response,
            "mitigation": threat.mitigation,
            "industries": threat.industries,
            "geographies": threat.geographies,
            "threat_types": threat.threat_types,
            "threat_actors": threat.threat_actors,
            "techniques": threat.techniques,
            "attack_flow": threat.attack_flow,
            "tdm_created": threat.created,
            "tdm_updated": threat.updated,
        }

    @staticmethod
    def _hos_to_row(hos_rule: TdmRule) -> dict[str, Any]:
        # TDM reports the level structurally; fall back to the body for rows where it is absent
        # (same precedence the Kafka rules producer uses for TdmRule).
        return {
            "tdm_id": hos_rule.id,
            "name": hos_rule.name,
            "body": hos_rule.body,
            "level": normalize_sigma_level(hos_rule.level) or get_level_from_sigma_body(hos_rule.body),
            "tdm_created": hos_rule.created,
            "tdm_updated": hos_rule.updated,
        }

    @staticmethod
    def _system_rule_to_row(rule: TdmRule) -> dict[str, Any]:
        return {
            "external_id": rule.id,
            "name": rule.name,
            "body": rule.body,
            "created": rule.created,
            "updated": rule.updated,
        }
