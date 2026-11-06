"""Response converters for model-to-schema transformations.

This module provides centralized converter functions for converting
database models to Pydantic response schemas. Eliminates duplication
of conversion logic across routers.
"""

from uuid import UUID

from apps.core.models import (
    ActiveThreat,
    AttackChain,
    Filter,
    HighOrderSigma,
    Integration,
    LogSource,
    Rule,
    RuleContent,
    User,
)
from apps.core.schemas import (
    ActiveThreatLinkedRuleResponse,
    ActiveThreatListItemResponse,
    ActiveThreatPipelineMonitoringItem,
    ActiveThreatResponse,
    AttackChainDetailResponse,
    AttackChainResponse,
    CorrelationPipelineRef,
    FilterActiveResponse,
    FilterDetailResponse,
    FilterFullResponse,
    HighOrderSigmaDetailResponse,
    HighOrderSigmaListItemResponse,
    IntegrationDetailResponse,
    IntegrationListItem,
    LogSourceResponse,
    MitreIdNameRef,
    RuleDetailResponse,
    RuleFullDetailResponse,
    RuleIssueResponse,
    RuleVersionDetailResponse,
    RuleVersionItem,
    UserDetailResponse,
)
from apps.core.settings import settings
from apps.integrations.base import BaseIntegrationProvider, get_provider_capabilities
from apps.services.mitre_attack import mitre_attack

# Repository types whose Sigma bodies stay visible when ``hide_restricted_rule_bodies`` is on.
# ``external`` is the opensource GitHub catalog only (see ``EXTERNAL_REPOSITORIES``).
_RULE_BODY_VISIBLE_REPO_TYPES = frozenset({"local", "external"})


class RuleConverter:
    """Converter for Rule model to response schemas."""

    @staticmethod
    def _api_rule_body(body: str, repository_type: str | None) -> str:
        """Return body for HTTP responses, optionally redacting restricted repos."""
        if not settings.hide_restricted_rule_bodies:
            return body
        if repository_type in _RULE_BODY_VISIBLE_REPO_TYPES:
            return body
        return ""

    @staticmethod
    def to_detail(rule: Rule) -> RuleDetailResponse:
        """Convert Rule to RuleDetailResponse."""
        return RuleDetailResponse(
            id=str(rule.id),
            name=rule.name,
            version=rule.version,
            repository_id=str(rule.repository_id),
            repository_type=rule.repository.type if rule.repository else "",
            repository_name=rule.repository.name if rule.repository else "",
            product=rule.product if rule.product else None,
            service=rule.service if rule.service else None,
            category=rule.category if rule.category else None,
            folder_path=rule.folder_path,
            sigma_uuid=str(rule.sigma_uuid) if rule.sigma_uuid else None,
            level=rule.level,
            external_id=rule.external_id,
            created=rule.created,
            updated=rule.updated,
            is_supported=rule.is_supported,
            unsupported_reason=rule.unsupported_reason,
            is_user_edited_system_rule=bool(rule.is_user_edited_system_rule),
        )

    @staticmethod
    def to_full_detail(rule: Rule, versions: list[RuleContent] | None = None) -> RuleFullDetailResponse:
        """Convert Rule to RuleFullDetailResponse (includes body, content_hash, optional versions)."""
        detail = RuleConverter.to_detail(rule)
        version_items = [RuleConverter.to_version_item(v) for v in versions] if versions is not None else None
        repository_type = rule.repository.type if rule.repository else None
        return RuleFullDetailResponse(
            **detail.model_dump(),
            body=RuleConverter._api_rule_body(rule.body, repository_type),
            content_hash=rule.content_hash,
            versions=version_items,
        )

    @staticmethod
    def to_version_item(content: RuleContent) -> RuleVersionItem:
        """Convert RuleContent to RuleVersionItem (metadata only)."""
        return RuleVersionItem(
            version=content.version,
            content_hash=content.content_hash,
            changed_at=content.changed_at,
            change_source=content.change_source,
            changed_by=content.changed_by,
        )

    @staticmethod
    def to_version_detail(
        content: RuleContent,
        repository_type: str | None = None,
    ) -> RuleVersionDetailResponse:
        """Convert RuleContent to RuleVersionDetailResponse (with body)."""
        return RuleVersionDetailResponse(
            rule_id=str(content.rule_id),
            version=content.version,
            body=RuleConverter._api_rule_body(content.body, repository_type),
            content_hash=content.content_hash,
            changed_at=content.changed_at,
            change_source=content.change_source,
            changed_by=content.changed_by,
        )


class UserConverter:
    """Converter for User model to response schemas."""

    @staticmethod
    def to_detail(user: User) -> UserDetailResponse:
        """Convert User to UserDetailResponse."""
        return UserDetailResponse(
            id=str(user.id),
            full_name=user.full_name,
            email=user.email,
            is_active=user.is_active,
            role=user.role,
            must_change_password=user.must_change_password,
            created=user.created,
            updated=user.updated,
        )


class FilterConverter:
    """Converter for Filter model to response schemas."""

    @staticmethod
    def to_active(filter_obj: Filter) -> FilterActiveResponse:
        """Convert Filter to FilterActiveResponse (id and name only)."""
        return FilterActiveResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
        )

    @staticmethod
    def to_detail(filter_obj: Filter) -> FilterDetailResponse:
        """Convert Filter to FilterDetailResponse."""
        return FilterDetailResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
            created=filter_obj.created.isoformat() if filter_obj.created else "",
            updated=filter_obj.updated.isoformat() if filter_obj.updated else "",
        )

    @staticmethod
    def to_full(filter_obj: Filter) -> FilterFullResponse:
        """Convert Filter to FilterFullResponse (includes body)."""
        return FilterFullResponse(
            id=str(filter_obj.id),
            name=filter_obj.name,
            body=filter_obj.body,
            created=filter_obj.created.isoformat() if filter_obj.created else "",
            updated=filter_obj.updated.isoformat() if filter_obj.updated else "",
        )


class LogSourceConverter:
    """Converter for LogSource model to response schemas."""

    @staticmethod
    def _repository_selections_to_response(value: list | None) -> list[dict[str, list[str] | str]] | None:
        """Normalize stored repository selections for API responses."""
        if not value:
            return None

        selections = []
        for item in value:
            if isinstance(item, dict):
                selections.append({"id": str(item.get("id", "")), "folders": list(item.get("folders") or [])})
            else:
                selections.append({"id": str(item), "folders": []})
        return selections

    @staticmethod
    def to_response(log_source: LogSource) -> LogSourceResponse:
        """Convert LogSource to LogSourceResponse."""
        return LogSourceResponse(
            id=str(log_source.id),
            name=log_source.name,
            parsing_script=log_source.parsing_script,
            parsing_config=log_source.parsing_config,
            mapping=log_source.mapping,
            test_topics=log_source.test_topics,
            test_repository_ids=LogSourceConverter._repository_selections_to_response(log_source.test_repository_ids),
            created=log_source.created.isoformat() if log_source.created else "",
            updated=log_source.updated.isoformat() if log_source.updated else "",
        )


class AttackChainConverter:
    """Converter for AttackChain list and detail API responses."""

    @staticmethod
    def _pipeline_refs(correlation_pipeline_id: UUID | None, name: str | None) -> list[CorrelationPipelineRef]:
        """The chain's correlation pipeline as a ONE-element array (D10), or ``[]``.

        A chain belongs to exactly one correlation pipeline, but the UI contract is an array so it
        survives that ever being relaxed. Empty when the FK is NULL — the pipeline was deleted
        (``ON DELETE SET NULL``) or the chain predates per-customer correlation.
        """
        if correlation_pipeline_id is None or name is None:
            return []
        return [CorrelationPipelineRef(id=correlation_pipeline_id, name=name)]

    @staticmethod
    def to_list_item(
        row: tuple[AttackChain, str, str, str | None, str | None],
    ) -> AttackChainResponse:
        chain, hos_rule_name, _threat_name, threat_severity, correlation_pipeline_name = row
        return AttackChainResponse(
            id=chain.id,
            name=hos_rule_name,
            chain_type=chain.chain_type,
            hostname=chain.hostname,
            username=chain.username,
            status=chain.status,
            severity=threat_severity,
            correlation_pipeline_id=chain.correlation_pipeline_id,
            correlation_pipeline_name=correlation_pipeline_name,
            correlation_pipelines=AttackChainConverter._pipeline_refs(
                chain.correlation_pipeline_id, correlation_pipeline_name
            ),
            first_seen=chain.first_seen,
            last_seen=chain.last_seen,
            event_count=chain.event_count,
            rule_count=chain.rule_count,
            match=chain.match,
            created_at=chain.created_at,
            updated_at=chain.updated_at,
        )

    @staticmethod
    def to_detail(
        chain: AttackChain, techniques: list[dict], chain_graph: list[dict], threat: ActiveThreat
    ) -> AttackChainDetailResponse:
        hos = chain.hos_rule
        correlation_pipeline_name = chain.correlation_pipeline.name if chain.correlation_pipeline is not None else None

        return AttackChainDetailResponse(
            id=chain.id,
            name=hos.name,
            hos_rule=HighOrderSigmaConverter.to_detail(hos),
            active_threat=ActiveThreatConverter.to_detail(threat),
            chain_type=chain.chain_type,
            hostname=chain.hostname,
            username=chain.username,
            status=chain.status,
            severity=threat.severity,
            correlation_pipeline_id=chain.correlation_pipeline_id,
            correlation_pipeline_name=correlation_pipeline_name,
            correlation_pipelines=AttackChainConverter._pipeline_refs(
                chain.correlation_pipeline_id, correlation_pipeline_name
            ),
            first_seen=chain.first_seen,
            last_seen=chain.last_seen,
            event_count=chain.event_count,
            rule_count=chain.rule_count,
            match=chain.match,
            ai_summary=chain.ai_summary,
            techniques=techniques,
            llm_verdict=chain.llm_verdict,
            llm_confidence=float(chain.llm_confidence) if chain.llm_confidence is not None else None,
            llm_review=chain.llm_review,
            analysed_at=chain.analysed_at,
            threat_actors=ActiveThreatConverter._threat_actor_refs(threat.threat_actors),
            created_at=chain.created_at,
            updated_at=chain.updated_at,
            chain_graph=chain_graph,
        )


class ActiveThreatConverter:
    """Converter for ActiveThreat and nested HighOrderSigma models."""

    @staticmethod
    def to_list_item(
        threat: ActiveThreat, pipelines: list[CorrelationPipelineRef] | None = None
    ) -> ActiveThreatListItemResponse:
        """Row for the Monitored Threats list.

        ``pipelines`` is the batched hydration the manager computes for the whole page (every
        correlation pipeline minus this threat's exclusions, D8) — never a per-row lookup.
        """
        release_date = threat.tdm_created if threat.tdm_created is not None else threat.created
        return ActiveThreatListItemResponse(
            tdm_id=threat.tdm_id,
            name=threat.name,
            is_blocked=bool(threat.is_blocked),
            pipelines=pipelines or [],
            severity=threat.severity,
            industries=list(threat.industries) if threat.industries is not None else None,
            geographies=list(threat.geographies) if threat.geographies is not None else None,
            threat_types=list(threat.threat_types) if threat.threat_types is not None else None,
            threat_actors=ActiveThreatConverter._threat_actor_refs(threat.threat_actors),
            techniques=ActiveThreatConverter._technique_refs(getattr(threat, "techniques", None)),
            source=threat.source,
            release_date=release_date,
            is_hidden=bool(threat.is_hidden),
            rule_check_results=RuleIssueResponse.model_validate(
                getattr(
                    threat,
                    "rule_check_results",
                    {
                        "unsupported_rules": [],
                        "not_connected_rules": [],
                        "disabled_rules": [],
                    },
                )
            ),
        )

    @staticmethod
    def to_detail(
        threat: ActiveThreat,
        include_all: bool = False,
        pipelines: list[ActiveThreatPipelineMonitoringItem] | None = None,
    ) -> ActiveThreatResponse:
        """Build ``ActiveThreatResponse``; set ``include_all=True`` to populate ``high_order_sigmas``.

        ``pipelines`` is the threat-detail page's per-pipeline monitoring list. It stays empty when
        the threat is embedded in another entity (the chain detail's Monitored Threat card), which
        must not pay for a correlation-pipeline fan-out.
        """
        high_order_sigmas: list[HighOrderSigma] = list(threat.high_order_sigmas or [])
        high_order_sigmas.sort(key=lambda hoc: (hoc.name or "").lower())
        linked_rules: list[Rule] = getattr(threat, "linked_rules", [])
        rule_check_results = RuleIssueResponse.model_validate(
            getattr(
                threat,
                "rule_check_results",
                {
                    "unsupported_rules": [],
                    "not_connected_rules": [],
                    "disabled_rules": [],
                },
            )
        )
        reasons = rule_check_results.subtitles_by_rule_id()
        response = ActiveThreatResponse(
            tdm_id=threat.tdm_id,
            name=threat.name,
            is_blocked=bool(threat.is_blocked),
            pipelines=pipelines or [],
            summary=threat.summary,
            severity=threat.severity,
            investigation=threat.investigation,
            response=threat.response,
            mitigation=threat.mitigation,
            industries=list(threat.industries) if threat.industries is not None else None,
            geographies=list(threat.geographies) if threat.geographies is not None else None,
            threat_types=list(threat.threat_types) if threat.threat_types is not None else None,
            threat_actors=ActiveThreatConverter._threat_actor_refs(threat.threat_actors),
            techniques=ActiveThreatConverter._technique_refs(getattr(threat, "techniques", None)),
            source=threat.source,
            attack_flow=threat.attack_flow,
            tdm_created=threat.tdm_created,
            tdm_updated=threat.tdm_updated,
            created=threat.created,
            updated=threat.updated,
            rules=[ActiveThreatConverter._to_linked_rule(rule, reasons.get(str(rule.id))) for rule in linked_rules],
            is_hidden=bool(threat.is_hidden),
            rule_check_results=rule_check_results,
            rule_check_results_count=rule_check_results.unique_rule_count(),
        )
        if include_all:
            response.high_order_sigmas = [
                HighOrderSigmaConverter.to_detail(high_order_sigma) for high_order_sigma in high_order_sigmas
            ]
        return response

    @staticmethod
    def _threat_actor_refs(values: list[str] | None) -> list[MitreIdNameRef] | None:
        if values is None:
            return None
        return [MitreIdNameRef(id=ref.id, name=ref.name) for ref in map(mitre_attack.resolve_group_ref, values)]

    @staticmethod
    def _technique_refs(values: list[str] | None) -> list[MitreIdNameRef] | None:
        if values is None:
            return None
        return [MitreIdNameRef(id=ref.id, name=ref.name) for ref in map(mitre_attack.resolve_technique_ref, values)]

    @staticmethod
    def _to_linked_rule(rule: Rule, reason: str | None) -> ActiveThreatLinkedRuleResponse:
        """Short rule row plus the Issue Navigator subtitle for this threat, if any."""
        return ActiveThreatLinkedRuleResponse(
            **RuleConverter.to_detail(rule).model_dump(),
            rule_check_results_reason=reason,
        )


class HighOrderSigmaConverter:
    """Converter for standalone HighOrderSigma API responses."""

    @staticmethod
    def to_detail(high_order_sigma: HighOrderSigma) -> HighOrderSigmaDetailResponse:
        return HighOrderSigmaDetailResponse(
            id=str(high_order_sigma.id),
            tdm_id=high_order_sigma.tdm_id,
            name=high_order_sigma.name,
            body=high_order_sigma.body,
            tdm_created=high_order_sigma.tdm_created,
            tdm_updated=high_order_sigma.tdm_updated,
            created=high_order_sigma.created,
            updated=high_order_sigma.updated,
            is_user_edited=bool(high_order_sigma.is_user_edited),
            level=high_order_sigma.level,
        )

    @staticmethod
    def to_list_item(high_order_sigma: HighOrderSigma) -> HighOrderSigmaListItemResponse:
        return HighOrderSigmaListItemResponse(
            id=str(high_order_sigma.id),
            tdm_id=high_order_sigma.tdm_id,
            name=high_order_sigma.name,
            created=high_order_sigma.created,
            updated=high_order_sigma.updated,
            is_user_edited=bool(high_order_sigma.is_user_edited),
            level=high_order_sigma.level,
        )


class IntegrationConverter:
    """Converter for Integration model to response schemas.

    Detail responses include ``capabilities`` from :func:`apps.integrations.base.get_provider_capabilities`.
    """

    @staticmethod
    def to_list_item(
        integration: Integration,
        provider_cls: type[BaseIntegrationProvider],
    ) -> IntegrationListItem:
        """Convert Integration to IntegrationListItem (Connected tab row)."""
        return IntegrationListItem(
            id=str(integration.id),
            name=integration.name,
            integration_type=integration.integration_type,
            integration_type_display=provider_cls.display_name,
            category=integration.category,
            icon=provider_cls.icon,
            status=integration.status,
            status_message=integration.status_message,
            created=integration.created,
            updated=integration.updated,
        )

    @staticmethod
    def to_detail(
        integration: Integration,
        provider_cls: type[BaseIntegrationProvider],
        masked_config: dict,
    ) -> IntegrationDetailResponse:
        """Convert Integration to IntegrationDetailResponse (masked config + capability list)."""
        return IntegrationDetailResponse(
            id=str(integration.id),
            name=integration.name,
            integration_type=integration.integration_type,
            integration_type_display=provider_cls.display_name,
            category=integration.category,
            icon=provider_cls.icon,
            status=integration.status,
            status_message=integration.status_message,
            config=masked_config,
            capabilities=get_provider_capabilities(provider_cls),
            last_checked_at=integration.last_checked_at,
            created=integration.created,
            updated=integration.updated,
        )
