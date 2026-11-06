"""Rule management endpoints.

This module provides REST API endpoints for managing detection rules,
including CRUD operations for both local and API-synced rules.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.converters import RuleConverter
from apps.core.database import get_db
from apps.core.enums import RuleSearchField
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    RuleBulkCreatedItem,
    RuleBulkCreateRequest,
    RuleBulkCreateResponse,
    RuleCreateRequest,
    RuleFullDetailResponse,
    RuleListResponse,
    RuleRollbackRequest,
    RuleUpdateRequest,
    RuleVersionDetailResponse,
    RuleVersionsListResponse,
    SigmaValidateBulkItemResponse,
    SigmaValidateBulkRequest,
    SigmaValidateBulkResponse,
    SigmaValidateRequest,
    SigmaValidateResponse,
)
from apps.managers.rule import RulesOrchestrator

logger = get_logger(__name__)

# Graceful degradation: schema-parser sigma_validation may not be available
try:
    from schema_parser import __version__ as schema_parser_version
    from schema_parser.sigma_validation import SigmaValidator

    SIGMA_VALIDATOR_AVAILABLE = True
    _sigma_validator = SigmaValidator()
except ImportError:
    SIGMA_VALIDATOR_AVAILABLE = False
    schema_parser_version = None
    _sigma_validator = None
    logger.warning("schema_parser.sigma_validation not available, validation endpoints will return is_supported=True")

router = APIRouter(prefix="/api/v1", tags=["Rules"])


@router.get(
    "/rules",
    response_model=RuleListResponse,
    summary="List rules",
    responses=errors(UNAUTHORIZED),
)
@router.get("/rules/", include_in_schema=False)
async def get_rules(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    repository_id: str | None = Query(default=None, description="Filter by repository ID"),
    folder_path: str | None = Query(default=None, description="Filter by virtual folder path"),
    search_fields: list[RuleSearchField] | None = Query(
        default=None,
        description="Fields to apply search in. Allowed: name, product, service, category, sigma_uuid. Default: name, product, service, category. Example: search_fields=name&search_fields=product",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    List all detection rules with pagination.

    **Query parameters:**
    - Pagination: `limit`, `offset` (from pagination dependency).
    - `repository_id`: filter rules by repository UUID.
    - `folder_path`: filter rules by virtual folder path. Parent folder paths include all nested folders.
    - `search`: case-insensitive search string; applied to the fields specified by `search_fields`.
    - `search_fields`: optional list of rule fields to search in. Allowed values: `name`, `product`, `service`, `category`, `sigma_uuid`. If omitted, the four text fields are used. Pass multiple as `search_fields=name&search_fields=product`.
    - `search_fields=sigma_uuid` is the "By ID" search mode: a whole Sigma UUID (dashed, undashed,
      upper-case or URN-wrapped) matches exactly, a shorter hex fragment matches any UUID containing
      it (dashes are ignored on both sides), and a term that cannot be part of a UUID returns no
      rows. It combines with `repository_id`/`folder_path`, so it works both across all repositories
      and inside one.
    - Sort: `sort`, `order` (from pagination dependency).
    """
    orchestrator = RulesOrchestrator(db)
    repo_uuid = UUID(repository_id) if repository_id else None
    rules, total = await orchestrator.get_all(
        pagination,
        repository_id=repo_uuid,
        folder_path=folder_path,
        search_fields=search_fields,
    )

    data = [RuleConverter.to_detail(r) for r in rules]

    return RuleListResponse(
        total=total,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"] or "",
        order=pagination["order"],
        data=data,
    )


@router.post(
    "/rules",
    response_model=RuleFullDetailResponse,
    status_code=201,
    summary="Create rule",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Repository is system-managed or not a local repository"),
        err(404, "Repository not found"),
    ),
)
@router.post("/rules/", include_in_schema=False)
async def create_rule(
    rule: RuleCreateRequest,
    repository_id: UUID = Query(..., description="Repository UUID to create the rule(s) in"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a detection rule in a local repository.

    Rules may only be created in local repositories — API (SOCPrime) and external repositories
    are read-only and managed by sync, and system-managed repositories are locked (both 400).
    The new rule is validated for Sigma support, linked to every pipeline using the repository,
    published to Kafka for enabled pipelines when supported (live hot-reload), and logged as a
    `create` activity entry.
    """
    orchestrator = RulesOrchestrator(db)
    new_rule = await orchestrator.create_local_rule(
        rule_data=rule.model_dump(),
        repository_id=repository_id,
        user=current_user,
    )
    return RuleConverter.to_full_detail(new_rule)


@router.post(
    "/rules/bulk",
    response_model=RuleBulkCreateResponse,
    status_code=201,
    summary="Create rules in bulk",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Repository is system-managed or not a local repository"),
        err(404, "Repository not found"),
    ),
)
@router.post("/rules/bulk/", include_in_schema=False)
async def create_rules_bulk(
    body: RuleBulkCreateRequest,
    repository_id: UUID = Query(..., description="Repository UUID to create the rule(s) in"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create multiple detection rules in a local repository in one request.

    Same local-only / system-locked constraints as the single-rule create (both 400). Every
    rule is validated for Sigma support, linked to pipelines using the repository, and the
    supported ones are published to Kafka for enabled pipelines (live hot-reload). The response
    reports the total and how many are supported; one `create` activity entry is logged for the batch.
    """
    orchestrator = RulesOrchestrator(db)
    rules_data = [r.model_dump() for r in body.rules]
    created = await orchestrator.create_local_rules_bulk(
        rules_data=rules_data,
        repository_id=repository_id,
        user=current_user,
    )
    items = [RuleBulkCreatedItem(id=str(r.id), name=r.name, is_supported=r.is_supported) for r in created]
    return RuleBulkCreateResponse(
        total=len(items),
        supported=sum(1 for i in items if i.is_supported),
        data=items,
    )


@router.get(
    "/rules/{rule_id}",
    response_model=RuleFullDetailResponse,
    summary="Get rule by ID",
    responses=errors(UNAUTHORIZED, err(404, "Rule not found")),
)
@router.get("/rules/{rule_id}/", include_in_schema=False)
async def get_rule(
    rule_id: UUID,
    include_versions: bool = False,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a rule.

    Returns the full rule configuration including the Sigma rule body and support status.
    Read-only.
    """
    orchestrator = RulesOrchestrator(db)
    rule, versions = await orchestrator.get_by_id(rule_id, include_versions=include_versions)
    return RuleConverter.to_full_detail(rule, versions=versions)


@router.patch(
    "/rules/{rule_id}",
    response_model=RuleFullDetailResponse,
    summary="Update rule",
    responses=errors(
        UNAUTHORIZED,
        err(
            400,
            "Rule is in an external (read-only) repository, TDM integration is not configured, "
            "or the TDM API rejected the update",
        ),
        err(404, "Rule not found"),
        err(409, "GitLab file changed remotely since last sync or sync in progress"),
    ),
)
@router.patch("/rules/{rule_id}/", include_in_schema=False)
async def update_rule(
    rule_id: UUID = Path(..., description="UUID of the rule to update"),
    rule: RuleUpdateRequest = ...,
    gitlab_push: bool = Query(
        False,
        description="For GitLab rules: push the update to GitLab with optimistic locking before saving locally",
    ),
    gitlab_force: bool = Query(
        False,
        description=(
            "For GitLab rules: overwrite GitLab even when the file changed remotely (skips the "
            "optimistic-lock check). Only takes effect when gitlab_push=true; ignored otherwise."
        ),
    ),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing rule and re-sync it to the affected pipelines.

    Local rules update the DB only; GitLab rules accept ``gitlab_push`` / ``gitlab_force`` query params to write
    back to GitLab before saving locally (409 on optimistic-lock conflict unless ``gitlab_force=true``); API
    (SOCPrime) rules are also pushed back to TDM (a missing TDM integration or a rejected update yields 400);
    system-repo rules are flagged as user-edited so Active Threat sync stops overwriting them.
    External-repository rules are read-only (400). When the body changes the rule is re-validated;
    supported rules are republished to Kafka for enabled pipelines and rules that became unsupported
    are removed (live hot-reload). Logged as an `update` activity entry.
    """
    orchestrator = RulesOrchestrator(db)
    updated_rule = await orchestrator.update_rule(
        rule_id=rule_id,
        update_data=rule.model_dump(exclude_unset=True),
        user=current_user,
        gitlab_push=gitlab_push,
        gitlab_force=gitlab_force,
    )
    return RuleConverter.to_full_detail(updated_rule)


@router.delete(
    "/rules/{rule_id}",
    status_code=204,
    summary="Delete rule",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Rule is system-managed or not in a local/GitLab repository"),
        err(404, "Rule not found"),
        err(409, "GitLab file changed remotely since last sync or sync in progress"),
    ),
)
@router.delete("/rules/{rule_id}/", include_in_schema=False)
async def delete_rule(
    rule_id: UUID = Path(..., description="UUID of the rule to delete"),
    gitlab_push: bool = Query(
        False,
        description="For GitLab rules: delete the file on GitLab with optimistic locking before removing locally",
    ),
    gitlab_force: bool = Query(
        False,
        description=(
            "For GitLab rules: delete on GitLab even when the file changed remotely (skips the "
            "optimistic-lock check). Only takes effect when gitlab_push=true; ignored otherwise."
        ),
    ),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a rule from a local or GitLab repository.

    Local rules are removed from the DB only. GitLab rules accept ``gitlab_push`` /
    ``gitlab_force`` (same names and semantics as update): default is local-only removal
    (the file stays on GitLab and may reappear on the next pull); set ``gitlab_push=true``
    to delete on GitLab first (409 on optimistic-lock conflict unless ``gitlab_force=true``). API, external, and system-managed rules cannot be deleted here (400).
    Deletion emits Kafka rule-deletes for every connected pipeline (live hot-reload) and a
    ``delete`` activity entry.
    """
    orchestrator = RulesOrchestrator(db)
    await orchestrator.delete_rule(
        rule_id=rule_id,
        user=current_user,
        gitlab_push=gitlab_push,
        gitlab_force=gitlab_force,
    )


@router.get(
    "/rules/{rule_id}/versions",
    response_model=RuleVersionsListResponse,
    summary="Get rule version history",
    responses=errors(
        {200: {"description": "List of rule versions (metadata, no body)"}},
        UNAUTHORIZED,
        err(404, "Rule not found"),
    ),
)
@router.get("/rules/{rule_id}/versions/", include_in_schema=False)
async def get_rule_versions(
    rule_id: UUID,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get version history for a rule. Returns metadata without bodies."""
    orchestrator = RulesOrchestrator(db)
    rule, versions = await orchestrator.get_by_id(rule_id, include_versions=True)
    return RuleVersionsListResponse(
        rule_id=str(rule_id),
        current_version=rule.version,
        versions=[RuleConverter.to_version_item(v) for v in versions],
    )


@router.get(
    "/rules/{rule_id}/versions/{version}",
    response_model=RuleVersionDetailResponse,
    summary="Get specific rule version",
    responses=errors(
        {200: {"description": "Rule version with body"}},
        UNAUTHORIZED,
        err(404, "Rule or version not found"),
    ),
)
@router.get("/rules/{rule_id}/versions/{version}/", include_in_schema=False)
async def get_rule_version(
    rule_id: UUID,
    version: int,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Get a specific version of a rule including the full body."""
    orchestrator = RulesOrchestrator(db)
    rule, content = await orchestrator.get_rule_version(rule_id, version)
    repository_type = rule.repository.type if rule.repository else None
    return RuleConverter.to_version_detail(content, repository_type=repository_type)


@router.post(
    "/rules/{rule_id}/rollback",
    response_model=RuleFullDetailResponse,
    summary="Rollback rule to a previous version",
    responses=errors(
        {200: {"description": "Rule rolled back successfully"}},
        err(400, "Invalid rollback (same version or external repo)"),
        UNAUTHORIZED,
        err(404, "Rule or version not found"),
    ),
)
@router.post("/rules/{rule_id}/rollback/", include_in_schema=False)
async def rollback_rule(
    rule_id: UUID,
    request: RuleRollbackRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """Rollback a rule to a previous version. Creates a new version with the old body content."""
    orchestrator = RulesOrchestrator(db)
    rolled_back_rule = await orchestrator.rollback_rule(
        rule_id=rule_id,
        target_version=request.version,
        user=current_user,
    )
    return RuleConverter.to_full_detail(rolled_back_rule)


@router.post(
    "/rules/validate",
    response_model=SigmaValidateResponse,
    summary="Validate sigma rule",
    responses=errors(UNAUTHORIZED),
)
@router.post("/rules/validate/", include_in_schema=False)
async def validate_sigma_rule(
    request: SigmaValidateRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Validate a single Sigma rule without saving it.

    Checks whether the rule is supported by the current Flink matcher: returns
    `is_supported=true`, or `is_supported=false` with the reason and unsupported feature labels.
    Useful for testing rules before adding them. Does not touch the database or Kafka.

    Degrades gracefully: when the `schema_parser` dependency is unavailable the validator
    cannot run, so every rule is reported as `is_supported=true` with a null validator version.
    """
    if not SIGMA_VALIDATOR_AVAILABLE:
        return SigmaValidateResponse(
            is_supported=True,
            unsupported_reason=None,
            unsupported_labels=[],
            validator_version=None,
        )

    result = _sigma_validator.validate(request.sigma_text)

    return SigmaValidateResponse(
        is_supported=result.is_supported,
        unsupported_reason=result.unsupported_reason,
        unsupported_labels=result.unsupported_labels or [],
        validator_version=schema_parser_version,
    )


@router.post(
    "/rules/validate/bulk",
    response_model=SigmaValidateBulkResponse,
    summary="Validate multiple sigma rules",
    responses=errors(UNAUTHORIZED),
)
@router.post("/rules/validate/bulk/", include_in_schema=False)
async def validate_sigma_rules_bulk(
    request: SigmaValidateBulkRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Validate multiple Sigma rules in one request (max 100).

    Checks each rule against the current Flink matcher and returns aggregated counts
    (total / supported / unsupported) plus per-rule results. Does not touch the database or Kafka.

    Degrades gracefully: when the `schema_parser` dependency is unavailable the validator
    cannot run, so every rule is reported as `is_supported=true` with a null validator version.
    """
    total = len(request.rules)

    if not SIGMA_VALIDATOR_AVAILABLE:
        return SigmaValidateBulkResponse(
            total=total,
            supported=total,
            unsupported=0,
            validator_version=None,
            results=[
                SigmaValidateBulkItemResponse(
                    index=i,
                    is_supported=True,
                    unsupported_reason=None,
                    unsupported_labels=[],
                )
                for i in range(total)
            ],
        )

    results: list[SigmaValidateBulkItemResponse] = []
    supported_count = 0

    for index, rule in enumerate(request.rules):
        result = _sigma_validator.validate(rule.sigma_text)
        if result.is_supported:
            supported_count += 1

        results.append(
            SigmaValidateBulkItemResponse(
                index=index,
                is_supported=result.is_supported,
                unsupported_reason=result.unsupported_reason,
                unsupported_labels=result.unsupported_labels or [],
            )
        )

    return SigmaValidateBulkResponse(
        total=total,
        supported=supported_count,
        unsupported=total - supported_count,
        validator_version=schema_parser_version,
        results=results,
    )
