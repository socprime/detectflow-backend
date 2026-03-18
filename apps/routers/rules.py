"""Rule management endpoints.

This module provides REST API endpoints for managing detection rules,
including CRUD operations for both local and API-synced rules.
"""

from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.converters import RuleConverter
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    ErrorResponse,
    RuleCreateRequest,
    RuleFullDetailResponse,
    RuleListResponse,
    RuleUpdateRequest,
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
    responses={
        200: {"description": "Paginated list of rules"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/rules/", include_in_schema=False)
async def get_rules(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    repository_id: str | None = Query(default=None, description="Filter by repository ID"),
    search_fields: list[str] | None = Query(
        default=None,
        description="Fields to apply search in. Allowed: name, product, service, category. Default: all four. Example: search_fields=name&search_fields=product",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    List all detection rules with pagination.

    **Query parameters:**
    - Pagination: `limit`, `offset` (from pagination dependency).
    - `repository_id`: filter rules by repository UUID.
    - `search`: case-insensitive search string; applied to the fields specified by `search_fields`.
    - `search_fields`: optional list of rule fields to search in. Allowed values: `name`, `product`, `service`, `category`. If omitted, all four are used. Pass multiple as `search_fields=name&search_fields=product`.
    - Sort: `sort`, `order` (from pagination dependency).
    """
    orchestrator = RulesOrchestrator(db)
    repo_uuid = UUID(repository_id) if repository_id else None
    rules, total = await orchestrator.get_all(pagination, repository_id=repo_uuid, search_fields=search_fields)

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
    responses={
        201: {"description": "Rule created successfully"},
        400: {"model": ErrorResponse, "description": "Repository is not local"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Repository not found"},
        422: {"description": "Validation error"},
    },
)
@router.post("/rules/", include_in_schema=False)
async def create_rule(
    rule: RuleCreateRequest,
    repository_id: UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new detection rule in a local repository.

    **Note:** Rules can only be created in local repositories.
    API and external repositories are read-only (managed by sync).
    """
    orchestrator = RulesOrchestrator(db)
    new_rule = await orchestrator.create_local_rule(
        rule_data=rule.model_dump(),
        repository_id=repository_id,
        user=current_user,
    )
    return RuleConverter.to_full_detail(new_rule)


@router.get(
    "/rules/{rule_id}",
    response_model=RuleFullDetailResponse,
    summary="Get rule by ID",
    responses={
        200: {"description": "Rule details with body"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Rule not found"},
    },
)
@router.get("/rules/{rule_id}/", include_in_schema=False)
async def get_rule(
    rule_id: UUID,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a rule.

    Returns full rule configuration including the Sigma rule body.
    """
    orchestrator = RulesOrchestrator(db)
    rule = await orchestrator.get_by_id(rule_id)
    return RuleConverter.to_full_detail(rule)


@router.patch(
    "/rules/{rule_id}",
    response_model=RuleFullDetailResponse,
    summary="Update rule",
    responses={
        200: {"description": "Rule updated successfully"},
        400: {"model": ErrorResponse, "description": "Rule is in external repository (read-only)"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Rule not found"},
        422: {"description": "Validation error"},
    },
)
@router.patch("/rules/{rule_id}/", include_in_schema=False)
async def update_rule(
    rule_id: UUID,
    rule: RuleUpdateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update an existing rule.

    Local rules are updated in DB only. API rules are also synced back to SOCPrime TDM.
    External repository rules cannot be updated (read-only).
    Changes are propagated to Kafka for pipelines using this rule.
    """
    orchestrator = RulesOrchestrator(db)
    updated_rule = await orchestrator.update_rule(
        rule_id=rule_id,
        update_data=rule.model_dump(exclude_unset=True),
        user=current_user,
    )
    return RuleConverter.to_full_detail(updated_rule)


@router.delete(
    "/rules/{rule_id}",
    status_code=204,
    summary="Delete rule",
    responses={
        204: {"description": "Rule deleted successfully"},
        400: {"model": ErrorResponse, "description": "Rule is not in a local repository"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Rule not found"},
    },
)
@router.delete("/rules/{rule_id}/", include_in_schema=False)
async def delete_rule(
    rule_id: UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a rule from a local repository.

    **Note:** Only rules in local repositories can be deleted.
    API and external repository rules are read-only (managed by sync).
    """
    orchestrator = RulesOrchestrator(db)
    await orchestrator.delete_local_rule(rule_id=rule_id, user=current_user)


@router.post(
    "/rules/validate",
    response_model=SigmaValidateResponse,
    summary="Validate sigma rule",
    responses={
        200: {"description": "Validation result"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.post("/rules/validate/", include_in_schema=False)
async def validate_sigma_rule(
    request: SigmaValidateRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Validate a sigma rule without saving it.

    Check if the sigma rule is supported by the current Flink matcher.
    Returns is_supported=true if the rule can be processed, or
    is_supported=false with the reason if the rule uses unsupported features.

    Useful for testing rules before adding them to the system.
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
    responses={
        200: {"description": "Bulk validation results"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.post("/rules/validate/bulk/", include_in_schema=False)
async def validate_sigma_rules_bulk(
    request: SigmaValidateBulkRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Validate multiple sigma rules at once.

    Check if each sigma rule is supported by the current Flink matcher.
    Returns aggregated statistics and per-rule validation results.

    Maximum 100 rules per request.
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
