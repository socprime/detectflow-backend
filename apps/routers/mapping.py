"""Field mapping generation endpoints.

This module provides REST API endpoints for generating field mappings
between parsed log data and Sigma rule fields using AI assistance.
"""

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.exceptions import NotFoundError
from apps.core.models import User
from apps.core.schemas import ErrorResponse
from apps.managers.mapping import MappingManager
from apps.services.mapping_watcher import get_mapping_watcher


class GenerateMappingRequest(BaseModel):
    """Request for AI-assisted field mapping generation."""

    repository_ids: list[str] = Field(description="Repository IDs to extract Sigma fields from")
    topics: list[str] = Field(description="Kafka topics to sample events from")
    parser_query: str = Field(description="Parser query to apply to events")


class GenerateMappingPromptResponse(BaseModel):
    """Response containing the generated prompt for manual or LLM-assisted mapping."""

    prompt: str


class GetSigmaFieldsRequest(BaseModel):
    """Request for getting Sigma fields from repositories."""

    repository_ids: list[str] = Field(description="Repository IDs to extract Sigma fields from")


class GetSigmaFieldsResponse(BaseModel):
    """Response with Sigma fields."""

    sigma_fields: list[str] = Field(description="List of unique Sigma fields from the specified repositories")


router = APIRouter(tags=["Mapping"])


@router.post(
    "/api/v1/generate-mapping",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start field mapping generation",
    responses={
        202: {"description": "Job enqueued, returns job_id"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
@router.post("/api/v1/generate-mapping/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def generate_mapping(
    request: GenerateMappingRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Enqueue a field mapping generation job.

    Returns 202 Accepted with job_id. Poll GET /api/v1/generate-mapping/status/{job_id}
    for status and result (same format: status, started_at, completed_at, error, mapping when completed).
    """
    watcher = get_mapping_watcher()
    job_id = watcher.enqueue(
        repository_ids=request.repository_ids,
        topics=request.topics,
        parser_query=request.parser_query,
    )
    return {"job_id": job_id}


@router.get(
    "/api/v1/generate-mapping/status/{job_id}",
    summary="Get mapping job status",
    responses={
        200: {"description": "Job status (status, started_at, completed_at, error, mapping when completed)"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Job not found"},
    },
)
@router.get("/api/v1/generate-mapping/status/{job_id}/", include_in_schema=False)
async def get_generate_mapping_status(
    job_id: str,
    _: User = Depends(get_current_active_user),
):
    """
    Get status for a mapping job by job_id.

    **Status values:** `queued`, `running`, `completed`, `failed`
    When status is `completed`, the response includes `mapping` with the result.
    The job is removed after the first fetch when status is `completed` or `failed`;
    subsequent requests for the same job_id will return 404.
    """
    watcher = get_mapping_watcher()
    s = watcher.get_status(job_id)
    if s is None:
        raise NotFoundError(f"Mapping job {job_id} not found")
    body: dict = {
        "status": s.status,
        "started_at": s.started_at,
        "completed_at": s.completed_at,
        "error": s.error,
    }
    if s.result is not None:
        body["mapping"] = s.result
    if s.status in ("completed", "failed"):
        watcher.remove_job(job_id)
    return body


@router.post(
    "/api/v1/generate-mapping-prompt",
    response_model=GenerateMappingPromptResponse,
    summary="Generate mapping prompt",
    responses={
        200: {"description": "Generated mapping prompt"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def generate_mapping_prompt(
    request: GenerateMappingRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """Generate a prompt for manual AI-assisted mapping generation."""
    manager = MappingManager(db)
    prompt = await manager.generate_mapping_prompt(
        repository_ids=request.repository_ids,
        topics=request.topics,
        parser_query=request.parser_query,
    )
    return GenerateMappingPromptResponse(prompt=prompt)


@router.post(
    "/api/v1/get-sigma-fields",
    response_model=GetSigmaFieldsResponse,
    summary="Get Sigma fields from repositories",
    responses={
        200: {"description": "List of Sigma fields"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def get_sigma_fields(
    request: GetSigmaFieldsRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get Sigma fields from specified repositories.

    Extracts all unique field names referenced by Sigma rules
    in the specified repositories.
    """
    manager = MappingManager(db)
    sigma_fields = await manager.get_sigma_fields(request.repository_ids)
    return GetSigmaFieldsResponse(sigma_fields=sigma_fields)
