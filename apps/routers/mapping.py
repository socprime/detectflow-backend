"""Field mapping generation endpoints.

This module provides REST API endpoints for generating field mappings
between parsed log data and Sigma rule fields using AI assistance.

Mapping generation is asynchronous: ``POST /generate-mapping`` enqueues a job and
returns a ``job_id`` (202 Accepted); the client then polls
``GET /generate-mapping/status/{job_id}`` until the job reaches ``completed`` or ``failed``.
"""

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Path, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.database import get_db
from apps.core.exceptions import NotFoundError
from apps.core.models import User
from apps.core.openapi import BAD_REQUEST, UNAUTHORIZED, err, errors
from apps.core.schemas import RepositorySelection, normalize_repository_selections
from apps.managers.mapping import MappingManager
from apps.services.mapping_watcher import get_mapping_watcher


class GenerateMappingRequest(BaseModel):
    """Request for AI-assisted field mapping generation."""

    repository_ids: list[RepositorySelection] = Field(description="Repository selections to extract Sigma fields from")
    topics: list[str] = Field(description="Kafka topics to sample events from")
    parser_query: str = Field(description="Parser query to apply to sampled events")

    @field_validator("repository_ids", mode="before")
    @classmethod
    def validate_repository_selections(cls, v: object) -> list[dict] | None:
        return normalize_repository_selections(v)


class GenerateMappingJobResponse(BaseModel):
    """Response returned when a mapping generation job is enqueued."""

    job_id: str = Field(description="Identifier of the enqueued job; poll /generate-mapping/status/{job_id} for result")


class GenerateMappingPromptResponse(BaseModel):
    """Response containing the generated prompt for manual or LLM-assisted mapping."""

    prompt: str = Field(description="Generated prompt describing Sigma and event fields for manual mapping")


class GenerateMappingStatusResponse(BaseModel):
    """Response containing status and optional result for a mapping generation job."""

    status: Literal["queued", "running", "completed", "failed"] = Field(description="Current job status")
    started_at: datetime | None = Field(default=None, description="When processing started")
    completed_at: datetime | None = Field(default=None, description="When processing completed or failed")
    error: str | None = Field(default=None, description="Error message for failed jobs")
    mapping: str | None = Field(default=None, description="Generated mapping YAML for completed jobs")


class GetSigmaFieldsRequest(BaseModel):
    """Request for getting Sigma fields from repositories."""

    repository_ids: list[RepositorySelection] = Field(description="Repository selections to extract Sigma fields from")

    @field_validator("repository_ids", mode="before")
    @classmethod
    def validate_repository_selections(cls, v: object) -> list[dict] | None:
        return normalize_repository_selections(v)


class GetSigmaFieldsResponse(BaseModel):
    """Response with Sigma fields."""

    sigma_fields: list[str] = Field(description="List of unique Sigma fields from the specified repositories")


router = APIRouter(prefix="/api/v1", tags=["Mapping"])


@router.post(
    "/generate-mapping",
    response_model=GenerateMappingJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start field mapping generation",
    responses=errors(UNAUTHORIZED),
)
@router.post("/generate-mapping/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def generate_mapping(
    request: GenerateMappingRequest,
    _: User = Depends(require_write_access),
):
    """
    Enqueue an asynchronous field mapping generation job.

    The job samples events from the requested Kafka topics, extracts Sigma fields
    from the repositories, and calls the TDM API to produce a mapping. It runs in a
    background worker and is not awaited here.

    Returns 202 Accepted with a `job_id`. Poll
    `GET /api/v1/generate-mapping/status/{job_id}` until `status` is `completed`
    (the response then carries `mapping`) or `failed`.
    """
    watcher = get_mapping_watcher()
    job_id = watcher.enqueue(
        repository_ids=request.repository_ids,
        topics=request.topics,
        parser_query=request.parser_query,
    )
    return GenerateMappingJobResponse(job_id=job_id)


@router.get(
    "/generate-mapping/status/{job_id}",
    response_model=GenerateMappingStatusResponse,
    summary="Get mapping job status",
    responses=errors(UNAUTHORIZED, err(404, "Mapping job not found")),
)
@router.get(
    "/generate-mapping/status/{job_id}/",
    include_in_schema=False,
    response_model=GenerateMappingStatusResponse,
)
async def get_generate_mapping_status(
    job_id: str = Path(..., description="Identifier returned by POST /generate-mapping"),
    _: User = Depends(get_current_active_user),
):
    """
    Poll the status of a mapping generation job by `job_id`.

    Returned by `POST /generate-mapping`; call this until `status` is `completed`
    (the response then carries `mapping`) or `failed` (the response carries `error`).

    **Status values:** `queued`, `running`, `completed`, `failed`.

    The job is removed after the first fetch once it reaches `completed` or `failed`,
    so subsequent requests for the same `job_id` return 404.
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
    return GenerateMappingStatusResponse(**body)


@router.post(
    "/generate-mapping-prompt",
    response_model=GenerateMappingPromptResponse,
    summary="Generate mapping prompt",
    responses=errors(UNAUTHORIZED, BAD_REQUEST),
)
async def generate_mapping_prompt(
    request: GenerateMappingRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate a prompt for manual AI-assisted mapping generation.

    Samples events from the requested topics and extracts Sigma fields from the
    repositories synchronously, then returns a ready-to-use LLM prompt. Returns 400
    if no Sigma fields or no event fields can be resolved.
    """
    manager = MappingManager(db)
    prompt = await manager.generate_mapping_prompt(
        repository_ids=request.repository_ids,
        topics=request.topics,
        parser_query=request.parser_query,
    )
    return GenerateMappingPromptResponse(prompt=prompt)


@router.post(
    "/get-sigma-fields",
    response_model=GetSigmaFieldsResponse,
    summary="Get Sigma fields from repositories",
    responses=errors(UNAUTHORIZED, BAD_REQUEST),
)
async def get_sigma_fields(
    request: GetSigmaFieldsRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get Sigma fields from specified repositories.

    Extracts all unique field names referenced by Sigma rules in the specified
    repositories. Returns 400 if `repository_ids` is empty or contains an invalid UUID.
    """
    manager = MappingManager(db)
    sigma_fields = await manager.get_sigma_fields(request.repository_ids)
    return GetSigmaFieldsResponse(sigma_fields=sigma_fields)
