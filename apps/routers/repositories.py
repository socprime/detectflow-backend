"""Repository management endpoints.

This module provides REST API endpoints for managing rule repositories:
SOCPrime TDM API, external (GitHub), and local repositories.
Includes sync triggers and status via the scheduler service.
"""

import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.exceptions import ConflictError
from apps.core.models import User
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    AddExternalRepositoriesRequest,
    AddExternalRepositoriesResponse,
    AddSocprimeRepositoriesRequest,
    AddSocprimeRepositoriesResponse,
    ErrorResponse,
    ExternalRepositoryListResponse,
    ExternalRepositoryResponse,
    RepositoryCreateUpdateRequest,
    RepositoryDetailResponse,
    RepositoryListResponse,
    RepositorySyncToggleRequest,
    RepositorySyncToggleResponse,
    SocprimeRepositoryListResponse,
    SocprimeRepositoryResponse,
)
from apps.managers.repositories import RepositoriesManager
from apps.modules.kafka.activity import activity_producer
from apps.services.scheduler import get_scheduler_service

router = APIRouter(prefix="/api/v1", tags=["Repositories"])

# Mapping for UI-friendly repository type names
REPOSITORY_TYPE_DISPLAY = {
    "api": "SOC Prime",
    "external": "GitHub",
    "local": "Local",
}


def get_repository_type_display(repo_type: str) -> str:
    """Get human-readable repository type for UI display."""
    return REPOSITORY_TYPE_DISPLAY.get(repo_type, repo_type)


@router.post(
    "/sync-repositories",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start repository sync",
    responses={
        202: {"description": "Sync started in background"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        409: {"model": ErrorResponse, "description": "Sync already running"},
    },
)
@router.post("/sync-repositories/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def sync_repositories(
    current_user: User = Depends(get_current_active_user),
):
    """
    Start syncing repositories from SOCPrime TDM API.

    Returns immediately with 202 Accepted. The sync runs in background.
    Use `/sync-repositories/status` to check progress.
    """
    scheduler = get_scheduler_service()
    if scheduler.is_sync_running:
        raise ConflictError("Sync is already running")

    await activity_producer.log_action(
        action="sync_start",
        entity_type="repository",
        user=current_user,
        details="Manual repository sync triggered",
        source="user",
    )

    asyncio.create_task(scheduler.run_sync_in_background())
    return {"message": "Sync started in background"}


@router.get(
    "/sync-repositories/status",
    summary="Get sync status",
    responses={
        200: {"description": "Current sync status"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/sync-repositories/status/", include_in_schema=False)
async def get_sync_status(
    _: User = Depends(get_current_active_user),
):
    """
    Get the current repository sync status.

    **Status values:** `idle`, `running`, `completed`, `failed`
    Returns separate status for each sync type: `api_repos`, `git_hub_repos`.
    """
    scheduler = get_scheduler_service()
    return scheduler.sync_statuses


@router.get(
    "/socprime-repositories",
    response_model=SocprimeRepositoryListResponse,
    summary="Get list of SOCPrime repositories",
    responses={
        200: {"description": "List of SOCPrime repositories"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        400: {"model": ErrorResponse, "description": "API key not configured or not valid"},
    },
)
@router.get("/socprime-repositories/", include_in_schema=False)
async def get_socprime_repositories(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get list of repositories from SOCPrime TDM API.

    Returns repositories with their id and name. Requires SOCPrime API key to be configured.
    """
    manager = RepositoriesManager(db)
    repos = await manager.get_socprime_repositories()

    return SocprimeRepositoryListResponse(
        data=[
            SocprimeRepositoryResponse(
                id=repo["id"],
                name=repo["name"],
                is_added=repo["is_added"],
                source_link=repo["source_link"],
            )
            for repo in repos
        ]
    )


@router.post(
    "/add-socprime-repositories",
    response_model=AddSocprimeRepositoriesResponse,
    summary="Add SOCPrime repositories",
    responses={
        200: {"description": "Repositories added successfully"},
        400: {"model": ErrorResponse, "description": "API key not configured or invalid repository IDs"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"model": ErrorResponse, "description": "Invalid API key or API error"},
    },
)
@router.post("/add-socprime-repositories/", include_in_schema=False)
async def add_socprime_repositories(
    request: AddSocprimeRepositoriesRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Add SOCPrime repositories to the database.

    Validates repository IDs against SOCPrime TDM API and adds valid repositories.
    Skips repositories that are already added. Raises error if any repository ID is invalid.
    """
    manager = RepositoriesManager(db)
    added_ids, skipped_ids = await manager.add_socprime_repositories(
        repository_ids=request.repository_ids,
        user=current_user,
    )

    return AddSocprimeRepositoriesResponse(
        added=added_ids,
        skipped=skipped_ids,
    )


@router.get(
    "/external-repositories",
    response_model=ExternalRepositoryListResponse,
    summary="Get list of external repositories",
    responses={
        200: {"description": "List of external repositories"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.get("/external-repositories/", include_in_schema=False)
async def get_external_repositories(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get list of available external repositories.

    Returns hardcoded list of external repositories with their id and name.
    """
    manager = RepositoriesManager(db)
    repos = await manager.get_external_repositories()

    return ExternalRepositoryListResponse(
        data=[
            ExternalRepositoryResponse(
                id=repo["id"],
                name=repo["name"],
                source_link=repo["source_link"],
                is_added=repo["is_added"],
            )
            for repo in repos
        ]
    )


@router.post(
    "/add-external-repositories",
    response_model=AddExternalRepositoriesResponse,
    summary="Add external repositories",
    responses={
        200: {"description": "Repositories added successfully"},
        400: {"model": ErrorResponse, "description": "Invalid repository IDs"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
@router.post("/add-external-repositories/", include_in_schema=False)
async def add_external_repositories(
    request: AddExternalRepositoriesRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Add external repositories to the database.

    Validates repository IDs against available external repositories and adds valid repositories.
    Skips repositories that are already added. Raises error if any repository ID is invalid.
    """
    manager = RepositoriesManager(db)
    added_ids, skipped_ids = await manager.add_external_repositories(
        repository_ids=request.repository_ids,
        user=current_user,
    )

    return AddExternalRepositoriesResponse(
        added=added_ids,
        skipped=skipped_ids,
    )


@router.post(
    "/repositories",
    response_model=RepositoryDetailResponse,
    status_code=201,
    summary="Create local repository",
    responses={
        201: {"description": "Repository created successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
@router.post("/repositories/", include_in_schema=False)
async def create_local_repository(
    repository: RepositoryCreateUpdateRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new local repository.

    Local repositories store rules created manually, as opposed to
    API repositories synced from SOCPrime TDM.
    """
    manager = RepositoriesManager(db)
    repo = await manager.create_local(name=repository.name, user=current_user)

    return RepositoryDetailResponse(
        id=str(repo.id),
        name=repo.name,
        type=repo.type,
        type_display=get_repository_type_display(repo.type),
        rules=repo.rules_count,
        created=repo.created,
        updated=repo.updated,
        sync_enabled=repo.sync_enabled,
        source_link=repo.source_link,
        pipelines=[],  # New repository has no pipelines yet
    )


@router.patch(
    "/repositories/{repository_id}",
    response_model=RepositoryDetailResponse,
    summary="Update local repository",
    responses={
        200: {"description": "Repository updated successfully"},
        400: {"model": ErrorResponse, "description": "Repository is not local"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Repository not found"},
    },
)
@router.patch("/repositories/{repository_id}/", include_in_schema=False)
async def update_local_repository(
    repository_id: UUID,
    repository: RepositoryCreateUpdateRequest = ...,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Update a local repository.

    **Note:** Only local repositories can be updated. API repositories are read-only.
    """
    manager = RepositoriesManager(db)
    repo = await manager.update_local(
        repository_id=repository_id,
        name=repository.name,
        user=current_user,
    )

    # Fetch pipelines for the repository
    pipeline_infos = await manager.get_pipelines_for_repository(repo.id)

    return RepositoryDetailResponse(
        id=str(repo.id),
        name=repo.name,
        rules=repo.rules_count,
        type=repo.type,
        type_display=get_repository_type_display(repo.type),
        created=repo.created,
        updated=repo.updated,
        sync_enabled=repo.sync_enabled,
        source_link=repo.source_link,
        pipelines=pipeline_infos,
    )


@router.delete(
    "/repositories/{repository_id}",
    status_code=204,
    summary="Delete repository",
    responses={
        204: {"description": "Repository deleted successfully"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Repository not found"},
    },
)
@router.delete("/repositories/{repository_id}/", include_in_schema=False)
async def delete_repository(
    repository_id: UUID,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a repository.

    Works for local, API (SOCPrime), and external repositories.
    All rules in the repository will also be deleted.
    Repositories can be added back using the corresponding add endpoints if needed.
    """
    manager = RepositoriesManager(db)
    await manager.delete(repository_id=repository_id, user=current_user)


@router.patch(
    "/repositories/{repository_id}/sync",
    response_model=RepositorySyncToggleResponse,
    summary="Toggle sync for repository",
    responses={
        200: {"description": "Sync status updated successfully or already in requested state"},
        400: {"model": ErrorResponse, "description": "Repository is local"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Repository not found"},
    },
)
@router.patch("/repositories/{repository_id}/sync/", include_in_schema=False)
async def toggle_repository_sync(
    repository_id: UUID,
    request: RepositorySyncToggleRequest,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Enable or disable sync for a SOCPrime or external repository.

    **Note:** Only API (SOCPrime) and external repositories support sync. Local repositories cannot be synced.
    """
    manager = RepositoriesManager(db)
    message, sync_enabled = await manager.toggle_sync(
        repository_id=repository_id,
        sync_enabled=request.sync_enabled,
        user=current_user,
    )

    return RepositorySyncToggleResponse(
        message=message,
        sync_enabled=sync_enabled,
    )


@router.get(
    "/repositories/{repository_id}",
    response_model=RepositoryDetailResponse,
    summary="Get repository by ID",
    responses={
        200: {"description": "Repository details"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        404: {"model": ErrorResponse, "description": "Repository not found"},
    },
)
@router.get("/repositories/{repository_id}/", include_in_schema=False)
async def get_repository(
    repository_id: UUID,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a repository.

    Returns repository metadata including rules count and type (local/api/external).
    """
    manager = RepositoriesManager(db)
    repo = await manager.get_by_id(repository_id)

    # Fetch pipelines for the repository
    pipeline_infos = await manager.get_pipelines_for_repository(repository_id)

    return RepositoryDetailResponse(
        id=str(repo.id),
        name=repo.name,
        rules=repo.rules_count,
        type=repo.type,
        type_display=get_repository_type_display(repo.type),
        created=repo.created,
        updated=repo.updated,
        sync_enabled=repo.sync_enabled,
        source_link=repo.source_link,
        pipelines=pipeline_infos,
    )


@router.get(
    "/repositories",
    response_model=RepositoryListResponse,
    summary="List repositories",
    responses={
        200: {"description": "Paginated list of repositories"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
    },
)
async def get_repositories(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    db: AsyncSession = Depends(get_db),
):
    """
    List all repositories with pagination.

    Returns local, API (SOCPrime TDM), and external repositories with rules count.
    """
    manager = RepositoriesManager(db)
    repos, total = await manager.get_all(pagination)

    # Fetch pipelines for all repositories in a single query
    repo_ids = [repo.id for repo in repos]
    repo_pipelines_map = await manager.get_pipelines_for_repositories(repo_ids)

    return RepositoryListResponse(
        total=total,
        limit=pagination["limit"],
        offset=pagination["offset"],
        sort=pagination["sort"],
        order=pagination["order"],
        data=[
            RepositoryDetailResponse(
                id=str(repo.id),
                name=repo.name,
                rules=repo.rules_count,
                type=repo.type,
                type_display=get_repository_type_display(repo.type),
                created=repo.created,
                updated=repo.updated,
                sync_enabled=repo.sync_enabled,
                source_link=repo.source_link,
                pipelines=repo_pipelines_map.get(repo.id, []),
            )
            for repo in repos
        ],
    )
