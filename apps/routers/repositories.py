"""Repository management endpoints.

This module provides REST API endpoints for managing rule repositories:
SOCPrime TDM API, external (GitHub), GitLab (integration-owned), and local repositories.
Includes sync triggers and status via the scheduler service.
"""

import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, Path, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.database import get_db
from apps.core.exceptions import ConflictError
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.pagination import get_pagination_params_no_page
from apps.core.schemas import (
    AddExternalRepositoriesRequest,
    AddExternalRepositoriesResponse,
    ExternalRepositoryListResponse,
    ExternalRepositoryResponse,
    RepositoryCreateUpdateRequest,
    RepositoryDetailResponse,
    RepositoryListResponse,
    RepositorySyncToggleRequest,
    RepositorySyncToggleResponse,
    SyncStartedResponse,
    SyncTypeStatus,
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
    "gitlab": "GitLab",
}


def get_repository_type_display(repo_type: str) -> str:
    """Get human-readable repository type for UI display."""
    return REPOSITORY_TYPE_DISPLAY.get(repo_type, repo_type)


@router.post(
    "/sync-repositories",
    response_model=SyncStartedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Start repository sync",
    responses={
        202: {"description": "Sync started in background"},
        **errors(UNAUTHORIZED, err(409, "Sync is already running")),
    },
)
@router.post("/sync-repositories/", include_in_schema=False, status_code=status.HTTP_202_ACCEPTED)
async def sync_repositories(
    current_user: User = Depends(require_write_access),
):
    """
    Start manual sync tasks in background.

    Returns immediately with 202 Accepted; the work runs in an asyncio background task
    (repository sync of SOCPrime TDM API, GitHub, and GitLab integrations with
    ``auto_pull=true``, plus Active Threat sync when enabled). GitLab integrations with
    ``auto_pull=false`` are skipped here — use ``POST /api/v1/integrations/{id}/pull`` instead.
    Emits a `sync_start` activity-log entry and rejects with 409 if a sync is already
    running. Poll `GET /sync-repositories/status` for progress.
    """
    scheduler = get_scheduler_service()
    if scheduler.is_sync_running:
        raise ConflictError("Sync is already running")

    await activity_producer.log_action(
        action="sync_start",
        entity_type="repository",
        user=current_user,
        details="Manual sync triggered",
        source="user",
    )

    asyncio.create_task(scheduler.run_sync_in_background())
    return {"message": "Sync started in background"}


@router.get(
    "/sync-repositories/status",
    response_model=dict[str, SyncTypeStatus],
    summary="Get sync status",
    responses=errors(UNAUTHORIZED),
)
@router.get("/sync-repositories/status/", include_in_schema=False)
async def get_sync_status(
    _: User = Depends(get_current_active_user),
):
    """
    Get the status of the most recent (or in-progress) manual sync run.

    Returns a map keyed by sync type — `api_repos`, `git_hub_repos`, `gitlab_repos`,
    `active_threats` — where each value carries the run state (`running`, `completed`,
    `failed`) plus timestamps and any error. Sync types that are currently idle are omitted
    from the map.
    """
    scheduler = get_scheduler_service()
    return scheduler.sync_statuses


@router.get(
    "/external-repositories",
    response_model=ExternalRepositoryListResponse,
    summary="Get list of external repositories",
    responses=errors(UNAUTHORIZED),
)
@router.get("/external-repositories/", include_in_schema=False)
async def get_external_repositories(
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List the available external (GitHub) repositories.

    Returns the static catalog of supported external repositories, each flagged with
    `is_added` to indicate whether it already exists locally. Read-only; no state changes.
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
    responses=errors(UNAUTHORIZED, err(400, "Unknown external repository IDs")),
)
@router.post("/add-external-repositories/", include_in_schema=False)
async def add_external_repositories(
    request: AddExternalRepositoriesRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Add external (GitHub) repositories to the local database.

    Validates the requested IDs against the static external catalog, persists the ones not
    already present (each logged as a `create` activity entry), and reports already-present
    IDs as `skipped`. Rejects unknown repository IDs with 400.
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
    responses=errors(UNAUTHORIZED),
)
@router.post("/repositories/", include_in_schema=False)
async def create_local_repository(
    repository: RepositoryCreateUpdateRequest,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new local repository.

    Local repositories hold rules authored manually in the UI, unlike API (SOCPrime TDM)
    or external (GitHub) repositories which are populated by sync. The new repository starts
    empty (no rules, no pipelines) and the action is recorded in the activity log.
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
        is_system=repo.is_system,
        system_key=repo.system_key,
        pipelines=[],  # New repository has no pipelines yet
    )


@router.patch(
    "/repositories/{repository_id}",
    response_model=RepositoryDetailResponse,
    summary="Update local repository",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Repository is system-managed or not a local repository"),
        err(404, "Repository not found"),
    ),
)
@router.patch("/repositories/{repository_id}/", include_in_schema=False)
async def update_local_repository(
    repository_id: UUID = Path(..., description="UUID of the local repository to update"),
    repository: RepositoryCreateUpdateRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Rename a local repository and record the change in the activity log.

    Only local repositories are editable: API (SOCPrime) and external repositories are
    read-only, and system-managed repositories are locked — both reject with 400. GitLab
    (integration-owned) repositories are renamed indirectly by renaming their integration profile.
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
        is_system=repo.is_system,
        system_key=repo.system_key,
        pipelines=pipeline_infos,
    )


@router.delete(
    "/repositories/{repository_id}",
    status_code=204,
    summary="Delete repository",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Repository is system-managed and cannot be deleted"),
        err(404, "Repository not found"),
        err(409, "Repository is still attached to one or more pipelines"),
    ),
)
@router.delete("/repositories/{repository_id}/", include_in_schema=False)
async def delete_repository(
    repository_id: UUID = Path(..., description="UUID of the repository to delete"),
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Delete a repository and all of its rules.

    Works for local, API (SOCPrime), and external repositories; system-managed repositories
    are locked (400) and repositories still attached to a pipeline are rejected (409). Deleting
    a repository cascades to its rules and emits Kafka rule-deletes for any connected pipelines,
    plus a `delete` activity-log entry. Non-system repositories can be re-added later via the
    corresponding add endpoints.
    """
    manager = RepositoriesManager(db)
    await manager.delete(repository_id=repository_id, user=current_user)


@router.patch(
    "/repositories/{repository_id}/sync",
    response_model=RepositorySyncToggleResponse,
    summary="Toggle sync for repository",
    responses=errors(
        UNAUTHORIZED,
        err(400, "Repository is system-managed or local (sync unsupported)"),
        err(404, "Repository not found"),
    ),
)
@router.patch("/repositories/{repository_id}/sync/", include_in_schema=False)
async def toggle_repository_sync(
    repository_id: UUID = Path(..., description="UUID of the repository to toggle sync for"),
    request: RepositorySyncToggleRequest = ...,
    current_user: User = Depends(require_write_access),
    db: AsyncSession = Depends(get_db),
):
    """
    Enable or disable scheduled sync for an API (SOCPrime), external, or GitLab repository.

    Persists the new `sync_enabled` flag and logs a `toggle` activity entry; a no-op request
    (already in the requested state) succeeds with an explanatory message. For GitLab
    (integration-owned) repositories this flips the owning integration's `auto_pull` flag, which
    drives the scheduler. Local and system-managed repositories do not support sync and are
    rejected with 400.
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
    responses=errors(UNAUTHORIZED, err(404, "Repository not found")),
)
@router.get("/repositories/{repository_id}/", include_in_schema=False)
async def get_repository(
    repository_id: UUID = Path(..., description="UUID of the repository to fetch"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed information about a single repository.

    Returns repository metadata (name, type, rules count, sync flag, source link) plus the
    list of pipelines it is attached to. Read-only.
    """
    manager = RepositoriesManager(db)
    repo = await manager.get_by_id(repository_id)

    # Fetch pipelines for the repository
    pipeline_infos = await manager.get_pipelines_for_repository(repository_id)
    folder_trees = await manager.get_folder_trees_for_repositories([repository_id])

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
        is_system=repo.is_system,
        system_key=repo.system_key,
        pipelines=pipeline_infos,
        folders=folder_trees.get(repository_id, []),
    )


@router.get(
    "/repositories",
    response_model=RepositoryListResponse,
    summary="List repositories",
    responses=errors(UNAUTHORIZED),
)
async def get_repositories(
    _: User = Depends(get_current_active_user),
    pagination: dict = Depends(get_pagination_params_no_page),
    db: AsyncSession = Depends(get_db),
):
    """
    List all repositories with pagination, sorting, and search.

    Returns local, API (SOCPrime TDM), and external repositories together with their rules
    count and attached pipelines. Read-only.
    """
    manager = RepositoriesManager(db)
    repos, total = await manager.get_all(pagination)

    # Fetch pipelines for all repositories in a single query
    repo_ids = [repo.id for repo in repos]
    repo_pipelines_map = await manager.get_pipelines_for_repositories(repo_ids)
    repo_folders_map = await manager.get_folder_trees_for_repositories(repo_ids)

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
                is_system=repo.is_system,
                system_key=repo.system_key,
                pipelines=repo_pipelines_map.get(repo.id, []),
                folders=repo_folders_map.get(repo.id, []),
            )
            for repo in repos
        ],
    )
