"""Repository management orchestrator.

This module provides the RepositoriesManager class for handling
repository lifecycle operations with activity logging.
"""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.constants import EXTERNAL_REPOSITORIES
from apps.core.exceptions import (
    BadRequestError,
    ConflictError,
    NotFoundError,
)
from apps.core.logger import get_logger
from apps.core.models import GITLAB_REPOSITORY_TYPE, Repository, User
from apps.core.schemas import PipelineInfo
from apps.managers.rule import RepositoryNotFoundError, RulesOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.gitlab import GitlabRepositoryDAO
from apps.modules.postgre.integration import IntegrationDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.repository import RepositoryDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import build_rule_folder_tree, coerce_config_bool

logger = get_logger(__name__)


def _reject_gitlab_managed(repo: Repository) -> None:
    if repo.type == GITLAB_REPOSITORY_TYPE:
        raise BadRequestError("This GitLab repository is managed by its integration profile.")


class RepositoriesManager:
    """Manager for repository lifecycle operations."""

    def __init__(self, db: AsyncSession):
        """Initialize manager with database session."""
        self.db = db
        self.repo_storage = RepositoryDAO(db)
        self.pipeline_dao = PipelineDAO(db)
        self.rule_dao = RuleDAO(db)

    async def get_all(
        self,
        pagination: dict,
    ) -> tuple[list[Repository], int]:
        """Get paginated list of repositories."""
        return await self.repo_storage.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            sort=pagination["sort"],
            order=pagination["order"],
        )

    async def get_by_id(self, repository_id: UUID) -> Repository:
        """Get repository by ID.

        Raises:
            NotFoundError: If repository not found
        """
        repo = await self.repo_storage.get_by_id(repository_id)
        if not repo:
            raise NotFoundError(f"Repository with id {repository_id} not found")
        return repo

    async def get_pipelines_for_repository(self, repository_id: UUID) -> list[PipelineInfo]:
        """Get pipelines associated with a repository."""
        pipelines = await self.pipeline_dao.get_by_repository_id(repository_id)
        return [PipelineInfo(id=str(p.id), name=p.name) for p in pipelines]

    async def get_pipelines_for_repositories(self, repo_ids: list[UUID]) -> dict[UUID, list[PipelineInfo]]:
        """Get pipelines for multiple repositories in a single query."""
        pipelines_by_repo = await self.pipeline_dao.get_by_repository_ids(repo_ids)
        return {
            repo_id: [PipelineInfo(id=str(p.id), name=p.name) for p in pipelines]
            for repo_id, pipelines in pipelines_by_repo.items()
        }

    async def get_folder_trees_for_repositories(self, repo_ids: list[UUID]) -> dict[UUID, list[dict]]:
        """Get virtual rule folder trees for multiple repositories."""
        folder_counts_by_repo = await self.rule_dao.get_folder_counts_by_repositories(repo_ids)
        return {repo_id: build_rule_folder_tree(folder_counts_by_repo.get(repo_id, {})) for repo_id in repo_ids}

    # ==========================================
    # LOCAL REPOSITORY OPERATIONS
    # ==========================================

    async def create_local(
        self,
        name: str,
        user: User,
    ) -> Repository:
        """Create a new local repository.

        Args:
            name: Repository name
            user: Current user for activity logging

        Returns:
            Created Repository instance
        """
        repo = await self.repo_storage.create(name=name, type="local")

        await activity_producer.log_action(
            action="create",
            entity_type="repository",
            entity_id=str(repo.id),
            entity_name=repo.name,
            user=user,
            details=f"Created local repository {repo.name}",
        )

        return repo

    async def update_local(
        self,
        repository_id: UUID,
        name: str,
        user: User,
    ) -> Repository:
        """Update a local repository.

        GitLab integration-owned repositories are renamed indirectly by renaming their
        integration profile (see :meth:`IntegrationsManager.update`), so this path keeps them
        read-only to preserve the integration↔repository name link.

        Args:
            repository_id: UUID of repository to update
            name: New repository name
            user: Current user for activity logging

        Returns:
            Updated Repository instance

        Raises:
            NotFoundError: If repository not found
            HTTPException: If repository is not local
        """
        repo = await self.repo_storage.get_by_id(repository_id)
        if not repo:
            raise NotFoundError(f"Repository with id {repository_id} not found")
        if repo.is_system:
            raise BadRequestError("This repository is managed by the system and cannot be modified.")
        _reject_gitlab_managed(repo)
        if repo.type != "local":
            raise BadRequestError("Repository is not local")

        await self.repo_storage.update(repository_id, name=name)

        await activity_producer.log_action(
            action="update",
            entity_type="repository",
            entity_id=str(repo.id),
            entity_name=name,
            user=user,
            details=f"Updated local repository {name}",
        )

        # Refresh repository to get updated data
        return await self.repo_storage.get_by_id(repository_id)

    async def delete(
        self,
        repository_id: UUID,
        user: User,
    ) -> None:
        """Delete a repository.

        Args:
            repository_id: UUID of repository to delete
            user: Current user for activity logging

        Raises:
            NotFoundError: If repository not found
        """
        repo = await self.repo_storage.get_by_id(repository_id)
        if not repo:
            raise NotFoundError(f"Repository with id {repository_id} not found")
        if repo.is_system:
            raise BadRequestError("This repository is managed by the system and cannot be deleted.")
        _reject_gitlab_managed(repo)

        repo_name = repo.name
        repo_type = repo.type

        # Check if repository is connected to any pipelines
        pipelines = await self.pipeline_dao.get_by_repository_id(repository_id)
        if pipelines:
            pipeline_names = ", ".join(p.name for p in pipelines)
            raise ConflictError(f"Cannot delete repository '{repo_name}': it is used by pipeline(s): {pipeline_names}")

        rules_orchestrator = RulesOrchestrator(self.db)
        try:
            await rules_orchestrator.delete_repository(repository_id=repository_id)
        except RepositoryNotFoundError as e:
            raise NotFoundError(str(e)) from e

        repo_type_label = {
            "local": "local folder",
            "api": "SOCPrime repository",
            "external": "external repository",
        }.get(repo_type, "repository")

        await activity_producer.log_action(
            action="delete",
            entity_type="repository",
            entity_id=str(repository_id),
            entity_name=repo_name,
            user=user,
            details=f"Deleted {repo_type_label} {repo_name}",
        )

    # ==========================================
    # SYNC TOGGLE
    # ==========================================

    async def toggle_sync(
        self,
        repository_id: UUID,
        sync_enabled: bool,
        user: User,
    ) -> tuple[str, bool]:
        """Toggle sync for a repository.

        Args:
            repository_id: UUID of repository
            sync_enabled: New sync state
            user: Current user for activity logging

        Returns:
            Tuple of (message, sync_enabled)

        Raises:
            NotFoundError: If repository not found
            BadRequestError: If repository is local (sync unsupported)
        """
        repo = await self.repo_storage.get_by_id(repository_id)
        if not repo:
            raise NotFoundError(f"Repository with id {repository_id} not found")
        if repo.is_system:
            raise BadRequestError("This repository is managed by the system and cannot be modified.")
        if repo.type == GITLAB_REPOSITORY_TYPE:
            return await self._toggle_gitlab_sync(repo, sync_enabled, user)
        if repo.type == "local":
            raise BadRequestError(
                "Local repositories do not support sync. Only API and external repositories can be synced."
            )

        # Check if sync is already in the requested state
        if repo.sync_enabled is sync_enabled:
            status_text = "enabled" if sync_enabled else "disabled"
            return f"Sync is already {status_text} for this repository", sync_enabled

        await self.repo_storage.update(repository_id, sync_enabled=sync_enabled)

        action_text = "Enabled" if sync_enabled else "Disabled"
        message_text = "enabled" if sync_enabled else "disabled"

        await activity_producer.log_action(
            action="toggle",
            entity_type="repository",
            entity_id=str(repo.id),
            entity_name=repo.name,
            user=user,
            details=f"{action_text} sync (pull/push) for {repo.type} repository {repo.name}",
        )

        return f"Sync {message_text} successfully", sync_enabled

    async def _toggle_gitlab_sync(
        self,
        repo: Repository,
        sync_enabled: bool,
        user: User,
    ) -> tuple[str, bool]:
        """Toggle scheduled sync for a GitLab repository by flipping its integration ``auto_pull``.

        The scheduler decides which GitLab profiles to pull from ``config.auto_pull`` (see
        ``list_gitlab_integrations(auto_pull_only=True)``), so the repository's ``sync_enabled``
        column is kept in lockstep with that flag purely so the UI shows a consistent state.
        """
        link = await GitlabRepositoryDAO(self.db).get_by_repository(repo.id)
        if link is None:
            raise NotFoundError(f"GitLab integration link for repository {repo.id} not found")

        integration_dao = IntegrationDAO(self.db)
        integration = await integration_dao.get_by_id(link.integration_id)
        if integration is None:
            raise NotFoundError(f"GitLab integration {link.integration_id} not found")

        current = coerce_config_bool((integration.config or {}).get("auto_pull"), default=True)
        if current is sync_enabled and repo.sync_enabled is sync_enabled:
            status_text = "enabled" if sync_enabled else "disabled"
            return f"Sync is already {status_text} for this repository", sync_enabled

        await integration_dao.update(
            link.integration_id, config={**(integration.config or {}), "auto_pull": sync_enabled}
        )
        await self.repo_storage.update(repo.id, sync_enabled=sync_enabled)

        action_text = "Enabled" if sync_enabled else "Disabled"
        message_text = "enabled" if sync_enabled else "disabled"

        await activity_producer.log_action(
            action="toggle",
            entity_type="repository",
            entity_id=str(repo.id),
            entity_name=repo.name,
            user=user,
            details=f"{action_text} auto-pull for GitLab repository {repo.name}",
        )

        return f"Sync {message_text} successfully", sync_enabled

    # ==========================================
    # EXTERNAL REPOSITORY OPERATIONS
    # ==========================================

    async def get_external_repositories(self) -> list[dict]:
        """Get list of available external repositories.

        Returns:
            List of repository dicts with id, name, source_link, is_added
        """
        # Get all existing external repositories from database
        existing_repos, _ = await self.repo_storage.get_all(filters={"type": "external"}, limit=1000)
        existing_repo_ids = {str(repo.id) for repo in existing_repos}

        return [
            {
                "id": repo["id"],
                "name": repo["name"],
                "source_link": repo["source_link"],
                "is_added": repo["id"] in existing_repo_ids,
            }
            for repo in EXTERNAL_REPOSITORIES
        ]

    async def add_external_repositories(
        self,
        repository_ids: list[str],
        user: User,
    ) -> tuple[list[str], list[str]]:
        """Add external repositories to the database.

        Args:
            repository_ids: List of repository IDs to add
            user: Current user for activity logging

        Returns:
            Tuple of (added_ids, skipped_ids)

        Raises:
            HTTPException: If invalid repository IDs
        """
        # Create mapping of available external repositories
        available_repos_dict = {repo["id"]: repo for repo in EXTERNAL_REPOSITORIES}
        available_repo_ids = set(available_repos_dict.keys())

        # Validate requested repository IDs
        requested_ids = set(repository_ids)
        invalid_ids = requested_ids - available_repo_ids

        if invalid_ids:
            raise BadRequestError(f"Invalid repository IDs: {', '.join(sorted(invalid_ids))}")

        # Get existing repositories from database
        existing_repos, _ = await self.repo_storage.get_all(filters={"type": "external"}, limit=1000)
        existing_repo_ids = {str(repo.id) for repo in existing_repos}

        # Separate repositories into added and skipped
        added_ids = []
        skipped_ids = []

        for repo_id in requested_ids:
            if repo_id in existing_repo_ids:
                skipped_ids.append(repo_id)
            else:
                # Add repository to database
                repo = available_repos_dict[repo_id]
                await self.repo_storage.create(
                    id=UUID(repo_id),
                    name=repo["name"],
                    type="external",
                    sync_enabled=False,
                    last_synced_rule_date=None,
                    source_link=repo["source_link"],
                )
                added_ids.append(repo_id)

                await activity_producer.log_action(
                    action="create",
                    entity_type="repository",
                    entity_id=repo_id,
                    entity_name=repo["name"],
                    user=user,
                    details=f"Added external repository {repo['name']}",
                )

        return added_ids, skipped_ids
