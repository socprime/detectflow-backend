"""GitLab repository sync — pull rules from GitLab integration-owned repositories."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.gitlab_api import GitLabApiClient, GitLabApiError, GitLabPullChanges, GitlabRule
from apps.core.logger import get_logger
from apps.core.models import GITLAB_REPOSITORY_TYPE, GitlabRepository, Integration, Repository
from apps.core.settings import settings
from apps.integrations.providers.gitlab import GitLabProvider
from apps.managers.rule import RulesOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.gitlab import (
    GitlabRepositoryDAO,
    GitlabRuleMetadataDAO,
    acquire_gitlab_integration_lock,
    list_gitlab_integrations,
)
from apps.modules.postgre.integration import IntegrationDAO
from apps.modules.postgre.repository import RepositoryDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.postgre.rule_content import CHANGE_SOURCE_SYSTEM, RuleContentDAO
from apps.modules.utils import coerce_config_bool, gitlab_folder_path_from_file_path

logger = get_logger(__name__)


class GitlabSyncManager:
    """Manager for syncing GitLab integration-owned repositories."""

    @classmethod
    async def provision_repository(cls, db: AsyncSession, integration: Integration) -> Repository:
        """Create the owned empty repository row for a GitLab integration (idempotent)."""
        gitlab_repo_dao = GitlabRepositoryDAO(db)
        existing = await gitlab_repo_dao.get_by_integration(integration.id)
        if existing is not None:
            repo = await RepositoryDAO(db).get_by_id(existing.repository_id)
            if repo is not None:
                return repo

        config = integration.config or {}
        project_path = str(config.get("project_path") or integration.name)
        base_url = str(config.get("base_url") or "https://gitlab.com").rstrip("/")
        repo_name = integration.name or project_path

        repository = await RepositoryDAO(db).create(
            name=repo_name,
            type=GITLAB_REPOSITORY_TYPE,
            sync_enabled=True,
            source_link=f"{base_url}/{project_path}",
        )
        await gitlab_repo_dao.create(repository_id=repository.id, integration_id=integration.id)
        logger.info(f"Provisioned GitLab repository {repository.id} for integration {integration.id}")
        return repository

    @classmethod
    async def reconcile_owned_repository(cls, db: AsyncSession, integration: Integration) -> None:
        """Align the owned repository's ``name`` and ``sync_enabled`` with the integration profile.

        The integration profile is the source of truth: its ``name`` drives the repository name
        (so the UI can tell which repository belongs to which integration) and its
        ``config.auto_pull`` drives the repository ``sync_enabled`` flag. Called after any GitLab
        integration update. No-op when the link/repository is missing or already in sync.
        """
        link = await GitlabRepositoryDAO(db).get_by_integration(integration.id)
        if link is None:
            return
        repo_dao = RepositoryDAO(db)
        repo = await repo_dao.get_by_id(link.repository_id)
        if repo is None:
            return

        updates: dict[str, object] = {}
        if integration.name and repo.name != integration.name:
            updates["name"] = integration.name
        desired_sync = coerce_config_bool((integration.config or {}).get("auto_pull"), default=True)
        if repo.sync_enabled != desired_sync:
            updates["sync_enabled"] = desired_sync

        if not updates:
            return
        await repo_dao.update(link.repository_id, **updates)
        logger.info(f"Reconciled GitLab repository {link.repository_id} from integration {integration.id}: {updates}")

    @classmethod
    async def delete_owned_repository(cls, db: AsyncSession, integration_id: UUID) -> None:
        """Delete the repository owned by a GitLab integration (rules cascade)."""
        gitlab_repo_dao = GitlabRepositoryDAO(db)
        repository_id = await gitlab_repo_dao.delete_by_integration(integration_id)
        if repository_id is None:
            return
        await RepositoryDAO(db).delete(repository_id)
        logger.info(f"Deleted GitLab-owned repository {repository_id} for integration {integration_id}")

    @classmethod
    async def ensure_repository(cls, db: AsyncSession, integration: Integration) -> Repository:
        """Return the owned repository, creating it when missing."""
        gitlab_repo_dao = GitlabRepositoryDAO(db)
        link = await gitlab_repo_dao.get_by_integration(integration.id)
        if link is not None:
            repo = await RepositoryDAO(db).get_by_id(link.repository_id)
            if repo is not None:
                return repo
        return await cls.provision_repository(db, integration)

    @classmethod
    async def sync_all_enabled_repos(cls, db: AsyncSession) -> bool:
        integrations = await list_gitlab_integrations(db, auto_pull_only=True)
        logger.info(f"Syncing {len(integrations)} GitLab integration(s) with auto_pull enabled")
        for integration in integrations:
            integration_id = integration.id
            integration_name = integration.name
            try:
                await cls.sync_integration(db, integration_id)
                await db.commit()
            except Exception as exc:
                await db.rollback()
                logger.error(f"Error syncing GitLab integration {integration_id}: {exc}", exc_info=True)
                await activity_producer.log_action(
                    action="sync_failed",
                    entity_type="integration",
                    entity_id=str(integration_id),
                    entity_name=integration_name,
                    details=f"GitLab sync failed: {exc}",
                    source="system",
                    severity="error",
                )
        return bool(integrations)

    @classmethod
    async def sync_integration(cls, db: AsyncSession, integration_id: UUID) -> None:
        integration = await cls._get_gitlab_integration(db, integration_id)
        repository = await cls.ensure_repository(db, integration)
        gitlab_link = await GitlabRepositoryDAO(db).get_by_integration(integration.id)
        if gitlab_link is None:
            raise GitLabApiError(f"GitLab repository link missing for integration {integration_id}")
        await cls._sync_repo(db, integration, repository, gitlab_link)

    @classmethod
    async def _get_gitlab_integration(cls, db: AsyncSession, integration_id: UUID) -> Integration:
        integration = await IntegrationDAO(db).get_by_id(integration_id)
        if integration is None:
            raise GitLabApiError(f"Integration {integration_id} not found")
        if integration.integration_type != GitLabProvider.integration_type:
            raise GitLabApiError(f"Integration {integration_id} is not a GitLab profile")
        if not (integration.config or {}).get("token"):
            raise GitLabApiError(f"GitLab integration {integration_id} has no token configured")
        return integration

    @classmethod
    async def _sync_repo(
        cls,
        db: AsyncSession,
        integration: Integration,
        repository: Repository,
        gitlab_link: GitlabRepository,
    ) -> None:
        client = GitLabApiClient.from_integration_config(integration.config or {})
        changes = await client.pull_changes(from_sha=gitlab_link.last_synced_commit_sha)

        if cls._pull_is_noop(changes, gitlab_link.last_synced_commit_sha):
            logger.info(f"GitLab project {client.project_path}: no DB changes to apply")
            return

        await acquire_gitlab_integration_lock(db, integration.id)
        await cls._apply_pull_changes(db, integration, repository, changes)

    @staticmethod
    def _pull_is_noop(changes: GitLabPullChanges, last_synced_commit_sha: str | None) -> bool:
        cursor_unchanged = changes.commit_sha == last_synced_commit_sha
        return cursor_unchanged and not changes.upserts and not changes.deletes

    @classmethod
    async def _apply_pull_changes(
        cls,
        db: AsyncSession,
        integration: Integration,
        repository: Repository,
        changes: GitLabPullChanges,
    ) -> None:
        metadata_dao = GitlabRuleMetadataDAO(db)
        rules_dao = RuleDAO(db)
        existing_metadata = await metadata_dao.get_by_repository(repository.id)
        path_to_rule_id = {row.file_path: row.rule_id for row in existing_metadata}
        existing_rules = await rules_dao.get_all_by_repository([repository.id])
        rules_by_id = {rule.id: rule for rule in existing_rules}

        delete_paths = set(changes.deletes)
        if changes.full:
            upsert_paths = {rule.file_path for rule in changes.upserts}
            delete_paths.update(path for path in path_to_rule_id if path not in upsert_paths)

        deleted_rule_ids: list[UUID] = []
        for file_path in sorted(delete_paths):
            rule_id = path_to_rule_id.get(file_path)
            if rule_id is not None:
                deleted_rule_ids.append(rule_id)

        new_rules: list[GitlabRule] = []
        updated_rules: list[GitlabRule] = []
        metadata_rows: list[dict] = []
        now = datetime.now(UTC)

        for gitlab_rule in changes.upserts:
            folder_path = gitlab_folder_path_from_file_path(gitlab_rule.file_path)
            existing_rule_id = path_to_rule_id.get(gitlab_rule.file_path)
            if existing_rule_id is None:
                rule_id = uuid4()
                await rules_dao.create_many(
                    [
                        {
                            "id": rule_id,
                            "name": gitlab_rule.name,
                            "body": gitlab_rule.body,
                            "repository_id": repository.id,
                            "folder_path": folder_path,
                        }
                    ],
                    change_source=CHANGE_SOURCE_SYSTEM,
                    changed_by="system",
                )
                new_rules.append(gitlab_rule)
                metadata_rows.append(
                    {
                        "rule_id": rule_id,
                        "repository_id": repository.id,
                        "file_path": gitlab_rule.file_path,
                        "last_commit_id": gitlab_rule.last_commit_id,
                        "synced_at": now,
                    }
                )
                path_to_rule_id[gitlab_rule.file_path] = rule_id
                continue

            existing_db_rule = rules_by_id.get(existing_rule_id)
            needs_update = True
            if existing_db_rule is not None:
                needs_update = (
                    gitlab_rule.name != existing_db_rule.name
                    or gitlab_rule.body != existing_db_rule.body
                    or folder_path != existing_db_rule.folder_path
                    or gitlab_rule.last_commit_id
                    != next((row.last_commit_id for row in existing_metadata if row.rule_id == existing_rule_id), None)
                )
            if needs_update:
                await rules_dao.upsert_many(
                    [
                        {
                            "id": existing_rule_id,
                            "name": gitlab_rule.name,
                            "body": gitlab_rule.body,
                            "folder_path": folder_path,
                        }
                    ],
                    change_source=CHANGE_SOURCE_SYSTEM,
                    changed_by="system",
                )
                updated_rules.append(gitlab_rule)
                metadata_rows.append(
                    {
                        "rule_id": existing_rule_id,
                        "repository_id": repository.id,
                        "file_path": gitlab_rule.file_path,
                        "last_commit_id": gitlab_rule.last_commit_id,
                        "synced_at": now,
                    }
                )

        if deleted_rule_ids:
            await rules_dao.delete_many(deleted_rule_ids)

        if metadata_rows:
            await metadata_dao.upsert_many(metadata_rows)

        if new_rules or updated_rules or deleted_rule_ids:
            rules_orchestrator = RulesOrchestrator(db)
            await rules_orchestrator._update_rules_in_pipelines_and_kafka(
                repo_id=repository.id,
                new_rules=new_rules,
                deleted_rule_ids=[str(rule_id) for rule_id in deleted_rule_ids],
                updated_rules=updated_rules,
            )
            await RuleContentDAO(db).prune_all_versions(keep=settings.rule_version_retention)

        if changes.commit_sha is not None:
            await GitlabRepositoryDAO(db).update_cursor(repository.id, changes.commit_sha)

        await activity_producer.log_action(
            action="sync",
            entity_type="integration",
            entity_id=str(integration.id),
            entity_name=integration.name,
            details=(
                f"GitLab sync completed: {len(new_rules)} new, {len(updated_rules)} updated, "
                f"{len(deleted_rule_ids)} deleted rules"
            ),
            source="system",
        )
