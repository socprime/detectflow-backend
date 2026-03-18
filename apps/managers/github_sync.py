from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.github import GitHubClient, GithubRule
from apps.core.constants import SIGMAHQ_URL, SIGMAHQ_UUID
from apps.core.logger import get_logger
from apps.core.models import Rule
from apps.managers.rule import RulesOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.repository import RepositoryDAO
from apps.modules.postgre.rule import RuleDAO

logger = get_logger(__name__)


class GithubRepoNotSupportedError(Exception):
    pass


class GithubSyncManager:
    """Manager for syncing GitHub repositories with the database."""

    @classmethod
    async def sync_all_enabled_repos(cls, db: AsyncSession) -> bool:
        """Sync all external repositories that have sync_enabled=True.

        Args:
            db: Database session

        Returns:
            True if there were enabled repos to sync, False otherwise.
        """
        repos, _ = await RepositoryDAO(db).get_all(filters={"type": "external"})
        enabled_repos = [repo for repo in repos if repo.sync_enabled is True]
        logger.info(f"Syncing {len(enabled_repos)} enabled external repositories")
        for repo in enabled_repos:
            try:
                await cls.sync_repo(repo.id, db)
            except Exception as e:
                logger.error(f"Error syncing external repository {repo.id}: {e}", exc_info=True)
                await activity_producer.log_action(
                    action="sync_failed",
                    entity_type="repository",
                    entity_id=str(repo.id),
                    entity_name=repo.name,
                    details=f"External repository sync failed: {str(e)}",
                    source="system",
                    severity="error",
                )
                # Continue with other repos even if one fails
                continue
        logger.info("Completed syncing external repositories")
        return bool(enabled_repos)

    @classmethod
    async def sync_repo(cls, repo_id: str | UUID, db: AsyncSession) -> None:
        """Sync a repository from GitHub.

        Args:
            repo_id: Repository UUID or string UUID
            db: Database session

        Raises:
            GithubRepoNotSupportedError: If repository is not supported
        """
        repo_uuid = repo_id if isinstance(repo_id, UUID) else UUID(repo_id)
        if str(repo_id) == SIGMAHQ_UUID:
            logger.info(f"Syncing SigmaHQ repository {repo_id}")
            await cls._sync_repo(
                repo_id=repo_uuid,
                url=SIGMAHQ_URL,
                branch="master",
                db=db,
                exclude_patterns=["deprecated/"],
            )
            logger.info(f"Synced SigmaHQ repository {repo_id}")
        else:
            raise GithubRepoNotSupportedError("Repository type is not supported for GitHub sync")

    @classmethod
    async def _sync_repo(
        cls,
        repo_id: UUID,
        url: str,
        branch: str,
        db: AsyncSession,
        exclude_patterns: list[str] | None = None,
    ) -> None:
        """Sync repository rules from GitHub.

        Args:
            repo_id: Repository UUID
            url: GitHub repository URL
            branch: Branch name to sync
            db: Database session
            exclude_patterns: Patterns to exclude from sync
        """
        try:
            github_client = GitHubClient()
            github_rules = await github_client.get_sigma_rules(
                github_url=url, branch=branch, exclude_patterns=exclude_patterns, include_patterns=[".yml", ".yaml"]
            )

            logger.info(f"Got {len(github_rules)} rules from repository {url}")

            rules_dao = RuleDAO(db)
            rules = await rules_dao.get_all_by_repository([repo_id])

            new_rules = cls._get_new_rules(github_rules, rules)
            updated_rules = cls._get_updated_rules(github_rules, rules)
            deleted_rules = cls._get_deleted_rules(github_rules, rules)

            logger.info(
                f"Rules to create: {len(new_rules)}."
                f" Rules to update: {len(updated_rules)}."
                f" Rules to delete: {len(deleted_rules)}."
            )

            if new_rules:
                await cls._create_rules(new_rules, repo_id, rules_dao)
                logger.info(f"Created {len(new_rules)} rules in repository {repo_id}")

            if updated_rules:
                await cls._update_rules(updated_rules, rules_dao)
                logger.info(f"Updated {len(updated_rules)} rules in repository {repo_id}")

            if deleted_rules:
                await cls._delete_rules(deleted_rules, rules_dao)
                logger.info(f"Deleted {len(deleted_rules)} rules in repository {repo_id}")

            rules_orchestrator = RulesOrchestrator(db)
            await rules_orchestrator._update_rules_in_pipelines_and_kafka(
                repo_id=repo_id,
                new_rules=new_rules,
                deleted_rule_ids=[str(rule_id) for rule_id in deleted_rules],
                updated_rules=updated_rules,
            )

            await activity_producer.log_action(
                action="sync",
                entity_type="repository",
                entity_id=str(repo_id),
                details=f"External repository sync completed: {len(new_rules)} new, {len(updated_rules)} updated, {len(deleted_rules)} deleted rules",
                source="system",
            )
        except Exception as e:
            logger.error(f"Error syncing repository {repo_id}: {e}", exc_info=True)
            await activity_producer.log_action(
                action="sync_failed",
                entity_type="repository",
                entity_id=str(repo_id),
                details=f"External repository sync failed: {str(e)}",
                source="system",
                severity="error",
            )
            raise

    @staticmethod
    def _is_valid_uuid(uuid: str) -> bool:
        try:
            UUID(uuid)
            return True
        except ValueError:
            return False

    @staticmethod
    def _get_new_rules(github_rules: list[GithubRule], rules: list[Rule]) -> list[GithubRule]:
        """Get rules that need to be created (not found in database by ID)."""
        new_rules = []
        current_rule_ids = {rule.id for rule in rules}
        for github_rule in github_rules:
            github_rule_uuid = UUID(github_rule.id)
            if github_rule_uuid not in current_rule_ids:
                new_rules.append(github_rule)
        return new_rules

    @staticmethod
    def _get_updated_rules(github_rules: list[GithubRule], rules: list[Rule]) -> list[GithubRule]:
        """Get rules that need to be updated (found in database by ID, body or name changed)."""
        updated_rules = []
        rules_map = {rule.id: rule for rule in rules}
        for github_rule in github_rules:
            github_rule_uuid = UUID(github_rule.id)
            if github_rule_uuid in rules_map:
                existing_rule = rules_map[github_rule_uuid]
                # Update if body or name changed
                if github_rule.body != existing_rule.body or github_rule.name != existing_rule.name:
                    updated_rules.append(github_rule)
        return updated_rules

    @staticmethod
    def _get_deleted_rules(github_rules: list[GithubRule], rules: list[Rule]) -> list[UUID]:
        """Get rules that need to be deleted (exist in database but not in GitHub rules by ID)."""
        deleted_uuids = []
        github_rule_ids = {UUID(rule.id) for rule in github_rules}
        for rule in rules:
            if rule.id not in github_rule_ids:
                deleted_uuids.append(rule.id)
        return deleted_uuids

    @staticmethod
    async def _create_rules(github_rules: list[GithubRule], repository_id: UUID, rules_dao: RuleDAO) -> None:
        """Create new rules in the database using the YAML ID as the primary key."""
        await rules_dao.create_many(
            [
                {
                    "id": github_rule.id,  # Already a string UUID from YAML
                    "name": github_rule.name,
                    "body": github_rule.body,
                    "repository_id": repository_id,
                }
                for github_rule in github_rules
            ]
        )

    @staticmethod
    async def _update_rules(github_rules: list[GithubRule], rules_dao: RuleDAO) -> None:
        """Update existing rules in the database, including name if it changed."""
        await rules_dao.upsert_many(
            [
                {
                    "id": github_rule.id,  # Already a string UUID from YAML
                    "name": github_rule.name,
                    "body": github_rule.body,
                }
                for github_rule in github_rules
            ]
        )

    @staticmethod
    async def _delete_rules(rule_ids: list[UUID], rules_dao: RuleDAO) -> None:
        """Delete rules from the database."""
        await rules_dao.delete_many(rule_ids)
