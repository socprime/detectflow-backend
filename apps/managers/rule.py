import asyncio
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.github import GithubRule
from apps.clients.tdm_api import (
    TdmApiBadRequestError,
    TDMAPIClient,
    TdmApiNotFoundError,
    TdmRepository,
    TdmRule,
)
from apps.core.exceptions import NotFoundError, RepositoryNotLocalError
from apps.core.logger import get_logger
from apps.core.models import Repository, Rule, User
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.rules import KafkaRulesSyncService
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO
from apps.modules.postgre.repository import RepositoryDAO
from apps.modules.postgre.rule import RuleDAO

logger = get_logger(__name__)


# Legacy aliases for backward compatibility with existing imports
RepositoryNotFoundError = NotFoundError
RuleNotFoundError = NotFoundError


class RulesOrchestrator:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.kafka = KafkaRulesSyncService()
        self.rule_repo = RuleDAO(db)
        self.pipeline_rule_repo = PipelineRulesDAO(db)
        self.pipeline_repo = PipelineDAO(db)
        self.config_repo = ConfigDAO(db)
        self.repository_repo = RepositoryDAO(db)

    # ==========================================
    # RULE OPERATIONS
    # ==========================================

    async def get_all(
        self,
        pagination: dict,
        repository_id: UUID | None = None,
    ) -> tuple[list[Rule], int]:
        """Get paginated list of rules with optional repository filter."""
        filters = {}
        if repository_id:
            filters["repository_id"] = repository_id

        return await self.rule_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            filters=filters if filters else None,
            search=pagination["search"],
            search_fields=["name", "product", "service", "category"],
            sort=pagination["sort"],
            order=pagination["order"],
        )

    async def get_by_id(self, rule_id: UUID) -> Rule:
        """Get rule by ID.

        Raises:
            RuleNotFoundError: If rule not found
        """
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        return rule

    async def create_local_rule(
        self,
        rule_data: dict,
        repository_id: UUID,
        user: User,
    ) -> Rule:
        """
        Create a local rule and sync it to connected pipelines.

        Raises:
            RepositoryNotFoundError: If repository not found.
            RepositoryNotLocalError: If repository is not local.
        """
        repository = await self.repository_repo.get_by_id(repository_id)
        if not repository:
            raise RepositoryNotFoundError("Repository not found")
        if repository.type != "local":
            raise RepositoryNotLocalError(
                f"Rules can only be created in local repositories. Repository type is '{repository.type}'"
            )

        rule = await self.rule_repo.create(repository_id=repository_id, **rule_data)

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)

        await self._add_rules_to_pipelines(pipeline_ids=[pipeline.id for pipeline in pipelines], rule_ids=[rule.id])

        for pipeline in pipelines:
            await self.kafka.send_rules(str(pipeline.id), [rule])

        await activity_producer.log_action(
            action="create",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule.name,
            user=user,
            details=f"Created rule {rule.name}",
        )

        return rule

    async def _add_rules_to_pipelines(self, pipeline_ids: list[UUID | str], rule_ids: list[UUID | str]) -> None:
        pipeline_rules_data = []
        for pipeline_id in pipeline_ids:
            for rule_id in rule_ids:
                pipeline_rules_data.append({"pipeline_id": pipeline_id, "rule_id": rule_id, "enabled": True})
        if pipeline_rules_data:
            await self.pipeline_rule_repo.create_many(pipeline_rules_data)

    async def delete_local_rule(self, rule_id: UUID, user: User) -> None:
        """
        Delete a local rule and remove it from Kafka for connected pipelines.
        """
        # validate that the rule is local
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        if rule.repository.type != "local":
            raise RepositoryNotLocalError(
                f"Rules can only be deleted from local repositories. Repository type is '{rule.repository.type}'"
            )

        rule_name = rule.name

        pipeline_ids = await self.pipeline_rule_repo.get_pipelines_by_rule_id(rule_id)

        for pipeline_id in pipeline_ids:
            await self.kafka.delete_rules(str(pipeline_id), [str(rule_id)])

        # Delete in DB (Cascade handles pipeline_rules)
        await self.rule_repo.delete(rule_id)

        await activity_producer.log_action(
            action="delete",
            entity_type="rule",
            entity_id=str(rule_id),
            entity_name=rule_name,
            user=user,
            details=f"Deleted rule {rule_name}",
        )

    async def update_rule(self, rule_id: UUID, update_data: dict, user: User) -> Rule:
        """
        Update a rule and resync to Kafka for pipelines where it is enabled.

        For API rules, also updates the rule in the remote API.
        External repository rules cannot be updated (read-only).

        Args:
            rule_id: UUID of the rule to update.
            update_data: Dictionary with fields to update (name, body, etc.).
            user: Current user for activity logging.

        Returns:
            Updated Rule object, or None if rule not found.

        Raises:
            RepositoryNotLocalError: If rule is in external repository (read-only).
            TdmApiBadRequestError: If API update fails with bad request.
            TdmApiNotFoundError: If API rule not found.
            RuleNotFoundError: If rule not found in database.
        """
        existing_rule = await self.rule_repo.get_by_id(rule_id)

        if not existing_rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")

        # Prevent updating rules in external repositories
        if existing_rule.repository.type == "external":
            raise RepositoryNotLocalError(
                "Rules in external repositories cannot be updated. They are read-only and managed by external sources."
            )

        # Set default updated timestamp (will be overwritten by API response if API rule)
        update_data["updated"] = datetime.now(tz=UTC)

        # Update in remote API if this is an API rule
        if existing_rule.repository.type == "api":
            api_key = await self.config_repo.get_api_key()
            tdm_api_client = TDMAPIClient(api_key=api_key)
            try:
                tdm_rule = await tdm_api_client.update_rule(
                    repository_id=str(existing_rule.repository.id),
                    rule_id=str(rule_id),
                    name=update_data.get("name"),
                    body=update_data.get("body"),
                )
                update_data["updated"] = tdm_rule.updated
                update_data["name"] = tdm_rule.name
                update_data["body"] = tdm_rule.body
            except (TdmApiBadRequestError, TdmApiNotFoundError):
                raise

        rule = await self.rule_repo.update(rule_id, **update_data)

        # Send rules to Kafka for pipelines where it is enabled
        pipeline_rules = await self.pipeline_rule_repo.get_by_rule_id(rule_id)
        for pr in pipeline_rules:
            if pr.enabled:
                await self.kafka.send_rules(str(pr.pipeline_id), [rule])

        await activity_producer.log_action(
            action="update",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule.name,
            user=user,
            details=f"Updated rule {rule.name}",
        )

        return rule

    # ==========================================
    # REPOSITORY OPERATIONS
    # ==========================================

    async def delete_repository(self, repository_id: UUID) -> None:
        """
        Delete a repository and all its rules, ensuring Kafka cleanup.
        Works for local, API, and external repositories.
        """
        repository = await self.repository_repo.get_by_id(repository_id)
        if not repository:
            raise RepositoryNotFoundError("Repository not found")

        await self._delete_repository(repository_id)

    async def _delete_repository(self, repository_id: UUID) -> None:
        rules = await self.rule_repo.get_all_by_repository([repository_id])
        rule_ids = [str(r.id) for r in rules]

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)

        for pipeline in pipelines:
            await self.kafka.delete_rules(str(pipeline.id), rule_ids)

        # Delete from DB (cascade will handle rules, pipeline_rules and pipeline_repositories)
        await self.repository_repo.delete(repository_id)

    # ==========================================
    # PIPELINE OPERATIONS
    # ==========================================

    async def handle_pipeline_creation(self, pipeline_id: UUID, repository_ids: list[UUID]) -> None:
        """
        Handle the creation of a pipeline. This is called after a pipeline is created.
        It adds repositories to the pipeline and adds rules to the pipeline.
        It also sends rules to Kafka for the pipeline.
        """
        await self._add_repos_to_pipeline(pipeline_id, repository_ids)

    async def _add_repos_to_pipeline(self, pipeline_id: UUID, repository_ids: list[UUID]) -> None:
        if not repository_ids:
            return

        await self.pipeline_repo.add_repositories(pipeline_id, repository_ids)

        rules = await self.rule_repo.get_all_by_repository(repository_ids)
        await self._add_rules_to_pipelines(pipeline_ids=[pipeline_id], rule_ids=[rule.id for rule in rules])
        await self.kafka.send_rules(str(pipeline_id), rules)

    async def handle_pipeline_deletion(self, pipeline_id: UUID) -> None:
        """
        Handle the deletion of a pipeline. This is called BEFORE a pipeline is deleted.
        It removes all repositories from the pipeline and deletes all rules from the pipeline.
        Ir also deletes all rules from Kafka for the pipeline.
        """
        # TODO: consider to remove deletion of pipeline_rules and pipeline_repositories as it is handled by cascade
        rules = await self.pipeline_rule_repo.get_rules_by_pipeline(pipeline_id)
        await self.pipeline_rule_repo.delete_by_pipeline(pipeline_id)
        await self.pipeline_repo.remove_all_repositories(pipeline_id)
        if rules:
            await self.kafka.delete_rules(str(pipeline_id), [str(r.id) for r in rules])

    async def enable_rule_in_pipeline(self, pipeline_id: UUID, rule_id: UUID) -> None:
        logger.info(f"Enabling rule {rule_id} in pipeline {pipeline_id}")
        await self.pipeline_rule_repo.update_rule_status(pipeline_id, rule_id, True)
        rule = await self.rule_repo.get_by_id(rule_id)
        if rule:
            logger.info(f"Sending rule {rule_id} to Kafka for pipeline {pipeline_id}")
            await self.kafka.send_rules(str(pipeline_id), [rule])
        else:
            logger.warning(f"Rule {rule_id} not found in database, cannot send to Kafka")

    async def disable_rule_in_pipeline(self, pipeline_id: UUID, rule_id: UUID) -> None:
        await self.pipeline_rule_repo.update_rule_status(pipeline_id, rule_id, False)
        await self.kafka.delete_rules(str(pipeline_id), [str(rule_id)])

    async def handle_pipeline_enable(self, pipeline_id: UUID) -> None:
        """
        Handle Pipeline Enable event: Sync all enabled rules to Kafka.
        """
        rules, _ = await self.pipeline_rule_repo.get_by_pipeline(pipeline_id, limit=100000)
        enabled_rules: list[Rule] = [rule.rule for rule in rules if rule.enabled]
        disabled_rules: list[Rule] = [rule.rule for rule in rules if not rule.enabled]

        if enabled_rules:
            await self.kafka.send_rules(str(pipeline_id), enabled_rules)
        if disabled_rules:
            await self.kafka.delete_rules(str(pipeline_id), [str(rule.id) for rule in disabled_rules])

    async def handle_pipeline_update(
        self,
        pipeline_id: UUID,
        added_repo_ids: list[UUID],
        removed_repo_ids: list[UUID],
    ) -> None:
        """
        Handle changes in pipeline. This is called after a pipeline is updated.

        This function is called after a pipeline is updated. It handles the following:
        - Add new rules to pipeline
        - Remove rules from pipeline

        Args:
            pipeline_id: UUID of the pipeline.
            added_repo_ids: List of repository IDs that were added to the pipeline.
            removed_repo_ids: List of repository IDs that were removed from the pipeline.
        """
        await self._add_repos_to_pipeline(pipeline_id, added_repo_ids)

        removed_rules = await self.pipeline_rule_repo.get_by_pipeline_and_repo(pipeline_id, removed_repo_ids)
        await self.pipeline_rule_repo.delete_by_pipeline_and_repo(pipeline_id, removed_repo_ids)
        await self.pipeline_repo.remove_repositories(pipeline_id, removed_repo_ids)
        if removed_rules:
            await self.kafka.delete_rules(str(pipeline_id), [str(r.rule_id) for r in removed_rules])

    # ==========================================
    # REPOSITORY SYNC OPERATIONS
    # ==========================================

    async def sync_api_repos(self) -> None:
        # TODO: process kafka updates in the end to avoid inconsistency between database and kafka
        # Get all API repositories, but only sync those with sync_enabled=True
        all_current_repos, _ = await self.repository_repo.get_all(filters={"type": "api"}, limit=1000)
        # Filter to only repos with sync_enabled=True
        current_repos = [repo for repo in all_current_repos if repo.sync_enabled is True]

        tdm_api_client = TDMAPIClient(api_key=await self.config_repo.get_api_key())
        tdm_repos = await tdm_api_client.get_repositories()

        current_repo_ids = {str(repo.id) for repo in current_repos}
        tdm_repo_ids = {repo.id for repo in tdm_repos}

        # Delete repositories that no longer exist in SOCPrime API
        deleted_repos = current_repo_ids - tdm_repo_ids
        for repo_id in deleted_repos:
            await self._delete_repository(UUID(repo_id))

        # Update repositories that exist in both DB and SOCPrime API
        # Only process repos that have sync_enabled=True
        repos_to_update = [repo for repo in tdm_repos if repo.id in current_repo_ids]
        self._enrich_tdm_repos_with_last_synced_rule_date(repos_to_update, current_repos)
        await self._update_api_repos(repos_to_update)

    def _enrich_tdm_repos_with_last_synced_rule_date(
        self, tdm_repos: list[TdmRepository], current_repos: list[Repository]
    ) -> None:
        current_repos_dict = {str(repo.id): repo for repo in current_repos}
        for repo in tdm_repos:
            if repo.id in current_repos_dict:
                repo.last_synced_rule_date = current_repos_dict[repo.id].last_synced_rule_date

    async def _create_new_repos(self, tdm_repos: list[TdmRepository]) -> None:
        repo_rules_pairs = await self._fetch_rules_from_tdm(tdm_repos, fetch_all=True)
        for repo, rules in repo_rules_pairs:
            await self.repository_repo.create(
                id=repo.id,
                name=repo.name,
                type="api",
                sync_enabled=True,
                last_synced_rule_date=max(rule.updated for rule in rules) if rules else None,
            )
            await self._upsert_rules(repo.id, rules)

    async def _update_api_repos(self, tdm_repos: list[TdmRepository]) -> None:
        repo_rules_pairs = await self._fetch_rules_from_tdm(tdm_repos, fetch_all=False)
        tdm_rule_ids_by_repo_id = await self._fetch_tdm_rule_ids(tdm_repos)
        for repo, rules in repo_rules_pairs:
            repo_uuid = UUID(repo.id)
            current_rules = await self.rule_repo.get_all_by_repository([repo_uuid])
            current_rule_ids = {str(rule.id) for rule in current_rules}
            all_tdm_rule_ids = set(tdm_rule_ids_by_repo_id.get(repo.id, []))

            await self.repository_repo.update(
                repo_uuid,
                name=repo.name,
                last_synced_rule_date=max(rule.updated for rule in rules) if rules else repo.last_synced_rule_date,
            )

            deleted_rule_ids = current_rule_ids - all_tdm_rule_ids
            new_rules = [rule for rule in rules if rule.id not in current_rule_ids]
            updated_rules = [rule for rule in rules if rule.id in current_rule_ids]

            await self.rule_repo.delete_many([UUID(rule_id) for rule_id in deleted_rule_ids])
            await self._upsert_rules(repo.id, new_rules)
            await self._upsert_rules(repo.id, updated_rules)

            await self._update_rules_in_pipelines_and_kafka(
                repo_id=repo_uuid,
                new_rules=new_rules,
                deleted_rule_ids=[str(rule_id) for rule_id in deleted_rule_ids],
                updated_rules=updated_rules,
            )

    async def _update_rules_in_pipelines_and_kafka(
        self,
        repo_id: UUID,
        new_rules: list[TdmRule | GithubRule],
        deleted_rule_ids: list[str] | list[UUID],
        updated_rules: list[TdmRule | GithubRule],
    ) -> None:
        """Update rules in pipelines and sync to Kafka.

        Args:
            repo_id: Repository UUID
            new_rules: List of new rules to add
            deleted_rule_ids: List of rule IDs (as strings or UUIDs) to delete
            updated_rules: List of updated rules to sync
        """
        pipeline_ids = await self.pipeline_rule_repo.get_pipelines_by_repository_id(repo_id)

        # Convert deleted_rule_ids to strings for Kafka
        deleted_rule_ids_str = [str(rule_id) for rule_id in deleted_rule_ids]

        for pipeline_id in pipeline_ids:
            await self.kafka.delete_rules(str(pipeline_id), deleted_rule_ids_str)

            await self._add_rules_to_pipelines(pipeline_ids=[pipeline_id], rule_ids=[rule.id for rule in new_rules])
            await self.kafka.send_rules(str(pipeline_id), new_rules)

            enabled_rules_ids = await self.pipeline_rule_repo.get_enabled_rules_ids(pipeline_id)
            enabled_rules_ids = {str(rule_id) for rule_id in enabled_rules_ids}
            updated_enabled_rules = [rule for rule in updated_rules if rule.id in enabled_rules_ids]
            await self.kafka.send_rules(str(pipeline_id), updated_enabled_rules)

    async def _fetch_tdm_rule_ids(self, tdm_repos: list[TdmRepository]) -> dict[str, list[str]]:
        semaphore = asyncio.Semaphore(5)
        tdm_api_client = TDMAPIClient(api_key=await self.config_repo.get_api_key())

        async def _fetch_tdm_rule_ids(repo_id: str) -> list[str]:
            async with semaphore:
                return await tdm_api_client.get_rule_ids(repo_id)

        fetch_tasks = [_fetch_tdm_rule_ids(repo.id) for repo in tdm_repos]
        fetch_results = await asyncio.gather(*fetch_tasks)
        repo_rules_pairs = zip(tdm_repos, fetch_results, strict=True)
        return {repo.id: rules for repo, rules in repo_rules_pairs}

    async def _fetch_rules_from_tdm(
        self, tdm_repos: list[TdmRepository], fetch_all: bool
    ) -> list[tuple[TdmRepository, list[TdmRule]]]:
        semaphore = asyncio.Semaphore(5)

        tdm_api_client = TDMAPIClient(api_key=await self.config_repo.get_api_key())

        async def _fetch_rules(repo: TdmRepository, fetch_all: bool) -> tuple[TdmRepository, list[TdmRule]]:
            async with semaphore:
                rules = await tdm_api_client.get_rules(
                    repository_id=repo.id,
                    updated_after=None if fetch_all else repo.last_synced_rule_date,
                )
                return repo, rules

        fetch_tasks = [_fetch_rules(repo=repo, fetch_all=fetch_all) for repo in tdm_repos]
        return await asyncio.gather(*fetch_tasks)

    async def _upsert_rules(self, repo_id: str, rules: list[TdmRule]) -> None:
        rules_data = [
            {
                "id": rule.id,
                "name": rule.name,
                "body": rule.body,
                "created": rule.created,
                "updated": rule.updated,
                "repository_id": repo_id,
            }
            for rule in rules
        ]
        await self.rule_repo.upsert_many(rules_data)
