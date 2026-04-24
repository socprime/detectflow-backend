import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

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
from apps.managers.sigma_validation_service import SigmaValidationService
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
        search_fields: list[str] | None = None,
    ) -> tuple[list[Rule], int]:
        """Get paginated list of rules with optional repository and search field filters.

        Args:
            pagination: Dict with keys: skip, limit, search, sort, order (from pagination dependency).
            repository_id: If set, only rules from this repository are returned.
            search_fields: Rule fields to apply search to. Allowed: name, product, service, category.
                If None or empty, defaults to ["name", "product", "service", "category"].

        Returns:
            Tuple of (list of Rule models, total count).
        """
        filters = {}
        if repository_id:
            filters["repository_id"] = repository_id

        return await self.rule_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            filters=filters if filters else None,
            search=pagination["search"],
            search_fields=search_fields or ["name", "product", "service", "category"],
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

        # Validate rule
        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rule(rule, force=True)

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)

        await self._add_rules_to_pipelines(pipeline_ids=[pipeline.id for pipeline in pipelines], rule_ids=[rule.id])

        # Only send to Kafka if rule is supported AND pipeline is enabled (BUG #2 fix)
        for pipeline in pipelines:
            if pipeline.enabled and rule.is_supported:
                await self.kafka.send_rules(str(pipeline.id), [rule])

        await activity_producer.log_action(
            action="create",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule.name,
            user=user,
            details=f"Created rule {rule.name}",
        )

        # Refresh rule to load all attributes after flush operations (avoids MissingGreenlet)
        refreshed_rule = await self.rule_repo.get_by_id(rule.id)
        return refreshed_rule if refreshed_rule else rule

    async def create_local_rules_bulk(
        self,
        rules_data: list[dict],
        repository_id: UUID,
        user: User,
    ) -> list[Rule]:
        """
        Create multiple local rules and sync to connected pipelines.

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

        rows = [{**d, "id": str(uuid4()), "repository_id": repository_id} for d in rules_data]
        await self.rule_repo.create_many(rows)

        # Re-fetch to get fully loaded instances (create_many returns expired objects after flush)
        created = await self.rule_repo.get_by_ids([UUID(r["id"]) for r in rows])

        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rules_batch(created, force=True)

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)
        await self._add_rules_to_pipelines(
            pipeline_ids=[p.id for p in pipelines],
            rule_ids=[r.id for r in created],
        )

        for pipeline in pipelines:
            if pipeline.enabled:
                to_send = [r for r in created if r.is_supported]
                if to_send:
                    await self.kafka.send_rules(str(pipeline.id), to_send)

        supported_count = sum(1 for r in created if r.is_supported)
        await activity_producer.log_action(
            action="create",
            entity_type="rule",
            entity_id=None,
            entity_name=None,
            user=user,
            details=f"Bulk created {len(created)} rules ({supported_count} supported) in repository {repository.name}",
        )

        # Intentionally skip per-rule refresh via get_by_id (unlike create_local_rule)
        # because the response schema (RuleBulkCreatedItem) only uses column attributes
        # (id, name, is_supported) — no lazy-loaded relationships are accessed.
        return created

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

        # Re-validate if body was changed
        if "body" in update_data:
            validation_service = SigmaValidationService(self.db)
            await validation_service.validate_rule(rule, force=True)

        # Sync to Kafka for pipelines where rule is enabled (BUG #1 fix)
        pipeline_rules = await self.pipeline_rule_repo.get_by_rule_id(rule_id)
        for pr in pipeline_rules:
            if pr.enabled:
                if rule.is_supported:
                    # Send supported rule to Kafka
                    await self.kafka.send_rules(str(pr.pipeline_id), [rule])
                else:
                    # Delete unsupported rule from Kafka (rule became unsupported after body update)
                    await self.kafka.delete_rules(str(pr.pipeline_id), [str(rule.id)])

        await activity_producer.log_action(
            action="update",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule.name,
            user=user,
            details=f"Updated rule {rule.name}",
        )

        # Refresh rule to load all attributes after flush operations (avoids MissingGreenlet)
        refreshed_rule = await self.rule_repo.get_by_id(rule.id)
        return refreshed_rule if refreshed_rule else rule

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

        # Only send supported rules to Kafka (ISSUE #4 fix)
        supported_rules = [rule for rule in rules if rule.is_supported]
        if supported_rules:
            await self.kafka.send_rules(str(pipeline_id), supported_rules)

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

    async def enable_rule_in_pipeline(self, pipeline_id: UUID, rule_id: UUID, user: User) -> None:
        logger.info(f"Enabling rule {rule_id} in pipeline {pipeline_id}")
        rows_updated = await self.pipeline_rule_repo.update_rule_status(pipeline_id, rule_id, True)
        if rows_updated == 0:
            raise NotFoundError(f"Pipeline {pipeline_id} and/or rule {rule_id} not found")
        rule = await self.rule_repo.get_by_id(rule_id)
        if rule:
            # Only send to Kafka if rule is supported
            if rule.is_supported:
                logger.info(f"Sending rule {rule_id} to Kafka for pipeline {pipeline_id}")
                await self.kafka.send_rules(str(pipeline_id), [rule])
            else:
                logger.info(f"Rule {rule_id} is unsupported, not sending to Kafka")

            await activity_producer.log_action(
                action="toggle",
                entity_type="pipeline_rule",
                entity_id=str(rule_id),
                entity_name=rule.name,
                user=user,
                details=f"Enabled rule '{rule.name}' in pipeline",
                changes={"pipeline_id": str(pipeline_id), "enabled": {"old": False, "new": True}},
            )
        else:
            logger.warning(f"Rule {rule_id} not found in database, cannot send to Kafka")

    async def disable_rule_in_pipeline(self, pipeline_id: UUID, rule_id: UUID, user: User) -> None:
        rule = await self.rule_repo.get_by_id(rule_id)
        rule_name = rule.name if rule else str(rule_id)

        rows_updated = await self.pipeline_rule_repo.update_rule_status(pipeline_id, rule_id, False)
        if rows_updated == 0:
            raise NotFoundError(f"Pipeline {pipeline_id} and/or rule {rule_id} not found")
        await self.kafka.delete_rules(str(pipeline_id), [str(rule_id)])

        await activity_producer.log_action(
            action="toggle",
            entity_type="pipeline_rule",
            entity_id=str(rule_id),
            entity_name=rule_name,
            user=user,
            details=f"Disabled rule '{rule_name}' in pipeline",
            changes={"pipeline_id": str(pipeline_id), "enabled": {"old": True, "new": False}},
        )

    async def handle_pipeline_enable(self, pipeline_id: UUID, needs_revalidation: bool = False) -> None:
        """
        Handle Pipeline Enable event: Sync all enabled rules to Kafka.

        This is called when a pipeline is enabled/resumed. It:
        1. If needs_revalidation=True: Re-validates all rules for the pipeline
        2. Syncs enabled + supported rules to Kafka
        3. Deletes disabled/unsupported rules from Kafka

        Args:
            pipeline_id: UUID of the pipeline
            needs_revalidation: If True, re-validate all rules (when module version changed)
        """
        rules, _ = await self.pipeline_rule_repo.get_by_pipeline(pipeline_id, limit=100000)

        # Re-validate all rules only if version changed
        if needs_revalidation:
            all_rules = [pr.rule for pr in rules if pr.rule]
            if all_rules:
                validation_service = SigmaValidationService(self.db)
                await validation_service.validate_rules_batch(all_rules, force=True)
                logger.info(
                    f"Re-validated {len(all_rules)} rules for pipeline {pipeline_id} due to module version change"
                )

        # Separate enabled and disabled rules, filter by is_supported
        enabled_rules: list[Rule] = [pr.rule for pr in rules if pr.enabled and pr.rule and pr.rule.is_supported]
        disabled_rules: list[Rule] = [pr.rule for pr in rules if not pr.enabled and pr.rule]
        # Also add unsupported rules to "delete" list (don't send to Flink)
        unsupported_rules: list[Rule] = [pr.rule for pr in rules if pr.enabled and pr.rule and not pr.rule.is_supported]

        if enabled_rules:
            await self.kafka.send_rules(str(pipeline_id), enabled_rules)
        if disabled_rules or unsupported_rules:
            rules_to_delete = disabled_rules + unsupported_rules
            await self.kafka.delete_rules(str(pipeline_id), [str(rule.id) for rule in rules_to_delete])

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

    async def sync_api_repos(self) -> bool:
        """Sync API repositories from SOCPrime TDM (only those with sync_enabled=True).

        Returns:
            True if there were repos to update, False if none.

        # TODO: process kafka updates in the end to avoid inconsistency between database and kafka
        # Get all API repositories, but only sync those with sync_enabled=True
        all_current_repos, _ = await self.repository_repo.get_all(filters={"type": "api"}, limit=1000)
        # Filter to only repos with sync_enabled=True
        current_repos = [repo for repo in all_current_repos if repo.sync_enabled is True]

        Logs sync_start at beginning, sync on success with stats, sync_failed on error.
        """
        await activity_producer.log_action(
            action="sync_start",
            entity_type="repository",
            details="Starting TDM API repositories sync",
            source="system",
        )

        try:
            # TODO: process kafka updates in the end to avoid inconsistency between database and kafka
            # Get all API repositories, but only sync those with sync_enabled=True
            all_current_repos, _ = await self.repository_repo.get_all(filters={"type": "api"}, limit=1000)
            # Filter to only repos with sync_enabled=True
            current_repos = [repo for repo in all_current_repos if repo.sync_enabled is True]
            # If no repos to sync, return False
            if not current_repos:
                return False

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

            await activity_producer.log_action(
                action="sync",
                entity_type="repository",
                details=f"TDM API sync completed: {len(repos_to_update)} repos synced, {len(deleted_repos)} repos deleted",
                source="system",
            )
        except Exception as e:
            await activity_producer.log_action(
                action="sync_failed",
                entity_type="repository",
                details=f"TDM API sync failed: {str(e)}",
                source="system",
                severity="error",
            )
            raise

        return bool(repos_to_update)

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

        Note: Rules are fetched from DB after validation to check is_supported status.
        """
        pipeline_ids = await self.pipeline_rule_repo.get_pipelines_by_repository_id(repo_id)

        # Convert deleted_rule_ids to strings for Kafka
        deleted_rule_ids_str = [str(rule_id) for rule_id in deleted_rule_ids]

        # Fetch DB rules to check is_supported status
        all_rule_ids = [UUID(rule.id) for rule in new_rules] + [UUID(rule.id) for rule in updated_rules]
        db_rules_list = await self.rule_repo.get_by_ids(all_rule_ids) if all_rule_ids else []
        db_rules_map = {str(rule.id): rule for rule in db_rules_list}

        for pipeline_id in pipeline_ids:
            await self.kafka.delete_rules(str(pipeline_id), deleted_rule_ids_str)

            # Add new rules to pipeline_rules table
            await self._add_rules_to_pipelines(pipeline_ids=[pipeline_id], rule_ids=[rule.id for rule in new_rules])

            # Only send supported new rules to Kafka
            supported_new_rules = [
                db_rules_map[rule.id]
                for rule in new_rules
                if rule.id in db_rules_map and db_rules_map[rule.id].is_supported
            ]
            if supported_new_rules:
                await self.kafka.send_rules(str(pipeline_id), supported_new_rules)

            # Only send supported enabled updated rules to Kafka
            enabled_rules_ids = await self.pipeline_rule_repo.get_enabled_rules_ids(pipeline_id)
            enabled_rules_ids = {str(rule_id) for rule_id in enabled_rules_ids}
            supported_updated_rules = [
                db_rules_map[rule.id]
                for rule in updated_rules
                if rule.id in enabled_rules_ids and rule.id in db_rules_map and db_rules_map[rule.id].is_supported
            ]
            if supported_updated_rules:
                await self.kafka.send_rules(str(pipeline_id), supported_updated_rules)

            # Delete unsupported rules from Kafka (they might have become unsupported after update)
            unsupported_rule_ids = [
                rule.id
                for rule in updated_rules
                if rule.id in enabled_rules_ids and rule.id in db_rules_map and not db_rules_map[rule.id].is_supported
            ]
            if unsupported_rule_ids:
                await self.kafka.delete_rules(str(pipeline_id), unsupported_rule_ids)

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
        """Upsert rules from TDM API and validate them."""
        if not rules:
            return

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

        # Validate upserted rules
        rule_ids = [UUID(rule.id) for rule in rules]
        db_rules = await self.rule_repo.get_by_ids(rule_ids)
        if db_rules:
            validation_service = SigmaValidationService(self.db)
            await validation_service.validate_rules_batch(db_rules, force=True)
