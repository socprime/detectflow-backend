import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.github import GithubRule
from apps.clients.gitlab_api import GitLabApiClient, GitLabConflictError, GitlabRule
from apps.core.enums import RuleSearchField
from apps.core.exceptions import (
    BadRequestError,
    ConfigurationError,
    ConflictError,
    NotFoundError,
    RepositoryNotLocalError,
)
from apps.core.logger import get_logger
from apps.core.models import GITLAB_REPOSITORY_TYPE, Repository, Rule, RuleContent, User
from apps.core.settings import settings
from apps.integrations.providers.socprime_tdm import (
    SocPrimeTdmProvider,
    TdmApiBadRequestError,
    TdmApiNotFoundError,
    TdmRepository,
    TdmRule,
)
from apps.managers.high_order_sigma_rule_checks import HighOrderSigmaRuleCheckService
from apps.managers.sigma_validation_service import SigmaValidationService
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.rules import KafkaRulesSyncService
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.gitlab import (
    GitlabIntegrationLockBusyError,
    GitlabRepositoryDAO,
    GitlabRuleMetadataDAO,
    try_acquire_gitlab_integration_lock_with_retry,
)
from apps.modules.postgre.integration import IntegrationDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO
from apps.modules.postgre.repository import RepositoryDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.postgre.rule_content import CHANGE_SOURCE_MANUAL, CHANGE_SOURCE_SYSTEM, RuleContentDAO

logger = get_logger(__name__)

# Fields a plain text search covers when the caller does not pick one. RuleSearchField.SIGMA_UUID
# is deliberately absent: it backs the explicit "By ID" search type, not generic text search.
_DEFAULT_SEARCH_FIELDS = [
    RuleSearchField.NAME.value,
    RuleSearchField.PRODUCT.value,
    RuleSearchField.SERVICE.value,
    RuleSearchField.CATEGORY.value,
]


# Legacy aliases for backward compatibility with existing imports
RepositoryNotFoundError = NotFoundError
RuleNotFoundError = NotFoundError


class RulesOrchestrator:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.kafka = KafkaRulesSyncService()
        self.rule_repo = RuleDAO(db)
        self.rule_content_repo = RuleContentDAO(db)
        self.pipeline_rule_repo = PipelineRulesDAO(db)
        self.pipeline_repo = PipelineDAO(db)
        self.config_repo = ConfigDAO(db)
        self.repository_repo = RepositoryDAO(db)
        self.rule_check_service = HighOrderSigmaRuleCheckService(db)

    # ==========================================
    # RULE OPERATIONS
    # ==========================================

    async def get_all(
        self,
        pagination: dict,
        repository_id: UUID | None = None,
        folder_path: str | None = None,
        search_fields: list[RuleSearchField] | None = None,
    ) -> tuple[list[Rule], int]:
        """Get paginated list of rules with optional repository and search field filters.

        Args:
            pagination: Dict with keys: skip, limit, search, sort, order (from pagination dependency).
            repository_id: If set, only rules from this repository are returned.
            folder_path: If set, only rules from this virtual folder are returned.
            search_fields: Rule fields to apply search to (see RuleSearchField). If None or empty,
                the text fields name/product/service/category are used; `sigma_uuid` ("By ID"
                search) has to be requested explicitly and matches a whole UUID or a hex fragment.

        Returns:
            Tuple of (list of Rule models, total count).
        """
        filters = {}
        if repository_id:
            filters["repository_id"] = repository_id
        if folder_path:
            filters["folder_path"] = folder_path

        return await self.rule_repo.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            filters=filters if filters else None,
            search=pagination["search"],
            search_fields=[str(field) for field in search_fields] if search_fields else _DEFAULT_SEARCH_FIELDS,
            sort=pagination["sort"],
            order=pagination["order"],
        )

    async def get_by_id(self, rule_id: UUID, include_versions: bool = False) -> tuple[Rule, list[RuleContent] | None]:
        """Get rule by ID, optionally with version history.

        Returns:
            Tuple of (rule, versions). versions is None if include_versions is False.

        Raises:
            RuleNotFoundError: If rule not found
        """
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        versions = None
        if include_versions:
            versions = await self.rule_content_repo.get_versions_metadata(rule_id)
        return rule, versions

    @staticmethod
    def _rule_matches_folders(rule: Rule, folder_paths: list[str]) -> bool:
        """Return true when a rule belongs to a selected folder subtree."""
        if not folder_paths:
            return True
        rule_folder = rule.folder_path or ""
        return any(rule_folder == folder or rule_folder.startswith(f"{folder}/") for folder in folder_paths)

    def _filter_rules_by_folders(self, rules: list[Rule], folder_paths: list[str]) -> list[Rule]:
        return [rule for rule in rules if self._rule_matches_folders(rule, folder_paths)]

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
        if repository.is_system:
            raise RepositoryNotLocalError("Rules in system repositories are managed by the system.")
        if repository.type == GITLAB_REPOSITORY_TYPE:
            raise RepositoryNotLocalError("Rules in GitLab repositories are created by GitLab sync.")
        if repository.type != "local":
            raise RepositoryNotLocalError(
                f"Rules can only be created in local repositories. Repository type is '{repository.type}'"
            )

        rule = await self.rule_repo.create(
            repository_id=repository_id,
            change_source=CHANGE_SOURCE_MANUAL,
            changed_by=user.full_name,
            **rule_data,
        )

        # Validate rule
        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rule(rule, force=True)

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)
        pipeline_map = {pipeline.id: pipeline for pipeline in pipelines}
        selections_by_pipeline = await self.pipeline_repo.get_repository_selections_by_repository_id(repository_id)
        matching_pipeline_ids = [
            pipeline_id
            for pipeline_id, folder_paths in selections_by_pipeline.items()
            if self._rule_matches_folders(rule, folder_paths)
        ]

        await self._add_rules_to_pipelines(pipeline_ids=matching_pipeline_ids, rule_ids=[rule.id])

        # Only send to Kafka if rule is supported AND pipeline is enabled (BUG #2 fix)
        for pipeline_id in matching_pipeline_ids:
            pipeline = pipeline_map.get(pipeline_id)
            if pipeline and pipeline.enabled and rule.is_supported:
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
        if repository.is_system:
            raise RepositoryNotLocalError("Rules in system repositories are managed by the system.")
        if repository.type == GITLAB_REPOSITORY_TYPE:
            raise RepositoryNotLocalError("Rules in GitLab repositories are created by GitLab sync.")
        if repository.type != "local":
            raise RepositoryNotLocalError(
                f"Rules can only be created in local repositories. Repository type is '{repository.type}'"
            )

        rows = [{**d, "id": str(uuid4()), "repository_id": repository_id} for d in rules_data]
        await self.rule_repo.create_many(rows, change_source=CHANGE_SOURCE_MANUAL, changed_by=user.full_name)

        # Re-fetch to get fully loaded instances (create_many returns expired objects after flush)
        created = await self.rule_repo.get_by_ids([UUID(r["id"]) for r in rows])

        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rules_batch(created, force=True)

        pipelines = await self.pipeline_repo.get_by_repository_id(repository_id)
        pipeline_map = {pipeline.id: pipeline for pipeline in pipelines}
        selections_by_pipeline = await self.pipeline_repo.get_repository_selections_by_repository_id(repository_id)

        rules_by_pipeline = {
            pipeline_id: self._filter_rules_by_folders(created, folder_paths)
            for pipeline_id, folder_paths in selections_by_pipeline.items()
        }
        for pipeline_id, matching_rules in rules_by_pipeline.items():
            await self._add_rules_to_pipelines(
                pipeline_ids=[pipeline_id],
                rule_ids=[rule.id for rule in matching_rules],
            )

        for pipeline_id, matching_rules in rules_by_pipeline.items():
            pipeline = pipeline_map.get(pipeline_id)
            if pipeline and pipeline.enabled:
                to_send = [r for r in matching_rules if r.is_supported]
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
        if rule_ids:
            await self.rule_check_service.refresh_for_rule_ids([UUID(str(rule_id)) for rule_id in rule_ids])

    async def delete_rule(
        self,
        rule_id: UUID,
        user: User,
        *,
        gitlab_push: bool = False,
        gitlab_force: bool = False,
    ) -> None:
        """Delete a rule.

        GitLab rules accept ``gitlab_push`` / ``gitlab_force`` the same way updates do:
        default is local-only; opt in to write GitLab first (delete the remote file).
        """
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        if rule.repository.type == GITLAB_REPOSITORY_TYPE:
            await self._delete_gitlab_rule(
                rule,
                user,
                gitlab_push=gitlab_push,
                gitlab_force=gitlab_force,
            )
            return
        await self.delete_local_rule(rule_id, user)

    async def delete_local_rule(self, rule_id: UUID, user: User) -> None:
        """
        Delete a local rule and remove it from Kafka for connected pipelines.
        """
        # validate that the rule is local
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        if rule.repository.is_system:
            raise RepositoryNotLocalError("This rule is managed by the system.")
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

    async def update_rule(
        self,
        rule_id: UUID,
        update_data: dict,
        user: User,
        *,
        gitlab_push: bool = False,
        gitlab_force: bool = False,
    ) -> Rule:
        """
        Update a rule and resync to Kafka for pipelines where it is enabled.

        For API rules, also updates the rule in the remote API.
        External repository rules cannot be updated (read-only).

        Args:
            rule_id: UUID of the rule to update.
            update_data: Dictionary with rule fields to update (name, body, etc.).
            user: Current user for activity logging.
            gitlab_push: For GitLab rules only — push the update to GitLab before saving locally.
            gitlab_force: For GitLab rules only — overwrite GitLab on optimistic-lock conflict when gitlab_push is true.

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

        new_gitlab_commit_id: str | None = None
        gitlab_integration_id: UUID | None = None

        if existing_rule.repository.type == GITLAB_REPOSITORY_TYPE:
            gitlab_integration_id = await self._gitlab_integration_id(existing_rule.repository_id)
            if gitlab_push:
                new_gitlab_commit_id = await self._push_gitlab_remote(
                    existing_rule, update_data, user, gitlab_force=gitlab_force
                )
        elif existing_rule.repository.type == "external":
            raise RepositoryNotLocalError(
                "Rules in external repositories cannot be updated. They are read-only and managed by external sources."
            )

        # Set default updated timestamp (will be overwritten by API response if API rule)
        update_data["updated"] = datetime.now(tz=UTC)

        # Mark system-repo rules as user-edited so the Active Threat sync stops overwriting them.
        # System rules are kept locally and never propagated back to TDM, so we skip the API call below.
        if existing_rule.repository.is_system:
            update_data["is_user_edited_system_rule"] = True
        # Update in remote API if this is an API rule (non-system api repos only)
        elif existing_rule.repository.type == "api":
            tdm_cfg = await self.config_repo.get_socprime_tdm_provider_config()
            push_allowed = ConfigDAO.is_push_enabled_config(tdm_cfg) and existing_rule.repository.sync_enabled
            if push_allowed:
                tdm = SocPrimeTdmProvider(tdm_cfg)
                try:
                    tdm_rule = await tdm.update_rule(
                        repository_id=str(existing_rule.repository.id),
                        rule_id=str(rule_id),
                        name=update_data.get("name"),
                        body=update_data.get("body"),
                    )
                    update_data["updated"] = tdm_rule.updated
                    update_data["name"] = tdm_rule.name
                    update_data["body"] = tdm_rule.body
                    update_data["has_unpushed_changes"] = False
                except (TdmApiBadRequestError, TdmApiNotFoundError):
                    raise
            else:
                # Push off (globally or for this repo) — save locally without contacting TDM and
                # mark the rule as pending reconciliation on the next sync_api_repos.
                update_data["has_unpushed_changes"] = True

        if gitlab_integration_id is not None:
            if not await try_acquire_gitlab_integration_lock_with_retry(self.db, gitlab_integration_id):
                raise GitlabIntegrationLockBusyError()
            if new_gitlab_commit_id is not None:
                await self._update_gitlab_metadata_commit_id(existing_rule, new_gitlab_commit_id)
            update_data["folder_path"] = existing_rule.folder_path

        rule = await self.rule_repo.update(
            rule_id,
            change_source=CHANGE_SOURCE_MANUAL,
            changed_by=user.full_name,
            **update_data,
        )

        # Re-validate if body was changed
        if "body" in update_data:
            validation_service = SigmaValidationService(self.db)
            await validation_service.validate_rule(rule, force=True)

        # Prune old versions after manual edit
        await self.rule_content_repo.prune_versions(rule_id, keep=settings.rule_version_retention)

        # Sync to Kafka for pipelines where rule is enabled (BUG #1 fix)
        await self._sync_rule_to_enabled_pipelines(rule)
        await self.rule_check_service.refresh_for_rule_ids([rule.id])

        await activity_producer.log_action(
            action="update",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule.name,
            user=user,
            details=f"Updated rule {rule.name}",
        )

        # Re-fetch before returning so ``RuleConverter.to_full_detail`` reads generated
        # columns (``folder_path``) and joined content without lazy-load.
        refreshed_rule = await self.rule_repo.get_by_id(rule.id)
        return refreshed_rule if refreshed_rule else rule

    async def _gitlab_integration_id(self, repository_id: UUID) -> UUID:
        gitlab_link = await GitlabRepositoryDAO(self.db).get_by_repository(repository_id)
        if gitlab_link is None:
            raise ConfigurationError("GitLab repository link not found")
        return gitlab_link.integration_id

    async def _push_gitlab_remote(
        self,
        existing_rule: Rule,
        update_data: dict,
        user: User,
        *,
        gitlab_force: bool,
    ) -> str:
        metadata = await GitlabRuleMetadataDAO(self.db).get_by_rule_id(existing_rule.id)
        if metadata is None:
            raise BadRequestError("GitLab metadata missing for this rule")

        gitlab_link = await GitlabRepositoryDAO(self.db).get_by_repository(existing_rule.repository_id)
        if gitlab_link is None:
            raise ConfigurationError("GitLab repository link not found")

        integration = await IntegrationDAO(self.db).get_by_id(gitlab_link.integration_id)
        if integration is None or not integration.config:
            raise ConfigurationError("GitLab integration is not configured")

        gitlab_rule = GitlabRule(
            id=str(existing_rule.sigma_uuid or existing_rule.id),
            name=update_data.get("name", existing_rule.name),
            body=update_data.get("body", existing_rule.body),
            file_path=metadata.file_path,
            last_commit_id=metadata.last_commit_id,
        )
        client = GitLabApiClient.from_integration_config(
            integration.config,
            author_name=user.full_name,
            author_email=user.email,
        )
        try:
            return await client.push_rule(
                gitlab_rule,
                last_commit_id=metadata.last_commit_id,
                force=gitlab_force,
            )
        except GitLabConflictError as exc:
            raise ConflictError(
                "Rule changed on GitLab since last sync. Pull latest from GitLab or retry with gitlab_force=true."
            ) from exc

    async def _update_gitlab_metadata_commit_id(self, rule: Rule, last_commit_id: str) -> None:
        metadata = await GitlabRuleMetadataDAO(self.db).get_by_rule_id(rule.id)
        if metadata is None:
            raise BadRequestError("GitLab metadata missing for this rule")
        await GitlabRuleMetadataDAO(self.db).upsert_many(
            [
                {
                    "rule_id": rule.id,
                    "repository_id": rule.repository_id,
                    "file_path": metadata.file_path,
                    "last_commit_id": last_commit_id,
                }
            ]
        )

    async def _delete_gitlab_remote(self, rule: Rule, user: User, *, gitlab_force: bool) -> None:
        metadata = await GitlabRuleMetadataDAO(self.db).get_by_rule_id(rule.id)
        if metadata is None:
            raise BadRequestError("GitLab metadata missing for this rule")

        gitlab_link = await GitlabRepositoryDAO(self.db).get_by_repository(rule.repository_id)
        if gitlab_link is None:
            raise ConfigurationError("GitLab repository link not found")

        integration = await IntegrationDAO(self.db).get_by_id(gitlab_link.integration_id)
        if integration is None or not integration.config:
            raise ConfigurationError("GitLab integration is not configured")

        client = GitLabApiClient.from_integration_config(
            integration.config,
            author_name=user.full_name,
            author_email=user.email,
        )
        try:
            await client.delete_rule(metadata.file_path, last_commit_id=metadata.last_commit_id, force=gitlab_force)
        except GitLabConflictError as exc:
            raise ConflictError(
                "Rule changed on GitLab since last sync. Pull latest from GitLab or retry with gitlab_force=true."
            ) from exc

    async def _delete_gitlab_rule(
        self,
        rule: Rule,
        user: User,
        *,
        gitlab_push: bool,
        gitlab_force: bool,
    ) -> None:
        if gitlab_push:
            await self._delete_gitlab_remote(rule, user, gitlab_force=gitlab_force)

        integration_id = await self._gitlab_integration_id(rule.repository_id)
        if not await try_acquire_gitlab_integration_lock_with_retry(self.db, integration_id):
            raise GitlabIntegrationLockBusyError()

        rule_name = rule.name
        pipeline_ids = await self.pipeline_rule_repo.get_pipelines_by_rule_id(rule.id)
        for pipeline_id in pipeline_ids:
            await self.kafka.delete_rules(str(pipeline_id), [str(rule.id)])

        await self.rule_repo.delete(rule.id)

        if gitlab_push:
            details = f"Deleted GitLab rule {rule_name} on GitLab and locally"
        else:
            details = f"Deleted GitLab rule {rule_name} locally (file remains on GitLab until next pull)"
        await activity_producer.log_action(
            action="delete",
            entity_type="rule",
            entity_id=str(rule.id),
            entity_name=rule_name,
            user=user,
            details=details,
        )

    async def _sync_rule_to_enabled_pipelines(self, rule: Rule) -> None:
        """Sync a single rule to Kafka for all pipelines where it is enabled.

        Sends supported rules or deletes unsupported ones.
        """
        pipeline_rules = await self.pipeline_rule_repo.get_by_rule_id(rule.id)
        for pr in pipeline_rules:
            if pr.enabled:
                if rule.is_supported:
                    await self.kafka.send_rules(str(pr.pipeline_id), [rule])
                else:
                    await self.kafka.delete_rules(str(pr.pipeline_id), [str(rule.id)])

    async def get_rule_versions(self, rule_id: UUID) -> list[RuleContent]:
        """Get version history metadata for a rule."""
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        return await self.rule_content_repo.get_versions_metadata(rule_id)

    async def get_rule_version(self, rule_id: UUID, version: int) -> tuple[Rule, RuleContent]:
        """Get a specific version of a rule (with body) and its owning rule (for repo type)."""
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")
        content = await self.rule_content_repo.get_version(rule_id, version)
        if not content:
            raise NotFoundError(f"Version {version} not found for rule {rule_id}")
        return rule, content

    async def rollback_rule(self, rule_id: UUID, target_version: int, user: User) -> Rule:
        """Rollback a rule to a previous version by creating a new version with the old body."""
        rule = await self.rule_repo.get_by_id(rule_id)
        if not rule:
            raise RuleNotFoundError(f"Rule with id {rule_id} not found")

        if rule.repository and rule.repository.type != "local":
            raise RepositoryNotLocalError(
                "Rollback is only allowed for local rules. "
                "Rules in synced repositories are managed by their external source."
            )

        if target_version == rule.version:
            raise BadRequestError(f"Rule is already at version {target_version}")

        target_content = await self.rule_content_repo.get_version(rule_id, target_version)
        if not target_content:
            raise NotFoundError(f"Version {target_version} not found for rule {rule_id}")

        if target_content.content_hash == rule.content_hash:
            raise BadRequestError(f"Version {target_version} has the same content as the current version")

        updated_rule = await self.rule_repo.update(
            rule_id,
            body=target_content.body,
            change_source=CHANGE_SOURCE_MANUAL,
            changed_by=user.full_name,
        )

        # Re-validate after rollback
        validation_service = SigmaValidationService(self.db)
        await validation_service.validate_rule(updated_rule, force=True)

        # Prune after rollback
        await self.rule_content_repo.prune_versions(rule_id, keep=settings.rule_version_retention)

        await self._sync_rule_to_enabled_pipelines(updated_rule)

        await activity_producer.log_action(
            action="rollback",
            entity_type="rule",
            entity_id=str(rule_id),
            entity_name=updated_rule.name,
            user=user,
            details=f"Rolled back rule '{updated_rule.name}' to version {target_version}",
            changes={"version": {"old": rule.version, "new": updated_rule.version}},
        )

        # Re-fetch before returning (see ``update_rule``).
        refreshed_rule = await self.rule_repo.get_by_id(rule_id)
        return refreshed_rule if refreshed_rule else updated_rule

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
        if repository.is_system:
            raise RepositoryNotLocalError("This repository is managed by the system and cannot be deleted.")

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

    async def handle_pipeline_creation(self, pipeline_id: UUID, repository_selections: dict[UUID, list[str]]) -> None:
        """
        Handle the creation of a pipeline. This is called after a pipeline is created.
        It adds repositories to the pipeline and adds rules to the pipeline.
        It also sends rules to Kafka for the pipeline.
        """
        await self._add_repos_to_pipeline(pipeline_id, repository_selections)

    async def _add_repos_to_pipeline(self, pipeline_id: UUID, repository_selections: dict[UUID, list[str]]) -> None:
        if not repository_selections:
            return

        await self.pipeline_repo.add_repositories(pipeline_id, repository_selections)

        rules = await self.rule_repo.get_all_by_repository_selections(repository_selections)
        await self._add_rules_to_pipelines(pipeline_ids=[pipeline_id], rule_ids=[rule.id for rule in rules])

        # Only send supported rules to Kafka (ISSUE #4 fix)
        supported_rules = [rule for rule in rules if rule.is_supported]
        if supported_rules:
            await self.kafka.send_rules(str(pipeline_id), supported_rules)

    async def handle_pipeline_duplication(self, source_pipeline_id: UUID, new_pipeline_id: UUID) -> None:
        """
        Handle the duplication of a pipeline. This is called after the copy row is created.
        It copies the source repository selections (with folder paths) and its pipeline_rules
        links, preserving each link's ``enabled`` flag, then sends the copy's live rules to Kafka.

        Membership is copied from ``pipeline_rules`` instead of being re-derived from the
        repositories (as ``handle_pipeline_creation`` does), so per-rule toggles and the exact rule
        set of the source survive duplication even if the repositories drifted since.

        Rules are published to ``settings.kafka_sigma_rules_topic`` under the new pipeline as
        ``job_id``; keys are ``{pipeline_id}:{rule_id}``, so the copy needs its own message set and
        shares nothing with the source. Only enabled + supported rules are sent, matching what the
        match node would run for this pipeline.
        """
        repository_selections = await self.pipeline_repo.get_repository_selections(source_pipeline_id)
        if repository_selections:
            await self.pipeline_repo.add_repositories(new_pipeline_id, repository_selections)

        source_rules, _ = await self.pipeline_rule_repo.get_by_pipeline(source_pipeline_id, limit=100000)
        if not source_rules:
            return

        await self.pipeline_rule_repo.create_many(
            [{"pipeline_id": new_pipeline_id, "rule_id": pr.rule_id, "enabled": pr.enabled} for pr in source_rules]
        )
        # The HOS check snapshot reads live pipeline_rules.enabled — refresh once the copies exist.
        await self.rule_check_service.refresh_for_rule_ids([pr.rule_id for pr in source_rules if pr.rule_id])

        rules_to_send = [pr.rule for pr in source_rules if pr.enabled and pr.rule and pr.rule.is_supported]
        if rules_to_send:
            await self.kafka.send_rules(str(new_pipeline_id), rules_to_send)

    async def handle_pipeline_deletion(self, pipeline_id: UUID) -> None:
        """
        Handle the deletion of a pipeline. This is called BEFORE a pipeline is deleted.
        It removes all repositories from the pipeline and deletes all rules from the pipeline.
        Ir also deletes all rules from Kafka for the pipeline.
        """
        # TODO: consider to remove deletion of pipeline_rules and pipeline_repositories as it is handled by cascade
        rules = await self.pipeline_rule_repo.get_rules_by_pipeline(pipeline_id)
        await self.pipeline_rule_repo.delete_by_pipeline(pipeline_id)
        await self.rule_check_service.refresh_for_rule_ids([rule.id for rule in rules])
        await self.pipeline_repo.remove_all_repositories(pipeline_id)
        if rules:
            await self.kafka.delete_rules(str(pipeline_id), [str(r.id) for r in rules])

    async def enable_rule_in_pipeline(self, pipeline_id: UUID, rule_id: UUID, user: User) -> None:
        logger.info(f"Enabling rule {rule_id} in pipeline {pipeline_id}")
        rows_updated = await self.pipeline_rule_repo.update_rule_status(pipeline_id, rule_id, True)
        if rows_updated == 0:
            raise NotFoundError(f"Pipeline {pipeline_id} and/or rule {rule_id} not found")
        await self.rule_check_service.refresh_for_rule_ids([rule_id])
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
        await self.rule_check_service.refresh_for_rule_ids([rule_id])
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
                await self.rule_check_service.refresh_for_rule_ids([rule.id for rule in all_rules])

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
        repository_selections: dict[UUID, list[str]],
    ) -> None:
        """
        Handle changes in pipeline. This is called after a pipeline is updated.

        This function is called after a pipeline is updated. It handles the following:
        - Add new rules to pipeline
        - Remove rules from pipeline

        Args:
            pipeline_id: UUID of the pipeline.
            repository_selections: Repository IDs and selected folders to keep on the pipeline.
        """
        current_pipeline_rules, _ = await self.pipeline_rule_repo.get_by_pipeline(pipeline_id, limit=100000)
        current_rule_ids = {pipeline_rule.rule_id for pipeline_rule in current_pipeline_rules}

        selected_rules = await self.rule_repo.get_all_by_repository_selections(repository_selections)
        selected_rule_ids = {rule.id for rule in selected_rules}

        removed_rule_ids = list(current_rule_ids - selected_rule_ids)
        added_rules = [rule for rule in selected_rules if rule.id not in current_rule_ids]
        added_rule_ids = [rule.id for rule in added_rules]

        await self.pipeline_repo.set_repository_selections(pipeline_id, repository_selections)

        if removed_rule_ids:
            await self.pipeline_rule_repo.delete_by_pipeline_and_rule_ids(pipeline_id, removed_rule_ids)
            await self.kafka.delete_rules(str(pipeline_id), [str(rule_id) for rule_id in removed_rule_ids])

        if added_rules:
            await self._add_rules_to_pipelines(pipeline_ids=[pipeline_id], rule_ids=added_rule_ids)
            supported_rules = [rule for rule in added_rules if rule.is_supported]
            if supported_rules:
                await self.kafka.send_rules(str(pipeline_id), supported_rules)

        if removed_rule_ids:
            await self.rule_check_service.refresh_for_rule_ids(removed_rule_ids)

    # ==========================================
    # REPOSITORY SYNC OPERATIONS
    # ==========================================

    async def sync_api_repos(self) -> bool:
        """Discover and sync all repositories available from SOCPrime TDM.

        Missing local repositories are created automatically with sync enabled. Existing
        repositories are pulled only when ``sync_enabled=True``; that same filter deliberately
        protects sync-disabled repositories from deletion when they disappear from TDM.

        Returns:
            True if any repository was created, updated, or deleted; False otherwise.

        Note:
            TODO: batch Kafka updates at end of sync to reduce DB/Kafka inconsistency windows.
        """
        await activity_producer.log_action(
            action="sync_start",
            entity_type="repository",
            details="Starting TDM API repositories sync",
            source="system",
        )

        new_repos: list[TdmRepository] = []
        repos_to_update: list[TdmRepository] = []
        deleted_repo_ids: set[str] = set()
        try:
            # TODO: process kafka updates in the end to avoid inconsistency between database and kafka
            all_current_repos, _ = await self.repository_repo.get_all(filters={"type": "api"}, limit=1000)
            current_repos = [repo for repo in all_current_repos if not repo.is_system]
            sync_enabled_repos = [repo for repo in current_repos if repo.sync_enabled is True]

            tdm_cfg = await self.config_repo.get_socprime_tdm_provider_config()
            if not tdm_cfg:
                await activity_producer.log_action(
                    action="sync_failed",
                    entity_type="repository",
                    details="TDM API sync skipped: SOCPrime TDM integration is not configured or API key is missing",
                    source="system",
                    severity="error",
                )
                return False

            tdm = SocPrimeTdmProvider(tdm_cfg)
            tdm_repos = await tdm.get_repositories()

            known_repo_ids = {str(repo.id) for repo in all_current_repos}
            sync_enabled_repo_ids = {str(repo.id) for repo in sync_enabled_repos}
            tdm_repo_ids = {repo.id for repo in tdm_repos}

            new_repos = [repo for repo in tdm_repos if repo.id not in known_repo_ids]
            await self._create_new_repos(tdm, new_repos)

            # Only sync-enabled repositories participate in remote deletion. Disabled repositories
            # remain local even when TDM no longer lists them.
            deleted_repo_ids = sync_enabled_repo_ids - tdm_repo_ids
            for repo_id in deleted_repo_ids:
                await self._delete_repository(UUID(repo_id))

            repos_to_update = [repo for repo in tdm_repos if repo.id in sync_enabled_repo_ids]
            self._enrich_tdm_repos_with_last_synced_rule_date(repos_to_update, sync_enabled_repos)
            await self._update_api_repos(tdm, repos_to_update)

            await activity_producer.log_action(
                action="sync",
                entity_type="repository",
                details=(
                    f"TDM API sync completed: {len(new_repos)} repos added, "
                    f"{len(repos_to_update)} repos synced, {len(deleted_repo_ids)} repos deleted"
                ),
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

        return bool(new_repos or repos_to_update or deleted_repo_ids)

    def _enrich_tdm_repos_with_last_synced_rule_date(
        self, tdm_repos: list[TdmRepository], current_repos: list[Repository]
    ) -> None:
        current_repos_dict = {str(repo.id): repo for repo in current_repos}
        for repo in tdm_repos:
            if repo.id in current_repos_dict:
                repo.last_synced_rule_date = current_repos_dict[repo.id].last_synced_rule_date

    async def _create_new_repos(self, tdm: SocPrimeTdmProvider, tdm_repos: list[TdmRepository]) -> None:
        repo_rules_pairs = await self._fetch_rules_from_tdm(tdm, tdm_repos, fetch_all=True)
        for repo, rules in repo_rules_pairs:
            await self.repository_repo.create(
                id=UUID(repo.id),
                name=repo.name,
                type="api",
                sync_enabled=True,
                last_synced_rule_date=max(rule.updated for rule in rules) if rules else None,
                source_link=(
                    f"https://{settings.tdm_hostname}/expert/?repositoryIds%5B%5D={repo.id}&repositoryType=custom"
                ),
            )
            await self._upsert_rules(repo.id, rules)

    async def _update_api_repos(self, tdm: SocPrimeTdmProvider, tdm_repos: list[TdmRepository]) -> None:
        push_enabled = await self.config_repo.is_tdm_push_enabled()
        push_reenable_strategy = await self.config_repo.get_tdm_push_reenable_strategy()

        repo_rules_pairs = await self._fetch_rules_from_tdm(tdm, tdm_repos, fetch_all=False)
        tdm_rule_ids_by_repo_id = await self._fetch_tdm_rule_ids(tdm, tdm_repos)
        for repo, rules in repo_rules_pairs:
            repo_uuid = UUID(repo.id)
            current_rules = await self.rule_repo.get_all_by_repository([repo_uuid])
            current_rule_ids = {str(rule.id) for rule in current_rules}
            pending_rules_by_id = {str(rule.id): rule for rule in current_rules if rule.has_unpushed_changes}
            all_tdm_rule_ids = set(tdm_rule_ids_by_repo_id.get(repo.id, []))

            # Rules with a pending local edit (has_unpushed_changes=True) are protected from
            # remote delete and blind overwrite; they are reconciled below instead.
            deleted_rule_ids = current_rule_ids - all_tdm_rule_ids - set(pending_rules_by_id)
            new_rules = [rule for rule in rules if rule.id not in current_rule_ids]
            updated_rules = [
                rule for rule in rules if rule.id in current_rule_ids and rule.id not in pending_rules_by_id
            ]

            await self.rule_repo.delete_many([UUID(rule_id) for rule_id in deleted_rule_ids])
            await self._upsert_rules(repo.id, new_rules + updated_rules, change_source=CHANGE_SOURCE_SYSTEM)

            reconciled_rules = await self._reconcile_unpushed_rules(
                tdm=tdm,
                repository_id=repo.id,
                pending_rules_by_id=pending_rules_by_id,
                remote_rules=rules,
                push_enabled=push_enabled,
                push_reenable_strategy=push_reenable_strategy,
            )

            # Advance the sync cursor past everything synced this run, including reconcile-pushed
            # rules whose new ``updated`` (TDM push response) is not part of the incremental ``rules``.
            synced_dates = [rule.updated for rule in rules] + [rule.updated for rule in reconciled_rules]
            await self.repository_repo.update(
                repo_uuid,
                name=repo.name,
                last_synced_rule_date=max(synced_dates) if synced_dates else repo.last_synced_rule_date,
            )

            await self._update_rules_in_pipelines_and_kafka(
                repo_id=repo_uuid,
                new_rules=new_rules,
                deleted_rule_ids=[str(rule_id) for rule_id in deleted_rule_ids],
                updated_rules=updated_rules + reconciled_rules,
            )

        # Bulk prune old versions after entire TDM sync
        await self.rule_content_repo.prune_all_versions(keep=settings.rule_version_retention)

    async def _reconcile_unpushed_rules(
        self,
        tdm: SocPrimeTdmProvider,
        repository_id: str,
        pending_rules_by_id: dict[str, Rule],
        remote_rules: list[TdmRule],
        push_enabled: bool,
        push_reenable_strategy: str,
    ) -> list[TdmRule]:
        """Reconcile ``has_unpushed_changes=True`` rules with TDM during ``sync_api_repos``.

        Push OFF leaves every pending rule untouched (still protected from the caller's blind
        delete/overwrite). Push ON compares each pending rule's ``updated`` against the
        incrementally-fetched remote's ``updated`` — absent from ``remote_rules`` means TDM
        hasn't changed it since the pre-sync cursor, i.e. older than the local edit:

        - remote newer-or-tied ⇒ pull remote (rule overwritten, flag cleared).
        - local edit newer ⇒ ``next_sync`` pushes it (PATCH + flag cleared); ``next_save`` leaves
          it pending — pushed only by an explicit re-save through the edit-time path.

        Returns:
            TdmRule entries whose local content changed (remote pulled in, or local pushed and
            replaced by TDM's response) so the caller can resync connected pipelines/Kafka the
            same way as a normal blind-overwrite update.
        """
        if not pending_rules_by_id or not push_enabled:
            return []

        remote_by_id = {rule.id: rule for rule in remote_rules}
        changed_rules: list[TdmRule] = []

        for rule_id, local_rule in pending_rules_by_id.items():
            remote_rule = remote_by_id.get(rule_id)

            if remote_rule is not None and local_rule.updated <= remote_rule.updated:
                await self.rule_repo.update(
                    local_rule.id,
                    name=remote_rule.name,
                    body=remote_rule.body,
                    updated=remote_rule.updated,
                    has_unpushed_changes=False,
                    change_source=CHANGE_SOURCE_SYSTEM,
                    changed_by="system",
                )
                changed_rules.append(remote_rule)
                continue

            # Local edit is newer (or remote hasn't changed since the pre-sync cursor).
            if push_reenable_strategy == "next_save":
                continue  # keep has_unpushed_changes=True; pushed only on next explicit re-save

            # A single rejected push must not abort the whole scheduled sync — leave the rule
            # pending (flag stays True) and let the next sync / explicit re-save retry it.
            try:
                tdm_rule = await tdm.update_rule(
                    repository_id=repository_id,
                    rule_id=rule_id,
                    name=local_rule.name,
                    body=local_rule.body,
                )
            except (TdmApiBadRequestError, TdmApiNotFoundError) as exc:
                logger.warning("TDM push failed while reconciling rule %s; keeping it pending: %s", rule_id, exc)
                continue

            await self.rule_repo.update(
                local_rule.id,
                name=tdm_rule.name,
                body=tdm_rule.body,
                updated=tdm_rule.updated,
                has_unpushed_changes=False,
                change_source=CHANGE_SOURCE_SYSTEM,
                changed_by="system",
            )
            changed_rules.append(tdm_rule)

        if changed_rules:
            db_rules = await self.rule_repo.get_by_ids([UUID(rule.id) for rule in changed_rules])
            if db_rules:
                validation_service = SigmaValidationService(self.db)
                await validation_service.validate_rules_batch(db_rules, force=True)

        return changed_rules

    async def sync_system_repository_changes(
        self,
        repository_id: UUID,
        new_rule_ids: list[UUID],
        updated_rule_ids: list[UUID],
        deleted_rule_ids: list[UUID],
    ) -> None:
        """Propagate system repository rule changes to connected pipelines and Kafka.

        System repositories (e.g. SOC Prime Active Threats) are kept in sync
        by the Active Threat sync flow, which writes rules straight to the DB. This
        method mirrors those DB changes onto every pipeline that has the system repo
        attached: it adds new rules to ``pipeline_rules``, sends new + enabled
        updated rules to Kafka, and emits Kafka deletes for removed rules and any
        updated rules that became unsupported.
        """
        if not (new_rule_ids or updated_rule_ids or deleted_rule_ids):
            return

        all_rule_ids = list(dict.fromkeys([*new_rule_ids, *updated_rule_ids]))
        selections_by_pipeline = await self.pipeline_repo.get_repository_selections_by_repository_id(repository_id)
        if not selections_by_pipeline:
            if all_rule_ids:
                await self.rule_check_service.refresh_for_rule_ids(all_rule_ids)
            return

        db_rules_map: dict[UUID, Rule] = {}
        if all_rule_ids:
            db_rules = await self.rule_repo.get_by_ids(all_rule_ids)
            db_rules_map = {rule.id: rule for rule in db_rules}

        new_rules = [db_rules_map[rid] for rid in new_rule_ids if rid in db_rules_map]
        updated_rules = [db_rules_map[rid] for rid in updated_rule_ids if rid in db_rules_map]
        deleted_rule_ids_str = [str(rid) for rid in deleted_rule_ids]

        for pipeline_id, folder_paths in selections_by_pipeline.items():
            if deleted_rule_ids_str:
                await self.kafka.delete_rules(str(pipeline_id), deleted_rule_ids_str)

            matching_new_rules = self._filter_rules_by_folders(new_rules, folder_paths)
            if matching_new_rules:
                await self._add_rules_to_pipelines(
                    pipeline_ids=[pipeline_id],
                    rule_ids=[rule.id for rule in matching_new_rules],
                )
                supported_new_rules = [rule for rule in matching_new_rules if rule.is_supported]
                if supported_new_rules:
                    await self.kafka.send_rules(str(pipeline_id), supported_new_rules)

            if updated_rules:
                enabled_rule_ids = await self.pipeline_rule_repo.get_enabled_rules_ids(pipeline_id)
                enabled_rule_ids_set = {str(rid) for rid in enabled_rule_ids}
                current_rule_ids = set(await self.pipeline_rule_repo.get_rule_ids(pipeline_id))
                matching_updated_rules = self._filter_rules_by_folders(updated_rules, folder_paths)
                matching_updated_rule_ids = {rule.id for rule in matching_updated_rules}

                missing_matching_updated_rules = [
                    rule for rule in matching_updated_rules if rule.id not in current_rule_ids
                ]
                if missing_matching_updated_rules:
                    await self._add_rules_to_pipelines(
                        pipeline_ids=[pipeline_id],
                        rule_ids=[rule.id for rule in missing_matching_updated_rules],
                    )

                supported_updated_rules = [
                    rule
                    for rule in matching_updated_rules
                    if (str(rule.id) in enabled_rule_ids_set or rule.id not in current_rule_ids) and rule.is_supported
                ]
                if supported_updated_rules:
                    await self.kafka.send_rules(str(pipeline_id), supported_updated_rules)

                non_matching_updated_rule_ids = [
                    rule.id for rule in updated_rules if rule.id not in matching_updated_rule_ids
                ]
                if non_matching_updated_rule_ids:
                    await self.pipeline_rule_repo.delete_by_pipeline_and_rule_ids(
                        pipeline_id,
                        non_matching_updated_rule_ids,
                    )
                    await self.kafka.delete_rules(
                        str(pipeline_id),
                        [str(rule_id) for rule_id in non_matching_updated_rule_ids],
                    )

                unsupported_updated_ids = [
                    str(rule.id)
                    for rule in matching_updated_rules
                    if str(rule.id) in enabled_rule_ids_set and not rule.is_supported
                ]
                if unsupported_updated_ids:
                    await self.kafka.delete_rules(str(pipeline_id), unsupported_updated_ids)

        await self.rule_check_service.refresh_for_rule_ids(all_rule_ids)

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
        selections_by_pipeline = await self.pipeline_repo.get_repository_selections_by_repository_id(repo_id)

        # Convert deleted_rule_ids to strings for Kafka
        deleted_rule_ids_str = [str(rule_id) for rule_id in deleted_rule_ids]

        # Fetch DB rules to check is_supported status
        all_rule_ids = [UUID(rule.id) for rule in new_rules] + [UUID(rule.id) for rule in updated_rules]
        db_rules_list = await self.rule_repo.get_by_ids(all_rule_ids) if all_rule_ids else []
        db_rules_map = {str(rule.id): rule for rule in db_rules_list}

        for pipeline_id, folder_paths in selections_by_pipeline.items():
            await self.kafka.delete_rules(str(pipeline_id), deleted_rule_ids_str)

            # Add new rules to pipeline_rules table
            matching_new_rules = [
                db_rules_map[rule.id]
                for rule in new_rules
                if rule.id in db_rules_map and self._rule_matches_folders(db_rules_map[rule.id], folder_paths)
            ]
            if matching_new_rules:
                await self._add_rules_to_pipelines(
                    pipeline_ids=[pipeline_id],
                    rule_ids=[rule.id for rule in matching_new_rules],
                )

            # Only send supported new rules to Kafka
            supported_new_rules = [rule for rule in matching_new_rules if rule.is_supported]
            if supported_new_rules:
                await self.kafka.send_rules(str(pipeline_id), supported_new_rules)

            # Only send supported enabled updated rules to Kafka
            enabled_rules_ids = await self.pipeline_rule_repo.get_enabled_rules_ids(pipeline_id)
            enabled_rules_ids = {str(rule_id) for rule_id in enabled_rules_ids}
            current_rule_ids = set(await self.pipeline_rule_repo.get_rule_ids(pipeline_id))
            matching_updated_rules = [
                db_rules_map[rule.id]
                for rule in updated_rules
                if rule.id in db_rules_map and self._rule_matches_folders(db_rules_map[rule.id], folder_paths)
            ]
            missing_matching_updated_rules = [
                rule for rule in matching_updated_rules if rule.id not in current_rule_ids
            ]
            if missing_matching_updated_rules:
                await self._add_rules_to_pipelines(
                    pipeline_ids=[pipeline_id],
                    rule_ids=[rule.id for rule in missing_matching_updated_rules],
                )

            supported_updated_rules = [
                rule
                for rule in matching_updated_rules
                if (str(rule.id) in enabled_rules_ids or rule.id not in current_rule_ids) and rule.is_supported
            ]
            if supported_updated_rules:
                await self.kafka.send_rules(str(pipeline_id), supported_updated_rules)

            non_matching_updated_rule_ids = [
                UUID(rule.id)
                for rule in updated_rules
                if rule.id in db_rules_map and not self._rule_matches_folders(db_rules_map[rule.id], folder_paths)
            ]
            if non_matching_updated_rule_ids:
                await self.pipeline_rule_repo.delete_by_pipeline_and_rule_ids(
                    pipeline_id, non_matching_updated_rule_ids
                )
                await self.kafka.delete_rules(
                    str(pipeline_id),
                    [str(rule_id) for rule_id in non_matching_updated_rule_ids],
                )

            # Delete unsupported rules from Kafka (they might have become unsupported after update)
            unsupported_rule_ids = [
                rule.id
                for rule in matching_updated_rules
                if str(rule.id) in enabled_rules_ids and not rule.is_supported
            ]
            if unsupported_rule_ids:
                await self.kafka.delete_rules(str(pipeline_id), unsupported_rule_ids)

        await self.rule_check_service.refresh_for_rule_ids(all_rule_ids)

    async def _fetch_tdm_rule_ids(
        self, tdm: SocPrimeTdmProvider, tdm_repos: list[TdmRepository]
    ) -> dict[str, list[str]]:
        semaphore = asyncio.Semaphore(5)

        async def _fetch_ids_for_repo(repo_id: str) -> list[str]:
            async with semaphore:
                return await tdm.get_rule_ids(repo_id)

        fetch_tasks = [_fetch_ids_for_repo(repo.id) for repo in tdm_repos]
        fetch_results = await asyncio.gather(*fetch_tasks)
        repo_rules_pairs = zip(tdm_repos, fetch_results, strict=True)
        return {repo.id: rules for repo, rules in repo_rules_pairs}

    async def _fetch_rules_from_tdm(
        self, tdm: SocPrimeTdmProvider, tdm_repos: list[TdmRepository], fetch_all: bool
    ) -> list[tuple[TdmRepository, list[TdmRule]]]:
        semaphore = asyncio.Semaphore(5)

        async def _fetch_rules(repo: TdmRepository, fetch_all: bool) -> tuple[TdmRepository, list[TdmRule]]:
            async with semaphore:
                rules = await tdm.get_rules(
                    repository_id=repo.id,
                    updated_after=None if fetch_all else repo.last_synced_rule_date,
                )
                return repo, rules

        fetch_tasks = [_fetch_rules(repo=repo, fetch_all=fetch_all) for repo in tdm_repos]
        return await asyncio.gather(*fetch_tasks)

    async def _upsert_rules(
        self, repo_id: str, rules: list[TdmRule], change_source: str = CHANGE_SOURCE_SYSTEM
    ) -> None:
        """Upsert rules and validate them. Content hash prevents duplicate versions on unchanged body."""
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
        await self.rule_repo.upsert_many(rules_data, change_source=change_source, changed_by="system")

        # Validate upserted rules
        rule_ids = [UUID(rule.id) for rule in rules]
        db_rules = await self.rule_repo.get_by_ids(rule_ids)
        if db_rules:
            validation_service = SigmaValidationService(self.db)
            await validation_service.validate_rules_batch(db_rules, force=True)
