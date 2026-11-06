"""Tests for the TDM Push toggle.

Covers:
- Edit-time push gating in ``RulesOrchestrator.update_rule`` (push on/off, repo sync off).
- Pull-time protection of ``has_unpushed_changes`` rules in ``RulesOrchestrator._update_api_repos``.
- Reconciliation of pending rules in ``RulesOrchestrator._reconcile_unpushed_rules``.
- The ``ConfigDAO`` push-decision helpers and ``SocPrimeTdmConfig`` defaults.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.integrations.providers.socprime_tdm import (
    SocPrimeTdmConfig,
    TdmApiBadRequestError,
    TdmRepository,
    TdmRule,
)
from apps.managers.rule import RulesOrchestrator
from apps.modules.postgre.config import ConfigDAO

API_REPO_ID = UUID("770e8400-e29b-41d4-a716-446655440001")
RULE_ID = UUID("880e8400-e29b-41d4-a716-446655440000")


def _api_repository(*, sync_enabled: bool = True) -> MagicMock:
    repo = MagicMock()
    repo.id = API_REPO_ID
    repo.name = "Regular SOCPrime Repository"
    repo.type = "api"
    repo.is_system = False
    repo.sync_enabled = sync_enabled
    repo.last_synced_rule_date = None
    return repo


def _api_rule(repo: MagicMock, *, has_unpushed_changes: bool = False) -> MagicMock:
    rule = MagicMock()
    rule.id = RULE_ID
    rule.name = "Rule"
    rule.body = "title: Rule"
    rule.repository = repo
    rule.repository_id = repo.id
    rule.has_unpushed_changes = has_unpushed_changes
    return rule


@pytest.fixture
def admin_user():
    user = MagicMock()
    user.id = UUID("660e8400-e29b-41d4-a716-446655440000")
    user.email = "admin@example.com"
    user.full_name = "Admin User"
    return user


@pytest.fixture
def orchestrator():
    manager = RulesOrchestrator(AsyncMock())
    manager.repository_repo = AsyncMock()
    manager.rule_repo = AsyncMock()
    manager.pipeline_repo = AsyncMock()
    manager.pipeline_rule_repo = AsyncMock()
    manager.config_repo = AsyncMock()
    manager.kafka = AsyncMock()
    manager.rule_check_service = AsyncMock()
    manager.rule_content_repo = AsyncMock()
    return manager


class TestUpdateRulePushGating:
    """``RulesOrchestrator.update_rule`` push gating for non-system API rules."""

    @pytest.mark.asyncio
    async def test_push_on_pushes_to_tdm_and_clears_flag(self, orchestrator, admin_user):
        repo = _api_repository(sync_enabled=True)
        existing_rule = _api_rule(repo, has_unpushed_changes=True)
        updated_rule = MagicMock(id=RULE_ID, name="Updated", body="title: Updated", is_supported=True)

        orchestrator.rule_repo.get_by_id.side_effect = [existing_rule, updated_rule, updated_rule]
        orchestrator.rule_repo.update.return_value = updated_rule
        orchestrator.pipeline_rule_repo.get_by_rule_id.return_value = []
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "key", "push": True}

        tdm_rule = TdmRule(
            id=str(RULE_ID),
            name="Updated",
            body="title: Updated",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 2, tzinfo=UTC),
        )
        tdm = AsyncMock()
        tdm.update_rule.return_value = tdm_rule

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider", return_value=tdm),
            patch("apps.managers.rule.SigmaValidationService") as mock_validation,
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            mock_validation.return_value.validate_rule = AsyncMock()
            result = await orchestrator.update_rule(RULE_ID, {"name": "Updated", "body": "title: Updated"}, admin_user)

        assert result is updated_rule
        tdm.update_rule.assert_awaited_once()
        update_kwargs = orchestrator.rule_repo.update.await_args.kwargs
        assert update_kwargs["has_unpushed_changes"] is False
        assert update_kwargs["name"] == "Updated"
        assert update_kwargs["body"] == "title: Updated"

    @pytest.mark.asyncio
    async def test_push_off_skips_tdm_and_sets_flag(self, orchestrator, admin_user):
        repo = _api_repository(sync_enabled=True)
        existing_rule = _api_rule(repo)
        updated_rule = MagicMock(id=RULE_ID, name="Updated", body="title: Updated", is_supported=True)

        orchestrator.rule_repo.get_by_id.side_effect = [existing_rule, updated_rule, updated_rule]
        orchestrator.rule_repo.update.return_value = updated_rule
        orchestrator.pipeline_rule_repo.get_by_rule_id.return_value = []
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "key", "push": False}

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider") as mock_provider,
            patch("apps.managers.rule.SigmaValidationService") as mock_validation,
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            mock_validation.return_value.validate_rule = AsyncMock()
            result = await orchestrator.update_rule(RULE_ID, {"name": "Updated", "body": "title: Updated"}, admin_user)

        assert result is updated_rule
        mock_provider.assert_not_called()
        update_kwargs = orchestrator.rule_repo.update.await_args.kwargs
        assert update_kwargs["has_unpushed_changes"] is True
        assert update_kwargs["name"] == "Updated"
        assert update_kwargs["body"] == "title: Updated"

    @pytest.mark.asyncio
    async def test_repo_sync_disabled_skips_push_even_with_push_on(self, orchestrator, admin_user):
        repo = _api_repository(sync_enabled=False)
        existing_rule = _api_rule(repo)
        updated_rule = MagicMock(id=RULE_ID, name="Updated", body="title: Updated", is_supported=True)

        orchestrator.rule_repo.get_by_id.side_effect = [existing_rule, updated_rule, updated_rule]
        orchestrator.rule_repo.update.return_value = updated_rule
        orchestrator.pipeline_rule_repo.get_by_rule_id.return_value = []
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "key", "push": True}

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider") as mock_provider,
            patch("apps.managers.rule.SigmaValidationService") as mock_validation,
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            mock_validation.return_value.validate_rule = AsyncMock()
            await orchestrator.update_rule(RULE_ID, {"name": "Updated", "body": "title: Updated"}, admin_user)

        mock_provider.assert_not_called()
        update_kwargs = orchestrator.rule_repo.update.await_args.kwargs
        assert update_kwargs["has_unpushed_changes"] is True


class TestUpdateApiReposProtectsPendingRules:
    """Pull-time protection of ``has_unpushed_changes=True`` rules in ``_update_api_repos``."""

    def _setup(self, orchestrator, *, current_rules, tdm_rule_ids, fetched_rules):
        tdm_repo = TdmRepository(id=str(API_REPO_ID), name="Regular SOCPrime Repository", last_synced_rule_date=None)
        orchestrator._fetch_rules_from_tdm = AsyncMock(return_value=[(tdm_repo, fetched_rules)])
        orchestrator._fetch_tdm_rule_ids = AsyncMock(return_value={str(API_REPO_ID): tdm_rule_ids})
        orchestrator.rule_repo.get_all_by_repository.return_value = current_rules
        orchestrator._upsert_rules = AsyncMock()
        orchestrator._update_rules_in_pipelines_and_kafka = AsyncMock()
        return tdm_repo

    @pytest.mark.asyncio
    async def test_pull_does_not_overwrite_pending_rule(self, orchestrator):
        """Remote has an update for the pending rule, but it must not be applied."""
        pending_rule = _api_rule(_api_repository(), has_unpushed_changes=True)
        remote_rule = TdmRule(
            id=str(RULE_ID),
            name="Remote Name",
            body="title: Remote",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 3, tzinfo=UTC),
        )
        self._setup(
            orchestrator,
            current_rules=[pending_rule],
            tdm_rule_ids=[str(RULE_ID)],
            fetched_rules=[remote_rule],
        )
        orchestrator.config_repo.is_tdm_push_enabled.return_value = False
        orchestrator.config_repo.get_tdm_push_reenable_strategy.return_value = "next_sync"

        await orchestrator._update_api_repos(AsyncMock(), [TdmRepository(id=str(API_REPO_ID), name="x")])

        orchestrator.rule_repo.delete_many.assert_awaited_once_with([])
        # new_rules + updated_rules passed to the single upsert call; the pending rule must be excluded.
        assert orchestrator._upsert_rules.await_args.args[1] == []
        kafka_kwargs = orchestrator._update_rules_in_pipelines_and_kafka.await_args.kwargs
        assert kafka_kwargs["updated_rules"] == []

    @pytest.mark.asyncio
    async def test_pull_does_not_delete_pending_rule_removed_remotely(self, orchestrator):
        """TDM no longer has the pending rule (push was off when it was edited); local is kept."""
        pending_rule = _api_rule(_api_repository(), has_unpushed_changes=True)
        self._setup(
            orchestrator,
            current_rules=[pending_rule],
            tdm_rule_ids=[],
            fetched_rules=[],
        )
        orchestrator.config_repo.is_tdm_push_enabled.return_value = False
        orchestrator.config_repo.get_tdm_push_reenable_strategy.return_value = "next_sync"

        await orchestrator._update_api_repos(AsyncMock(), [TdmRepository(id=str(API_REPO_ID), name="x")])

        orchestrator.rule_repo.delete_many.assert_awaited_once_with([])

    @pytest.mark.asyncio
    async def test_cursor_advances_past_reconcile_pushed_rule(self, orchestrator):
        """A reconcile-pushed rule's new ``updated`` (absent from the incremental fetch) must still
        advance ``last_synced_rule_date`` so the next sync starts after it."""
        pending_rule = _api_rule(_api_repository(), has_unpushed_changes=True)
        self._setup(
            orchestrator,
            current_rules=[pending_rule],
            tdm_rule_ids=[str(RULE_ID)],
            fetched_rules=[],  # remote unchanged since the cursor → local edit is newer, gets pushed
        )
        orchestrator.config_repo.is_tdm_push_enabled.return_value = True
        orchestrator.config_repo.get_tdm_push_reenable_strategy.return_value = "next_sync"

        pushed_date = datetime(2026, 2, 1, tzinfo=UTC)
        tdm = AsyncMock()
        tdm.update_rule.return_value = TdmRule(
            id=str(RULE_ID),
            name="Local Name",
            body="title: Local",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=pushed_date,
        )

        with patch("apps.managers.rule.SigmaValidationService") as mock_validation:
            mock_validation.return_value.validate_rules_batch = AsyncMock()
            await orchestrator._update_api_repos(tdm, [TdmRepository(id=str(API_REPO_ID), name="x")])

        tdm.update_rule.assert_awaited_once()
        assert orchestrator.repository_repo.update.await_args.kwargs["last_synced_rule_date"] == pushed_date


class TestReconcileUnpushedRules:
    """Unit tests for ``RulesOrchestrator._reconcile_unpushed_rules``."""

    @staticmethod
    def _local_rule(rule_id: UUID, updated: datetime) -> MagicMock:
        rule = MagicMock()
        rule.id = rule_id
        rule.name = "Local Name"
        rule.body = "title: Local"
        rule.updated = updated
        return rule

    @pytest.mark.asyncio
    async def test_push_disabled_leaves_all_pending_untouched(self, orchestrator):
        local_rule = self._local_rule(RULE_ID, datetime(2026, 1, 5, tzinfo=UTC))
        tdm = AsyncMock()

        result = await orchestrator._reconcile_unpushed_rules(
            tdm=tdm,
            repository_id=str(API_REPO_ID),
            pending_rules_by_id={str(RULE_ID): local_rule},
            remote_rules=[],
            push_enabled=False,
            push_reenable_strategy="next_sync",
        )

        assert result == []
        tdm.update_rule.assert_not_awaited()
        orchestrator.rule_repo.update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_next_sync_pushes_local_newer_and_pulls_remote_newer_and_tie(self, orchestrator):
        local_newer_id = UUID("11111111-1111-1111-1111-111111111111")
        remote_newer_id = UUID("22222222-2222-2222-2222-222222222222")
        tie_id = UUID("33333333-3333-3333-3333-333333333333")

        local_rules = {
            str(local_newer_id): self._local_rule(local_newer_id, datetime(2026, 1, 10, tzinfo=UTC)),
            str(remote_newer_id): self._local_rule(remote_newer_id, datetime(2026, 1, 1, tzinfo=UTC)),
            str(tie_id): self._local_rule(tie_id, datetime(2026, 1, 5, tzinfo=UTC)),
        }
        remote_newer_rule = TdmRule(
            id=str(remote_newer_id),
            name="Remote",
            body="title: Remote",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 9, tzinfo=UTC),
        )
        tie_rule = TdmRule(
            id=str(tie_id),
            name="Remote Tie",
            body="title: Remote Tie",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 5, tzinfo=UTC),
        )

        tdm_rule_response = TdmRule(
            id=str(local_newer_id),
            name="Local Name",
            body="title: Local",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 11, tzinfo=UTC),
        )
        tdm = AsyncMock()
        tdm.update_rule.return_value = tdm_rule_response

        with patch("apps.managers.rule.SigmaValidationService") as mock_validation:
            mock_validation.return_value.validate_rules_batch = AsyncMock()
            result = await orchestrator._reconcile_unpushed_rules(
                tdm=tdm,
                repository_id=str(API_REPO_ID),
                pending_rules_by_id=local_rules,
                remote_rules=[remote_newer_rule, tie_rule],
                push_enabled=True,
                push_reenable_strategy="next_sync",
            )

        # Local-newer rule is pushed to TDM.
        tdm.update_rule.assert_awaited_once_with(
            repository_id=str(API_REPO_ID), rule_id=str(local_newer_id), name="Local Name", body="title: Local"
        )
        update_calls = {call.args[0]: call.kwargs for call in orchestrator.rule_repo.update.await_args_list}
        assert update_calls[local_newer_id]["has_unpushed_changes"] is False
        assert update_calls[remote_newer_id]["has_unpushed_changes"] is False
        assert update_calls[remote_newer_id]["body"] == "title: Remote"
        assert update_calls[tie_id]["has_unpushed_changes"] is False
        assert update_calls[tie_id]["body"] == "title: Remote Tie"

        # All three changed locally (local-newer pushed + replaced by TDM response, remote-newer
        # and tie pulled) so all are resynced to pipelines/Kafka.
        assert {rule.id for rule in result} == {str(local_newer_id), str(remote_newer_id), str(tie_id)}

    @pytest.mark.asyncio
    async def test_next_save_keeps_local_newer_pending_but_pulls_remote_newer(self, orchestrator):
        local_newer_id = UUID("11111111-1111-1111-1111-111111111111")
        remote_newer_id = UUID("22222222-2222-2222-2222-222222222222")

        local_rules = {
            str(local_newer_id): self._local_rule(local_newer_id, datetime(2026, 1, 10, tzinfo=UTC)),
            str(remote_newer_id): self._local_rule(remote_newer_id, datetime(2026, 1, 1, tzinfo=UTC)),
        }
        remote_newer_rule = TdmRule(
            id=str(remote_newer_id),
            name="Remote",
            body="title: Remote",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 9, tzinfo=UTC),
        )
        tdm = AsyncMock()

        with patch("apps.managers.rule.SigmaValidationService") as mock_validation:
            mock_validation.return_value.validate_rules_batch = AsyncMock()
            result = await orchestrator._reconcile_unpushed_rules(
                tdm=tdm,
                repository_id=str(API_REPO_ID),
                pending_rules_by_id=local_rules,
                remote_rules=[remote_newer_rule],
                push_enabled=True,
                push_reenable_strategy="next_save",
            )

        # Local-newer rule is neither pushed nor its flag cleared — stays pending.
        tdm.update_rule.assert_not_awaited()
        updated_ids = {call.args[0] for call in orchestrator.rule_repo.update.await_args_list}
        assert local_newer_id not in updated_ids
        assert remote_newer_id in updated_ids
        assert {rule.id for rule in result} == {str(remote_newer_id)}

    @pytest.mark.asyncio
    async def test_push_failure_keeps_rule_pending_without_aborting_sync(self, orchestrator):
        """A rejected TDM push leaves the rule pending and does not raise (one bad rule must not
        abort the whole scheduled sync)."""
        failing_id = UUID("11111111-1111-1111-1111-111111111111")
        ok_id = UUID("22222222-2222-2222-2222-222222222222")

        local_rules = {
            str(failing_id): self._local_rule(failing_id, datetime(2026, 1, 10, tzinfo=UTC)),
            str(ok_id): self._local_rule(ok_id, datetime(2026, 1, 10, tzinfo=UTC)),
        }
        ok_response = TdmRule(
            id=str(ok_id),
            name="Local Name",
            body="title: Local",
            created=datetime(2026, 1, 1, tzinfo=UTC),
            updated=datetime(2026, 1, 11, tzinfo=UTC),
        )
        tdm = AsyncMock()
        tdm.update_rule.side_effect = [TdmApiBadRequestError("nope"), ok_response]

        with patch("apps.managers.rule.SigmaValidationService") as mock_validation:
            mock_validation.return_value.validate_rules_batch = AsyncMock()
            result = await orchestrator._reconcile_unpushed_rules(
                tdm=tdm,
                repository_id=str(API_REPO_ID),
                pending_rules_by_id=local_rules,
                remote_rules=[],
                push_enabled=True,
                push_reenable_strategy="next_sync",
            )

        # Failing rule is left pending (no DB update); the other rule reconciles normally.
        assert tdm.update_rule.await_count == 2
        updated_ids = {call.args[0] for call in orchestrator.rule_repo.update.await_args_list}
        assert failing_id not in updated_ids
        assert ok_id in updated_ids
        assert {rule.id for rule in result} == {str(ok_id)}


class TestConfigPushHelpers:
    """``SocPrimeTdmConfig`` defaults and ``ConfigDAO`` push-decision helpers."""

    def test_config_defaults_push_on(self):
        config = SocPrimeTdmConfig(api_key="key")
        assert config.push is True
        # push_reenable_strategy is no longer a config field; it is always "next_sync".
        assert not hasattr(config, "push_reenable_strategy")

    def test_is_push_enabled_config_pure_helper(self):
        assert ConfigDAO.is_push_enabled_config(None) is False
        assert ConfigDAO.is_push_enabled_config({"api_key": "key"}) is True
        assert ConfigDAO.is_push_enabled_config({"api_key": "key", "push": False}) is False

    @pytest.mark.asyncio
    async def test_is_tdm_push_enabled_false_when_unconfigured(self):
        dao = ConfigDAO(AsyncMock())
        dao.get_socprime_tdm_provider_config = AsyncMock(return_value=None)
        assert await dao.is_tdm_push_enabled() is False

    @pytest.mark.asyncio
    async def test_is_tdm_push_enabled_defaults_true_when_field_missing(self):
        dao = ConfigDAO(AsyncMock())
        dao.get_socprime_tdm_provider_config = AsyncMock(return_value={"api_key": "key"})
        assert await dao.is_tdm_push_enabled() is True

    @pytest.mark.asyncio
    async def test_is_tdm_push_enabled_respects_explicit_false(self):
        dao = ConfigDAO(AsyncMock())
        dao.get_socprime_tdm_provider_config = AsyncMock(return_value={"api_key": "key", "push": False})
        assert await dao.is_tdm_push_enabled() is False

    @pytest.mark.asyncio
    async def test_get_tdm_push_reenable_strategy_defaults_to_next_sync(self):
        dao = ConfigDAO(AsyncMock())
        dao.get_socprime_tdm_provider_config = AsyncMock(return_value=None)
        assert await dao.get_tdm_push_reenable_strategy() == "next_sync"

    @pytest.mark.asyncio
    async def test_get_tdm_push_reenable_strategy_returns_configured_value(self):
        dao = ConfigDAO(AsyncMock())
        dao.get_socprime_tdm_provider_config = AsyncMock(
            return_value={"api_key": "key", "push_reenable_strategy": "next_save"}
        )
        assert await dao.get_tdm_push_reenable_strategy() == "next_save"
