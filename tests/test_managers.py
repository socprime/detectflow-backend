"""Unit tests for managers (business logic layer)."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.enums import UserRole
from apps.core.exceptions import BadRequestError, DuplicateEntityError, NotFoundError, RepositoryNotLocalError
from apps.core.schemas import UserCreateRequest, UserUpdateRequest
from apps.core.settings import settings
from apps.integrations.providers.socprime_tdm import TdmRepository
from apps.managers.rule import RulesOrchestrator

# ==========================================
# USERS MANAGER TESTS
# ==========================================


class TestUsersManager:
    """Tests for UsersManager."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()

    @pytest.fixture
    def mock_user_dao(self):
        """Create mock UserDAO."""
        return AsyncMock()

    @pytest.fixture
    def mock_activity_producer(self):
        """Create mock activity producer."""
        mock = AsyncMock()
        mock.log_action = AsyncMock()
        return mock

    @pytest.fixture
    def sample_user(self):
        """Create sample user object."""
        user = MagicMock()
        user.id = UUID("550e8400-e29b-41d4-a716-446655440000")
        user.email = "test@example.com"
        user.full_name = "Test User"
        user.is_active = True
        user.role = "admin"
        user.created = datetime.now()
        user.updated = datetime.now()
        return user

    @pytest.fixture
    def admin_user(self):
        """Create admin user for activity logging."""
        user = MagicMock()
        user.id = UUID("660e8400-e29b-41d4-a716-446655440000")
        user.email = "admin@example.com"
        user.full_name = "Admin User"
        return user

    @pytest.mark.asyncio
    async def test_get_by_id_success(self, mock_db, mock_user_dao, sample_user):
        """Should return user when found."""
        mock_user_dao.get_by_id.return_value = sample_user

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            result = await manager.get_by_id(sample_user.id)

            assert result == sample_user
            mock_user_dao.get_by_id.assert_called_once_with(sample_user.id)

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, mock_db, mock_user_dao):
        """Should raise NotFoundError when user not found."""
        mock_user_dao.get_by_id.return_value = None
        user_id = UUID("550e8400-e29b-41d4-a716-446655440000")

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            with pytest.raises(NotFoundError) as exc_info:
                await manager.get_by_id(user_id)

            assert str(user_id) in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_create_success(self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user):
        """Should create user and log activity."""
        mock_user_dao.get_by_email.return_value = None
        mock_user_dao.create.return_value = sample_user

        request = UserCreateRequest(
            full_name="Test User",
            email="test@example.com",
            password="SecurePass123!",
            is_active=True,
            role="admin",
        )

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
            patch("apps.managers.users.hash_password_async", return_value="hashed_password"),
        ):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            result = await manager.create(request, admin_user)

            assert result == sample_user
            mock_user_dao.get_by_email.assert_called_once_with(request.email)
            mock_user_dao.create.assert_called_once()
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_viewer_does_not_require_password_change(
        self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user
    ):
        """Viewer accounts must not be forced to change their initial password."""
        mock_user_dao.get_by_email.return_value = None
        mock_user_dao.create.return_value = sample_user
        request = UserCreateRequest(
            full_name="Read Only",
            email="viewer@example.com",
            password="SecurePass123!",
            role=UserRole.VIEWER,
        )

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
            patch("apps.managers.users.hash_password_async", return_value="hashed_password"),
        ):
            from apps.managers.users import UsersManager

            await UsersManager(mock_db).create(request, admin_user)

        assert mock_user_dao.create.await_args.kwargs["must_change_password"] is False

    @pytest.mark.asyncio
    async def test_create_duplicate_email(self, mock_db, mock_user_dao, sample_user, admin_user):
        """Should raise DuplicateEntityError when email exists."""
        mock_user_dao.get_by_email.return_value = sample_user

        request = UserCreateRequest(
            full_name="Test User",
            email="test@example.com",
            password="SecurePass123!",
            is_active=True,
            role="admin",
        )

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            with pytest.raises(DuplicateEntityError) as exc_info:
                await manager.create(request, admin_user)

            assert "email" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_delete_success(self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user):
        """Should delete user and log activity."""
        mock_user_dao.get_by_id.return_value = sample_user
        mock_user_dao.delete.return_value = True

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
        ):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            result = await manager.delete(sample_user.id, admin_user)

            assert result is True
            mock_user_dao.delete.assert_called_once_with(sample_user.id)
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_not_found(self, mock_db, mock_user_dao, admin_user):
        """Should raise NotFoundError when user not found."""
        mock_user_dao.get_by_id.return_value = None
        user_id = UUID("550e8400-e29b-41d4-a716-446655440000")

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            with pytest.raises(NotFoundError):
                await manager.delete(user_id, admin_user)

    @pytest.mark.asyncio
    async def test_update_success(self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user):
        """Should update user and log activity."""
        mock_user_dao.get_by_id.return_value = sample_user
        mock_user_dao.get_by_email.return_value = None

        updated_user = MagicMock()
        updated_user.id = sample_user.id
        updated_user.email = "newemail@example.com"
        updated_user.full_name = "Updated Name"
        mock_user_dao.update.return_value = updated_user

        request = UserUpdateRequest(
            full_name="Updated Name",
            email="newemail@example.com",
        )

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
        ):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            result = await manager.update(sample_user.id, request, admin_user)

            assert result == updated_user
            mock_user_dao.update.assert_called_once()
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_reset_viewer_password_requires_change_on_next_login(
        self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user
    ):
        """After admin reset, a viewer must change the temporary password."""
        sample_user.role = UserRole.VIEWER
        mock_user_dao.get_by_id.return_value = sample_user

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
            patch("apps.managers.users.generate_temporary_password", return_value="TemporaryPass123!"),
            patch("apps.managers.users.hash_password_async", return_value="hashed_password"),
        ):
            from apps.managers.users import UsersManager

            await UsersManager(mock_db).reset_password(sample_user.id, admin_user)

        mock_user_dao.update_password.assert_awaited_once_with(
            sample_user.id,
            "hashed_password",
            must_change=True,
        )

    @pytest.mark.asyncio
    async def test_update_not_found(self, mock_db, mock_user_dao, admin_user):
        """Should raise NotFoundError when user not found."""
        mock_user_dao.get_by_id.return_value = None
        user_id = UUID("550e8400-e29b-41d4-a716-446655440000")

        request = UserUpdateRequest(full_name="Updated Name")

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            with pytest.raises(NotFoundError) as exc_info:
                await manager.update(user_id, request, admin_user)

            assert str(user_id) in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_update_with_same_email_no_error(
        self, mock_db, mock_user_dao, mock_activity_producer, sample_user, admin_user
    ):
        """Should NOT raise error when updating user with their same email.

        Regression test for bug: previously the duplicate check did not
        exclude the user being updated, causing false positives.
        """
        mock_user_dao.get_by_id.return_value = sample_user
        # get_by_email returns the same user (same email lookup)
        mock_user_dao.get_by_email.return_value = sample_user
        mock_user_dao.update.return_value = sample_user

        # Update with the same email the user already has
        request = UserUpdateRequest(email=sample_user.email)

        with (
            patch("apps.managers.users.UserDAO", return_value=mock_user_dao),
            patch("apps.managers.users.activity_producer", mock_activity_producer),
        ):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            # Should NOT raise DuplicateEntityError
            result = await manager.update(sample_user.id, request, admin_user)

            assert result == sample_user
            mock_user_dao.update.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_with_duplicate_email_raises_error(self, mock_db, mock_user_dao, sample_user, admin_user):
        """Should raise DuplicateEntityError when updating with another user's email."""
        # Create another user who owns the email we're trying to use
        other_user = MagicMock()
        other_user.id = UUID("660e8400-e29b-41d4-a716-446655440001")
        other_user.email = "other@example.com"

        mock_user_dao.get_by_id.return_value = sample_user
        # get_by_email returns the other user who owns this email
        mock_user_dao.get_by_email.return_value = other_user

        request = UserUpdateRequest(email="other@example.com")

        with patch("apps.managers.users.UserDAO", return_value=mock_user_dao):
            from apps.managers.users import UsersManager

            manager = UsersManager(mock_db)
            manager.user_repo = mock_user_dao

            with pytest.raises(DuplicateEntityError) as exc_info:
                await manager.update(sample_user.id, request, admin_user)

            assert "email" in str(exc_info.value.detail)


# ==========================================
# REPOSITORIES MANAGER TESTS
# ==========================================


class TestRepositoriesManager:
    """Tests for RepositoriesManager."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()

    @pytest.fixture
    def mock_repo_dao(self):
        """Create mock RepositoryDAO."""
        return AsyncMock()

    @pytest.fixture
    def mock_activity_producer(self):
        """Create mock activity producer."""
        mock = AsyncMock()
        mock.log_action = AsyncMock()
        return mock

    @pytest.fixture
    def sample_repository(self):
        """Create sample repository object."""
        repo = MagicMock()
        repo.id = UUID("770e8400-e29b-41d4-a716-446655440000")
        repo.name = "Test Repository"
        repo.type = "local"
        repo.sync_enabled = False
        repo.is_system = False
        repo.system_key = None
        repo.rules_count = 10
        repo.source_link = None
        repo.created = datetime.now()
        repo.updated = datetime.now()
        return repo

    @pytest.fixture
    def admin_user(self):
        """Create admin user for activity logging."""
        user = MagicMock()
        user.id = UUID("660e8400-e29b-41d4-a716-446655440000")
        user.email = "admin@example.com"
        user.full_name = "Admin User"
        return user

    @pytest.mark.asyncio
    async def test_get_by_id_success(self, mock_db, mock_repo_dao, sample_repository):
        """Should return repository when found."""
        mock_repo_dao.get_by_id.return_value = sample_repository

        with patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            result = await manager.get_by_id(sample_repository.id)

            assert result == sample_repository

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, mock_db, mock_repo_dao):
        """Should raise NotFoundError when repository not found."""
        mock_repo_dao.get_by_id.return_value = None
        repo_id = UUID("770e8400-e29b-41d4-a716-446655440000")

        with patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            with pytest.raises(NotFoundError) as exc_info:
                await manager.get_by_id(repo_id)

            assert str(repo_id) in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_create_local_success(
        self, mock_db, mock_repo_dao, mock_activity_producer, sample_repository, admin_user
    ):
        """Should create local repository and log activity."""
        mock_repo_dao.create.return_value = sample_repository

        with (
            patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao),
            patch("apps.managers.repositories.activity_producer", mock_activity_producer),
        ):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            result = await manager.create_local(name="Test Repository", user=admin_user)

            assert result == sample_repository
            mock_repo_dao.create.assert_called_once()
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_toggle_sync_local_repository(self, mock_db, mock_repo_dao, sample_repository, admin_user):
        """Should raise BadRequestError when toggling sync on local repository."""
        sample_repository.type = "local"
        mock_repo_dao.get_by_id.return_value = sample_repository

        with patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao):
            from apps.core.exceptions import BadRequestError
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            with pytest.raises(BadRequestError) as exc_info:
                await manager.toggle_sync(sample_repository.id, True, admin_user)

            assert "local" in str(exc_info.value.detail).lower()

    @pytest.mark.asyncio
    async def test_toggle_sync_api_repository(
        self, mock_db, mock_repo_dao, mock_activity_producer, sample_repository, admin_user
    ):
        """Should toggle sync for API repository."""
        sample_repository.type = "api"
        sample_repository.sync_enabled = False
        mock_repo_dao.get_by_id.return_value = sample_repository

        with (
            patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao),
            patch("apps.managers.repositories.activity_producer", mock_activity_producer),
        ):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            message, sync_enabled = await manager.toggle_sync(sample_repository.id, True, admin_user)

            assert sync_enabled is True
            assert "enabled" in message.lower()
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_local_rejects_gitlab_rename(self, mock_db, mock_repo_dao, sample_repository, admin_user):
        """Should reject direct renaming of a GitLab integration-owned repository."""
        from apps.core.models import GITLAB_REPOSITORY_TYPE

        sample_repository.type = GITLAB_REPOSITORY_TYPE
        mock_repo_dao.get_by_id.return_value = sample_repository

        with patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            with pytest.raises(BadRequestError):
                await manager.update_local(sample_repository.id, "Renamed GitLab", admin_user)

            mock_repo_dao.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_toggle_sync_gitlab_updates_integration_auto_pull(
        self, mock_db, mock_repo_dao, mock_activity_producer, sample_repository, admin_user
    ):
        """Should flip the owning integration's auto_pull for GitLab repositories."""
        from apps.core.models import GITLAB_REPOSITORY_TYPE

        sample_repository.type = GITLAB_REPOSITORY_TYPE
        sample_repository.sync_enabled = True
        mock_repo_dao.get_by_id.return_value = sample_repository

        link = MagicMock()
        link.integration_id = UUID("990e8400-e29b-41d4-a716-446655440000")
        integration = MagicMock()
        integration.config = {"token": "secret", "auto_pull": True}

        mock_gitlab_dao = AsyncMock()
        mock_gitlab_dao.get_by_repository.return_value = link
        mock_integration_dao = AsyncMock()
        mock_integration_dao.get_by_id.return_value = integration

        with (
            patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao),
            patch("apps.managers.repositories.GitlabRepositoryDAO", return_value=mock_gitlab_dao),
            patch("apps.managers.repositories.IntegrationDAO", return_value=mock_integration_dao),
            patch("apps.managers.repositories.activity_producer", mock_activity_producer),
        ):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao

            message, sync_enabled = await manager.toggle_sync(sample_repository.id, False, admin_user)

            assert sync_enabled is False
            assert "disabled" in message.lower()
            assert mock_integration_dao.update.await_args.kwargs["config"]["auto_pull"] is False
            assert mock_integration_dao.update.await_args.kwargs["config"]["token"] == "secret"
            mock_repo_dao.update.assert_awaited_once()
            mock_activity_producer.log_action.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method_name", ["update_local", "delete", "toggle_sync"])
    async def test_system_repository_mutations_are_rejected(
        self,
        method_name,
        mock_db,
        mock_repo_dao,
        sample_repository,
        admin_user,
    ):
        """Should reject user mutation paths for system repositories."""
        sample_repository.type = "api"
        sample_repository.sync_enabled = True
        sample_repository.is_system = True
        sample_repository.system_key = "active_threat_sigma_rules"
        mock_repo_dao.get_by_id.return_value = sample_repository

        with patch("apps.managers.repositories.RepositoryDAO", return_value=mock_repo_dao):
            from apps.managers.repositories import RepositoriesManager

            manager = RepositoriesManager(mock_db)
            manager.repo_storage = mock_repo_dao
            manager.pipeline_dao = AsyncMock()

            with pytest.raises(BadRequestError) as exc_info:
                if method_name == "update_local":
                    await manager.update_local(sample_repository.id, "Updated Repository", admin_user)
                elif method_name == "delete":
                    await manager.delete(sample_repository.id, admin_user)
                else:
                    await manager.toggle_sync(sample_repository.id, False, admin_user)

            assert "system" in str(exc_info.value.detail).lower()
            mock_repo_dao.update.assert_not_called()
            mock_repo_dao.delete.assert_not_called()


# ==========================================
# RULES MANAGER TESTS
# ==========================================


class TestRulesOrchestratorSystemRepositories:
    """Tests for system repository protection in RulesOrchestrator."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()

    @pytest.fixture
    def admin_user(self):
        """Create admin user for activity logging."""
        user = MagicMock()
        user.id = UUID("660e8400-e29b-41d4-a716-446655440000")
        user.email = "admin@example.com"
        user.full_name = "Admin User"
        return user

    @pytest.fixture
    def system_repository(self):
        """Create system repository object."""
        repo = MagicMock()
        repo.id = UUID("770e8400-e29b-41d4-a716-446655440000")
        repo.name = "SOC Prime Active Threats"
        repo.type = "api"
        repo.sync_enabled = True
        repo.is_system = True
        repo.system_key = "active_threat_sigma_rules"
        return repo

    @pytest.fixture
    def regular_api_repository(self):
        """Create regular API repository object."""
        repo = MagicMock()
        repo.id = UUID("770e8400-e29b-41d4-a716-446655440001")
        repo.name = "Regular SOCPrime Repository"
        repo.type = "api"
        repo.sync_enabled = True
        repo.is_system = False
        repo.system_key = None
        repo.last_synced_rule_date = None
        return repo

    @pytest.fixture
    def system_rule(self, system_repository):
        """Create rule in system repository."""
        rule = MagicMock()
        rule.id = UUID("880e8400-e29b-41d4-a716-446655440000")
        rule.name = "System Rule"
        rule.repository = system_repository
        return rule

    @pytest.fixture
    def orchestrator(self, mock_db):
        """Create RulesOrchestrator with mocked dependencies."""
        manager = RulesOrchestrator(mock_db)
        manager.repository_repo = AsyncMock()
        manager.rule_repo = AsyncMock()
        manager.pipeline_repo = AsyncMock()
        manager.pipeline_rule_repo = AsyncMock()
        manager.config_repo = AsyncMock()
        manager.kafka = AsyncMock()
        manager.rule_check_service = AsyncMock()
        return manager

    @pytest.mark.asyncio
    async def test_create_rule_in_system_repository_is_rejected(
        self,
        orchestrator,
        system_repository,
        admin_user,
    ):
        """Should reject creating one rule in a system repository."""
        orchestrator.repository_repo.get_by_id.return_value = system_repository

        with pytest.raises(RepositoryNotLocalError) as exc_info:
            await orchestrator.create_local_rule(
                rule_data={"name": "Rule", "body": "title: Rule"},
                repository_id=system_repository.id,
                user=admin_user,
            )

        assert "system" in str(exc_info.value.detail).lower()
        orchestrator.rule_repo.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_bulk_create_rules_in_system_repository_is_rejected(
        self,
        orchestrator,
        system_repository,
        admin_user,
    ):
        """Should reject bulk rule creation in a system repository."""
        orchestrator.repository_repo.get_by_id.return_value = system_repository

        with pytest.raises(RepositoryNotLocalError) as exc_info:
            await orchestrator.create_local_rules_bulk(
                rules_data=[{"name": "Rule", "body": "title: Rule"}],
                repository_id=system_repository.id,
                user=admin_user,
            )

        assert "system" in str(exc_info.value.detail).lower()
        orchestrator.rule_repo.create_many.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_rule_in_system_repository_is_rejected(
        self,
        orchestrator,
        system_rule,
        admin_user,
    ):
        """Should reject delete for rules owned by a system repository."""
        orchestrator.rule_repo.get_by_id.return_value = system_rule

        with pytest.raises(RepositoryNotLocalError) as exc_info:
            await orchestrator.delete_local_rule(system_rule.id, admin_user)

        assert "system" in str(exc_info.value.detail).lower()
        orchestrator.rule_repo.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_rule_in_system_repository_marks_user_edited(
        self,
        orchestrator,
        system_rule,
        admin_user,
    ):
        """System-repo rule edits succeed and persist ``is_user_edited_system_rule=True``.

        TDM API round-trips are skipped because system rules are not propagated back to TDM,
        and Active Threat sync skips rows where the flag is set.
        """
        updated_rule = MagicMock()
        updated_rule.id = system_rule.id
        updated_rule.name = "Updated Rule"
        updated_rule.body = "title: Updated Rule"
        updated_rule.is_supported = True
        orchestrator.rule_repo.get_by_id.side_effect = [system_rule, updated_rule, updated_rule]
        orchestrator.rule_repo.update.return_value = updated_rule
        orchestrator.pipeline_rule_repo.get_by_rule_id.return_value = []

        with patch("apps.managers.rule.SigmaValidationService") as mock_validation:
            mock_validation.return_value.validate_rule = AsyncMock()
            result = await orchestrator.update_rule(
                system_rule.id, {"name": "Updated Rule", "body": "title: Updated Rule"}, admin_user
            )

        assert result is updated_rule
        orchestrator.rule_repo.update.assert_awaited_once()
        update_kwargs = orchestrator.rule_repo.update.await_args.kwargs
        assert update_kwargs["is_user_edited_system_rule"] is True
        assert update_kwargs["name"] == "Updated Rule"
        assert update_kwargs["body"] == "title: Updated Rule"
        orchestrator.config_repo.get_socprime_tdm_provider_config.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_system_repository_via_rules_orchestrator_is_rejected(
        self,
        orchestrator,
        system_repository,
    ):
        """Should reject repository delete path used by repository manager."""
        orchestrator.repository_repo.get_by_id.return_value = system_repository

        with pytest.raises(RepositoryNotLocalError) as exc_info:
            await orchestrator.delete_repository(system_repository.id)

        assert "system" in str(exc_info.value.detail).lower()
        orchestrator.repository_repo.delete.assert_not_called()

    @pytest.mark.asyncio
    async def test_sync_api_repos_skips_system_repositories(
        self,
        orchestrator,
        system_repository,
        regular_api_repository,
    ):
        """Regular SOCPrime sync should not delete or update system repositories."""
        # DB has two sync-enabled API repositories. The system one should be
        # removed from the sync candidate list before comparing with TDM.
        orchestrator.repository_repo.get_all.return_value = ([system_repository, regular_api_repository], 2)
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "test-key"}
        orchestrator._delete_repository = AsyncMock()
        orchestrator._update_api_repos = AsyncMock()

        # TDM returns no repositories. If the system repo were not skipped,
        # sync would try to delete both DB repositories as missing remotely.
        tdm = AsyncMock()
        tdm.get_repositories.return_value = []

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider", return_value=tdm),
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            result = await orchestrator.sync_api_repos()

        assert result is True
        # Only the regular API repository is treated as deleted by TDM sync.
        # The system repository must be ignored and left for its own lifecycle.
        orchestrator._delete_repository.assert_called_once_with(regular_api_repository.id)
        orchestrator._update_api_repos.assert_awaited_once_with(tdm, [])

    @pytest.mark.asyncio
    async def test_sync_api_repos_auto_creates_all_available_tdm_repositories(self, orchestrator):
        """TDM repositories are discovered and fully imported without an explicit add request."""
        tdm_repository = TdmRepository(
            id="770e8400-e29b-41d4-a716-446655440002",
            name="New SOCPrime Repository",
        )
        orchestrator.repository_repo.get_all.return_value = ([], 0)
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "test-key"}
        orchestrator._fetch_rules_from_tdm = AsyncMock(return_value=[(tdm_repository, [])])
        orchestrator._upsert_rules = AsyncMock()
        orchestrator._delete_repository = AsyncMock()
        orchestrator._update_api_repos = AsyncMock()

        tdm = AsyncMock()
        tdm.get_repositories.return_value = [tdm_repository]

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider", return_value=tdm),
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            result = await orchestrator.sync_api_repos()

        assert result is True
        orchestrator.repository_repo.create.assert_awaited_once_with(
            id=UUID(tdm_repository.id),
            name=tdm_repository.name,
            type="api",
            sync_enabled=True,
            last_synced_rule_date=None,
            source_link=(
                f"https://{settings.tdm_hostname}/expert/?repositoryIds%5B%5D={tdm_repository.id}&repositoryType=custom"
            ),
        )
        orchestrator._fetch_rules_from_tdm.assert_awaited_once_with(tdm, [tdm_repository], fetch_all=True)
        orchestrator._upsert_rules.assert_awaited_once_with(tdm_repository.id, [])
        orchestrator._update_api_repos.assert_awaited_once_with(tdm, [])

    @pytest.mark.asyncio
    async def test_sync_api_repos_retains_sync_disabled_repository_missing_from_tdm(
        self,
        orchestrator,
        regular_api_repository,
    ):
        """A disabled local repository survives when TDM no longer returns it."""
        regular_api_repository.sync_enabled = False
        orchestrator.repository_repo.get_all.return_value = ([regular_api_repository], 1)
        orchestrator.config_repo.get_socprime_tdm_provider_config.return_value = {"api_key": "test-key"}
        orchestrator._delete_repository = AsyncMock()
        orchestrator._update_api_repos = AsyncMock()

        tdm = AsyncMock()
        tdm.get_repositories.return_value = []

        with (
            patch("apps.managers.rule.SocPrimeTdmProvider", return_value=tdm),
            patch("apps.managers.rule.activity_producer", AsyncMock()),
        ):
            result = await orchestrator.sync_api_repos()

        assert result is False
        orchestrator._delete_repository.assert_not_awaited()
        orchestrator._update_api_repos.assert_awaited_once_with(tdm, [])


class TestRulesOrchestratorSyncSystemRepositoryChanges:
    """Tests for ``RulesOrchestrator.sync_system_repository_changes``."""

    @pytest.fixture
    def mock_db(self):
        return AsyncMock()

    @pytest.fixture
    def orchestrator(self, mock_db):
        manager = RulesOrchestrator(mock_db)
        manager.repository_repo = AsyncMock()
        manager.rule_repo = AsyncMock()
        manager.pipeline_repo = AsyncMock()
        manager.pipeline_rule_repo = AsyncMock()
        manager.config_repo = AsyncMock()
        manager.kafka = AsyncMock()
        manager.rule_check_service = AsyncMock()
        manager._add_rules_to_pipelines = AsyncMock()
        return manager

    @staticmethod
    def _rule(rule_id: str, *, is_supported: bool = True, folder_path: str | None = None) -> MagicMock:
        rule = MagicMock()
        rule.id = UUID(rule_id)
        rule.is_supported = is_supported
        rule.name = f"rule-{rule_id}"
        rule.folder_path = folder_path
        return rule

    @pytest.mark.asyncio
    async def test_no_pipelines_short_circuits(self, orchestrator):
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {}
        orchestrator.rule_repo.get_by_ids.return_value = []

        await orchestrator.sync_system_repository_changes(
            repository_id=UUID("770e8400-e29b-41d4-a716-446655440000"),
            new_rule_ids=[UUID("11111111-1111-1111-1111-111111111111")],
            updated_rule_ids=[],
            deleted_rule_ids=[],
        )

        orchestrator.kafka.send_rules.assert_not_called()
        orchestrator.kafka.delete_rules.assert_not_called()
        orchestrator._add_rules_to_pipelines.assert_not_called()
        orchestrator.rule_check_service.refresh_for_rule_ids.assert_awaited_once_with(
            [UUID("11111111-1111-1111-1111-111111111111")]
        )

    @pytest.mark.asyncio
    async def test_sends_new_and_updated_supported_rules_and_deletes_removed(self, orchestrator):
        repository_id = UUID("770e8400-e29b-41d4-a716-446655440000")
        pipeline = MagicMock()
        pipeline.id = UUID("99999999-9999-9999-9999-999999999999")
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {pipeline.id: []}

        new_rule = self._rule("11111111-1111-1111-1111-111111111111")
        updated_rule_supported = self._rule("22222222-2222-2222-2222-222222222222")
        updated_rule_unsupported = self._rule("33333333-3333-3333-3333-333333333333", is_supported=False)
        deleted_rule_id = UUID("44444444-4444-4444-4444-444444444444")

        orchestrator.rule_repo.get_by_ids.return_value = [
            new_rule,
            updated_rule_supported,
            updated_rule_unsupported,
        ]
        orchestrator.pipeline_rule_repo.get_enabled_rules_ids.return_value = [
            updated_rule_supported.id,
            updated_rule_unsupported.id,
        ]
        orchestrator.pipeline_rule_repo.get_rule_ids.return_value = [
            updated_rule_supported.id,
            updated_rule_unsupported.id,
        ]

        await orchestrator.sync_system_repository_changes(
            repository_id=repository_id,
            new_rule_ids=[new_rule.id],
            updated_rule_ids=[updated_rule_supported.id, updated_rule_unsupported.id],
            deleted_rule_ids=[deleted_rule_id],
        )

        orchestrator._add_rules_to_pipelines.assert_awaited_once_with(
            pipeline_ids=[pipeline.id],
            rule_ids=[new_rule.id],
        )

        send_call_args = [call.args for call in orchestrator.kafka.send_rules.await_args_list]
        delete_call_args = [call.args for call in orchestrator.kafka.delete_rules.await_args_list]

        assert (str(pipeline.id), [new_rule]) in send_call_args
        assert (str(pipeline.id), [updated_rule_supported]) in send_call_args
        assert (str(pipeline.id), [str(deleted_rule_id)]) in delete_call_args
        assert (str(pipeline.id), [str(updated_rule_unsupported.id)]) in delete_call_args
        orchestrator.rule_check_service.refresh_for_rule_ids.assert_awaited_once_with(
            [new_rule.id, updated_rule_supported.id, updated_rule_unsupported.id]
        )

    @pytest.mark.asyncio
    async def test_sync_system_repository_changes_filters_added_rules_by_selected_folders(self, orchestrator):
        repository_id = UUID("770e8400-e29b-41d4-a716-446655440000")
        pipeline_id = UUID("99999999-9999-9999-9999-999999999999")
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {
            pipeline_id: ["_product/windows"]
        }

        matching_new_rule = self._rule(
            "11111111-1111-1111-1111-111111111111",
            folder_path="_product/windows/_service/security",
        )
        non_matching_new_rule = self._rule(
            "22222222-2222-2222-2222-222222222222",
            folder_path="_product/linux",
        )
        matching_updated_rule = self._rule(
            "33333333-3333-3333-3333-333333333333",
            folder_path="_product/windows",
        )
        orchestrator.rule_repo.get_by_ids.return_value = [
            matching_new_rule,
            non_matching_new_rule,
            matching_updated_rule,
        ]
        orchestrator.pipeline_rule_repo.get_enabled_rules_ids.return_value = []
        orchestrator.pipeline_rule_repo.get_rule_ids.return_value = []

        await orchestrator.sync_system_repository_changes(
            repository_id=repository_id,
            new_rule_ids=[matching_new_rule.id, non_matching_new_rule.id],
            updated_rule_ids=[matching_updated_rule.id],
            deleted_rule_ids=[],
        )

        add_call_args = [call.kwargs for call in orchestrator._add_rules_to_pipelines.await_args_list]
        assert {"pipeline_ids": [pipeline_id], "rule_ids": [matching_new_rule.id]} in add_call_args
        assert {"pipeline_ids": [pipeline_id], "rule_ids": [matching_updated_rule.id]} in add_call_args
        assert all(non_matching_new_rule.id not in kwargs["rule_ids"] for kwargs in add_call_args)

        send_call_args = [call.args for call in orchestrator.kafka.send_rules.await_args_list]
        assert (str(pipeline_id), [matching_new_rule]) in send_call_args
        assert (str(pipeline_id), [matching_updated_rule]) in send_call_args

    @pytest.mark.asyncio
    async def test_sync_system_repository_changes_does_not_readd_disabled_matching_rule(self, orchestrator):
        repository_id = UUID("770e8400-e29b-41d4-a716-446655440000")
        pipeline_id = UUID("99999999-9999-9999-9999-999999999999")
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {
            pipeline_id: ["_product/windows"]
        }

        disabled_existing_rule = self._rule(
            "33333333-3333-3333-3333-333333333333",
            folder_path="_product/windows",
        )
        orchestrator.rule_repo.get_by_ids.return_value = [disabled_existing_rule]
        orchestrator.pipeline_rule_repo.get_enabled_rules_ids.return_value = []
        orchestrator.pipeline_rule_repo.get_rule_ids.return_value = [disabled_existing_rule.id]

        await orchestrator.sync_system_repository_changes(
            repository_id=repository_id,
            new_rule_ids=[],
            updated_rule_ids=[disabled_existing_rule.id],
            deleted_rule_ids=[],
        )

        orchestrator._add_rules_to_pipelines.assert_not_called()
        orchestrator.kafka.send_rules.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_rules_in_pipelines_filters_added_rules_by_selected_folders(self, orchestrator):
        repository_id = UUID("770e8400-e29b-41d4-a716-446655440000")
        pipeline_id = UUID("99999999-9999-9999-9999-999999999999")
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {
            pipeline_id: ["_product/windows"]
        }

        matching_new = MagicMock(id="11111111-1111-1111-1111-111111111111")
        non_matching_new = MagicMock(id="22222222-2222-2222-2222-222222222222")
        matching_updated = MagicMock(id="33333333-3333-3333-3333-333333333333")

        matching_new_db = self._rule(matching_new.id, folder_path="_product/windows")
        non_matching_new_db = self._rule(non_matching_new.id, folder_path="_product/linux")
        matching_updated_db = self._rule(matching_updated.id, folder_path="_product/windows/_service/security")

        orchestrator.rule_repo.get_by_ids.return_value = [
            matching_new_db,
            non_matching_new_db,
            matching_updated_db,
        ]
        orchestrator.pipeline_rule_repo.get_enabled_rules_ids.return_value = []
        orchestrator.pipeline_rule_repo.get_rule_ids.return_value = []

        await orchestrator._update_rules_in_pipelines_and_kafka(
            repo_id=repository_id,
            new_rules=[matching_new, non_matching_new],
            deleted_rule_ids=[],
            updated_rules=[matching_updated],
        )

        add_call_args = [call.kwargs for call in orchestrator._add_rules_to_pipelines.await_args_list]
        assert {"pipeline_ids": [pipeline_id], "rule_ids": [matching_new_db.id]} in add_call_args
        assert {"pipeline_ids": [pipeline_id], "rule_ids": [matching_updated_db.id]} in add_call_args
        assert all(non_matching_new_db.id not in kwargs["rule_ids"] for kwargs in add_call_args)

        send_call_args = [call.args for call in orchestrator.kafka.send_rules.await_args_list]
        assert (str(pipeline_id), [matching_new_db]) in send_call_args
        assert (str(pipeline_id), [matching_updated_db]) in send_call_args

    @pytest.mark.asyncio
    async def test_update_rules_in_pipelines_does_not_readd_disabled_matching_rule(self, orchestrator):
        repository_id = UUID("770e8400-e29b-41d4-a716-446655440000")
        pipeline_id = UUID("99999999-9999-9999-9999-999999999999")
        orchestrator.pipeline_repo.get_repository_selections_by_repository_id.return_value = {
            pipeline_id: ["_product/windows"]
        }

        updated_rule = MagicMock(id="33333333-3333-3333-3333-333333333333")
        disabled_existing_rule = self._rule(updated_rule.id, folder_path="_product/windows")

        orchestrator.rule_repo.get_by_ids.return_value = [disabled_existing_rule]
        orchestrator.pipeline_rule_repo.get_enabled_rules_ids.return_value = []
        orchestrator.pipeline_rule_repo.get_rule_ids.return_value = [disabled_existing_rule.id]

        await orchestrator._update_rules_in_pipelines_and_kafka(
            repo_id=repository_id,
            new_rules=[],
            deleted_rule_ids=[],
            updated_rules=[updated_rule],
        )

        orchestrator._add_rules_to_pipelines.assert_not_called()
        orchestrator.kafka.send_rules.assert_not_called()


# ==========================================
# FILTERS MANAGER TESTS
# ==========================================


class TestFiltersManager:
    """Tests for FiltersManager."""

    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()

    @pytest.fixture
    def mock_filter_dao(self):
        """Create mock FilterDAO."""
        return AsyncMock()

    @pytest.fixture
    def mock_activity_producer(self):
        """Create mock activity producer."""
        mock = AsyncMock()
        mock.log_action = AsyncMock()
        return mock

    @pytest.fixture
    def sample_filter(self):
        """Create sample filter object."""
        filter_obj = MagicMock()
        filter_obj.id = UUID("880e8400-e29b-41d4-a716-446655440000")
        filter_obj.name = "Test Filter"
        filter_obj.is_active = True
        filter_obj.conditions = {"field": "value"}
        filter_obj.created = datetime.now()
        filter_obj.updated = datetime.now()
        return filter_obj

    @pytest.fixture
    def admin_user(self):
        """Create admin user for activity logging."""
        user = MagicMock()
        user.id = UUID("660e8400-e29b-41d4-a716-446655440000")
        user.email = "admin@example.com"
        user.full_name = "Admin User"
        return user

    @pytest.mark.asyncio
    async def test_get_by_id_success(self, mock_db, mock_filter_dao, sample_filter):
        """Should return filter when found."""
        mock_filter_dao.get_by_id.return_value = sample_filter

        with patch("apps.managers.filters.FilterDAO", return_value=mock_filter_dao):
            from apps.managers.filters import FiltersManager

            manager = FiltersManager(mock_db)
            manager.filter_repo = mock_filter_dao

            result = await manager.get_by_id(sample_filter.id)

            assert result == sample_filter

    @pytest.mark.asyncio
    async def test_get_by_id_not_found(self, mock_db, mock_filter_dao):
        """Should raise NotFoundError when filter not found."""
        mock_filter_dao.get_by_id.return_value = None
        filter_id = UUID("880e8400-e29b-41d4-a716-446655440000")

        with patch("apps.managers.filters.FilterDAO", return_value=mock_filter_dao):
            from apps.managers.filters import FiltersManager

            manager = FiltersManager(mock_db)
            manager.filter_repo = mock_filter_dao

            with pytest.raises(NotFoundError) as exc_info:
                await manager.get_by_id(filter_id)

            assert str(filter_id) in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_get_active_filters(self, mock_db, mock_filter_dao, sample_filter):
        """Should return only active filters."""
        mock_filter_dao.get_active.return_value = [sample_filter]

        with patch("apps.managers.filters.FilterDAO", return_value=mock_filter_dao):
            from apps.managers.filters import FiltersManager

            manager = FiltersManager(mock_db)
            manager.filter_repo = mock_filter_dao

            result = await manager.get_active()

            assert len(result) == 1
            assert result[0].is_active is True

    @pytest.mark.asyncio
    async def test_delete_success(self, mock_db, mock_filter_dao, mock_activity_producer, sample_filter, admin_user):
        """Should delete filter and log activity."""
        mock_filter_dao.get_by_id.return_value = sample_filter
        mock_filter_dao.delete.return_value = True

        mock_filters_orchestrator = MagicMock()
        mock_filters_orchestrator.handle_filter_deletion = AsyncMock()

        mock_pipeline_dao = MagicMock()
        mock_pipeline_dao.get_by_filter_id = AsyncMock(return_value=[])

        with (
            patch("apps.managers.filters.FilterDAO", return_value=mock_filter_dao),
            patch("apps.managers.filters.PipelineDAO", return_value=mock_pipeline_dao),
            patch("apps.managers.filters.activity_producer", mock_activity_producer),
            patch("apps.managers.filters.FiltersOrchestrator", return_value=mock_filters_orchestrator),
        ):
            from apps.managers.filters import FiltersManager

            manager = FiltersManager(mock_db)
            manager.filter_repo = mock_filter_dao

            result = await manager.delete(sample_filter.id, admin_user)

            assert result is True
            mock_filter_dao.delete.assert_called_once_with(sample_filter.id)
            mock_activity_producer.log_action.assert_called_once()
