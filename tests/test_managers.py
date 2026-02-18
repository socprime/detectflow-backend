"""Unit tests for managers (business logic layer)."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from apps.core.exceptions import DuplicateEntityError, NotFoundError
from apps.core.schemas import UserCreateRequest

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
            patch("apps.managers.users.hash_password", return_value="hashed_password"),
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
