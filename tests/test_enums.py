"""Unit tests for apps/core/enums.py."""

from apps.core.enums import PipelineRuleSortField, RuleSearchField, UserRole


class TestUserRole:
    """Tests for UserRole enum."""

    def test_admin_value(self):
        """Admin role should have value 'admin'."""
        assert UserRole.ADMIN.value == "admin"
        assert UserRole.ADMIN == "admin"

    def test_user_value(self):
        """User role should have value 'user'."""
        assert UserRole.USER.value == "user"
        assert UserRole.USER == "user"

    def test_viewer_value(self):
        """Viewer role should have value 'viewer'."""
        assert UserRole.VIEWER.value == "viewer"
        assert UserRole.VIEWER == "viewer"

    def test_enum_members(self):
        """Enum should have exactly 3 members."""
        members = list(UserRole)
        assert len(members) == 3
        assert UserRole.ADMIN in members
        assert UserRole.USER in members
        assert UserRole.VIEWER in members

    def test_string_comparison(self):
        """UserRole should be comparable with strings."""
        assert UserRole.ADMIN == "admin"
        assert UserRole.USER == "user"
        assert UserRole.VIEWER == "viewer"

    def test_string_representation(self):
        """UserRole value should be accessible as string."""
        assert UserRole.ADMIN.value == "admin"
        assert UserRole.USER.value == "user"
        assert UserRole.VIEWER.value == "viewer"


class TestPipelineRuleSortField:
    """Tests for PipelineRuleSortField enum."""

    def test_all_values(self):
        """All sort fields should have correct values."""
        assert PipelineRuleSortField.NAME.value == "name"
        assert PipelineRuleSortField.REPOSITORY.value == "repository"
        assert PipelineRuleSortField.ENABLED.value == "enabled"
        assert PipelineRuleSortField.TAGGED_EVENTS.value == "tagged_events"
        assert PipelineRuleSortField.CREATED.value == "created"
        assert PipelineRuleSortField.UPDATED.value == "updated"

    def test_enum_members_count(self):
        """Enum should have exactly 6 members."""
        members = list(PipelineRuleSortField)
        assert len(members) == 6

    def test_string_comparison(self):
        """PipelineRuleSortField should be comparable with strings."""
        assert PipelineRuleSortField.NAME == "name"
        assert PipelineRuleSortField.REPOSITORY == "repository"


class TestRuleSearchField:
    """Tests for RuleSearchField enum."""

    def test_all_values(self):
        """All search fields should have correct values."""
        assert RuleSearchField.NAME.value == "name"
        assert RuleSearchField.PRODUCT.value == "product"
        assert RuleSearchField.SERVICE.value == "service"
        assert RuleSearchField.CATEGORY.value == "category"
        assert RuleSearchField.SIGMA_UUID.value == "sigma_uuid"

    def test_enum_members_count(self):
        """Enum should have exactly 5 members."""
        members = list(RuleSearchField)
        assert len(members) == 5

    def test_string_comparison(self):
        """RuleSearchField should be comparable with strings."""
        assert RuleSearchField.NAME == "name"
        assert RuleSearchField.SIGMA_UUID == "sigma_uuid"
