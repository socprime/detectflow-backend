"""Unit tests for module imports.

These tests verify that core modules can be imported without database connection.
"""


class TestCoreImports:
    """Tests for core module imports."""

    def test_import_settings(self, setup_test_env):
        """Should import Settings class."""
        from apps.core.settings import Settings

        assert Settings is not None

    def test_import_enums(self):
        """Should import enums."""
        from apps.core.enums import UserRole

        assert UserRole is not None
        assert UserRole.ADMIN is not None
        assert UserRole.USER is not None

    def test_import_exceptions(self):
        """Should import custom exceptions."""
        from apps.core.exceptions import ConflictError, NotFoundError, ValidationError

        assert NotFoundError is not None
        assert ValidationError is not None
        assert ConflictError is not None

    def test_import_pagination(self):
        """Should import pagination utilities."""
        from apps.core.pagination import calculate_skip

        assert calculate_skip is not None

    def test_import_schemas_validators(self):
        """Should import schema validators."""
        from apps.core.schemas import validate_uuid4, validate_yaml

        assert validate_yaml is not None
        assert validate_uuid4 is not None


class TestSchemaImports:
    """Tests for schema imports."""

    def test_import_pagination_params(self):
        """Should import PaginationParams."""
        from apps.core.schemas import PaginationParams

        assert PaginationParams is not None

    def test_import_error_schemas(self):
        """Should import error response schemas."""
        from apps.core.schemas import ErrorResponse, ValidationErrorResponse

        assert ErrorResponse is not None
        assert ValidationErrorResponse is not None

    def test_import_pipeline_schemas(self):
        """Should import pipeline schemas."""
        from apps.core.schemas import (
            PipelineCreateRequest,
            PipelineDetailResponse,
            PipelineListResponse,
        )

        assert PipelineCreateRequest is not None
        assert PipelineDetailResponse is not None
        assert PipelineListResponse is not None

    def test_import_user_schemas(self):
        """Should import user schemas."""
        from apps.core.schemas import (
            LoginRequest,
            LoginResponse,
            UserCreateRequest,
            UserDetailResponse,
        )

        assert LoginRequest is not None
        assert LoginResponse is not None
        assert UserCreateRequest is not None
        assert UserDetailResponse is not None


class TestModuleImports:
    """Tests for apps/modules imports."""

    def test_import_utils(self):
        """Should import utils module."""
        from apps.modules.utils import get_fields_from_sigma

        assert get_fields_from_sigma is not None


class TestModelImports:
    """Tests for model imports (without DB connection)."""

    def test_import_base_skipped(self, setup_test_env):
        """Base import requires DB driver, skip in unit tests.

        Note: Base import triggers database engine creation,
        which requires psycopg2. This is tested in integration tests.
        """
        # Just verify we can import SQLAlchemy declarative base
        from sqlalchemy.orm import DeclarativeBase

        assert DeclarativeBase is not None
