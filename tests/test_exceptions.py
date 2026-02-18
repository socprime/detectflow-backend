"""Unit tests for apps/core/exceptions.py."""

import pytest
from fastapi import HTTPException

from apps.core.exceptions import (
    BadGatewayError,
    BadRequestError,
    ConfigurationError,
    ConflictError,
    DuplicateEntityError,
    EntityInUseError,
    ExternalServiceError,
    ForbiddenError,
    NotFoundError,
    RepositoryNotLocalError,
    ServiceUnavailableError,
    UnauthorizedError,
    UnprocessableEntityError,
    ValidationError,
)


class TestNotFoundError:
    """Tests for NotFoundError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = NotFoundError()
        assert error.status_code == 404
        assert error.detail == "Resource not found"

    def test_custom_message(self):
        """Should accept custom message."""
        error = NotFoundError(detail="Pipeline not found")
        assert error.status_code == 404
        assert error.detail == "Pipeline not found"

    def test_is_http_exception(self):
        """Should be HTTPException subclass."""
        error = NotFoundError()
        assert isinstance(error, HTTPException)


class TestValidationError:
    """Tests for ValidationError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = ValidationError()
        assert error.status_code == 400
        assert error.detail == "Validation error"

    def test_custom_message(self):
        """Should accept custom message."""
        error = ValidationError(detail="Invalid field format")
        assert error.status_code == 400
        assert error.detail == "Invalid field format"


class TestConflictError:
    """Tests for ConflictError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = ConflictError()
        assert error.status_code == 409
        assert error.detail == "Resource conflict"

    def test_custom_message(self):
        """Should accept custom message."""
        error = ConflictError(detail="Name already exists")
        assert error.status_code == 409
        assert error.detail == "Name already exists"


class TestBadRequestError:
    """Tests for BadRequestError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = BadRequestError()
        assert error.status_code == 400
        assert error.detail == "Bad request"

    def test_custom_message(self):
        """Should accept custom message."""
        error = BadRequestError(detail="Invalid parameter")
        assert error.status_code == 400
        assert error.detail == "Invalid parameter"


class TestUnauthorizedError:
    """Tests for UnauthorizedError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = UnauthorizedError()
        assert error.status_code == 401
        assert error.detail == "Not authenticated"

    def test_custom_message(self):
        """Should accept custom message."""
        error = UnauthorizedError(detail="Invalid token")
        assert error.status_code == 401
        assert error.detail == "Invalid token"


class TestForbiddenError:
    """Tests for ForbiddenError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = ForbiddenError()
        assert error.status_code == 403
        assert error.detail == "Access forbidden"

    def test_custom_message(self):
        """Should accept custom message."""
        error = ForbiddenError(detail="Admin access required")
        assert error.status_code == 403
        assert error.detail == "Admin access required"


class TestUnprocessableEntityError:
    """Tests for UnprocessableEntityError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = UnprocessableEntityError()
        assert error.status_code == 422
        assert error.detail == "Cannot process request"

    def test_custom_message(self):
        """Should accept custom message."""
        error = UnprocessableEntityError(detail="Invalid YAML")
        assert error.status_code == 422
        assert error.detail == "Invalid YAML"


class TestServiceUnavailableError:
    """Tests for ServiceUnavailableError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = ServiceUnavailableError()
        assert error.status_code == 503
        assert error.detail == "Service temporarily unavailable"

    def test_custom_message(self):
        """Should accept custom message."""
        error = ServiceUnavailableError(detail="Database down")
        assert error.status_code == 503
        assert error.detail == "Database down"


class TestBadGatewayError:
    """Tests for BadGatewayError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = BadGatewayError()
        assert error.status_code == 502
        assert error.detail == "Bad gateway"

    def test_custom_message(self):
        """Should accept custom message."""
        error = BadGatewayError(detail="Upstream error")
        assert error.status_code == 502
        assert error.detail == "Upstream error"


class TestDuplicateEntityError:
    """Tests for DuplicateEntityError exception."""

    def test_message_format(self):
        """Should format message with entity type and identifier."""
        error = DuplicateEntityError("User", "email test@example.com")
        assert error.status_code == 409
        assert error.detail == "User with email test@example.com already exists"

    def test_inherits_from_conflict(self):
        """Should be ConflictError subclass."""
        error = DuplicateEntityError("Pipeline", "name test-pipeline")
        assert isinstance(error, ConflictError)


class TestEntityInUseError:
    """Tests for EntityInUseError exception."""

    def test_message_format(self):
        """Should format message with entity type and name."""
        error = EntityInUseError("Filter", "my-filter", "pipelines")
        assert error.status_code == 409
        assert error.detail == "Cannot delete Filter 'my-filter': it is still used by pipelines"

    def test_default_referenced_by(self):
        """Should use default referenced_by value."""
        error = EntityInUseError("Repository", "test-repo")
        assert "other resources" in error.detail

    def test_inherits_from_conflict(self):
        """Should be ConflictError subclass."""
        error = EntityInUseError("Filter", "test")
        assert isinstance(error, ConflictError)


class TestRepositoryNotLocalError:
    """Tests for RepositoryNotLocalError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = RepositoryNotLocalError()
        assert error.status_code == 400
        assert "local repositories" in error.detail

    def test_custom_message(self):
        """Should accept custom message."""
        error = RepositoryNotLocalError(detail="Cannot modify API repository")
        assert error.status_code == 400
        assert error.detail == "Cannot modify API repository"

    def test_inherits_from_bad_request(self):
        """Should be BadRequestError subclass."""
        error = RepositoryNotLocalError()
        assert isinstance(error, BadRequestError)


class TestConfigurationError:
    """Tests for ConfigurationError exception."""

    def test_default_message(self):
        """Should have default message."""
        error = ConfigurationError()
        assert error.status_code == 400
        assert error.detail == "Required configuration is missing"

    def test_custom_message(self):
        """Should accept custom message."""
        error = ConfigurationError(detail="API key not configured")
        assert error.status_code == 400
        assert error.detail == "API key not configured"

    def test_inherits_from_bad_request(self):
        """Should be BadRequestError subclass."""
        error = ConfigurationError()
        assert isinstance(error, BadRequestError)


class TestExternalServiceError:
    """Tests for ExternalServiceError exception."""

    def test_message_format(self):
        """Should format message with service name and detail."""
        error = ExternalServiceError("SOCPrime API", "Connection timeout")
        assert error.status_code == 502
        assert error.detail == "SOCPrime API: Connection timeout"

    def test_inherits_from_bad_gateway(self):
        """Should be BadGatewayError subclass."""
        error = ExternalServiceError("Kafka", "Broker unavailable")
        assert isinstance(error, BadGatewayError)


class TestExceptionRaising:
    """Tests for raising exceptions."""

    def test_raise_not_found(self):
        """NotFoundError should be raisable."""
        with pytest.raises(NotFoundError) as exc_info:
            raise NotFoundError("Test not found")
        assert exc_info.value.status_code == 404

    def test_raise_validation(self):
        """ValidationError should be raisable."""
        with pytest.raises(ValidationError) as exc_info:
            raise ValidationError("Test validation")
        assert exc_info.value.status_code == 400

    def test_raise_conflict(self):
        """ConflictError should be raisable."""
        with pytest.raises(ConflictError) as exc_info:
            raise ConflictError("Test conflict")
        assert exc_info.value.status_code == 409

    def test_raise_duplicate_entity(self):
        """DuplicateEntityError should be raisable and catchable as ConflictError."""
        with pytest.raises(ConflictError):
            raise DuplicateEntityError("User", "email test@test.com")

    def test_raise_external_service(self):
        """ExternalServiceError should be raisable and catchable as BadGatewayError."""
        with pytest.raises(BadGatewayError):
            raise ExternalServiceError("API", "error")


class TestExceptionHierarchy:
    """Tests for exception inheritance hierarchy."""

    def test_all_inherit_from_http_exception(self):
        """All exceptions should inherit from HTTPException."""
        exceptions = [
            NotFoundError(),
            ValidationError(),
            BadRequestError(),
            UnauthorizedError(),
            ForbiddenError(),
            ConflictError(),
            UnprocessableEntityError(),
            ServiceUnavailableError(),
            BadGatewayError(),
            DuplicateEntityError("Type", "id"),
            EntityInUseError("Type", "name"),
            RepositoryNotLocalError(),
            ConfigurationError(),
            ExternalServiceError("Service", "error"),
        ]
        for exc in exceptions:
            assert isinstance(exc, HTTPException), f"{type(exc).__name__} should inherit from HTTPException"
