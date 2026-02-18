"""Custom exceptions for the application.

These exceptions inherit from HTTPException for automatic handling by FastAPI,
but provide semantic meaning for different error types.
"""

from fastapi import HTTPException, status

# ==========================================
# 4xx Client Errors
# ==========================================


class BadRequestError(HTTPException):
    """400 Bad Request - Invalid request data or parameters."""

    def __init__(self, detail: str = "Bad request"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class ValidationError(HTTPException):
    """400 Bad Request - Validation error for request data."""

    def __init__(self, detail: str = "Validation error"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


class UnauthorizedError(HTTPException):
    """401 Unauthorized - Authentication required or failed."""

    def __init__(self, detail: str = "Not authenticated"):
        super().__init__(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)


class ForbiddenError(HTTPException):
    """403 Forbidden - Insufficient permissions."""

    def __init__(self, detail: str = "Access forbidden"):
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=detail)


class NotFoundError(HTTPException):
    """404 Not Found - Resource does not exist."""

    def __init__(self, detail: str = "Resource not found"):
        super().__init__(status_code=status.HTTP_404_NOT_FOUND, detail=detail)


class ConflictError(HTTPException):
    """409 Conflict - Resource state conflict (e.g., duplicate, in use)."""

    def __init__(self, detail: str = "Resource conflict"):
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=detail)


class UnprocessableEntityError(HTTPException):
    """422 Unprocessable Entity - Request valid but cannot be processed."""

    def __init__(self, detail: str = "Cannot process request"):
        super().__init__(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail)


# ==========================================
# 5xx Server Errors
# ==========================================


class ServiceUnavailableError(HTTPException):
    """503 Service Unavailable - External service is down."""

    def __init__(self, detail: str = "Service temporarily unavailable"):
        super().__init__(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=detail)


class BadGatewayError(HTTPException):
    """502 Bad Gateway - Error from upstream service."""

    def __init__(self, detail: str = "Bad gateway"):
        super().__init__(status_code=status.HTTP_502_BAD_GATEWAY, detail=detail)


# ==========================================
# Domain-Specific Errors
# ==========================================


class DuplicateEntityError(ConflictError):
    """Entity with this identifier already exists."""

    def __init__(self, entity_type: str, identifier: str):
        super().__init__(detail=f"{entity_type} with {identifier} already exists")


class EntityInUseError(ConflictError):
    """Entity cannot be deleted because it is referenced by other entities."""

    def __init__(self, entity_type: str, name: str, referenced_by: str = "other resources"):
        super().__init__(detail=f"Cannot delete {entity_type} '{name}': it is still used by {referenced_by}")


class RepositoryNotLocalError(BadRequestError):
    """Operation only allowed on local repositories."""

    def __init__(self, detail: str = "This operation is only allowed on local repositories"):
        super().__init__(detail=detail)


class ExternalServiceError(BadGatewayError):
    """Error communicating with external service."""

    def __init__(self, service: str, detail: str):
        super().__init__(detail=f"{service}: {detail}")


class ConfigurationError(BadRequestError):
    """Required configuration is missing or invalid."""

    def __init__(self, detail: str = "Required configuration is missing"):
        super().__init__(detail=detail)
