"""Logging configuration for Admin Panel Backend.

This module provides centralized logging configuration with consistent formatting
and structured logging support across the application.
"""

import logging
import sys

from apps.core.settings import settings
from apps.core.version import get_version


class VersionFilter(logging.Filter):
    """Add application version to all log records (like structlog bound context)."""

    def __init__(self) -> None:
        super().__init__()
        self.detectflow_backend_version = get_version()

    def filter(self, record: logging.LogRecord) -> bool:
        record.detectflow_backend_version = self.detectflow_backend_version
        return True


class HealthCheckFilter(logging.Filter):
    """Filter out health check requests from uvicorn access logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "/health" not in message


def setup_uvicorn_logging() -> None:
    """Configure uvicorn access logger to filter health checks."""
    uvicorn_access = logging.getLogger("uvicorn.access")
    uvicorn_access.addFilter(HealthCheckFilter())


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a configured logger instance.

    Creates or retrieves a logger with consistent formatting and configuration.
    The logger is configured with:
    - Console output to stdout
    - Timestamp, level, name, and message format
    - Configurable log level via LOG_LEVEL environment variable
    - Support for structured logging via extra fields

    Args:
        name: Logger name, typically __name__ of the calling module.
            If None, uses the logger module's __name__.

    Returns:
        logging.Logger: Configured logger instance ready for use.

    Example:
        >>> from apps.core.logger import get_logger
        >>> logger = get_logger(__name__)
        >>> logger.info("Application started")
        >>> logger.error("Database error", extra={"db": "postgres", "code": 500})
    """
    logger = logging.getLogger(name or __name__)

    # Only configure if no handlers exist (avoid duplicate logs)
    if not logger.handlers:
        # Get log level from settings
        log_level_str = settings.log_level.upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logger.setLevel(log_level)

        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)

        # Create formatter with timestamp, version, and structured format
        # Format: timestamp - detectflow_backend_version - level - name - message
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(detectflow_backend_version)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Add version filter (injects version into every log record)
        handler.addFilter(VersionFilter())

        # Add handler to logger
        logger.addHandler(handler)

        # Prevent propagation to root logger to avoid duplicate logs
        logger.propagate = False

    return logger
