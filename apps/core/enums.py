from enum import StrEnum


class UserRole(StrEnum):
    ADMIN = "admin"
    USER = "user"


class PipelineSortField(StrEnum):
    """Allowed sort fields for pipeline list endpoint."""

    NAME = "name"
    SOURCE_TOPICS = "source_topics"
    DESTINATION_TOPIC = "destination_topic"
    LOG_SOURCE = "log_source"
    FILTERS = "filters"
    RULES = "rules"
    EVENTS_TAGGED = "events_tagged"
    EVENTS_UNTAGGED = "events_untagged"
    CREATED = "created"
    ENABLED = "enabled"
    STATUS = "status"


class PipelineRuleSortField(StrEnum):
    """Allowed sort fields for pipeline rules endpoint."""

    NAME = "name"
    REPOSITORY = "repository"
    ENABLED = "enabled"
    TAGGED_EVENTS = "tagged_events"
    CREATED = "created"
    UPDATED = "updated"


class HealthCheckStatus(StrEnum):
    """Status of a health check result: Operational, Warning, Error, Not Enabled."""

    OPERATIONAL = "Operational"
    WARNING = "Warning"
    ERROR = "Error"
    NOT_ENABLED = "Not Enabled"


class AuditSeverity(StrEnum):
    """Severity levels for audit log entries."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# Mapping of action types to their default severity levels
ACTION_SEVERITY_MAP: dict[str, AuditSeverity] = {
    # Info level - normal operations
    "login": AuditSeverity.INFO,
    "logout": AuditSeverity.INFO,
    "create": AuditSeverity.INFO,
    "update": AuditSeverity.INFO,
    "toggle": AuditSeverity.INFO,
    "sync": AuditSeverity.INFO,
    "status_change": AuditSeverity.INFO,
    # Warning level - potentially sensitive operations
    "login_failed": AuditSeverity.WARNING,
    "delete": AuditSeverity.WARNING,
    "alert": AuditSeverity.WARNING,  # system alerts (backpressure, resource issues, etc.)
    # Error level
    "error": AuditSeverity.ERROR,
}


def get_severity_for_action(action: str) -> AuditSeverity:
    """Get severity level for a given action.

    Args:
        action: The action type string.

    Returns:
        AuditSeverity level. Defaults to INFO for unknown actions.
    """
    return ACTION_SEVERITY_MAP.get(action, AuditSeverity.INFO)


# =============================================================================
# Pipeline Status Severity Mapping
# =============================================================================

# Pipeline status → severity level mapping
# See CLAUDE.md for full status documentation
STATUS_LEVEL_MAP: dict[str, str] = {
    # Error level - action required
    "failed": "error",
    # Warning level - needs attention
    "not_found": "warning",
    "unknown": "warning",
    "rolled_back": "warning",
    # Info level - normal operation (default for all others)
    "running": "info",
    "disabled": "info",
    "created": "info",
    "reconciling": "info",
    "deploying": "info",
    "upgrading": "info",
    "suspended": "info",
    "restarting": "info",
    "cancelling": "info",
    "stable": "info",
}


def get_status_level(
    status: str,
    has_warnings: bool = False,
    has_error: bool = False,
) -> str:
    """Get severity level for pipeline status.

    Severity levels:
    - info: Normal operation (deploying, reconciling without issues)
    - warning: Needs attention (scheduling issues, node affinity problems)
    - error: Action required (failed, error states)

    Args:
        status: Pipeline status string
        has_warnings: Whether there are K8s warnings (scheduling, node affinity)
        has_error: Whether there's an error condition (job_manager_status=ERROR)

    Returns:
        Severity level: "info", "warning", or "error"
    """
    # Error conditions override status-based level
    if has_error:
        return "error"
    # Warnings override to warning level
    if has_warnings:
        return "warning"
    # Use status-based mapping, default to info
    return STATUS_LEVEL_MAP.get(status, "info")
