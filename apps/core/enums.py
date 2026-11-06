from enum import StrEnum


class UserRole(StrEnum):
    """Access role of a user account.

    Members:
    - ADMIN ("admin"): full administrative access (manage users, integrations, all entities).
    - USER ("user"): standard non-administrative access (read + write).
    - VIEWER ("viewer"): read-only access (GET endpoints only).
    """

    ADMIN = "admin"
    USER = "user"
    VIEWER = "viewer"


class PipelineSortField(StrEnum):
    """Allowed sort fields for the pipeline list endpoint.

    Members (column sorted on):
    - NAME ("name"): pipeline name.
    - SOURCE_TOPICS ("source_topics"): source Kafka topics.
    - DESTINATION_TOPIC ("destination_topic"): destination Kafka topic.
    - LOG_SOURCE ("log_source"): associated log source.
    - FILTERS ("filters"): number of filters.
    - RULES ("rules"): number of rules.
    - EVENTS_TAGGED ("events_tagged"): count of tagged (matched) events.
    - EVENTS_UNTAGGED ("events_untagged"): count of untagged events.
    - CREATED ("created"): creation time.
    - ENABLED ("enabled"): enabled flag.
    - STATUS ("status"): runtime status.
    """

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


class ActiveThreatSortField(StrEnum):
    """Allowed sort fields for the active threats list endpoint.

    Members (column sorted on):
    - RELEASE_DATE ("release_date"): TDM release time (`tdm_created`, falling back to local `created`).
    - NAME ("name"): active threat display name.
    - SEVERITY ("severity"): severity label.
    - ENABLED ("enabled"): legacy alias of BLOCKED with the direction inverted, so the frontend's
      existing "enabled" column keeps its meaning after D8 replaced the flag.
    - BLOCKED ("blocked"): platform-wide block flag (`active_threats.is_blocked`).
    """

    RELEASE_DATE = "release_date"
    NAME = "name"
    SEVERITY = "severity"
    ENABLED = "enabled"
    BLOCKED = "blocked"


class AttackChainSortField(StrEnum):
    """Allowed sort fields for the attack chains list endpoint.

    Members (column sorted on):
    - LAST_SEEN ("last_seen"): timestamp of the latest event in the chain (default).
    - FIRST_SEEN ("first_seen"): timestamp of the earliest event in the chain.
    - CREATED_AT ("created_at"): when the chain row was created.
    - UPDATED_AT ("updated_at"): when the chain row was last updated.
    - WINDOW_START ("window_start"): correlation window start time.
    - SEVERITY ("severity"): linked active threat severity.
    - HOSTNAME ("hostname"): hostname the chain was correlated on.
    - STATUS ("status"): workflow status.
    - EVENT_COUNT ("event_count"): number of events in the chain.
    - RULE_COUNT ("rule_count"): number of distinct matched rules.
    - COVERAGE ("coverage"): chain coverage metric.
    - CHAIN_TYPE ("chain_type"): correlation chain type.
    - REVISION ("revision"): chain revision.
    """

    LAST_SEEN = "last_seen"
    FIRST_SEEN = "first_seen"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    WINDOW_START = "window_start"
    SEVERITY = "severity"
    HOSTNAME = "hostname"
    STATUS = "status"
    EVENT_COUNT = "event_count"
    RULE_COUNT = "rule_count"
    COVERAGE = "coverage"
    CHAIN_TYPE = "chain_type"
    REVISION = "revision"


class CorrelationPipelineSortField(StrEnum):
    """Allowed sort fields for the correlation pipelines list endpoint.

    Members (column sorted on):
    - NAME ("name"): correlation pipeline display name.
    - CREATED_AT ("created_at"): when the correlation pipeline was created.
    - UPDATED_AT ("updated_at"): when the correlation pipeline was last updated.
    - JOBS_STATUS ("jobs_status"): desired control state of both Flink jobs.
    - ATTACK_CHAINS ("attack_chains"): number of attack chains attributed to the pipeline.
    """

    NAME = "name"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    JOBS_STATUS = "jobs_status"
    ATTACK_CHAINS = "attack_chains"


class CorrelationPipelineStatus(StrEnum):
    """Observed composite status of one correlation pipeline's correlation-job + agent-job pair.

    Derived at read time from the row's desired state (``correlation_pipelines.jobs_status``) and
    the two observed Kubernetes deployment states — it is NOT a column. The DB CHECK still admits
    only ``starting`` / ``stopping`` / ``stopped``; ``running`` / ``partial`` / ``failed`` are
    never persisted.

    ``stopped`` renders as **"Disabled"** in the UI. That is a label, not a seventh state — do not
    add a ``disabled`` member.

    Members:
    - RUNNING ("running"): both jobs observed running in Kubernetes.
    - STARTING ("starting"): desired running; at least one job not running yet, none failed.
    - STOPPING ("stopping"): desired stopped; at least one deployment still present.
    - STOPPED ("stopped"): desired stopped; both deployments gone. UI label: "Disabled".
    - PARTIAL ("partial"): desired running; exactly one half failed (the healthy half stays up).
    - FAILED ("failed"): desired running; both halves failed.
    """

    RUNNING = "running"
    STARTING = "starting"
    STOPPING = "stopping"
    STOPPED = "stopped"
    PARTIAL = "partial"
    FAILED = "failed"


class PipelineRuleSortField(StrEnum):
    """Allowed sort fields for the pipeline rules endpoint.

    Members (column sorted on):
    - NAME ("name"): rule name.
    - REPOSITORY ("repository"): source repository.
    - ENABLED ("enabled"): enabled flag.
    - TAGGED_EVENTS ("tagged_events"): count of events the rule tagged.
    - CREATED ("created"): creation time.
    - UPDATED ("updated"): last-update time.
    """

    NAME = "name"
    REPOSITORY = "repository"
    ENABLED = "enabled"
    TAGGED_EVENTS = "tagged_events"
    CREATED = "created"
    UPDATED = "updated"


class RuleSearchField(StrEnum):
    """Rule fields the free-text search of the rules list endpoint may target.

    Members (column searched):
    - NAME ("name"): rule title.
    - PRODUCT ("product"): Sigma logsource product.
    - SERVICE ("service"): Sigma logsource service.
    - CATEGORY ("category"): Sigma logsource category.
    - SIGMA_UUID ("sigma_uuid"): the `id` declared inside the Sigma body ("By ID" search mode).
      A whole UUID matches exactly, a shorter hex fragment matches any UUID containing it (dashes
      ignored), and a term that cannot be part of a UUID returns no rows.
    """

    NAME = "name"
    PRODUCT = "product"
    SERVICE = "service"
    CATEGORY = "category"
    SIGMA_UUID = "sigma_uuid"


class HealthCheckStatus(StrEnum):
    """Outcome of a single platform health check.

    Members:
    - OPERATIONAL ("Operational"): the checked dependency is healthy.
    - WARNING ("Warning"): reachable but degraded or partially functional.
    - ERROR ("Error"): unreachable or failing.
    - NOT_ENABLED ("Not Enabled"): the check is configured off / not applicable.
    """

    OPERATIONAL = "Operational"
    WARNING = "Warning"
    ERROR = "Error"
    NOT_ENABLED = "Not Enabled"


class ChainsAnalysedErrorClass(StrEnum):
    """How a failed ``detectflow.chains-analysed`` apply is retried.

    Members:
    - INFRASTRUCTURE ("infrastructure"): the database/transport failed, not the record. Retry with
      capped backoff, never park — the dead-letter store is that same database.
    - DEFERRABLE ("deferrable"): may apply once catalogue sync catches up (missing high-order Sigma
      or Active Threat row). Retry until the budget or deadline is spent, then park and skip.
    - PERMANENT ("permanent"): can never apply. Park and skip on the first failure.
    """

    INFRASTRUCTURE = "infrastructure"
    DEFERRABLE = "deferrable"
    PERMANENT = "permanent"


class AuditSeverity(StrEnum):
    """Severity level attached to an audit-log (activity) entry.

    Members:
    - INFO ("info"): normal operation (login, logout, create, read, update, toggle, sync, status_change).
    - WARNING ("warning"): potentially sensitive or notable events (login_failed, delete, alert).
    - ERROR ("error"): failures requiring attention.

    The default severity per action type is in ``ACTION_SEVERITY_MAP``.
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# Mapping of action types to their default severity levels
ACTION_SEVERITY_MAP: dict[str, AuditSeverity] = {
    # Info level - normal operations
    "login": AuditSeverity.INFO,
    "logout": AuditSeverity.INFO,
    "create": AuditSeverity.INFO,
    "read": AuditSeverity.INFO,  # read-only actions (e.g. generating a View-in-SIEM deep link)
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


class IntegrationCategory(StrEnum):
    """Category an integration type belongs to (UI filter chips).

    Members:
    - SOC_PRIME ("soc_prime"): SOC Prime services (e.g. Threat Detection Marketplace).
    - SIEM ("siem"): SIEM platforms.
    - EDR ("edr"): endpoint detection and response platforms.
    - DATABASE ("database"): database/data-store targets.
    - LLM_FIREWALL ("llm_firewall"): LLM firewall/guardrail services.
    - GIT_HOSTING ("git_hosting"): Git hosting providers.
    - OTHER ("other"): anything that does not fit the above.
    """

    SOC_PRIME = "soc_prime"
    SIEM = "siem"
    EDR = "edr"
    DATABASE = "database"
    LLM_FIREWALL = "llm_firewall"
    GIT_HOSTING = "git_hosting"
    OTHER = "other"


class IntegrationStatus(StrEnum):
    """Connection status of a stored integration profile.

    Members:
    - CONNECTED ("connected"): last connection check succeeded.
    - DISCONNECTED ("disconnected"): not yet checked or intentionally disconnected.
    - ERROR ("error"): last connection check failed.
    """

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"


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
