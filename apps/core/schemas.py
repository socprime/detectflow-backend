import datetime
import hashlib
import json
import re
from collections.abc import Sequence
from typing import Any, Literal, Self
from uuid import UUID

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from apps.core.enums import AuditSeverity, CorrelationPipelineStatus, HealthCheckStatus, UserRole
from apps.core.settings import settings

# Prefixes of generated, per-entity topics (``<prefix>.<uuid>`` — see ``apps.core.constants``).
# A user-chosen topic must not collide with one, or a detection pipeline could be pointed at a
# correlation pipeline's private topic.
RESERVED_TOPIC_PREFIXES: tuple[str, ...] = (
    settings.kafka_tagged_events_topic,
    settings.kafka_chains_raw_topic,
    settings.kafka_chains_analysed_topic,
    settings.kafka_chains_config_topic,
)

# Pipeline display names plus glob metacharacters. Spaces are legal (``Customer A``).
# ``|`` ``\`` ``{`` ``}`` are rejected so an operator cannot treat this as a regex.
_AUTO_ADD_PIPELINE_NAME_PATTERN_CHARSET = re.compile(r"^[^\\|{}\x00-\x1f\x7f]+$")


def get_reserved_topics() -> set[str]:
    """Get set of reserved system topics that cannot be used by pipelines."""
    return {
        settings.kafka_sigma_rules_topic,  # detectflow.sigma-rules
        settings.kafka_active_threats_topic,  # detectflow.active-threats
        settings.kafka_hos_rules_topic,  # detectflow.hos-rules
        settings.kafka_tagged_events_topic,  # detectflow.tagged-events
        settings.kafka_activity_topic,  # detectflow.activity
        settings.kafka_metrics_topic,  # detectflow.rule-metrics
        settings.kafka_chains_config_topic,
        settings.kafka_chains_raw_topic,
        settings.kafka_chains_analysed_topic,
    }


def validate_topic_not_reserved(topic: str, field_name: str) -> None:
    """Validate that a topic is neither a reserved system topic nor under a reserved prefix.

    Args:
        topic: Topic name supplied by the user.
        field_name: Field label used in the error message.

    Raises:
        ValueError: If the topic is reserved, or sits under a prefix this platform generates
            per-pipeline topics from (``detectflow.tagged-events.<uuid>`` and the three
            per-correlation-pipeline chains topics).
    """
    reserved = get_reserved_topics()
    if topic in reserved:
        raise ValueError(
            f"{field_name} '{topic}' is a reserved system topic. Reserved topics: {', '.join(sorted(reserved))}"
        )
    for prefix in RESERVED_TOPIC_PREFIXES:
        if topic.startswith(f"{prefix}."):
            raise ValueError(
                f"{field_name} '{topic}' uses reserved prefix '{prefix}.', under which the platform generates "
                f"per-pipeline topics. Reserved prefixes: {', '.join(sorted(RESERVED_TOPIC_PREFIXES))}"
            )


def validate_threshold_pct(v: int | None) -> int | None:
    """Validate a coverage threshold: 50–100 and a multiple of 5 (mirrors the DB CHECK)."""
    if v is None:
        return v
    if v < 50 or v > 100 or v % 5 != 0:
        raise ValueError("threshold_pct must be between 50 and 100 and a multiple of 5")
    return v


def validate_correlation_window_seconds(v: int | None) -> int | None:
    """Validate a correlation window: 30–10800 seconds (mirrors the DB CHECK). None = inherit."""
    if v is None:
        return v
    if v < 30 or v > 10800:
        raise ValueError("correlation_window_seconds must be between 30 and 10800")
    return v


def validate_auto_add_pipeline_name_pattern(value: str | None) -> str | None:
    """Normalise and validate a correlation pipeline's auto-add detection-pipeline name pattern.

    The value is a shell-style **glob** (``fnmatch``), not a regex: operators type ``cust1-*``,
    and an unanchored regex would silently match substrings and can backtrack pathologically.

    Args:
        value: Raw pattern from the request.

    Returns:
        The stripped pattern, or None when empty/blank (meaning "match every new pipeline name").

    Raises:
        ValueError: If the pattern is longer than 255 chars or contains control / regex characters.
    """
    if value is None:
        return None
    pattern = value.strip()
    if not pattern:
        return None
    if len(pattern) > 255:
        raise ValueError("auto_add_pipeline_name_pattern must be at most 255 characters")
    if not _AUTO_ADD_PIPELINE_NAME_PATTERN_CHARSET.match(pattern):
        raise ValueError(
            "auto_add_pipeline_name_pattern must be a glob over a detection pipeline's name "
            "(printable characters plus '*?[]!'); '|', '\\', '{' and '}' are not allowed"
        )
    return pattern


def validate_yaml(value: str | None) -> str | None:
    """Validate YAML format.

    Args:
        value: YAML string to validate (can be None).

    Returns:
        Valid YAML string or None.

    Raises:
        ValueError: If value is not valid YAML.
    """
    if value is None:
        return value
    try:
        yaml.safe_load(value)
        return value
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML format: {e}") from e


def validate_uuid4(value: str | UUID | None) -> str | None:
    """Validate UUID4 format.

    Args:
        value: Value to validate (can be None, str or UUID object).

    Returns:
        Valid value as string or None.

    Raises:
        ValueError: If value is not a valid UUID4.
    """
    if value is None:
        return value
    # If already a UUID object, convert to string
    if isinstance(value, UUID):
        return str(value)
    # If string, validate format
    try:
        UUID(value)
        return value
    except (ValueError, AttributeError, TypeError) as e:
        raise ValueError(f"Invalid UUID format: {value}") from e


# =============================================================================
# Pipeline output destinations (match-node contract)
# =============================================================================
# A pipeline fans its output out to 1..MAX_OUTPUT_DESTINATIONS Kafka topics. The canonical
# in-process shape is a list of plain dicts — that is what the ``pipelines.destinations``
# JSONB column stores and what is injected into the match node as
# ``KAFKA_OUTPUT_DESTINATIONS`` (see ``FlinkDeploymentConfig.kafka_output_destinations_json``).
# The singular ``destination_topic`` / ``save_untagged`` / ``apply_parser_to_output_events``
# columns are kept as a mirror of ``destinations[0]`` for older API clients and for sorting.
MAX_OUTPUT_DESTINATIONS = 10
OutputMode = Literal["matched_only", "all_events", "unmatched_only"]
Severity = Literal["informational", "low", "medium", "high", "critical"]

# Which events each output mode accepts, as (matched, unmatched) — mirrors
# ``OUTPUT_MODE_ACCEPTS`` in the match node (detectflow-matchnode ``app/config/settings.py``),
# which is what actually routes the events. Used here for dashboard EPS attribution.
OUTPUT_MODE_ACCEPTS: dict[str, tuple[bool, bool]] = {
    "all_events": (True, True),
    "matched_only": (True, False),
    "unmatched_only": (False, True),
}


def destination_accepts(output_mode: str, matched: bool) -> bool:
    """True when a destination in ``output_mode`` receives (un)matched events."""
    accepts_matched, accepts_unmatched = OUTPUT_MODE_ACCEPTS.get(output_mode, (True, False))
    return accepts_matched if matched else accepts_unmatched


class PipelineOutputDestination(BaseModel):
    """One Kafka output destination for a match-node pipeline."""

    topic: str = Field(min_length=1, description="Kafka destination topic (unique within the pipeline)")
    output_mode: OutputMode = Field(
        default="matched_only",
        description=(
            'Output mode: "matched_only" (detections only), "all_events" (matched + unmatched), '
            'or "unmatched_only" (only events that matched no rule — all_events minus the detections)'
        ),
    )
    apply_parser_to_output_events: bool = Field(
        default=False,
        description="Whether to apply the log-source parser to events written to this destination",
    )
    severities: list[Severity] = Field(
        default_factory=list,
        description=(
            "matched_only only: Sigma levels a detection must have (any matching rule) to reach this "
            "destination; empty = all. Cleared for all_events and unmatched_only, which always receive "
            "every event their mode accepts."
        ),
    )

    @field_validator("topic")
    @classmethod
    def strip_topic(cls, v: str) -> str:
        topic = v.strip()
        if not topic:
            raise ValueError("Empty topic names are not allowed")
        return topic

    @field_validator("severities")
    @classmethod
    def dedupe_severities(cls, v: list[str]) -> list[str]:
        # Canonical order, so resubmitting the same selection is not a change (no restart).
        return [s for s in Severity.__args__ if s in v]

    @model_validator(mode="after")
    def severities_only_for_matched_only(self) -> Self:
        # Mirrors the match node, which ignores severities outside matched_only.
        if self.output_mode != "matched_only":
            self.severities = []
        return self


def destinations_from_legacy(
    destination_topic: str,
    save_untagged: bool = False,
    apply_parser_to_output_events: bool = False,
) -> list[dict[str, Any]]:
    """Synthesize a single-item destinations list from legacy singular fields."""
    return [
        {
            "topic": destination_topic,
            "output_mode": "all_events" if save_untagged else "matched_only",
            "apply_parser_to_output_events": bool(apply_parser_to_output_events),
        }
    ]


def legacy_fields_from_destinations(destinations: list[dict[str, Any]]) -> tuple[str, bool, bool]:
    """Mirror the first destination into the legacy singular columns.

    ``save_untagged`` means "unmatched events reach this topic", so it is True for both
    ``all_events`` and ``unmatched_only``. The legacy field cannot express three modes — a
    legacy client that writes it back gets ``all_events``/``matched_only``, which is what it
    asked for.

    Returns:
        (destination_topic, save_untagged, apply_parser_to_output_events); empty/False for
        an empty list.
    """
    if not destinations:
        return "", False, False
    first = destinations[0]
    return (
        first["topic"],
        destination_accepts(first.get("output_mode", "matched_only"), matched=False),
        bool(first.get("apply_parser_to_output_events", False)),
    )


def normalize_destinations(
    destinations: Sequence[PipelineOutputDestination | dict[str, Any]],
    source_topics: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate destinations and return them as plain dicts (DB / Kafka env shape).

    Args:
        destinations: Destination models or dicts, in output order.
        source_topics: Source topics to check for feedback loops, when known.

    Returns:
        Normalized destination dicts, order preserved.

    Raises:
        ValueError / PydanticCustomError: On an empty or oversized list, a duplicate,
            reserved, or source-overlapping topic.
    """
    normalized = [
        item.model_dump() if isinstance(item, PipelineOutputDestination) else _destination_dict(item)
        for item in destinations
    ]
    if not normalized:
        raise ValueError("At least one destination is required")
    if len(normalized) > MAX_OUTPUT_DESTINATIONS:
        raise ValueError(f"At most {MAX_OUTPUT_DESTINATIONS} destinations are allowed")

    seen: set[str] = set()
    sources = set(source_topics or ())
    for dest in normalized:
        topic = dest["topic"]
        if topic in seen:
            raise ValueError(f"Duplicate destination topic '{topic}'")
        seen.add(topic)
        validate_topic_not_reserved(topic, "Destination topic")
        if topic in sources:
            raise PydanticCustomError(
                "topic_conflict",
                f"Destination topic '{topic}' cannot be the same as a source topic. "
                "This would create an infinite loop.",
            )
    return normalized


def _destination_dict(item: dict[str, Any]) -> dict[str, Any]:
    return PipelineOutputDestination.model_validate(item).model_dump()


def pipeline_destinations(pipeline: Any) -> list[dict[str, Any]]:
    """Read a pipeline's destinations, synthesizing one from the legacy columns when empty.

    Accepts anything with the pipeline attributes: an ORM ``Pipeline``, a partial ``Row``,
    or a stub. Rows written before the destinations column existed are covered by the
    Alembic backfill; this fallback covers rows loaded without it.
    """
    raw = getattr(pipeline, "destinations", None)
    if isinstance(raw, list) and raw:
        return [_destination_dict(item) for item in raw]
    topic = getattr(pipeline, "destination_topic", None)
    if not topic:
        return []
    return destinations_from_legacy(
        topic,
        bool(getattr(pipeline, "save_untagged", False)),
        bool(getattr(pipeline, "apply_parser_to_output_events", False)),
    )


def destination_topics_from_pipeline(pipeline: Any) -> list[str]:
    """Collect a pipeline's destination topic names, order preserved.

    Cheaper than ``pipeline_destinations`` and needs only the ``destinations`` /
    ``destination_topic`` attributes, so callers can select just those two columns.
    """
    raw = getattr(pipeline, "destinations", None)
    topics = [d["topic"] for d in raw if isinstance(d, dict) and d.get("topic")] if isinstance(raw, list) else []
    if topics:
        return topics
    topic = getattr(pipeline, "destination_topic", None)
    return [topic] if topic else []


# =============================================================================
# Common Error Schemas
# =============================================================================


class ActionResponse(BaseModel):
    """Generic action response for CRUD operations."""

    id: str = Field(description="Identifier of the affected entity")
    status: bool = Field(description="True when the operation succeeded")
    message: str = Field(description="Human-readable result message")


class ErrorResponse(BaseModel):
    """Standard error response for API errors."""

    detail: str = Field(description="Human-readable error message")

    model_config = {"json_schema_extra": {"examples": [{"detail": "Resource not found"}]}}


class ValidationErrorDetail(BaseModel):
    """Validation error detail item."""

    loc: list[str | int] = Field(description="Location of the error in the request")
    msg: str = Field(description="Error message")
    type: str = Field(description="Error type identifier")


class ValidationErrorResponse(BaseModel):
    """Validation error response (HTTP 422)."""

    detail: list[ValidationErrorDetail] = Field(description="List of validation errors")


# =============================================================================
# Service Meta Schemas (root banner, /health, background sync status)
# =============================================================================


class RootResponse(BaseModel):
    """Service banner returned by the public root endpoint."""

    message: str = Field(description="Static service banner string")


class HealthServicesStatus(BaseModel):
    """Health flags of the background services started in the server lifespan.

    With ``uvicorn --workers N`` the singleton services (metrics consumer,
    monitor, scheduler) run only in the elected leader worker — on a
    non-leader worker their flags legitimately read ``false``; check
    ``is_leader`` before alerting on them. The Flink metrics poller runs on
    every worker (in-process dashboard EPS cache).
    """

    is_leader: bool = Field(
        description="This worker holds the singleton-services leader lock (multi-worker deployments)"
    )
    flink_metrics_poller: bool = Field(description="Flink REST metrics poller is running and polling")
    kafka_metrics_consumer: bool = Field(description="Kafka rule-metrics consumer is connected and consuming")
    kafka_chains_analysed_consumer: bool | None = Field(
        description="Kafka chains-analysed consumer health; null when the consumer is disabled by configuration"
    )
    kafka_activity_consumer: bool = Field(description="Kafka activity consumer is connected and consuming")
    dashboard: bool = Field(description="Dashboard SSE aggregation service is running")
    flink_monitor: bool = Field(description="Flink pipeline health monitor is running")


class ChainsAnalysedHealth(BaseModel):
    """Per-topic detail for the pattern-subscribed chains-analysed consumer."""

    running: bool = Field(description="Consumer task is alive")
    pattern: str | None = Field(description="Subscribed topic regex; null when on a literal topic")
    assigned_partitions: int = Field(description="Partitions currently assigned to this consumer")
    assigned_topics: list[str] = Field(description="Distinct topics currently assigned")
    last_poll_age_seconds: float | None = Field(description="Seconds since the last poll returned")
    topics: dict[str, float] = Field(description="Topic → seconds since its last consumed record")
    total_lag: int | None = Field(description="Cached highwater minus last processed offset; null when unknown")
    orphan_chain_count: int = Field(description="Chains stored with a null pipeline because it was deleted")
    pipeline_id_fallback_count: int = Field(
        description="Chains attributed from the topic name because the envelope stamp was absent or malformed"
    )
    parked_record_count: int = Field(description="Records parked in chains_analysed_dead_letters since start")


class DbPoolStatus(BaseModel):
    """SQLAlchemy connection-pool snapshot; the leading indicator of pool exhaustion.

    ``utilization_percent`` is ``checked_out / max_capacity``; sustained high values mean
    connections are being held (e.g. a session leak) and precede ``QueuePool limit ... reached``.
    """

    pool_size: int = Field(description="Configured number of permanent pooled connections")
    max_overflow: int = Field(description="Configured number of extra connections allowed beyond pool_size")
    checked_out: int = Field(description="Connections currently checked out (in use)")
    checked_in: int = Field(description="Idle connections currently available in the pool")
    total: int = Field(description="checked_out + checked_in")
    overflow_current: int = Field(description="Overflow connections currently in use beyond pool_size")
    max_capacity: int = Field(description="Total possible connections (pool_size + max_overflow)")
    utilization_percent: float = Field(description="checked_out as a percentage of max_capacity")


class HealthResponse(BaseModel):
    """Liveness/health report returned by the public ``GET /health`` endpoint."""

    status: str = Field(description="Overall service status; `healthy` when the API is up")
    detectflow_backend_version: str = Field(description="Running detectflow-backend version (from pyproject.toml)")
    services: HealthServicesStatus = Field(description="Per-service health of background consumers and pollers")
    database_pool: dict = Field(description="SQLAlchemy connection pool statistics (opaque, driver-dependent)")
    db_pool: DbPoolStatus = Field(description="Typed connection-pool snapshot; watch utilization_percent")
    kafka_chains_analysed: ChainsAnalysedHealth | None = Field(
        default=None, description="Detail for the chains-analysed consumer; null when disabled"
    )


class SyncStartedResponse(BaseModel):
    """Acknowledgement that a background repository sync was scheduled (HTTP 202)."""

    message: str = Field(description="Human-readable confirmation that the sync started in the background")


class SyncTypeStatus(BaseModel):
    """Status of the last sync run for one sync type (`api_repos`, `git_hub_repos`, `gitlab_repos`, or `active_threats`)."""

    status: str = Field(description="Sync state: `running`, `completed`, or `failed` (idle types are omitted)")
    started_at: datetime.datetime | None = Field(default=None, description="When the last sync run started (UTC)")
    completed_at: datetime.datetime | None = Field(
        default=None, description="When the last sync run finished (UTC); null while running"
    )
    error: str | None = Field(default=None, description="Failure reason when `status` is `failed`")


# =============================================================================
# Dashboard SSE Schemas (Real-time)
# =============================================================================


class SourceTopicStats(BaseModel):
    """Source topic with real-time EPS metrics."""

    id: str = Field(description="Source topic node identifier")
    name: str = Field(description="Kafka source topic name")
    eps: float = Field(description="Events per second (input throughput)")


class RepositoryStats(BaseModel):
    """Repository with rules count."""

    id: str = Field(description="Repository identifier")
    name: str = Field(description="Repository display name")
    rules_count: int = Field(description="Number of rules in the repository")


class DestinationTopicStats(BaseModel):
    """Destination topic with tagged/untagged EPS metrics."""

    id: str = Field(description="Destination topic node identifier")
    name: str = Field(description="Kafka destination topic name")
    tagged_eps: float = Field(description="Tagged events per second")
    untagged_eps: float = Field(description="Untagged events per second")


class DashboardGraphData(BaseModel):
    """Graph visualization data for dashboard."""

    source_topics: list[SourceTopicStats] = Field(description="Source topic nodes with input throughput")
    repositories: list[RepositoryStats] = Field(description="Repository nodes with rule counts")
    destination_topics: list[DestinationTopicStats] = Field(
        description="Destination topic nodes with output throughput"
    )
    pipelines_count: int = Field(description="Total number of pipelines represented in the graph")
    total_events_eps: float = Field(description="Total input events per second")
    total_tagged_eps: float = Field(description="Total tagged events per second")
    total_untagged_eps: float = Field(default=0.0, description="Total untagged events per second")
    total_rules: int = Field(description="Total number of rules across all repositories")


class PipelineStatsItem(BaseModel):
    """Per-pipeline statistics for the modal table."""

    id: str = Field(description="Pipeline identifier")
    name: str = Field(description="Pipeline display name")
    source_topics: list[str] = Field(description="Kafka input topics (can be multiple)")
    destination_topic: str = Field(
        description="Primary destination topic (destinations[0].topic; kept for backward compatibility)"
    )
    destinations: list[PipelineOutputDestination] = Field(
        default_factory=list,
        description="Kafka output destinations",
    )
    save_untagged: bool = Field(description="Whether to save untagged events (legacy mirror of first destination)")
    repository_ids: list[str] = Field(default_factory=list, description="Repository IDs for mapping")
    input_eps: float = Field(description="Input events per second")
    output_eps: float = Field(description="Output (matched) events per second")
    topic_lag: int = Field(description="Kafka consumer lag (pending records)")
    status: str = Field(description="Pipeline status: running, suspended, failed, deploying, reconciling, or unknown")
    status_details: "StatusDetails | None" = Field(
        None,
        description="Detailed status info for non-running states",
    )


class RecentEventItem(BaseModel):
    """Recent activity event."""

    id: str = Field(description="Event identifier")
    message: str = Field(description="Human-readable event message")
    detail: list[Any] = Field(default_factory=list, description="Optional structured detail payload for the event")
    timestamp: str = Field(description="Event timestamp (ISO 8601 string)")
    type: str = Field(description="Event severity/category: info, warning, error, or success")


class DashboardData(BaseModel):
    """Complete dashboard data sent via SSE."""

    graph: DashboardGraphData = Field(description="Graph visualization data (nodes and throughput totals)")
    pipelines_stats: list[PipelineStatsItem] = Field(description="Per-pipeline statistics for the dashboard table")
    recent_activity: list["ActivityItem"] = Field(
        default_factory=list,
        description="Most recent activity/audit-log items. Empty for viewer accounts (same gate as GET /audit-logs).",
    )


class DashboardSSEMessage(BaseModel):
    """SSE message envelope."""

    type: Literal["dashboard_update", "error", "connection_established"] = Field(
        description="SSE message type: dashboard_update, error, or connection_established"
    )
    data: DashboardData | dict | None = Field(
        default=None, description="Dashboard payload for dashboard_update messages; null otherwise"
    )
    timestamp: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow, description="When the message was emitted (UTC)"
    )
    error: str | None = Field(default=None, description="Error message for error-type messages; null otherwise")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "type": "dashboard_update",
                    "timestamp": "2026-06-04T12:00:00Z",
                    "error": None,
                    "data": {
                        "graph": {
                            "source_topics": [{"id": "t1", "name": "raw-logs", "eps": 1500.0}],
                            "repositories": [{"id": "r1", "name": "Windows", "rules_count": 42}],
                            "destination_topics": [
                                {"id": "d1", "name": "tagged-logs", "tagged_eps": 120.0, "untagged_eps": 1380.0}
                            ],
                            "pipelines_count": 1,
                            "total_events_eps": 1500.0,
                            "total_tagged_eps": 120.0,
                            "total_untagged_eps": 1380.0,
                            "total_rules": 42,
                        },
                        "pipelines_stats": [],
                        "recent_activity": [],
                    },
                }
            ]
        }
    }


class DashboardSnapshotResponse(BaseModel):
    """REST endpoint response for initial dashboard state."""

    graph: DashboardGraphData = Field(description="Graph visualization data (nodes and throughput totals)")
    pipelines_stats: list[PipelineStatsItem] = Field(description="Per-pipeline statistics for the dashboard table")
    recent_activity: list["ActivityItem"] = Field(
        default_factory=list,
        description="Most recent activity/audit-log items. Empty for viewer accounts (same gate as GET /audit-logs).",
    )
    last_kafka_update: datetime.datetime | None = Field(description="Last time metrics were received from Kafka")
    consumer_healthy: bool = Field(description="Whether Kafka consumer is running")


# =============================================================================
# Kafka Metrics Schema (from Flink job)
# =============================================================================


class KafkaMetricState(BaseModel):
    """State metrics from Kafka."""

    event_buffer_size: int = Field(description="Number of events currently buffered in operator state")
    event_buffer_memory_bytes: int = Field(description="Approximate memory used by the event buffer, in bytes")
    broadcast_state_rules: int = Field(description="Number of rules held in broadcast state")


class KafkaMetricErrors(BaseModel):
    """Error metrics from Kafka."""

    event_parsing_errors: int = Field(default=0, description="Count of event parsing errors in the window")
    rule_parsing_errors: int = Field(default=0, description="Count of rule parsing errors in the window")
    matching_errors: int = Field(default=0, description="Count of matching errors in the window")
    total: int = Field(default=0, description="Total error count in the window")
    messages: list[str] = Field(default_factory=list, description="Human-readable error messages (max 10 per window)")


class KafkaMetricRuleTop(BaseModel):
    """Top rule by matches."""

    rule_id: str = Field(description="Sigma rule identifier")
    count: int = Field(description="Number of matches for the rule in the window")


class KafkaMetricSlowRule(BaseModel):
    """Slow rule info."""

    rule_id: str = Field(description="Sigma rule identifier")
    match_count: int = Field(description="Number of matches evaluated for the rule")
    estimated_time_ms: float = Field(description="Estimated total evaluation time for the rule, in milliseconds")


class KafkaMetricMatchedRule(BaseModel):
    """Rule match count for the window (for per-rule totals)."""

    rule_id: str = Field(description="Sigma rule identifier")
    window_matches: int = Field(description="Number of matches for the rule in this window")


class KafkaMetricRules(BaseModel):
    """Rules metrics from Kafka.

    Note: total and triggered_unique are optional for backward compatibility.
    Flink simplified message only sends matched_rules.
    """

    total: int = Field(default=0, description="Total number of loaded rules (optional, for backward compatibility)")
    triggered_unique: int = Field(
        default=0, description="Number of unique rules triggered in the window (optional, for backward compatibility)"
    )
    top_by_matches: list[KafkaMetricRuleTop] = Field(
        default_factory=list, description="Top rules ranked by match count in the window"
    )
    slow_rules_top_3: list[KafkaMetricSlowRule] = Field(
        default_factory=list, description="Three slowest rules by estimated evaluation time"
    )
    avg_time_per_rule_ms: float = Field(default=0.0, description="Average evaluation time per rule, in milliseconds")
    # Per-rule matches for accumulating totals (only rules with matches > 0)
    matched_rules: list[KafkaMetricMatchedRule] = Field(
        default_factory=list, description="Per-rule window match counts (only rules with matches > 0)"
    )


class KafkaMetricSystem(BaseModel):
    """System metrics from Kafka."""

    parallelism: int = Field(description="Flink job parallelism")
    subtask_index: int = Field(description="Index of the reporting subtask")
    hostname: str = Field(description="Hostname of the reporting task manager")
    input_topic: str | None = Field(default=None, description="Kafka input topic for the subtask; null if unknown")
    output_topic: str | None = Field(default=None, description="Kafka output topic for the subtask; null if unknown")


class KafkaMetricMessage(BaseModel):
    """Full Kafka metric message from Flink job."""

    job_id: str = Field(description="Pipeline/job identifier the metrics belong to")
    instance_id: int = Field(default=0, description="Reporting subtask/instance index")
    timestamp: int = Field(default=0, description="Message emission time as a Unix timestamp in milliseconds")
    detectflow_matchnode_version: str | None = Field(
        default=None, description="Match node (Flink job) version from its pyproject.toml; null if not reported"
    )

    # Window metrics (optional for backward compatibility)
    window_total_events: int = Field(default=0, description="Total events processed in the window")
    window_matched_events: int = Field(default=0, description="Events that matched at least one rule in the window")
    window_match_rate_percent: float = Field(default=0.0, description="Percentage of window events that matched")

    window_start_time: float = Field(default=0.0, description="Window start time (epoch seconds)")
    window_end_time: float = Field(default=0.0, description="Window end time (epoch seconds)")
    window_duration_seconds: float = Field(default=0.0, description="Window duration in seconds")
    window_throughput_eps: int = Field(default=0, description="Window throughput in events per second")

    on_timer_start_time: float = Field(default=0.0, description="onTimer phase start time (epoch seconds)")
    on_timer_end_time: float = Field(default=0.0, description="onTimer phase end time (epoch seconds)")
    on_timer_duration_seconds: float = Field(default=0.0, description="onTimer phase duration in seconds")
    on_timer_throughput_eps: int = Field(default=0, description="onTimer phase throughput in events per second")

    matching_start_time: float = Field(default=0.0, description="Matching phase start time (epoch seconds)")
    matching_end_time: float = Field(default=0.0, description="Matching phase end time (epoch seconds)")
    matching_duration_seconds: float = Field(default=0.0, description="Matching phase duration in seconds")
    matching_throughput_eps: int = Field(default=0, description="Matching phase throughput in events per second")

    # Nested objects (optional with defaults)
    state: KafkaMetricState | None = Field(default=None, description="Operator state metrics; null if not reported")
    errors: KafkaMetricErrors = Field(
        default_factory=KafkaMetricErrors, description="Error counters and messages for the window"
    )
    rules: KafkaMetricRules | None = Field(default=None, description="Rule-level metrics; null if not reported")
    system: KafkaMetricSystem | None = Field(default=None, description="System/topology metrics; null if not reported")


class ChainsAnalysedEvent(BaseModel):
    """Single raw event attributed to a chain inside a ``detectflow.chains-analysed`` message."""

    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(description="Identifier of the raw event; generated deterministically if omitted/null")
    sigma_rule_id: str = Field(description="Sigma rule that matched this event")
    sigma_rule_title: str | None = Field(default=None, description="Title of the matching Sigma rule; null if unknown")
    rule_id: UUID | None = Field(
        default=None,
        validation_alias=AliasChoices("rule_db_id", "rule_id"),
        description=(
            "Local ``rules.id`` of the matching copy (Kafka ``rule_db_id``). Null when the producer "
            "omitted it or sent a value that is not a UUID — never fail the envelope on a bad id."
        ),
    )
    seq: int = Field(description="Position of the event in the chain sequence")
    timestamp: datetime.datetime = Field(description="Event timestamp (UTC)")
    technique_ids: list[str] = Field(default_factory=list, description="MITRE ATT&CK technique IDs for the event")
    raw_event: Any | None = Field(
        default=None,
        validation_alias=AliasChoices("parsed_event", "raw_event"),
        description="Parsed/raw event payload (accepts `parsed_event` or `raw_event`); null if absent",
    )

    @field_validator("rule_id", mode="before")
    @classmethod
    def _tolerate_bad_rule_id(cls, v: Any) -> Any:
        """Degrade a malformed ``rule_db_id`` to ``None`` instead of 422-ing the whole chain."""
        if v is None or isinstance(v, UUID):
            return v
        try:
            return UUID(str(v))
        except (ValueError, AttributeError, TypeError):
            return None

    @model_validator(mode="before")
    @classmethod
    def populate_missing_event_id(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        event_id = data.get("event_id")
        if isinstance(event_id, str) and event_id.strip():
            return data

        event_payload = data.get("parsed_event", data.get("raw_event"))
        event_fingerprint = {
            "sigma_rule_id": data.get("sigma_rule_id"),
            "seq": data.get("seq"),
            "timestamp": data.get("timestamp"),
            "event": event_payload,
        }
        fingerprint_json = json.dumps(
            event_fingerprint,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        normalized = dict(data)
        normalized["event_id"] = f"synthetic:{hashlib.sha256(fingerprint_json.encode('utf-8')).hexdigest()}"
        return normalized


class ChainsAnalysedMessage(BaseModel):
    """Payload on ``detectflow.chains-analysed`` (chains-raw + LLM enrichment + per-event attribution).

    ``chain_id`` or ``id`` identifies the row PK. ``events[]`` carries raw events attributed to the chain.
    """

    model_config = ConfigDict(extra="ignore")

    chain_id: UUID = Field(
        validation_alias=AliasChoices("chain_id", "id"),
        description="Chain row primary key (accepts `chain_id` or `id`)",
    )
    revision: int = Field(default=0, description="Chain revision number; increments on re-analysis")
    correlation_pipeline_id: UUID | None = Field(
        default=None,
        description="Correlation pipeline that produced the chain; null for pre-epic or unstamped records",
    )
    hos_rule_id: str = Field(
        ...,
        description="``high_order_sigmas.id`` UUID string or the HoS identifier stored in ``high_order_sigmas.tdm_id``",
    )
    active_threat_id: str = Field(description="Active threat identifier associated with the chain")
    chain_type: str = Field(description="Type/category of the detected chain")
    hostname: str = Field(description="Host on which the chain was observed")
    first_seen: datetime.datetime = Field(description="Timestamp of the earliest event in the chain (UTC)")
    last_seen: datetime.datetime = Field(description="Timestamp of the latest event in the chain (UTC)")
    window_start: datetime.datetime = Field(description="Start of the correlation window for the chain (UTC)")
    event_count: int = Field(description="Number of raw events in the chain")
    rule_count: int = Field(description="Number of distinct rules contributing to the chain")
    seqs: list[Any] = Field(default_factory=list, description="Ordered sequence identifiers for the chain steps")
    hos_tags: list[str] | None = Field(default=None, description="High-order Sigma tags for the chain; null if none")
    coverage: float = Field(description="Fraction of the HoS pattern covered by observed events (0.0-1.0)")
    pipeline_names: list[str] = Field(
        default_factory=list, description="Names of pipelines that contributed events to the chain"
    )
    username: str | None = Field(default=None, description="Associated username; null if not attributed")
    status: str | None = Field(
        default=None,
        description="Applied on INSERT; active-chain UPDATE uses the status merge matrix in AttackChainsDAO",
    )
    ai_summary: str | None = Field(default=None, description="LLM-generated chain summary; null if not analysed")
    llm_verdict: str | None = Field(default=None, description="LLM verdict for the chain; null if not analysed")
    llm_confidence: float | None = Field(default=None, description="LLM confidence score; null if not analysed")
    llm_review: Any | None = Field(default=None, description="Structured LLM review payload; null if not analysed")
    analysed_at: datetime.datetime | None = Field(
        default=None, description="When the chain was analysed by the agent (UTC); null if not yet analysed"
    )
    events: list[ChainsAnalysedEvent] = Field(default_factory=list, description="Raw events attributed to the chain")

    @field_validator("correlation_pipeline_id", mode="before")
    @classmethod
    def _tolerate_bad_pipeline_id(cls, v: Any) -> Any:
        """Never fail the envelope on a malformed stamp — degrade to ``None``.

        A producer that stamps its ``JOB_ID`` (``corr-<uuid>``, not a UUID) or an empty string would
        otherwise make the whole record unparseable, and the consumer would dead-letter a real chain.
        ``None`` here hands the record to ``resolve_correlation_pipeline_id``, which recovers the id
        from the topic name.
        """
        if v is None or isinstance(v, UUID):
            return v
        try:
            return UUID(str(v))
        except (ValueError, AttributeError, TypeError):
            return None


# Pipeline Schemas
class PipelineStatisticsResponse(BaseModel):
    """Aggregate pipeline statistics grouped by category."""

    topics: dict = Field(description="Topic-level statistics (opaque category-keyed counters)")
    networks: dict = Field(description="Network/graph statistics (opaque category-keyed counters)")
    rules: dict = Field(description="Rule statistics (opaque category-keyed counters)")
    events: dict = Field(description="Event throughput statistics (opaque category-keyed counters)")


class PaginationParams(BaseModel):
    """Common pagination, sorting, and search query parameters."""

    page: int | None = Field(default=1, description="1-based page number")
    limit: int | None = Field(default=10, description="Maximum number of items per page")
    sort: str | None = Field(default=None, description="Field name to sort by")
    offset: int | None = Field(default=0, description="Number of items to skip")
    order: str | None = Field(default="asc", description="Sort direction: asc or desc")
    search: str | None = Field(default=None, description="Free-text search term")


def normalize_repository_selections(value: Any, field_name: str = "repository_ids") -> list[dict[str, Any]] | None:
    """Merge duplicate repository selections; folder paths already come from backend folder trees."""
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list of repository selections")

    merged: dict[str, list[str]] = {}
    for selection in value:
        if isinstance(selection, BaseModel):
            selection = selection.model_dump()
        if not isinstance(selection, dict):
            raise ValueError("Each repository selection must be an object")

        repository_id = selection.get("id")
        if not repository_id:
            raise ValueError("Repository selection id is required")

        folders = selection.get("folders") or []
        if not isinstance(folders, list):
            raise ValueError("Repository selection folders must be a list")

        if repository_id not in merged:
            merged[repository_id] = list(folders)
            continue

        if not folders or not merged[repository_id]:
            merged[repository_id] = []
            continue

        merged[repository_id].extend(folder for folder in folders if folder not in merged[repository_id])

    return [{"id": repository_id, "folders": folders} for repository_id, folders in merged.items()]


def normalize_pipeline_repository_selections(value: Any) -> list[dict[str, Any]] | None:
    """Backward-compatible wrapper for pipeline repository selections."""
    return normalize_repository_selections(value)


class RepositorySelection(BaseModel):
    """Repository and optional virtual folders selected for a pipeline."""

    id: str = Field(description="Repository UUID")
    folders: list[str] = Field(
        default_factory=list,
        description="Virtual folder paths to include. Empty list means the whole repository.",
    )

    @field_validator("id")
    @classmethod
    def validate_repository_id(cls, v: str) -> str:
        return validate_uuid4(v.strip()) or v


class PipelineRepositorySelection(RepositorySelection):
    """Pipeline-specific alias for repository selections."""

    pass


class PipelineCreateRequest(BaseModel):
    """Request to create a new ETL pipeline."""

    name: str = Field(description="Pipeline display name")
    source_topics: list[str] = Field(
        min_length=1,
        description="Kafka topics to read events from (minimum 1 required, supports multiple for union)",
    )
    destinations: list[PipelineOutputDestination] | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_OUTPUT_DESTINATIONS,
        description=(
            "Kafka output destinations (1–10). Preferred over singular destination_topic. "
            "Each entry has topic, output_mode, and apply_parser_to_output_events."
        ),
    )
    destination_topic: str | None = Field(
        default=None,
        description="Legacy singular destination topic. Used when destinations is omitted; mirrored from destinations[0] on write.",
    )
    save_untagged: bool | None = Field(
        default=None,
        description='Legacy: save unmatched events (maps to output_mode "all_events"). Used when destinations is omitted; cannot express "unmatched_only".',
    )
    apply_parser_to_output_events: bool | None = Field(
        default=None,
        description="Legacy: apply parser to output events. Used when destinations is omitted; otherwise per-destination.",
    )
    filters: list[str] = Field(default=[], description="List of filter IDs to apply")
    log_source_id: str | None = Field(default=None, description="Log source ID for parsing configuration")
    enabled: bool = Field(default=True, description="Start pipeline immediately after creation")
    kafka_starting_offset_earliest: bool = Field(
        default=False,
        description="If true, Kafka consumer starts from earliest offset; not stored in DB",
    )
    repository_ids: list[PipelineRepositorySelection] | None = Field(
        default=None,
        description="Repository selections containing rules. Empty folders means the whole repository.",
    )
    custom_fields: str | None = Field(default=None, description="Custom fields to add (YAML format)")
    resources: "FlinkResourceConfig | None" = Field(
        default=None,
        description="Flink resource configuration. Null values use global defaults.",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Production Pipeline",
                    "source_topics": ["raw-logs", "detectflow.events"],
                    "destinations": [
                        {
                            "topic": "tagged-logs",
                            "output_mode": "matched_only",
                            "apply_parser_to_output_events": False,
                        },
                        {
                            "topic": "all-events-parsed",
                            "output_mode": "all_events",
                            "apply_parser_to_output_events": True,
                        },
                    ],
                    "log_source_id": "550e8400-e29b-41d4-a716-446655440000",
                    "enabled": True,
                    "repository_ids": [
                        {
                            "id": "550e8400-e29b-41d4-a716-446655440001",
                            "folders": ["_product/windows"],
                        }
                    ],
                    "resources": {"parallelism": 4, "taskmanager_memory_mb": 4096},
                }
            ]
        }
    }

    @field_validator("repository_ids", mode="before")
    @classmethod
    def validate_repository_selections(cls, v: Any) -> list[dict[str, Any]] | None:
        return normalize_repository_selections(v)

    @field_validator("source_topics")
    @classmethod
    def validate_source_topics(cls, v: list[str]) -> list[str]:
        """Validate source_topics is non-empty and contains valid topic names."""
        if not v:
            raise ValueError("At least one source topic is required")
        # Remove duplicates while preserving order, strip whitespace
        seen = set()
        unique_topics = []
        for topic in v:
            topic = topic.strip()
            if not topic:
                raise ValueError("Empty topic names are not allowed")
            if topic not in seen:
                seen.add(topic)
                unique_topics.append(topic)
        return unique_topics

    @field_validator("log_source_id")
    @classmethod
    def validate_uuid_format(cls, v: str | None) -> str | None:
        """Validate UUID format for log source IDs."""
        return validate_uuid4(v)

    @field_validator("custom_fields")
    @classmethod
    def validate_custom_fields_yaml(cls, v: str | None) -> str | None:
        """Validate YAML format for custom fields."""
        return validate_yaml(v)

    @model_validator(mode="after")
    def validate_topics(self) -> Self:
        """Validate source topics and resolve ``destinations`` from the new or legacy fields.

        After validation ``destinations`` is always populated and the legacy singular fields
        mirror ``destinations[0]``, so the orchestrator never has to look at both shapes.
        """
        for topic in self.source_topics:
            validate_topic_not_reserved(topic, "Source topic")

        if self.destinations is not None:
            resolved = normalize_destinations(self.destinations, self.source_topics)
        elif self.destination_topic is not None:
            resolved = normalize_destinations(
                destinations_from_legacy(
                    self.destination_topic,
                    bool(self.save_untagged),
                    bool(self.apply_parser_to_output_events),
                ),
                self.source_topics,
            )
        else:
            raise ValueError("Either destinations or destination_topic is required")

        self.destinations = [PipelineOutputDestination.model_validate(d) for d in resolved]
        self.destination_topic, self.save_untagged, self.apply_parser_to_output_events = (
            legacy_fields_from_destinations(resolved)
        )
        return self


class PipelineUpdateRequest(BaseModel):
    """Request to update an existing pipeline. All fields are optional."""

    name: str | None = Field(default=None, description="Pipeline display name")
    source_topics: list[str] | None = Field(
        default=None,
        description="Kafka source topics (triggers restart). At least 1 required when provided.",
    )
    destinations: list[PipelineOutputDestination] | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_OUTPUT_DESTINATIONS,
        description="Kafka output destinations (1–10). Preferred over singular destination_topic (triggers restart).",
    )
    destination_topic: str | None = Field(
        default=None,
        description="Legacy singular destination topic (triggers restart). Used when destinations is omitted.",
    )
    save_untagged: bool | None = Field(
        default=None,
        description='Legacy: save unmatched events / output_mode "all_events" (triggers restart). Cannot express "unmatched_only".',
    )
    apply_parser_to_output_events: bool | None = Field(
        default=None,
        description="Legacy: apply parser to output events (triggers restart)",
    )
    filters: list[str] | None = Field(default=None, description="Filter IDs (hot-reloadable)")
    log_source_id: str | None = Field(default=None, description="Log source ID (hot-reloadable)")
    enabled: bool | None = Field(default=None, description="Enable/disable pipeline")
    repository_ids: list[PipelineRepositorySelection] | None = Field(
        default=None,
        description="Repository selections (hot-reloadable). Empty folders means the whole repository.",
    )
    custom_fields: str | None = Field(default=None, description="Custom fields YAML (hot-reloadable)")
    resources: "FlinkResourceConfig | None" = Field(
        default=None,
        description=(
            "Flink resource, processing, and autoscaler settings. Provided fields trigger a "
            "last-state Flink upgrade (~30s). Null or omitted fields leave existing values unchanged."
        ),
    )

    @field_validator("source_topics")
    @classmethod
    def validate_source_topics(cls, v: list[str] | None) -> list[str] | None:
        """Validate source_topics if provided."""
        if v is None:
            return v
        if not v:
            raise ValueError("At least one source topic is required when updating source_topics")
        # Remove duplicates while preserving order, strip whitespace
        seen = set()
        unique_topics = []
        for topic in v:
            topic = topic.strip()
            if not topic:
                raise ValueError("Empty topic names are not allowed")
            if topic not in seen:
                seen.add(topic)
                unique_topics.append(topic)
        return unique_topics

    @field_validator("log_source_id")
    @classmethod
    def validate_uuid_format(cls, v: str | None) -> str | None:
        """Validate UUID format for log source IDs."""
        return validate_uuid4(v)

    @field_validator("repository_ids", mode="before")
    @classmethod
    def validate_repository_selections(cls, v: Any) -> list[dict[str, Any]] | None:
        return normalize_repository_selections(v)

    @field_validator("custom_fields")
    @classmethod
    def validate_custom_fields_yaml(cls, v: str | None) -> str | None:
        """Validate YAML format for custom fields."""
        return validate_yaml(v)

    @model_validator(mode="after")
    def validate_topics(self) -> Self:
        """Validate the topics that are present in the request.

        Fields are left exactly as sent: a partial legacy update (e.g. ``save_untagged``
        alone) only means something relative to the stored destinations, so the merge —
        and the final validation against the pipeline's effective source topics — happens
        in ``detect_pipeline_changes``.
        """
        if self.source_topics is not None:
            for topic in self.source_topics:
                validate_topic_not_reserved(topic, "Source topic")

        if self.destinations is not None:
            normalize_destinations(self.destinations, self.source_topics)
        elif self.destination_topic is not None:
            validate_topic_not_reserved(self.destination_topic, "Destination topic")
            if self.source_topics and self.destination_topic in self.source_topics:
                raise PydanticCustomError(
                    "topic_conflict",
                    f"Destination topic '{self.destination_topic}' cannot be the same as a source topic. "
                    "This would create an infinite loop.",
                )
        return self


class PipelineResponse(ActionResponse):
    pass


class TaggedEventsTopicAssignRequest(BaseModel):
    """Body of the one operation that migrates a pipeline off the shared tagged-events topic.

    It restarts the customer's matcher (~30 s), so the confirmation is explicit and required —
    there is no implicit assignment anywhere in the platform.
    """

    confirm_restart: bool = Field(
        description="Must be true — assigning a dedicated tagged-events topic restarts this pipeline (~30s)"
    )


class StatusDetails(BaseModel):
    """Detailed status information for pipeline diagnostics."""

    level: str = Field(
        "info",
        description="Severity level: info (normal operation), warning (needs attention), error (action required)",
    )
    job_manager_status: str | None = Field(
        None,
        description="JobManager deployment status: READY, DEPLOYED_NOT_READY, DEPLOYING, MISSING, ERROR",
    )
    lifecycle_state: str | None = Field(None, description="Flink lifecycle state: STABLE, DEPLOYING, etc.")
    warnings: list[str] = Field(default_factory=list, description="Warning messages from Kubernetes events")
    error: str | None = Field(None, description="Error message if any")
    source: str = Field("unknown", description="Status source: flink, kubernetes, local")


class PipelineListItem(BaseModel):
    """Pipeline summary row for the paginated pipeline list."""

    id: str = Field(description="Pipeline identifier")
    enabled: bool = Field(description="Whether the pipeline is enabled")
    name: str = Field(description="Pipeline display name")
    status: str = Field(description="Pipeline status: running, suspended, failed, deploying, reconciling, or unknown")
    status_details: StatusDetails | None = Field(
        None,
        description="Detailed status info with warnings (only populated for non-running states)",
    )
    source_topics: list[str] = Field(description="Kafka input topics")
    destination_topic: str = Field(
        description="Primary Kafka destination topic (destinations[0].topic; kept for backward compatibility)"
    )
    destinations: list[PipelineOutputDestination] = Field(
        default_factory=list,
        description="Kafka output destinations (topic, output_mode, apply_parser_to_output_events)",
    )
    tagged_events_topic: str | None = Field(
        default=None,
        description="Dedicated tagged-events topic; null means the shared legacy topic",
    )
    repositories: list[dict[str, str]] = Field(description="Connected repositories as id/name maps")
    log_source: list[dict[str, str]] = Field(description="Associated log source as id/name map(s)")
    filters: int = Field(description="Number of filters applied to the pipeline")
    rules: int = Field(description="Total number of rules in the pipeline")
    supported_rules: int = Field(0, description="Count of supported rules")
    events_tagged: int = Field(description="Accumulated count of tagged (matched) events")
    events_untagged: int = Field(description="Accumulated count of untagged (unmatched) events")
    detectflow_matchnode_version: str | None = Field(None, description="Last known Flink job version")
    needs_restart: bool = Field(False, description="Pipeline needs restart due to rule loader module update")
    correlation_pipelines: list["CorrelationPipelineRef"] = Field(
        default_factory=list,
        description="Correlation pipelines this detection pipeline is a member of (id/name chips)",
    )
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")


class PipelineListResponse(BaseModel):
    """Paginated list of pipelines."""

    total: int = Field(description="Total number of pipelines matching the query")
    page: int = Field(description="Current 1-based page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[PipelineListItem] = Field(description="Pipeline summary rows for the current page")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "total": 1,
                    "page": 1,
                    "limit": 10,
                    "sort": "name",
                    "offset": 0,
                    "order": "asc",
                    "data": [
                        {
                            "id": "550e8400-e29b-41d4-a716-446655440000",
                            "enabled": True,
                            "name": "Production Pipeline",
                            "status": "running",
                            "status_details": None,
                            "source_topics": ["raw-logs"],
                            "destination_topic": "tagged-logs",
                            "repositories": [{"id": "550e8400-e29b-41d4-a716-446655440001", "name": "Windows"}],
                            "log_source": [],
                            "filters": 1,
                            "rules": 42,
                            "supported_rules": 40,
                            "events_tagged": 1200,
                            "events_untagged": 34000,
                            "detectflow_matchnode_version": "1.0.0",
                            "needs_restart": False,
                            "correlation_pipelines": [
                                {"id": "550e8400-e29b-41d4-a716-446655440002", "name": "Default"}
                            ],
                            "created": "2026-06-01T10:00:00Z",
                            "updated": "2026-06-04T12:00:00Z",
                        }
                    ],
                }
            ]
        }
    }


class PipelineDetailResponse(BaseModel):
    """Full configuration, metrics, and Flink state for a single pipeline."""

    id: str = Field(description="Pipeline identifier")
    name: str = Field(description="Pipeline display name")
    source_topics: list[str] = Field(description="Kafka input topics")
    destinations: list[PipelineOutputDestination] = Field(
        description="Kafka output destinations (topic, output_mode, apply_parser_to_output_events)"
    )
    destination_topic: str = Field(
        description="Primary destination topic (destinations[0].topic; kept for backward compatibility)"
    )
    tagged_events_topic: str | None = Field(
        default=None,
        description="Dedicated tagged-events topic; null means the shared legacy topic",
    )
    save_untagged: bool = Field(
        description='Legacy mirror: true when destinations[0] receives unmatched events ("all_events" or "unmatched_only")'
    )
    apply_parser_to_output_events: bool = Field(
        description="Legacy mirror of destinations[0].apply_parser_to_output_events"
    )
    filters: list[str] = Field(description="IDs of filters applied to the pipeline")
    log_source_id: str = Field(description="Associated log source ID")
    log_source_name: str | None = Field(default=None, description="Associated log source name; null if none")
    enabled: bool = Field(description="Whether the pipeline is enabled")
    repository_ids: list[PipelineRepositorySelection] = Field(
        description="Repositories supplying rules, each with optional virtual-folder selections"
    )
    custom_fields: str | None = Field(default=None, description="Custom fields in YAML format; null if none")

    # Metrics from PostgreSQL (accumulated totals)
    events_tagged: int = Field(default=0, description="Accumulated count of tagged (matched) events")
    events_untagged: int = Field(default=0, description="Accumulated count of untagged (unmatched) events")

    # Rules statistics
    active_rules: int = Field(default=0, description="Total count of enabled rules")
    matched_rules: int = Field(default=0, description="Count of rules with tagged events > 0")

    # Flink fields (values come from DB, defaults from settings at creation time)
    rules_topic: str | None = Field(default=None, description="Kafka topic the rules are published to; null if unset")
    metrics_topic: str | None = Field(default=None, description="Kafka topic metrics are consumed from; null if unset")
    parallelism: int | None = Field(default=None, description="Flink job parallelism; null uses default")
    window_size_sec: int | None = Field(
        default=None, description="Aggregation window size in seconds; null uses default"
    )
    status: str | None = Field(
        default=None,
        description="Pipeline status: running, suspended, failed, deploying, reconciling, or unknown; null if unknown",
    )
    status_details: StatusDetails | None = Field(
        None,
        description="Detailed status info with warnings (only populated for non-running states)",
    )
    deployment_name: str | None = Field(
        default=None, description="Kubernetes FlinkDeployment name; null if not deployed"
    )
    namespace: str | None = Field(default=None, description="Kubernetes namespace; null if not deployed")
    last_sync_at: str | None = Field(default=None, description="Last sync timestamp (ISO 8601 string); null if never")

    # Flink resource configuration
    taskmanager_memory_mb: int | None = Field(default=None, description="TaskManager memory in MB; null uses default")
    taskmanager_cpu: float | None = Field(default=None, description="TaskManager CPU cores; null uses default")
    checkpoint_interval_sec: int | None = Field(
        default=None, description="Checkpoint interval in seconds; null uses default"
    )
    autoscaler_enabled: bool | None = Field(
        default=None, description="Whether the autoscaler is enabled; null uses default"
    )
    autoscaler_min_parallelism: int | None = Field(
        default=None, description="Minimum autoscaler parallelism; null uses default"
    )
    autoscaler_max_parallelism: int | None = Field(
        default=None, description="Maximum autoscaler parallelism; null uses default"
    )

    # Flink job version (last known, persists when pipeline not running)
    detectflow_matchnode_version: str | None = Field(None, description="Last known Flink job version")
    warnings: list[str] = Field(default_factory=list, description="Pipeline warnings (e.g., no supported rules)")
    supported_rules_count: int = Field(0, description="Count of supported (valid) rules")
    total_rules_count: int = Field(0, description="Total count of rules")
    needs_restart: bool = Field(False, description="Pipeline needs restart due to rule loader module update")
    correlation_pipelines: list["CorrelationPipelineRef"] = Field(
        default_factory=list,
        description="Correlation pipelines this detection pipeline is a member of (id/name chips)",
    )


class PipelineStatisticsDetailResponse(BaseModel):
    """Accumulated event and rule counters for a single pipeline."""

    events_tagged: int = Field(description="Accumulated count of tagged (matched) events")
    events_untagged: int = Field(description="Accumulated count of untagged (unmatched) events")
    active_rules: int = Field(description="Total count of enabled rules")
    matched_rules: int = Field(description="Count of rules with tagged events > 0")


class RuleListItem(BaseModel):
    """Rule summary row in a pipeline's rule list."""

    id: str = Field(description="Rule identifier")
    name: str = Field(description="Rule title/name")
    repository: str = Field(description="Name of the repository the rule belongs to")
    repository_id: str | None = Field(
        default=None, description="ID of the repository the rule belongs to; null if none"
    )
    sigma_uuid: str | None = Field(default=None, description="UUID declared inside the Sigma body (id field)")
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")
    enabled: bool = Field(description="Whether the rule is enabled")
    tagged_events: int = Field(description="Accumulated count of events tagged by this rule")
    level: str | None = Field(
        default=None,
        description="Sigma severity from the rule body (informational, low, medium, high, critical); "
        "null when the body declares no level",
    )
    # Sigma validation fields
    is_supported: bool = Field(default=True, description="Whether the rule is supported by the match node")
    unsupported_reason: str | None = Field(
        default=None, description="Reason the rule is unsupported; null when supported"
    )


class PipelineRulesListResponse(BaseModel):
    """Paginated list of rules for a pipeline with validation summary."""

    total: int = Field(description="Total number of rules matching the query")
    page: int = Field(description="Current 1-based page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[RuleListItem] = Field(description="Rule summary rows for the current page")
    # Rule validation summary
    supported_count: int = Field(0, description="Count of supported rules in this pipeline")
    unsupported_count: int = Field(0, description="Count of unsupported rules in this pipeline")


class PipelineSimpleResponse(BaseModel):
    """Minimal pipeline reference describing its relation to a topic."""

    id: str = Field(description="Pipeline identifier")
    name: str = Field(description="Pipeline display name")
    type: str = Field(description="How pipeline uses this topic: source, destination")


class TopicDetailResponse(BaseModel):
    """A Kafka topic and the pipelines that use it."""

    name: str = Field(description="Kafka topic name")
    pipelines: list[PipelineSimpleResponse] = Field(
        description="Pipelines that use this topic as source or destination"
    )


class TopicsResponse(BaseModel):
    """Paginated topics response."""

    data: list[TopicDetailResponse] = Field(description="Topics with their associated pipelines for the current page")
    total: int = Field(description="Total number of topics matching the query")
    offset: int = Field(description="Number of items skipped")
    limit: int = Field(description="Maximum items per page")


class TopicsEventsRequest(BaseModel):
    """Request schema for getting events from topics."""

    topics: list[str] = Field(description="List of topic names to fetch events from")


class TopicEventItem(BaseModel):
    """Single event item with topic name."""

    topic: str = Field(description="Topic name")
    event: str = Field(description="Event data as string")


# Filters Schemas
class FilterActiveResponse(BaseModel):
    """Minimal active-filter reference (id and name)."""

    id: str = Field(description="Filter identifier")
    name: str = Field(description="Filter display name")


class FilterDetailResponse(BaseModel):
    """Filter metadata without the body."""

    id: str = Field(description="Filter identifier")
    name: str = Field(description="Filter display name")
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")


class FilterListResponse(BaseModel):
    """Paginated list of filters."""

    total: int = Field(description="Total number of filters matching the query")
    page: int = Field(description="Current 1-based page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[FilterDetailResponse] = Field(description="Filter rows for the current page")


class FilterCreateRequest(BaseModel):
    """Request to create a new detection filter."""

    name: str = Field(description="Filter display name")
    body: str = Field(description="Filter definition in YAML format")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Windows Security Events",
                    "body": "filter:\n  EventID:\n    - 4624\n    - 4625\n    - 4688",
                }
            ]
        }
    }


class FilterUpdateRequest(BaseModel):
    """Request to update an existing filter. All fields are optional."""

    name: str | None = Field(default=None, description="Filter display name")
    body: str | None = Field(default=None, description="Filter definition in YAML format")


class FilterResponse(ActionResponse):
    pass


class FilterFullResponse(BaseModel):
    """Full filter response with body for editing."""

    id: str = Field(description="Filter identifier")
    name: str = Field(description="Filter display name")
    body: str = Field(description="Filter definition in YAML format")
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")


# Presets & Rule Filters Schemas
class PresetCreateRequest(BaseModel):
    """Request to create a preset."""

    name: str = Field(description="Preset name")
    siem_type: str = Field(description="SIEM type")
    rule_filter_ids: list[str] = Field(default_factory=list, description="Rule filter UUIDs")
    body: dict[str, Any] = Field(description="Preset body as JSON")


class PresetUpdateRequest(BaseModel):
    """Request to update a preset. All fields optional."""

    name: str | None = Field(default=None, description="New preset name")
    siem_type: str | None = Field(default=None, description="New SIEM type key")
    rule_filter_ids: list[str] | None = Field(default=None, description="Replacement list of rule filter UUIDs")
    body: dict[str, Any] | None = Field(default=None, description="New preset body as JSON")


class PresetDetailResponse(BaseModel):
    """Preset list/detail item."""

    id: str = Field(description="Preset UUID")
    name: str = Field(description="Preset name")
    siem_type: str = Field(description="SIEM type key the preset targets")
    siem_name: str = Field(description="Human-readable SIEM display name")
    user_id: str | None = Field(description="UUID of the owning user; null for shared presets")
    rule_filter_ids: list[str] = Field(description="Rule filter UUIDs attached to the preset")
    body: dict[str, Any] = Field(description="Preset body as JSON")
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")


class PresetListResponse(BaseModel):
    """Paginated list of presets."""

    total: int = Field(description="Total number of presets matching the query")
    page: int = Field(description="Current 1-based page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped from the start")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[PresetDetailResponse] = Field(description="Presets on this page")


class RuleFilterCreateRequest(BaseModel):
    """Request to create a rule filter."""

    name: str = Field(description="Rule filter name")
    siem_type: str = Field(description="SIEM type")
    filter_body: str = Field(description="Filter key/body")
    append_to_rule: bool = Field(
        default=False,
        description="When true, append filter after the rule query (e.g. Sentinel tail | where); when false, prepend",
    )


class RuleFilterUpdateRequest(BaseModel):
    """Request to update a rule filter. All fields optional."""

    name: str | None = Field(default=None, description="New rule filter name")
    siem_type: str | None = Field(default=None, description="New SIEM type key")
    filter_body: str | None = Field(default=None, description="New filter key/body")
    append_to_rule: bool | None = Field(
        default=None, description="When true, append filter after the rule query; when false, prepend"
    )


class RuleFilterDetailResponse(BaseModel):
    """Rule filter list/detail item."""

    id: str = Field(description="Rule filter UUID")
    name: str = Field(description="Rule filter name")
    user_id: str | None = Field(description="UUID of the owning user; null for shared filters")
    siem_type: str = Field(description="SIEM type key the filter targets")
    siem_name: str = Field(description="Human-readable SIEM display name")
    filter_body: str = Field(description="Filter key/body")
    append_to_rule: bool = Field(
        description="When true, append filter after the rule query (e.g. Sentinel tail | where); when false, prepend"
    )
    created: str = Field(description="Creation timestamp (ISO 8601 string)")
    updated: str = Field(description="Last update timestamp (ISO 8601 string)")


class RuleFilterListResponse(BaseModel):
    """Paginated list of rule filters."""

    total: int = Field(description="Total number of rule filters matching the query")
    page: int = Field(description="Current 1-based page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped from the start")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[RuleFilterDetailResponse] = Field(description="Rule filters on this page")


class ApplyPresetRequest(BaseModel):
    """Request to apply a preset to a rule."""

    preset_id: str = Field(description="Preset ID")
    rule_str: str = Field(description="Rule string")


class ApplyFiltersRequest(BaseModel):
    """Request to apply only rule filters to a rule (no preset body)."""

    rule_str: str = Field(description="Rule string")
    rule_filter_ids: list[str] = Field(
        default_factory=list,
        description="Rule filter UUIDs; their filter_body values are applied to the rule",
    )


class ApplyPresetResponse(BaseModel):
    """Response to apply a preset to a rule."""

    updated_rule: str = Field(description="Updated rule string")
    error: str | None = Field(default=None, description="Error message")
    status: bool = Field(description="Status of the preset application")


class RuleTemplateBodyFieldInfo(BaseModel):
    """Metadata for one key in a preset ``body`` object for a given ``siem_type`` (UI / OpenAPI only)."""

    name: str = Field(
        description="Same as the key in the parent `fields` map (JSON property name in preset body)",
    )
    type: str = Field(
        description=(
            "Logical type for forms: e.g. string, boolean, string[], or notes like `object | string` "
            "(not a strict JSON Schema type)"
        ),
    )
    title: str = Field(description="Short label for UI")
    description: str = Field(description="Semantics, constraints, and mapping hints for this body key")
    options: list[str] | None = Field(
        default=None,
        description=("Typical or suggested discrete values for dropdowns; not an exhaustive product allow-list"),
    )


class RuleTemplateBodyParameter(BaseModel):
    """One supported ``siem_type`` with documented keys for the preset ``body`` JSON."""

    value: str = Field(
        description="Must match ``Presets.siem_type`` / ``PresetCreateRequest.siem_type`` (e.g. elastic-eql-rule)",
    )
    name: str = Field(description="Human-readable template name for UI")
    fields: dict[str, RuleTemplateBodyFieldInfo] = Field(
        default_factory=dict,
        description="Map of JSON key -> field metadata (type, title, description, optional options)",
    )


class RuleTemplateSchemaResponse(BaseModel):
    """Response of ``GET /api/v1/rule_template/schema``: static documentation for admin UI forms."""

    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    body_parameters: list[RuleTemplateBodyParameter] = Field(
        description=(
            "Per-SIEM documentation for keys allowed or meaningful inside preset ``body`` "
            "(align keys with the target rule export where applicable)"
        ),
    )
    field_documentation: dict[str, dict[str, str]] = Field(
        serialization_alias="schema",
        validation_alias="schema",
        description=(
            "Top-level hints for preset and rule_filter resources: keys ``preset`` and ``rule_filter``, "
            "each mapping API field name -> short type/semantics string (not JSON Schema)"
        ),
    )


# Repositories Schemas
class PipelineInfo(BaseModel):
    """Pipeline information with id and name."""

    id: str = Field(description="Pipeline ID")
    name: str = Field(description="Pipeline name")


class RuleFolderResponse(BaseModel):
    """Virtual rule folder with subtree rule count."""

    path: str = Field(description="Stable folder path used to query rules")
    rules_count: int = Field(description="Number of rules in this folder subtree")
    children: list["RuleFolderResponse"] = Field(default_factory=list, description="Nested folders")


class RepositoryDetailResponse(BaseModel):
    """Repository metadata, type, rule count, and connected pipelines."""

    id: str = Field(description="Repository identifier")
    name: str = Field(description="Repository display name")
    type: Literal["api", "local", "external", "gitlab"] = Field(
        description="Repository type: api (SOC Prime/TDM), local, external (GitHub), or gitlab"
    )
    type_display: str = Field(description="Human-readable type for UI display")
    rules: int = Field(description="Number of rules in the repository")
    created: datetime.datetime = Field(description="Creation timestamp (UTC)")
    updated: datetime.datetime = Field(description="Last update timestamp (UTC)")
    sync_enabled: bool | None = Field(description="Whether sync is enabled; null when not applicable")
    source_link: str | None = Field(default=None, description="Source link for external repositories")
    is_system: bool = Field(default=False, description="True when repository is system-managed")
    system_key: str | None = Field(default=None, description="Stable system repository key")
    pipelines: list[PipelineInfo] = Field(default_factory=list, description="List of connected pipelines")
    folders: list[RuleFolderResponse] = Field(default_factory=list, description="Virtual rule folder tree")


class RepositoryListResponse(BaseModel):
    """Paginated list of repositories."""

    total: int = Field(description="Total number of repositories matching the query")
    limit: int = Field(description="Maximum items per page")
    offset: int = Field(description="Number of items skipped")
    sort: str | None = Field(default=None, description="Field the results are sorted by; null for default")
    order: Literal["asc", "desc"] = Field(description="Sort direction: asc or desc")
    data: list[RepositoryDetailResponse] = Field(description="Repository rows for the current page")


class RepositoryCreateUpdateRequest(BaseModel):
    """Request to create or rename a repository."""

    name: str = Field(description="Repository name")


class RepositoryUpdateRequest(BaseModel):
    """Request to update a repository. Fields are optional and depend on repository type."""

    name: str | None = Field(default=None, description="Repository name (for local repositories)")
    sync_enabled: bool | None = Field(
        default=None, description="Sync enabled status (for API and external repositories)"
    )


# =============================================================================
# Flink Resource Configuration Schemas
# =============================================================================


class FlinkDefaultsResponse(BaseModel):
    """Global Flink defaults for new pipelines."""

    parallelism: int = Field(default=2, ge=1, le=128, description="Number of parallel tasks")
    taskmanager_memory_mb: int = Field(
        default=2048, ge=1536, le=131072, description="Total memory per TaskManager pod in MB (JVM + Python)"
    )
    taskmanager_cpu: float = Field(
        default=1.5, ge=0.5, le=16.0, description="CPU cores per TaskManager (each TaskManager = one parallelism unit)"
    )
    window_size_sec: int = Field(default=30, ge=10, le=120, description="Event aggregation window in seconds")
    checkpoint_interval_sec: int = Field(default=60, ge=30, le=600, description="Checkpoint interval in seconds")
    autoscaler_enabled: bool = Field(default=False, description="Enable Flink autoscaling")
    autoscaler_min_parallelism: int = Field(default=1, ge=1, le=24, description="Minimum parallelism for autoscaler")
    autoscaler_max_parallelism: int = Field(default=24, ge=1, le=720, description="Maximum parallelism for autoscaler")


class FlinkResourceConfig(BaseModel):
    """Flink resource configuration for pipeline create/update.

    All fields are optional. On create, null uses global defaults. On pipeline update, null
    leaves the existing pipeline value unchanged.
    """

    parallelism: int | None = Field(default=None, ge=1, le=128, description="Number of parallel tasks")
    taskmanager_memory_mb: int | None = Field(
        default=None, ge=1536, le=131072, description="Total memory per TaskManager pod in MB (JVM + Python)"
    )
    taskmanager_cpu: float | None = Field(
        default=None, ge=0.5, le=16.0, description="CPU cores per TaskManager (each TaskManager = one parallelism unit)"
    )
    window_size_sec: int | None = Field(default=None, ge=10, le=120, description="Event aggregation window in seconds")
    checkpoint_interval_sec: int | None = Field(
        default=None, ge=30, le=600, description="Checkpoint interval in seconds"
    )
    autoscaler_enabled: bool | None = Field(default=None, description="Enable Flink autoscaling")
    autoscaler_min_parallelism: int | None = Field(
        default=None, ge=1, le=24, description="Minimum parallelism for autoscaler"
    )
    autoscaler_max_parallelism: int | None = Field(
        default=None, ge=1, le=720, description="Maximum parallelism for autoscaler"
    )


class FlinkDefaultsUpdateRequest(FlinkResourceConfig):
    """Partial update for Flink defaults. Only provided fields are updated."""

    pass


class FlinkParameterInfo(BaseModel):
    """Documentation for a single Flink parameter."""

    name: str = Field(description="Parameter name (e.g., 'parallelism')")
    type: Literal["integer", "number", "boolean", "string"] = Field(description="Parameter data type")
    default: int | float | bool | str | None = Field(
        default=None, description="Default value; null when the parameter has no default (see `nullable`)"
    )
    nullable: bool = Field(
        default=False,
        description=(
            "Whether null is a meaningful value for this parameter rather than 'unset' — e.g. a "
            "correlation window of null inherits each high-order Sigma rule's detection.timeframe"
        ),
    )
    min: int | float | None = Field(default=None, description="Minimum allowed value")
    max: int | float | None = Field(default=None, description="Maximum allowed value")
    unit: str | None = Field(default=None, description="Unit of measurement (e.g., 'MB', 'cores', 'seconds')")
    title: str = Field(description="Human-readable title")
    description: str = Field(description="Detailed description of what this parameter does")
    impact: Literal["restart", "hot_reload"] = Field(description="Whether changing requires restart")
    category: Literal["resources", "processing", "autoscaler", "correlation", "correlation_job", "agent_job"] = Field(
        description="Parameter category"
    )
    requires: str | None = Field(
        default=None, description="Parent field that must be enabled (e.g., 'autoscaler_enabled')"
    )
    tips: list[str] = Field(default_factory=list, description="Usage tips and recommendations")


class FlinkCategoryInfo(BaseModel):
    """Documentation for a parameter category."""

    title: str = Field(description="Category display title")
    description: str = Field(description="Category description")
    order: int = Field(description="Display order (lower = first)")


class FlinkDefaultsSchemaResponse(BaseModel):
    """Full schema documentation for Flink configuration (for UI)."""

    parameters: list[FlinkParameterInfo] = Field(description="List of configurable parameters")
    categories: dict[str, FlinkCategoryInfo] = Field(description="Parameter categories")
    impact_descriptions: dict[str, str] = Field(description="Descriptions for impact types")


class AddRepositoriesRequest(BaseModel):
    """Base request for adding repositories."""

    repository_ids: list[str] = Field(description="List of repository IDs to add", min_length=1)


class AddRepositoriesResponse(BaseModel):
    """Base response after adding repositories."""

    added: list[str] = Field(description="Repository IDs that were added")
    skipped: list[str] = Field(description="Repository IDs that were already in database")


class AddExternalRepositoriesRequest(AddRepositoriesRequest):
    """Request to add external repositories by ID."""

    pass


class AddExternalRepositoriesResponse(AddRepositoriesResponse):
    """Response after adding external repositories."""

    pass


class RepositorySyncToggleRequest(BaseModel):
    """Request to toggle repository sync."""

    sync_enabled: bool = Field(description="True to enable sync, False to disable")


class RepositorySyncToggleResponse(BaseModel):
    """Response after toggling repository sync."""

    message: str = Field(description="Success message")
    sync_enabled: bool = Field(description="Current sync status")


class ExternalRepositoryResponse(BaseModel):
    """External repository response with id and name."""

    id: str = Field(description="Repository ID")
    name: str = Field(description="Repository name")
    source_link: str = Field(description="Source link to the external repository")
    is_added: bool = Field(description="True if repository exists in database")


class ExternalRepositoryListResponse(BaseModel):
    """List of external repositories."""

    data: list[ExternalRepositoryResponse] = Field(description="List of repositories")


# Rules Schemas
class RuleCreateRequest(BaseModel, extra="forbid"):
    """Request to create a new detection rule."""

    name: str = Field(description="Rule display name")
    body: str = Field(description="Sigma rule definition in YAML format")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Suspicious PowerShell Execution",
                    "body": "title: Suspicious PowerShell\nstatus: experimental\nlogsource:\n  product: windows\n  service: powershell\ndetection:\n  selection:\n    EventID: 4104\n  condition: selection",
                }
            ]
        }
    }


class RuleUpdateRequest(BaseModel, extra="forbid"):
    """Partial update for a detection rule. Only provided fields are changed."""

    name: str | None = Field(default=None, description="New rule display name")
    body: str | None = Field(default=None, description="New Sigma rule definition in YAML format")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "Suspicious PowerShell Execution (v2)",
                    "body": "title: Suspicious PowerShell\nstatus: stable\nlogsource:\n  product: windows\n  service: powershell\ndetection:\n  selection:\n    EventID: 4104\n  condition: selection",
                }
            ]
        }
    }


class ShortRuleDetailResponse(BaseModel, extra="ignore"):
    """Compact rule row (no body) used in lists and linked-rule views."""

    id: str = Field(description="Rule UUID")
    name: str = Field(description="Rule display name")
    version: int = Field(default=1, description="Current rule version number (incremented on body change)")
    repository_id: str = Field(description="UUID of the repository that owns the rule")
    repository_type: str = Field(description="Repository type: socprime, external, or local")
    repository_name: str = Field(description="Display name of the repository the rule belongs to")
    created: datetime.datetime = Field(description="Creation timestamp (UTC)")
    updated: datetime.datetime = Field(description="Last update timestamp (UTC)")
    # Declared on the base class deliberately: ``ActiveThreatResponse.rules`` is annotated
    # list[ShortRuleDetailResponse], and Pydantic serializes by the declared type — a field
    # added only to RuleDetailResponse would be dropped from that response.
    level: str | None = Field(
        default=None,
        description="Sigma severity from the rule body (informational, low, medium, high, critical); "
        "null when the body declares no level",
    )


class ActiveThreatLinkedRuleResponse(ShortRuleDetailResponse):
    """Linked rule on an Active Threat detail, plus its Issue Navigator subtitle when any."""

    rule_check_results_reason: str | None = Field(
        default=None,
        description=(
            "Issue Navigator subtitle for this rule, or null when it has no readiness issue. "
            "One reason per rule: unsupported wins over not connected, which wins over disabled"
        ),
    )


class RuleDetailResponse(ShortRuleDetailResponse):
    """Rule detail row: taxonomy, identifiers, and Sigma validation state (no body)."""

    product: str | None = Field(default=None, description="Sigma logsource product (e.g. windows)")
    service: str | None = Field(default=None, description="Sigma logsource service (e.g. powershell)")
    category: str | None = Field(default=None, description="Sigma logsource category (e.g. process_creation)")
    folder_path: str | None = Field(default=None, description="Folder path of the rule within its repository")
    sigma_uuid: str | None = Field(default=None, description="UUID declared inside the Sigma body (id field)")
    external_id: str | None = Field(default=None, description="Source identifier in the external system (e.g. TDM)")
    # Sigma validation fields
    is_supported: bool = Field(default=True, description="Whether the rule is supported by the current matcher")
    unsupported_reason: str | None = Field(default=None, description="Reason the rule is not supported, if any")
    # True when a user has edited the rule body in a system repository;
    # such rules are not overwritten by Active Threat sync.
    is_user_edited_system_rule: bool = Field(
        default=False,
        description="True when a user edited the body of a system-repository rule; "
        "such rules are not overwritten by Active Threat sync",
    )


class RuleFullDetailResponse(RuleDetailResponse):
    """Complete rule detail including the YAML body and optional version history."""

    body: str = Field(
        description=(
            "Full Sigma rule definition in YAML format. Empty when "
            "HIDE_RESTRICTED_RULE_BODIES is enabled and the rule is not from a "
            "local or external (opensource GitHub) repository"
        )
    )
    content_hash: str | None = Field(default=None, description="Hash of the current body, used for change detection")
    versions: list["RuleVersionItem"] | None = Field(
        default=None, description="Version history metadata; populated only when versions are requested"
    )


class RuleListResponse(BaseModel):
    """Paginated list of detection rules."""

    total: int = Field(description="Total number of matching rules")
    limit: int = Field(description="Page size")
    offset: int = Field(description="Offset from start")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[RuleDetailResponse] = Field(description="Rules on this page")


class RuleVersionItem(BaseModel):
    """Single version metadata entry (without body)."""

    version: int = Field(description="Version number")
    content_hash: str = Field(description="Hash of the body at this version")
    changed_at: datetime.datetime = Field(description="When this version was recorded (UTC)")
    change_source: str | None = Field(default=None, description="What produced the version (e.g. user edit, sync)")
    changed_by: str | None = Field(default=None, description="User who made the change, when applicable")


class RuleVersionDetailResponse(BaseModel):
    """Full version detail including body."""

    rule_id: str = Field(description="UUID of the rule the version belongs to")
    version: int = Field(description="Version number")
    body: str = Field(
        description=(
            "Sigma rule body (YAML) at this version. Empty when "
            "HIDE_RESTRICTED_RULE_BODIES is enabled and the rule is not from a "
            "local or external (opensource GitHub) repository"
        )
    )
    content_hash: str = Field(description="Hash of the body at this version")
    changed_at: datetime.datetime = Field(description="When this version was recorded (UTC)")
    change_source: str | None = Field(default=None, description="What produced the version (e.g. user edit, sync)")
    changed_by: str | None = Field(default=None, description="User who made the change, when applicable")


class RuleVersionsListResponse(BaseModel):
    """List of rule version metadata."""

    rule_id: str = Field(description="UUID of the rule")
    current_version: int = Field(description="The rule's current version number")
    versions: list[RuleVersionItem] = Field(description="Version history entries, newest first")


class RuleRollbackRequest(BaseModel, extra="forbid"):
    """Request to rollback a rule to a specific version."""

    version: int = Field(description="Version number to rollback to")


class RuleBulkCreateRequest(BaseModel, extra="forbid"):
    """Request to create multiple detection rules in one local repository."""

    rules: list[RuleCreateRequest] = Field(
        description="Rules to create (max 200 per request)",
        min_length=1,
        max_length=200,
    )


class RuleBulkCreatedItem(BaseModel):
    """One created rule summary returned from bulk create."""

    id: str = Field(description="Rule UUID")
    name: str = Field(description="Rule display name")
    is_supported: bool = Field(description="Whether the rule is supported by the current matcher after validation")


class RuleBulkCreateResponse(BaseModel):
    """Response from bulk rule creation."""

    total: int = Field(description="Total number of created rules")
    supported: int = Field(description="Number of supported rules")
    data: list[RuleBulkCreatedItem] = Field(description="Created rules with id, name, and support status")


# Sigma Validation Schemas
class SigmaValidateRequest(BaseModel):
    """Request to validate a sigma rule."""

    sigma_text: str = Field(description="Sigma rule YAML text to validate")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "sigma_text": "title: Test Rule\nstatus: experimental\nlogsource:\n  product: windows\ndetection:\n  selection:\n    EventID: 4104\n  condition: selection"
                }
            ]
        }
    }


class SigmaValidateResponse(BaseModel):
    """Response from sigma rule validation."""

    is_supported: bool = Field(description="Whether the rule is supported by the current matcher")
    unsupported_reason: str | None = Field(None, description="Reason why the rule is not supported")
    unsupported_labels: list[str] = Field(
        default_factory=list, description="List of unsupported features found in the rule"
    )
    validator_version: str | None = Field(None, description="Version of the sigma validator used")


class SigmaValidateBulkRequest(BaseModel):
    """Request to validate multiple sigma rules."""

    rules: list[SigmaValidateRequest] = Field(description="List of sigma rules to validate", max_length=100)


class SigmaValidateBulkItemResponse(BaseModel):
    """Validation result for a single rule in bulk validation."""

    index: int = Field(description="Index of the rule in the request list (0-based)")
    is_supported: bool = Field(description="Whether the rule is supported")
    unsupported_reason: str | None = Field(None, description="Reason why the rule is not supported")
    unsupported_labels: list[str] = Field(default_factory=list, description="List of unsupported features")


class SigmaValidateBulkResponse(BaseModel):
    """Response from bulk sigma rule validation."""

    total: int = Field(description="Total number of rules validated")
    supported: int = Field(description="Number of supported rules")
    unsupported: int = Field(description="Number of unsupported rules")
    validator_version: str | None = Field(None, description="Version of the sigma validator used")
    results: list[SigmaValidateBulkItemResponse] = Field(description="Validation results per rule")


# Parsers Schemas
class ParserDetailResponse(BaseModel):
    """Event parser detail with its parsing query."""

    id: str = Field(description="Parser UUID")
    name: str = Field(description="Parser display name")
    created: str = Field(description="Creation timestamp (ISO 8601)")
    updated: str = Field(description="Last update timestamp (ISO 8601)")
    parser_query: str = Field(description="Event-parser DSL query that transforms raw events")


class ParserSimpleResponse(BaseModel):
    """Minimal parser reference (id and name)."""

    id: str = Field(description="Parser UUID")
    name: str = Field(description="Parser display name")


class ParserListResponse(BaseModel):
    """Paginated list of event parsers."""

    total: int = Field(description="Total number of matching parsers")
    page: int = Field(description="Current page number")
    limit: int = Field(description="Page size")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Offset from start")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[ParserDetailResponse] = Field(description="Parsers on this page")


class ParserCreateRequest(BaseModel):
    """Request to create a new event parser."""

    name: str = Field(description="Parser display name")
    parser_query: str = Field(description="Event-parser DSL query that transforms raw events")
    parser_config: dict[str, Any] | None = Field(default=None, description="Optional parser configuration options")


class ParserUpdateRequest(BaseModel):
    """Partial update for an event parser. Only provided fields are changed."""

    name: str | None = Field(default=None, description="New parser display name")
    parser_query: str | None = Field(default=None, description="New event-parser DSL query")


class ParserResponse(ActionResponse):
    """Action result for parser create/update/delete operations."""

    pass


class ParserRunRequest(BaseModel):
    """Request to test-run a parser query against sample events from source topics."""

    parser_query: str = Field(description="Event-parser DSL query to test-run")
    source_topic_ids: list[str] = Field(description="Source topic IDs to pull sample events from")
    mapping: str | None = Field(default=None, description="Optional field mapping applied to parsed output")


class ParserRunResultItem(BaseModel):
    """Parsing result for a single sample event."""

    source_topic: str = Field(description="Source topic the sample event came from")
    source_data: str = Field(description="Raw sample event used as parser input")
    parsed_data: dict[str, Any] | None = Field(description="Parsed output fields, or null when parsing failed")
    success: bool = Field(description="Whether the parser ran successfully on this event")
    error_message: str | None = Field(description="Error message when parsing failed, otherwise null")


class ParserRunResponse(BaseModel):
    """Response from a parser test-run."""

    result: list[ParserRunResultItem] = Field(description="Per-event parsing results")


# Log Sources Schemas
class LogSourceResponse(BaseModel):
    """Log source detail: parser configuration and field mapping."""

    id: str = Field(description="Log source UUID")
    name: str = Field(description="Log source display name")
    parsing_script: str | None = Field(default=None, description="Event-parser DSL script applied to incoming events")
    parsing_config: dict[str, Any] | None = Field(default=None, description="Optional parser configuration options")
    mapping: str | None = Field(default=None, description="Sigma-to-event field mapping definition")
    test_topics: list[str] | None = Field(default=None, description="Source topic IDs used for parser/mapping testing")
    test_repository_ids: list[RepositorySelection] | None = Field(
        default=None, description="Repository selections (id/name) used for parser/mapping testing"
    )
    created: str = Field(description="Creation timestamp (ISO 8601)")
    updated: str = Field(description="Last update timestamp (ISO 8601)")


class LogSourceListResponse(BaseModel):
    """Paginated list of log sources."""

    total: int = Field(description="Total number of matching log sources")
    page: int = Field(description="Current page number")
    limit: int = Field(description="Page size")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Offset from start")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[LogSourceResponse] = Field(description="Log sources on this page")


class LogSourceCreateRequest(BaseModel):
    """Request to create a new log source."""

    name: str = Field(description="Log source display name")
    parsing_script: str | None = Field(default=None, description="Event-parser DSL script applied to incoming events")
    mapping: str | None = Field(default=None, description="Sigma-to-event field mapping definition")
    test_topics: list[str] | None = Field(default=None, description="Source topic IDs used for parser/mapping testing")
    test_repository_ids: list[RepositorySelection] | None = Field(
        default=None, description="Repository selections (id/name) used for parser/mapping testing"
    )

    @field_validator("test_repository_ids", mode="before")
    @classmethod
    def validate_test_repository_selections(cls, v: Any) -> list[dict[str, Any]] | None:
        return normalize_repository_selections(v, field_name="test_repository_ids")


class LogSourceUpdateRequest(BaseModel):
    """Partial update for a log source. Only provided fields are changed."""

    name: str | None = Field(default=None, description="New log source display name")
    parsing_script: str | None = Field(default=None, description="New event-parser DSL script")
    mapping: str | None = Field(default=None, description="New Sigma-to-event field mapping definition")
    test_topics: list[str] | None = Field(default=None, description="Source topic IDs used for parser/mapping testing")
    test_repository_ids: list[RepositorySelection] | None = Field(
        default=None, description="Repository selections (id/name) used for parser/mapping testing"
    )

    @field_validator("test_repository_ids", mode="before")
    @classmethod
    def validate_test_repository_selections(cls, v: Any) -> list[dict[str, Any]] | None:
        return normalize_repository_selections(v, field_name="test_repository_ids")


class LogSourceActionResponse(ActionResponse):
    """Action result for log-source create/update/delete operations."""

    pass


# User Schemas
class UserCreateRequest(BaseModel):
    """Request to create a new user account (Admin only)."""

    full_name: str = Field(description="User's full name")
    email: str = Field(description="User's email address (used for login)")
    password: str = Field(description="Initial password")
    is_active: bool = Field(default=True, description="Account active status")
    role: str = Field(default=UserRole.ADMIN, description="User role: admin, user, or viewer")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "full_name": "John Doe",
                    "email": "john.doe@example.com",
                    "password": "SecurePass123",
                    "is_active": True,
                    "role": "user",
                }
            ]
        }
    }

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        if "@" not in v:
            raise ValueError("Invalid email format")
        return v.lower()


class UserUpdateRequest(BaseModel):
    """Request to update a user account (Admin only). All fields are optional."""

    full_name: str | None = Field(default=None, description="User's full name")
    email: str | None = Field(default=None, description="User's email address; must contain '@'")
    password: str | None = Field(default=None, description="New password")
    is_active: bool | None = Field(default=None, description="Account active status")
    role: str | None = Field(default=None, description="User role: admin, user, or viewer")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "full_name": "John Doe",
                    "email": "john.doe@example.com",
                    "is_active": True,
                    "role": "user",
                }
            ]
        }
    }

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str | None) -> str | None:
        """Validate email format."""
        if v is not None and "@" not in v:
            raise ValueError("Invalid email format")
        return v.lower() if v else None


class UserDetailResponse(BaseModel):
    """User account detail."""

    id: str = Field(description="User UUID")
    full_name: str = Field(description="User's full name")
    email: str = Field(description="User's email address (login identifier)")
    is_active: bool = Field(description="Whether the account is active")
    role: str = Field(description="User role: admin, user, or viewer")
    must_change_password: bool = Field(description="True when the user must change their password at next login")
    created: datetime.datetime = Field(description="Account creation timestamp")
    updated: datetime.datetime = Field(description="Last update timestamp")


class UserListResponse(BaseModel):
    """Paginated list of user accounts."""

    total: int = Field(description="Total number of matching users")
    limit: int = Field(description="Page size")
    offset: int = Field(description="Offset from start")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[UserDetailResponse] = Field(description="Users on this page")


# Auth Schemas
class LoginRequest(BaseModel):
    """User login credentials."""

    email: str = Field(description="User email address")
    password: str = Field(description="User password")

    model_config = {"json_schema_extra": {"examples": [{"email": "user@example.com", "password": "SecurePass123"}]}}

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        if "@" not in v:
            raise ValueError("Invalid email format")
        return v.lower()


class LoginResponse(BaseModel):
    """Successful login response with JWT token."""

    access_token: str = Field(description="JWT access token for API authentication")
    token_type: str = Field(default="bearer", description="Token type (always 'bearer')")
    user: UserDetailResponse = Field(description="Authenticated user details")


class LogoutResponse(BaseModel):
    """Logout confirmation response."""

    message: str = Field(description="Logout status message")


class RefreshResponse(BaseModel):
    """Token refresh response with new access token."""

    access_token: str = Field(description="New JWT access token")
    token_type: str = Field(default="bearer", description="Token type (always 'bearer')")


class ProfileUpdateRequest(BaseModel):
    """Profile update request."""

    full_name: str = Field(description="User's full name")

    model_config = {"json_schema_extra": {"examples": [{"full_name": "John Doe"}]}}


class ChangePasswordRequest(BaseModel):
    """Password change request."""

    current_password: str = Field(description="Current password for verification")
    new_password: str = Field(description="New password (minimum 8 characters; must differ from the current one)")
    confirm_password: str = Field(description="New password confirmation (must match)")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "current_password": "OldPass123",
                    "new_password": "NewSecure456",
                    "confirm_password": "NewSecure456",
                }
            ]
        }
    }


class ChangePasswordResponse(BaseModel):
    """Password change confirmation."""

    message: str = Field(description="Password change status message")


class ResetPasswordResponse(BaseModel):
    """Admin password reset response."""

    temporary_password: str = Field(description="Generated temporary password")
    message: str = Field(description="Reset status message")


# =============================================================================
# Activity / Audit Log Schemas
# =============================================================================


class ActivityEvent(BaseModel):
    """Activity event format for Kafka topic detectflow.activity.

    Used by both producer (Backend/Flink) and consumer.
    """

    id: str = Field(description="Unique event ID (UUID)")
    timestamp: datetime.datetime = Field(description="Event timestamp")

    # Action
    action: str = Field(description="Action type: create, update, delete, toggle, error, sync, login")

    # Entity
    entity_type: str = Field(description="Entity type: pipeline, rule, filter, repository, topic, node, user")
    entity_id: str | None = Field(default=None, description="Entity UUID")
    entity_name: str | None = Field(default=None, description="Entity name for display")

    # User (None for system/flink events)
    user_id: str | None = Field(default=None, description="User UUID who performed the action")
    user_email: str | None = Field(default=None, description="User email for display")

    # Details
    details: str | None = Field(default=None, description="Human-readable description")
    changes: dict | None = Field(default=None, description="Field changes: {field: {old, new}}")

    # Source
    source: str = Field(default="user", description="Event source: user, flink, system")

    # Severity level
    severity: str = Field(default=AuditSeverity.INFO, description="Severity level: info, warning, error")


class ActivityItem(BaseModel):
    """Unified activity/audit item for all endpoints.

    Used for:
    - Dashboard recent activity (SSE and REST)
    - Audit logs API responses
    """

    id: str = Field(description="Unique event ID (UUID)")
    timestamp: str = Field(description="ISO 8601 format timestamp")
    action: str = Field(description="Action type: create, update, delete, toggle, error, sync, login")
    entity_type: str = Field(description="Entity type: pipeline, rule, filter, repository, topic, node, user")
    entity_id: str | None = Field(default=None, description="Entity UUID")
    entity_name: str | None = Field(default=None, description="Entity name for display")
    user_id: str | None = Field(default=None, description="UUID of the user who performed the action")
    user_email: str | None = Field(default=None, description="Email of the user who performed the action")
    details: str | None = Field(default=None, description="Human-readable description of the event")
    source: str = Field(default="user", description="Event source: user, flink, or system")
    severity: str = Field(default=AuditSeverity.INFO, description="Severity level: info, warning, or error")


# Backward compatibility alias
AuditLogItem = ActivityItem


class AuditLogsResponse(BaseModel):
    """Paginated audit logs response."""

    total: int = Field(description="Total number of matching records")
    limit: int = Field(description="Page size")
    offset: int = Field(description="Offset from start")
    data: list[AuditLogItem] = Field(description="Audit log entries")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "total": 1,
                    "limit": 50,
                    "offset": 0,
                    "data": [
                        {
                            "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
                            "timestamp": "2026-06-04T12:00:00Z",
                            "action": "update",
                            "entity_type": "pipeline",
                            "entity_id": "a1b2c3d4-0000-0000-0000-000000000000",
                            "entity_name": "windows-edr",
                            "user_id": "00000000-0000-0000-0000-000000000001",
                            "user_email": "admin@example.com",
                            "details": "Updated pipeline parallelism",
                            "source": "user",
                            "severity": "info",
                        }
                    ],
                }
            ]
        }
    }


class AuditLogsFiltersResponse(BaseModel):
    """Available filter options for audit logs."""

    actions: list[str] = Field(description="Available action types")
    entity_types: list[str] = Field(description="Available entity types")
    severities: list[str] = Field(description="Available severity levels")


# RecentActivityItem removed - use ActivityItem instead


class HealthCheckSingleCheck(BaseModel):
    """Single check result for health check."""

    status: HealthCheckStatus = Field(description="Check status: Operational, Warning, Error, or Not Enabled")
    title: str = Field(description="Check display title")
    descriptions: list[str] = Field(description="Human-readable detail lines for the check result")
    updated: datetime.datetime = Field(description="Timestamp of the check result")


class HealthCheckPlatformStatus(BaseModel):
    """Status of one platform (from DB)."""

    name: str = Field(description="Platform name")
    checks: list[HealthCheckSingleCheck] = Field(description="List of checks for this platform")
    updated: datetime.datetime | None = Field(default=None, description="Last row update time")


class HealthCheckAllResponse(BaseModel):
    """Response for GET /all and POST /check_now/all: status of all platforms and versions.

    platforms: last saved status per platform (name, checks, updated).
    versions: detectflow_backend_version (str) — backend app version from settings;
              match_node (dict[str, str | None]) — pipeline name → last known Flink job version (detectflow_matchnode_version).
    """

    platforms: list[HealthCheckPlatformStatus] = Field(description="Status of each platform")
    versions: dict[str, str | dict[str, str | None]] = Field(
        default_factory=dict,
        description="detectflow_backend_version (str), match_node (dict pipeline_name -> detectflow_matchnode_version)",
    )


# =============================================================================
# Integration Schemas
# =============================================================================


class IntegrationTypeItem(BaseModel):
    """One integration type in the catalog (Available tab)."""

    integration_type: str = Field(description="Unique type key (e.g. 'splunk')")
    display_name: str = Field(description="Human-readable name")
    category: str = Field(
        description="Category key for UI filter chips (soc_prime, siem, edr, database, llm_firewall, git_hosting, other)",
    )
    description: str = Field(description="Short description for the card")
    icon: str = Field(description="Icon identifier for frontend")
    capabilities: list[str] = Field(
        description="Capability keys from the provider (e.g. check_connection, deploy_rules, search)",
    )
    connected_count: int = Field(default=0, description="Number of connected profiles of this type")
    singleton_profile: bool = Field(
        default=False,
        description="If true, only one connected profile of this type is allowed system-wide",
    )


class IntegrationTypeListResponse(BaseModel):
    """Response for GET /integrations/types."""

    data: list[IntegrationTypeItem] = Field(description="Catalog of available integration types")


class IntegrationTypeDetailResponse(IntegrationTypeItem):
    """Detailed response with JSON Schema for dynamic form rendering.

    ``config_schema`` is the provider's config model schema plus a required form-only
    ``profile_name`` property (title "Profile Name", ``ui_order: 0``) prepended by
    ``with_profile_name_field``. The manager maps ``config.profile_name`` to the
    ``integrations.name`` column on create/update and ``GET /{id}`` re-injects it for the edit
    form — it is never stored in the config JSONB.
    """

    config_schema: dict = Field(description="JSON Schema for the config form fields")
    sensitive_fields: list[str] = Field(description="Field names that contain secrets")


class IntegrationListItem(BaseModel):
    """One row in the Connected tab table."""

    id: str = Field(description="Integration profile id")
    name: str = Field(description="Profile display name")
    integration_type: str = Field(description="Integration type key (e.g. 'splunk')")
    integration_type_display: str = Field(description="Human-readable integration type name")
    category: str = Field(description="Category key (soc_prime, siem, edr, database, llm_firewall, git_hosting, other)")
    icon: str = Field(description="Icon identifier for frontend")
    status: str = Field(description="Connection status: connected, disconnected, or error")
    status_message: str | None = Field(default=None, description="Detail for the current status (e.g. error reason)")
    created: datetime.datetime = Field(description="When the profile was created")
    updated: datetime.datetime = Field(description="When the profile was last updated")


class IntegrationListResponse(BaseModel):
    """Paginated response for GET /integrations."""

    total: int = Field(description="Total number of integration profiles matching the query")
    page: int = Field(description="Current page number")
    limit: int = Field(description="Maximum items per page")
    sort: str = Field(description="Field the results are sorted by")
    offset: int = Field(description="Number of items skipped before this page")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[IntegrationListItem] = Field(description="Connected integration profiles for this page")


class IntegrationDetailResponse(BaseModel):
    """Full profile detail (config with sensitive fields masked)."""

    id: str = Field(description="Integration profile id")
    name: str = Field(description="Profile display name")
    integration_type: str = Field(description="Integration type key (e.g. 'splunk')")
    integration_type_display: str = Field(description="Human-readable integration type name")
    category: str = Field(description="Category key (soc_prime, siem, edr, database, llm_firewall, git_hosting, other)")
    icon: str = Field(description="Icon identifier for frontend")
    status: str = Field(description="Connection status: connected, disconnected, or error")
    status_message: str | None = Field(default=None, description="Detail for the current status (e.g. error reason)")
    config: dict = Field(description="Provider-specific config with sensitive values masked as '******'")
    capabilities: list[str] = Field(
        description="Same capability keys as catalog (check_connection, deploy_rules, search as applicable)",
    )
    last_checked_at: datetime.datetime | None = Field(
        default=None, description="When the connection was last verified, if ever"
    )
    created: datetime.datetime = Field(description="When the profile was created")
    updated: datetime.datetime = Field(description="When the profile was last updated")


class IntegrationCreateRequest(BaseModel):
    """Request to create a new integration profile.

    The display name comes from either the top-level ``name`` or the form-rendered
    ``config.profile_name`` (the manager maps the latter to the name column); at least one must be
    non-empty. ``name`` wins when both are sent.
    """

    name: str | None = Field(
        default=None,
        max_length=255,
        description="Profile display name (≤255 chars); optional when config.profile_name is provided",
        examples=["My Splunk Cloud"],
    )
    integration_type: str = Field(
        ...,
        description="Integration type key (e.g., 'splunk')",
        examples=["splunk"],
    )
    config: dict = Field(
        ...,
        description=(
            "Provider-specific configuration (may include 'profile_name'); keys and required fields come from "
            "the type's config_schema. Sensitive values are stored encrypted and masked in responses."
        ),
        examples=[
            {
                "splunk_url": "https://your-splunk.example.com:8000",
                "host": "your-splunk.example.com",
                "port": 8089,
                "token": "your-api-token",
                "index_names": ["main"],
            }
        ],
    )


class IntegrationUpdateRequest(BaseModel):
    """Request to update an integration profile. All fields optional."""

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="New profile display name (1–255 chars)"
    )
    config: dict | None = Field(
        default=None, description="Updated config (sensitive fields can be '******' to keep existing)"
    )


class IntegrationResponse(ActionResponse):
    """Action response for integration CRUD operations."""

    pass


class ConnectionCheckRequest(BaseModel):
    """Request to check connection without saving."""

    integration_type: str = Field(..., description="Integration type key")
    config: dict = Field(..., description="Config to test")


class ConnectionCheckResponse(BaseModel):
    """Response from a connection check."""

    success: bool = Field(description="True when the provider could reach and authenticate to the target")
    message: str = Field(description="Human-readable result or error summary")
    details: dict | None = Field(default=None, description="Optional provider-specific diagnostic details")


class IntegrationDeployRequest(BaseModel):
    """Body for ``POST /api/v1/integrations/{integration_id}/deploy`` (``deploy_rules`` capability)."""

    rules: list[str | dict[str, Any]] = Field(
        ...,
        description="Rule payloads as either a YAML string or a structured dict; interpretation is provider-specific",
    )
    params: dict[str, Any] | None = Field(
        default=None,
        description="Optional provider-specific options (workspace, folder, overwrite, …)",
    )


class IntegrationDeployResponse(BaseModel):
    """HTTP response for deploy; built from provider :class:`~apps.integrations.base.DeployResult`.

    ``items`` lists per-rule outcomes when the provider supplies them (e.g. Sentinel).
    Provider-specific fields are preserved via ``extra="allow"``.
    """

    model_config = ConfigDict(extra="allow")

    status: bool | None = Field(default=None, description="Overall success when provider returns it")
    message: str | None = Field(default=None, description="Summary or error message")
    items: list[Any] | None = Field(
        default=None,
        description="Per-rule or per-operation outcome entries when the provider returns them",
    )


class IntegrationSearchRequest(BaseModel):
    """Body for ``POST /api/v1/integrations/{integration_id}/search`` (``search`` capability)."""

    query: str | None = Field(default=None, description="Free-text search when the provider supports it")
    external_ids: list[str] | None = Field(
        default=None,
        description="Restrict to these external ids when the API supports it",
    )
    cursor: str | None = Field(default=None, description="Opaque pagination cursor from a prior response")
    limit: int = Field(default=100, ge=1, le=1000, description="Max items to return")
    params: dict[str, Any] | None = Field(default=None, description="Provider-specific filters")


class IntegrationSearchResponse(BaseModel):
    """HTTP response for search; built from provider :class:`~apps.integrations.base.SearchResult`.

    Provider-specific fields are preserved via ``extra="allow"``.
    """

    model_config = ConfigDict(extra="allow")

    status: bool | None = Field(default=None, description="Overall success when provider returns it")
    message: str | None = Field(default=None, description="Summary or error message")
    items: list[Any] | None = Field(default=None, description="Result entries when present")
    next_cursor: str | None = Field(default=None, description="Cursor for the next page if any")


class IntegrationViewInSiemRequest(BaseModel):
    """Body for ``POST /api/v1/integrations/{integration_id}/view-in-siem`` (``view_in_siem`` capability)."""

    chain_id: UUID = Field(..., description="Attack chain to build the SIEM search deep link for")
    sigma_rule_id: UUID | None = Field(
        default=None,
        description=(
            "Optional: narrow the link to a single matched Sigma rule in the chain (per-rule "
            "'View in SIEM'). Omit to aggregate all rules matched in the chain. Must be a rule that "
            "matched within the chain."
        ),
    )
    params: dict[str, Any] | None = Field(default=None, description="Optional provider-specific overrides")


class IntegrationViewInSiemResponse(BaseModel):
    """HTTP response for view-in-SIEM; built from provider :class:`~apps.integrations.base.ViewInSiemResult`.

    On success ``url`` is a ready-to-open deep link into the SIEM search UI and ``query`` exposes the
    generated query text. Provider-specific fields are preserved via ``extra="allow"``.
    """

    model_config = ConfigDict(extra="allow")

    status: bool | None = Field(default=None, description="Overall success when provider returns it")
    message: str | None = Field(default=None, description="Summary or error message")
    url: str | None = Field(default=None, description="Deep link that opens the SIEM running the query")
    query: str | None = Field(default=None, description="Generated query text (preview/debug)")


# Active threats and High Order Sigmas (TDM catalog)
class HighOrderSigmaRuleCheckRuleResponse(BaseModel):
    """One Sigma rule referenced in an active-threat readiness check."""

    rule_id: str = Field(description="Rule identifier")
    rule_name: str = Field(description="Rule display name")
    description: str = Field(default="", description="Human-readable readiness issue description")


class HighOrderSigmaRuleCheckPipelineResponse(BaseModel):
    """One pipeline referenced in an active-threat readiness check."""

    id: str = Field(description="Pipeline id")
    name: str = Field(description="Pipeline display name")


class HighOrderSigmaDisabledRuleResponse(HighOrderSigmaRuleCheckRuleResponse):
    """A rule that is present but disabled, with the pipelines where it is disabled."""

    pipelines: list[HighOrderSigmaRuleCheckPipelineResponse] = Field(
        default_factory=list, description="Pipelines in which this rule is disabled"
    )


class RuleIssueResponse(BaseModel):
    """Readiness issues for an active threat's linked rules, grouped by problem type."""

    unsupported_rules: list[HighOrderSigmaRuleCheckRuleResponse] = Field(
        default_factory=list, description="Rules the match node cannot run (unsupported Sigma features)"
    )
    not_connected_rules: list[HighOrderSigmaRuleCheckRuleResponse] = Field(
        default_factory=list, description="Rules not connected to any pipeline"
    )
    disabled_rules: list[HighOrderSigmaDisabledRuleResponse] = Field(
        default_factory=list, description="Rules connected but disabled in their pipelines"
    )

    def unique_rule_count(self) -> int:
        """How many distinct rules appear in any readiness bucket.

        A rule can land in more than one list (unsupported *and* disabled); the badge counts
        the rule once.
        """
        return len(
            {rule.rule_id for rule in self.unsupported_rules}
            | {rule.rule_id for rule in self.not_connected_rules}
            | {rule.rule_id for rule in self.disabled_rules}
        )

    def subtitles_by_rule_id(self) -> dict[str, str]:
        """Issue Navigator subtitle text keyed by ``rules.id``.

        One reason per rule, highest bucket wins: unsupported > not connected > disabled.
        Disabled copy is the bucket ``description`` plus pipeline names.
        """
        subtitles: dict[str, str] = {}

        def add(rule_id: str, text: str) -> None:
            if text and rule_id not in subtitles:
                subtitles[rule_id] = text

        for rule in self.unsupported_rules:
            add(rule.rule_id, rule.description)
        for rule in self.not_connected_rules:
            add(rule.rule_id, rule.description)
        for rule in self.disabled_rules:
            add(rule.rule_id, self._disabled_subtitle(rule))
        return subtitles

    @staticmethod
    def _disabled_subtitle(rule: HighOrderSigmaDisabledRuleResponse) -> str:
        names = ", ".join(pipeline.name for pipeline in rule.pipelines if pipeline.name)
        if names:
            return f"{rule.description} {names}".strip()
        return rule.description


class HighOrderSigmaListItemResponse(BaseModel):
    """Short high-order Sigma row for catalog lists."""

    id: str = Field(description="Local high-order Sigma id")
    tdm_id: str = Field(description="SOC Prime TDM identifier")
    name: str = Field(description="High-order Sigma display name")
    created: datetime.datetime = Field(description="When the row was created locally")
    updated: datetime.datetime = Field(description="When the row was last updated locally")
    is_user_edited: bool = Field(default=False, description="True when the body was edited locally (diverges from TDM)")
    level: str | None = Field(
        default=None,
        description="Sigma severity from the high-order Sigma body (informational, low, medium, high, critical); "
        "null when the body declares no level",
    )


class HighOrderSigmaListResponse(BaseModel):
    """Paginated list of high-order Sigmas."""

    total: int = Field(description="Total number of high-order Sigmas matching the query")
    limit: int = Field(description="Maximum items per page")
    offset: int = Field(description="Number of items skipped before this page")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[HighOrderSigmaListItemResponse] = Field(description="High-order Sigma rows for this page")


class HighOrderSigmaUpdateRequest(BaseModel, extra="forbid"):
    """PATCH payload for editing high-order Sigma user-managed fields."""

    name: str | None = Field(default=None, min_length=1, description="Replacement high-order Sigma display name")
    body: str | None = Field(default=None, description="Replacement YAML body for the high-order Sigma")

    @model_validator(mode="after")
    def validate_has_update(self) -> Self:
        if self.name is None and self.body is None:
            raise ValueError("At least one of name or body must be provided")
        return self


class HighOrderSigmaDetailResponse(BaseModel):
    """One high-order Sigma (standalone GET)."""

    id: str = Field(description="Local high-order Sigma id")
    tdm_id: str = Field(description="SOC Prime TDM identifier")
    name: str = Field(description="High-order Sigma display name")
    body: str | None = Field(default=None, description="High-order Sigma YAML body")
    tdm_created: datetime.datetime | None = Field(default=None, description="Creation time reported by TDM")
    tdm_updated: datetime.datetime | None = Field(default=None, description="Last-update time reported by TDM")
    created: datetime.datetime = Field(description="When the row was created locally")
    updated: datetime.datetime = Field(description="When the row was last updated locally")
    is_user_edited: bool = Field(default=False, description="True when the body was edited locally (diverges from TDM)")
    level: str | None = Field(
        default=None,
        description="Sigma severity from the high-order Sigma body (informational, low, medium, high, critical); "
        "null when the body declares no level",
    )


class MitreIdNameRef(BaseModel):
    """MITRE ATT&CK id with a display name resolved from the cached enterprise bundle."""

    id: str = Field(
        description=(
            "MITRE ATT&CK external id as stored (group G#### or technique T####); raw string if the value is not an id"
        )
    )
    name: str = Field(description="ATT&CK display name; equals ``id`` when the bundle has no match or failed to load")


class ActiveThreatListItemResponse(BaseModel):
    """Short active threat row for list views."""

    tdm_id: str = Field(description="SOC Prime TDM identifier for the active threat")
    name: str = Field(description="Active threat display name")
    is_blocked: bool = Field(
        description=(
            "True when the threat is blocked platform-wide: removed from monitoring in every "
            "correlation pipeline, now and future. Monitoring is otherwise opt-out"
        )
    )
    severity: str | None = Field(default=None, description="Severity label as reported by TDM")
    industries: list[str] | None = Field(default=None, description="Targeted industries")
    geographies: list[str] | None = Field(default=None, description="Targeted geographies/regions")
    threat_types: list[str] | None = Field(default=None, description="Threat type tags")
    threat_actors: list[MitreIdNameRef] | None = Field(
        default=None,
        description="Associated MITRE ATT&CK groups (id + name; name falls back to id)",
    )
    techniques: list[MitreIdNameRef] | None = Field(
        default=None,
        description="Associated MITRE ATT&CK techniques (id + name; name falls back to id)",
    )
    source: str | None = Field(default=None, description="Origin/source of the active threat")
    release_date: datetime.datetime = Field(description="When the active threat was released by TDM")
    is_hidden: bool = Field(default=False, description="True when the threat is hidden from default lists")
    pipelines: list["CorrelationPipelineRef"] = Field(
        default_factory=list,
        description=(
            "Correlation pipelines currently monitoring this threat (every pipeline minus the ones "
            "that excluded it). Empty when the threat is blocked platform-wide"
        ),
    )
    rule_check_results: RuleIssueResponse = Field(
        default_factory=RuleIssueResponse, description="Readiness issues for this threat's linked rules"
    )


class ActiveThreatListResponse(BaseModel):
    """Paginated list of active threats."""

    total: int = Field(description="Total number of active threats matching the query")
    limit: int = Field(description="Maximum items per page")
    offset: int = Field(description="Number of items skipped before this page")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[ActiveThreatListItemResponse] = Field(description="Active threat rows for this page")


class ActiveThreatResponse(BaseModel):
    """Active threat payload: core fields, linked rules, optional nested HoS (see ``include_all`` in converter)."""

    tdm_id: str = Field(description="SOC Prime TDM identifier for the active threat")
    name: str = Field(description="Active threat display name")
    summary: str | None = Field(default=None, description="Short summary of the threat")
    investigation: str | None = Field(default=None, description="Investigation guidance text")
    response: str | None = Field(default=None, description="Response guidance text")
    mitigation: str | None = Field(default=None, description="Mitigation guidance text")
    attack_flow: str | None = Field(default=None, description="Attack flow narrative/diagram text")
    tdm_created: datetime.datetime | None = Field(default=None, description="Creation time reported by TDM")
    tdm_updated: datetime.datetime | None = Field(default=None, description="Last-update time reported by TDM")
    created: datetime.datetime = Field(description="When the row was created locally")
    updated: datetime.datetime = Field(description="When the row was last updated locally")
    rules: list[ActiveThreatLinkedRuleResponse] = Field(
        default_factory=list,
        description=(
            "Sigma rules linked to this active threat (HOS-referenced and behavioral, deduplicated). "
            "Each row carries ``rule_check_results_reason`` (Issue Navigator subtitle, or null)"
        ),
    )
    is_blocked: bool = Field(
        description=(
            "True when the threat is blocked platform-wide: removed from monitoring in every "
            "correlation pipeline, now and future"
        )
    )
    severity: str | None = Field(default=None, description="Severity label as reported by TDM")
    industries: list[str] | None = Field(default=None, description="Targeted industries")
    geographies: list[str] | None = Field(default=None, description="Targeted geographies/regions")
    threat_types: list[str] | None = Field(default=None, description="Threat type tags")
    source: str | None = Field(default=None, description="Origin/source of the active threat")
    threat_actors: list[MitreIdNameRef] | None = Field(
        default=None,
        description="Associated MITRE ATT&CK groups (id + name; name falls back to id)",
    )
    techniques: list[MitreIdNameRef] | None = Field(
        default=None,
        description="Associated MITRE ATT&CK techniques (id + name; name falls back to id)",
    )
    is_hidden: bool = Field(default=False, description="True when the threat is hidden from default lists")
    rule_check_results: RuleIssueResponse = Field(
        default_factory=RuleIssueResponse, description="Readiness issues for this threat's linked rules"
    )
    rule_check_results_count: int = Field(
        default=0,
        description=(
            "Distinct linked rules with at least one readiness issue "
            "(unsupported, not connected, or disabled). A rule in several buckets counts once"
        ),
    )
    high_order_sigmas: list[HighOrderSigmaDetailResponse] = Field(
        default_factory=list, description="High-order Sigmas linked to this active threat (when include_all)"
    )
    pipelines: list["ActiveThreatPipelineMonitoringItem"] = Field(
        default_factory=list,
        description=(
            "EVERY correlation pipeline with this threat's monitoring state and chain breakdown; "
            "empty when the threat is embedded in another entity's response"
        ),
    )


class ActiveThreatChainStatusCounts(BaseModel):
    """Attack chains of one active threat in one correlation pipeline, grouped by workflow status.

    All seven statuses are always present and zero-filled. The UI's status bar renders six of them
    and ignores ``filtered_out``, but the counts stay complete so the total reconciles with
    ``GET /attack_chains``.
    """

    new: int = Field(default=0, description="Chains awaiting triage")
    investigating: int = Field(default=0, description="Chains currently under investigation")
    confirmed: int = Field(default=0, description="Chains confirmed as true positives")
    false_positive: int = Field(default=0, description="Chains dismissed as false positives")
    resolved: int = Field(default=0, description="Chains resolved")
    closed: int = Field(default=0, description="Chains closed")
    filtered_out: int = Field(
        default=0,
        description=(
            "Chains the LLM filtered out (status `filtered-out`). Not rendered by the status bar, "
            "but counted in `attack_chains_total` so the total reconciles with `GET /attack_chains`"
        ),
    )


class ActiveThreatPipelineMonitoringItem(BaseModel):
    """One correlation pipeline as seen from an active threat's detail page."""

    correlation_pipeline_id: UUID = Field(description="Correlation pipeline id")
    name: str = Field(description="Correlation pipeline display name")
    jobs_status: str = Field(description="Desired control state of both Flink jobs: starting, stopping, or stopped")
    runtime_state: "AttackChainsRuntimeStateResponse | None" = Field(
        default=None,
        description="Observed runtime state of this pipeline's correlation-job + agent-job, when available",
    )
    monitored: bool = Field(
        description=(
            "Whether this pipeline monitors the threat: false when the threat is blocked "
            "platform-wide or excluded from this pipeline"
        )
    )
    attack_chains_total: int = Field(
        default=0, description="Chains this pipeline produced for the threat; the sum of all seven status counts"
    )
    attack_chain_counts: ActiveThreatChainStatusCounts = Field(
        default_factory=ActiveThreatChainStatusCounts, description="Per-status chain breakdown for the status bar"
    )
    first_seen: datetime.datetime | None = Field(
        default=None,
        description=(
            "Earliest ``attack_chains.first_seen`` for this threat in this pipeline; "
            "null when the pipeline has produced no chains for it"
        ),
    )
    last_seen: datetime.datetime | None = Field(
        default=None,
        description=(
            "Latest ``attack_chains.last_seen`` for this threat in this pipeline; "
            "null when the pipeline has produced no chains for it"
        ),
    )


class ActiveThreatPipelineMonitoringRequest(BaseModel, extra="forbid"):
    """Body for setting one threat's monitoring state in one correlation pipeline."""

    monitored: bool = Field(description="True removes the exclusion row (monitor), false writes one (stop monitoring)")


class ActiveThreatPipelineMonitoringResponse(BaseModel):
    """Outcome of a per-pipeline monitoring change."""

    threat_id: str = Field(description="TDM identifier of the active threat")
    correlation_pipeline_id: UUID = Field(description="Correlation pipeline the change applied to")
    monitored: bool = Field(description="Monitoring state after the call")
    changed: bool = Field(
        description="False when the pipeline was already in that state; no chains-config record was published"
    )


class ActiveThreatBlockRequest(BaseModel, extra="forbid"):
    """Body for the platform-wide block switch."""

    blocked: bool = Field(
        description=(
            "True removes the threat from monitoring everywhere, now and future. False re-enables "
            "it in all existing pipelines by clearing every per-pipeline exclusion"
        )
    )


class ActiveThreatBlockResponse(BaseModel):
    """Outcome of a platform-wide block/unblock."""

    threat_id: str = Field(description="TDM identifier of the active threat")
    is_blocked: bool = Field(description="Block state after the call")
    changed: bool = Field(description="False when the threat was already in that state")
    pipelines_republished: int = Field(
        description=(
            "Correlation pipelines whose chains-config was republished. Always 0 for a block "
            "(exclusions are preserved so the block is reversible); on unblock, the pipelines whose "
            "exclusion of this threat was cleared"
        )
    )


class ActiveThreatBulkIdsRequest(BaseModel, extra="forbid"):
    """Body for bulk active-threat monitoring operations (per-pipeline include/exclude by id)."""

    threat_ids: list[str] = Field(
        ...,
        description="TDM identifiers (`active_threats.tdm_id`); between 1 and 500 ids",
        min_length=1,
        max_length=500,
    )
    # Required since the cutover. It shipped optional for exactly one release, where
    # omitting it meant "every correlation pipeline" — a caller who forgot the field silently
    # changed monitoring for every customer, which is why the fallback did not survive.
    # 200 mirrors the manager's pipeline fan-out limit; a request naming more has bigger problems.
    correlation_pipeline_ids: list[UUID] = Field(
        ...,
        description="Correlation pipelines to apply the change to; between 1 and 200 ids",
        min_length=1,
        max_length=200,
    )


class ActiveThreatBulkItemResult(BaseModel):
    """Per-id outcome of a bulk active-threat operation."""

    threat_id: str = Field(description="TDM identifier this result is for")
    status: Literal["updated", "not_found", "blocked"] = Field(
        description=(
            "Outcome: 'updated' when monitoring changed, 'not_found' when no row matched the id, "
            "'blocked' when the threat is blocked platform-wide and was skipped"
        )
    )


class ActiveThreatBulkResponse(BaseModel):
    """Aggregate result of a bulk active-threat operation."""

    results: list[ActiveThreatBulkItemResult] = Field(description="Per-id outcomes")
    updated: int = Field(description="Number of rows successfully updated")
    not_found: int = Field(description="Number of ids that did not match a row")
    blocked: int = Field(default=0, description="Number of ids skipped because the threat is blocked platform-wide")
    correlation_pipeline_ids: list[UUID] = Field(
        default_factory=list, description="Correlation pipelines the change was applied to"
    )
    pipelines_applied: int = Field(default=0, description="Number of correlation pipelines the change was applied to")


# =============================================================================
# Attack chains — correlation configuration (singleton)
# =============================================================================


class AttackChainConfigurationResponse(BaseModel):
    """Stored singleton row exposed to the Configure UI."""

    id: int = Field(description="Always 1 (singleton)")
    threshold_pct: int = Field(description="Coverage threshold percentage (50–100, step 5)")
    correlation_window_seconds: int = Field(description="Correlation window in seconds (30–10800)")
    auto_add_new_pipelines: bool = Field(
        description="When true, new detection pipelines matching auto_add_pipeline_name_pattern may be auto-added as members"
    )
    updated_at: datetime.datetime = Field(description="When the configuration was last updated")
    updated_by: UUID | None = Field(default=None, description="User id who last updated the configuration")


class AttackChainConfigurationUpdateRequest(BaseModel, extra="forbid"):
    """Partial update; send only fields that change.

    Membership is ``PATCH /correlation_pipelines/{id}`` with ``member_pipeline_ids``.
    Destination-topic toggles are rejected (422).
    """

    threshold_pct: int | None = Field(
        default=None,
        description="Coverage threshold 50–100, step 5 (must be a multiple of 5)",
        examples=[75],
    )
    correlation_window_seconds: int | None = Field(
        default=None,
        description="Correlation window in seconds (30–10800)",
        examples=[3600],
    )
    auto_add_new_pipelines: bool | None = Field(
        default=None,
        description="When true, new detection pipelines matching auto_add_pipeline_name_pattern may be auto-added as members",
        examples=[True],
    )

    @model_validator(mode="after")
    def at_least_one_field(self) -> Self:
        data = self.model_dump(exclude_none=True)
        if not data:
            raise ValueError("At least one field must be provided for update")
        return self

    @field_validator("threshold_pct")
    @classmethod
    def check_threshold_pct(cls, v: int | None) -> int | None:
        return validate_threshold_pct(v)

    @field_validator("correlation_window_seconds")
    @classmethod
    def check_correlation_window_seconds(cls, v: int | None) -> int | None:
        return validate_correlation_window_seconds(v)


class CorrelationJobControlResponse(BaseModel):
    """Result of correlation-job run/stop orchestration via Kubernetes Deployment.

    Retained for internal callers (pipeline orchestrator's non-critical restart
    on subscription drift). The public lifecycle is the composite
    ``/api/v1/attack_chains/runtime/{start,stop}`` — see
    :class:`AttackChainsRuntimeResponse`.
    """

    action: Literal["run", "stop"] = Field(description="Requested action: 'run' or 'stop'")
    operation: Literal["created", "updated", "deleted", "not_found"] = Field(
        description="Kubernetes operation performed: created, updated, deleted, or not_found"
    )
    deployment_name: str = Field(description="Name of the correlation-job Kubernetes Deployment")
    input_topics: list[str] = Field(
        default_factory=list, description="Kafka input topics injected into the correlation job"
    )


class AttackChainsJobState(BaseModel):
    """Per-job status observed in Kubernetes for one half of the runtime."""

    deployment_name: str = Field(description="Name of the job's Kubernetes Deployment")
    status: Literal["running", "starting", "stopping", "failed", "missing"] = Field(
        description="K8s tile state: running, starting, stopping, failed, or missing"
    )
    started_at: datetime.datetime | None = Field(default=None, description="When the job started, if running")
    error: str | None = Field(default=None, description="Error detail when the job is in a failed state")
    health: Literal["healthy", "degraded", "error"] = Field(
        default="healthy",
        description=(
            "Window-based health derived from recent Flink activity events in "
            "audit_logs (independent of the K8s tile state). 'degraded' when a "
            "recent warning was logged, 'error' on a recent error; auto-clears "
            "('healthy') once issues age out of the window."
        ),
    )
    last_issue: str | None = Field(
        default=None,
        description="Human-readable details of the most recent warning/error driving 'health'.",
    )


class AttackChainsRuntimeStateResponse(BaseModel):
    """Composite state of correlation-job + agent-job. ``starting`` means jobs are desired but not running yet."""

    overall: CorrelationPipelineStatus = Field(
        description=(
            "Composite runtime state: running, starting, stopping, stopped, partial (exactly one "
            "half failed) or failed (both did). ``stopped`` renders as 'Disabled' in the UI — a "
            "label, not a seventh state"
        )
    )
    correlation_job: AttackChainsJobState = Field(description="State of the correlation-job half")
    agent_job: AttackChainsJobState = Field(description="State of the agent-job (LLM enrichment) half")
    job_uptime: str | None = Field(
        default=None,
        description="Human-readable runtime uptime; populated only when both jobs are running",
    )


class AttackChainsRuntimeResponse(BaseModel):
    """Result of ``POST /attack_chains/runtime/{start,stop}``. ``partial`` when one half failed."""

    status: CorrelationPipelineStatus = Field(
        description=(
            "Composite outcome: running, starting, stopping, stopped, partial (exactly one half "
            "failed) or failed (both did)"
        )
    )
    correlation_job: dict = Field(description="Raw correlation-job orchestration result (provider-specific shape)")
    agent_job: dict = Field(description="Raw agent-job orchestration result (provider-specific shape)")


class AttackChainConfigurationFieldSchema(BaseModel):
    """Static UI/schema hints for one configuration field."""

    key: str = Field(description="Configuration field key this hint describes")
    title: str = Field(description="Human-readable field label")
    value_type: Literal["integer", "array", "boolean"] = Field(
        description="Value type for form rendering: integer, array, or boolean"
    )
    minimum: int | None = Field(default=None, description="Minimum allowed value (numeric fields)")
    maximum: int | None = Field(default=None, description="Maximum allowed value (numeric fields)")
    step: int | None = Field(default=None, description="Allowed increment between values (numeric fields)")
    description: str | None = Field(default=None, description="Help text for the field")


class CorrelationJobGraphPoint(BaseModel):
    """One labelled value in a correlation-job graph series."""

    name: str = Field(description="Point label (e.g. bucket name)")
    value: int = Field(description="Point value")


class CorrelationJobGraphBlock(BaseModel):
    """One graph block (series) on the correlation-job dashboard."""

    name: str = Field(description="Block/series name")
    count_all: int = Field(description="Total count across all points in the block")
    count_active: int | None = Field(default=None, description="Count of currently active items, when applicable")
    points: list[CorrelationJobGraphPoint] = Field(description="Data points in this block")


class CorrelationJobGraphData(BaseModel):
    """Platform-wide correlation dashboard graph payload."""

    blocks: list[CorrelationJobGraphBlock] = Field(
        default_factory=list,
        description=(
            "The five graph blocks (ATTACK CHAINS, MONITORED THREATS, AFFECTED HOSTNAMES, "
            "TAGGED EVENTS, MATCHED RULES) aggregated across every correlation pipeline, "
            "including chains whose producing pipeline was deleted"
        ),
    )


# =============================================================================
# Correlation pipelines (per-customer correlation runtimes)
# =============================================================================


class CorrelationPipelineRef(BaseModel):
    """Minimal reference to a correlation pipeline, for embedding in other entities' rows."""

    id: UUID = Field(description="Correlation pipeline id")
    name: str = Field(description="Correlation pipeline display name")


class CorrelationPipelineMember(BaseModel):
    """One detection pipeline that feeds a correlation pipeline."""

    pipeline_id: UUID = Field(description="Detection pipeline id — the identity of this membership")
    name: str = Field(description="Detection pipeline display name")
    destination_topic: str = Field(
        description=(
            "Destination topic of the detection pipeline. Chip label only: destination topics are "
            "not unique across pipelines, so they never identify a member"
        )
    )
    tagged_events_topic: str | None = Field(
        default=None,
        description="Per-pipeline tagged-events topic; null means it is still on the legacy shared topic",
    )
    enabled: bool = Field(description="Whether the detection pipeline itself is enabled")


class CorrelationPipelineJobResources(BaseModel):
    """Flink resources for one job of a correlation pipeline. Null fields inherit the platform default."""

    parallelism: int | None = Field(
        default=None, ge=1, le=64, description="Flink parallelism, 1–64; null = inherit the platform default"
    )
    taskmanager_memory_mb: int | None = Field(
        default=None,
        ge=1024,
        le=65536,
        description="Memory per TaskManager in MB, 1024–65536; null = inherit the platform default",
    )
    taskmanager_cpu: float | None = Field(
        default=None,
        gt=0,
        le=32,
        description="CPU cores per TaskManager, >0 and ≤32; null = inherit the platform default",
    )
    jobmanager_memory_mb: int | None = Field(
        default=None,
        ge=512,
        le=32768,
        description="JobManager memory in MB, 512–32768; null = inherit the platform default",
    )
    jobmanager_cpu: float | None = Field(
        default=None, gt=0, le=16, description="JobManager CPU cores, >0 and ≤16; null = inherit the platform default"
    )


class CorrelationPipelineCreateRequest(BaseModel, extra="forbid"):
    """Request to create a correlation pipeline."""

    name: str = Field(min_length=1, max_length=255, description="Correlation pipeline display name (unique)")
    threshold_pct: int = Field(default=60, description="Coverage threshold 50–100, step 5 (must be a multiple of 5)")
    correlation_window_seconds: int | None = Field(
        default=None,
        description=(
            "Correlation window override in seconds (30–10800). Null inherits each HoS rule's "
            "detection.timeframe; a value wins over it"
        ),
        examples=[3600],
    )
    auto_add_new_pipelines: bool = Field(
        default=False, description="When true, newly created detection pipelines may be auto-added as members"
    )
    auto_add_pipeline_name_pattern: str | None = Field(
        default=None,
        description=(
            "Glob matched case-sensitively against a new detection pipeline's name when "
            "auto_add_new_pipelines is on. Null/blank matches every new pipeline"
        ),
        examples=["cust1-*"],
    )
    pipeline_ids: list[UUID] = Field(
        default_factory=list,
        description=("Detection pipelines to make members. A pipeline may belong to several correlation pipelines"),
    )
    correlation_resources: CorrelationPipelineJobResources | None = Field(
        default=None, description="correlation-job Flink resources; null (or null fields) inherit platform defaults"
    )
    agent_resources: CorrelationPipelineJobResources | None = Field(
        default=None, description="agent-job Flink resources; null (or null fields) inherit platform defaults"
    )

    @field_validator("threshold_pct")
    @classmethod
    def check_threshold_pct(cls, v: int) -> int:
        return validate_threshold_pct(v)

    @field_validator("correlation_window_seconds")
    @classmethod
    def check_correlation_window_seconds(cls, v: int | None) -> int | None:
        return validate_correlation_window_seconds(v)

    @field_validator("auto_add_pipeline_name_pattern")
    @classmethod
    def check_auto_add_pipeline_name_pattern(cls, v: str | None) -> str | None:
        return validate_auto_add_pipeline_name_pattern(v)


class CorrelationPipelineUpdateRequest(BaseModel, extra="forbid"):
    """Partial update of a correlation pipeline; send only fields that change."""

    name: str | None = Field(
        default=None, min_length=1, max_length=255, description="Correlation pipeline display name (unique)"
    )
    threshold_pct: int | None = Field(
        default=None, description="Coverage threshold 50–100, step 5 (must be a multiple of 5)"
    )
    correlation_window_seconds: int | None = Field(
        default=None,
        description=(
            "Correlation window override in seconds (30–10800). Null inherits each HoS rule's "
            "detection.timeframe; a value wins over it"
        ),
    )
    auto_add_new_pipelines: bool | None = Field(
        default=None, description="When true, newly created detection pipelines may be auto-added as members"
    )
    auto_add_pipeline_name_pattern: str | None = Field(
        default=None,
        description="Glob matched against a new detection pipeline's name; null/blank matches all",
    )
    pipeline_ids: list[UUID] | None = Field(
        default=None,
        description=("Replace the full member set. A pipeline may belong to several correlation pipelines"),
    )
    correlation_resources: CorrelationPipelineJobResources | None = Field(
        default=None, description="correlation-job Flink resources; null fields inherit platform defaults"
    )
    agent_resources: CorrelationPipelineJobResources | None = Field(
        default=None, description="agent-job Flink resources; null fields inherit platform defaults"
    )

    @model_validator(mode="after")
    def at_least_one_field(self) -> Self:
        data = self.model_dump(exclude_none=True)
        if not data:
            raise ValueError("At least one field must be provided for update")
        return self

    @field_validator("threshold_pct")
    @classmethod
    def check_threshold_pct(cls, v: int | None) -> int | None:
        return validate_threshold_pct(v)

    @field_validator("correlation_window_seconds")
    @classmethod
    def check_correlation_window_seconds(cls, v: int | None) -> int | None:
        return validate_correlation_window_seconds(v)

    @field_validator("auto_add_pipeline_name_pattern")
    @classmethod
    def check_auto_add_pipeline_name_pattern(cls, v: str | None) -> str | None:
        return validate_auto_add_pipeline_name_pattern(v)


class CorrelationPipelineListItem(BaseModel):
    """Correlation pipeline summary row for the paginated list."""

    id: UUID = Field(description="Correlation pipeline id")
    name: str = Field(description="Correlation pipeline display name")
    jobs_status: str = Field(description="Desired control state of both Flink jobs: starting, stopping, or stopped")
    runtime_state: AttackChainsRuntimeStateResponse | None = Field(
        default=None,
        description=(
            "Observed runtime of this pipeline's correlation-job + agent-job. Served from a 15s "
            "in-memory cache (Kubernetes on miss), matching detection-pipeline list status. "
            "Stopped pipelines skip the cluster and report overall=stopped. List `overall` never "
            "emits `partial`: one failed half is `failed` (job tiles still show which half)"
        ),
    )
    members: list[CorrelationPipelineMember] = Field(
        default_factory=list, description="Detection pipelines feeding this correlation pipeline"
    )
    destination_topics: list[str] = Field(
        default_factory=list,
        description=(
            "Destination topics of the member detection pipelines, unique and sorted. Chip labels "
            "only: topic names are not unique across pipelines"
        ),
    )
    attack_chains_count: int = Field(description="Number of attack chains attributed to this correlation pipeline")
    created_at: datetime.datetime = Field(description="When the correlation pipeline was created")
    updated_at: datetime.datetime = Field(description="When the correlation pipeline was last updated")


class CorrelationPipelineListResponse(BaseModel):
    """Paginated list of correlation pipelines."""

    total: int = Field(description="Total number of correlation pipelines matching the query")
    limit: int = Field(description="Maximum items per page")
    offset: int = Field(description="Number of items skipped before this page")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    data: list[CorrelationPipelineListItem] = Field(description="Correlation pipeline rows for this page")


class CorrelationPipelineResponse(CorrelationPipelineListItem):
    """Full correlation pipeline row for the detail view."""

    threshold_pct: int = Field(description="Coverage threshold percentage (50–100, step 5)")
    correlation_window_seconds: int | None = Field(
        description="Correlation window override in seconds; null inherits each HoS rule's detection.timeframe"
    )
    auto_add_new_pipelines: bool = Field(description="Whether new detection pipelines may be auto-added as members")
    auto_add_pipeline_name_pattern: str | None = Field(
        description="Glob new detection pipeline names must match to be auto-added; null matches all"
    )
    correlation_resources: CorrelationPipelineJobResources = Field(
        description="correlation-job Flink resources; null fields inherit the platform defaults"
    )
    agent_resources: CorrelationPipelineJobResources = Field(
        description="agent-job Flink resources; null fields inherit the platform defaults"
    )
    correlation_deployment_name: str | None = Field(
        description="Name of the deployed correlation-job Deployment, if it has ever been deployed"
    )
    agent_deployment_name: str | None = Field(
        description="Name of the deployed agent-job Deployment, if it has ever been deployed"
    )
    updated_by: UUID | None = Field(default=None, description="User id who last updated the correlation pipeline")
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Non-fatal notes about what the operation did or could not do — e.g. that a window "
            "change re-buckets chain ids"
        ),
    )
    restarted: list[Literal["correlation", "agent"]] = Field(
        default_factory=list,
        description="Which Flink jobs this request actually restarted; empty when nothing was interrupted",
    )


class CorrelationPipelineRuntimeResponse(BaseModel):
    """Result of ``POST /correlation_pipelines/{id}/{run,stop}`` — accepted intent plus current state."""

    status: CorrelationPipelineStatus = Field(
        description=(
            "Composite status right after the request: starting/stopping while the CRDs settle, "
            "partial when exactly one half failed, failed when both did. ``stopped`` renders as "
            "'Disabled' in the UI"
        )
    )
    correlation_job: dict = Field(description="Raw correlation-job orchestration result (provider-specific shape)")
    agent_job: dict = Field(description="Raw agent-job orchestration result (provider-specific shape)")
    warnings: list[str] = Field(
        default_factory=list,
        description="Non-fatal notes about what the operation did or could not do",
    )
    restarted: list[Literal["correlation", "agent"]] = Field(
        default_factory=list,
        description="Which Flink jobs this request actually restarted; empty when nothing was interrupted",
    )


# =============================================================================
# Attack chains — correlated chain rows (CRUD / list)
# =============================================================================


class AttackChainResponse(BaseModel):
    """Short chain row for tables and filters."""

    id: UUID = Field(description="Chain id")
    name: str = Field(description="Display name; derived from the linked ``high_order_sigmas.name``")
    chain_type: str = Field(description="Correlation chain type (e.g. 'HOS')")
    hostname: str = Field(description="Hostname the chain was correlated on")
    username: str | None = Field(description="Username associated with the chain, if any")
    status: str = Field(
        description=(
            "Workflow status: new, investigating, confirmed, false-positive, resolved, closed, or filtered-out"
        ),
    )
    severity: str | None = Field(
        default=None,
        description="Same as linked active threat severity (``active_threats.severity`` for ``active_threat_id``)",
    )
    correlation_pipeline_id: UUID | None = Field(
        default=None,
        description="Correlation pipeline that produced the chain; null for legacy/unattributed chains",
    )
    correlation_pipeline_name: str | None = Field(
        default=None,
        description="Name of the correlation pipeline that produced the chain, when it is still known",
    )
    correlation_pipelines: list[CorrelationPipelineRef] = Field(
        default_factory=list,
        description=(
            "The chain's correlation pipeline as a ONE-element array (a chain belongs to exactly "
            "one); empty when the pipeline was deleted or the chain is legacy/unattributed. "
            "Derived from correlation_pipeline_id/-_name, which stay the source of truth"
        ),
    )
    first_seen: datetime.datetime = Field(description="Timestamp of the earliest event in the chain")
    last_seen: datetime.datetime = Field(description="Timestamp of the latest event in the chain")
    event_count: int = Field(description="Number of events in the chain")
    rule_count: int = Field(description="Number of distinct rules matched in the chain")
    match: int = Field(description="Matched sequence percentage, rounded to the nearest integer")
    created_at: datetime.datetime = Field(description="When the chain row was created")
    updated_at: datetime.datetime = Field(description="When the chain row was last updated")


AttackChainGroupBy = Literal["none", "hostname", "chain_name"]
AttackChainStatus = Literal["new", "investigating", "confirmed", "false-positive", "resolved", "closed", "filtered-out"]


class FilterOptionResponse(BaseModel):
    """One selectable filter option (label/value pair)."""

    label: str = Field(description="Display label for the option")
    value: str = Field(description="Value submitted when the option is selected")


class AttackChainsFiltersSectionResponse(BaseModel):
    """Available filter options for the attack-chains list."""

    group: list[FilterOptionResponse] = Field(description="Grouping options (e.g. hostname, chain name)")
    severity: list[str] = Field(description="Available severity values to filter by")
    status: list[str] = Field(
        description="Available status values (new, investigating, confirmed, false-positive, resolved, closed, "
        "filtered-out)"
    )


class MonitoredThreatsFiltersSectionResponse(BaseModel):
    """Available filter options for the monitored-threats list."""

    model_config = ConfigDict(populate_by_name=True)

    severity: list[str] = Field(description="Available severity values to filter by")
    industry: list[str] = Field(description="Available industry values to filter by")
    threat_type: list[str] = Field(
        serialization_alias="threatType", description="Available threat-type values to filter by"
    )
    region: list[str] = Field(description="Available region/geography values to filter by")


class AttackChainsFiltersResponse(BaseModel):
    """Filter options for both the attack-chains and monitored-threats views."""

    attack_chains: AttackChainsFiltersSectionResponse = Field(description="Filter options for attack chains")
    monitored_threats: MonitoredThreatsFiltersSectionResponse = Field(
        description="Filter options for monitored threats"
    )


class AttackChainsListSSEMessage(BaseModel):
    """SSE envelope for ``GET /api/v1/attack_chains/chains/stream``.

    Read-only invalidation channel: ``attack_chains_changed`` tells the UI to refetch
    ``GET /api/v1/attack_chains/chains``; the stream never carries list rows.
    """

    type: Literal[
        "connection_established",
        "attack_chains_changed",
        "heartbeat",
        "error",
    ] = Field(
        description=(
            "connection_established on connect; attack_chains_changed when the catalog changed; "
            "heartbeat keep-alive; error on stream failure"
        )
    )
    timestamp: datetime.datetime = Field(description="When the event was emitted (UTC)")
    revision: int | None = Field(
        default=None,
        ge=0,
        description="Global in-memory catalog revision; omitted for error events",
    )
    error: str | None = Field(default=None, description="Error message when type is error")


class AttackChainListResponse(BaseModel):
    """Paginated, optionally grouped list of attack chains."""

    list_revision: int = Field(
        ge=0,
        description=(
            "Global in-memory catalog revision captured before the list query; compare with the "
            "SSE stream ``revision`` to detect changes between REST and invalidation events"
        ),
    )
    total: int = Field(description="Total number of chains matching the query")
    filtered_out_count: int | None = Field(
        default=None,
        description="Total number of chains with status ``filtered-out``; omitted when filtered-out rows are shown",
    )
    limit: int = Field(description="Maximum items per page")
    offset: int = Field(description="Number of items skipped before this page")
    sort: str = Field(description="Field the results are sorted by")
    order: str = Field(description="Sort direction: asc or desc")
    group_by: AttackChainGroupBy = Field(description="Grouping applied: none, hostname, or chain_name")
    groups: dict[str, list[AttackChainResponse]] = Field(
        description="Chains keyed by group value (single key when group_by is none)"
    )


class AttackChainDetailResponse(AttackChainResponse):
    """Full chain row plus related entities for the detail view."""

    hos_rule: HighOrderSigmaDetailResponse = Field(description="High-order Sigma that defines this chain")
    active_threat: ActiveThreatResponse = Field(description="Active threat linked to the chain")
    ai_summary: str | None = Field(description="LLM-generated summary of the chain, if analysed")
    llm_verdict: str | None = Field(description="LLM verdict for the chain, if analysed")
    llm_confidence: float | None = Field(description="LLM confidence score for the verdict, if analysed")
    llm_review: Any | None = Field(description="Full LLM review payload, if analysed")
    analysed_at: datetime.datetime | None = Field(description="When the agent-job analysed the chain, if ever")
    threat_actors: list[MitreIdNameRef] | None = Field(
        default=None,
        description="Same as linked active threat threat_actors (id + name)",
    )
    techniques: list[dict] = Field(
        default_factory=list,
        description="MITRE techniques tied to correlation hits (subset of chain graph)",
    )
    chain_graph: list[dict] = Field(
        default_factory=list,
        description="All attributed techniques with hit flag for visualization",
    )


class AttackChainPatchRequest(BaseModel, extra="forbid"):
    """Partial chain update; currently only ``status`` is supported."""

    status: AttackChainStatus = Field(..., description="Workflow status for the chain")
