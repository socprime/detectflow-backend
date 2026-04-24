import datetime
from typing import Any, Literal
from uuid import UUID

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from apps.core.enums import AuditSeverity, HealthCheckStatus, UserRole
from apps.core.settings import settings


def get_reserved_topics() -> set[str]:
    """Get set of reserved system topics that cannot be used by pipelines."""
    return {
        settings.kafka_sigma_rules_topic,  # sigma-rules
        settings.kafka_activity_topic,  # etl-activity
        settings.kafka_metrics_topic,  # rule-statistics
    }


def validate_topic_not_reserved(topic: str, field_name: str) -> None:
    """Validate that a topic is not a reserved system topic."""
    reserved = get_reserved_topics()
    if topic in reserved:
        raise ValueError(
            f"{field_name} '{topic}' is a reserved system topic. Reserved topics: {', '.join(sorted(reserved))}"
        )


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
# Common Error Schemas
# =============================================================================


class ActionResponse(BaseModel):
    """Generic action response for CRUD operations."""

    id: str
    status: bool
    message: str


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
# Dashboard SSE Schemas (Real-time)
# =============================================================================


class SourceTopicStats(BaseModel):
    """Source topic with real-time EPS metrics."""

    id: str
    name: str
    eps: float = Field(description="Events per second (input throughput)")


class RepositoryStats(BaseModel):
    """Repository with rules count."""

    id: str
    name: str
    rules_count: int


class DestinationTopicStats(BaseModel):
    """Destination topic with tagged/untagged EPS metrics."""

    id: str
    name: str
    tagged_eps: float = Field(description="Tagged events per second")
    untagged_eps: float = Field(description="Untagged events per second")


class DashboardGraphData(BaseModel):
    """Graph visualization data for dashboard."""

    source_topics: list[SourceTopicStats]
    repositories: list[RepositoryStats]
    destination_topics: list[DestinationTopicStats]
    pipelines_count: int
    total_events_eps: float = Field(description="Total input events per second")
    total_tagged_eps: float = Field(description="Total tagged events per second")
    total_untagged_eps: float = Field(default=0.0, description="Total untagged events per second")
    total_rules: int


class PipelineStatsItem(BaseModel):
    """Per-pipeline statistics for the modal table."""

    id: str
    name: str
    source_topics: list[str] = Field(description="Kafka input topics (can be multiple)")
    destination_topic: str
    save_untagged: bool = Field(description="Whether to save untagged events")
    repository_ids: list[str] = Field(default_factory=list, description="Repository IDs for mapping")
    input_eps: float = Field(description="Input events per second")
    output_eps: float = Field(description="Output (matched) events per second")
    topic_lag: int = Field(description="Kafka consumer lag (pending records)")
    status: str = Field(description="Pipeline status (running, suspended, failed)")
    status_details: "StatusDetails | None" = Field(
        None,
        description="Detailed status info for non-running states",
    )


class RecentEventItem(BaseModel):
    """Recent activity event."""

    id: str
    message: str
    detail: list[Any] = Field(default_factory=list)
    timestamp: str
    type: str  # info, warning, error, success


class DashboardData(BaseModel):
    """Complete dashboard data sent via SSE."""

    graph: DashboardGraphData
    pipelines_stats: list[PipelineStatsItem]
    recent_activity: list["ActivityItem"] = Field(default_factory=list)


class DashboardSSEMessage(BaseModel):
    """SSE message envelope."""

    type: Literal["dashboard_update", "error", "connection_established"]
    data: DashboardData | dict | None = None
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    error: str | None = None


class DashboardSnapshotResponse(BaseModel):
    """REST endpoint response for initial dashboard state."""

    graph: DashboardGraphData
    pipelines_stats: list[PipelineStatsItem]
    recent_activity: list["ActivityItem"] = Field(default_factory=list)
    last_kafka_update: datetime.datetime | None = Field(description="Last time metrics were received from Kafka")
    consumer_healthy: bool = Field(description="Whether Kafka consumer is running")


# =============================================================================
# Kafka Metrics Schema (from Flink job)
# =============================================================================


class KafkaMetricState(BaseModel):
    """State metrics from Kafka."""

    event_buffer_size: int
    event_buffer_memory_bytes: int
    broadcast_state_rules: int


class KafkaMetricErrors(BaseModel):
    """Error metrics from Kafka."""

    event_parsing_errors: int = 0
    rule_parsing_errors: int = 0
    matching_errors: int = 0
    total: int = 0
    messages: list[str] = Field(default_factory=list, description="Human-readable error messages (max 10 per window)")


class KafkaMetricRuleTop(BaseModel):
    """Top rule by matches."""

    rule_id: str
    count: int


class KafkaMetricSlowRule(BaseModel):
    """Slow rule info."""

    rule_id: str
    match_count: int
    estimated_time_ms: float


class KafkaMetricMatchedRule(BaseModel):
    """Rule match count for the window (for per-rule totals)."""

    rule_id: str
    window_matches: int


class KafkaMetricRules(BaseModel):
    """Rules metrics from Kafka.

    Note: total and triggered_unique are optional for backward compatibility.
    Flink simplified message only sends matched_rules.
    """

    total: int = 0
    triggered_unique: int = 0
    top_by_matches: list[KafkaMetricRuleTop] = Field(default_factory=list)
    slow_rules_top_3: list[KafkaMetricSlowRule] = Field(default_factory=list)
    avg_time_per_rule_ms: float = 0.0
    # Per-rule matches for accumulating totals (only rules with matches > 0)
    matched_rules: list[KafkaMetricMatchedRule] = Field(default_factory=list)


class KafkaMetricSystem(BaseModel):
    """System metrics from Kafka."""

    parallelism: int
    subtask_index: int
    hostname: str
    input_topic: str | None = None
    output_topic: str | None = None


class KafkaMetricMessage(BaseModel):
    """Full Kafka metric message from Flink job."""

    job_id: str
    instance_id: int = 0
    timestamp: int = 0  # Unix timestamp in milliseconds
    detectflow_matchnode_version: str | None = None  # Flink job version from pyproject.toml

    # Window metrics (optional for backward compatibility)
    window_total_events: int = 0
    window_matched_events: int = 0
    window_match_rate_percent: float = 0.0

    window_start_time: float = 0.0
    window_end_time: float = 0.0
    window_duration_seconds: float = 0.0
    window_throughput_eps: int = 0

    on_timer_start_time: float = 0.0
    on_timer_end_time: float = 0.0
    on_timer_duration_seconds: float = 0.0
    on_timer_throughput_eps: int = 0

    matching_start_time: float = 0.0
    matching_end_time: float = 0.0
    matching_duration_seconds: float = 0.0
    matching_throughput_eps: int = 0

    # Nested objects (optional with defaults)
    state: KafkaMetricState | None = None
    errors: KafkaMetricErrors = Field(default_factory=KafkaMetricErrors)
    rules: KafkaMetricRules | None = None
    system: KafkaMetricSystem | None = None


# Pipeline Schemas
class PipelineStatisticsResponse(BaseModel):
    topics: dict
    networks: dict
    rules: dict
    events: dict


class PaginationParams(BaseModel):
    page: int | None = 1
    limit: int | None = 10
    sort: str | None = None
    offset: int | None = 0
    order: str | None = "asc"
    search: str | None = None


class PipelineCreateRequest(BaseModel):
    """Request to create a new ETL pipeline."""

    name: str = Field(description="Pipeline display name")
    source_topics: list[str] = Field(
        min_length=1,
        description="Kafka topics to read events from (minimum 1 required, supports multiple for union)",
    )
    destination_topic: str = Field(description="Kafka topic to write tagged events to")
    save_untagged: bool = Field(default=False, description="Save events that don't match any rules")
    apply_parser_to_output_events: bool = Field(default=False, description="Apply parser to output events")
    filters: list[str] = Field(default=[], description="List of filter IDs to apply")
    log_source_id: str | None = Field(default=None, description="Log source ID for parsing configuration")
    enabled: bool = Field(default=True, description="Start pipeline immediately after creation")
    kafka_starting_offset_earliest: bool = Field(
        default=False,
        description="If true, Kafka consumer starts from earliest offset; not stored in DB",
    )
    repository_ids: list[str] | None = Field(default=None, description="Repository IDs containing rules")
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
                    "source_topics": ["raw-logs", "windows-events"],
                    "destination_topic": "tagged-logs",
                    "save_untagged": False,
                    "apply_parser_to_output_events": False,
                    "log_source_id": "550e8400-e29b-41d4-a716-446655440000",
                    "enabled": True,
                    "repository_ids": ["550e8400-e29b-41d4-a716-446655440001"],
                    "resources": {"parallelism": 4, "taskmanager_memory_mb": 4096},
                }
            ]
        }
    }

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
    def validate_topics(self) -> "PipelineCreateRequest":
        """Validate that source and destination topics are valid."""
        # Check source topics are not reserved
        for topic in self.source_topics:
            validate_topic_not_reserved(topic, "Source topic")

        # Check destination topic is not reserved
        validate_topic_not_reserved(self.destination_topic, "Destination topic")

        # Check source topics don't contain destination topic
        if self.destination_topic in self.source_topics:
            raise PydanticCustomError(
                "topic_conflict",
                f"Destination topic '{self.destination_topic}' cannot be the same as a source topic. "
                "This would create an infinite loop.",
            )

        return self


class PipelineUpdateRequest(BaseModel):
    """Request to update an existing pipeline. All fields are optional."""

    name: str | None = Field(default=None, description="Pipeline display name")
    source_topics: list[str] | None = Field(
        default=None,
        description="Kafka source topics (triggers restart). At least 1 required when provided.",
    )
    destination_topic: str | None = Field(default=None, description="Kafka destination topic (triggers restart)")
    save_untagged: bool | None = Field(default=None, description="Save unmatched events (triggers restart)")
    apply_parser_to_output_events: bool | None = Field(
        default=None, description="Apply parser to output events (triggers restart)"
    )
    filters: list[str] | None = Field(default=None, description="Filter IDs (hot-reloadable)")
    log_source_id: str | None = Field(default=None, description="Log source ID (hot-reloadable)")
    enabled: bool | None = Field(default=None, description="Enable/disable pipeline")
    repository_ids: list[str] | None = Field(default=None, description="Repository IDs (hot-reloadable)")
    custom_fields: str | None = Field(default=None, description="Custom fields YAML (hot-reloadable)")
    # NOTE: resources field removed - changing parallelism/memory after pipeline creation
    # causes Flink checkpoint incompatibility (maxParallelism mismatch). To change resources,
    # delete and recreate the pipeline.

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

    @field_validator("custom_fields")
    @classmethod
    def validate_custom_fields_yaml(cls, v: str | None) -> str | None:
        """Validate YAML format for custom fields."""
        return validate_yaml(v)

    @model_validator(mode="after")
    def validate_topics(self) -> "PipelineUpdateRequest":
        """Validate that source and destination topics are valid when provided."""
        # Check source topics are not reserved (if provided)
        if self.source_topics is not None:
            for topic in self.source_topics:
                validate_topic_not_reserved(topic, "Source topic")

        # Check destination topic is not reserved (if provided)
        if self.destination_topic is not None:
            validate_topic_not_reserved(self.destination_topic, "Destination topic")

        # Check source topics don't contain destination topic (if both provided)
        if self.source_topics is not None and self.destination_topic is not None:
            if self.destination_topic in self.source_topics:
                raise PydanticCustomError(
                    "topic_conflict",
                    f"Destination topic '{self.destination_topic}' cannot be the same as a source topic. "
                    "This would create an infinite loop.",
                )

        return self


class PipelineResponse(ActionResponse):
    pass


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
    id: str
    enabled: bool
    name: str
    status: str
    status_details: StatusDetails | None = Field(
        None,
        description="Detailed status info with warnings (only populated for non-running states)",
    )
    source_topics: list[str] = Field(description="Kafka input topics")
    destination_topic: str
    repositories: list[dict[str, str]]
    log_source: list[dict[str, str]]
    filters: int
    rules: int
    supported_rules: int = Field(0, description="Count of supported rules")
    events_tagged: int
    events_untagged: int
    detectflow_matchnode_version: str | None = Field(None, description="Last known Flink job version")
    needs_restart: bool = Field(False, description="Pipeline needs restart due to rule loader module update")
    created: str
    updated: str


class PipelineListResponse(BaseModel):
    total: int
    page: int
    limit: int
    sort: str
    offset: int
    order: str
    data: list[PipelineListItem]


class PipelineDetailResponse(BaseModel):
    id: str
    name: str
    source_topics: list[str] = Field(description="Kafka input topics")
    destination_topic: str
    save_untagged: bool
    apply_parser_to_output_events: bool
    filters: list[str]
    log_source_id: str
    log_source_name: str | None = None
    enabled: bool
    repository_ids: list[str]
    custom_fields: str | None = None

    # Metrics from PostgreSQL (accumulated totals)
    events_tagged: int = 0
    events_untagged: int = 0

    # Rules statistics
    active_rules: int = 0  # Total count of enabled rules
    matched_rules: int = 0  # Count of rules with tagged_events > 0

    # Flink fields (values come from DB, defaults from settings at creation time)
    rules_topic: str | None = None
    metrics_topic: str | None = None
    parallelism: int | None = None
    window_size_sec: int | None = None
    status: str | None = None
    status_details: StatusDetails | None = Field(
        None,
        description="Detailed status info with warnings (only populated for non-running states)",
    )
    deployment_name: str | None = None
    namespace: str | None = None
    last_sync_at: str | None = None

    # Flink resource configuration
    taskmanager_memory_mb: int | None = None
    taskmanager_cpu: float | None = None
    checkpoint_interval_sec: int | None = None
    autoscaler_enabled: bool | None = None
    autoscaler_min_parallelism: int | None = None
    autoscaler_max_parallelism: int | None = None

    # Flink job version (last known, persists when pipeline not running)
    detectflow_matchnode_version: str | None = Field(None, description="Last known Flink job version")
    warnings: list[str] = Field(default_factory=list, description="Pipeline warnings (e.g., no supported rules)")
    supported_rules_count: int = Field(0, description="Count of supported (valid) rules")
    total_rules_count: int = Field(0, description="Total count of rules")
    needs_restart: bool = Field(False, description="Pipeline needs restart due to rule loader module update")


class PipelineStatisticsDetailResponse(BaseModel):
    events_tagged: int
    events_untagged: int
    active_rules: int
    matched_rules: int


class RuleListItem(BaseModel):
    id: str
    name: str
    repository: str
    repository_id: str | None = None
    created: str
    updated: str
    enabled: bool
    tagged_events: int
    # Sigma validation fields
    is_supported: bool = True
    unsupported_reason: str | None = None


class PipelineRulesListResponse(BaseModel):
    total: int
    page: int
    limit: int
    sort: str
    offset: int
    order: str
    data: list[RuleListItem]
    # Rule validation summary
    supported_count: int = Field(0, description="Count of supported rules in this pipeline")
    unsupported_count: int = Field(0, description="Count of unsupported rules in this pipeline")


class PipelineSimpleResponse(BaseModel):
    id: str
    name: str
    type: str = Field(description="How pipeline uses this topic: source, destination")


class TopicDetailResponse(BaseModel):
    name: str
    pipelines: list[PipelineSimpleResponse]


class TopicsResponse(BaseModel):
    """Paginated topics response."""

    data: list[TopicDetailResponse]
    total: int
    offset: int
    limit: int


class TopicsEventsRequest(BaseModel):
    """Request schema for getting events from topics."""

    topics: list[str] = Field(description="List of topic names to fetch events from")


class TopicEventItem(BaseModel):
    """Single event item with topic name."""

    topic: str = Field(description="Topic name")
    event: str = Field(description="Event data as string")


# Filters Schemas
class FilterActiveResponse(BaseModel):
    id: str
    name: str


class FilterDetailResponse(BaseModel):
    id: str
    name: str
    created: str
    updated: str


class FilterListResponse(BaseModel):
    total: int
    page: int
    limit: int
    sort: str
    offset: int
    order: str
    data: list[FilterDetailResponse]


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

    id: str
    name: str
    body: str
    created: str
    updated: str


# Repositories Schemas
class PipelineInfo(BaseModel):
    """Pipeline information with id and name."""

    id: str = Field(description="Pipeline ID")
    name: str = Field(description="Pipeline name")


class RepositoryDetailResponse(BaseModel):
    id: str
    name: str
    type: Literal["api", "local", "external"]
    type_display: str = Field(description="Human-readable type for UI display")
    rules: int
    created: datetime.datetime
    updated: datetime.datetime
    sync_enabled: bool | None
    source_link: str | None = Field(default=None, description="Source link for external repositories")
    pipelines: list[PipelineInfo] = Field(default_factory=list, description="List of connected pipelines")


class RepositoryListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    sort: str | None = None
    order: Literal["asc", "desc"]
    data: list[RepositoryDetailResponse]


class RepositoryCreateUpdateRequest(BaseModel):
    name: str


class RepositoryUpdateRequest(BaseModel):
    """Request to update a repository. Fields are optional and depend on repository type."""

    name: str | None = Field(default=None, description="Repository name (for local repositories)")
    sync_enabled: bool | None = Field(
        default=None, description="Sync enabled status (for API and external repositories)"
    )


class RepositoryApiKeyUpdateResponse(BaseModel):
    message: str


class SettingsResponse(BaseModel):
    """Settings information."""

    api_key_configured: bool = Field(description="Whether SOCPrime API key is configured")
    api_key_mask: str | None = Field(default=None, description="Masked API key showing first 3 and last 3 characters")


# =============================================================================
# Flink Resource Configuration Schemas
# =============================================================================


class FlinkDefaultsResponse(BaseModel):
    """Global Flink defaults for new pipelines."""

    parallelism: int = Field(default=2, ge=1, le=128, description="Number of parallel tasks")
    taskmanager_memory_mb: int = Field(default=2048, ge=1024, le=131072, description="Memory per TaskManager in MB")
    taskmanager_cpu: float = Field(
        default=1.0, ge=0.5, le=16.0, description="CPU cores per TaskManager (each TaskManager = one parallelism unit)"
    )
    window_size_sec: int = Field(default=30, ge=10, le=120, description="Event aggregation window in seconds")
    checkpoint_interval_sec: int = Field(default=60, ge=30, le=600, description="Checkpoint interval in seconds")
    autoscaler_enabled: bool = Field(default=False, description="Enable Flink autoscaling")
    autoscaler_min_parallelism: int = Field(default=1, ge=1, le=24, description="Minimum parallelism for autoscaler")
    autoscaler_max_parallelism: int = Field(default=24, ge=1, le=720, description="Maximum parallelism for autoscaler")


class FlinkResourceConfig(BaseModel):
    """Flink resource configuration for pipeline create/update.

    All fields are optional. Null values use global defaults.
    """

    parallelism: int | None = Field(default=None, ge=1, le=128, description="Number of parallel tasks")
    taskmanager_memory_mb: int | None = Field(
        default=None, ge=1024, le=131072, description="Memory per TaskManager in MB"
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
    type: Literal["integer", "number", "boolean"] = Field(description="Parameter data type")
    default: int | float | bool = Field(description="Default value")
    min: int | float | None = Field(default=None, description="Minimum allowed value")
    max: int | float | None = Field(default=None, description="Maximum allowed value")
    unit: str | None = Field(default=None, description="Unit of measurement (e.g., 'MB', 'cores', 'seconds')")
    title: str = Field(description="Human-readable title")
    description: str = Field(description="Detailed description of what this parameter does")
    impact: Literal["restart", "hot_reload"] = Field(description="Whether changing requires restart")
    category: Literal["resources", "processing", "autoscaler"] = Field(description="Parameter category")
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


class SocprimeRepositoryResponse(BaseModel):
    """SOCPrime repository response with id and name."""

    id: str = Field(description="Repository ID from SOCPrime TDM API")
    name: str = Field(description="Repository name")
    is_added: bool = Field(description="True if repository exists in database")
    source_link: str = Field(description="Link to repository in SOCPrime Expert UI")


class SocprimeRepositoryListResponse(BaseModel):
    """List of SOCPrime repositories."""

    data: list[SocprimeRepositoryResponse] = Field(description="List of repositories")


class AddRepositoriesRequest(BaseModel):
    """Base request for adding repositories."""

    repository_ids: list[str] = Field(description="List of repository IDs to add", min_length=1)


class AddRepositoriesResponse(BaseModel):
    """Base response after adding repositories."""

    added: list[str] = Field(description="Repository IDs that were added")
    skipped: list[str] = Field(description="Repository IDs that were already in database")


class AddSocprimeRepositoriesRequest(AddRepositoriesRequest):
    pass


class AddSocprimeRepositoriesResponse(AddRepositoriesResponse):
    pass


class AddExternalRepositoriesRequest(AddRepositoriesRequest):
    pass


class AddExternalRepositoriesResponse(AddRepositoriesResponse):
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
    name: str | None = None
    body: str | None = None


class RuleDetailResponse(BaseModel, extra="ignore"):
    id: str
    name: str
    repository_id: str
    repository_type: str
    repository_name: str
    created: datetime.datetime
    updated: datetime.datetime
    product: str | None = None
    service: str | None = None
    category: str | None = None
    # Sigma validation fields
    is_supported: bool = True
    unsupported_reason: str | None = None


class RuleFullDetailResponse(RuleDetailResponse):
    body: str


class RuleListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    sort: str
    order: str
    data: list[RuleDetailResponse]


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
    id: str
    name: str
    created: str
    updated: str
    parser_query: str


class ParserSimpleResponse(BaseModel):
    id: str
    name: str


class ParserListResponse(BaseModel):
    total: int
    page: int
    limit: int
    sort: str
    offset: int
    order: str
    data: list[ParserDetailResponse]


class ParserCreateRequest(BaseModel):
    name: str
    parser_query: str
    parser_config: dict[str, Any] | None = None


class ParserUpdateRequest(BaseModel):
    name: str | None = None
    parser_query: str | None = None


class ParserResponse(ActionResponse):
    pass


class ParserRunRequest(BaseModel):
    parser_query: str
    source_topic_ids: list[str]
    mapping: str | None = None


class ParserRunResultItem(BaseModel):
    source_topic: str
    source_data: str
    parsed_data: dict[str, Any] | None
    success: bool
    error_message: str | None


class ParserRunResponse(BaseModel):
    result: list[ParserRunResultItem]


# Log Sources Schemas
class LogSourceResponse(BaseModel):
    id: str
    name: str
    parsing_script: str | None = None
    parsing_config: dict[str, Any] | None = None
    mapping: str | None = None
    test_topics: list[str] | None = None
    test_repository_ids: list[str] | None = None
    created: str
    updated: str


class LogSourceListResponse(BaseModel):
    total: int
    page: int
    limit: int
    sort: str
    offset: int
    order: str
    data: list[LogSourceResponse]


class LogSourceCreateRequest(BaseModel):
    name: str
    parsing_script: str | None = None
    mapping: str | None = None
    test_topics: list[str] | None = None
    test_repository_ids: list[str] | None = None


class LogSourceUpdateRequest(BaseModel):
    name: str | None = None
    parsing_script: str | None = None
    mapping: str | None = None
    test_topics: list[str] | None = None
    test_repository_ids: list[str] | None = None


class LogSourceActionResponse(ActionResponse):
    pass


# User Schemas
class UserCreateRequest(BaseModel):
    """Request to create a new user account (Admin only)."""

    full_name: str = Field(description="User's full name")
    email: str = Field(description="User's email address (used for login)")
    password: str = Field(description="Initial password")
    is_active: bool = Field(default=True, description="Account active status")
    role: str = Field(default=UserRole.ADMIN, description="User role (admin or user)")

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
    email: str | None = Field(default=None, description="User's email address")
    password: str | None = Field(default=None, description="New password")
    is_active: bool | None = Field(default=None, description="Account active status")
    role: str | None = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str | None) -> str | None:
        """Validate email format."""
        if v is not None and "@" not in v:
            raise ValueError("Invalid email format")
        return v.lower() if v else None


class UserDetailResponse(BaseModel):
    id: str
    full_name: str
    email: str
    is_active: bool
    role: str
    must_change_password: bool
    created: datetime.datetime
    updated: datetime.datetime


class UserListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    sort: str
    order: str
    data: list[UserDetailResponse]


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
    new_password: str = Field(description="New password (min 8 chars, uppercase, lowercase, digit)")
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
    """Activity event format for Kafka topic etl-activity.

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

    id: str
    timestamp: str = Field(description="ISO 8601 format timestamp")
    action: str
    entity_type: str
    entity_id: str | None = None
    entity_name: str | None = None
    user_id: str | None = None
    user_email: str | None = None
    details: str | None = None
    source: str = "user"
    severity: str = AuditSeverity.INFO


# Backward compatibility alias
AuditLogItem = ActivityItem


class AuditLogsResponse(BaseModel):
    """Paginated audit logs response."""

    total: int = Field(description="Total number of matching records")
    limit: int = Field(description="Page size")
    offset: int = Field(description="Offset from start")
    data: list[AuditLogItem] = Field(description="Audit log entries")


class AuditLogsFiltersResponse(BaseModel):
    """Available filter options for audit logs."""

    actions: list[str] = Field(description="Available action types")
    entity_types: list[str] = Field(description="Available entity types")
    severities: list[str] = Field(description="Available severity levels")


# RecentActivityItem removed - use ActivityItem instead


class HealthCheckSingleCheck(BaseModel):
    """Single check result for health check."""

    status: HealthCheckStatus
    title: str
    descriptions: list[str]
    updated: datetime.datetime


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
