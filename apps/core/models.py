from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from apps.core.database import Base
from apps.core.enums import AuditSeverity, UserRole


class Repository(Base):
    __tablename__ = "repositories"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    last_synced_rule_date = Column(DateTime(timezone=True), nullable=True)
    sync_enabled = Column(Boolean, nullable=True)
    source_link = Column(String(512), nullable=True)

    pipelines = relationship("Pipeline", secondary="pipeline_repositories", back_populates="repositories")
    rules = relationship("Rule", back_populates="repository", lazy="noload")

    # rules_count is calculated in repository layer when fetching from DB
    rules_count: int = 0


class Filter(Base):
    __tablename__ = "filters"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    body = Column(Text, nullable=False)
    active = Column(Boolean, server_default="true")
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())


class LogSource(Base):
    __tablename__ = "log_sources"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    # Parser config embedded directly (no FK to parsers table)
    parsing_script = Column(Text, nullable=True)  # Parser query/script
    parsing_config = Column(JSONB, nullable=True)  # Generated parser config
    mapping = Column(Text, nullable=True)
    # Test configuration
    test_topics = Column(ARRAY(String), nullable=True)  # Kafka topics for testing
    test_repository_ids = Column(ARRAY(UUID(as_uuid=True)), nullable=True)  # Repositories for field mapping
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())


class Rule(Base):
    __tablename__ = "rules"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    body = Column(Text, nullable=False)
    repository_id = Column(
        UUID(as_uuid=True), ForeignKey("repositories.id", ondelete="CASCADE"), nullable=True, index=True
    )
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    product = Column(String(255), nullable=True)
    service = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    is_supported = Column(Boolean, default=True, nullable=False, server_default="true")
    unsupported_reason = Column(String(512), nullable=True)
    validated_at = Column(DateTime(timezone=True), nullable=True)
    validated_with_version = Column(String(50), nullable=True)

    repository = relationship("Repository", back_populates="rules")


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    enabled = Column(Boolean, server_default="true", index=True)

    # Topic references (Strings directly from Kafka)
    # source_topics: Array of input topics (Flink unions them)
    # Index created via Alembic migration with GIN type for array containment queries
    source_topics = Column(ARRAY(String(255)), nullable=False, server_default="{}")
    destination_topic = Column(String(255), nullable=False, index=True)

    save_untagged = Column(Boolean, server_default="false")
    apply_parser_to_output_events = Column(Boolean, server_default="false")
    filters = Column(ARRAY(UUID(as_uuid=True)), server_default="{}")
    log_source_id = Column(
        UUID(as_uuid=True), ForeignKey("log_sources.id", ondelete="SET NULL"), nullable=True, index=True
    )
    custom_fields = Column(Text, nullable=True)  # YAML key-value pairs for event enrichment
    # Accumulated totals (updated via UPSERT from metrics)
    events_tagged = Column(BigInteger, server_default="0")
    events_untagged = Column(BigInteger, server_default="0")
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())

    # Flink-specific fields
    rules_topic = Column(String(255), server_default="sigma-rules")
    metrics_topic = Column(String(255), server_default="rule-statistics")
    parallelism = Column(Integer, server_default="1")
    window_size_sec = Column(Integer, server_default="30")
    status = Column(String(20), server_default="reconciling")  # running, suspended, failed, reconciling
    deployment_name = Column(String(255), nullable=True)  # e.g., "flink-{id}"
    namespace = Column(String(63), server_default="security")
    last_sync_at = Column(DateTime(timezone=True), nullable=True)

    # Flink resource configuration (nullable = use global defaults)
    taskmanager_memory_mb = Column(Integer, nullable=True)  # Memory per TaskManager in MB
    taskmanager_cpu = Column(Float, nullable=True)  # CPU cores per TaskManager
    checkpoint_interval_sec = Column(Integer, nullable=True)  # Checkpoint interval in seconds
    autoscaler_enabled = Column(Boolean, nullable=True)  # Enable Flink autoscaling
    autoscaler_min_parallelism = Column(Integer, nullable=True)  # Min parallelism for autoscaler
    autoscaler_max_parallelism = Column(Integer, nullable=True)  # Max parallelism for autoscaler

    needs_restart = Column(Boolean, default=False, nullable=False, server_default="false")
    log_source = relationship("LogSource", backref="pipelines")
    pipeline_rules = relationship("PipelineRule", back_populates="pipeline")
    repositories = relationship("Repository", secondary="pipeline_repositories", back_populates="pipelines")


# Junction table for Pipeline-Repository many-to-many relationship
# Composite PK (pipeline_id, repository_id) already ensures uniqueness
pipeline_repositories = Table(
    "pipeline_repositories",
    Base.metadata,
    Column(
        "pipeline_id",
        UUID(as_uuid=True),
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "repository_id",
        UUID(as_uuid=True),
        ForeignKey("repositories.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class PipelineRule(Base):
    __tablename__ = "pipeline_rules"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=True, index=True)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id", ondelete="CASCADE"), nullable=True, index=True)
    enabled = Column(Boolean, server_default="true")
    tagged_events = Column(Integer, server_default="0")
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())

    pipeline = relationship("Pipeline", back_populates="pipeline_rules")
    rule = relationship("Rule", backref="pipeline_rules")


class Event(Base):
    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    message = Column(String(500), nullable=False)
    detail = Column(Text, nullable=True)
    type = Column(String(50), nullable=False)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())


class AuditLog(Base):
    """Audit log for tracking user actions and system events."""

    __tablename__ = "audit_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Action type: create, update, delete, toggle, error, sync, login
    action = Column(String(50), nullable=False)

    # Entity being acted upon
    entity_type = Column(String(50), nullable=False)  # pipeline, rule, filter, repository, etc.
    entity_id = Column(String(100), nullable=True)
    entity_name = Column(String(255), nullable=True)

    # User who performed the action (null for system/flink events)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    user_email = Column(String(255), nullable=True)

    # Details
    details = Column(Text, nullable=True)  # Human-readable description
    changes = Column(JSONB, nullable=True)  # {"field": {"old": x, "new": y}}

    # Source of the event: user, flink, system
    source = Column(String(50), server_default="user", nullable=False)

    # Severity level: info, warning, error
    severity = Column(String(20), server_default=AuditSeverity.INFO.value, nullable=False)

    __table_args__ = (
        Index("ix_audit_logs_timestamp", "timestamp"),
        Index("ix_audit_logs_action", "action"),
        Index("ix_audit_logs_entity_type", "entity_type"),
        Index("ix_audit_logs_entity_id", "entity_id"),
        Index("ix_audit_logs_user_id", "user_id"),
        Index("ix_audit_logs_severity", "severity"),
    )


class Config(Base):
    __tablename__ = "config"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    key = Column(String(255), nullable=False, unique=True)
    value = Column(Text, nullable=False)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())


class PipelineMetric(Base):
    """Time-series metrics history for dashboards (append-only, with retention)."""

    __tablename__ = "pipeline_metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Window metrics
    window_total_events = Column(BigInteger, server_default="0")
    window_matched_events = Column(BigInteger, server_default="0")
    window_duration_seconds = Column(Float, server_default="0")
    throughput_eps = Column(Integer, server_default="0")

    # Errors
    event_parsing_errors = Column(Integer, server_default="0")
    rule_parsing_errors = Column(Integer, server_default="0")
    matching_errors = Column(Integer, server_default="0")

    __table_args__ = (
        Index("ix_pipeline_metrics_pipeline_ts", "pipeline_id", "timestamp"),
        Index("ix_pipeline_metrics_timestamp", "timestamp"),  # For cleanup queries
    )


class PipelineRuleMetric(Base):
    """Accumulated rule match counters per pipeline.

    Stores total matches per rule for each pipeline.
    Updated atomically via UPSERT when metrics arrive from Kafka.
    """

    __tablename__ = "pipeline_rule_metrics"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    pipeline_id = Column(UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False, index=True)
    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id", ondelete="CASCADE"), nullable=False, index=True)

    # Accumulated total matches (updated via atomic increment)
    total_matches = Column(BigInteger, server_default="0", nullable=False)

    # Timestamps
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    __table_args__ = (
        UniqueConstraint("pipeline_id", "rule_id", name="uq_pipeline_rule_metric"),
        Index("ix_pipeline_rule_metrics_pipeline", "pipeline_id"),
        Index("ix_pipeline_rule_metrics_rule", "rule_id"),
    )


class Parser(Base):
    __tablename__ = "parsers"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    parser_query = Column(Text, nullable=False)
    parser_config = Column(JSONB, nullable=False)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    full_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False, unique=True)
    password = Column(String(255), nullable=False)
    is_active = Column(Boolean, server_default="true")
    role = Column(String(50), nullable=False, server_default=UserRole.USER.value)
    must_change_password = Column(Boolean, server_default="false")
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class RuleLoaderModuleVersion(Base):
    """Tracks rule loader module versions for sigma validation .

    When a new version is deployed, rules need to be re-validated against the new schema.
    """

    __tablename__ = "rule_loader_module_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    version = Column(String(50), nullable=False)
    is_current = Column(Boolean, default=False, nullable=False, server_default="false")
    created = Column(DateTime(timezone=True), server_default=func.now())
