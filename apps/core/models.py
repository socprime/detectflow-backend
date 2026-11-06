from datetime import UTC, datetime

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Table,
    Text,
    UniqueConstraint,
    and_,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import backref, foreign, relationship
from sqlalchemy.sql import func

from apps.core.database import Base
from apps.core.enums import AuditSeverity, UserRole

GITLAB_REPOSITORY_TYPE = "gitlab"


def _utcnow() -> datetime:
    """Python-side timestamp default.

    Columns using this instead of ``func.now()`` stay populated in memory after a
    flush: SQLAlchemy evaluates the callable, sends it as a statement parameter and
    writes it back into the instance (a *prefetch* column). A SQL-side
    ``onupdate=func.now()`` instead *expires* the attribute, and reading an expired
    attribute under ``AsyncSession`` raises ``MissingGreenlet`` — which is how a
    post-validation rule reached the Kafka producer and 500'd.

    ``server_default=func.now()`` is kept alongside it so writes that bypass the ORM
    (Alembic, psql) still get a timestamp.
    """
    return datetime.now(tz=UTC)


class Repository(Base):
    __tablename__ = "repositories"
    __table_args__ = (UniqueConstraint("system_key", name="uq_repositories_system_key"),)

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    type = Column(String(50), nullable=False)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    last_synced_rule_date = Column(DateTime(timezone=True), nullable=True)
    sync_enabled = Column(Boolean, nullable=True)
    source_link = Column(String(512), nullable=True)
    is_system = Column(Boolean, nullable=False, server_default="false")
    system_key = Column(String(100), nullable=True)

    pipelines = relationship("Pipeline", secondary="pipeline_repositories", back_populates="repositories")
    rules = relationship("Rule", back_populates="repository", lazy="noload")
    gitlab_repository = relationship(
        "GitlabRepository",
        back_populates="repository",
        uselist=False,
        lazy="noload",
    )

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
    test_repository_ids = Column(JSONB, nullable=True)  # Repository selections for field mapping tests
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now())


class Rule(Base):
    __tablename__ = "rules"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    version = Column(Integer, nullable=False, server_default="1")
    repository_id = Column(
        UUID(as_uuid=True), ForeignKey("repositories.id", ondelete="CASCADE"), nullable=True, index=True
    )
    created = Column(DateTime(timezone=True), default=_utcnow, server_default=func.now())
    updated = Column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow, server_default=func.now())
    product = Column(String(255), nullable=True)
    service = Column(String(255), nullable=True)
    category = Column(String(255), nullable=True)
    folder_path = Column(Text, nullable=True)
    sigma_uuid = Column(UUID(as_uuid=True), nullable=True, index=True)
    # Sigma severity (`level:` in the body), derived on every write by
    # RuleDAO._parse_additional_fields_from_body. NULL when the body declares none — the REST
    # API surfaces that as null rather than inventing a default (the Kafka producer keeps its
    # own "medium" fallback, which the match node relies on).
    # Body-derived like product/service/category/sigma_uuid: TDM also reports a level structurally
    # (``TdmRule.level``) but for rules the body is the single source of truth, so a TDM rule whose
    # body omits `level:` is NULL here even though the Kafka payload carries TDM's value.
    level = Column(String(255), nullable=True)
    external_id = Column(String(255), nullable=True, index=True)
    is_supported = Column(Boolean, default=True, nullable=False, server_default="true")
    unsupported_reason = Column(String(512), nullable=True)
    validated_at = Column(DateTime(timezone=True), nullable=True)
    validated_with_version = Column(String(50), nullable=True)
    is_user_edited_system_rule = Column(Boolean, nullable=False, server_default="false")
    # API rule edited locally and not yet reconciled with TDM (push off / repo sync off at edit
    # time). Protected from pull overwrite and remote delete; reconciled on next sync_api_repos.
    has_unpushed_changes = Column(Boolean, nullable=False, server_default="false")

    repository = relationship("Repository", back_populates="rules")
    gitlab_metadata = relationship(
        "GitlabRuleMetadata",
        back_populates="rule",
        uselist=False,
        lazy="noload",
    )
    content_versions = relationship(
        "RuleContent", back_populates="rule", order_by="RuleContent.version.desc()", lazy="noload"
    )
    current_content = relationship(
        "RuleContent",
        primaryjoin=lambda: and_(
            Rule.id == RuleContent.rule_id,
            Rule.version == foreign(RuleContent.version),
        ),
        uselist=False,
        viewonly=True,
        lazy="noload",
    )

    @property
    def body(self) -> str:
        if self.current_content is None:
            raise RuntimeError(
                f"PROGRAMMING ERROR — Rule {self.id}: current_content not loaded. "
                f"Use joinedload(Rule.current_content) in query."
            )
        return self.current_content.body

    @property
    def content_hash(self) -> str:
        if self.current_content is None:
            raise RuntimeError(
                f"PROGRAMMING ERROR — Rule {self.id}: current_content not loaded. "
                f"Use joinedload(Rule.current_content) in query."
            )
        return self.current_content.content_hash


class RuleContent(Base):
    """Stores all versions of rule body content.

    The current version is identified by rules.version = rule_content.version.
    Old versions are kept for history/rollback (pruned to last N by retention policy).
    """

    __tablename__ = "rule_content"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id", ondelete="CASCADE"), nullable=False)
    version = Column(Integer, nullable=False)
    body = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    changed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    change_source = Column(String(50), nullable=True)
    changed_by = Column(String(255), nullable=True)

    rule = relationship("Rule", back_populates="content_versions")

    __table_args__ = (UniqueConstraint("rule_id", "version", name="uq_rule_content_rule_version"),)


class ActiveThreat(Base):
    __tablename__ = "active_threats"

    tdm_id = Column(String(255), primary_key=True)
    name = Column(String(255), nullable=False)
    # True = removed from monitoring in every correlation pipeline, now and future — D8. Monitoring
    # is opt-out: `monitored = NOT is_blocked AND tdm_id NOT IN pipeline.excluded`. This half rides
    # the SHARED active-threats topic as the frozen `enabled` key (`enabled = NOT is_blocked`); the
    # per-pipeline half rides each pipeline's chains-config envelope.
    is_blocked = Column(Boolean, nullable=False, server_default="false", index=True)
    is_hidden = Column(Boolean, nullable=False, server_default="false")
    summary = Column(Text, nullable=True)  # raw_ai_summary
    severity = Column(String(255), nullable=True)  # socprime_bias
    investigation = Column(Text, nullable=True)
    response = Column(Text, nullable=True)
    mitigation = Column(Text, nullable=True)
    industries = Column(ARRAY(String(255)), nullable=True)
    geographies = Column(ARRAY(String(255)), nullable=True)
    threat_types = Column(ARRAY(String(255)), nullable=True)
    threat_actors = Column(ARRAY(String(255)), nullable=True)
    techniques = Column(ARRAY(String(255)), nullable=True)
    source = Column(String(512), nullable=True)  # source_link
    attack_flow = Column(Text, nullable=True)
    tdm_created = Column(DateTime(timezone=True), nullable=True)
    tdm_updated = Column(DateTime(timezone=True), nullable=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    high_order_sigmas = relationship(
        "HighOrderSigma",
        secondary="active_threat_high_order_sigmas",
        back_populates="active_threats",
    )


class HighOrderSigma(Base):
    __tablename__ = "high_order_sigmas"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    tdm_id = Column(String(255), nullable=False, unique=True)
    name = Column(String(255), nullable=False)
    body = Column(Text, nullable=True)
    # Sigma severity of the correlation rule. Unlike Rule.level this is not derived by a DAO
    # choke point: it is set from TDM (``TdmRule.level``, falling back to the body) on sync, and
    # a user edit re-derives it from the new body — keeping the previous value when that body
    # declares no level, so a rename cannot silently discard TDM's severity.
    level = Column(String(255), nullable=True)
    tdm_created = Column(DateTime(timezone=True), nullable=True)
    tdm_updated = Column(DateTime(timezone=True), nullable=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())
    is_user_edited = Column(Boolean, nullable=False, server_default="false")
    # Tracks current Kafka state on detectflow.hos-rules so each sync only writes the delta.
    # True = a tombstone has been sent (row should NOT be republished unless visibility
    # changes); False = either no tombstone has been sent yet or the row is currently
    # published (or has never been touched). Reset to False on every publish/insert/user-edit.
    is_kafka_tombstoned = Column(Boolean, nullable=False, server_default="false")

    active_threats = relationship(
        "ActiveThreat",
        secondary="active_threat_high_order_sigmas",
        back_populates="high_order_sigmas",
    )
    rule_refs = relationship("HighOrderSigmaRuleRef", back_populates="high_order_sigma")
    rule_check_failures = relationship(
        "HighOrderSigmaRuleCheckFailure",
        back_populates="high_order_sigma",
        passive_deletes=True,
        lazy="noload",
    )


class ActiveThreatHighOrderSigma(Base):
    __tablename__ = "active_threat_high_order_sigmas"

    active_threat_id = Column(
        String(255),
        ForeignKey("active_threats.tdm_id", ondelete="CASCADE"),
        primary_key=True,
    )
    high_order_sigma_id = Column(
        UUID(as_uuid=True),
        ForeignKey("high_order_sigmas.id", ondelete="CASCADE"),
        primary_key=True,
    )

    __table_args__ = (Index("ix_active_threat_high_order_sigmas_high_order_sigma_id", "high_order_sigma_id"),)


class ActiveThreatBehavioralRule(Base):
    """AT->behavioral Sigma rule relation, keyed by TDM ``case.id`` (our ``rules.external_id``).

    Mirrors :class:`ActiveThreatHighOrderSigma`. Populated by
    ``ActiveThreatsSyncManager._sync_behavioral_memberships`` directly from
    ``TdmActiveThreat.related_behavioral_rules`` (TDM case ids) — no resolution to a
    local rule id, so the relation is recorded even before the referenced rule lands in
    the Active Threat system repository. ``external_id`` is intentionally not FK'd to
    ``rules`` (``rules.external_id`` is nullable and not unique across repositories); the
    read path resolves it against the system repo. Referenced rules are deduplicated with
    HOS-referenced rules and protected from cleanup while a membership exists.
    """

    __tablename__ = "active_threat_behavioral_rules"

    active_threat_id = Column(
        String(255),
        ForeignKey("active_threats.tdm_id", ondelete="CASCADE"),
        primary_key=True,
    )
    external_id = Column(
        String(255),
        primary_key=True,
    )

    __table_args__ = (Index("ix_active_threat_behavioral_rules_external_id", "external_id"),)


class CorrelationPipelineExcludedActiveThreat(Base):
    """One analyst opt-OUT: this correlation pipeline does NOT monitor this Active Threat (D8).

    A DENY-list, never an allow-list. Absent means monitored, so a new correlation pipeline
    inherits every non-blocked threat by writing zero rows and the "empty list means correlate
    nothing" trap cannot occur. Rows travel to the data plane as ``excluded_active_threat_ids`` on
    that pipeline's compacted ``detectflow.chains-config.<id>`` envelope — exclusions are rare, so
    the message stays small.

    ``ON DELETE CASCADE`` on both sides: an exclusion has no meaning without its pipeline or its
    threat. Losing rows to a catalogue delete is deliberate but never silent —
    ``ActiveThreatsSyncManager`` captures them first, logs a warning activity entry per affected
    pipeline and republishes the shrunken deny-list.
    """

    __tablename__ = "correlation_pipeline_excluded_active_threats"

    correlation_pipeline_id = Column(
        UUID(as_uuid=True),
        ForeignKey("correlation_pipelines.id", ondelete="CASCADE"),
        primary_key=True,
    )
    active_threat_tdm_id = Column(
        String(255),
        ForeignKey("active_threats.tdm_id", ondelete="CASCADE"),
        primary_key=True,
    )
    excluded_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    excluded_by = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    # Reverse lookup: which pipelines exclude this threat (the list view's Pipelines column).
    __table_args__ = (Index("ix_cp_excluded_active_threats_at_id", "active_threat_tdm_id"),)


class HighOrderSigmaRuleRef(Base):
    __tablename__ = "high_order_sigma_rule_refs"

    high_order_sigma_id = Column(
        UUID(as_uuid=True), ForeignKey("high_order_sigmas.id", ondelete="CASCADE"), primary_key=True
    )
    sigma_uuid = Column(UUID(as_uuid=True), primary_key=True)

    __table_args__ = (Index("ix_high_order_sigma_rule_refs_sigma_uuid", "sigma_uuid"),)

    high_order_sigma = relationship("HighOrderSigma", back_populates="rule_refs")


class HighOrderSigmaRuleCheckFailure(Base):
    __tablename__ = "high_order_sigma_rule_check_failures"

    high_order_sigma_id = Column(
        UUID(as_uuid=True),
        ForeignKey("high_order_sigmas.id", ondelete="CASCADE"),
        primary_key=True,
    )
    rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("rules.id", ondelete="CASCADE"),
        primary_key=True,
    )
    is_not_supported = Column(Boolean, nullable=False, server_default="false")
    is_not_connected = Column(Boolean, nullable=False, server_default="false")
    disabled_pipeline_ids = Column(
        ARRAY(UUID(as_uuid=True)),
        nullable=False,
        server_default=text("'{}'::uuid[]"),
    )
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_hos_rule_check_failures_hos_id", "high_order_sigma_id"),
        Index("ix_hos_rule_check_failures_rule_id", "rule_id"),
    )

    high_order_sigma = relationship("HighOrderSigma", back_populates="rule_check_failures")
    rule = relationship("Rule")


class AttackChain(Base):
    """Correlated attack chain (HoS / future Open Tide). ``id`` is set by correlation-job (stable hash)."""

    __tablename__ = "attack_chains"
    __table_args__ = (
        # Serves the threat detail's per-pipeline status bar: one GROUP BY over exactly these three.
        Index("ix_attack_chains_at_cp_status", "active_threat_id", "correlation_pipeline_id", "status"),
        # Consumer sticky-open lookup: one open row per (pipeline, HoS, hostname).
        Index(
            "ix_attack_chains_sticky_lookup",
            "correlation_pipeline_id",
            "hos_rule_id",
            "hostname",
            postgresql_where=text("lower(status) IN ('new', 'investigating')"),
        ),
    )

    id = Column(UUID(as_uuid=True), primary_key=True)
    hos_rule_id = Column(
        UUID(as_uuid=True),
        ForeignKey("high_order_sigmas.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    active_threat_id = Column(
        String(255),
        ForeignKey("active_threats.tdm_id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # SET NULL on purpose: a chain carries analyst-owned state (``status`` and the
    # priority/assignee/comments the chains-analysed consumer must never overwrite), so CASCADE
    # would destroy investigation history when a customer is offboarded, while RESTRICT would make
    # a correlation pipeline undeletable the moment it produced one chain. The column is nullable
    # anyway (legacy and unattributed chains), so orphaned chains stay valid, listable rows.
    correlation_pipeline_id = Column(
        UUID(as_uuid=True),
        ForeignKey("correlation_pipelines.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    chain_type = Column(String(20), nullable=False)  # HOS | OPEN_TIDE (MVP: HOS)
    hostname = Column(String(255), nullable=False)
    username = Column(String(255), nullable=True)
    status = Column(String(20), nullable=False, server_default="new")
    first_seen = Column(DateTime(timezone=True), nullable=False)
    last_seen = Column(DateTime(timezone=True), nullable=False)
    window_start = Column(DateTime(timezone=True), nullable=False, index=True)
    event_count = Column(Integer, nullable=False, server_default="0")
    rule_count = Column(Integer, nullable=False, server_default="0")
    seqs = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    match = Column(Integer, nullable=False, server_default="0")
    hos_tags = Column(ARRAY(String(255)), nullable=True)
    coverage = Column(Numeric(5, 2), nullable=False)
    pipeline_names = Column(ARRAY(Text), nullable=False, server_default=text("'{}'::text[]"))
    ai_summary = Column(Text, nullable=True)
    llm_verdict = Column(String(20), nullable=True)  # TRUE | SUSPICIOUS | FALSE_POSITIVE | INSUFFICIENT_CONTEXT
    llm_confidence = Column(Numeric(3, 2), nullable=True)
    llm_review = Column(JSONB, nullable=True)
    revision = Column(Integer, nullable=False, server_default="0")
    # Last Kafka wire ``chain_id`` applied by the consumer. Distinct from ``id`` after a
    # cross-window sticky merge; NULL until the first apply (or the backfill ``id``).
    last_wire_chain_id = Column(UUID(as_uuid=True), nullable=True)
    analysed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), nullable=False)

    hos_rule = relationship("HighOrderSigma", foreign_keys=[hos_rule_id])
    active_threat = relationship("ActiveThreat", foreign_keys=[active_threat_id])
    correlation_pipeline = relationship("CorrelationPipeline", foreign_keys=[correlation_pipeline_id])
    events = relationship(
        "AttackChainEvent",
        back_populates="chain",
        passive_deletes=True,
        lazy="noload",
        order_by="AttackChainEvent.seq, AttackChainEvent.timestamp",
    )


class AttackChainEvent(Base):
    """Raw upstream event attributed to an attack chain (one row per correlated event)."""

    __tablename__ = "attack_chain_events"
    __table_args__ = (
        UniqueConstraint("chain_id", "event_id", name="uq_attack_chain_events_chain_event"),
        Index("ix_attack_chain_events_chain_seq_ts", "chain_id", "seq", "timestamp"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    chain_id = Column(
        UUID(as_uuid=True),
        ForeignKey("attack_chains.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_id = Column(String(255), nullable=False)
    sigma_rule_id = Column(UUID(as_uuid=True), nullable=False)
    sigma_rule_title = Column(String(255), nullable=True)
    # Local ``rules.id`` from Kafka ``rule_db_id``. Intentionally not an FK: deleting a rule must
    # not fail ingest or cascade-delete correlated events. Null on legacy rows and when the
    # producer omitted or sent a non-UUID value.
    rule_id = Column(UUID(as_uuid=True), nullable=True)
    seq = Column(Integer, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    technique_ids = Column(ARRAY(Text), nullable=True)
    raw_event = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    chain = relationship("AttackChain", back_populates="events")


class AttackChainConfiguration(Base):
    """DEPRECATED singleton correlation settings (single logical row, ``id`` must be 1).

    Superseded by :class:`CorrelationPipeline`. The migration
    ``c7e1f04b93aa_migrate_singleton_config_to_default_pipeline`` copies this row onto a ``Default``
    correlation pipeline and stops the runtime from reading it, but deliberately **keeps** the
    table: it is the only surviving record of the pre-migration ``correlation_window_seconds`` and
    ``monitored_topics`` (the migration carries the window across as NULL on purpose), and the
    cheapest rollback source for that deploy. A follow-up drops it one release later.

    The model class is kept alongside the table so ``alembic revision --autogenerate`` does not
    propose a ``drop_table`` nobody asked for. Its only remaining readers are the two deprecated
    ``/correlation_job/configuration`` operations, which
    ``rebind-singleton-endpoints-to-default-pipeline.md`` re-points at the ``Default`` row.

    ``jobs_status`` stored the desired control state for the composite Attack Chains runtime; the
    live equivalent is ``correlation_pipelines.jobs_status``.
    """

    __tablename__ = "attack_chain_configuration"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_attack_chain_configuration_singleton_id"),
        CheckConstraint(
            "threshold_pct >= 50 AND threshold_pct <= 100 AND threshold_pct % 5 = 0",
            name="ck_attack_chain_configuration_threshold_pct",
        ),
        CheckConstraint(
            "correlation_window_seconds >= 30 AND correlation_window_seconds <= 10800",
            name="ck_attack_chain_configuration_window_seconds",
        ),
        CheckConstraint(
            "jobs_status IN ('starting', 'stopping', 'stopped')",
            name="ck_attack_chain_configuration_jobs_status",
        ),
    )

    id = Column(Integer, primary_key=True, server_default="1")
    threshold_pct = Column(Integer, nullable=False, server_default="60")
    correlation_window_seconds = Column(Integer, nullable=False, server_default="300")
    monitored_topics = Column(ARRAY(Text), nullable=False, server_default=text("'{}'::text[]"))
    auto_add_new_pipelines = Column(Boolean, nullable=False, server_default="false")
    jobs_status = Column(String(20), nullable=False, server_default="stopped")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), nullable=False)
    updated_by = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)


class ChainsAnalysedDeadLetter(Base):
    """A ``detectflow.chains-analysed`` record that could not be applied, parked with its payload.

    Written **before** the consumer commits past a poison record, so the payload survives the skip:
    ``events[]`` is batch-scoped (``_upsert_chain_events`` only inserts what the message carried and
    the counts are recomputed from what is stored), so a discarded message loses those event rows
    for good. Parking first is what makes skipping recoverable instead of silent loss.

    One row per ``(kafka_topic, kafka_partition, kafka_offset)``: redelivery after a rebalance bumps
    ``attempts`` and refreshes the error instead of duplicating the row.

    ``kafka_topic`` is free text on purpose — it holds
    ``detectflow.chains-analysed.<correlation_pipeline_id>`` once the topic goes per correlation
    pipeline, with no schema change. ``chain_id`` carries **no** FK: the chain, its high-order Sigma
    and its Active Threat may all be absent, which is usually the reason the row is here at all.
    """

    __tablename__ = "chains_analysed_dead_letters"
    __table_args__ = (
        UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset", name="uq_chains_dlq_tp_offset"),
        Index("ix_chains_dlq_created_at", "created_at"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    kafka_topic = Column(String(255), nullable=False)
    kafka_partition = Column(Integer, nullable=False)
    kafka_offset = Column(BigInteger, nullable=False)
    chain_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    hos_rule_id = Column(String(255), nullable=True)
    error_class = Column(String(20), nullable=False)
    error_type = Column(String(255), nullable=False)
    error_message = Column(Text, nullable=False)
    attempts = Column(Integer, nullable=False, server_default="1")
    payload = Column(Text, nullable=True)
    payload_truncated = Column(Boolean, nullable=False, server_default="false")
    first_failed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_failed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    replayed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# Resource-column bounds for ``CorrelationPipeline``, one set per Flink job ("correlation" and
# "agent"): (column suffix, CHECK-name suffix, bounds SQL). NULL always passes — it means
# "inherit the platform default from settings", mirroring ``Pipeline``'s nullable resource
# columns, which is what keeps the production 4g/3.0 correlation numbers in force with no data
# migration. Mirrored by the Alembic revision that creates the table.
_CORRELATION_RESOURCE_BOUNDS: tuple[tuple[str, str, str], ...] = (
    ("parallelism", "parallelism", "BETWEEN 1 AND 64"),
    ("taskmanager_memory_mb", "tm_memory_mb", "BETWEEN 1024 AND 65536"),
    ("taskmanager_cpu", "tm_cpu", "> 0 AND {col} <= 32"),
    ("jobmanager_memory_mb", "jm_memory_mb", "BETWEEN 512 AND 32768"),
    ("jobmanager_cpu", "jm_cpu", "> 0 AND {col} <= 16"),
)


def _correlation_resource_checks(prefix: str) -> tuple[CheckConstraint, ...]:
    """Five bounds CHECKs for one job's resource columns; NULL is always allowed."""
    checks = []
    for column, short, bounds in _CORRELATION_RESOURCE_BOUNDS:
        col = f"{prefix}_{column}"
        checks.append(
            CheckConstraint(
                f"{col} IS NULL OR ({col} {bounds.format(col=col)})",
                name=f"ck_correlation_pipelines_{prefix}_{short}",
            )
        )
    return tuple(checks)


class CorrelationPipeline(Base):
    """One customer-scoped correlation runtime: its member detection pipelines, correlation
    parameters and the resources of both its Flink jobs (correlation-job + agent-job).

    Replaces the ``attack_chain_configuration`` singleton as the unit of correlation. Nullable
    resource columns mean "inherit ``settings.*``"; ``correlation_window_seconds`` is nullable
    because NULL means "inherit each HoS rule's ``detection.timeframe``" while a value is an
    explicit override that wins over it.
    """

    __tablename__ = "correlation_pipelines"
    __table_args__ = (
        CheckConstraint(
            "threshold_pct >= 50 AND threshold_pct <= 100 AND threshold_pct % 5 = 0",
            name="ck_correlation_pipelines_threshold_pct",
        ),
        CheckConstraint(
            "correlation_window_seconds IS NULL OR "
            "(correlation_window_seconds >= 30 AND correlation_window_seconds <= 10800)",
            name="ck_correlation_pipelines_window_seconds",
        ),
        CheckConstraint(
            "jobs_status IN ('starting', 'stopping', 'stopped')",
            name="ck_correlation_pipelines_jobs_status",
        ),
        CheckConstraint(
            "auto_add_pipeline_name_pattern IS NULL OR length(btrim(auto_add_pipeline_name_pattern)) > 0",
            name="ck_correlation_pipelines_auto_add_pipeline_name_pattern",
        ),
        *_correlation_resource_checks("correlation"),
        *_correlation_resource_checks("agent"),
        UniqueConstraint("name", name="uq_correlation_pipelines_name"),
    )

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    jobs_status = Column(String(20), nullable=False, server_default="stopped")
    threshold_pct = Column(Integer, nullable=False, server_default="60")
    # NULL = inherit each HoS rule's detection.timeframe; a value overrides it.
    correlation_window_seconds = Column(Integer, nullable=True)
    auto_add_new_pipelines = Column(Boolean, nullable=False, server_default="false")
    # Glob (fnmatch) evaluated against a new detection pipeline's name when
    # auto_add_new_pipelines is on. NULL = match every new pipeline (the pre-epic behaviour).
    auto_add_pipeline_name_pattern = Column(String(255), nullable=True)

    # Flink resources per job (nullable = use the global defaults from settings)
    correlation_parallelism = Column(Integer, nullable=True)
    correlation_taskmanager_memory_mb = Column(Integer, nullable=True)
    correlation_taskmanager_cpu = Column(Float, nullable=True)
    correlation_jobmanager_memory_mb = Column(Integer, nullable=True)
    correlation_jobmanager_cpu = Column(Float, nullable=True)
    agent_parallelism = Column(Integer, nullable=True)
    agent_taskmanager_memory_mb = Column(Integer, nullable=True)
    agent_taskmanager_cpu = Column(Float, nullable=True)
    agent_jobmanager_memory_mb = Column(Integer, nullable=True)
    agent_jobmanager_cpu = Column(Float, nullable=True)

    # What was actually deployed (apps/core/constants.py builds the names)
    correlation_deployment_name = Column(String(63), nullable=True)
    agent_deployment_name = Column(String(63), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), nullable=False)
    updated_by = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)

    members = relationship(
        "Pipeline",
        secondary="correlation_pipeline_members",
        backref=backref("correlation_pipelines"),
        lazy="noload",
    )
    # D8 deny-list: the threats this pipeline does NOT monitor. Never loaded implicitly — every
    # read path is a batched lookup over many pipelines or many threats at once.
    excluded_active_threats = relationship(
        "ActiveThreat",
        secondary="correlation_pipeline_excluded_active_threats",
        lazy="noload",
    )


# Junction table for CorrelationPipeline ↔ Pipeline membership. Membership is keyed by
# ``pipeline_id`` (``pipelines.destination_topic`` has no unique constraint, so a topic name does
# not identify a customer). CASCADE on both sides: membership has no meaning of its own.
# A detection pipeline MAY belong to several correlation pipelines: each job independently
# consumes that member's tagged-events topic (duplicate chains are expected).
correlation_pipeline_members = Table(
    "correlation_pipeline_members",
    Base.metadata,
    Column(
        "correlation_pipeline_id",
        UUID(as_uuid=True),
        ForeignKey("correlation_pipelines.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "pipeline_id",
        UUID(as_uuid=True),
        ForeignKey("pipelines.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Index("ix_correlation_pipeline_members_pipeline_id", "pipeline_id"),
)


class Pipeline(Base):
    __tablename__ = "pipelines"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    enabled = Column(Boolean, server_default="true", index=True)

    # Topic references (Strings directly from Kafka)
    # source_topics: Array of input topics (Flink unions them)
    # Index created via Alembic migration with GIN type for array containment queries
    source_topics = Column(ARRAY(String(255)), nullable=False, server_default="{}")
    # Legacy singular destination (mirrored from destinations[0] for old clients / sort)
    destination_topic = Column(String(255), nullable=False, index=True)
    # Multi-destination Kafka outputs: [{topic, output_mode, apply_parser_to_output_events}, ...]
    destinations = Column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))

    # Orthogonal to ``destinations``: those are the customer-facing sinks, this is the internal
    # side-output the correlation job consumes. Per-pipeline, fixed at assignment.
    # NULL = still on the legacy shared settings.kafka_tagged_events_topic (never backfilled).
    tagged_events_topic = Column(String(255), nullable=True, unique=True)

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
    rules_topic = Column(String(255), server_default="detectflow.sigma-rules")
    metrics_topic = Column(String(255), server_default="detectflow.rule-metrics")
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

    # Flink job version (updated from running job, persists for history)
    detectflow_matchnode_version = Column(String(50), nullable=True)  # e.g., "v1.2.0"

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
    Column("folder_paths", ARRAY(Text), nullable=False, server_default=text("'{}'::text[]")),
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


class HealthCheck(Base):
    """Health check: each row has name and checks (JSONB array of {status, title, descriptions, updated})."""

    __tablename__ = "health_check"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False, unique=True)
    # checks: list[{"status": HealthCheckStatus, "title": str, "descriptions": [str], "updated": datetime}]
    checks = Column(JSONB, nullable=False, server_default="[]")
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class GitlabRepository(Base):
    """Links a GitLab integration profile to its owned rules repository and sync cursor."""

    __tablename__ = "gitlab_repository"
    __table_args__ = (UniqueConstraint("integration_id", name="uq_gitlab_repository_integration_id"),)

    repository_id = Column(
        UUID(as_uuid=True),
        ForeignKey("repositories.id", ondelete="CASCADE"),
        primary_key=True,
    )
    integration_id = Column(
        UUID(as_uuid=True),
        ForeignKey("integrations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    last_synced_commit_sha = Column(Text, nullable=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())

    repository = relationship("Repository", back_populates="gitlab_repository")
    integration = relationship("Integration", lazy="noload")


class GitlabRuleMetadata(Base):
    """Per-rule GitLab sync state keyed by file path within the project subpath."""

    __tablename__ = "gitlab_rule_metadata"
    __table_args__ = (Index("ix_gitlab_rule_metadata_repository_file_path", "repository_id", "file_path"),)

    rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id", ondelete="CASCADE"), primary_key=True)
    repository_id = Column(UUID(as_uuid=True), ForeignKey("repositories.id", ondelete="CASCADE"), nullable=False)
    file_path = Column(Text, nullable=False)
    last_commit_id = Column(Text, nullable=True)
    synced_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    rule = relationship("Rule", back_populates="gitlab_metadata")
    repository = relationship("Repository", lazy="noload")


class Integration(Base):
    __tablename__ = "integrations"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    name = Column(String(255), nullable=False)
    integration_type = Column(String(50), nullable=False, index=True)
    category = Column(String(50), nullable=False, index=True)
    status = Column(String(20), nullable=False, server_default="disconnected")
    config = Column(JSONB, nullable=False, server_default="{}")
    status_message = Column(String(512), nullable=True)
    last_checked_at = Column(DateTime(timezone=True), nullable=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    updated = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now())


class RuleLoaderModuleVersion(Base):
    """Tracks rule loader module versions for sigma validation.

    When a new version is deployed, rules need to be re-validated against the new schema.
    """

    __tablename__ = "rule_loader_module_versions"

    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()"))
    version = Column(String(50), nullable=False)
    is_current = Column(Boolean, default=False, nullable=False, server_default="false")
    created = Column(DateTime(timezone=True), server_default=func.now())
