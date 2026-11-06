"""Schema/contract tests for correlation pipelines.

Pins the persistence contract (columns, CHECK/UNIQUE names, delete semantics), the generated
Deployment/topic names, and the Pydantic request/response bounds. Pure metadata + validation —
no database required.
"""

import re
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, Index, UniqueConstraint

from apps.core.constants import (
    agent_deployment_name,
    chains_analysed_topic,
    chains_config_topic,
    chains_raw_topic,
    correlation_deployment_name,
    pipeline_name_matches_auto_add_pattern,
    scoped_topic,
    tagged_events_topic,
)
from apps.core.models import AttackChain, CorrelationPipeline, Pipeline, correlation_pipeline_members
from apps.core.schemas import (
    CorrelationPipelineCreateRequest,
    CorrelationPipelineMember,
    CorrelationPipelineUpdateRequest,
)

# DNS-1123 label: lowercase alphanumeric or '-', start/end with alphanumeric, ≤63 chars.
_DNS_1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

_RESOURCE_COLUMNS = [
    f"{prefix}_{suffix}"
    for prefix in ("correlation", "agent")
    for suffix in (
        "parallelism",
        "taskmanager_memory_mb",
        "taskmanager_cpu",
        "jobmanager_memory_mb",
        "jobmanager_cpu",
    )
]

_EXPECTED_CHECK_NAMES = {
    "ck_correlation_pipelines_threshold_pct",
    "ck_correlation_pipelines_window_seconds",
    "ck_correlation_pipelines_jobs_status",
    "ck_correlation_pipelines_auto_add_pipeline_name_pattern",
} | {
    f"ck_correlation_pipelines_{prefix}_{short}"
    for prefix in ("correlation", "agent")
    for short in ("parallelism", "tm_memory_mb", "tm_cpu", "jm_memory_mb", "jm_cpu")
}


class TestCorrelationPipelineTable:
    """The ``correlation_pipelines`` table contract."""

    def test_check_constraint_names_are_exact(self):
        """All 14 CHECKs exist under the ck_correlation_pipelines_* vocabulary, and no others."""
        names = {c.name for c in CorrelationPipeline.__table__.constraints if isinstance(c, CheckConstraint)}
        assert names == _EXPECTED_CHECK_NAMES
        assert len(names) == 14

    def test_unique_name_constraint(self):
        """Name is an identity key — the migration runbook resolves the default pipeline by name."""
        unique_names = {c.name for c in CorrelationPipeline.__table__.constraints if isinstance(c, UniqueConstraint)}
        assert "uq_correlation_pipelines_name" in unique_names

    def test_constraint_identifiers_fit_postgres_limit(self):
        """Every generated identifier must stay within Postgres' 63-char limit."""
        for name in _EXPECTED_CHECK_NAMES | {"uq_correlation_pipelines_name"}:
            assert len(name) <= 63

    def test_window_is_nullable_but_threshold_is_not(self):
        """NULL window = inherit each HoS rule's detection.timeframe; threshold always has a value."""
        columns = CorrelationPipeline.__table__.c
        assert columns.correlation_window_seconds.nullable is True
        assert columns.threshold_pct.nullable is False

    def test_all_ten_resource_columns_exist_and_are_nullable(self):
        """Nullable = inherit settings.*, which is what keeps the production 4g/3.0 numbers in force."""
        columns = CorrelationPipeline.__table__.c
        for column in _RESOURCE_COLUMNS:
            assert column in columns, f"missing resource column {column}"
            assert columns[column].nullable is True, f"{column} must be nullable (inherit default)"

    def test_auto_add_pipeline_name_pattern_is_nullable(self):
        """NULL pattern = auto-add matches every new detection pipeline name (pre-epic behaviour)."""
        assert CorrelationPipeline.__table__.c.auto_add_pipeline_name_pattern.nullable is True


class TestCorrelationPipelineMembers:
    """The membership join table contract (a pipeline may belong to several correlation pipelines)."""

    def test_no_unique_on_pipeline_id_alone(self):
        unique_constraints = [c for c in correlation_pipeline_members.constraints if isinstance(c, UniqueConstraint)]
        assert unique_constraints == []

    def test_pipeline_id_index_serves_the_reverse_lookup(self):
        indexes = [idx for idx in correlation_pipeline_members.indexes if isinstance(idx, Index)]
        assert any(idx.name == "ix_correlation_pipeline_members_pipeline_id" for idx in indexes)

    def test_membership_cascades_on_both_sides(self):
        """Membership is a pure relation — it has no meaning once either side is gone."""
        deletes = {fk.column.table.name: fk.ondelete for fk in correlation_pipeline_members.foreign_keys}
        assert deletes == {"correlation_pipelines": "CASCADE", "pipelines": "CASCADE"}


class TestRelatedTableExtensions:
    """Additive columns on existing tables."""

    def test_attack_chain_correlation_pipeline_id_is_set_null(self):
        """SET NULL, never CASCADE: chains carry analyst-owned state that must survive offboarding."""
        column = AttackChain.__table__.c.correlation_pipeline_id
        assert column.nullable is True
        assert column.index is True
        foreign_key = next(iter(column.foreign_keys))
        assert foreign_key.column.table.name == "correlation_pipelines"
        assert foreign_key.ondelete == "SET NULL"

    def test_pipeline_tagged_events_topic_is_nullable_and_unique(self):
        """NULL is load-bearing: it means 'still on the legacy shared tagged-events topic'."""
        column = Pipeline.__table__.c.tagged_events_topic
        assert column.nullable is True
        assert column.unique is True


class TestGeneratedNames:
    """Deployment names must stay inside the DNS-1123 budget every derived name is built on."""

    def test_deployment_names_are_dns_1123_and_within_budget(self):
        pipeline_id = uuid4()
        for name in (correlation_deployment_name(pipeline_id), agent_deployment_name(pipeline_id)):
            assert _DNS_1123_LABEL.match(name), name
            assert len(name) <= 42
            assert name == name.lower()
            # cleanup Job name is the longest derived name this repo generates.
            assert len(f"cleanup-{name}") <= 63
            assert _DNS_1123_LABEL.match(f"cleanup-{name}")

    def test_deployment_names_are_stable_and_case_insensitive(self):
        pipeline_id = uuid4()
        assert correlation_deployment_name(pipeline_id) == correlation_deployment_name(str(pipeline_id).upper())
        assert agent_deployment_name(pipeline_id) == agent_deployment_name(str(pipeline_id))

    def test_deployment_prefixes_are_distinct(self):
        pipeline_id = uuid4()
        assert correlation_deployment_name(pipeline_id) != agent_deployment_name(pipeline_id)


class TestGeneratedTopics:
    """Topic builders take the prefix as an argument — the module stays settings-free."""

    def test_tagged_events_topic_shape(self):
        pipeline_id = uuid4()
        assert tagged_events_topic("detectflow.tagged-events", pipeline_id) == (
            f"detectflow.tagged-events.{pipeline_id}"
        )

    def test_correlation_scoped_topic_shapes(self):
        correlation_pipeline_id = uuid4()
        assert chains_raw_topic("detectflow.chains-raw", correlation_pipeline_id) == (
            f"detectflow.chains-raw.{correlation_pipeline_id}"
        )
        assert chains_analysed_topic("detectflow.chains-analysed", correlation_pipeline_id) == (
            f"detectflow.chains-analysed.{correlation_pipeline_id}"
        )
        assert chains_config_topic("detectflow.chains-config", correlation_pipeline_id) == (
            f"detectflow.chains-config.{correlation_pipeline_id}"
        )

    @pytest.mark.parametrize("prefix", ["", "   "])
    def test_blank_prefix_raises_instead_of_yielding_dot_topic(self, prefix):
        """An empty prefix is a caller bug — silently producing '.<uuid>' would be worse."""
        with pytest.raises(ValueError, match="non-empty"):
            scoped_topic(prefix, uuid4())


class TestAutoAddPipelineNamePattern:
    """``auto_add_pipeline_name_pattern`` is a glob, matched case-sensitively against pipeline names."""

    @pytest.mark.parametrize("pattern", [None, "", "   "])
    def test_absent_pattern_matches_everything(self, pattern):
        assert pipeline_name_matches_auto_add_pattern(pattern, "anything-at-all") is True

    def test_glob_matches_expected_names_only(self):
        assert pipeline_name_matches_auto_add_pattern("cust1-*", "cust1-edr") is True
        assert pipeline_name_matches_auto_add_pattern("cust1-*", "cust2-edr") is False

    def test_matching_is_case_sensitive(self):
        assert pipeline_name_matches_auto_add_pattern("cust1-*", "CUST1-edr") is False

    def test_spaces_in_pipeline_names_are_matched(self):
        assert pipeline_name_matches_auto_add_pattern("Customer *", "Customer A") is True
        assert pipeline_name_matches_auto_add_pattern("Customer *", "CustomerA") is False


class TestCorrelationPipelineRequestSchemas:
    """Pydantic bounds mirror the DB CHECKs exactly."""

    def test_threshold_must_be_multiple_of_five(self):
        with pytest.raises(ValidationError):
            CorrelationPipelineCreateRequest(name="cp", threshold_pct=63)
        assert CorrelationPipelineCreateRequest(name="cp", threshold_pct=65).threshold_pct == 65

    def test_window_none_is_accepted_and_bounds_enforced(self):
        assert CorrelationPipelineCreateRequest(name="cp").correlation_window_seconds is None
        assert CorrelationPipelineCreateRequest(name="cp", correlation_window_seconds=None) is not None
        for invalid in (29, 10801):
            with pytest.raises(ValidationError):
                CorrelationPipelineCreateRequest(name="cp", correlation_window_seconds=invalid)

    @pytest.mark.parametrize(
        "resources",
        [
            {"parallelism": 0},
            {"parallelism": 65},
            {"taskmanager_memory_mb": 1023},
            {"taskmanager_memory_mb": 65537},
            {"taskmanager_cpu": 0},
            {"taskmanager_cpu": 33},
            {"jobmanager_memory_mb": 511},
            {"jobmanager_memory_mb": 32769},
            {"jobmanager_cpu": 0},
            {"jobmanager_cpu": 17},
        ],
    )
    def test_resource_bounds_rejected_out_of_range(self, resources):
        with pytest.raises(ValidationError):
            CorrelationPipelineCreateRequest(name="cp", correlation_resources=resources)

    def test_resources_accept_the_production_correlation_numbers(self):
        request = CorrelationPipelineCreateRequest(
            name="cp",
            correlation_resources={"taskmanager_memory_mb": 4096, "taskmanager_cpu": 3.0},
        )
        assert request.correlation_resources.taskmanager_cpu == 3.0
        assert request.correlation_resources.parallelism is None  # null = inherit

    def test_requests_forbid_unknown_fields(self):
        with pytest.raises(ValidationError):
            CorrelationPipelineCreateRequest(name="cp", monitored_active_threat_ids=[])
        with pytest.raises(ValidationError):
            CorrelationPipelineUpdateRequest(unknown_field=1)

    def test_empty_update_is_rejected(self):
        with pytest.raises(ValidationError, match="At least one field"):
            CorrelationPipelineUpdateRequest()

    def test_auto_add_pipeline_name_pattern_rejects_regex_characters(self):
        with pytest.raises(ValidationError):
            CorrelationPipelineCreateRequest(name="cp", auto_add_pipeline_name_pattern="cust1-.*|x")

    def test_auto_add_pipeline_name_pattern_accepts_spaces(self):
        assert (
            CorrelationPipelineCreateRequest(
                name="cp", auto_add_pipeline_name_pattern="Customer *"
            ).auto_add_pipeline_name_pattern
            == "Customer *"
        )

    def test_auto_add_pipeline_name_pattern_normalises_blank_to_none(self):
        empty = CorrelationPipelineCreateRequest(name="cp", auto_add_pipeline_name_pattern="")
        blank = CorrelationPipelineCreateRequest(name="cp", auto_add_pipeline_name_pattern="  ")
        stripped = CorrelationPipelineCreateRequest(name="cp", auto_add_pipeline_name_pattern=" cust1-* ")
        assert empty.auto_add_pipeline_name_pattern is None
        assert blank.auto_add_pipeline_name_pattern is None
        assert stripped.auto_add_pipeline_name_pattern == "cust1-*"

    def test_member_identity_is_pipeline_id_not_destination_topic(self):
        """Two members may share a destination topic — it has no unique constraint."""
        with pytest.raises(ValidationError):
            CorrelationPipelineMember(name="p1", destination_topic="shared", enabled=True)
        first = CorrelationPipelineMember(pipeline_id=uuid4(), name="p1", destination_topic="shared", enabled=True)
        second = CorrelationPipelineMember(pipeline_id=uuid4(), name="p2", destination_topic="shared", enabled=False)
        assert first.pipeline_id != second.pipeline_id
        assert first.tagged_events_topic is None  # null = legacy shared topic
