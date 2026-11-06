"""Unit tests for apps/core/schemas.py validators."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from apps.core.schemas import (
    AttackChainConfigurationUpdateRequest,
    ChainsAnalysedEvent,
    ChainsAnalysedMessage,
    FlinkResourceConfig,
    PipelineCreateRequest,
    PipelineUpdateRequest,
    get_reserved_topics,
    validate_correlation_window_seconds,
    validate_threshold_pct,
    validate_topic_not_reserved,
    validate_uuid4,
    validate_yaml,
)
from apps.core.settings import settings


class TestValidateYaml:
    """Tests for validate_yaml function."""

    def test_valid_yaml_returns_value(self, valid_yaml):
        """Valid YAML should be returned unchanged."""
        result = validate_yaml(valid_yaml)
        assert result == valid_yaml

    def test_none_returns_none(self):
        """None input should return None."""
        assert validate_yaml(None) is None

    def test_invalid_yaml_raises_error(self, invalid_yaml):
        """Invalid YAML should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid YAML format"):
            validate_yaml(invalid_yaml)

    def test_empty_string_is_valid(self):
        """Empty string is valid YAML."""
        result = validate_yaml("")
        assert result == ""

    def test_simple_key_value(self):
        """Simple key-value YAML is valid."""
        yaml_str = "key: value"
        result = validate_yaml(yaml_str)
        assert result == yaml_str

    def test_yaml_with_list(self):
        """YAML with list is valid."""
        yaml_str = "items:\n  - one\n  - two\n  - three"
        result = validate_yaml(yaml_str)
        assert result == yaml_str

    def test_yaml_with_special_characters(self):
        """YAML with special characters should be handled."""
        yaml_str = 'key: "value with: colon"'
        result = validate_yaml(yaml_str)
        assert result == yaml_str


class TestValidateUuid4:
    """Tests for validate_uuid4 function."""

    def test_valid_uuid_string(self, valid_uuid):
        """Valid UUID string should be returned unchanged."""
        result = validate_uuid4(valid_uuid)
        assert result == valid_uuid

    def test_none_returns_none(self):
        """None input should return None."""
        assert validate_uuid4(None) is None

    def test_uuid_object(self, valid_uuid):
        """UUID object should be converted to string."""
        uuid_obj = UUID(valid_uuid)
        result = validate_uuid4(uuid_obj)
        assert result == valid_uuid

    def test_invalid_uuid_raises_error(self, invalid_uuid):
        """Invalid UUID should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid UUID format"):
            validate_uuid4(invalid_uuid)

    def test_empty_string_raises_error(self):
        """Empty string should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid UUID format"):
            validate_uuid4("")

    def test_uppercase_uuid(self):
        """Uppercase UUID should be valid."""
        uuid_str = "550E8400-E29B-41D4-A716-446655440000"
        result = validate_uuid4(uuid_str)
        assert result == uuid_str

    def test_lowercase_uuid(self):
        """Lowercase UUID should be valid."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        result = validate_uuid4(uuid_str)
        assert result == uuid_str

    def test_uuid_without_hyphens_is_valid(self):
        """UUID without hyphens is valid in Python."""
        # Python's uuid module accepts UUIDs without hyphens
        result = validate_uuid4("550e8400e29b41d4a716446655440000")
        assert result == "550e8400e29b41d4a716446655440000"


class TestChainsAnalysedMessage:
    """Tests for detectflow.chains-analysed payload validation."""

    def test_accepts_hos_tdm_short_id(self):
        """Correlation sends HoS TDM short ids, not DB UUIDs."""
        now = datetime(2026, 1, 1, tzinfo=UTC)

        message = ChainsAnalysedMessage.model_validate(
            {
                "chain_id": "550e8400-e29b-41d4-a716-446655440000",
                "revision": 1,
                "hos_rule_id": "PthCvxsxjnSK",
                "active_threat_id": "AT-123",
                "chain_type": "HOS",
                "hostname": "host-a",
                "first_seen": now.isoformat(),
                "last_seen": now.isoformat(),
                "window_start": now.isoformat(),
                "event_count": 1,
                "rule_count": 1,
                "coverage": 100.0,
                "events": [
                    {
                        "event_id": "event-1",
                        "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
                        "seq": 1,
                        "timestamp": now.isoformat(),
                        "parsed_event": {"event": {"action": "process_start"}, "host": {"name": "host-a"}},
                    }
                ],
            }
        )

        assert message.hos_rule_id == "PthCvxsxjnSK"
        assert message.events[0].sigma_rule_id == "660e8400-e29b-41d4-a716-446655440000"
        assert message.events[0].raw_event == {"event": {"action": "process_start"}, "host": {"name": "host-a"}}

    def test_generates_event_id_when_null(self):
        """Agent payloads may omit raw event identifiers."""
        now = datetime(2026, 1, 1, tzinfo=UTC)
        event = {
            "event_id": None,
            "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
            "seq": 1,
            "timestamp": now.isoformat(),
            "parsed_event": {"event": {"action": "process_start"}, "host": {"name": "host-a"}},
        }

        message = ChainsAnalysedMessage.model_validate(
            {
                "chain_id": "550e8400-e29b-41d4-a716-446655440000",
                "revision": 1,
                "hos_rule_id": "PthCvxsxjnSK",
                "active_threat_id": "AT-123",
                "chain_type": "HOS",
                "hostname": "host-a",
                "first_seen": now.isoformat(),
                "last_seen": now.isoformat(),
                "window_start": now.isoformat(),
                "event_count": 1,
                "rule_count": 1,
                "coverage": 100.0,
                "events": [event],
            }
        )
        repeated = ChainsAnalysedMessage.model_validate(
            {
                "chain_id": "550e8400-e29b-41d4-a716-446655440000",
                "revision": 1,
                "hos_rule_id": "PthCvxsxjnSK",
                "active_threat_id": "AT-123",
                "chain_type": "HOS",
                "hostname": "host-a",
                "first_seen": now.isoformat(),
                "last_seen": now.isoformat(),
                "window_start": now.isoformat(),
                "event_count": 1,
                "rule_count": 1,
                "coverage": 100.0,
                "events": [event],
            }
        )

        assert message.events[0].event_id.startswith("synthetic:")
        assert message.events[0].event_id == repeated.events[0].event_id

    def test_generated_event_id_uses_raw_event_alias_consistently(self):
        """`parsed_event` and `raw_event` aliases should produce the same synthetic id."""
        now = datetime(2026, 1, 1, tzinfo=UTC)
        base_event = {
            "event_id": None,
            "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
            "seq": 1,
            "timestamp": now.isoformat(),
        }
        common_message = {
            "chain_id": "550e8400-e29b-41d4-a716-446655440000",
            "revision": 1,
            "hos_rule_id": "PthCvxsxjnSK",
            "active_threat_id": "AT-123",
            "chain_type": "HOS",
            "hostname": "host-a",
            "first_seen": now.isoformat(),
            "last_seen": now.isoformat(),
            "window_start": now.isoformat(),
            "event_count": 1,
            "rule_count": 1,
            "coverage": 100.0,
        }

        parsed_message = ChainsAnalysedMessage.model_validate(
            {
                **common_message,
                "events": [{**base_event, "parsed_event": {"event": {"action": "process_start"}}}],
            }
        )
        raw_message = ChainsAnalysedMessage.model_validate(
            {
                **common_message,
                "events": [{**base_event, "raw_event": {"event": {"action": "process_start"}}}],
            }
        )

        assert parsed_message.events[0].event_id == raw_message.events[0].event_id

    def test_accepts_rule_db_id_from_kafka(self):
        """``extra='ignore'`` used to drop Kafka ``rule_db_id``; it must now persist as ``rule_id``."""
        now = datetime(2026, 1, 1, tzinfo=UTC)
        rule_id = "770e8400-e29b-41d4-a716-446655440000"
        event = ChainsAnalysedEvent.model_validate(
            {
                "event_id": "event-1",
                "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
                "rule_db_id": rule_id,
                "seq": 1,
                "timestamp": now.isoformat(),
            }
        )
        assert event.rule_id == UUID(rule_id)

    @pytest.mark.parametrize("bad_rule_id", ["not-a-uuid", "", "corr-123", "null"])
    def test_invalid_rule_id_degrades_to_none_without_failing_the_chain(self, bad_rule_id: str):
        """A malformed ``rule_db_id`` must not 422 the envelope — degrade that field only."""
        now = datetime(2026, 1, 1, tzinfo=UTC)
        message = ChainsAnalysedMessage.model_validate(
            {
                "chain_id": "550e8400-e29b-41d4-a716-446655440000",
                "revision": 1,
                "hos_rule_id": "PthCvxsxjnSK",
                "active_threat_id": "AT-123",
                "chain_type": "HOS",
                "hostname": "host-a",
                "first_seen": now.isoformat(),
                "last_seen": now.isoformat(),
                "window_start": now.isoformat(),
                "event_count": 1,
                "rule_count": 1,
                "coverage": 100.0,
                "events": [
                    {
                        "event_id": "event-1",
                        "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
                        "rule_db_id": bad_rule_id,
                        "seq": 1,
                        "timestamp": now.isoformat(),
                    }
                ],
            }
        )
        assert message.events[0].rule_id is None
        assert message.events[0].sigma_rule_id == "660e8400-e29b-41d4-a716-446655440000"

    def test_missing_rule_id_defaults_to_none(self):
        now = datetime(2026, 1, 1, tzinfo=UTC)
        event = ChainsAnalysedEvent.model_validate(
            {
                "event_id": "event-1",
                "sigma_rule_id": "660e8400-e29b-41d4-a716-446655440000",
                "seq": 1,
                "timestamp": now.isoformat(),
            }
        )
        assert event.rule_id is None


class TestReservedTopics:
    """Tests for reserved topic validation."""

    def test_get_reserved_topics_returns_set(self):
        """get_reserved_topics should return a set of topic names."""
        reserved = get_reserved_topics()
        assert isinstance(reserved, set)
        assert len(reserved) >= 3

    def test_validate_topic_not_reserved_passes_for_normal_topic(self):
        """Normal topics should pass validation."""
        validate_topic_not_reserved("my-events", "Source topic")
        validate_topic_not_reserved("logs-output", "Destination topic")

    def test_validate_topic_not_reserved_fails_for_reserved_topic(self):
        """Reserved topics should raise ValueError."""
        reserved = get_reserved_topics()
        for topic in reserved:
            with pytest.raises(ValueError, match="reserved system topic"):
                validate_topic_not_reserved(topic, "Source topic")

    def test_chains_raw_topic_is_reserved(self):
        """chains-raw carries correlation output and was missing from the reserved set."""
        assert settings.kafka_chains_raw_topic in get_reserved_topics()

    @pytest.mark.parametrize(
        "prefix_setting",
        [
            "kafka_tagged_events_topic",
            "kafka_chains_raw_topic",
            "kafka_chains_analysed_topic",
            "kafka_chains_config_topic",
        ],
    )
    def test_generated_per_entity_topics_are_rejected(self, prefix_setting):
        """``<reserved prefix>.<uuid>`` names are generated per pipeline — users may not claim them."""
        generated = f"{getattr(settings, prefix_setting)}.{uuid4()}"
        with pytest.raises(ValueError, match="reserved prefix"):
            validate_topic_not_reserved(generated, "Destination topic")

    def test_similar_name_outside_the_prefix_is_still_allowed(self):
        """Only the '<prefix>.' namespace is reserved, not every name starting with those characters."""
        validate_topic_not_reserved(f"{settings.kafka_tagged_events_topic}-archive", "Destination topic")


class TestExtractedConfigurationValidators:
    """Bounds shared by the singleton configuration and correlation pipelines live in one place."""

    @pytest.mark.parametrize("value", [50, 65, 100, None])
    def test_threshold_pct_accepts_valid_values(self, value):
        assert validate_threshold_pct(value) == value

    @pytest.mark.parametrize("value", [45, 63, 105])
    def test_threshold_pct_rejects_invalid_values(self, value):
        with pytest.raises(ValueError, match="multiple of 5"):
            validate_threshold_pct(value)

    @pytest.mark.parametrize("value", [30, 3600, 10800, None])
    def test_window_accepts_valid_values(self, value):
        assert validate_correlation_window_seconds(value) == value

    @pytest.mark.parametrize("value", [29, 10801])
    def test_window_rejects_out_of_range(self, value):
        with pytest.raises(ValueError, match="between 30 and 10800"):
            validate_correlation_window_seconds(value)

    def test_singleton_update_request_uses_the_shared_bounds(self):
        """The singleton endpoint must not drift from the extracted helpers."""
        with pytest.raises(ValueError, match="multiple of 5"):
            AttackChainConfigurationUpdateRequest(threshold_pct=63)
        with pytest.raises(ValueError, match="between 30 and 10800"):
            AttackChainConfigurationUpdateRequest(correlation_window_seconds=10801)
        assert AttackChainConfigurationUpdateRequest(threshold_pct=65).threshold_pct == 65


class TestPipelineCreateRequestTopicValidation:
    """Tests for PipelineCreateRequest topic validation."""

    def test_valid_topics_pass(self):
        """Valid source and destination topics should pass."""
        request = PipelineCreateRequest(
            name="Test Pipeline",
            source_topics=["input-logs"],
            destination_topic="output-logs",
        )
        assert request.source_topics == ["input-logs"]
        assert request.destination_topic == "output-logs"

    def test_source_topic_equals_destination_fails(self):
        """Source topic cannot equal destination topic."""
        with pytest.raises(ValueError, match="cannot be the same as a source topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["same-topic"],
                destination_topic="same-topic",
            )

    def test_destination_in_multiple_sources_fails(self):
        """Destination topic cannot be in source topics list."""
        with pytest.raises(ValueError, match="cannot be the same as a source topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["input-1", "input-2", "output-logs"],
                destination_topic="output-logs",
            )

    def test_destination_in_sources_error_message_has_no_value_error_prefix(self):
        """Validation message should not include automatic 'Value error,' prefix."""
        with pytest.raises(ValueError) as exc_info:
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["same-topic"],
                destination_topic="same-topic",
            )

        assert (
            exc_info.value.errors()[0]["msg"] == "Destination topic 'same-topic' cannot be the same as a source topic. "
            "This would create an infinite loop."
        )

    def test_reserved_source_topic_fails(self):
        """Reserved topics cannot be used as source."""
        reserved_topic = next(iter(get_reserved_topics()))
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=[reserved_topic],
                destination_topic="output-logs",
            )

    def test_generated_per_pipeline_destination_topic_fails(self):
        """A pipeline may not claim a topic the platform generates per pipeline."""
        with pytest.raises(ValueError, match="reserved prefix"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["input-logs"],
                destination_topic=f"{settings.kafka_tagged_events_topic}.{uuid4()}",
            )

    def test_reserved_destination_topic_fails(self):
        """Reserved topics cannot be used as destination."""
        reserved_topic = next(iter(get_reserved_topics()))
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["input-logs"],
                destination_topic=reserved_topic,
            )


class TestPipelineUpdateRequestTopicValidation:
    """Tests for PipelineUpdateRequest topic validation."""

    def test_valid_update_passes(self):
        """Valid topic updates should pass."""
        request = PipelineUpdateRequest(
            source_topics=["new-input"],
            destination_topic="new-output",
        )
        assert request.source_topics == ["new-input"]
        assert request.destination_topic == "new-output"

    def test_none_topics_pass(self):
        """None topics (no update) should pass."""
        request = PipelineUpdateRequest(name="New Name")
        assert request.source_topics is None
        assert request.destination_topic is None
        assert request.resources is None

    def test_partial_resources_accepted(self):
        """PATCH may send a subset of Flink resource fields."""
        request = PipelineUpdateRequest(resources=FlinkResourceConfig(taskmanager_cpu=2.0, parallelism=4))
        assert request.resources is not None
        assert request.resources.taskmanager_cpu == 2.0
        assert request.resources.parallelism == 4
        assert request.resources.window_size_sec is None

    def test_source_equals_destination_in_update_fails(self):
        """Source cannot equal destination in update."""
        with pytest.raises(ValueError, match="cannot be the same as a source topic"):
            PipelineUpdateRequest(
                source_topics=["same-topic"],
                destination_topic="same-topic",
            )

    def test_reserved_source_in_update_fails(self):
        """Reserved topics cannot be used as source in update."""
        reserved = get_reserved_topics()
        reserved_topic = next(iter(reserved))  # Get any reserved topic
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineUpdateRequest(source_topics=[reserved_topic])

    def test_reserved_destination_in_update_fails(self):
        """Reserved topics cannot be used as destination in update."""
        reserved_topic = next(iter(get_reserved_topics()))
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineUpdateRequest(destination_topic=reserved_topic)
