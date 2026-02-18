"""Unit tests for apps/core/schemas.py validators."""

from uuid import UUID

import pytest

from apps.core.schemas import (
    PipelineCreateRequest,
    PipelineUpdateRequest,
    get_reserved_topics,
    validate_topic_not_reserved,
    validate_uuid4,
    validate_yaml,
)


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

    def test_reserved_source_topic_fails(self):
        """Reserved topics cannot be used as source."""
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["sigma-rules"],
                destination_topic="output-logs",
            )

    def test_reserved_destination_topic_fails(self):
        """Reserved topics cannot be used as destination."""
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="Test Pipeline",
                source_topics=["input-logs"],
                destination_topic="etl-activity",
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
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineUpdateRequest(destination_topic="sigma-rules")
