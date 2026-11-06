"""Unit tests for Kafka parser/log source payload contents."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
import yaml

from apps.core.exceptions import BadRequestError
from apps.core.schemas import LogSourceCreateRequest, LogSourceUpdateRequest
from apps.managers.log_sources import LogSourcesManager
from apps.modules.utils import (
    HOSTNAME_FIELDS_KEY,
    TIMESTAMP_FIELDS_KEY,
    normalize_field_mapping_yaml,
    parse_mapping_metadata,
    quote_yaml_unsafe_field_scalars,
)


def _build_kafka_service():
    """Construct ``KafkaParserPublisher`` without touching real Kafka."""
    with patch("apps.modules.kafka.parsers.Producer", return_value=MagicMock()):
        from apps.modules.kafka.parsers import KafkaParserPublisher

        return KafkaParserPublisher()


def test_prepare_message_includes_log_source_identity_fields():
    service = _build_kafka_service()

    _, value_bytes = service._prepare_message(
        pipeline_id="pipeline-1",
        log_source_id="log-source-1",
        log_source_name="Windows Events",
        parsing_config={"steps": []},
        mapping=(
            "EventID: event.code\n"
            f"{HOSTNAME_FIELDS_KEY}:\n"
            "  - host.name\n"
            "  - winlog.computer_name\n"
            f"{TIMESTAMP_FIELDS_KEY}:\n"
            '  - "@timestamp"\n'
        ),
    )

    payload = json.loads(value_bytes.decode("utf-8"))
    assert HOSTNAME_FIELDS_KEY not in payload["mapping"]
    assert TIMESTAMP_FIELDS_KEY not in payload["mapping"]
    assert payload["hostname_fields"] == ["host.name", "winlog.computer_name"]
    assert payload["timestamp_fields"] == ["@timestamp"]


def test_parse_mapping_metadata_reads_reserved_yaml_keys():
    metadata = parse_mapping_metadata(
        f"{HOSTNAME_FIELDS_KEY}: host.name\n{TIMESTAMP_FIELDS_KEY}:\n  - event.created\nEventID: event.code\n"
    )

    assert metadata.hostname_fields == ["host.name"]
    assert metadata.timestamp_fields == ["event.created"]


def test_quote_yaml_unsafe_field_scalars_quotes_at_timestamp():
    raw = (
        "EventID: event.code\n"
        f"{TIMESTAMP_FIELDS_KEY}:\n"
        "  - @timestamp\n"
        "  - winlog.event_data.UtcTime\n"
        "Timestamp: @timestamp\n"
    )
    fixed = quote_yaml_unsafe_field_scalars(raw)

    assert '  - "@timestamp"' in fixed
    assert 'Timestamp: "@timestamp"' in fixed
    assert "winlog.event_data.UtcTime" in fixed
    # Already-quoted values must not be double-wrapped.
    assert quote_yaml_unsafe_field_scalars('  - "@timestamp"\n') == '  - "@timestamp"\n'


def test_quote_yaml_unsafe_field_scalars_covers_keys_and_trailing_comments():
    """Bare ``@`` breaks YAML on either side of a mapping entry, and a trailing comment must survive."""
    assert yaml.safe_load(quote_yaml_unsafe_field_scalars("@timestamp: event.created\n")) == {
        "@timestamp": "event.created"
    }
    assert yaml.safe_load(quote_yaml_unsafe_field_scalars("Timestamp: @timestamp  # elastic\n")) == {
        "Timestamp": "@timestamp"
    }
    assert yaml.safe_load(quote_yaml_unsafe_field_scalars(f"{TIMESTAMP_FIELDS_KEY}:\n  - @timestamp # elastic\n")) == {
        TIMESTAMP_FIELDS_KEY: ["@timestamp"]
    }


def test_quote_yaml_unsafe_field_scalars_is_idempotent_and_leaves_prose_alone():
    """Callers re-apply the repair to stored mappings, so it must be stable and must not touch prose."""
    raw = f'EventID: event.code\nTimestamp: @timestamp\n{TIMESTAMP_FIELDS_KEY}:\n  - "@timestamp"\n'
    once = quote_yaml_unsafe_field_scalars(raw)

    assert quote_yaml_unsafe_field_scalars(once) == once
    assert quote_yaml_unsafe_field_scalars("description: mentions @timestamp field here\n") == (
        "description: mentions @timestamp field here\n"
    )
    assert quote_yaml_unsafe_field_scalars("note: |\n  bare @timestamp inside literal block\n") == (
        "note: |\n  bare @timestamp inside literal block\n"
    )


@pytest.mark.parametrize(
    "line",
    [
        "EventID: event.code  # see @timestamp\n",
        "EventID: event.code # alias: @timestamp\n",
        "EventID: event.code #@timestamp\n",
        "- event.code # or @timestamp\n",
        "# use @timestamp for elastic\n",
        "# @timestamp: event.created\n",
        "  # maps to @timestamp\n",
    ],
)
def test_quote_yaml_unsafe_field_scalars_never_touches_comments(line):
    """A ``@`` mentioned in a comment is documentation, not a field name — quoting it would be wrong."""
    assert quote_yaml_unsafe_field_scalars(line) == line


@pytest.mark.parametrize(
    "line",
    [
        "owner: user@example.com\n",
        "- user@example.com\n",
        "user@example.com: something\n",
        "Field: foo@bar\n",
        "Field: winlog.event@data.Utc\n",
        "Field: foo@\n",
        "Field: @\n",
        "description: send to a@b then wait\n",
    ],
)
def test_quote_yaml_unsafe_field_scalars_ignores_at_inside_words(line):
    """Only a leading ``@`` makes a plain scalar invalid; ``user@example.com`` is already valid YAML."""
    assert quote_yaml_unsafe_field_scalars(line) == line


@pytest.mark.parametrize(
    "line",
    [
        'Timestamp: "@timestamp"\n',
        "Timestamp: '@timestamp'\n",
        '  - "@timestamp"\n',
        "  - '@timestamp'\n",
        '"@timestamp": event.created\n',
        "'@timestamp': event.created\n",
        '"@timestamp": "@other"\n',
        'Timestamp: "@timestamp"   \n',
        '  - "@timestamp"  # elastic\n',
    ],
)
def test_quote_yaml_unsafe_field_scalars_never_double_quotes(line):
    """Re-quoting an already-quoted scalar would turn the field name into literal quote characters."""
    assert quote_yaml_unsafe_field_scalars(line) == line


def test_parse_mapping_metadata_accepts_unquoted_at_timestamp():
    metadata = parse_mapping_metadata(f"{TIMESTAMP_FIELDS_KEY}:\n  - @timestamp\n  - winlog.event_data.UtcTime\n")

    assert metadata.timestamp_fields == ["@timestamp", "winlog.event_data.UtcTime"]


def test_normalize_field_mapping_yaml_quotes_and_accepts_valid_mapping():
    raw = f"EventID: event.code\n{TIMESTAMP_FIELDS_KEY}:\n  - @timestamp\n  - winlog.event_data.UtcTime\n"
    normalized = normalize_field_mapping_yaml(raw)

    assert normalized is not None
    assert '  - "@timestamp"' in normalized
    assert "EventID: event.code" in normalized


def test_normalize_field_mapping_yaml_rejects_invalid_shape():
    with pytest.raises(ValueError, match="dictionary"):
        normalize_field_mapping_yaml("- just a list\n")
    with pytest.raises(ValueError, match="list items must be strings"):
        normalize_field_mapping_yaml("EventID:\n  - 1\n")
    with pytest.raises(ValueError, match="Invalid mapping YAML"):
        normalize_field_mapping_yaml("EventID: [unterminated\n")


@pytest.mark.asyncio
async def test_log_source_create_autoquotes_mapping():
    created_log_source = MagicMock()
    created_log_source.id = UUID("550e8400-e29b-41d4-a716-446655440010")
    created_log_source.name = "Windows"

    log_source_repo = AsyncMock()
    log_source_repo.create.return_value = created_log_source

    request = LogSourceCreateRequest(
        name="Windows",
        mapping=f"EventID: event.code\n{TIMESTAMP_FIELDS_KEY}:\n  - @timestamp\n",
    )
    user = MagicMock()

    with patch("apps.managers.log_sources.activity_producer") as activity_producer:
        activity_producer.log_action = AsyncMock()
        manager = LogSourcesManager(AsyncMock())
        manager.log_source_repo = log_source_repo
        await manager.create(request, user)

    stored_mapping = log_source_repo.create.await_args.kwargs["mapping"]
    assert '  - "@timestamp"' in stored_mapping


@pytest.mark.asyncio
async def test_log_source_update_rejects_invalid_mapping_with_400():
    existing = MagicMock()
    existing.parsing_script = None
    existing.mapping = None

    log_source_repo = AsyncMock()
    log_source_repo.get_by_id.return_value = existing

    manager = LogSourcesManager(AsyncMock())
    manager.log_source_repo = log_source_repo

    with pytest.raises(BadRequestError, match="dictionary") as exc_info:
        await manager.update(
            UUID("550e8400-e29b-41d4-a716-446655440010"),
            LogSourceUpdateRequest(mapping="- not a mapping dict\n"),
            MagicMock(),
        )

    assert exc_info.value.status_code == 400


def test_parser_field_mapping_ignores_reserved_yaml_keys():
    from apps.managers.parser import ParserManager

    field_mapping = ParserManager._get_field_mapping_from_str(
        f'EventID: event.code\n{HOSTNAME_FIELDS_KEY}:\n  - host.name\n{TIMESTAMP_FIELDS_KEY}:\n  - "@timestamp"\n'
    )

    assert field_mapping == {"EventID": ["event.code"]}
