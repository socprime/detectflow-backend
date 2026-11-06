"""Unit tests for KafkaRulesSyncService payload contents."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import UUID

from apps.core.models import Rule
from apps.integrations.providers.socprime_tdm import TdmRule


def _build_kafka_service():
    """Construct ``KafkaRulesSyncService`` without touching real Kafka."""
    with patch("apps.modules.kafka.rules.Producer", return_value=MagicMock()):
        from apps.modules.kafka.rules import KafkaRulesSyncService

        return KafkaRulesSyncService()


def _build_rule(*, sigma_uuid: UUID | None) -> MagicMock:
    rule = MagicMock(spec=Rule)
    rule.id = UUID("11111111-2222-3333-4444-555555555555")
    rule.name = "Test Rule"
    rule.body = (
        "title: Example\n"
        f"{'id: ' + str(sigma_uuid) if sigma_uuid else 'description: no id'}\n"
        "logsource:\n"
        "  product: windows\n"
        "level: high\n"
    )
    rule.sigma_uuid = sigma_uuid
    rule.created = datetime(2026, 1, 1, tzinfo=UTC)
    rule.updated = datetime(2026, 1, 2, tzinfo=UTC)
    return rule


def test_prepare_message_includes_sigma_uuid_for_rule_model():
    service = _build_kafka_service()
    sigma_uuid = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    rule = _build_rule(sigma_uuid=sigma_uuid)

    _, value_bytes = service._prepare_message("pipeline-1", rule)

    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload["sigma"]["sigma_uuid"] == str(sigma_uuid)


def test_prepare_message_sigma_uuid_none_when_rule_has_no_uuid():
    service = _build_kafka_service()
    rule = _build_rule(sigma_uuid=None)

    _, value_bytes = service._prepare_message("pipeline-1", rule)

    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload["sigma"]["sigma_uuid"] is None


def test_prepare_message_extracts_sigma_uuid_from_tdm_rule_body():
    service = _build_kafka_service()

    tdm_rule = TdmRule(
        id="tdm-1",
        name="TDM Rule",
        body=(
            "title: Example\n"
            "id: bbbbbbbb-cccc-dddd-eeee-ffffffffffff\n"
            "logsource:\n"
            "  product: windows\n"
            "level: medium\n"
            "tags:\n"
            "  - attack.t1059\n"
        ),
        level="medium",
        tags={"technique": [{"id": "T1059"}]},
        created=datetime(2026, 1, 1, tzinfo=UTC),
        updated=datetime(2026, 1, 2, tzinfo=UTC),
    )

    _, value_bytes = service._prepare_message("pipeline-1", tdm_rule)

    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload["sigma"]["sigma_uuid"] == "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
    assert payload["sigma"]["text"] == tdm_rule.body
    assert payload["sigma"]["level"] == "medium"
    assert payload["created"] == "2026-01-01T00:00:00Z"
    assert payload["updated"] == "2026-01-02T00:00:00Z"


def test_prepare_message_includes_created_and_updated_for_rule_model():
    service = _build_kafka_service()
    rule = _build_rule(sigma_uuid=None)

    _, value_bytes = service._prepare_message("pipeline-1", rule)

    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload["created"] == "2026-01-01T00:00:00Z"
    assert payload["updated"] == "2026-01-02T00:00:00Z"


def test_prepare_message_created_and_updated_null_when_missing():
    service = _build_kafka_service()
    rule = _build_rule(sigma_uuid=None)
    rule.created = None
    rule.updated = None

    _, value_bytes = service._prepare_message("pipeline-1", rule)

    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload["created"] is None
    assert payload["updated"] is None
