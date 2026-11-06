"""Unit tests for KafkaActiveThreatsSyncService payload contents."""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from apps.core.models import ActiveThreat, HighOrderSigma
from apps.services.mitre_attack import MitreAttack


def _build_kafka_service():
    with patch("apps.modules.kafka.active_threats.Producer", return_value=MagicMock()):
        from apps.modules.kafka.active_threats import KafkaActiveThreatsSyncService

        return KafkaActiveThreatsSyncService()


def test_build_active_threat_payload_includes_expected_fields():
    service = _build_kafka_service()
    threat = MagicMock(spec=ActiveThreat)
    threat.tdm_id = "6ced3bac-8955-5d7a-8b8f-41bd3e1e0708"
    threat.name = "Cisco Infrastructure Reconnaissance + Credential Access Pattern"
    threat.threat_types = ["APT", "MALWARE"]
    threat.threat_actors = ["Actor A", "Actor B"]
    threat.severity = "HIGH"
    threat.industries = ["Government", "Telecommunications"]
    threat.geographies = ["Global"]
    threat.summary = "summary"
    threat.investigation = "investigation"
    threat.mitigation = "mitigation"
    threat.response = "response"
    threat.is_blocked = False
    threat.tdm_created = datetime(2026, 4, 22, tzinfo=UTC)
    threat.tdm_updated = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)
    threat.updated = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)

    payload = service._build_active_threat_payload(threat)

    assert payload["id"] == threat.tdm_id
    assert payload["threat_type"] == "APT,MALWARE"
    assert payload["threat_actors"] == [
        {"id": "Actor A", "name": "Actor A"},
        {"id": "Actor B", "name": "Actor B"},
    ]
    assert payload["release_date"] == "2026-04-22"
    assert payload["updated_at"] == "2026-04-22T10:00:00Z"
    assert payload["enabled"] is True


def test_build_active_threat_payload_carries_is_blocked_inverted_under_the_frozen_key():
    """D8/D8a: the wire key stays ``enabled``; only the source of its value moved to ``is_blocked``.

    A blocked threat must reach the matcher as ``enabled: false`` — its two ``at.enabled`` gates are
    the enforcement point of the global conjunct — and no redundant ``is_blocked`` key may appear.
    """
    service = _build_kafka_service()
    threat = MagicMock(spec=ActiveThreat)
    threat.tdm_id = "id-1"
    threat.name = "n"
    threat.threat_types = []
    threat.severity = None
    threat.industries = None
    threat.geographies = None
    threat.threat_actors = None
    threat.summary = None
    threat.investigation = None
    threat.mitigation = None
    threat.response = None
    threat.tdm_created = None
    threat.tdm_updated = None
    threat.updated = None

    threat.is_blocked = True
    blocked_payload = service._build_active_threat_payload(threat)
    assert blocked_payload["enabled"] is not threat.is_blocked

    threat.is_blocked = False
    unblocked_payload = service._build_active_threat_payload(threat)
    assert unblocked_payload["enabled"] is not threat.is_blocked

    assert blocked_payload["enabled"] is False
    assert unblocked_payload["enabled"] is True
    assert "is_blocked" not in blocked_payload
    assert set(blocked_payload) == set(unblocked_payload)


def test_build_active_threat_payload_threat_actors_defaults_to_empty_list():
    service = _build_kafka_service()
    threat = MagicMock(spec=ActiveThreat)
    threat.tdm_id = "id-1"
    threat.name = "n"
    threat.threat_types = []
    threat.severity = None
    threat.industries = None
    threat.geographies = None
    threat.threat_actors = None
    threat.summary = None
    threat.investigation = None
    threat.mitigation = None
    threat.response = None
    threat.is_blocked = True
    threat.tdm_created = None
    threat.tdm_updated = None
    threat.updated = None

    payload = service._build_active_threat_payload(threat)

    assert payload["threat_actors"] == []


def test_build_active_threat_payload_enriches_threat_actors_from_attack_bundle():
    """Correlation-job inlines this array into chains-raw; agent-job ChainRaw.ThreatActor wants objects."""
    stub = object.__new__(MitreAttack)
    stub.groups_by_id = {
        "G0024": {"code": "G0024", "name": "APT28", "description": None},
    }
    stub.techniques_by_id = {}

    service = _build_kafka_service()
    threat = MagicMock(spec=ActiveThreat)
    threat.tdm_id = "id-1"
    threat.name = "n"
    threat.threat_types = []
    threat.threat_actors = ["G0024", "Actor A"]
    threat.severity = None
    threat.industries = None
    threat.geographies = None
    threat.summary = None
    threat.investigation = None
    threat.mitigation = None
    threat.response = None
    threat.is_blocked = False
    threat.tdm_created = None
    threat.tdm_updated = None
    threat.updated = None

    with patch("apps.modules.kafka.active_threats.mitre_attack", stub):
        payload = service._build_active_threat_payload(threat)

    assert payload["threat_actors"] == [
        {"id": "G0024", "name": "APT28"},
        {"id": "Actor A", "name": "Actor A"},
    ]


def test_build_hos_payload_extracts_fields_from_yaml():
    service = _build_kafka_service()
    hos = MagicMock(spec=HighOrderSigma)
    hos.tdm_id = "721cbc8d-7be6-5db6-b8d5-5565920fd977"
    hos.name = "Cisco Infrastructure Reconnaissance + Credential Access Pattern"
    hos.body = (
        "title: Cisco Infrastructure Reconnaissance + Credential Access Pattern\n"
        "description: Detects clustered activity\n"
        "status: stable\n"
        "author: e2e-harness\n"
        "references:\n"
        "  - https://example.invalid/real-data-e2e\n"
        "tags:\n"
        "  - attack.T1059\n"
        "level: high\n"
    )
    hos.tdm_updated = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)
    hos.updated = datetime(2026, 4, 22, 10, 0, tzinfo=UTC)

    payload = service._build_hos_payload(hos, ["6ced3bac-8955-5d7a-8b8f-41bd3e1e0708"])

    assert payload["id"] == hos.tdm_id
    assert payload["active_threat_id"] == "6ced3bac-8955-5d7a-8b8f-41bd3e1e0708"
    assert payload["title"] == hos.name
    assert payload["level"] == "HIGH"
    assert payload["status_lifecycle"] == "stable"
    assert payload["references"] == ["https://example.invalid/real-data-e2e"]
    assert payload["tags"] == ["attack.T1059"]
    assert payload["updated_at"] == "2026-04-22T10:00:00Z"


def test_prepare_active_threat_logical_delete_message():
    service = _build_kafka_service()

    key_bytes, value_bytes = service._prepare_active_threat_logical_delete_message("threat-1")

    assert key_bytes == b"threat-1"
    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload == {"id": "threat-1", "deleted": True}


def test_prepare_hos_logical_delete_and_tombstone_messages():
    service = _build_kafka_service()

    key_bytes, value_bytes = service._prepare_hos_logical_delete_message("hos-1")
    assert key_bytes == b"hos-1"
    payload = json.loads(value_bytes.decode("utf-8"))
    assert payload == {"id": "hos-1", "deleted": True}

    tombstone_key, tombstone_value = service._prepare_tombstone_message("hos-1")
    assert tombstone_key == b"hos-1"
    assert tombstone_value is None
