from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from apps.core.converters import ActiveThreatConverter
from apps.integrations.providers.socprime_tdm import TdmActiveThreat
from apps.managers.active_threats import ActiveThreatsSyncManager
from apps.services.mitre_attack import MitreAttack, MitreRef


def _stub_mitre() -> MitreAttack:
    svc = object.__new__(MitreAttack)
    svc.techniques_by_id = {
        "T1053.005": {"code": "T1053.005", "name": "Scheduled Task", "description": None},
    }
    svc.groups_by_id = {
        "G0024": {"code": "G0024", "name": "APT28", "description": None},
    }
    return svc


def _threat(**overrides: object) -> SimpleNamespace:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    base = {
        "tdm_id": "at-1",
        "name": "Threat",
        "is_blocked": False,
        "is_hidden": False,
        "severity": "high",
        "industries": None,
        "geographies": None,
        "threat_types": None,
        "threat_actors": ["G0024", "Actor A"],
        "techniques": ["T1053.005", "T9999"],
        "source": None,
        "summary": None,
        "investigation": None,
        "response": None,
        "mitigation": None,
        "attack_flow": None,
        "tdm_created": now,
        "tdm_updated": now,
        "created": now,
        "updated": now,
        "high_order_sigmas": [],
        "linked_rules": [],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_list_item_enriches_threat_actors_and_techniques() -> None:
    stub = _stub_mitre()
    with patch("apps.core.converters.mitre_attack", stub):
        row = ActiveThreatConverter.to_list_item(_threat())

    assert [item.model_dump() for item in row.threat_actors or []] == [
        {"id": "G0024", "name": "APT28"},
        {"id": "Actor A", "name": "Actor A"},
    ]
    assert [item.model_dump() for item in row.techniques or []] == [
        {"id": "T1053.005", "name": "Scheduled Task"},
        {"id": "T9999", "name": "T9999"},
    ]


def test_detail_enriches_threat_actors_and_techniques() -> None:
    stub = _stub_mitre()
    with patch("apps.core.converters.mitre_attack", stub):
        row = ActiveThreatConverter.to_detail(_threat())

    assert row.threat_actors is not None
    assert row.techniques is not None
    assert row.threat_actors[0].name == "APT28"
    assert row.techniques[1].id == "T9999"
    assert row.techniques[1].name == "T9999"


def test_to_row_persists_technique_ids_from_tdm() -> None:
    threat = TdmActiveThreat(
        id="AnJ1s58BVkdTJeU0KIe0",
        name="UAC-0247",
        created=datetime(2026, 6, 30, tzinfo=UTC),
        updated=datetime(2026, 9, 3, tzinfo=UTC),
        threat_actors=["G0024", "G0056"],
        techniques=["T1053", "T1053.005"],
    )
    row = ActiveThreatsSyncManager._to_row(threat)
    assert row["threat_actors"] == ["G0024", "G0056"]
    assert row["techniques"] == ["T1053", "T1053.005"]


def test_resolve_ref_namedtuple_matches_http_shape() -> None:
    assert MitreRef(id="T1053", name="Scheduled Task/Job")._asdict() == {
        "id": "T1053",
        "name": "Scheduled Task/Job",
    }
