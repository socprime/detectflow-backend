from apps.services.mitre_attack import MitreAttack


def _stub_mitre() -> MitreAttack:
    svc = object.__new__(MitreAttack)
    svc.techniques_by_id = {
        "T1053": {"code": "T1053", "name": "Scheduled Task/Job", "description": None},
        "T1053.005": {"code": "T1053.005", "name": "Scheduled Task", "description": None},
    }
    svc.groups_by_id = {
        "G0024": {"code": "G0024", "name": "APT28", "description": None},
        "G0056": {"code": "G0056", "name": "APT32", "description": None},
    }
    return svc


def test_normalize_technique_external_id_from_sigma_tag() -> None:
    assert MitreAttack.normalize_technique_external_id("attack.T1059") == "T1059"
    assert MitreAttack.normalize_technique_external_id("attack.t1560.001") == "T1560.001"


def test_normalize_technique_external_id_from_mitre_id() -> None:
    assert MitreAttack.normalize_technique_external_id("t1059") == "T1059"
    assert MitreAttack.normalize_technique_external_id("T1059.001") == "T1059.001"


def test_normalize_technique_external_id_invalid() -> None:
    assert MitreAttack.normalize_technique_external_id("attack.credential_access") is None
    assert MitreAttack.normalize_technique_external_id("") is None


def test_normalize_group_external_id() -> None:
    assert MitreAttack.normalize_group_external_id("g0024") == "G0024"
    assert MitreAttack.normalize_group_external_id("G0056") == "G0056"
    assert MitreAttack.normalize_group_external_id("APT28") is None
    assert MitreAttack.normalize_group_external_id("") is None


def test_resolve_technique_ref_uses_name_or_falls_back_to_id() -> None:
    svc = _stub_mitre()
    hit = svc.resolve_technique_ref("t1053.005")
    assert hit.id == "T1053.005"
    assert hit.name == "Scheduled Task"
    miss = svc.resolve_technique_ref("T9999")
    assert miss.id == "T9999"
    assert miss.name == "T9999"


def test_resolve_group_ref_uses_name_or_falls_back_to_raw() -> None:
    svc = _stub_mitre()
    hit = svc.resolve_group_ref("g0024")
    assert hit.id == "G0024"
    assert hit.name == "APT28"
    legacy = svc.resolve_group_ref("Actor A")
    assert legacy.id == "Actor A"
    assert legacy.name == "Actor A"
