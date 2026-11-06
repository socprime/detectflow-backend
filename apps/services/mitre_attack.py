import json
import re
from pathlib import Path
from typing import NamedTuple

import requests
from stix2 import Filter, MemoryStore

from apps.core.logger import get_logger
from apps.core.settings import settings

logger = get_logger(__name__)

# MITRE ATT&CK technique / sub-technique id on external_references (e.g. T1059, T1059.001)
_MITRE_TECHNIQUE_ID = re.compile(r"^T\d{4}(?:\.\d{3})?$")
# MITRE ATT&CK group / intrusion-set id (e.g. G0024)
_MITRE_GROUP_ID = re.compile(r"^G\d{4}$")
# Sigma-style tag (e.g. attack.t1216, attack.T1560.001)
_SIGMA_ATTACK_TECHNIQUE_TAG = re.compile(r"^attack\.t(\d+(?:\.\d+)?)$", re.IGNORECASE)


class TechniqueInfo(NamedTuple):
    """MITRE ATT&CK technique row resolved from the enterprise bundle."""

    code: str
    name: str
    description: str | None


class GroupInfo(NamedTuple):
    """MITRE ATT&CK group (intrusion-set) row resolved from the enterprise bundle."""

    code: str
    name: str
    description: str | None


class MitreRef(NamedTuple):
    """HTTP-facing MITRE id + display name; ``name`` falls back to ``id`` when unresolved."""

    id: str
    name: str


class MitreAttack:
    """Load MITRE ATT&CK enterprise STIX once and serve O(1) technique / group lookups."""

    _src: MemoryStore
    techniques_by_id: dict[str, dict[str, str | None]]
    groups_by_id: dict[str, dict[str, str | None]]

    def __init__(self) -> None:
        # Loading is best-effort: this singleton is instantiated at import time, so a
        # failure here (missing bundle in an air-gapped deploy, unreachable MITRE/CTI)
        # must never crash app startup. Degrade to empty indexes → lookups return None,
        # which callers already handle (HTTP falls back to the raw id as the name).
        try:
            self._src = self._load_enterprise_attack()
            self.techniques_by_id = self._build_technique_index()
            self.groups_by_id = self._build_group_index()
        except Exception as exc:
            logger.error("Failed to load MITRE ATT&CK bundle; technique enrichment disabled: %s", exc)
            self._src = MemoryStore(stix_data=[])
            self.techniques_by_id = {}
            self.groups_by_id = {}

    def _load_enterprise_attack(self) -> MemoryStore:
        """Load the enterprise ATT&CK bundle from the image-baked file, else from MITRE/CTI.

        The Docker image downloads the STIX bundle to ``settings.mitre_attack_data_path``
        at build time, so container startup reads it from disk with no network call. When
        the file is absent (e.g. local dev), fall back to fetching from MITRE/CTI.
        """
        path = Path(settings.mitre_attack_data_path)
        if path.is_file():
            logger.info("Loading MITRE ATT&CK bundle from %s", path)
            with path.open(encoding="utf-8") as fh:
                stix_json = json.load(fh)
            return MemoryStore(stix_data=stix_json["objects"])
        logger.warning("MITRE ATT&CK bundle not found at %s; fetching from MITRE/CTI over the network", path)
        return self._get_data_from_branch("enterprise-attack")

    def _get_data_from_branch(self, domain: str, branch: str | None = None) -> MemoryStore:
        """Fetch ATT&CK STIX from MITRE/CTI (``enterprise-attack`` | ``mobile-attack`` | ``ics-attack``)."""
        branch = branch or settings.mitre_attack_cti_branch
        stix_json = requests.get(
            url=f"https://raw.githubusercontent.com/mitre/cti/{branch}/{domain}/{domain}.json",
            timeout=120,
            verify=False,
        ).json()
        return MemoryStore(stix_data=stix_json["objects"])

    def _build_technique_index(self) -> dict[str, dict[str, str | None]]:
        """Single scan of all attack-patterns: ``technique_id`` → ``{code, name, description}``."""
        out: dict[str, dict[str, str | None]] = {}
        filt = [Filter("type", "=", "attack-pattern")]
        for pat in self._src.query(filt):
            inner = pat._inner if hasattr(pat, "_inner") else pat
            for ref in inner.get("external_references") or []:
                eid = ref.get("external_id") if isinstance(ref, dict) else getattr(ref, "external_id", None)
                if not eid or not isinstance(eid, str):
                    continue
                eid = eid.strip()
                if not _MITRE_TECHNIQUE_ID.match(eid):
                    continue
                out[eid.upper()] = {
                    "code": eid.upper(),
                    "name": inner.get("name") or "",
                    "description": inner.get("description"),
                }
        return out

    def _build_group_index(self) -> dict[str, dict[str, str | None]]:
        """Single scan of intrusion-sets: ``group_id`` → ``{code, name, description}``."""
        out: dict[str, dict[str, str | None]] = {}
        filt = [Filter("type", "=", "intrusion-set")]
        for group in self._src.query(filt):
            inner = group._inner if hasattr(group, "_inner") else group
            for ref in inner.get("external_references") or []:
                eid = ref.get("external_id") if isinstance(ref, dict) else getattr(ref, "external_id", None)
                if not eid or not isinstance(eid, str):
                    continue
                eid = eid.strip()
                if not _MITRE_GROUP_ID.match(eid.upper()):
                    continue
                out[eid.upper()] = {
                    "code": eid.upper(),
                    "name": inner.get("name") or "",
                    "description": inner.get("description"),
                }
        return out

    @staticmethod
    def normalize_technique_external_id(raw: str) -> str | None:
        """Map Sigma tag or MITRE id to canonical ``external_id`` (e.g. ``attack.t1059`` → ``T1059``)."""
        text = str(raw).strip()
        if not text:
            return None
        upper = text.upper()
        if _MITRE_TECHNIQUE_ID.match(upper):
            return upper
        match = _SIGMA_ATTACK_TECHNIQUE_TAG.match(text)
        if match:
            return f"T{match.group(1).upper()}"
        return None

    def get_technique_by_id(self, technique_mitre_id: str) -> TechniqueInfo | None:
        """Resolve a technique by MITRE ``external_id`` or Sigma ``attack.t####`` tag."""
        key = self.normalize_technique_external_id(technique_mitre_id)
        if key is None:
            return None
        row = self.techniques_by_id.get(key)
        if row is None:
            return None
        return TechniqueInfo(
            code=str(row["code"]),
            name=str(row["name"]) if row["name"] is not None else key,
            description=row["description"],
        )

    @staticmethod
    def normalize_group_external_id(raw: str) -> str | None:
        """Map a MITRE group id to canonical ``external_id`` (e.g. ``g0024`` → ``G0024``)."""
        text = str(raw).strip()
        if not text:
            return None
        upper = text.upper()
        if _MITRE_GROUP_ID.match(upper):
            return upper
        return None

    def get_group_by_id(self, group_mitre_id: str) -> GroupInfo | None:
        """Resolve a group by MITRE ``external_id`` (``G####``)."""
        key = self.normalize_group_external_id(group_mitre_id)
        if key is None:
            return None
        row = self.groups_by_id.get(key)
        if row is None:
            return None
        return GroupInfo(
            code=str(row["code"]),
            name=str(row["name"]) if row["name"] is not None else key,
            description=row["description"],
        )

    def resolve_technique_ref(self, raw: str) -> MitreRef:
        """HTTP ``{id, name}`` for a technique id; ``name`` falls back to the raw value."""
        text = str(raw).strip()
        info = self.get_technique_by_id(text)
        if info is None:
            return MitreRef(id=text, name=text)
        return MitreRef(id=info.code, name=info.name or info.code)

    def resolve_group_ref(self, raw: str) -> MitreRef:
        """HTTP ``{id, name}`` for a group id; ``name`` falls back to the raw value (legacy names too)."""
        text = str(raw).strip()
        info = self.get_group_by_id(text)
        if info is None:
            return MitreRef(id=text, name=text)
        return MitreRef(id=info.code, name=info.name or info.code)


mitre_attack = MitreAttack()
