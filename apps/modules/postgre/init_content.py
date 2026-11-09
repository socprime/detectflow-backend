"""Bundled SOC Prime content: Active Threats, HoS and Sigma rules for a fresh install.

``detection_content/`` holds a snapshot of real TDM content (real ids). It is applied once per
snapshot version (``detection_content/VERSION``), with or without the socprime_tdm integration.

Active Threats, their HoS and the Sigma rules those HoS reference go through the regular
Active Threat sync with :class:`SnapshotProvider` in place of TDM, so they land exactly
where a TDM sync puts them (catalog, "SOC Prime Active Threats" system repo, Kafka).

An item is written only when it is missing locally or newer in the snapshot, and the
snapshot never hides or deletes anything, so data already synced from TDM is never rolled
back. Because the ids are real, the TDM sync keeps the content current once connected.

Usage from entrypoint.sh:
    python -m apps.modules.postgre.init_content
"""

import asyncio
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import AsyncSessionLocal, engine
from apps.integrations.providers.socprime_tdm import TdmActiveThreat, TdmRule
from apps.managers.active_threats import ActiveThreatsSyncManager
from apps.modules.postgre.active_threats import ActiveThreatDAO
from apps.modules.postgre.config import ConfigDAO
from apps.modules.utils import get_sigma_uuid_from_sigma_body

CONTENT_DIR = Path(__file__).resolve().parents[3] / "detection_content"
CONTENT_VERSION_CONFIG_KEY = "content_snapshot_version"
# pg_advisory_xact_lock key: replicas starting together apply the snapshot one at a time.
CONTENT_LOCK_KEY = 0x434F4E54454E54  # "CONTENT"


@dataclass
class ContentSnapshot:
    version: str
    active_threats: list[TdmActiveThreat]
    hos: list[TdmRule]
    rules: list[TdmRule]


def _load_dir[M: BaseModel](directory: Path, model: type[M]) -> list[M]:
    items = []
    for path in sorted(directory.glob("*.yml")):
        item = model.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
        # Naive timestamps are UTC; comparing them with timestamptz values would raise.
        naive = {
            field: getattr(item, field).replace(tzinfo=UTC)
            for field in ("created", "updated")
            if getattr(item, field).tzinfo is None
        }
        items.append(item.model_copy(update=naive))
    return items


def load_snapshot(content_dir: Path = CONTENT_DIR) -> ContentSnapshot | None:
    """Read ``content_dir``; ``None`` when there is no snapshot (no ``VERSION`` file)."""
    version_file = content_dir / "VERSION"
    if not version_file.is_file():
        return None
    return ContentSnapshot(
        version=version_file.read_text(encoding="utf-8").strip(),
        active_threats=_load_dir(content_dir / "active_threats", TdmActiveThreat),
        hos=_load_dir(content_dir / "hos", TdmRule),
        rules=_load_dir(content_dir / "rules", TdmRule),
    )


def missing_or_newer(
    threats: list[TdmActiveThreat], local_updated: dict[str, datetime | None]
) -> list[TdmActiveThreat]:
    """Snapshot Active Threats that are absent locally or newer than the local copy."""
    return [
        threat
        for threat in threats
        if threat.id not in local_updated
        or local_updated[threat.id] is None
        or threat.updated > local_updated[threat.id]
    ]


class SnapshotProvider:
    """A content snapshot behind the part of ``SocPrimeTdmProvider`` the Active Threat sync calls."""

    def __init__(self, active_threats: list[TdmActiveThreat], hos: list[TdmRule], rules: list[TdmRule]):
        self.active_threats = active_threats
        self.rules_by_siem_type = {"hos": hos, "sigma": rules}
        self.sigma_uuid_by_rule_id = {rule.id: get_sigma_uuid_from_sigma_body(rule.body) for rule in rules}

    async def get_active_threats(
        self, updated_after: datetime, updated_before: datetime | None = None
    ) -> list[TdmActiveThreat]:
        # Already filtered per item (missing_or_newer): a snapshot is older than the sync cursor.
        return self.active_threats

    async def check_active_threats_exist(self, ids: list[str]) -> set[str]:
        # A snapshot only adds: nothing is hidden or deleted because it is absent here.
        return set(ids)

    async def search_rules_by_case_ids(
        self, case_ids: list[str], siem_type: str = "sigma", updated_after: datetime | None = None
    ) -> list[TdmRule]:
        wanted = set(case_ids)
        return self._newer([rule for rule in self.rules_by_siem_type[siem_type] if rule.id in wanted], updated_after)

    async def search_rules_by_sigma_uuids(
        self, sigma_uuids: list[str], siem_type: str = "sigma", updated_after: datetime | None = None
    ) -> list[TdmRule]:
        wanted = set(sigma_uuids)
        rules = [rule for rule in self.rules_by_siem_type["sigma"] if self.sigma_uuid_by_rule_id[rule.id] in wanted]
        return self._newer(rules, updated_after)

    @staticmethod
    def _newer(rules: list[TdmRule], updated_after: datetime | None) -> list[TdmRule]:
        return [rule for rule in rules if updated_after is None or rule.updated > updated_after]


async def apply_content_snapshot(session: AsyncSession, content_dir: Path = CONTENT_DIR) -> bool:
    """Apply the snapshot unless this version was applied already. The caller commits."""
    snapshot = load_snapshot(content_dir)
    if snapshot is None:
        return False
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": CONTENT_LOCK_KEY})
    config_dao = ConfigDAO(session)
    applied = await config_dao.get_by_key(CONTENT_VERSION_CONFIG_KEY)
    if applied is not None and applied.value == snapshot.version:
        return False

    local_updated = await ActiveThreatDAO(session).get_tdm_updated_by_ids([t.id for t in snapshot.active_threats])
    provider = SnapshotProvider(missing_or_newer(snapshot.active_threats, local_updated), snapshot.hos, snapshot.rules)
    result = await ActiveThreatsSyncManager(session).sync_active_threats(provider=provider)
    await config_dao.upsert_by_key(CONTENT_VERSION_CONFIG_KEY, snapshot.version)
    print(
        f"Content snapshot {snapshot.version}: active_threats={result.upserted}, hos={result.hos_upserted}, "
        f"rules={result.referenced_rules_upserted}"
    )
    return True


async def _run() -> bool:
    try:
        async with AsyncSessionLocal() as session:
            applied = await apply_content_snapshot(session)
            await session.commit()
            return applied
    finally:
        await engine.dispose()


def main() -> int:
    print("Checking bundled content snapshot...")
    try:
        if not asyncio.run(_run()):
            print("Content snapshot already applied or absent, skipping")
    except Exception as e:
        # Content is optional: never keep the backend down for it. The next start retries.
        print(f"WARNING: content snapshot not applied: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
