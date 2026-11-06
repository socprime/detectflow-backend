from datetime import UTC, datetime
from unittest.mock import MagicMock

from apps.modules.postgre.high_order_sigma import HighOrderSigmaDAO


def _hos_row(
    *,
    tdm_id: str = "hos-1",
    name: str = "Example HOS",
    body: str = "title: Example",
    level: str | None = "high",
    tdm_created: datetime | None = None,
    tdm_updated: datetime | None = None,
) -> dict:
    return {
        "tdm_id": tdm_id,
        "name": name,
        "body": body,
        "level": level,
        "tdm_created": tdm_created or datetime(2026, 1, 1, tzinfo=UTC),
        "tdm_updated": tdm_updated or datetime(2026, 1, 2, tzinfo=UTC),
    }


def _existing_hos(row: dict) -> MagicMock:
    existing = MagicMock()
    existing.name = row["name"]
    existing.body = row["body"]
    existing.level = row["level"]
    existing.tdm_created = row["tdm_created"]
    existing.tdm_updated = row["tdm_updated"]
    return existing


def test_hos_sync_row_changed_detects_body_update():
    existing = _existing_hos(_hos_row(body="title: Old"))

    assert HighOrderSigmaDAO._hos_sync_row_changed(existing, _hos_row(body="title: New")) is True


def test_hos_sync_row_changed_detects_level_update():
    """Severity is a sync field: TDM can revise a level without touching the stored body."""
    existing = _existing_hos(_hos_row(level="medium"))

    assert HighOrderSigmaDAO._hos_sync_row_changed(existing, _hos_row(level="critical")) is True


def test_hos_sync_row_changed_ignores_identical_payload():
    row = _hos_row()

    assert HighOrderSigmaDAO._hos_sync_row_changed(_existing_hos(row), row) is False
