"""Unit tests for attack-chain DAO helpers."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from apps.modules.postgre.attack_chains import AttackChainsDAO


@pytest.mark.parametrize(
    ("seqs", "expected"),
    [
        ([], 0),
        ([{"seq": 1, "hit": True}, {"seq": 2, "hit": False}], 50),
        ([{"seq": 1, "hit": True}, {"seq": 2, "hit": True}, {"seq": 3, "hit": False}], 67),
        ([{"seq": 1, "hit": True}, {"seq": 2, "hit": False}, {"seq": 3, "hit": False}], 33),
        ([{"seq": 1, "hit": True}, {"seq": 2, "hit": False}, {"seq": 3, "hit": False}, {"seq": 4}], 25),
    ],
)
def test_calculate_match_percent(seqs: list[dict], expected: int) -> None:
    assert AttackChainsDAO._calculate_match_percent(seqs) == expected


@pytest.mark.parametrize("verdict", ["FALSE_POSITIVE", "INSUFFICIENT_CONTEXT", " false_positive "])
def test_resolve_initial_status_filters_llm_negative_verdicts(verdict: str) -> None:
    assert AttackChainsDAO._resolve_initial_status("new", verdict) == "filtered-out"


@pytest.mark.parametrize(
    ("status", "verdict", "expected"),
    [
        (None, None, "new"),
        (None, "TRUE", "new"),
        ("investigating", "SUSPICIOUS", "investigating"),
    ],
)
def test_resolve_initial_status_defaults_or_uses_message_status(
    status: str | None,
    verdict: str | None,
    expected: str,
) -> None:
    assert AttackChainsDAO._resolve_initial_status(status, verdict) == expected


@pytest.mark.asyncio
async def test_resolve_hos_rule_fk_accepts_tdm_short_id() -> None:
    expected = UUID("770e8400-e29b-41d4-a716-446655440000")
    result = MagicMock()
    result.scalar_one_or_none.return_value = expected
    session = AsyncMock()
    session.execute.return_value = result

    resolved = await AttackChainsDAO(session)._resolve_hos_rule_fk("PthCvxsxjnSK")

    assert resolved == expected
    session.execute.assert_awaited_once()
