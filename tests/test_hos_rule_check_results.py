from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from apps.core.converters import ActiveThreatConverter
from apps.core.schemas import RuleIssueResponse
from apps.modules.postgre.high_order_sigma_rule_checks import (
    DISABLED_PIPELINE_ERROR_MESSAGE,
    NOT_CONNECTED_RULE_ERROR_MESSAGE,
    UNSUPPORTED_RULE_ERROR_MESSAGE,
    HighOrderSigmaRuleCheckFailureDAO,
)


def test_active_threat_list_item_includes_hos_rule_check_results() -> None:
    class Threat:
        pass

    threat = Threat()
    threat.tdm_id = "at-1"
    threat.name = "Active Threat"
    threat.is_blocked = False
    threat.is_hidden = False
    threat.severity = "high"
    threat.industries = None
    threat.geographies = None
    threat.threat_types = None
    threat.threat_actors = None
    threat.source = None
    threat.summary = None
    threat.investigation = None
    threat.response = None
    threat.mitigation = None
    threat.attack_flow = None
    threat.tdm_created = datetime(2026, 1, 1, tzinfo=UTC)
    threat.tdm_updated = datetime(2026, 1, 2, tzinfo=UTC)
    threat.created = datetime(2026, 1, 1, tzinfo=UTC)
    threat.updated = datetime(2026, 1, 2, tzinfo=UTC)
    threat.high_order_sigmas = []
    threat.linked_rules = []
    threat.rule_check_results = {
        "unsupported_rules": [
            {
                "rule_id": "22222222-2222-4222-8222-222222222222",
                "rule_name": "Unsupported Rule",
                "description": UNSUPPORTED_RULE_ERROR_MESSAGE,
            }
        ],
        "not_connected_rules": [
            {
                "rule_id": "33333333-3333-4333-8333-333333333333",
                "rule_name": "Not Connected Rule",
                "description": NOT_CONNECTED_RULE_ERROR_MESSAGE,
            }
        ],
        "disabled_rules": [
            {
                "rule_id": "44444444-4444-4444-8444-444444444444",
                "rule_name": "Disabled Rule",
                "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                "pipelines": [
                    {
                        "id": "55555555-5555-4555-8555-555555555555",
                        "name": "Windows Pipeline",
                    }
                ],
            }
        ],
    }

    response = ActiveThreatConverter.to_list_item(threat)

    checks = response.rule_check_results
    assert checks.unsupported_rules[0].rule_id == "22222222-2222-4222-8222-222222222222"
    assert checks.not_connected_rules[0].rule_name == "Not Connected Rule"
    assert checks.disabled_rules[0].pipelines[0].name == "Windows Pipeline"


def test_active_threat_detail_includes_hos_rule_check_results() -> None:
    class Threat:
        pass

    threat = Threat()
    threat.tdm_id = "at-1"
    threat.name = "Active Threat"
    threat.is_blocked = False
    threat.is_hidden = False
    threat.severity = "high"
    threat.summary = None
    threat.investigation = None
    threat.response = None
    threat.mitigation = None
    threat.attack_flow = None
    threat.industries = None
    threat.geographies = None
    threat.threat_types = None
    threat.threat_actors = None
    threat.source = None
    threat.tdm_created = datetime(2026, 1, 1, tzinfo=UTC)
    threat.tdm_updated = datetime(2026, 1, 2, tzinfo=UTC)
    threat.created = datetime(2026, 1, 1, tzinfo=UTC)
    threat.updated = datetime(2026, 1, 2, tzinfo=UTC)
    threat.high_order_sigmas = []
    threat.linked_rules = []
    threat.rule_check_results = {
        "unsupported_rules": [
            {
                "rule_id": "22222222-2222-4222-8222-222222222222",
                "rule_name": "Unsupported Rule",
                "description": UNSUPPORTED_RULE_ERROR_MESSAGE,
            }
        ],
        "not_connected_rules": [],
        "disabled_rules": [],
    }

    response = ActiveThreatConverter.to_detail(threat)

    assert response.rule_check_results.unsupported_rules[0].rule_name == "Unsupported Rule"
    assert response.rule_check_results_count == 1


async def test_get_grouped_results_by_active_threat_returns_single_payload() -> None:
    rule_id = UUID("22222222-2222-4222-8222-222222222222")
    pipeline_id = UUID("33333333-3333-4333-8333-333333333333")
    sigma_uuid = UUID("44444444-4444-4444-8444-444444444444")
    rows = [
        SimpleNamespace(
            rule_id=rule_id,
            is_not_supported=True,
            is_not_connected=True,
            disabled_pipeline_ids=[pipeline_id],
            name="Rule With Issues",
            sigma_uuid=sigma_uuid,
        )
    ]
    rule_result = MagicMock()
    rule_result.all.return_value = rows
    pipeline_result = MagicMock()
    pipeline_result.all.return_value = [(pipeline_id, "Windows Pipeline")]
    session = AsyncMock()
    session.execute.side_effect = [rule_result, pipeline_result]

    grouped = await HighOrderSigmaRuleCheckFailureDAO(session).get_grouped_results_by_active_threat("at-1")

    assert grouped == {
        "unsupported_rules": [
            {
                "rule_id": str(rule_id),
                "rule_name": "Rule With Issues",
                "description": UNSUPPORTED_RULE_ERROR_MESSAGE,
            }
        ],
        "not_connected_rules": [
            {
                "rule_id": str(rule_id),
                "rule_name": "Rule With Issues",
                "description": NOT_CONNECTED_RULE_ERROR_MESSAGE,
            }
        ],
        "disabled_rules": [
            {
                "rule_id": str(rule_id),
                "rule_name": "Rule With Issues",
                "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                "pipelines": [
                    {
                        "id": str(pipeline_id),
                        "name": "Windows Pipeline",
                    }
                ],
            }
        ],
    }


async def test_get_grouped_results_by_active_threat_deduplicates_by_sigma_uuid() -> None:
    sigma_uuid = UUID("11111111-1111-4111-8111-111111111111")
    first_rule_id = UUID("22222222-2222-4222-8222-222222222222")
    second_rule_id = UUID("33333333-3333-4333-8333-333333333333")
    first_pipeline_id = UUID("44444444-4444-4444-8444-444444444444")
    second_pipeline_id = UUID("55555555-5555-4555-8555-555555555555")
    rows = [
        SimpleNamespace(
            rule_id=first_rule_id,
            is_not_supported=True,
            is_not_connected=False,
            disabled_pipeline_ids=[first_pipeline_id],
            name="Duplicate Sigma Rule",
            sigma_uuid=sigma_uuid,
        ),
        SimpleNamespace(
            rule_id=second_rule_id,
            is_not_supported=True,
            is_not_connected=True,
            disabled_pipeline_ids=[first_pipeline_id, second_pipeline_id],
            name="Duplicate Sigma Rule",
            sigma_uuid=sigma_uuid,
        ),
    ]
    rule_result = MagicMock()
    rule_result.all.return_value = rows
    pipeline_result = MagicMock()
    pipeline_result.all.return_value = [
        (first_pipeline_id, "First Pipeline"),
        (second_pipeline_id, "Second Pipeline"),
    ]
    session = AsyncMock()
    session.execute.side_effect = [rule_result, pipeline_result]

    grouped = await HighOrderSigmaRuleCheckFailureDAO(session).get_grouped_results_by_active_threat("at-1")

    assert grouped == {
        "unsupported_rules": [
            {
                "rule_id": str(first_rule_id),
                "rule_name": "Duplicate Sigma Rule",
                "description": UNSUPPORTED_RULE_ERROR_MESSAGE,
            }
        ],
        "not_connected_rules": [
            {
                "rule_id": str(second_rule_id),
                "rule_name": "Duplicate Sigma Rule",
                "description": NOT_CONNECTED_RULE_ERROR_MESSAGE,
            }
        ],
        "disabled_rules": [
            {
                "rule_id": str(first_rule_id),
                "rule_name": "Duplicate Sigma Rule",
                "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                "pipelines": [
                    {
                        "id": str(first_pipeline_id),
                        "name": "First Pipeline",
                    },
                    {
                        "id": str(second_pipeline_id),
                        "name": "Second Pipeline",
                    },
                ],
            }
        ],
    }


def test_rule_check_results_count_deduplicates_a_rule_in_several_buckets() -> None:
    empty = RuleIssueResponse()
    assert empty.unique_rule_count() == 0

    results = RuleIssueResponse.model_validate(
        {
            "unsupported_rules": [
                {"rule_id": "r-1", "rule_name": "Same", "description": UNSUPPORTED_RULE_ERROR_MESSAGE}
            ],
            "not_connected_rules": [
                {"rule_id": "r-1", "rule_name": "Same", "description": NOT_CONNECTED_RULE_ERROR_MESSAGE}
            ],
            "disabled_rules": [
                {
                    "rule_id": "r-2",
                    "rule_name": "Other",
                    "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                    "pipelines": [],
                }
            ],
        }
    )

    assert results.unique_rule_count() == 2


def test_subtitles_by_rule_id_keeps_the_highest_priority_reason() -> None:
    results = RuleIssueResponse.model_validate(
        {
            "unsupported_rules": [
                {"rule_id": "r-1", "rule_name": "Same", "description": UNSUPPORTED_RULE_ERROR_MESSAGE}
            ],
            "not_connected_rules": [
                {"rule_id": "r-1", "rule_name": "Same", "description": NOT_CONNECTED_RULE_ERROR_MESSAGE},
                {"rule_id": "r-2", "rule_name": "Other", "description": NOT_CONNECTED_RULE_ERROR_MESSAGE},
            ],
            "disabled_rules": [
                {
                    "rule_id": "r-2",
                    "rule_name": "Other",
                    "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                    "pipelines": [{"id": "p-1", "name": "Windows Pipeline"}],
                },
                {
                    "rule_id": "r-3",
                    "rule_name": "Disabled only",
                    "description": DISABLED_PIPELINE_ERROR_MESSAGE,
                    "pipelines": [{"id": "p-1", "name": "Windows Pipeline"}],
                },
            ],
        }
    )

    subtitles = results.subtitles_by_rule_id()
    assert subtitles["r-1"] == UNSUPPORTED_RULE_ERROR_MESSAGE
    assert subtitles["r-2"] == NOT_CONNECTED_RULE_ERROR_MESSAGE
    assert subtitles["r-3"] == f"{DISABLED_PIPELINE_ERROR_MESSAGE} Windows Pipeline"
