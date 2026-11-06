"""Tests for health check service response mapping and the Kafka platform checks."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from apps.core.enums import HealthCheckStatus
from apps.core.models import HealthCheck
from apps.core.settings import settings
from apps.services.health_check.main import health_check_service
from apps.services.health_check.platforms.kafka import KafkaHealthChecker

CORRELATION_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_rows_to_platforms_status_reads_row_updated() -> None:
    updated_at = datetime(2026, 6, 26, 7, 44, 40, tzinfo=UTC)
    row = HealthCheck(
        id=uuid4(),
        name="PostgreSQL",
        checks=[
            {
                "status": "Operational",
                "title": "Database Connection",
                "descriptions": ["Connected to the database"],
                "updated": updated_at.isoformat(),
            }
        ],
        updated=updated_at,
    )

    platforms = health_check_service.rows_to_platforms_status([row])

    assert len(platforms) == 1
    assert platforms[0].name == "PostgreSQL"
    assert platforms[0].updated == updated_at


def _checker(topics: list[str]) -> KafkaHealthChecker:
    """A Kafka checker whose cluster is exactly `topics` (no broker call)."""
    checker = KafkaHealthChecker()
    checker._kafka_topics_cache = topics
    return checker


def _correlation_triple(entity_id: str) -> list[str]:
    return [
        f"{settings.kafka_chains_config_topic}.{entity_id}",
        f"{settings.kafka_chains_raw_topic}.{entity_id}",
        f"{settings.kafka_chains_analysed_topic}.{entity_id}",
    ]


def test_kafka_check_titles() -> None:
    """The registered checks are the shared topics, the per-pipeline families and parked records."""
    assert list(KafkaHealthChecker().checks) == [
        "Connected",
        "Service Topics - Sigma rules",
        "Service Topics - Activity",
        "Service Topics - Metrics",
        "Service Topics - Broadcast catalogs",
        "Service Topics - Chains analysed (legacy singleton)",
        "Service Topics - Per-pipeline families",
        "Chains analysed - Parked records",
    ]


async def test_broadcast_catalogs_check() -> None:
    """Active Threats + HoS rules are the only genuinely shared correlation inputs left."""
    both = [settings.kafka_active_threats_topic, settings.kafka_hos_rules_topic]
    status, _ = await _checker(both).check_broadcast_catalogs()
    assert status == HealthCheckStatus.OPERATIONAL

    status, descriptions = await _checker(both[:1]).check_broadcast_catalogs()
    assert status == HealthCheckStatus.ERROR
    assert settings.kafka_hos_rules_topic in descriptions[0]


@pytest.mark.parametrize(
    ("topics", "expected"),
    [
        ([settings.kafka_chains_analysed_topic], HealthCheckStatus.OPERATIONAL),
        ([], HealthCheckStatus.ERROR),
        # migrated cluster: the singleton is gone on purpose, and must not report ERROR forever
        ([f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}"], HealthCheckStatus.NOT_ENABLED),
    ],
)
async def test_legacy_chains_analysed_check(topics: list[str], expected: HealthCheckStatus) -> None:
    status, _ = await _checker(topics).check_chains_analysed_legacy()
    assert status == expected


async def test_pipeline_families_check_counts() -> None:
    """Counts are reported per family; a complete cluster is OPERATIONAL."""
    topics = [
        f"{settings.kafka_tagged_events_topic}.11111111-2222-3333-4444-555555555555",
        *_correlation_triple(CORRELATION_ID),
    ]
    status, descriptions = await _checker(topics).check_pipeline_topic_families()
    assert status == HealthCheckStatus.OPERATIONAL
    assert descriptions == ["tagged-events: 1", "chains-config: 1", "chains-raw: 1", "chains-analysed: 1"]


async def test_pipeline_families_check_no_dynamic_topics_is_operational() -> None:
    """A pre-epic cluster has no per-entity topics and is not unhealthy."""
    status, _ = await _checker([settings.kafka_sigma_rules_topic]).check_pipeline_topic_families()
    assert status == HealthCheckStatus.OPERATIONAL


async def test_pipeline_families_check_warns_on_incomplete_triple() -> None:
    """A chains-raw with no matching chains-analysed is exactly what a failed provision leaves."""
    topics = _correlation_triple(CORRELATION_ID)[:2]
    status, descriptions = await _checker(topics).check_pipeline_topic_families()
    assert status == HealthCheckStatus.WARNING
    assert CORRELATION_ID in descriptions[-1]
    assert "chains-analysed" in descriptions[-1]
