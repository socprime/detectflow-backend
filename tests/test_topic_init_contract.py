"""Drift-detection invariant for the Kafka wire contract.

The contract has two halves now. `REQUIRED_TOPICS` (`apps/modules/kafka/topic_init.py`) is the
boot-time source of truth for the **static, shared** topics; `apps/modules/kafka/topic_lifecycle.py`
is the source of truth for the four **per-entity families** created and retired at runtime. For
every topic in either half, partitions, cleanup policy and retention MUST be set explicitly —
relying on `settings.kafka_default_partitions` would let a future default change silently violate
the cross-repo Kafka contract with detectflow-agent.

Topics are keyed here by their **settings attribute**, not by a literal name: a deployment may
rename any topic via env, and it is the partition/cleanup/retention triple that is the contract.
The default names themselves are pinned separately by `test_default_topic_names`.

If architecture changes, update `ARCHITECTURE_STATIC_TOPICS` / `ARCHITECTURE_TOPIC_FAMILIES` here
AND the spec they mirror together — these tests guard against the two drifting apart.
"""

import pytest

# Shared topics, created once at boot. The four `{tagged-events,chains-*}` singletons stay listed
# while pre-epic rows still ride them (NULL `pipelines.tagged_events_topic`).
ARCHITECTURE_STATIC_TOPICS = {
    "kafka_sigma_rules_topic": {"partitions": 1, "cleanup_policy": "compact", "retention_ms": -1},
    "kafka_active_threats_topic": {"partitions": 1, "cleanup_policy": "compact", "retention_ms": -1},
    "kafka_hos_rules_topic": {"partitions": 1, "cleanup_policy": "compact", "retention_ms": -1},
    "kafka_activity_topic": {"partitions": 1, "cleanup_policy": "delete", "retention_ms": 604800000},
    "kafka_metrics_topic": {"partitions": 1, "cleanup_policy": "delete", "retention_ms": 604800000},
    # legacy singletons
    "kafka_chains_config_topic": {"partitions": 1, "cleanup_policy": "compact", "retention_ms": -1},
    "kafka_chains_raw_topic": {"partitions": 12, "cleanup_policy": "delete", "retention_ms": 604800000},
    "kafka_chains_analysed_topic": {"partitions": 12, "cleanup_policy": "delete", "retention_ms": 604800000},
    "kafka_tagged_events_topic": {"partitions": 12, "cleanup_policy": "delete", "retention_ms": 43200000},
}

# Per-entity families, created on demand as `<prefix>.<entity_id>`.
ARCHITECTURE_TOPIC_FAMILIES = {
    "kafka_tagged_events_topic": {"partitions": 6, "cleanup_policy": "delete", "retention_ms": 43200000},
    "kafka_chains_config_topic": {"partitions": 1, "cleanup_policy": "compact", "retention_ms": -1},
    "kafka_chains_raw_topic": {"partitions": 3, "cleanup_policy": "delete", "retention_ms": 604800000},
    "kafka_chains_analysed_topic": {"partitions": 3, "cleanup_policy": "delete", "retention_ms": 604800000},
}

# The names a default install ships with — the cross-repo wire contract every sibling hardcodes.
DEFAULT_TOPIC_NAMES = {
    "kafka_sigma_rules_topic": "detectflow.sigma-rules",
    "kafka_active_threats_topic": "detectflow.active-threats",
    "kafka_hos_rules_topic": "detectflow.hos-rules",
    "kafka_activity_topic": "detectflow.activity",
    "kafka_metrics_topic": "detectflow.rule-metrics",
    "kafka_tagged_events_topic": "detectflow.tagged-events",
    "kafka_chains_config_topic": "detectflow.chains-config",
    "kafka_chains_raw_topic": "detectflow.chains-raw",
    "kafka_chains_analysed_topic": "detectflow.chains-analysed",
}

ENTITY_ID = "11111111-2222-3333-4444-555555555555"


def _family_specs() -> dict[str, object]:
    """Generated spec of every family, keyed by the settings attribute holding its prefix."""
    from apps.core.settings import settings
    from apps.modules.kafka.topic_lifecycle import correlation_pipeline_topic_specs, pipeline_topic_specs

    specs = pipeline_topic_specs(ENTITY_ID) + correlation_pipeline_topic_specs(ENTITY_ID)
    by_prefix = {spec.name.rsplit(".", 1)[0]: spec for spec in specs}
    return {attr: by_prefix[getattr(settings, attr)] for attr in ARCHITECTURE_TOPIC_FAMILIES}


def test_default_topic_names(setup_test_env):
    """The shipped defaults are the names every sibling repo hardcodes — env may override, code may not."""
    from apps.core.settings import Settings

    for attr, expected in DEFAULT_TOPIC_NAMES.items():
        assert Settings.model_fields[attr].default == expected, f"{attr}: default topic name changed"


def test_required_topics_cover_all_static_topics(setup_test_env):
    """Every shared/static topic in the architecture must be in REQUIRED_TOPICS."""
    from apps.core.settings import settings
    from apps.modules.kafka.topic_init import REQUIRED_TOPICS

    declared = {t.name for t in REQUIRED_TOPICS}
    missing = {getattr(settings, attr) for attr in ARCHITECTURE_STATIC_TOPICS} - declared
    assert not missing, f"REQUIRED_TOPICS missing static topics: {missing}"


@pytest.mark.parametrize("attr", list(ARCHITECTURE_STATIC_TOPICS))
def test_required_topic_matches_architecture(attr: str, setup_test_env):
    """Every static topic spec must match the architecture exactly."""
    from apps.core.settings import settings
    from apps.modules.kafka.topic_init import REQUIRED_TOPICS

    name = getattr(settings, attr)
    cfg = {t.name: t for t in REQUIRED_TOPICS}[name]
    expected = ARCHITECTURE_STATIC_TOPICS[attr]
    assert cfg.partitions == expected["partitions"], (
        f"{name}: partitions mismatch — declared={cfg.partitions}, architecture={expected['partitions']}"
    )
    assert cfg.cleanup_policy == expected["cleanup_policy"], (
        f"{name}: cleanup_policy mismatch — declared={cfg.cleanup_policy}, architecture={expected['cleanup_policy']}"
    )
    assert cfg.retention_ms == expected["retention_ms"], (
        f"{name}: retention_ms mismatch — declared={cfg.retention_ms}, architecture={expected['retention_ms']}"
    )


@pytest.mark.parametrize("attr", list(ARCHITECTURE_TOPIC_FAMILIES))
def test_topic_family_specs_match_architecture(attr: str, setup_test_env):
    """Every per-entity family spec must match the architecture exactly (6 / 1 / 3 / 3)."""
    from apps.core.settings import settings

    spec = _family_specs()[attr]
    expected = ARCHITECTURE_TOPIC_FAMILIES[attr]
    assert spec.name == f"{getattr(settings, attr)}.{ENTITY_ID}"
    assert spec.partitions == expected["partitions"], (
        f"{spec.name}: partitions mismatch — declared={spec.partitions}, architecture={expected['partitions']}"
    )
    assert spec.cleanup_policy == expected["cleanup_policy"], (
        f"{spec.name}: cleanup_policy mismatch — declared={spec.cleanup_policy}, "
        f"architecture={expected['cleanup_policy']}"
    )
    assert spec.retention_ms == expected["retention_ms"], (
        f"{spec.name}: retention_ms mismatch — declared={spec.retention_ms}, architecture={expected['retention_ms']}"
    )


def test_no_topic_relies_on_default_partitions_for_wire_contract(setup_test_env):
    """No topic — static or generated — may leave partitions/cleanup/retention to the broker."""
    from apps.modules.kafka.topic_init import REQUIRED_TOPICS

    for spec in list(REQUIRED_TOPICS) + list(_family_specs().values()):
        assert spec.partitions > 0, f"{spec.name}: partitions must be explicit and positive"
        config = spec.to_topic_config()
        assert "cleanup.policy" in config, f"{spec.name}: cleanup.policy must be explicit"
        assert "retention.ms" in config, f"{spec.name}: retention.ms must be explicit"


def test_family_names_are_legal_kafka_topic_names(setup_test_env):
    """Generated names must be legal Kafka topic names and never a shared topic."""
    from apps.modules.kafka.topic_lifecycle import MAX_TOPIC_NAME_LENGTH, TOPIC_NAME_RE, shared_topics

    for spec in _family_specs().values():
        assert TOPIC_NAME_RE.match(spec.name), f"{spec.name}: illegal Kafka topic name"
        assert len(spec.name) <= MAX_TOPIC_NAME_LENGTH
        assert spec.name not in shared_topics(), f"{spec.name}: collides with a shared topic"
