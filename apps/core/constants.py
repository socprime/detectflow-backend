"""Pure, settings-free constants and name builders.

This module must never import ``apps.core.settings`` — ``tests/test_import_purity.py`` and
``tests/test_imports.py`` rely on it staying importable with no configuration. That is why the
topic builders below take the prefix as an argument; the thin wrapper that supplies
``settings.kafka_*_topic`` lives in ``apps/modules/kafka/topic_lifecycle.py``.
"""

from fnmatch import fnmatchcase
from uuid import NAMESPACE_DNS, UUID, uuid5

SOCPRIME_TDM_API_BASE_URL = "https://api.tdm.socprime.com"

# User-Agent for every outbound HTTP call this backend makes to external services.
HTTP_USER_AGENT = "SOCPrime-DetectFlow"

# Hardcoded list of external repositories
# Using UUID5 to generate deterministic UUIDs based on repository names
SIGMAHQ_UUID = str(uuid5(NAMESPACE_DNS, "sigmahq"))
SIGMAHQ_URL = "https://github.com/SigmaHQ/sigma"
SPLUNK_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "splunk-github"))
SPLUNK_GITHUB_URL = "https://github.com/splunk/security_content/tree/develop/detections"
ELASTIC_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "elastic-github"))
ELASTIC_GITHUB_URL = "https://github.com/elastic/detection-rules/tree/main/rules"
MICROSOFT_AZURE_SENTINEL_GITHUB_UUID = str(uuid5(NAMESPACE_DNS, "microsoft-azure-sentinel-github"))
MICROSOFT_AZURE_SENTINEL_GITHUB_URL = "https://github.com/Azure/Azure-Sentinel/tree/master/Hunting%20Queries"
EXTERNAL_REPOSITORIES = [
    {
        "id": SIGMAHQ_UUID,
        "name": "SigmaHQ",
        "source_link": SIGMAHQ_URL,
    },
    {
        "id": SPLUNK_GITHUB_UUID,
        "name": "Splunk (GitHub)",
        "source_link": SPLUNK_GITHUB_URL,
    },
    {
        "id": ELASTIC_GITHUB_UUID,
        "name": "Elastic (GitHub)",
        "source_link": ELASTIC_GITHUB_URL,
    },
    {
        "id": MICROSOFT_AZURE_SENTINEL_GITHUB_UUID,
        "name": "Microsoft Azure Sentinel (GitHub)",
        "source_link": MICROSOFT_AZURE_SENTINEL_GITHUB_URL,
    },
]

# =============================================================================
# Correlation pipelines — generated Deployment and Kafka topic names
# =============================================================================
# Name scheme: ``corr-{uuid}`` is 41 chars and ``agent-{uuid}`` is 42, both within the 42 of the
# production-proven matcher name ``flink-{uuid}`` (``KubernetesFlinkProvider._generate_deployment_
# name``). Every name Flink or this repo derives from them therefore stays inside the 63-char
# DNS-1123 label limit: ``-cluster-config-map`` → 61, ``-taskmanager-1-1`` → 58, ``{name}-rest`` →
# 47, ``cleanup-{name}`` → 50. The prefixes are deliberately distinct from ``flink-`` so listing
# by prefix never mixes per-pipeline matchers with correlation runtimes. The deployed value is
# persisted on ``correlation_pipelines.correlation_deployment_name`` / ``agent_deployment_name``,
# so changing a prefix later cannot orphan a running Deployment.

CORRELATION_DEPLOYMENT_PREFIX = "corr-"
AGENT_DEPLOYMENT_PREFIX = "agent-"


def correlation_deployment_name(correlation_pipeline_id: str | UUID) -> str:
    """DNS-1123 name for a correlation pipeline's correlation-job Deployment (41 chars)."""
    return f"{CORRELATION_DEPLOYMENT_PREFIX}{str(correlation_pipeline_id).lower()}"


def agent_deployment_name(correlation_pipeline_id: str | UUID) -> str:
    """DNS-1123 name for a correlation pipeline's agent-job Deployment (42 chars)."""
    return f"{AGENT_DEPLOYMENT_PREFIX}{str(correlation_pipeline_id).lower()}"


def scoped_topic(prefix: str, entity_id: str | UUID) -> str:
    """Build ``<prefix>.<uuid>``.

    Args:
        prefix: Topic prefix, e.g. ``settings.kafka_tagged_events_topic``.
        entity_id: Entity the topic belongs to (detection or correlation pipeline id).

    Returns:
        The scoped topic name, lowercased id.

    Raises:
        ValueError: If ``prefix`` is empty or blank — that is a caller bug, not a default, and
            would silently produce topic names starting with ``.``.
    """
    if not prefix or not prefix.strip():
        raise ValueError("topic prefix must be a non-empty string")
    return f"{prefix.strip()}.{str(entity_id).lower()}"


def tagged_events_topic(prefix: str, pipeline_id: str | UUID) -> str:
    """Per-detection-pipeline tagged-events topic; ``prefix`` is ``settings.kafka_tagged_events_topic``."""
    return scoped_topic(prefix, pipeline_id)


def chains_raw_topic(prefix: str, correlation_pipeline_id: str | UUID) -> str:
    """Per-correlation-pipeline chains-raw topic; ``prefix`` is ``settings.kafka_chains_raw_topic``."""
    return scoped_topic(prefix, correlation_pipeline_id)


def chains_analysed_topic(prefix: str, correlation_pipeline_id: str | UUID) -> str:
    """Per-correlation-pipeline chains-analysed topic; ``prefix`` is ``settings.kafka_chains_analysed_topic``."""
    return scoped_topic(prefix, correlation_pipeline_id)


def chains_config_topic(prefix: str, correlation_pipeline_id: str | UUID) -> str:
    """Per-correlation-pipeline chains-config topic; ``prefix`` is ``settings.kafka_chains_config_topic``."""
    return scoped_topic(prefix, correlation_pipeline_id)


def pipeline_name_matches_auto_add_pattern(pattern: str | None, pipeline_name: str) -> bool:
    """Match a detection pipeline's ``name`` against ``auto_add_pipeline_name_pattern``.

    The pattern is a shell-style glob (``fnmatch.fnmatchcase``), matched case-sensitively —
    operators type ``cust1-*``, and an unanchored regex would silently match substrings.

    Args:
        pattern: The correlation pipeline's ``auto_add_pipeline_name_pattern``.
        pipeline_name: Display name of the detection pipeline being considered.

    Returns:
        True when the pattern matches, or when no pattern is set (``None``/blank matches all).
    """
    if pattern is None or not pattern.strip():
        return True
    return fnmatchcase(pipeline_name, pattern.strip())
