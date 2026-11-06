"""Name-length and shape guards for generated correlation-pipeline deployment names.

Kubernetes rejects a Job name over 63 characters, and this repo derives ``cleanup-{name}`` from
every correlation/agent deployment name — that derived name, not the deployment name itself, is
the binding constraint. Flink adds its own suffixes on top (``-rest``, ``-taskmanager-N-N``,
``-cluster-config-map``), all shorter than the cleanup prefix.

The margin is asserted numerically rather than "it fits today" so a future prefix change fails
here instead of at stop time on a cluster.
"""

import re
from uuid import UUID, uuid4

import pytest

from apps.core.constants import agent_deployment_name, correlation_deployment_name
from apps.core.exceptions import ConfigurationError
from apps.core.settings import settings
from apps.providers.base import (
    CLEANUP_JOB_NAME_PREFIX,
    K8S_JOB_NAME_MAX_LEN,
    CorrelationDeploymentConfig,
)
from apps.providers.kubernetes import _assert_k8s_names_fit

# A real 36-char UUID: the only input these builders ever get.
PIPELINE_ID = UUID("11111111-1111-4111-8111-111111111111")

# DNS-1123 label: lowercase alphanumeric or '-', start/end with alphanumeric, ≤63 chars.
_DNS_1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

_BUILDERS = (correlation_deployment_name, agent_deployment_name)


def _config(correlation_name: str, agent_name: str) -> CorrelationDeploymentConfig:
    return CorrelationDeploymentConfig(
        correlation_pipeline_id=str(PIPELINE_ID),
        correlation_pipeline_name="Customer A",
        correlation_deployment_name=correlation_name,
        agent_deployment_name=agent_name,
        input_topics=[f"detectflow.tagged-events.{PIPELINE_ID}"],
        chains_config_topic=f"detectflow.chains-config.{PIPELINE_ID}",
        chains_raw_topic=f"detectflow.chains-raw.{PIPELINE_ID}",
        chains_analysed_topic=f"detectflow.chains-analysed.{PIPELINE_ID}",
    )


@pytest.mark.parametrize("builder", _BUILDERS)
def test_generated_name_is_a_dns_1123_label(builder) -> None:
    name = builder(PIPELINE_ID)

    assert _DNS_1123_LABEL.match(name), name
    # Also holds for an arbitrary UUID, not just the fixed one above.
    assert _DNS_1123_LABEL.match(builder(uuid4()))


@pytest.mark.parametrize("builder", _BUILDERS)
def test_generated_name_is_no_longer_than_the_matcher_convention(builder) -> None:
    """``flink-{uuid}`` (42) is the production-proven length; stay within it."""
    assert len(builder(PIPELINE_ID)) <= len(f"flink-{PIPELINE_ID}")


@pytest.mark.parametrize("builder", _BUILDERS)
def test_cleanup_job_name_fits_the_kubernetes_limit(builder) -> None:
    cleanup_name = f"{CLEANUP_JOB_NAME_PREFIX}{builder(PIPELINE_ID)}"

    assert len(cleanup_name) <= K8S_JOB_NAME_MAX_LEN
    # Keep the margin visible: cleanup-corr-<uuid> is 49 and cleanup-agent-<uuid> is 50, so
    # there are 13-14 characters of headroom for a future prefix.
    assert len(cleanup_name) in (49, 50), cleanup_name


def test_assert_k8s_names_fit_accepts_generated_names() -> None:
    _assert_k8s_names_fit(_config(correlation_deployment_name(PIPELINE_ID), agent_deployment_name(PIPELINE_ID)))


@pytest.mark.parametrize("field", ["correlation", "agent"])
def test_assert_k8s_names_fit_rejects_an_overlong_name(field: str) -> None:
    too_long = "x" * (K8S_JOB_NAME_MAX_LEN - len(CLEANUP_JOB_NAME_PREFIX) + 1)
    config = (
        _config(too_long, agent_deployment_name(PIPELINE_ID))
        if field == "correlation"
        else _config(correlation_deployment_name(PIPELINE_ID), too_long)
    )

    with pytest.raises(ConfigurationError, match="exceeds the 63-char Kubernetes Job limit"):
        _assert_k8s_names_fit(config)


def test_generated_names_never_equal_legacy_singleton_names() -> None:
    """The migration cutover leaves the pre-epic deployments up while ``Default`` starts.

    Both runtimes therefore coexist in one namespace for the length of the change window, and a
    name collision would make the new pipeline adopt — or delete — the live singleton. Collision is
    structurally impossible because every generated name ends in a 36-char UUID and ``job`` is not
    one, but the runbook depends on it, so pin it rather than assume it. The same argument covers
    the derived ``cleanup-`` Job names, which is what actually deletes state off the shared PVCs.
    """
    legacy_names = {settings.correlation_job_deployment_name, settings.agent_job_deployment_name}
    assert legacy_names == {"correlation-job", "agent-job"}

    for pipeline_id in (PIPELINE_ID, uuid4(), uuid4()):
        generated = {correlation_deployment_name(pipeline_id), agent_deployment_name(pipeline_id)}
        assert generated.isdisjoint(legacy_names)

        cleanup_generated = {f"{CLEANUP_JOB_NAME_PREFIX}{name}" for name in generated}
        cleanup_legacy = {f"{CLEANUP_JOB_NAME_PREFIX}{name}" for name in legacy_names}
        assert cleanup_generated.isdisjoint(cleanup_legacy)
