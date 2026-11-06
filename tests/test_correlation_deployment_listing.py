"""Unit tests for ``KubernetesFlinkProvider.list_correlation_deployments``.

The runtime view diffs what the cluster actually runs against ``correlation_pipelines``, so the
listing must select on the **existence** of the correlation-pipeline-id label, never on a value:
a deployment whose row was deleted is precisely the orphan we need to see. The legacy
single-instance deployment carries no such label and is therefore not listed.
"""

from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from apps.providers.base import CORRELATION_JOB_ROLE_LABEL, CORRELATION_PIPELINE_ID_LABEL
from apps.providers.kubernetes import KubernetesFlinkProvider

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")


@pytest.fixture
def provider() -> KubernetesFlinkProvider:
    with (
        patch("apps.providers.kubernetes.config.load_incluster_config"),
        patch("apps.providers.kubernetes.config.load_kube_config"),
        patch("apps.providers.kubernetes.client.CustomObjectsApi"),
        patch("apps.providers.kubernetes.client.CoreV1Api"),
    ):
        return KubernetesFlinkProvider(namespace="security")


def _item(name: str, pipeline_id: str, role: str, job_state: str | None = "RUNNING") -> dict:
    return {
        "metadata": {
            "name": name,
            "labels": {
                "app": "detectflow-correlation-job",
                CORRELATION_PIPELINE_ID_LABEL: pipeline_id,
                CORRELATION_JOB_ROLE_LABEL: role,
            },
        },
        "status": {"jobStatus": {"state": job_state}} if job_state else {},
    }


def test_lists_both_roles_with_normalized_state(provider: KubernetesFlinkProvider) -> None:
    provider.api.list_namespaced_custom_object = MagicMock(
        return_value={
            "items": [
                _item(f"corr-{PIPELINE_A}", str(PIPELINE_A), "correlation"),
                _item(f"agent-{PIPELINE_A}", str(PIPELINE_A), "agent", job_state="FAILED"),
            ]
        }
    )

    refs = provider._list_correlation_deployments()

    assert [(r.deployment_name, r.role, r.state) for r in refs] == [
        (f"corr-{PIPELINE_A}", "correlation", "running"),
        (f"agent-{PIPELINE_A}", "agent", "failed"),
    ]
    assert {r.correlation_pipeline_id for r in refs} == {str(PIPELINE_A)}


def test_selects_on_label_existence_not_value(provider: KubernetesFlinkProvider) -> None:
    """An orphaned deployment must still be listed, so the selector carries no value."""
    provider.api.list_namespaced_custom_object = MagicMock(return_value={"items": []})

    provider._list_correlation_deployments()

    kwargs = provider.api.list_namespaced_custom_object.call_args.kwargs
    assert kwargs["label_selector"] == CORRELATION_PIPELINE_ID_LABEL
    assert "=" not in kwargs["label_selector"]
    assert kwargs["namespace"] == "security"


def test_skips_items_with_an_unknown_job_role(provider: KubernetesFlinkProvider) -> None:
    provider.api.list_namespaced_custom_object = MagicMock(
        return_value={
            "items": [
                _item("mislabelled", str(PIPELINE_A), "matcher"),
                _item(f"corr-{PIPELINE_A}", str(PIPELINE_A), "correlation"),
            ]
        }
    )

    refs = provider._list_correlation_deployments()

    assert [r.deployment_name for r in refs] == [f"corr-{PIPELINE_A}"]
