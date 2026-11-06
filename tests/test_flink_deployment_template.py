"""Unit test for the detection-pipeline (match node) FlinkDeployment template.

This template had no test at all, which is how it kept a whole-volume mount of the three shared
RWX PVCs (architecture §22) and an init container mounting Kafka certs it never reads.

The invariant with teeth is the **state key**: the match node's state lives under the bare
``pipeline_id`` (``<pvc>/<pipeline_id>``), while its Deployment is named ``flink-<pipeline_id>``.
Keying the mounts on the deployment name would look tidier and would orphan every existing
pipeline's checkpoints, cold-starting every customer's detection pipeline.

Manifests are captured from ``KubernetesFlinkProvider._create_flink_deployment`` rather than
rendered from a hand-written var dict, so provider/template drift fails here instead of on a
cluster.
"""

from unittest.mock import patch
from uuid import UUID

import pytest

from apps.core.settings import settings
from apps.providers.base import FlinkDeploymentConfig
from apps.providers.kubernetes import KubernetesFlinkProvider

TEMPLATE_NAME = "flink-deployment.yaml.j2"

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


@pytest.fixture
def provider() -> KubernetesFlinkProvider:
    """Construct a provider without touching kubeconfig / K8s API clients."""
    with (
        patch("apps.providers.kubernetes.config.load_incluster_config"),
        patch("apps.providers.kubernetes.config.load_kube_config"),
        patch("apps.providers.kubernetes.client.CustomObjectsApi"),
        patch("apps.providers.kubernetes.client.CoreV1Api"),
    ):
        return KubernetesFlinkProvider(namespace="security")


def render_detection_manifest(provider: KubernetesFlinkProvider, pipeline_id: UUID) -> dict:
    """Render the detection template the way the provider does, and return the manifest.

    Shared with ``tests/test_state_isolation_subpaths.py``: both need the real render path, and
    a second hand-rolled copy of it is exactly how the two templates drifted apart before.
    """
    config = FlinkDeploymentConfig(
        pipeline_id=str(pipeline_id),
        pipeline_name="Customer pipeline",
        input_topics=["raw-events"],
        output_topic="matched-events",
    )
    provider.api.create_namespaced_custom_object.reset_mock()
    provider._create_flink_deployment(config)
    return provider.api.create_namespaced_custom_object.call_args.kwargs["body"]


def _containers(manifest: dict, *, init: bool) -> list[dict]:
    key = "initContainers" if init else "containers"
    return [c for tier in ("jobManager", "taskManager") for c in manifest["spec"][tier]["podTemplate"]["spec"][key]]


def _state_mounts(container: dict) -> list[dict]:
    return [m for m in container.get("volumeMounts") or [] if m["name"] in {"checkpoints", "ha-storage", "savepoints"}]


def test_deployment_is_named_for_the_pipeline(provider: KubernetesFlinkProvider) -> None:
    manifest = render_detection_manifest(provider, PIPELINE_A)

    assert manifest["kind"] == "FlinkDeployment"
    assert manifest["metadata"]["name"] == f"flink-{PIPELINE_A}"
    assert manifest["metadata"]["labels"]["pipeline-id"] == str(PIPELINE_A)


def test_state_subpath_is_the_bare_pipeline_id_not_the_deployment_name(provider: KubernetesFlinkProvider) -> None:
    """The directory the match node has always written under — so no data moves (§22)."""
    manifest = render_detection_manifest(provider, PIPELINE_A)

    mounts = [m for c in _containers(manifest, init=True) + _containers(manifest, init=False) for m in _state_mounts(c)]
    assert mounts, "no state mounts rendered"
    assert {m["subPath"] for m in mounts} == {str(PIPELINE_A)}
    assert f"flink-{PIPELINE_A}" not in {m["subPath"] for m in mounts}


def test_state_dirs_carry_no_pipeline_segment(provider: KubernetesFlinkProvider) -> None:
    """With the mounts scoped, the segment must come OUT of the paths or the tree double-nests."""
    flink_config = render_detection_manifest(provider, PIPELINE_A)["spec"]["flinkConfiguration"]

    assert flink_config["execution.checkpointing.dir"] == "file:///mnt/checkpoints"
    assert flink_config["high-availability.storageDir"] == "file:///mnt/ha"
    assert flink_config["execution.checkpointing.savepoint-dir"] == "file:///mnt/savepoints"


def test_init_container_prepares_only_its_own_state_roots(provider: KubernetesFlinkProvider) -> None:
    init_containers = _containers(render_detection_manifest(provider, PIPELINE_A), init=True)

    assert len(init_containers) == 2  # JM + TM
    for container in init_containers:
        script = container["command"][-1]
        assert "chmod -R" not in script
        assert "777" not in script
        assert "chown 9999:9999" in script
        assert container["securityContext"]["runAsUser"] == 0
        # No kafka-ssl-certs: the init container reads no certs, and mounting a Secret it does
        # not use is one more thing a root container can see.
        assert [m["name"] for m in container["volumeMounts"]] == ["checkpoints", "ha-storage", "savepoints"]


def test_kafka_ssl_certs_mounted_on_main_containers_when_not_plaintext(
    provider: KubernetesFlinkProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "kafka_auth_method", "SASL_SSL")
    manifest = render_detection_manifest(provider, PIPELINE_A)

    ssl_mounts = [
        m for c in _containers(manifest, init=False) for m in c["volumeMounts"] if m["name"] == "kafka-ssl-certs"
    ]
    assert len(ssl_mounts) == 2  # JM + TM main containers
    assert all(m["mountPath"] == "/certs" for m in ssl_mounts)


def test_kafka_ssl_certs_omitted_when_plaintext(
    provider: KubernetesFlinkProvider, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(settings, "kafka_auth_method", "PLAINTEXT")
    manifest = render_detection_manifest(provider, PIPELINE_A)

    all_mounts = [m for c in _containers(manifest, init=False) for m in c["volumeMounts"]]
    assert not [m for m in all_mounts if m["name"] == "kafka-ssl-certs"]
    volumes = [
        v for tier in ("jobManager", "taskManager") for v in manifest["spec"][tier]["podTemplate"]["spec"]["volumes"]
    ]
    assert not [v for v in volumes if v["name"] == "kafka-ssl-certs"]
