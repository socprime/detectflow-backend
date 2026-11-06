"""One deployment, one directory: the on-disk half of the per-customer isolation rule (§22).

The three RWX PVCs (`flink-{checkpoints,ha,savepoints}-pvc`) are shared by every Flink pod on the
platform, and RocksDB checkpoints hold raw customer events. Isolation used to be a path *naming*
convention — every pod mounted each PVC whole and politely stayed inside its own directory — which
is not a boundary at all: any pod could read, overwrite or `rm -rf` any other customer's state.

This file is parametrized over ALL THREE deployment templates so a fourth kind cannot quietly skip
the rule. It asserts the two halves that only work together:

1. every state mount, in every container, carries the deployment's own `subPath`;
2. no state path carries a per-deployment segment any more — with a scoped mount the container
   already sees only its own directory, so leaving the segment in would write to
   ``<pvc>/<name>/<name>``, orphan every existing checkpoint and cold-start the job.

The keys differ per template on purpose: correlation/agent are keyed on the deployment name,
the match node on the bare ``pipeline_id`` — in both cases the segment that already exists on
disk, which is why this change moves no bytes.
"""

from pathlib import Path
from unittest.mock import patch
from uuid import UUID

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader

from apps.core.constants import agent_deployment_name, correlation_deployment_name
from apps.providers.base import CorrelationDeploymentConfig
from apps.providers.kubernetes import KubernetesFlinkProvider
from tests.test_flink_deployment_template import render_detection_manifest

CORRELATION_TEMPLATE = "correlation-job-deployment.yaml.j2"
AGENT_TEMPLATE = "agent-job-deployment.yaml.j2"
DETECTION_TEMPLATE = "flink-deployment.yaml.j2"
ALL_TEMPLATES = (CORRELATION_TEMPLATE, AGENT_TEMPLATE, DETECTION_TEMPLATE)

STATE_VOLUMES = {"checkpoints", "ha-storage", "savepoints"}
STATE_MOUNT_PATHS = {"checkpoints": "/mnt/checkpoints", "ha-storage": "/mnt/ha", "savepoints": "/mnt/savepoints"}

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


def _correlation_config(pipeline_id: UUID) -> CorrelationDeploymentConfig:
    return CorrelationDeploymentConfig(
        correlation_pipeline_id=str(pipeline_id),
        correlation_pipeline_name=f"Customer {pipeline_id}",
        correlation_deployment_name=correlation_deployment_name(pipeline_id),
        agent_deployment_name=agent_deployment_name(pipeline_id),
        input_topics=[f"detectflow.tagged-events.{pipeline_id}"],
        chains_config_topic=f"detectflow.chains-config.{pipeline_id}",
        chains_raw_topic=f"detectflow.chains-raw.{pipeline_id}",
        chains_analysed_topic=f"detectflow.chains-analysed.{pipeline_id}",
    )


def expected_state_key(template_name: str, pipeline_id: UUID) -> str:
    """The directory this deployment's state already lives in on the shared PVCs."""
    if template_name == CORRELATION_TEMPLATE:
        return correlation_deployment_name(pipeline_id)
    if template_name == AGENT_TEMPLATE:
        return agent_deployment_name(pipeline_id)
    return str(pipeline_id)  # match node: the BARE pipeline id, not flink-<id>


def _render(provider: KubernetesFlinkProvider, template_name: str, pipeline_id: UUID) -> dict:
    """Render one template through the provider's own variable builder."""
    if template_name == DETECTION_TEMPLATE:
        return render_detection_manifest(provider, pipeline_id)

    config = _correlation_config(pipeline_id)
    if template_name == CORRELATION_TEMPLATE:
        template_vars = provider._correlation_job_template_vars(config)
    else:
        template_vars = provider._agent_job_template_vars(config)

    env = Environment(
        loader=FileSystemLoader(str(Path(__file__).parent.parent / "apps" / "templates")),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return yaml.safe_load(env.get_template(template_name).render(**template_vars))


def _state_mounts(manifest: dict) -> set[tuple[str, str, str]]:
    """(volume, mountPath, subPath) for the three state volumes, in EVERY container."""
    out: set[tuple[str, str, str]] = set()
    for tier in ("jobManager", "taskManager"):
        spec = manifest["spec"][tier]["podTemplate"]["spec"]
        for container in [*spec.get("initContainers", []), *spec["containers"]]:
            for mount in container.get("volumeMounts") or []:
                if mount["name"] in STATE_VOLUMES:
                    out.add((mount["name"], mount["mountPath"], mount.get("subPath", "")))
    return out


@pytest.mark.parametrize("template_name", ALL_TEMPLATES)
def test_two_instances_cannot_reach_each_others_state(provider: KubernetesFlinkProvider, template_name: str) -> None:
    key_a, key_b = expected_state_key(template_name, PIPELINE_A), expected_state_key(template_name, PIPELINE_B)
    a, b = _render(provider, template_name, PIPELINE_A), _render(provider, template_name, PIPELINE_B)

    for manifest, key in ((a, key_a), (b, key_b)):
        mounts = _state_mounts(manifest)
        assert len(mounts) == 3, f"expected 3 distinct state mounts, got {mounts}"
        assert {(volume, path) for volume, path, _ in mounts} == set(STATE_MOUNT_PATHS.items())
        assert all(sub == key for _, _, sub in mounts), f"un-isolated mount: {mounts}"

    assert key_b not in yaml.safe_dump(a)  # nothing in A names B, anywhere
    assert key_a not in yaml.safe_dump(b)


@pytest.mark.parametrize("template_name", ALL_TEMPLATES)
def test_every_container_mounts_all_three_state_volumes(provider: KubernetesFlinkProvider, template_name: str) -> None:
    """3 volumes x (main + init) x (JM + TM). A container that skips the scoping sees the PVC root."""
    manifest = _render(provider, template_name, PIPELINE_A)

    scoped = 0
    for tier in ("jobManager", "taskManager"):
        spec = manifest["spec"][tier]["podTemplate"]["spec"]
        containers = [*spec["initContainers"], *spec["containers"]]
        assert len(containers) == 2, f"{tier}: expected one init + one main container"
        for container in containers:
            mounts = [m for m in container["volumeMounts"] if m["name"] in STATE_VOLUMES]
            assert len(mounts) == 3, f"{tier}/{container['name']} mounts {mounts}"
            scoped += sum(1 for m in mounts if m.get("subPath"))
    assert scoped == 12


@pytest.mark.parametrize("template_name", ALL_TEMPLATES)
def test_state_dirs_carry_no_per_deployment_segment(provider: KubernetesFlinkProvider, template_name: str) -> None:
    """The segment moved from the path into the mount — leaving both double-nests the tree."""
    manifest = _render(provider, template_name, PIPELINE_A)

    flink_config = manifest["spec"]["flinkConfiguration"]
    assert flink_config["execution.checkpointing.dir"] == "file:///mnt/checkpoints"
    assert flink_config["high-availability.storageDir"] == "file:///mnt/ha"
    assert flink_config["execution.checkpointing.savepoint-dir"] == "file:///mnt/savepoints"

    # CHECKPOINT_PATH overrides execution.checkpointing.dir inside the job
    # (../detectflow-agent/app/jobs/_flink_env.py), so it has to lose the segment too. The match
    # node reads flinkConfiguration only and sets no such env.
    for tier in ("jobManager", "taskManager"):
        for container in manifest["spec"][tier]["podTemplate"]["spec"]["containers"]:
            for env in container.get("env") or []:
                if env["name"] == "CHECKPOINT_PATH":
                    assert env["value"] == "file:///mnt/checkpoints"


@pytest.mark.parametrize("template_name", ALL_TEMPLATES)
def test_no_world_writable_chmod_in_any_template(template_name: str) -> None:
    """0777 grants write to every other uid on every other pod sharing the PVC. 2770 is enough."""
    source = (Path(__file__).parent.parent / "apps" / "templates" / template_name).read_text()

    assert "chmod -R" not in source
    assert "777" not in source
    assert "chmod 2770" in source


@pytest.mark.parametrize("template_name", ALL_TEMPLATES)
def test_init_container_runs_as_root_and_only_fixes_ownership(
    provider: KubernetesFlinkProvider, template_name: str
) -> None:
    """Root is needed to chown a kubelet-created directory; the main container stays uid 9999."""
    manifest = _render(provider, template_name, PIPELINE_A)

    for tier in ("jobManager", "taskManager"):
        spec = manifest["spec"][tier]["podTemplate"]["spec"]
        assert spec["securityContext"]["fsGroup"] == 9999
        for container in spec["initContainers"]:
            assert container["securityContext"]["runAsUser"] == 0
            assert "chown 9999:9999" in container["command"][-1]
        for container in spec["containers"]:
            assert "securityContext" not in container
