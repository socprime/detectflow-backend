"""Unit test for the correlation-job FlinkDeployment template.

Guards the architectural invariants the template must satisfy, now that one template renders N
per-customer instances instead of a singleton:
- kind: FlinkDeployment (not plain Deployment), one object per correlation pipeline.
- Two pipelines share nothing: name, labels, JOB_ID, consumer group, state paths and the
  chains topics are all disjoint.
- KAFKA_INPUT_TOPICS is the comma-joined member set (epic D1), on BOTH containers.
- CORRELATION_PIPELINE_ID / _NAME identity env on BOTH containers.
- The resource keys that keep the Python worker alive (limit-factor, managed fraction) are
  present and unchanged — they are per-cluster physics, not per-pipeline tuning.
- No window env: epic D3's correlation window travels on the chains-config broadcast only.
- envFrom reuses cluster-wide ``flink-config`` ConfigMap + ``flink-sigma-config`` Secret, NOT
  the agent-only LLM ConfigMap — correlation-job does not talk to an LLM.
- The legacy pre-epic deployment (no pipeline id) renders without pipeline labels or identity
  env, so this parameterization did not change what production runs.

Template vars come from ``KubernetesFlinkProvider._correlation_job_template_vars`` rather than a
hand-written dict, so a provider/template drift fails here instead of on a cluster.
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

TEMPLATE_NAME = "correlation-job-deployment.yaml.j2"

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


def _config(
    pipeline_id: UUID,
    name: str,
    *,
    input_topics: list[str] | None = None,
    threshold_pct: int = 60,
) -> CorrelationDeploymentConfig:
    return CorrelationDeploymentConfig(
        correlation_pipeline_id=str(pipeline_id),
        correlation_pipeline_name=name,
        correlation_deployment_name=correlation_deployment_name(pipeline_id),
        agent_deployment_name=agent_deployment_name(pipeline_id),
        input_topics=input_topics or [f"detectflow.tagged-events.{pipeline_id}"],
        chains_config_topic=f"detectflow.chains-config.{pipeline_id}",
        chains_raw_topic=f"detectflow.chains-raw.{pipeline_id}",
        chains_analysed_topic=f"detectflow.chains-analysed.{pipeline_id}",
        threshold_pct=threshold_pct,
    )


def _legacy_config() -> CorrelationDeploymentConfig:
    """The pre-epic single-instance deployment: no pipeline identity, shared topics."""
    return CorrelationDeploymentConfig(
        correlation_pipeline_id="",
        correlation_pipeline_name="",
        correlation_deployment_name="correlation-job",
        agent_deployment_name="agent-job",
        input_topics=["detectflow.tagged-events"],
        chains_config_topic="detectflow.chains-config",
        chains_raw_topic="detectflow.chains-raw",
        chains_analysed_topic="detectflow.chains-analysed",
    )


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


def _render(
    provider: KubernetesFlinkProvider,
    config: CorrelationDeploymentConfig,
    *,
    kafka_auth_method: str = "SASL",
) -> dict:
    template_dir = Path(__file__).parent.parent / "apps" / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template_vars = provider._correlation_job_template_vars(config)
    template_vars["kafka_auth_method"] = kafka_auth_method
    return yaml.safe_load(env.get_template(TEMPLATE_NAME).render(**template_vars))


def _container_envs(manifest: dict) -> list[dict]:
    envs: list[dict] = []
    for tier in ("jobManager", "taskManager"):
        for container in manifest["spec"][tier]["podTemplate"]["spec"]["containers"]:
            envs.extend(container.get("env") or [])
    return envs


def _container_envfrom(manifest: dict) -> list[dict]:
    refs: list[dict] = []
    for tier in ("jobManager", "taskManager"):
        for container in manifest["spec"][tier]["podTemplate"]["spec"]["containers"]:
            refs.extend(container.get("envFrom") or [])
    return refs


def _envs_by_name(manifest: dict, name: str) -> list[dict]:
    return [e for e in _container_envs(manifest) if e["name"] == name]


def _env_pairs(manifest: dict) -> set[tuple[str, str]]:
    return {(e["name"], e["value"]) for e in _container_envs(manifest)}


def _pod_labels(manifest: dict) -> list[dict]:
    return [manifest["spec"][tier]["podTemplate"]["metadata"]["labels"] for tier in ("jobManager", "taskManager")]


def test_template_is_a_per_pipeline_flink_deployment(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))

    assert manifest["apiVersion"] == "flink.apache.org/v1beta1"
    assert manifest["kind"] == "FlinkDeployment"
    assert manifest["metadata"]["name"] == f"corr-{PIPELINE_A}"
    assert manifest["metadata"]["namespace"] == "security"
    assert manifest["spec"]["flinkVersion"] == "v2_2"
    assert manifest["spec"]["mode"] == "native"


def test_job_block_uses_pythondriver_without_input_topics(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))
    job = manifest["spec"]["job"]

    assert job["entryClass"] == "org.apache.flink.client.python.PythonDriver"
    assert "flink-python-2.2.0.jar" in job["jarURI"]
    assert job["upgradeMode"] == "last-state"
    # allowNonRestoredState must stay absent: dropping unrestored state silently discards every
    # keyed ChainBuffer instead of failing loudly.
    assert "allowNonRestoredState" not in job

    args = job["args"]
    assert "-pym" in args
    assert "app.jobs.correlation" in args
    # Topics ride KAFKA_INPUT_TOPICS env, not args — workers rebuild Settings() from the env.
    assert "--input-topics" not in args
    assert args[args.index("--job-id") + 1] == f"corr-{PIPELINE_A}"


def test_two_pipelines_share_no_identity_state_or_chains_topic(provider: KubernetesFlinkProvider) -> None:
    a = _render(provider, _config(PIPELINE_A, "Customer A"))
    b = _render(provider, _config(PIPELINE_B, "Customer B"))

    assert a["metadata"]["name"] != b["metadata"]["name"]

    # Labels: on the CR and on BOTH podTemplates, so pods are selectable too.
    for manifest, pipeline_id in ((a, PIPELINE_A), (b, PIPELINE_B)):
        for labels in [manifest["metadata"]["labels"], *_pod_labels(manifest)]:
            assert labels["detectflow.io/correlation-pipeline-id"] == str(pipeline_id)
            assert labels["detectflow.io/job-role"] == "correlation"

    # State: the paths are deliberately IDENTICAL now — isolation is the subPath mount, not the
    # path (architecture §22, exhaustively asserted in tests/test_state_isolation_subpaths.py).
    # A per-deployment segment here would double-nest the tree on the PVC and cold-start the job.
    for key in (
        "execution.checkpointing.dir",
        "high-availability.storageDir",
        "execution.checkpointing.savepoint-dir",
    ):
        assert a["spec"]["flinkConfiguration"][key] == b["spec"]["flinkConfiguration"][key]
        assert str(PIPELINE_A) not in a["spec"]["flinkConfiguration"][key]

    for name in ("JOB_ID", "KAFKA_CONSUMER_GROUP", "KAFKA_CHAINS_CONFIG_TOPIC"):
        values_a = {e["value"] for e in _envs_by_name(a, name)}
        values_b = {e["value"] for e in _envs_by_name(b, name)}
        assert len(values_a) == 1, f"{name}: JM and TM disagree"
        assert not values_a & values_b, f"{name}: shared between two pipelines"

    assert {e["value"] for e in _envs_by_name(a, "KAFKA_CHAINS_RAW_TOPIC")} == {f"detectflow.chains-raw.{PIPELINE_A}"}


def test_env_sets_intersect_only_on_cluster_wide_values(provider: KubernetesFlinkProvider) -> None:
    """Everything two instances have in common must be infrastructure, not customer data."""
    a = _render(provider, _config(PIPELINE_A, "Customer A", threshold_pct=60))
    b = _render(provider, _config(PIPELINE_B, "Customer B", threshold_pct=75))

    shared_names = {name for name, _ in _env_pairs(a) & _env_pairs(b)}
    assert shared_names == {
        "KAFKA_ACTIVE_THREATS_TOPIC",  # shared vendor catalogue
        "KAFKA_HOS_RULES_TOPIC",  # shared vendor catalogue
        "KAFKA_ACTIVITY_TOPIC",  # cross-tenant health channel
        "STATE_BACKEND",
        "CHECKPOINT_PATH",  # same path, different subPath — §22
        "FLINK_CHECKPOINT_INTERVAL_MS",
        "FLINK_PARALLELISM",
    }


def test_input_topics_are_comma_joined_on_both_containers(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A", input_topics=["a", "b", "c"]))

    entries = _envs_by_name(manifest, "KAFKA_INPUT_TOPICS")
    assert len(entries) == 2  # JM + TM
    assert all(e["value"] == "a,b,c" for e in entries)


def test_pipeline_identity_env_is_present_on_both_containers(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))

    assert [e["value"] for e in _envs_by_name(manifest, "CORRELATION_PIPELINE_ID")] == [str(PIPELINE_A)] * 2
    assert [e["value"] for e in _envs_by_name(manifest, "CORRELATION_PIPELINE_NAME")] == ["Customer A"] * 2


def test_threshold_is_seeded_and_window_is_not(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A", threshold_pct=75))

    assert [e["value"] for e in _envs_by_name(manifest, "CHAIN_COVERAGE_THRESHOLD_PCT")] == ["75"] * 2
    # The correlation window is an explicit override transported by the chains-config envelope.
    # A second, env-shaped channel would give a load-bearing value two sources of truth.
    assert _envs_by_name(manifest, "CHAIN_DEFAULT_WINDOW_SECONDS") == []
    assert _envs_by_name(manifest, "CORRELATION_WINDOW_SECONDS") == []


def test_python_worker_memory_keys_survive_parameterization(provider: KubernetesFlinkProvider) -> None:
    """The off-heap Python worker's headroom is per-cluster physics; parameterizing must not drop it."""
    fc = _render(provider, _config(PIPELINE_A, "Customer A"))["spec"]["flinkConfiguration"]

    assert fc["kubernetes.taskmanager.memory.limit-factor"] == "1.75"
    assert fc["taskmanager.memory.managed.fraction"] == "0.4"
    assert fc["taskmanager.memory.process.size"] == "4096m"  # settings default 4g
    assert fc["taskmanager.cpu.cores"] == "3.0"
    assert fc["jobmanager.cpu.cores"] == "1.0"


def test_envfrom_reuses_shared_flink_config_only(provider: KubernetesFlinkProvider) -> None:
    """correlation-job mounts ONLY the shared flink-config/sigma-config — no LLM."""
    refs = _container_envfrom(_render(provider, _config(PIPELINE_A, "Customer A")))

    configmap_refs = [r.get("configMapRef", {}).get("name") for r in refs if "configMapRef" in r]
    secret_refs = [r.get("secretRef", {}).get("name") for r in refs if "secretRef" in r]

    # Each container (JM + TM) has 1 configMapRef + 1 secretRef → 2 of each total.
    assert configmap_refs == ["flink-config", "flink-config"]
    assert secret_refs == ["flink-sigma-config", "flink-sigma-config"]
    # correlation-job does NOT talk to an LLM, so the agent-only ConfigMap / Secret must not
    # be mounted here.
    assert "detectflow-agent-config" not in configmap_refs
    assert "detectflow-agent-secrets" not in secret_refs


def test_legacy_deployment_renders_without_pipeline_identity(provider: KubernetesFlinkProvider) -> None:
    """No pipeline id ⇒ no per-pipeline labels and no identity env (pre-epic behaviour).

    The label is what ``list_correlation_deployments`` selects on, so labelling the legacy
    deployment would make the runtime view report the live production job as an orphan.
    """
    manifest = _render(provider, _legacy_config())

    assert manifest["metadata"]["name"] == "correlation-job"
    for labels in [manifest["metadata"]["labels"], *_pod_labels(manifest)]:
        assert "detectflow.io/correlation-pipeline-id" not in labels
        assert "detectflow.io/job-role" not in labels
    assert _envs_by_name(manifest, "CORRELATION_PIPELINE_ID") == []
    assert _envs_by_name(manifest, "CORRELATION_PIPELINE_NAME") == []
    assert [e["value"] for e in _envs_by_name(manifest, "KAFKA_INPUT_TOPICS")] == ["detectflow.tagged-events"] * 2


def test_jobmanager_cpu_is_wired_from_the_row(provider: KubernetesFlinkProvider) -> None:
    """JobManager CPU is a resource like any other: rendered, never left to a Flink default.

    Ported from the pre-epic hand-written var dict: the value now comes from the row's
    ``correlation_jobmanager_cpu`` column (NULL inherits ``settings.correlation_job_jobmanager_cpu``), so
    the assertion is on what the provider derives rather than on what a test dict said.
    """
    manifest = _render(provider, _config(PIPELINE_A, "tenant-a"))
    jm = manifest["spec"]["jobManager"]
    fc = manifest["spec"]["flinkConfiguration"]

    assert jm["resource"]["cpu"] == 1.0
    assert jm["resource"]["memory"] == "2048m"
    # The pod resource is the only place CPU is set — a flinkConfiguration twin would fight it.
    assert "kubernetes.jobmanager.cpu" not in fc


def test_jobmanager_cpu_accepts_fractional_cores(provider: KubernetesFlinkProvider) -> None:
    config = _config(PIPELINE_A, "tenant-a")
    config.correlation_jobmanager_cpu = 0.25
    config.correlation_taskmanager_cpu = 0.25

    manifest = _render(provider, config)

    assert manifest["spec"]["jobManager"]["resource"]["cpu"] == 0.25
    assert "kubernetes.jobmanager.cpu" not in manifest["spec"]["flinkConfiguration"]
    assert manifest["spec"]["flinkConfiguration"]["taskmanager.cpu.cores"] == "0.25"


def _container_volume_mounts(manifest: dict) -> list[dict]:
    mounts: list[dict] = []
    for tier in ("jobManager", "taskManager"):
        for container in manifest["spec"][tier]["podTemplate"]["spec"]["containers"]:
            mounts.extend(container.get("volumeMounts") or [])
    return mounts


def _pod_volumes(manifest: dict) -> list[dict]:
    volumes: list[dict] = []
    for tier in ("jobManager", "taskManager"):
        volumes.extend(manifest["spec"][tier]["podTemplate"]["spec"].get("volumes") or [])
    return volumes


def test_kafka_ssl_certs_mounted_when_auth_method_is_not_plaintext(provider: KubernetesFlinkProvider) -> None:
    # docs/devops/k8s-prereqs.md: kafka-ssl Secret mounted at /certs/ in JM and
    # TM pods when KAFKA_AUTH_METHOD != "PLAINTEXT" — the ConfigMap's
    # KAFKA_SSL_*_LOCATION values point inside /certs/.
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"), kafka_auth_method="SASL")

    ssl_mounts = [m for m in _container_volume_mounts(manifest) if m["name"] == "kafka-ssl-certs"]
    assert len(ssl_mounts) == 2  # JM + TM main containers
    assert all(m["mountPath"] == "/certs" for m in ssl_mounts)

    ssl_volumes = [v for v in _pod_volumes(manifest) if v["name"] == "kafka-ssl-certs"]
    assert len(ssl_volumes) == 2
    assert all(v["secret"]["secretName"] == "kafka-ssl" for v in ssl_volumes)


def test_kafka_ssl_certs_omitted_when_auth_method_is_plaintext(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"), kafka_auth_method="PLAINTEXT")

    assert not [m for m in _container_volume_mounts(manifest) if m["name"] == "kafka-ssl-certs"]
    assert not [v for v in _pod_volumes(manifest) if v["name"] == "kafka-ssl-certs"]
