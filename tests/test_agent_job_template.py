"""Unit test for the agent-job FlinkDeployment template.

Guards the architectural invariants the template must satisfy, now that one template renders N
per-customer instances instead of a singleton:
- kind: FlinkDeployment (not plain Deployment), one object per correlation pipeline.
- Two pipelines share nothing: name, labels, JOB_ID, consumer group, state paths and the
  chains-raw / chains-analysed topics are all disjoint. JOB_ID matters most here — agent-job
  starts from COMMITTED offsets, so a shared consumer group silently skips chains or re-pays
  an LLM for them.
- CORRELATION_PIPELINE_ID / _NAME identity env on BOTH containers, and no KAFKA_INPUT_TOPICS
  (agent-job has no subscription-universe knob; it consumes its pipeline's chains-raw).
- PythonDriver args: -pym app.jobs.agent, NO --input-topics.
- envFrom: cluster-wide ``flink-config`` ConfigMap + ``flink-sigma-config`` Secret (Kafka
  bootstrap + auth) PLUS ``detectflow-agent-config`` / ``detectflow-agent-secrets`` for the LLM.
- Optional dedicated node selector for thread-mode (Pemja) TMs, with fallback to the global
  flink_node_selector_{key,value}.
- Optional private-CA mount for an LLM endpoint with a self-signed certificate: mountPath /
  Secret key are derived from ``SSL_CERT_FILE``, never hardcoded. The CA is a property of the
  LLM endpoint, so it is cluster-wide — every pipeline's agent-job mounts the same bundle.

Template vars come from ``KubernetesFlinkProvider._agent_job_template_vars`` rather than a
hand-written dict, so a provider/template drift fails here instead of on a cluster.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader
from kubernetes.client.rest import ApiException

from apps.core.constants import agent_deployment_name, correlation_deployment_name
from apps.core.settings import settings
from apps.providers.base import CorrelationDeploymentConfig
from apps.providers.kubernetes import KubernetesFlinkProvider

TEMPLATE_NAME = "agent-job-deployment.yaml.j2"

PIPELINE_A = UUID("11111111-1111-4111-8111-111111111111")
PIPELINE_B = UUID("22222222-2222-4222-8222-222222222222")


def _config(pipeline_id: UUID, name: str) -> CorrelationDeploymentConfig:
    return CorrelationDeploymentConfig(
        correlation_pipeline_id=str(pipeline_id),
        correlation_pipeline_name=name,
        correlation_deployment_name=correlation_deployment_name(pipeline_id),
        agent_deployment_name=agent_deployment_name(pipeline_id),
        input_topics=[f"detectflow.tagged-events.{pipeline_id}"],
        chains_config_topic=f"detectflow.chains-config.{pipeline_id}",
        chains_raw_topic=f"detectflow.chains-raw.{pipeline_id}",
        chains_analysed_topic=f"detectflow.chains-analysed.{pipeline_id}",
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
    agent_node_selector_key: str | None = None,
    agent_node_selector_value: str | None = None,
    ssl_cert_file: str | None = None,
    ca_authoritative: bool = True,
    **overrides: object,
) -> dict:
    template_dir = Path(__file__).parent.parent / "apps" / "templates"
    env = Environment(
        loader=FileSystemLoader(str(template_dir)),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    # The CA path is normally read off the agent ConfigMap; stubbing the lookup keeps the
    # provider's own path derivation in the test (mountPath/Secret key still come from it) while
    # the fixture's mocked CoreV1Api cannot leak a Mock into the rendered manifest.
    derived = KubernetesFlinkProvider._derive_agent_ca_mount(ssl_cert_file)
    ca_mount = (*derived, ca_authoritative) if derived else None
    with patch.object(KubernetesFlinkProvider, "_agent_ca_mount", return_value=ca_mount):
        template_vars = provider._agent_job_template_vars(config)
    template_vars["kafka_auth_method"] = kafka_auth_method
    template_vars["agent_node_selector_key"] = agent_node_selector_key
    template_vars["agent_node_selector_value"] = agent_node_selector_value
    template_vars |= overrides
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
    assert manifest["metadata"]["name"] == f"agent-{PIPELINE_A}"
    assert manifest["metadata"]["namespace"] == "security"
    assert manifest["spec"]["flinkVersion"] == "v2_2"
    assert manifest["spec"]["mode"] == "native"


def test_job_block_uses_pythondriver_without_input_topics(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))
    job = manifest["spec"]["job"]

    assert job["entryClass"] == "org.apache.flink.client.python.PythonDriver"
    assert "flink-python-2.2.0.jar" in job["jarURI"]
    assert job["upgradeMode"] == "last-state"
    assert "allowNonRestoredState" not in job

    args = job["args"]
    assert "-pym" in args
    assert "app.jobs.agent" in args
    # agent-job has no subscription-universe knob; it consumes its pipeline's chains-raw topic
    # from the env below.
    assert "--input-topics" not in args
    assert args[args.index("--job-id") + 1] == f"agent-{PIPELINE_A}"


def test_two_pipelines_share_no_identity_state_or_chains_topic(provider: KubernetesFlinkProvider) -> None:
    a = _render(provider, _config(PIPELINE_A, "Customer A"))
    b = _render(provider, _config(PIPELINE_B, "Customer B"))

    assert a["metadata"]["name"] != b["metadata"]["name"]

    for manifest, pipeline_id in ((a, PIPELINE_A), (b, PIPELINE_B)):
        for labels in [manifest["metadata"]["labels"], *_pod_labels(manifest)]:
            assert labels["detectflow.io/correlation-pipeline-id"] == str(pipeline_id)
            assert labels["detectflow.io/job-role"] == "agent"

    # State: the paths are deliberately IDENTICAL now — isolation is the subPath mount, not the
    # path (architecture §22, exhaustively asserted in tests/test_state_isolation_subpaths.py).
    for key in (
        "execution.checkpointing.dir",
        "high-availability.storageDir",
        "execution.checkpointing.savepoint-dir",
    ):
        assert a["spec"]["flinkConfiguration"][key] == b["spec"]["flinkConfiguration"][key]
        assert str(PIPELINE_A) not in a["spec"]["flinkConfiguration"][key]

    for name in (
        "JOB_ID",
        "KAFKA_CONSUMER_GROUP",
        "KAFKA_CHAINS_RAW_TOPIC",
        "KAFKA_CHAINS_ANALYSED_TOPIC",
    ):
        values_a = {e["value"] for e in _envs_by_name(a, name)}
        values_b = {e["value"] for e in _envs_by_name(b, name)}
        assert len(values_a) == 1, f"{name}: JM and TM disagree"
        assert not values_a & values_b, f"{name}: shared between two pipelines"


def test_env_sets_intersect_only_on_cluster_wide_values(provider: KubernetesFlinkProvider) -> None:
    """Everything two agent instances have in common must be infrastructure, not customer data."""
    a = _render(provider, _config(PIPELINE_A, "Customer A"))
    b = _render(provider, _config(PIPELINE_B, "Customer B"))

    shared_names = {name for name, _ in _env_pairs(a) & _env_pairs(b)}
    assert shared_names == {
        "KAFKA_ACTIVITY_TOPIC",  # cross-tenant health channel
        "STATE_BACKEND",
        "CHECKPOINT_PATH",  # same path, different subPath — §22
        "FLINK_CHECKPOINT_INTERVAL_MS",
        "FLINK_PARALLELISM",
        "FLINK_UNALIGNED_CHECKPOINTS",
    }


def test_pipeline_identity_env_present_and_no_input_topics(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))

    assert [e["value"] for e in _envs_by_name(manifest, "CORRELATION_PIPELINE_ID")] == [str(PIPELINE_A)] * 2
    assert [e["value"] for e in _envs_by_name(manifest, "CORRELATION_PIPELINE_NAME")] == ["Customer A"] * 2
    assert _envs_by_name(manifest, "KAFKA_INPUT_TOPICS") == []


def test_jobmanager_cpu_is_templated(provider: KubernetesFlinkProvider) -> None:
    fc = _render(provider, _config(PIPELINE_A, "Customer A"))["spec"]["flinkConfiguration"]

    assert fc["jobmanager.cpu.cores"] == "1.0"
    assert fc["taskmanager.memory.process.size"] == "3072m"  # settings default 3g


def test_envfrom_reuses_shared_flink_config_and_mounts_agent_llm(provider: KubernetesFlinkProvider) -> None:
    """Shared Kafka infra + agent-only LLM. Order matters: shared first → LLM overlays."""
    refs = _container_envfrom(_render(provider, _config(PIPELINE_A, "Customer A")))

    configmap_refs = [r.get("configMapRef", {}).get("name") for r in refs if "configMapRef" in r]
    secret_refs = [r.get("secretRef", {}).get("name") for r in refs if "secretRef" in r]

    # JM + TM each have 2 configMapRefs and 2 secretRefs → 4 of each total.
    assert configmap_refs == [
        "flink-config",
        "detectflow-agent-config",
        "flink-config",
        "detectflow-agent-config",
    ]
    assert secret_refs == [
        "flink-sigma-config",
        "detectflow-agent-secrets",
        "flink-sigma-config",
        "detectflow-agent-secrets",
    ]


def test_legacy_deployment_renders_without_pipeline_identity(provider: KubernetesFlinkProvider) -> None:
    """No pipeline id ⇒ no per-pipeline labels and no identity env (pre-epic behaviour)."""
    manifest = _render(provider, _legacy_config())

    assert manifest["metadata"]["name"] == "agent-job"
    for labels in [manifest["metadata"]["labels"], *_pod_labels(manifest)]:
        assert "detectflow.io/correlation-pipeline-id" not in labels
        assert "detectflow.io/job-role" not in labels
    assert _envs_by_name(manifest, "CORRELATION_PIPELINE_ID") == []
    assert [e["value"] for e in _envs_by_name(manifest, "KAFKA_CHAINS_RAW_TOPIC")] == ["detectflow.chains-raw"] * 2


def test_jobmanager_cpu_is_wired_from_the_row(provider: KubernetesFlinkProvider) -> None:
    """JobManager CPU is a resource like any other: rendered, never left to a Flink default.

    Ported from the pre-epic hand-written var dict: the value now comes from the row's
    ``agent_jobmanager_cpu`` column (NULL inherits ``settings.agent_job_jobmanager_cpu``), so
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
    config.agent_jobmanager_cpu = 0.25
    config.agent_taskmanager_cpu = 0.25

    manifest = _render(provider, config)

    assert manifest["spec"]["jobManager"]["resource"]["cpu"] == 0.25
    assert "kubernetes.jobmanager.cpu" not in manifest["spec"]["flinkConfiguration"]
    assert manifest["spec"]["flinkConfiguration"]["taskmanager.cpu.cores"] == "0.25"


def _node_selectors(manifest: dict) -> list[dict]:
    selectors: list[dict] = []
    for tier in ("jobManager", "taskManager"):
        selector = manifest["spec"][tier]["podTemplate"]["spec"].get("nodeSelector")
        if selector:
            selectors.append(selector)
    return selectors


def test_node_selector_defaults_to_flink_pool(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))  # no agent-specific override

    selectors = _node_selectors(manifest)
    assert len(selectors) == 2  # JM + TM
    expected = {settings.flink_node_selector_key: settings.flink_node_selector_value}
    assert all(s == expected for s in selectors)


def test_node_selector_overrides_when_agent_specific_pool_set(provider: KubernetesFlinkProvider) -> None:
    # Architecture §1 + §9: thread-mode TMs may need a dedicated pool to
    # avoid sharing with process-mode TMs. Override must beat the default.
    manifest = _render(
        provider,
        _config(PIPELINE_A, "Customer A"),
        agent_node_selector_key="app",
        agent_node_selector_value="detectflow-agent",
    )

    selectors = _node_selectors(manifest)
    assert len(selectors) == 2
    assert all(s == {"app": "detectflow-agent"} for s in selectors)


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


def test_llm_ca_cert_mount_derives_paths_from_ssl_cert_file(provider: KubernetesFlinkProvider) -> None:
    """Self-signed LLM endpoint: CA bundle mounted at dirname(SSL_CERT_FILE).

    Nothing about the location is hardcoded — mountPath, Secret key and the
    inlined ``SSL_CERT_FILE`` env all come from the single configured path, so
    the file httpx opens is exactly the file the Secret projects.
    """
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"), ssl_cert_file="/etc/ssl/agent-ca/ca.pem")

    mounts = [m for m in _container_volume_mounts(manifest) if m["name"] == "agent-ca-cert"]
    assert len(mounts) == 2  # JM + TM main containers
    assert all(m["mountPath"] == "/etc/ssl/agent-ca" for m in mounts)
    assert all(m["readOnly"] is True for m in mounts)

    volumes = [v for v in _pod_volumes(manifest) if v["name"] == "agent-ca-cert"]
    assert len(volumes) == 2
    for volume in volumes:
        # Secret name comes from DETECTFLOW_AGENT_SECRET_NAME; only the CA key
        # is projected so LLM_API_KEY never lands on the filesystem.
        assert volume["secret"]["secretName"] == "detectflow-agent-secrets"
        assert volume["secret"]["items"] == [{"key": "ca.pem", "path": "ca.pem"}]
        # Path known for certain → a missing key must fail loudly, not silently.
        assert "optional" not in volume["secret"]

    cert_envs = _envs_by_name(manifest, "SSL_CERT_FILE")
    assert len(cert_envs) == 2
    assert all(e["value"] == "/etc/ssl/agent-ca/ca.pem" for e in cert_envs)


def test_llm_ca_cert_mount_omitted_when_ssl_cert_file_unset(provider: KubernetesFlinkProvider) -> None:
    manifest = _render(provider, _config(PIPELINE_A, "Customer A"))

    assert not [m for m in _container_volume_mounts(manifest) if m["name"] == "agent-ca-cert"]
    assert not [v for v in _pod_volumes(manifest) if v["name"] == "agent-ca-cert"]
    assert not _envs_by_name(manifest, "SSL_CERT_FILE")


def test_derive_agent_ca_mount_splits_absolute_paths() -> None:
    assert KubernetesFlinkProvider._derive_agent_ca_mount("/etc/ssl/agent-ca/ca.pem") == (
        "/etc/ssl/agent-ca",
        "ca.pem",
    )
    assert KubernetesFlinkProvider._derive_agent_ca_mount("  /opt/certs/vllm.cert  ") == (
        "/opt/certs",
        "vllm.cert",
    )


def test_derive_agent_ca_mount_rejects_unmountable_paths() -> None:
    """Bad paths degrade to "no CA volume" instead of producing a dangerous mount."""
    assert KubernetesFlinkProvider._derive_agent_ca_mount(None) is None
    assert KubernetesFlinkProvider._derive_agent_ca_mount("   ") is None
    # Relative — a Secret volume needs an absolute mountPath.
    assert KubernetesFlinkProvider._derive_agent_ca_mount("certs/ca.pem") is None
    # Directly under / — mounting there would shadow the container root.
    assert KubernetesFlinkProvider._derive_agent_ca_mount("/ca.pem") is None
    assert KubernetesFlinkProvider._derive_agent_ca_mount("/") is None


def _provider_reading(config_map_data: dict | None = None, *, error: Exception | None = None) -> object:
    """A provider whose ConfigMap read is stubbed — no cluster, no kubeconfig."""

    def _read(*, name: str, namespace: str) -> SimpleNamespace:
        if error is not None:
            raise error
        return SimpleNamespace(data=config_map_data)

    provider = object.__new__(KubernetesFlinkProvider)
    provider.namespace = "security"
    provider.core_api = SimpleNamespace(read_namespaced_config_map=_read)
    return provider


def test_ca_mount_read_from_the_agent_configmap() -> None:
    """The path the container reads via envFrom is the path the mount is built from."""
    provider = _provider_reading({"SSL_CERT_FILE": " /etc/ssl/agent-ca/ca.pem "})

    assert provider._agent_ca_mount() == ("/etc/ssl/agent-ca", "ca.pem", True)


def test_readable_configmap_without_ssl_cert_file_means_no_mount() -> None:
    # Definitive "this LLM needs no private CA" — not a reason to guess.
    provider = _provider_reading({"LLM_MODEL": "gpt-4o-mini"})

    assert provider._agent_ca_mount() is None


def test_backend_override_wins_without_touching_the_api() -> None:
    provider = _provider_reading(error=AssertionError("ConfigMap must not be read"))

    with patch.object(settings, "detectflow_agent_ssl_cert_file", "/opt/certs/vllm.pem"):
        assert provider._agent_ca_mount() == ("/opt/certs", "vllm.pem", True)


def test_unreadable_configmap_falls_back_to_the_conventional_path() -> None:
    """RBAC denial is the common case — mount the convention, flagged non-authoritative.

    The caller turns that flag into `optional: true` + no inlined SSL_CERT_FILE,
    so a cluster with no private CA is unaffected while one that follows the
    convention gets its bundle without granting the backend anything.
    """
    provider = _provider_reading(error=ApiException(status=403, reason="Forbidden"))

    assert provider._agent_ca_mount() == ("/etc/ssl/agent-ca", "ca.pem", False)


def test_configmap_transport_error_falls_back_the_same_way() -> None:
    provider = _provider_reading(error=OSError("connection reset"))

    assert provider._agent_ca_mount() == ("/etc/ssl/agent-ca", "ca.pem", False)


def test_fallback_can_be_disabled() -> None:
    provider = _provider_reading(error=ApiException(status=403, reason="Forbidden"))

    with patch.object(settings, "detectflow_agent_ssl_cert_file_default", ""):
        assert provider._agent_ca_mount() is None


def test_blind_fallback_mounts_optionally_and_leaves_ssl_cert_file_to_the_configmap(
    provider: KubernetesFlinkProvider,
) -> None:
    """Blind mount must not claim the path: no inlined env, missing key tolerated."""
    manifest = _render(
        provider,
        _config(PIPELINE_A, "Customer A"),
        ssl_cert_file="/etc/ssl/agent-ca/ca.pem",
        ca_authoritative=False,
    )

    mounts = [m for m in _container_volume_mounts(manifest) if m["name"] == "agent-ca-cert"]
    assert len(mounts) == 2
    assert all(m["mountPath"] == "/etc/ssl/agent-ca" for m in mounts)

    volumes = [v for v in _pod_volumes(manifest) if v["name"] == "agent-ca-cert"]
    assert len(volumes) == 2
    assert all(v["secret"]["optional"] is True for v in volumes)

    # The container keeps whatever SSL_CERT_FILE the agent ConfigMap gives it.
    assert not _envs_by_name(manifest, "SSL_CERT_FILE")
