"""Unit tests for KubernetesFlinkProvider deployment-storage cleanup.

Runtime contract (HoS 2026-05-27): ``stop_runtime`` must wipe the correlation-job AND agent-job
checkpoint / HA / savepoint prefixes so the next start cold-starts
(``KafkaOffsetsInitializer.latest()`` only applies when there is no restored source state).

These tests pin down the K8s Job spec that ``_cleanup_deployment_storage`` creates:
- mounts the three shared PVCs at PARENT level — no ``subPath``, because a scoped mount can only
  empty a directory and this Job has to remove it (architecture §22),
- passes the state key as ``STATE_SUBPATH`` env and never interpolates it into the shell body,
- rejects any key that is not a single DNS-1123 label *before* a Job object exists,
- has a DNS-1123 Job name that fits the 63-char Kubernetes Job limit,
- swallows 409 from ``BatchV1Api.create_namespaced_job`` (back-to-back stops within
  ``ttlSecondsAfterFinished``),
- re-raises any other ``ApiException``.

What the script itself does with that key is executed for real in
``tests/test_cleanup_guard_script.py``.

(The filename keeps its pre-epic "singleton" spelling so ``git log --follow`` stays intact.)

We patch ``BatchV1Api`` rather than the K8s client at module load time so
``KubernetesFlinkProvider.__init__`` can stay untouched.
"""

import re
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest
from kubernetes.client.rest import ApiException

from apps.core.constants import agent_deployment_name, correlation_deployment_name
from apps.core.exceptions import ConfigurationError
from apps.core.settings import settings
from apps.providers.kubernetes import KubernetesFlinkProvider

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


def _captured_job_body(batch_api_mock: MagicMock) -> dict:
    """Pull the Job manifest passed to ``create_namespaced_job``."""
    batch_api_mock.create_namespaced_job.assert_called_once()
    call_kwargs = batch_api_mock.create_namespaced_job.call_args.kwargs
    return call_kwargs["body"]


def _succeeding_batch_api() -> MagicMock:
    """A BatchV1Api whose Job reports one successful pod on the first status read."""
    batch_api = MagicMock()
    batch_api.read_namespaced_job_status.return_value = MagicMock(
        status=MagicMock(succeeded=1, failed=None, conditions=[])
    )
    return batch_api


def _cleanup(provider: KubernetesFlinkProvider, deployment_name: str) -> dict:
    batch_api = _succeeding_batch_api()
    with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
        assert provider._cleanup_deployment_storage(deployment_name).ok
    return _captured_job_body(batch_api)


# DNS-1123 label: lowercase alphanumeric or '-', start/end with alphanumeric, ≤63 chars.
_DNS_1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def _state_subpath_env(body: dict) -> str:
    container = body["spec"]["template"]["spec"]["containers"][0]
    return next(e["value"] for e in container["env"] if e["name"] == "STATE_SUBPATH")


class TestCleanupJobSpec:
    def test_creates_job_with_three_parent_level_pvc_mounts(self, provider: KubernetesFlinkProvider) -> None:
        deployment_name = correlation_deployment_name(PIPELINE_A)
        body = _cleanup(provider, deployment_name)

        assert body["metadata"]["name"] == f"cleanup-{deployment_name}"
        # ttlSecondsAfterFinished gates back-to-back stops to the 409 swallow.
        assert body["spec"]["ttlSecondsAfterFinished"] == 300

        spec = body["spec"]["template"]["spec"]

        # Three PVC volumes, each pointing at the shared state PVC.
        volumes = {v["name"]: v["persistentVolumeClaim"]["claimName"] for v in spec["volumes"]}
        assert volumes == {
            "checkpoints": settings.flink_checkpoints_pvc,
            "ha": settings.flink_ha_pvc,
            "savepoints": settings.flink_savepoints_pvc,
        }

        container = spec["containers"][0]
        assert container["image"] == "busybox:latest"
        mounts = {m["name"]: m["mountPath"] for m in container["volumeMounts"]}
        assert mounts == {
            "checkpoints": "/mnt/checkpoints",
            "ha": "/mnt/ha",
            "savepoints": "/mnt/savepoints",
        }
        # PARENT level on purpose (§22): a subPath mount could only empty the directory, and the
        # directory itself has to go. Isolation is the guard in the script, not the mount.
        assert all("subPath" not in m for m in container["volumeMounts"])

    def test_state_key_travels_as_env_and_never_enters_the_shell_body(self, provider: KubernetesFlinkProvider) -> None:
        """Nothing destructive is shell-interpolated — the key is data, not code."""
        deployment_name = correlation_deployment_name(PIPELINE_A)
        body = _cleanup(provider, deployment_name)

        assert _state_subpath_env(body) == deployment_name
        script = body["spec"]["template"]["spec"]["containers"][0]["command"][-1]
        assert deployment_name not in script
        assert 'rm -rf "$target"' in script
        # `|| true` made every refusal look like a success; the guard has to fail closed.
        assert "|| true" not in script

    def test_two_pipelines_cleanups_are_disjoint(self, provider: KubernetesFlinkProvider) -> None:
        """One customer's cleanup must not name another customer's state key."""
        a = _cleanup(provider, correlation_deployment_name(PIPELINE_A))
        b = _cleanup(provider, correlation_deployment_name(PIPELINE_B))

        assert a["metadata"]["name"] != b["metadata"]["name"]
        assert _state_subpath_env(a) == correlation_deployment_name(PIPELINE_A)
        assert _state_subpath_env(b) == correlation_deployment_name(PIPELINE_B)
        assert str(PIPELINE_A) not in str(b)
        assert str(PIPELINE_B) not in str(a)

    def test_correlation_and_agent_of_one_pipeline_are_disjoint(self, provider: KubernetesFlinkProvider) -> None:
        correlation = _cleanup(provider, correlation_deployment_name(PIPELINE_A))
        agent = _cleanup(provider, agent_deployment_name(PIPELINE_A))

        assert correlation["metadata"]["name"] != agent["metadata"]["name"]
        assert _state_subpath_env(correlation) != _state_subpath_env(agent)

    @pytest.mark.parametrize("bad", ["", ".", "..", "a/b", "../../etc", "corr-A", "-corr", "x" * 64])
    def test_invalid_state_key_raises_before_any_job_is_created(
        self, provider: KubernetesFlinkProvider, bad: str
    ) -> None:
        """A bad key is another customer's checkpoints — fail closed, and fail early."""
        batch_api = MagicMock()
        with (
            patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api),
            pytest.raises(ConfigurationError),
        ):
            provider._cleanup_deployment_storage(bad)

        batch_api.create_namespaced_job.assert_not_called()

    def test_detection_pipeline_cleanup_keys_on_the_bare_pipeline_id(self, provider: KubernetesFlinkProvider) -> None:
        """Not ``flink-{pipeline_id}``: that would name a directory no pipeline owns."""
        batch_api = _succeeding_batch_api()
        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            assert provider.cleanup_pipeline_storage(str(PIPELINE_A)).ok

        body = _captured_job_body(batch_api)
        assert body["metadata"]["name"] == f"cleanup-{PIPELINE_A}"
        assert _state_subpath_env(body) == str(PIPELINE_A)

    @pytest.mark.parametrize("builder", [correlation_deployment_name, agent_deployment_name])
    def test_job_name_is_dns_1123(self, builder) -> None:
        # Pins the convention so a future prefix change ("corr-" → something longer) surfaces
        # immediately rather than as a rejected Job at stop time.
        assert _DNS_1123_LABEL.match(f"cleanup-{builder(PIPELINE_A)}")

    def test_409_is_swallowed(self, provider: KubernetesFlinkProvider) -> None:
        batch_api = MagicMock()
        batch_api.create_namespaced_job.side_effect = ApiException(status=409)
        batch_api.read_namespaced_job_status.return_value = MagicMock(
            status=MagicMock(succeeded=1, failed=None, conditions=[])
        )
        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            # Swallowed, and then the EXISTING Job is watched rather than assumed successful.
            assert provider._cleanup_deployment_storage(correlation_deployment_name(PIPELINE_A)).ok
        batch_api.read_namespaced_job_status.assert_called()

    def test_non_409_api_exception_re_raises(self, provider: KubernetesFlinkProvider) -> None:
        batch_api = MagicMock()
        batch_api.create_namespaced_job.side_effect = ApiException(status=500)
        with (
            patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api),
            pytest.raises(Exception, match="Failed to create deployment cleanup job"),
        ):
            provider._cleanup_deployment_storage(correlation_deployment_name(PIPELINE_A))

    def test_pvc_names_come_from_settings(self, provider: KubernetesFlinkProvider) -> None:
        body = _cleanup(provider, "custom-deployment")

        assert body["metadata"]["name"] == "cleanup-custom-deployment"
        spec = body["spec"]["template"]["spec"]
        volumes = {v["name"]: v["persistentVolumeClaim"]["claimName"] for v in spec["volumes"]}
        # PVC names come from settings, NOT hardcoded — matches the FlinkDeployment templates.
        assert volumes["checkpoints"] == settings.flink_checkpoints_pvc
        assert volumes["ha"] == settings.flink_ha_pvc
        assert volumes["savepoints"] == settings.flink_savepoints_pvc

        assert _state_subpath_env(body) == "custom-deployment"


class TestCleanupJobOutcomeIsWatched:
    """ "Created" is not "cleaned".

    The Job used to end in ``|| true`` and could not fail, so returning once the API accepted the
    object was a fair proxy. §22 made the script fail closed — a bad state key, a symlinked target
    or a non-directory each exit non-zero — and a refusal means the state SURVIVED. Reporting that
    as success sends the next start into checkpoints the operator believes are gone; for
    correlation-job that silently breaks the "no replay while paused" runtime contract.
    """

    @staticmethod
    def _status(*, succeeded=None, failed=None, conditions=None) -> MagicMock:
        return MagicMock(status=MagicMock(succeeded=succeeded, failed=failed, conditions=conditions or []))

    def test_a_succeeded_job_reports_succeeded(self, provider: KubernetesFlinkProvider) -> None:
        batch_api = MagicMock()
        batch_api.read_namespaced_job_status.return_value = self._status(succeeded=1)

        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            outcome = provider._cleanup_deployment_storage("corr-x")

        assert (outcome.status, outcome.ok) == ("succeeded", True)

    def test_a_refusal_is_reported_with_the_reason_the_script_printed(self, provider: KubernetesFlinkProvider) -> None:
        batch_api = MagicMock()
        batch_api.read_namespaced_job_status.return_value = self._status(failed=1)
        pod = MagicMock()
        pod.metadata.name = "cleanup-corr-x-abcde"
        provider.core_api.list_namespaced_pod.return_value = MagicMock(items=[pod])
        provider.core_api.read_namespaced_pod_log.return_value = "refusing: '/mnt/ha/corr-x' is a symlink\n"

        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            outcome = provider._cleanup_deployment_storage("corr-x")

        assert outcome.status == "failed"
        assert outcome.ok is False
        assert "is a symlink" in outcome.reason
        # The caller renders this straight into a stop status / activity entry.
        assert outcome.describe().startswith("failed: refusing")

    def test_a_refusal_falls_back_to_the_job_condition_when_logs_are_unreadable(
        self, provider: KubernetesFlinkProvider
    ) -> None:
        batch_api = MagicMock()
        condition = MagicMock(type="Failed", message="BackoffLimitExceeded")
        batch_api.read_namespaced_job_status.return_value = self._status(failed=1, conditions=[condition])
        provider.core_api.list_namespaced_pod.side_effect = ApiException(status=403)

        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            outcome = provider._cleanup_deployment_storage("corr-x")

        assert outcome.reason == "BackoffLimitExceeded"

    def test_a_job_that_never_finishes_times_out_instead_of_hanging(
        self, provider: KubernetesFlinkProvider, monkeypatch
    ) -> None:
        """Bounded by settings, and never fatal: the state simply survives, visibly."""
        monkeypatch.setattr(settings, "flink_cleanup_job_timeout_seconds", 0)
        batch_api = MagicMock()
        batch_api.read_namespaced_job_status.return_value = self._status()

        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            outcome = provider._cleanup_deployment_storage("corr-x")

        assert outcome.status == "timed_out"

    def test_a_job_reaped_by_its_ttl_counts_as_completed(self, provider: KubernetesFlinkProvider) -> None:
        """``ttlSecondsAfterFinished`` only reaps a Job that finished — there is nothing left to read."""
        batch_api = MagicMock()
        batch_api.read_namespaced_job_status.side_effect = ApiException(status=404)

        with patch("apps.providers.kubernetes.client.BatchV1Api", return_value=batch_api):
            outcome = provider._cleanup_deployment_storage("corr-x")

        assert outcome.status == "succeeded"
