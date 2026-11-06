"""Unit tests for the PyFlink TaskManager JVM/pod memory split (K8s provider).

Pins the contract behind ``FLINK_TASKMANAGER_PROCESS_MEMORY_*`` /
``FLINK_TASKMANAGER_PROCESS_NATIVE_HEADROOM_MIN_MB`` /
``FLINK_TASKMANAGER_NATIVE_HEADROOM_MIN_MB`` that ``KubernetesFlinkProvider``
renders into ``flink-deployment.yaml.j2`` as ``taskmanager.memory.process.size``
(JVM) and ``kubernetes.taskmanager.memory.limit-factor`` (pod limit = factor x JVM):

- The configured TaskManager memory is the pod limit (not silently increased for
  Python headroom). Grow only when the value is below the JVM floor.
- Process mode: prefer ``process_native_headroom_min_mb`` outside the JVM; if that
  does not fit, compress headroom inside the configured total.
- Thread mode: nearly all configured memory goes to ``process.size``; only
  ``native_headroom_min_mb`` stays outside for off-heap in the same cgroup.
"""

import pytest

from apps.core.settings import settings
from apps.providers.kubernetes import KubernetesFlinkProvider


@pytest.fixture
def default_split(monkeypatch):
    """Force the documented default split regardless of env."""
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_base_mb", 1536)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_growth_fraction", 0.1)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_min_mb", 1280)
    monkeypatch.setattr(settings, "flink_taskmanager_process_native_headroom_min_mb", 1536)
    monkeypatch.setattr(settings, "flink_taskmanager_native_headroom_min_mb", 512)
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "process")


@pytest.mark.parametrize(
    ("user_mb", "expected_jvm_mb"),
    [
        (512, 512),  # below JVM floor but cannot exceed configured pod total
        (1024, 1024),  # below floor but cannot exceed configured pod total
        (1280, 1280),  # at the JVM floor
        (1536, 1280),  # headroom does not fit → compress; JVM at floor
        (2048, 1280),  # prefer 1536 headroom but that leaves 512 → floor 1280; pod stays 2048
        (2560, 1280),  # prefer 1536 headroom leaves 1024 → floor 1280 (was 1024 → factor 2.5)
        (3072, 1536),  # full preferred headroom fits
        (4096, 1792),  # 1536 + round(2560 * 0.1)
        (8192, 2202),  # 1536 + round(6656 * 0.1)
    ],
)
def test_process_memory_split(default_split, user_mb, expected_jvm_mb):
    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(user_mb) == expected_jvm_mb


@pytest.mark.parametrize(
    ("user_mb", "expected_factor"),
    [
        (2048, "1.6"),  # 2048 / 1280 — pod equals configured total
        (2560, "2.0"),  # 2560 / 1280
        (3072, "2.0"),  # 3072 / 1536
        (4096, "2.286"),  # 4096 / 1792
        (8192, "3.72"),  # 8192 / 2202
    ],
)
def test_memory_limit_factor(default_split, user_mb, expected_factor):
    assert KubernetesFlinkProvider._calculate_taskmanager_memory_limit_factor(user_mb) == expected_factor


@pytest.mark.parametrize("user_mb", [1536, 2048, 3072, 4096, 8192, 16384])
def test_pod_equals_configured_total(default_split, user_mb):
    """Configured memory is the pod limit — never silently inflated for Python headroom."""
    jvm_mb = KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(user_mb)
    pod_mb = KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(user_mb)
    factor = float(KubernetesFlinkProvider._calculate_taskmanager_memory_limit_factor(user_mb))

    assert pod_mb == user_mb
    assert jvm_mb >= settings.flink_taskmanager_process_memory_min_mb or jvm_mb == user_mb
    assert jvm_mb <= pod_mb
    assert factor >= 1.0
    assert jvm_mb * factor == pytest.approx(pod_mb, abs=jvm_mb * 0.001 + 1)


def test_preferred_headroom_applies_when_it_fits(default_split):
    """At 3072 the preferred 1536 Python reserve fits beside a 1536 JVM."""
    jvm_mb = KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(3072)
    pod_mb = KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(3072)

    assert jvm_mb == 1536
    assert pod_mb - jvm_mb == settings.flink_taskmanager_process_native_headroom_min_mb


def test_headroom_is_compressed_when_preferred_does_not_fit(default_split):
    """At 2048, preferred headroom 1536 does not fit beside the 1280 JVM floor — compress."""
    jvm_mb = KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(2048)
    pod_mb = KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(2048)

    assert pod_mb == 2048
    assert jvm_mb == 1280
    assert pod_mb - jvm_mb == 768  # compressed Python room


def test_sub_floor_value_grows_pod_only_to_cover_jvm_request(default_split):
    """Below the JVM floor the pod grows to the request size, not to JVM+headroom."""
    jvm_mb = KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(512)
    pod_mb = KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(512)

    assert jvm_mb == 512
    assert pod_mb == 512


def test_split_is_settings_driven(monkeypatch):
    """Changing the settings knobs changes the computed split."""
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_base_mb", 2048)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_growth_fraction", 0.5)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_min_mb", 1024)
    monkeypatch.setattr(settings, "flink_taskmanager_process_native_headroom_min_mb", 512)
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "process")

    # At/below the (new) base -> as-is, minus the headroom clamp.
    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(2048) == 1536
    # Above: 2048 + round((4096 - 2048) * 0.5) = 3072.
    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(4096) == 3072
    assert KubernetesFlinkProvider._calculate_taskmanager_memory_limit_factor(4096) == "1.333"
    assert KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(4096) == 4096


def test_process_headroom_knob_is_settings_driven(monkeypatch):
    """A larger process native reserve clamps the JVM when growth would exceed it."""
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_base_mb", 1536)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_growth_fraction", 0.5)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_min_mb", 1024)
    monkeypatch.setattr(settings, "flink_taskmanager_process_native_headroom_min_mb", 2048)
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "process")

    # Growth alone: 1536 + round(2560 * 0.5) = 2816; clamp to 4096 - 2048 = 2048.
    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(4096) == 2048
    assert KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(4096) == 4096


def test_zero_growth_fraction_pins_jvm_to_base(monkeypatch):
    """growth_fraction=0 keeps the JVM at the base and gives all extra memory to Python."""
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_base_mb", 1536)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_growth_fraction", 0.0)
    monkeypatch.setattr(settings, "flink_taskmanager_process_memory_min_mb", 1024)
    monkeypatch.setattr(settings, "flink_taskmanager_process_native_headroom_min_mb", 1536)
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "process")

    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(8192) == 1536
    assert KubernetesFlinkProvider._calculate_taskmanager_memory_limit_factor(8192) == "5.333"


def test_thread_mode_gives_jvm_nearly_all_configured_memory(default_split, monkeypatch):
    """Thread/Pemja: Python shares the JVM — process.size = total - thread headroom."""
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "thread")

    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(3072) == 2560
    assert KubernetesFlinkProvider._calculate_taskmanager_memory_limit_factor(3072) == "1.2"
    assert KubernetesFlinkProvider._calculate_taskmanager_pod_memory_mb(3072) == 3072


def test_thread_mode_explicit_arg_overrides_settings(default_split, monkeypatch):
    """Call-site mode wins over MATCHER_PYTHON_EXECUTION_MODE."""
    monkeypatch.setattr(settings, "matcher_python_execution_mode", "process")

    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(3072, "thread") == 2560
    # Default (settings=process) uses the process headroom clamp.
    assert KubernetesFlinkProvider._calculate_taskmanager_process_memory_mb(3072) == 1536
