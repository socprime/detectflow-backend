"""Pin the MATCHER_PYTHON_EXECUTION_MODE × MATCHER_STATE_BACKEND knob matrix."""

from apps.providers.matcher_runtime_tuning import (
    JVM_OVERHEAD_MIN_PROCESS,
    JVM_OVERHEAD_MIN_THREAD,
    MANAGED_FRACTION_HASHMAP,
    MANAGED_FRACTION_ROCKSDB,
    resolve_matcher_runtime,
)


def test_default_is_process_rocksdb() -> None:
    runtime = resolve_matcher_runtime()
    assert runtime.python_execution_mode == "process"
    assert runtime.state_backend == "rocksdb"
    assert runtime.flink_state_backend_type == "rocksdb"
    assert runtime.flink_conf["execution.checkpointing.incremental"] == "true"
    assert runtime.flink_conf["taskmanager.memory.managed.fraction"] == MANAGED_FRACTION_ROCKSDB
    assert runtime.flink_conf["taskmanager.memory.jvm-overhead.min"] == JVM_OVERHEAD_MIN_PROCESS
    assert "taskmanager.memory.network.fraction" not in runtime.flink_conf


def test_hashmap_companions() -> None:
    runtime = resolve_matcher_runtime(state_backend="memory")
    assert runtime.flink_state_backend_type == "hashmap"
    assert runtime.flink_conf["execution.checkpointing.incremental"] == "false"
    assert runtime.flink_conf["taskmanager.memory.managed.fraction"] == MANAGED_FRACTION_HASHMAP
    assert runtime.flink_conf["taskmanager.memory.network.fraction"] == "0.05"
    assert runtime.flink_conf["taskmanager.memory.network.max"] == "64m"
    assert "state.backend.rocksdb.compaction.style" not in runtime.flink_conf


def test_thread_companions() -> None:
    runtime = resolve_matcher_runtime(python_execution_mode="thread")
    assert runtime.python_execution_mode == "thread"
    assert runtime.flink_conf["taskmanager.memory.jvm-overhead.min"] == JVM_OVERHEAD_MIN_THREAD


def test_thread_hashmap_matrix() -> None:
    """Full matrix cell used when A/B-ing both switches."""
    runtime = resolve_matcher_runtime(python_execution_mode="thread", state_backend="memory")
    assert runtime.flink_conf == {
        "python.fn-execution.memory.managed": "false",
        "taskmanager.memory.jvm-overhead.min": JVM_OVERHEAD_MIN_THREAD,
        "state.backend.type": "hashmap",
        "execution.checkpointing.incremental": "false",
        "taskmanager.memory.managed.fraction": MANAGED_FRACTION_HASHMAP,
        "taskmanager.memory.network.fraction": "0.05",
        "taskmanager.memory.network.max": "64m",
    }


def test_unknown_values_fall_back_to_defaults() -> None:
    runtime = resolve_matcher_runtime(python_execution_mode="weird", state_backend="weird")
    assert runtime.python_execution_mode == "process"
    assert runtime.state_backend == "rocksdb"
