"""Matcher FlinkDeployment companions for PYTHON_EXECUTION_MODE × STATE_BACKEND.

Single place to read/edit the knob matrix used by:
- ``flink-deployment.yaml.j2`` / ``cmf-application.json.j2``
- TaskManager JVM/pod memory split (thread vs process)

Keep values aligned with the matcher repo ``app/jobs/runtime_tuning.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

PythonMode = Literal["process", "thread"]
StateBackendEnv = Literal["rocksdb", "memory"]

DEFAULT_PYTHON_MODE: PythonMode = "process"
DEFAULT_STATE_BACKEND: StateBackendEnv = "rocksdb"


# --- Python execution mode ---------------------------------------------------
# process: separate Python workers; Flink default overhead floor is enough.
# thread:  Pemja embeds CPython+Polars in the JVM — raise overhead.
JVM_OVERHEAD_MIN_PROCESS = "192m"
JVM_OVERHEAD_MIN_THREAD = "512m"

# --- State backend -----------------------------------------------------------
# rocksdb: managed memory for block cache / write buffers; incremental CPs.
# memory:  hashmap ListState on-heap; full CPs; shrink network → more heap.
MANAGED_FRACTION_ROCKSDB = "0.2"
MANAGED_FRACTION_HASHMAP = "0.0"
NETWORK_FRACTION_HASHMAP = "0.05"
NETWORK_MAX_HASHMAP = "64m"
ROCKSDB_COMPACTION_STYLE = "LEVEL"
ROCKSDB_THREAD_NUM = "2"


@dataclass(frozen=True)
class MatcherRuntimeTuning:
    """Resolved runtime profile passed into Jinja as ``runtime``."""

    python_execution_mode: PythonMode
    state_backend: StateBackendEnv  # job env STATE_BACKEND (memory | rocksdb)
    # FlinkConfiguration keys that depend on the two switches (quoted string values).
    flink_conf: dict[str, str]

    @property
    def flink_state_backend_type(self) -> str:
        """Value of ``state.backend.type`` (rocksdb | hashmap)."""
        return self.flink_conf["state.backend.type"]


def resolve_matcher_runtime(
    python_execution_mode: str | None = None,
    state_backend: str | None = None,
) -> MatcherRuntimeTuning:
    """Build the companion profile for the given switches (defaults = production)."""
    mode: PythonMode = "thread" if python_execution_mode == "thread" else DEFAULT_PYTHON_MODE
    backend: StateBackendEnv = "memory" if state_backend == "memory" else DEFAULT_STATE_BACKEND

    flink_conf: dict[str, str] = {
        "python.fn-execution.memory.managed": "false",
        "taskmanager.memory.jvm-overhead.min": (
            JVM_OVERHEAD_MIN_THREAD if mode == "thread" else JVM_OVERHEAD_MIN_PROCESS
        ),
    }

    if backend == "rocksdb":
        flink_conf.update(
            {
                "state.backend.type": "rocksdb",
                "execution.checkpointing.incremental": "true",
                "state.backend.rocksdb.compaction.style": ROCKSDB_COMPACTION_STYLE,
                "state.backend.rocksdb.thread.num": ROCKSDB_THREAD_NUM,
                "taskmanager.memory.managed.fraction": MANAGED_FRACTION_ROCKSDB,
            }
        )
    else:
        flink_conf.update(
            {
                "state.backend.type": "hashmap",
                "execution.checkpointing.incremental": "false",
                "taskmanager.memory.managed.fraction": MANAGED_FRACTION_HASHMAP,
                "taskmanager.memory.network.fraction": NETWORK_FRACTION_HASHMAP,
                "taskmanager.memory.network.max": NETWORK_MAX_HASHMAP,
            }
        )

    return MatcherRuntimeTuning(
        python_execution_mode=mode,
        state_backend=backend,
        flink_conf=flink_conf,
    )
