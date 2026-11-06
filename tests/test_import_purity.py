"""Import-purity guard: importing the app must not require infrastructure.

``tests/conftest.py`` imports ``server.app`` at collection time, the Docker entrypoint imports
it on every deploy, and CI runners have no cluster access — so importing ``server`` (and the
manager chain underneath) must never construct Kubernetes/Kafka/DB clients as an import side
effect. Module-level singletons are fine; eager resource clients in their ``__init__`` are not
(resolve them lazily on first use — see ``FlinkMetricsService._flink``).

Runs the import in a subprocess with ``HOME``/``KUBECONFIG`` isolated, so the guard bites on
developer machines that do have ``~/.kube/config`` — not only in CI.
"""

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def test_importing_server_requires_no_cluster_access(tmp_path: Path) -> None:
    env = {
        **os.environ,
        "HOME": str(tmp_path),  # hides ~/.kube/config
        "KUBECONFIG": str(tmp_path / "absent"),  # and any externally set kubeconfig
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost:5432/test_db",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    }
    result = subprocess.run(
        [sys.executable, "-c", "import server"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"importing server must need no kubeconfig/infra, got:\n{result.stderr}"
