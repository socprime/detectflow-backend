"""Execute the storage-cleanup guard for real, under a real ``sh``.

Every other test asserts the *shape* of the cleanup Job; this one runs the script that Job
actually executes, because the guard is the last thing between a name-derivation bug and another
customer's checkpoints (architecture §22). The Job mounts the three PVCs at parent level — it has
to remove the directory, not empty it — so nothing but this script stops a bad key.

``/mnt/`` is rewritten into ``tmp_path`` in the test rather than parameterized in the production
script: a base-path knob is one more thing a caller could get wrong.
"""

import subprocess
from pathlib import Path

import pytest

from apps.providers.kubernetes import _CLEANUP_SCRIPT

BASES = ("checkpoints", "ha", "savepoints")


def _state_root(tmp_path: Path) -> Path:
    return tmp_path / "mnt"


def _seed(tmp_path: Path, *keys: str) -> None:
    """Create ``<tmp>/mnt/<base>/<key>/state.db`` for every base × key."""
    for base in BASES:
        for key in keys:
            directory = _state_root(tmp_path) / base / key
            directory.mkdir(parents=True)
            (directory / "state.db").write_text(key)


def _run(tmp_path: Path, state_subpath: str | None) -> subprocess.CompletedProcess[str]:
    script = _CLEANUP_SCRIPT.replace("/mnt/", f"{_state_root(tmp_path)}/")
    env = {"PATH": "/usr/bin:/bin"}
    if state_subpath is not None:
        env["STATE_SUBPATH"] = state_subpath
    return subprocess.run(["sh", "-c", script], capture_output=True, text=True, env=env, timeout=30)


def _survives(tmp_path: Path, key: str) -> bool:
    return all((_state_root(tmp_path) / base / key / "state.db").exists() for base in BASES)


def test_deletes_only_its_own_directory(tmp_path: Path) -> None:
    _seed(tmp_path, "corr-a", "corr-b")

    result = _run(tmp_path, "corr-a")

    assert result.returncode == 0, result.stderr
    assert not (_state_root(tmp_path) / "checkpoints" / "corr-a").exists()
    assert _survives(tmp_path, "corr-b"), "a cleanup reached the sibling deployment's state"
    assert result.stdout.count("deleted") == len(BASES)


def test_missing_directory_is_not_an_error(tmp_path: Path) -> None:
    """A stop before the first checkpoint has nothing to delete — that is success, not failure."""
    _seed(tmp_path, "corr-b")

    result = _run(tmp_path, "corr-a")

    assert result.returncode == 0, result.stderr
    assert result.stdout.count("nothing to delete") == len(BASES)
    assert _survives(tmp_path, "corr-b")


@pytest.mark.parametrize("bad", ["../corr-b", ".", "..", "", "corr-b/../corr-b", "/"])
def test_refuses_anything_that_is_not_one_segment(tmp_path: Path, bad: str) -> None:
    _seed(tmp_path, "corr-b")

    result = _run(tmp_path, bad)

    assert result.returncode != 0, f"'{bad}' was accepted: {result.stdout}"
    assert _survives(tmp_path, "corr-b")
    assert (_state_root(tmp_path) / "checkpoints").exists(), "the PVC root itself was touched"


def test_unset_state_subpath_refuses(tmp_path: Path) -> None:
    _seed(tmp_path, "corr-b")

    result = _run(tmp_path, None)

    assert result.returncode != 0
    assert "STATE_SUBPATH" in result.stderr
    assert _survives(tmp_path, "corr-b")


def test_refuses_a_symlink_instead_of_following_it(tmp_path: Path) -> None:
    """Deleting through a symlink would delete the target, not the link."""
    _seed(tmp_path, "corr-b")
    for base in BASES:
        (_state_root(tmp_path) / base / "corr-a").symlink_to(_state_root(tmp_path) / base / "corr-b")

    result = _run(tmp_path, "corr-a")

    assert result.returncode != 0
    assert "symlink" in result.stderr
    assert _survives(tmp_path, "corr-b")


def test_refuses_a_non_directory_target(tmp_path: Path) -> None:
    _seed(tmp_path, "corr-b")
    for base in BASES:
        (_state_root(tmp_path) / base / "corr-a").write_text("not a state directory")

    result = _run(tmp_path, "corr-a")

    assert result.returncode != 0
    assert "not a directory" in result.stderr
    assert _survives(tmp_path, "corr-b")
