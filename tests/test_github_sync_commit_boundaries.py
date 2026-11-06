"""Tests: GitHub repo sync commits at per-repo boundaries (reduced transaction hold time).

``GithubSyncManager.sync_all_enabled_repos`` commits after each successfully synced repo so the
pooled connection is released between repos' GitHub HTTP fetches, and rolls back a failed repo's
partial writes while still continuing with the rest (unchanged per-repo failure semantics).
See ``tasks/T5_scheduler_session_hygiene.md``.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from apps.managers import github_sync as gs_module
from apps.managers.github_sync import GithubSyncManager


class _SpySession:
    """Records the interleaving of sync_repo / commit / rollback in a shared event log."""

    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def commit(self) -> None:
        self._events.append("commit")

    async def rollback(self) -> None:
        self._events.append("rollback")


def _repo(name: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid4(), name=name, sync_enabled=True)


async def test_commit_after_each_successful_repo(monkeypatch) -> None:
    events: list[str] = []
    db = _SpySession(events)

    repos = [_repo("a"), _repo("b")]
    monkeypatch.setattr(
        gs_module, "RepositoryDAO", lambda _db: SimpleNamespace(get_all=AsyncMock(return_value=(repos, len(repos))))
    )

    async def fake_sync_repo(repo_id, passed_db):
        events.append("sync")

    monkeypatch.setattr(
        GithubSyncManager, "sync_repo", classmethod(lambda cls, repo_id, db: fake_sync_repo(repo_id, db))
    )

    result = await GithubSyncManager.sync_all_enabled_repos(db)

    assert result is True
    # One commit follows each repo sync; the connection is released between repos.
    assert events == ["sync", "commit", "sync", "commit"]


async def test_failed_repo_rolls_back_and_continues(monkeypatch) -> None:
    events: list[str] = []
    db = _SpySession(events)

    repos = [_repo("bad"), _repo("good")]
    monkeypatch.setattr(
        gs_module, "RepositoryDAO", lambda _db: SimpleNamespace(get_all=AsyncMock(return_value=(repos, len(repos))))
    )
    monkeypatch.setattr(gs_module.activity_producer, "log_action", AsyncMock())

    async def fake_sync_repo(cls, repo_id, db):
        if repo_id == repos[0].id:
            events.append("sync_fail")
            raise RuntimeError("boom")
        events.append("sync_ok")

    monkeypatch.setattr(GithubSyncManager, "sync_repo", classmethod(fake_sync_repo))

    result = await GithubSyncManager.sync_all_enabled_repos(db)

    assert result is True
    # The failed repo rolls back; the run continues and commits the good repo.
    assert events == ["sync_fail", "rollback", "sync_ok", "commit"]
