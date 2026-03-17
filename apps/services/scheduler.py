"""Repository sync scheduler service.

Runs periodic and on-demand sync of rule repositories:
- api_repos: SOCPrime TDM API repositories (RulesOrchestrator.sync_api_repos)
- git_hub_repos: External GitHub repositories (GithubSyncManager.sync_all_enabled_repos)

Sync can be triggered via POST /api/v1/sync-repositories; status per type
is available at GET /api/v1/sync-repositories/status (only sync types with
non-idle status are included in the response).
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.managers.github_sync import GithubSyncManager
from apps.managers.rule import RulesOrchestrator
from apps.modules.kafka.activity import activity_producer

logger = get_logger(__name__)


@dataclass
class SyncStatus:
    """Status of the last sync operation for one sync type (api_repos or git_hub_repos).

    status: idle (no repos to sync), running, completed, or failed.
    """

    status: str = "idle"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class SchedulerService:
    """Scheduler that runs repository sync (API repos + GitHub repos) on interval or on demand."""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._lock = asyncio.Lock()
        self._repo_syncs: dict[str, RepoSync] = {
            ApiRepoSync.name: ApiRepoSync(),
            GitHubRepoSync.name: GitHubRepoSync(),
        }

    @property
    def is_sync_running(self) -> bool:
        """Check if sync is currently running."""
        return any(sync._sync_status.status == "running" for sync in self._repo_syncs.values())

    @property
    def sync_statuses(self) -> dict[str, SyncStatus]:
        """Get current sync status. Only sync types with status != \"idle\" are returned."""
        return {
            name: sync._sync_status for name, sync in self._repo_syncs.items() if sync._sync_status.status != "idle"
        }

    async def _run_sync_repos(self) -> None:
        """Run sync_repos from scheduler (waits if already running)."""
        # Note: db session is created before lock acquisition to pass to sync.
        # This means the db connection may be held while waiting for the lock.
        # This is acceptable as it's a short wait and ensures proper transaction handling.
        async with self._lock:
            for sync in self._repo_syncs.values():
                async with AsyncSessionLocal() as db:
                    try:
                        await sync.sync(db)
                        await db.commit()
                    except Exception:
                        await db.rollback()
                        logger.error(f"{sync.name} task failed", exc_info=True)

    async def run_sync_in_background(self) -> None:
        """Run sync in background (fire-and-forget from endpoint)."""
        try:
            await self._run_sync_repos()
        except Exception:
            # Already logged in sync.sync
            pass

    def start(self) -> None:
        """Start the scheduler if auto sync is enabled."""
        if not settings.enable_auto_sync:
            logger.info("Auto sync is disabled, scheduler will not start")
            return

        interval_minutes = settings.sync_api_repos_interval_minutes
        logger.info(f"Starting scheduler: sync_repos will run every {interval_minutes} minutes")

        self.scheduler.add_job(
            self._run_sync_repos,
            "interval",
            minutes=interval_minutes,
            id="sync_repos",
            replace_existing=True,
            max_instances=1,  # Skip new job if previous is still running (prevents accumulation)
        )

        self.scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        if self.scheduler.running:
            logger.info("Shutting down scheduler")
            self.scheduler.shutdown(wait=True)


class RepoSync:
    """Base class for a single repository sync task (API repos or GitHub repos)."""

    name: str
    timeout_seconds: int = settings.sync_api_repos_timeout_seconds

    def __init__(self) -> None:
        self._sync_status = SyncStatus()

    async def sync(self, db: AsyncSession) -> None:
        """
        Sync API repositories and external repositories (waits if already running).
        Commit and rollback must be handled by the caller.
        Status is set to \"completed\" if _sync_func returns True (work was done),
        or \"idle\" if it returns False (nothing to sync).

        Raises:
            TimeoutError: If sync operation exceeds the configured timeout.
        """
        logger.info(f"Starting {self.name} task")
        self._sync_status.status = "running"
        self._sync_status.started_at = datetime.now()
        try:
            updated_repos = await self._sync_func(db)
            self._sync_status.status = "completed" if updated_repos else "idle"
            self._sync_status.completed_at = datetime.now()
            logger.info(f"{self.name} task completed successfully")
            await activity_producer.log_action(
                action="sync",
                entity_type="system",
                details=f"{self.name} task completed successfully",
                source="system",
            )
        except TimeoutError:
            self._sync_status.status = "failed"
            self._sync_status.completed_at = datetime.now()
            self._sync_status.error = f"Timed out after {self.timeout_seconds} seconds"
            logger.error(f"{self.name} task timed out after {self.timeout_seconds} seconds")
            await activity_producer.log_action(
                action="sync_failed",
                entity_type="system",
                details=f"{self.name} task timed out after {self.timeout_seconds} seconds",
                source="system",
                severity="error",
            )
            raise
        except Exception as e:
            self._sync_status.status = "failed"
            self._sync_status.completed_at = datetime.now()
            self._sync_status.error = str(e)
            await activity_producer.log_action(
                action="sync_failed",
                entity_type="system",
                details=f"{self.name} task failed: {str(e)}",
                source="system",
                severity="error",
            )
            raise

    async def _sync_func(self, db: AsyncSession) -> bool:
        """Run the sync. Returns True if there was work to do (repos to sync), False if nothing to sync."""
        raise NotImplementedError


class ApiRepoSync(RepoSync):
    """Syncs SOCPrime TDM API repositories (RulesOrchestrator.sync_api_repos)."""

    name = "api_repos"

    async def _sync_func(self, db: AsyncSession) -> bool:
        """Run TDM API sync. Returns True if there were repos to update, False otherwise."""
        rules_orchestrator = RulesOrchestrator(db)
        result = await asyncio.wait_for(rules_orchestrator.sync_api_repos(), timeout=self.timeout_seconds)
        return bool(result)


class GitHubRepoSync(RepoSync):
    """Syncs external GitHub repositories (GithubSyncManager.sync_all_enabled_repos)."""

    name = "git_hub_repos"

    async def _sync_func(self, db: AsyncSession) -> bool:
        """Run GitHub sync for all enabled repos. Returns True if there were enabled repos, False otherwise."""
        result = await GithubSyncManager.sync_all_enabled_repos(db)
        return bool(result)


# Global scheduler instance
_scheduler_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService:
    """Get or create the global scheduler service instance."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
