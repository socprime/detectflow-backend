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

logger = get_logger(__name__)


# TODO: sync external repos even if api key is not set
# TODO: make db commits after each successful repository sync


@dataclass
class SyncStatus:
    """Status of the last sync operation."""

    status: str = "idle"  # idle, running, completed, failed
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class SchedulerService:
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._lock = asyncio.Lock()
        self._sync_status = SyncStatus()

    @property
    def is_sync_running(self) -> bool:
        """Check if sync is currently running."""
        return self._sync_status.status == "running"

    @property
    def sync_status(self) -> SyncStatus:
        """Get current sync status."""
        return self._sync_status

    async def _sync_repos(self, db: AsyncSession) -> None:
        """
        Sync API repositories and external repositories (waits if already running).
        Commit and rollback must be handled by the caller.

        Raises:
            TimeoutError: If sync operation exceeds the configured timeout.
        """
        async with self._lock:
            self._sync_status = SyncStatus(status="running", started_at=datetime.now())
            logger.info("Starting sync_repos task")
            try:
                rules_orchestrator = RulesOrchestrator(db)
                await asyncio.wait_for(
                    rules_orchestrator.sync_api_repos(),
                    timeout=settings.sync_api_repos_timeout_seconds,
                )
                await GithubSyncManager.sync_all_enabled_repos(db)
                self._sync_status.status = "completed"
                self._sync_status.completed_at = datetime.now()
                logger.info("Sync_repos task completed successfully")
            except TimeoutError:
                self._sync_status.status = "failed"
                self._sync_status.completed_at = datetime.now()
                self._sync_status.error = f"Timed out after {settings.sync_api_repos_timeout_seconds} seconds"
                logger.error(f"Sync_repos task timed out after {settings.sync_api_repos_timeout_seconds} seconds")
                raise
            except Exception as e:
                self._sync_status.status = "failed"
                self._sync_status.completed_at = datetime.now()
                self._sync_status.error = str(e)
                raise

    async def _run_sync_repos(self) -> None:
        """Run sync_repos from scheduler (waits if already running)."""
        # Note: db session is created before lock acquisition to pass to _sync_repos.
        # This means the db connection may be held while waiting for the lock.
        # This is acceptable as it's a short wait and ensures proper transaction handling.
        async with AsyncSessionLocal() as db:
            try:
                await self._sync_repos(db)
                await db.commit()
            except Exception:
                await db.rollback()
                logger.error("Sync_repos task failed", exc_info=True)
                raise

    async def run_sync_in_background(self) -> None:
        """Run sync in background (fire-and-forget from endpoint)."""
        try:
            await self._run_sync_repos()
        except Exception:
            # Already logged in _run_sync_api_repos
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


# Global scheduler instance
_scheduler_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService:
    """Get or create the global scheduler service instance."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
