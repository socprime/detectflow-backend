"""Repository and Active Threat sync scheduler service.

Runs periodic and on-demand sync of:
- api_repos: SOCPrime TDM API repositories (RulesOrchestrator.sync_api_repos)
- git_hub_repos: External GitHub repositories (GithubSyncManager.sync_all_enabled_repos)
- gitlab_repos: GitLab integration-owned repositories (GitlabSyncManager.sync_all_enabled_repos; auto_pull=true only)
- active_threats: Active Threat/HOS metadata (ActiveThreatsSyncManager.sync_active_threats)

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
from apps.managers.active_threats import ActiveThreatsSyncManager
from apps.managers.github_sync import GithubSyncManager
from apps.managers.gitlab_sync import GitlabSyncManager
from apps.managers.rule import RulesOrchestrator
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.rule_content import RuleContentDAO
from apps.services.health_check.main import health_check_service

logger = get_logger(__name__)


@dataclass
class SyncStatus:
    """Status of the last sync operation for one sync type (api_repos, git_hub_repos, gitlab_repos, …).

    status: idle (no repos to sync), running, completed, or failed.
    """

    status: str = "idle"
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None


class SchedulerService:
    """Scheduler that runs repository and Active Threat sync jobs."""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self._lock = asyncio.Lock()
        self._repo_syncs: dict[str, RepoSync] = {
            ApiRepoSync.name: ApiRepoSync(),
            GitHubRepoSync.name: GitHubRepoSync(),
            GitLabRepoSync.name: GitLabRepoSync(),
        }
        self._active_threats_sync: RepoSync | None = (
            ActiveThreatsSync() if settings.sync_active_threats_enabled else None
        )
        self._active_threats_full_refresh_sync: RepoSync | None = (
            ActiveThreatsFullRefreshSync() if settings.sync_active_threats_enabled else None
        )

    @property
    def _all_syncs(self) -> list["RepoSync"]:
        """All registered sync tasks (repo syncs + optional Active Threat sync variants)."""
        syncs = list(self._repo_syncs.values())
        if self._active_threats_sync is not None:
            syncs.append(self._active_threats_sync)
        if self._active_threats_full_refresh_sync is not None:
            syncs.append(self._active_threats_full_refresh_sync)
        return syncs

    @property
    def is_sync_running(self) -> bool:
        """Check if sync is currently running."""
        return any(sync._sync_status.status == "running" for sync in self._all_syncs)

    @property
    def sync_statuses(self) -> dict[str, SyncStatus]:
        """Get current sync status. Only sync types with status != \"idle\" are returned."""
        return {sync.name: sync._sync_status for sync in self._all_syncs if sync._sync_status.status != "idle"}

    async def _run_one_sync(self, sync: "RepoSync") -> None:
        async with AsyncSessionLocal() as db:
            try:
                await sync.sync(db)
                await db.commit()
            except Exception:
                await db.rollback()
                logger.error(f"{sync.name} task failed", exc_info=True)

    async def _run_sync_repos(self) -> None:
        """Run repository sync tasks from scheduler (waits if already running)."""
        async with self._lock:
            for sync in self._repo_syncs.values():
                await self._run_one_sync(sync)

    async def _run_active_threats_sync(self) -> None:
        """Run Active Threat sync from scheduler (waits if already running)."""
        if self._active_threats_sync is None:
            return
        async with self._lock:
            await self._run_one_sync(self._active_threats_sync)

    async def _run_active_threats_full_refresh(self) -> None:
        """Run a full (non-incremental) Active Threat re-sync (waits if already running)."""
        if self._active_threats_full_refresh_sync is None:
            return
        async with self._lock:
            await self._run_one_sync(self._active_threats_full_refresh_sync)

    async def run_sync_in_background(self) -> None:
        """Run manual sync in background (fire-and-forget from endpoint)."""
        try:
            await self._run_sync_repos()
            await self._run_active_threats_sync()
        except Exception:
            # Already logged in sync.sync
            pass

    async def run_active_threats_full_refresh_in_background(self) -> None:
        """Run a full Active Threat re-sync in background (fire-and-forget from endpoint)."""
        try:
            await self._run_active_threats_full_refresh()
        except Exception:
            # Already logged in sync.sync
            pass

    async def run_gitlab_pull_in_background(self, integration_id, user=None) -> None:
        """Pull one GitLab integration in the background (fire-and-forget from endpoint)."""
        from apps.core.models import User
        from apps.managers.integrations import IntegrationsManager

        async with AsyncSessionLocal() as db:
            try:
                actor = user if isinstance(user, User) else None
                await IntegrationsManager(db).pull_integration(integration_id, actor)
                await db.commit()
            except Exception:
                await db.rollback()
                logger.error(
                    f"GitLab pull failed for integration {integration_id}",
                    exc_info=True,
                )

    async def _run_health_check_all(self) -> None:
        """Run health check for all platforms from scheduler (persists results to DB)."""
        try:
            await health_check_service.check_platforms()
            logger.info("Health check (check_all) completed successfully")
        except Exception:
            logger.error("Health check (check_all) failed", exc_info=True)
            raise

    async def _run_prune_versions(self) -> None:
        """Prune old rule content versions according to retention policy."""
        async with AsyncSessionLocal() as db:
            try:
                dao = RuleContentDAO(db)
                deleted = await dao.prune_all_versions(keep=settings.rule_version_retention)
                await db.commit()
                if deleted:
                    logger.info(f"Pruned {deleted} old rule content versions")
            except Exception:
                await db.rollback()
                logger.error("Rule version pruning failed", exc_info=True)

    def start(self) -> None:
        """Start the scheduler if auto sync is enabled."""
        if not settings.enable_auto_sync:
            logger.info("Auto sync is disabled, scheduler will not start")
            return

        interval_minutes = settings.sync_api_repos_interval_minutes
        active_threats_interval_minutes = settings.sync_active_threats_interval_minutes
        active_threats_full_refresh_interval_hours = settings.sync_active_threats_full_refresh_interval_hours
        health_check_minutes = settings.health_check_interval_minutes
        logger.info(
            f"Starting scheduler: sync_repos every {interval_minutes} min, "
            f"sync_active_threats every {active_threats_interval_minutes} min "
            f"(enabled={settings.sync_active_threats_enabled}), "
            f"sync_active_threats_full_refresh every {active_threats_full_refresh_interval_hours} h, "
            f"health_check_all every {health_check_minutes} min"
        )

        self.scheduler.add_job(
            self._run_sync_repos,
            "interval",
            minutes=interval_minutes,
            id="sync_repos",
            replace_existing=True,
            max_instances=1,
        )

        self.scheduler.add_job(
            self._run_prune_versions,
            "interval",
            hours=1,
            id="prune_rule_versions",
            replace_existing=True,
            max_instances=1,
        )
        self.scheduler.add_job(
            self._run_health_check_all,
            "interval",
            minutes=health_check_minutes,
            id="health_check_all",
            replace_existing=True,
            max_instances=1,
        )
        if self._active_threats_sync is not None:
            self.scheduler.add_job(
                self._run_active_threats_sync,
                "interval",
                minutes=active_threats_interval_minutes,
                id="sync_active_threats",
                replace_existing=True,
                max_instances=1,
            )
        if self._active_threats_full_refresh_sync is not None:
            self.scheduler.add_job(
                self._run_active_threats_full_refresh,
                "interval",
                hours=active_threats_full_refresh_interval_hours,
                id="sync_active_threats_full_refresh",
                replace_existing=True,
                max_instances=1,
            )

        self.scheduler.start()

    def shutdown(self) -> None:
        """Shutdown the scheduler."""
        if self.scheduler.running:
            logger.info("Shutting down scheduler")
            self.scheduler.shutdown(wait=True)


class RepoSync:
    """Base class for a single repository sync task (API, GitHub, GitLab, or Active Threat)."""

    name: str
    timeout_seconds: int = settings.sync_api_repos_timeout_seconds

    def __init__(self) -> None:
        self._sync_status = SyncStatus()

    async def sync(self, db: AsyncSession) -> None:
        """
        Sync API, GitHub, and GitLab repositories (waits if already running).
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


class GitLabRepoSync(RepoSync):
    """Syncs GitLab integration-owned repositories (GitlabSyncManager.sync_all_enabled_repos)."""

    name = "gitlab_repos"

    async def _sync_func(self, db: AsyncSession) -> bool:
        result = await GitlabSyncManager.sync_all_enabled_repos(db)
        return bool(result)


class ActiveThreatsSync(RepoSync):
    """Syncs Active Threats + High Order Sigmas from SOC Prime TDM."""

    name = "active_threats"
    timeout_seconds = settings.sync_active_threats_timeout_seconds

    async def _sync_func(self, db: AsyncSession) -> bool:
        """Run Active Threat sync. Returns True if integration is configured."""
        manager = ActiveThreatsSyncManager(db)
        result = await asyncio.wait_for(manager.sync_active_threats(), timeout=self.timeout_seconds)
        return result.configured


class ActiveThreatsFullRefreshSync(RepoSync):
    """Full (non-incremental) re-sync of Active Threats + HOS + rules from SOC Prime TDM.

    Unlike :class:`ActiveThreatsSync`, this ignores the per-entity update cursors and
    re-fetches all Active Threats within the TDM lookback window plus every locally known
    HOS body and referenced/behavioral rule. Runs on its own (slower) schedule and via the
    ``POST /api/v1/active_threats/refresh`` endpoint.
    """

    name = "active_threats_full_refresh"
    timeout_seconds = settings.sync_active_threats_timeout_seconds

    async def _sync_func(self, db: AsyncSession) -> bool:
        """Run a full Active Threat re-sync. Returns True if integration is configured."""
        manager = ActiveThreatsSyncManager(db)
        result = await asyncio.wait_for(manager.sync_active_threats(full_refresh=True), timeout=self.timeout_seconds)
        return result.configured


# Global scheduler instance
_scheduler_service: SchedulerService | None = None


def get_scheduler_service() -> SchedulerService:
    """Get or create the global scheduler service instance."""
    global _scheduler_service
    if _scheduler_service is None:
        _scheduler_service = SchedulerService()
    return _scheduler_service
