"""DAO layer for GitLab integration-owned repositories and per-rule sync metadata."""

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.exceptions import ConflictError
from apps.core.models import GitlabRepository, GitlabRuleMetadata, Integration
from apps.modules.utils import coerce_config_bool

# PostgreSQL advisory-lock class id for GitLab per-integration sync (arbitrary stable namespace).
GITLAB_SYNC_LOCK_CLASS_ID = 724_001
# UI push/delete: one retry after a short wait before returning 409.
GITLAB_SYNC_LOCK_RETRY_DELAY_SECONDS = 3.0


class GitlabIntegrationLockBusyError(ConflictError):
    """Raised when a GitLab integration sync lock is already held."""

    def __init__(self) -> None:
        super().__init__(
            "GitLab sync is in progress for this integration. Retry the operation shortly.",
        )


def gitlab_integration_lock_object_id(integration_id: UUID) -> int:
    """Map an integration UUID to a PostgreSQL advisory-lock object id (int4)."""
    return integration_id.int % (2**31 - 1)


async def try_acquire_gitlab_integration_lock(session: AsyncSession, integration_id: UUID) -> bool:
    """Try to acquire a transaction-scoped advisory lock for one GitLab integration."""
    result = await session.execute(
        text("SELECT pg_try_advisory_xact_lock(:class_id, :object_id)"),
        {
            "class_id": GITLAB_SYNC_LOCK_CLASS_ID,
            "object_id": gitlab_integration_lock_object_id(integration_id),
        },
    )
    return bool(result.scalar())


async def try_acquire_gitlab_integration_lock_with_retry(
    session: AsyncSession,
    integration_id: UUID,
    *,
    retry_delay_seconds: float = GITLAB_SYNC_LOCK_RETRY_DELAY_SECONDS,
) -> bool:
    """Try to acquire the lock; on failure wait briefly and try once more."""
    if await try_acquire_gitlab_integration_lock(session, integration_id):
        return True
    await asyncio.sleep(retry_delay_seconds)
    return await try_acquire_gitlab_integration_lock(session, integration_id)


async def acquire_gitlab_integration_lock(session: AsyncSession, integration_id: UUID) -> None:
    """Acquire a transaction-scoped advisory lock, blocking until it is available."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(:class_id, :object_id)"),
        {
            "class_id": GITLAB_SYNC_LOCK_CLASS_ID,
            "object_id": gitlab_integration_lock_object_id(integration_id),
        },
    )


class GitlabRepositoryDAO:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_repository(self, repository_id: UUID) -> GitlabRepository | None:
        result = await self.session.execute(
            select(GitlabRepository).where(GitlabRepository.repository_id == repository_id)
        )
        return result.scalar_one_or_none()

    async def get_by_integration(self, integration_id: UUID) -> GitlabRepository | None:
        result = await self.session.execute(
            select(GitlabRepository).where(GitlabRepository.integration_id == integration_id)
        )
        return result.scalar_one_or_none()

    async def create(self, repository_id: UUID, integration_id: UUID) -> GitlabRepository:
        row = GitlabRepository(repository_id=repository_id, integration_id=integration_id)
        self.session.add(row)
        await self.session.flush()
        await self.session.refresh(row)
        return row

    async def update_cursor(self, repository_id: UUID, commit_sha: str | None) -> None:
        row = await self.get_by_repository(repository_id)
        if row is None:
            return
        row.last_synced_commit_sha = commit_sha
        row.updated = datetime.now(UTC)
        await self.session.flush()

    async def delete_by_integration(self, integration_id: UUID) -> UUID | None:
        """Return the owned repository id before deleting the link row (caller deletes repository)."""
        row = await self.get_by_integration(integration_id)
        if row is None:
            return None
        repository_id = row.repository_id
        await self.session.delete(row)
        await self.session.flush()
        return repository_id


class GitlabRuleMetadataDAO:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_rule_id(self, rule_id: UUID) -> GitlabRuleMetadata | None:
        result = await self.session.execute(select(GitlabRuleMetadata).where(GitlabRuleMetadata.rule_id == rule_id))
        return result.scalar_one_or_none()

    async def get_by_rule_ids(self, rule_ids: list[UUID]) -> list[GitlabRuleMetadata]:
        if not rule_ids:
            return []
        result = await self.session.execute(select(GitlabRuleMetadata).where(GitlabRuleMetadata.rule_id.in_(rule_ids)))
        return list(result.scalars().all())

    async def get_by_repository(self, repository_id: UUID) -> list[GitlabRuleMetadata]:
        result = await self.session.execute(
            select(GitlabRuleMetadata).where(GitlabRuleMetadata.repository_id == repository_id)
        )
        return list(result.scalars().all())

    async def get_by_paths(self, repository_id: UUID, file_paths: list[str]) -> list[GitlabRuleMetadata]:
        if not file_paths:
            return []
        result = await self.session.execute(
            select(GitlabRuleMetadata).where(
                GitlabRuleMetadata.repository_id == repository_id,
                GitlabRuleMetadata.file_path.in_(file_paths),
            )
        )
        return list(result.scalars().all())

    async def upsert_many(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        now = datetime.now(UTC)
        values = [
            {
                "rule_id": row["rule_id"],
                "repository_id": row["repository_id"],
                "file_path": row["file_path"],
                "last_commit_id": row.get("last_commit_id"),
                "synced_at": row.get("synced_at", now),
            }
            for row in rows
        ]
        stmt = insert(GitlabRuleMetadata).values(values)
        stmt = stmt.on_conflict_do_update(
            index_elements=[GitlabRuleMetadata.rule_id],
            set_={
                "file_path": stmt.excluded.file_path,
                "last_commit_id": stmt.excluded.last_commit_id,
                "synced_at": stmt.excluded.synced_at,
            },
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def delete_by_rule_ids(self, rule_ids: list[UUID]) -> None:
        if not rule_ids:
            return
        await self.session.execute(delete(GitlabRuleMetadata).where(GitlabRuleMetadata.rule_id.in_(rule_ids)))
        await self.session.flush()


async def list_gitlab_integrations(session: AsyncSession, *, auto_pull_only: bool = False) -> list[Integration]:
    """Return GitLab integration profiles, optionally filtered to ``auto_pull=True``."""
    result = await session.execute(
        select(Integration).where(Integration.integration_type == "gitlab").order_by(Integration.created.asc())
    )
    integrations = list(result.scalars().all())
    if not auto_pull_only:
        return integrations
    return [
        integration
        for integration in integrations
        if coerce_config_bool((integration.config or {}).get("auto_pull"), default=True)
    ]
