import hashlib
from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import load_only

from apps.core.models import RuleContent
from apps.core.settings import settings

CHANGE_SOURCE_MANUAL = "manual"
CHANGE_SOURCE_SYSTEM = "system"


def compute_content_hash(body: str) -> str:
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


class RuleContentDAO:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_versions_metadata(self, rule_id: UUID) -> list[RuleContent]:
        """Return version metadata (without body) for a rule, newest first."""
        query = (
            select(RuleContent)
            .options(
                load_only(
                    RuleContent.id,
                    RuleContent.rule_id,
                    RuleContent.version,
                    RuleContent.content_hash,
                    RuleContent.changed_at,
                    RuleContent.change_source,
                    RuleContent.changed_by,
                )
            )
            .where(RuleContent.rule_id == rule_id)
            .order_by(RuleContent.version.desc())
            .limit(settings.rule_version_retention)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_version(self, rule_id: UUID, version: int) -> RuleContent | None:
        query = select(RuleContent).where(
            RuleContent.rule_id == rule_id,
            RuleContent.version == version,
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def create_version(
        self,
        rule_id: UUID,
        version: int,
        body: str,
        content_hash: str,
        change_source: str | None = None,
        changed_by: str | None = None,
    ) -> RuleContent:
        instance = RuleContent(
            rule_id=rule_id,
            version=version,
            body=body,
            content_hash=content_hash,
            change_source=change_source,
            changed_by=changed_by,
        )
        self.session.add(instance)
        await self.session.flush()
        return instance

    async def create_versions_bulk(self, versions: list[dict]) -> None:
        """Batch-insert multiple content versions in a single flush."""
        if not versions:
            return
        self.session.add_all([RuleContent(**data) for data in versions])
        await self.session.flush()

    async def prune_versions(self, rule_id: UUID, keep: int) -> int:
        """Delete old versions for a single rule, keeping the last N."""
        ranked = (
            select(
                RuleContent.id,
                func.row_number().over(order_by=RuleContent.version.desc()).label("rn"),
            ).where(RuleContent.rule_id == rule_id)
        ).cte("ranked")

        result = await self.session.execute(
            delete(RuleContent).where(RuleContent.id.in_(select(ranked.c.id).where(ranked.c.rn > keep)))
        )
        return result.rowcount

    async def prune_all_versions(self, keep: int) -> int:
        """Delete old versions for ALL rules in one query, keeping last N per rule."""
        ranked = (
            select(
                RuleContent.id,
                func.row_number()
                .over(partition_by=RuleContent.rule_id, order_by=RuleContent.version.desc())
                .label("rn"),
            )
        ).cte("ranked")

        result = await self.session.execute(
            delete(RuleContent).where(RuleContent.id.in_(select(ranked.c.id).where(ranked.c.rn > keep)))
        )
        return result.rowcount
