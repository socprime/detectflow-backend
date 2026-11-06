"""DAO for correlation pipelines: rows, membership and attack-chain counts.

Membership is keyed by ``pipelines.id``, never by destination topic name —
``pipelines.destination_topic`` has no unique constraint, so a topic name does not identify a
customer. A detection pipeline may belong to several correlation pipelines; the composite primary
key ``(correlation_pipeline_id, pipeline_id)`` is the uniqueness of a membership row.
"""

from typing import Literal
from uuid import UUID

from sqlalchemy import delete, func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from apps.core.enums import CorrelationPipelineSortField
from apps.core.models import AttackChain, CorrelationPipeline, Pipeline, correlation_pipeline_members
from apps.modules.postgre.base import BaseDAO


class CorrelationPipelineDAO(BaseDAO[CorrelationPipeline]):
    """Data access for ``correlation_pipelines`` and its membership join table.

    List queries never load members implicitly (``lazy="noload"`` on the relationship): callers
    opt in via ``selectinload`` so a page of N pipelines stays a fixed number of statements.

    Args:
        session: Async SQLAlchemy session for database operations.
    """

    _SORT_CONFIG: dict[str, str] = {
        CorrelationPipelineSortField.NAME.value: "name",
        CorrelationPipelineSortField.CREATED_AT.value: "created_at",
        CorrelationPipelineSortField.UPDATED_AT.value: "updated_at",
        CorrelationPipelineSortField.JOBS_STATUS.value: "jobs_status",
        CorrelationPipelineSortField.ATTACK_CHAINS.value: "attack_chains",
    }

    ALLOWED_SORT_FIELDS = {e.value for e in CorrelationPipelineSortField}

    def __init__(self, session: AsyncSession):
        super().__init__(CorrelationPipeline, session)

    async def list_with_chain_counts(
        self,
        skip: int = 0,
        limit: int = 50,
        search: str | None = None,
        sort: str | None = None,
        order: Literal["asc", "desc"] = "asc",
    ) -> tuple[list[tuple[CorrelationPipeline, int]], int]:
        """List rows with their attack-chain count in ONE aggregate query (no N+1).

        Chain counts come from a single grouped subquery and the page total from a window
        function, so a page of N rows costs exactly two statements: the aggregate itself plus the
        ``selectinload`` that eager-loads members. The separate count below only runs when the
        page came back empty (an offset past the end), where the window function has no row to
        report the total on.

        Args:
            skip: Rows to skip (offset).
            limit: Maximum rows to return.
            search: Case-insensitive substring match on ``name``.
            sort: One of ``CorrelationPipelineSortField``; unknown values fall back to name.
            order: ``asc`` or ``desc``.

        Returns:
            ``([(pipeline, chain_count), ...], total)`` where ``total`` counts all matching rows.
        """
        counts = (
            select(
                AttackChain.correlation_pipeline_id.label("cp_id"),
                func.count(AttackChain.id).label("chain_count"),
            )
            .where(AttackChain.correlation_pipeline_id.is_not(None))
            .group_by(AttackChain.correlation_pipeline_id)
            .subquery()
        )
        chain_count = func.coalesce(counts.c.chain_count, 0).label("chain_count")
        stmt = (
            select(CorrelationPipeline, chain_count, func.count().over().label("total_count"))
            .outerjoin(counts, counts.c.cp_id == CorrelationPipeline.id)
            .options(selectinload(CorrelationPipeline.members))
        )
        if search:
            stmt = stmt.where(CorrelationPipeline.name.ilike(f"%{search}%"))

        sort_column = chain_count if sort == CorrelationPipelineSortField.ATTACK_CHAINS else None
        if sort_column is None:
            attribute = self._SORT_CONFIG.get(sort or "", "name")
            sort_column = getattr(CorrelationPipeline, attribute)
        direction = sort_column.desc() if order.lower() == "desc" else sort_column.asc()
        # Stable tiebreak so pagination cannot repeat or drop a row on equal sort values.
        stmt = stmt.order_by(direction, CorrelationPipeline.id.asc()).offset(skip).limit(limit)

        rows = (await self.session.execute(stmt)).all()
        if not rows:
            return [], await self._count(search)
        return [(row[0], row.chain_count) for row in rows], rows[0].total_count

    async def count_chains(self, correlation_pipeline_id: UUID) -> int:
        """Attack chains attributed to ONE correlation pipeline (the detail view's count).

        The list page gets its counts from :meth:`list_with_chain_counts`' single aggregate; this
        exists so the detail response does not have to run that page query for one row.
        """
        stmt = select(func.count(AttackChain.id)).where(AttackChain.correlation_pipeline_id == correlation_pipeline_id)
        return (await self.session.execute(stmt)).scalar() or 0

    async def _count(self, search: str | None) -> int:
        """Total matching rows; only used when a page came back empty."""
        stmt = select(func.count()).select_from(CorrelationPipeline)
        if search:
            stmt = stmt.where(CorrelationPipeline.name.ilike(f"%{search}%"))
        return (await self.session.execute(stmt)).scalar() or 0

    async def get_with_relations(self, id: UUID) -> CorrelationPipeline | None:
        """Get one correlation pipeline with its member detection pipelines eagerly loaded."""
        result = await self.session.execute(
            select(CorrelationPipeline)
            .where(CorrelationPipeline.id == id)
            .options(selectinload(CorrelationPipeline.members))
        )
        return result.scalar_one_or_none()

    async def get_by_name(self, name: str) -> CorrelationPipeline | None:
        """Get one correlation pipeline by its unique name (duplicate-name checks, migration runbook)."""
        result = await self.session.execute(select(CorrelationPipeline).where(CorrelationPipeline.name == name))
        return result.scalar_one_or_none()

    async def list_with_members(self, limit: int = 50) -> list[CorrelationPipeline]:
        """Rows with their member detection pipelines eagerly loaded, without chain counts.

        The cheap counterpart of :meth:`list_with_chain_counts` for callers that only need the
        rendering inputs of a pipeline's Flink deployments (member ``tagged_events_topic``,
        resources): one statement plus the ``selectinload``, and no aggregate over
        ``attack_chains``.

        Args:
            limit: Maximum rows to return.

        Returns:
            Correlation pipelines ordered by name, members loaded.
        """
        result = await self.session.execute(
            select(CorrelationPipeline)
            .options(selectinload(CorrelationPipeline.members))
            .order_by(CorrelationPipeline.name.asc(), CorrelationPipeline.id.asc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def list_refs(self, limit: int = 200) -> list[tuple[UUID, str]]:
        """``[(id, name), ...]`` ordered by name, without loading ORM entities or members.

        The bounded half of the Active Threat list's ``Pipelines`` column: one statement for the
        whole page regardless of how many threats it holds. ``limit`` is a fan-out guard, not
        pagination — a control plane with more correlation pipelines than that has bigger problems
        than a truncated chip list.
        """
        result = await self.session.execute(
            select(CorrelationPipeline.id, CorrelationPipeline.name)
            .order_by(CorrelationPipeline.name.asc(), CorrelationPipeline.id.asc())
            .limit(limit)
        )
        return [(row[0], row[1]) for row in result.all()]

    async def list_ids(self) -> set[UUID]:
        """Every ``correlation_pipelines.id``, without loading ORM entities.

        Used by the chains-analysed consumer's orphan guard, which caches the set.
        """
        result = await self.session.execute(select(CorrelationPipeline.id))
        return set(result.scalars().all())

    async def get_member_pipelines(self, correlation_pipeline_ids: list[UUID]) -> dict[UUID, list[Pipeline]]:
        """Get member detection pipelines for many correlation pipelines in a single query.

        Args:
            correlation_pipeline_ids: Correlation pipeline ids to fetch members for.

        Returns:
            Dictionary mapping correlation_pipeline_id to its member pipelines. Correlation
            pipelines with no members are absent from the mapping.
        """
        if not correlation_pipeline_ids:
            return {}

        result = await self.session.execute(
            select(correlation_pipeline_members.c.correlation_pipeline_id, Pipeline)
            .join(Pipeline, Pipeline.id == correlation_pipeline_members.c.pipeline_id)
            .where(correlation_pipeline_members.c.correlation_pipeline_id.in_(correlation_pipeline_ids))
        )

        members_map: dict[UUID, list[Pipeline]] = {}
        for row in result.all():
            members_map.setdefault(row.correlation_pipeline_id, []).append(row.Pipeline)
        return members_map

    async def get_correlation_pipeline_ids_for_pipeline(self, pipeline_id: UUID) -> list[UUID]:
        """Correlation pipelines that have ``pipeline_id`` as a member, possibly several.

        A destination-topic rename or tagged-events assign must refresh every owner: each
        independently consumes that member's tagged-events topic.
        """
        result = await self.session.execute(
            select(correlation_pipeline_members.c.correlation_pipeline_id)
            .where(correlation_pipeline_members.c.pipeline_id == pipeline_id)
            .order_by(correlation_pipeline_members.c.correlation_pipeline_id.asc())
        )
        return list(result.scalars().all())

    async def set_members(self, correlation_pipeline_id: UUID, pipeline_ids: list[UUID]) -> None:
        """Replace the membership of one correlation pipeline (delete-then-insert, deduplicated).

        Runs inside the caller's transaction — no commit here. Pipelines already attached to
        *other* correlation pipelines are allowed: sharing a detection pipeline is a valid
        membership, not a conflict.

        Args:
            correlation_pipeline_id: Correlation pipeline whose membership is replaced.
            pipeline_ids: Detection pipelines that should be its members.
        """
        unique_ids = list(dict.fromkeys(pipeline_ids))

        await self.session.execute(
            delete(correlation_pipeline_members).where(
                correlation_pipeline_members.c.correlation_pipeline_id == correlation_pipeline_id
            )
        )
        if not unique_ids:
            await self.session.flush()
            return

        await self.session.execute(
            insert(correlation_pipeline_members).values(
                [
                    {"correlation_pipeline_id": correlation_pipeline_id, "pipeline_id": pipeline_id}
                    for pipeline_id in unique_ids
                ]
            )
        )
        await self.session.flush()
