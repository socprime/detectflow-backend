from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.exceptions import NotFoundError
from apps.core.logger import get_logger
from apps.core.models import HighOrderSigma, User
from apps.modules.kafka.active_threats import get_kafka_active_threats_service
from apps.modules.kafka.activity import activity_producer
from apps.modules.postgre.high_order_sigma import HighOrderSigmaDAO
from apps.modules.utils import extract_referenced_sigma_uuids, get_level_from_sigma_body

_SEARCH_FIELDS = frozenset({"name", "tdm_id"})

logger = get_logger(__name__)


class HighOrderSigmasManager:
    """High-order Sigma catalog reads and user edits."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.dao = HighOrderSigmaDAO(db)
        self.kafka = get_kafka_active_threats_service()

    async def get_all(self, pagination: dict) -> tuple[list[HighOrderSigma], int]:
        return await self.dao.get_all(
            skip=pagination["skip"],
            limit=pagination["limit"],
            search=pagination.get("search"),
            search_fields=_SEARCH_FIELDS,
            sort=pagination.get("sort"),
            order=pagination.get("order") or "asc",
        )

    async def get_by_id(self, sigma_id: UUID) -> HighOrderSigma:
        row = await self.dao.get_by_id(sigma_id)
        if not row:
            raise NotFoundError(f"High-order Sigma with id {sigma_id} not found")
        return row

    async def update(self, sigma_id: UUID, name: str | None, body: str | None, user: User) -> HighOrderSigma:
        """Update HOS user-managed fields, mark it user-edited, and republish to Kafka.

        Active Threat sync skips ``is_user_edited=True`` HOS rows, so user edits are preserved
        in the DB across TDM updates. A HOS is only useful to the correlation job in the
        context of an Active Threat, so the Kafka message is sent only when the HOS still
        has at least one non-hidden AT membership; otherwise the row is tombstoned on Kafka
        while the user-edited body stays in the DB and is republished automatically once an
        AT becomes visible again.
        """
        row = await self.dao.get_by_id(sigma_id)
        if not row:
            raise NotFoundError(f"High-order Sigma with id {sigma_id} not found")

        values = {
            "is_user_edited": True,
            "updated": datetime.now(tz=UTC),
        }
        if name is not None:
            values["name"] = name
        if body is not None:
            values["body"] = body
            # Severity is part of the body, so an edit re-derives it — but only when the new body
            # actually declares one. TDM reports the level structurally and `_hos_to_row` prefers
            # that over the body, so a body-only overwrite would let a PATCH that merely renames
            # (resending the unchanged body) discard TDM's severity for good: sync cannot restore
            # it because `upsert_many` skips `is_user_edited=True` rows.
            values["level"] = get_level_from_sigma_body(body) or row.level

        await self.db.execute(update(HighOrderSigma).where(HighOrderSigma.id == sigma_id).values(**values))
        await self.db.flush()
        await self.db.refresh(row)

        if body is not None:
            await self.dao.replace_rule_refs(sigma_id, extract_referenced_sigma_uuids(body))

        visible_tdm_ids = await self.dao.get_tdm_ids_with_non_hidden_at([row.tdm_id])
        if row.tdm_id in visible_tdm_ids:
            active_threat_ids_by_hos_tdm_id = await self.dao.get_active_threat_ids_by_hos_tdm_ids([row.tdm_id])
            await self.kafka.send_hos([row], active_threat_ids_by_hos_tdm_id)
            await self.dao.set_kafka_tombstoned([row.tdm_id], tombstoned=False)
        else:
            await self.kafka.delete_hos([row.tdm_id])
            await self.dao.set_kafka_tombstoned([row.tdm_id], tombstoned=True)

        await self.db.refresh(row)

        await activity_producer.log_action(
            action="update",
            entity_type="high_order_sigma",
            entity_id=str(row.id),
            entity_name=row.name,
            user=user,
            details=f"User {user.email} updated high-order Sigma {row.name}",
            source="user",
        )
        return row

    async def update_body(self, sigma_id: UUID, body: str, user: User) -> HighOrderSigma:
        """Compatibility wrapper for callers that only replace the HOS body."""
        return await self.update(sigma_id=sigma_id, name=None, body=body, user=user)
