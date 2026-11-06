"""Audit Log DAO for activity/audit logging."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, delete, desc, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.enums import AuditSeverity
from apps.core.models import AuditLog
from apps.core.schemas import ActivityEvent
from apps.modules.postgre.base import BaseDAO


class AuditLogDAO(BaseDAO[AuditLog]):
    """Data access object for audit logs."""

    def __init__(self, session: AsyncSession):
        super().__init__(AuditLog, session)

    async def create_from_event(self, event: ActivityEvent) -> AuditLog:
        """Create audit log entry from ActivityEvent.

        Args:
            event: ActivityEvent from Kafka

        Returns:
            Created AuditLog instance
        """
        audit_log = AuditLog(
            id=UUID(event.id),
            timestamp=event.timestamp,
            action=event.action,
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            entity_name=event.entity_name,
            user_id=UUID(event.user_id) if event.user_id else None,
            user_email=event.user_email,
            details=event.details,
            changes=event.changes,
            source=event.source,
            severity=event.severity,
        )
        self.session.add(audit_log)
        await self.session.flush()
        return audit_log

    async def create_batch(self, events: list[ActivityEvent]) -> int:
        """Create multiple audit log entries in a single transaction.

        Args:
            events: List of ActivityEvents from Kafka batch

        Returns:
            Number of created records
        """
        if not events:
            return 0

        audit_logs = [
            AuditLog(
                id=UUID(event.id),
                timestamp=event.timestamp,
                action=event.action,
                entity_type=event.entity_type,
                entity_id=event.entity_id,
                entity_name=event.entity_name,
                user_id=UUID(event.user_id) if event.user_id else None,
                user_email=event.user_email,
                details=event.details,
                changes=event.changes,
                source=event.source,
                severity=event.severity,
            )
            for event in events
        ]
        self.session.add_all(audit_logs)
        await self.session.flush()
        return len(audit_logs)

    async def get_recent(self, limit: int = 5) -> list[AuditLog]:
        """Get most recent audit logs.

        Args:
            limit: Maximum number of records to return

        Returns:
            List of recent AuditLog entries
        """
        result = await self.session.execute(select(AuditLog).order_by(desc(AuditLog.timestamp)).limit(limit))
        return list(result.scalars().all())

    async def search(
        self,
        actions: list[str] | None = None,
        entity_types: list[str] | None = None,
        user_id: str | None = None,
        severities: list[str] | None = None,
        search: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        offset: int = 0,
        limit: int = 50,
    ) -> tuple[list[AuditLog], int]:
        """Search audit logs with filters.

        Args:
            actions: Filter by action types (create, update, delete, etc.)
            entity_types: Filter by entity types (pipeline, rule, etc.)
            user_id: Filter by user UUID
            severities: Filter by severity levels (info, warning, error)
            search: Search in entity_name, details, user_email
            start_date: Filter logs from this date
            end_date: Filter logs until this date
            offset: Pagination offset
            limit: Pagination limit

        Returns:
            Tuple of (list of AuditLog, total count)
        """
        query = select(AuditLog)
        conditions = []

        # Apply filters (support multiple values with IN)
        if actions:
            conditions.append(AuditLog.action.in_(actions))

        if entity_types:
            conditions.append(AuditLog.entity_type.in_(entity_types))

        if severities:
            conditions.append(AuditLog.severity.in_(severities))

        if user_id:
            conditions.append(AuditLog.user_id == UUID(user_id))

        if start_date:
            conditions.append(AuditLog.timestamp >= start_date)

        if end_date:
            conditions.append(AuditLog.timestamp <= end_date)

        # Apply search
        if search:
            search_conditions = [
                AuditLog.entity_name.ilike(f"%{search}%"),
                AuditLog.details.ilike(f"%{search}%"),
                AuditLog.user_email.ilike(f"%{search}%"),
            ]
            conditions.append(or_(*search_conditions))

        if conditions:
            query = query.where(and_(*conditions))

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await self.session.execute(count_query)
        total = total_result.scalar() or 0

        # Apply ordering and pagination
        query = query.order_by(desc(AuditLog.timestamp)).offset(offset).limit(limit)

        result = await self.session.execute(query)
        items = list(result.scalars().all())

        return items, total

    async def cleanup_old(self, days: int = 30) -> int:
        """Delete audit logs older than specified days.

        Args:
            days: Number of days to retain

        Returns:
            Number of deleted records
        """
        cutoff = datetime.now(UTC) - timedelta(days=days)

        result = await self.session.execute(delete(AuditLog).where(AuditLog.timestamp < cutoff))
        await self.session.flush()

        return result.rowcount or 0

    async def recent_health(
        self,
        entity_ids: list[str],
        window_seconds: int,
        *,
        source: str = "flink",
    ) -> dict[str, dict[str, object]]:
        """Summarise recent warning/error activity per entity (self-healing health signal).

        Scans ``audit_logs`` for the given ``entity_ids`` within the trailing
        ``window_seconds`` window, keeping only ``warning``/``error`` severities.
        Because the window slides with ``now()``, the signal auto-clears once
        the offending events age out — callers can derive a "degraded" state
        that heals itself when errors stop.

        Args:
            entity_ids: Audit ``entity_id`` values to summarise (e.g. Flink
                deployment names ``["correlation-job", "agent-job"]``).
            window_seconds: Trailing window size; only events with
                ``timestamp >= now() - window_seconds`` are considered.
            source: Audit ``source`` to scope to (defaults to ``"flink"``).

        Returns:
            Mapping of ``entity_id`` to a dict with keys ``worst_severity``
            (``"error"`` outranks ``"warning"``), ``last_issue_details`` (the
            most recent entry *at the worst severity*), and ``last_event_at``
            (that entry's ``timestamp``). ``last_issue_details``/``last_event_at``
            describe the same event that drives ``worst_severity``: when an
            older ``error`` outranks a newer ``warning`` inside the window, the
            summary surfaces the most recent ``error`` (not the warning) so the
            health tile and its last-issue text stay consistent. Entities with
            no recent warning/error events are omitted from the mapping.
        """
        if not entity_ids:
            return {}

        cutoff = datetime.now(UTC) - timedelta(seconds=window_seconds)
        result = await self.session.execute(
            select(AuditLog)
            .where(
                and_(
                    AuditLog.source == source,
                    AuditLog.entity_id.in_(entity_ids),
                    AuditLog.timestamp >= cutoff,
                    AuditLog.severity.in_([AuditSeverity.WARNING.value, AuditSeverity.ERROR.value]),
                )
            )
            .order_by(desc(AuditLog.timestamp))
        )

        # Rank error above warning; rows arrive newest-first. We track the
        # worst severity per entity AND keep last_issue_details/last_event_at
        # pointing at the most recent row of that worst severity, so the
        # surfaced issue always describes the event that drives the health.
        severity_rank = {AuditSeverity.WARNING.value: 1, AuditSeverity.ERROR.value: 2}
        summary: dict[str, dict[str, object]] = {}
        for row in result.scalars().all():
            entity_id = row.entity_id
            if entity_id is None:
                continue
            existing = summary.get(entity_id)
            if existing is None:
                summary[entity_id] = {
                    "worst_severity": row.severity,
                    "last_issue_details": row.details,
                    "last_event_at": row.timestamp,
                }
                continue
            if severity_rank.get(row.severity, 0) > severity_rank.get(str(existing["worst_severity"]), 0):
                # An older row of higher severity wins: re-point the summary to
                # this row so worst_severity and last_issue describe one event.
                # Rows are newest-first, so this is the most recent row at the
                # new worst severity (later higher-severity rows are skipped).
                existing["worst_severity"] = row.severity
                existing["last_issue_details"] = row.details
                existing["last_event_at"] = row.timestamp
        return summary

    async def get_distinct_actions(self) -> list[str]:
        """Get list of distinct action types.

        Returns:
            List of unique action strings
        """
        result = await self.session.execute(select(AuditLog.action).distinct().order_by(AuditLog.action))
        return [row[0] for row in result.fetchall()]

    async def get_distinct_entity_types(self) -> list[str]:
        """Get list of distinct entity types.

        Returns:
            List of unique entity_type strings
        """
        result = await self.session.execute(select(AuditLog.entity_type).distinct().order_by(AuditLog.entity_type))
        return [row[0] for row in result.fetchall()]

    async def get_distinct_severities(self) -> list[str]:
        """Get list of distinct severity levels.

        Returns:
            List of unique severity strings
        """
        result = await self.session.execute(select(AuditLog.severity).distinct().order_by(AuditLog.severity))
        return [row[0] for row in result.fetchall() if row[0] is not None]
