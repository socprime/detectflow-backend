"""Activity Service for centralized activity/audit management.

This module provides a single point of responsibility for:
- Converting activity events to display format
- Caching recent activities for dashboard
- Providing activity data to both dashboard and audit endpoints
"""

from apps.core.database import AsyncSessionLocal
from apps.core.logger import get_logger
from apps.core.models import AuditLog
from apps.core.schemas import ActivityEvent, ActivityItem
from apps.modules.postgre.audit import AuditLogDAO

logger = get_logger(__name__)


class ActivityService:
    """Centralized activity management service.

    Responsibilities:
    - Convert ActivityEvent (from Kafka) to ActivityItem (for display)
    - Convert AuditLog (from DB) to ActivityItem (for display)
    - Cache recent activities for dashboard SSE
    - Provide recent activities with DB fallback
    """

    def __init__(self, max_recent: int = 5):
        """Initialize the activity service.

        Args:
            max_recent: Maximum number of recent activities to cache
        """
        self._recent_activities: list[ActivityItem] = []
        self._max_recent = max_recent

    @staticmethod
    def from_event(event: ActivityEvent) -> ActivityItem:
        """Convert ActivityEvent (from Kafka) to ActivityItem.

        Args:
            event: Activity event from Kafka consumer

        Returns:
            ActivityItem for display
        """
        return ActivityItem(
            id=event.id,
            timestamp=event.timestamp.isoformat() if event.timestamp else "",
            action=event.action,
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            entity_name=event.entity_name,
            user_id=event.user_id,
            user_email=event.user_email,
            details=event.details,
            source=event.source,
            severity=event.severity,
        )

    @staticmethod
    def from_db(log: AuditLog) -> ActivityItem:
        """Convert AuditLog (from DB) to ActivityItem.

        Args:
            log: Audit log record from database

        Returns:
            ActivityItem for display
        """
        return ActivityItem(
            id=str(log.id),
            timestamp=log.timestamp.isoformat() if log.timestamp else "",
            action=log.action,
            entity_type=log.entity_type,
            entity_id=log.entity_id,
            entity_name=log.entity_name,
            user_id=str(log.user_id) if log.user_id else None,
            user_email=log.user_email,
            details=log.details,
            source=log.source,
            severity=log.severity,
        )

    def on_activity_received(self, event: ActivityEvent) -> None:
        """Callback when activity event is received from Kafka.

        Args:
            event: Activity event from Kafka consumer
        """
        activity_item = self.from_event(event)

        # Add to the front of the list (newest first)
        self._recent_activities.insert(0, activity_item)

        # Keep only the last N activities
        if len(self._recent_activities) > self._max_recent:
            self._recent_activities = self._recent_activities[: self._max_recent]

        logger.debug(
            "Activity event cached",
            extra={"event_id": event.id, "action": event.action},
        )

    async def get_recent(self, limit: int | None = None) -> list[ActivityItem]:
        """Get recent activities from cache or database.

        First checks in-memory cache. If empty, fetches from database.

        Args:
            limit: Maximum number of items to return (defaults to max_recent)

        Returns:
            List of recent activity items
        """
        limit = limit or self._max_recent

        # If we have cached activities, return them
        if self._recent_activities:
            return self._recent_activities[:limit]

        # Otherwise, fetch from database (fallback for server restart)
        try:
            async with AsyncSessionLocal() as db:
                audit_dao = AuditLogDAO(db)
                audit_logs = await audit_dao.get_recent(limit=limit)
                return [self.from_db(log) for log in audit_logs]
        except Exception as e:
            logger.error(f"Failed to fetch recent activity from database: {e}")
            # Note: Cannot log to activity_producer here - this IS the activity service
            # Error is logged to file, which is sufficient
            return []


# Singleton instance
activity_service = ActivityService()
