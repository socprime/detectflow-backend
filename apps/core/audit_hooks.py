"""Audit hooks for registering callbacks from low-level modules.

This module allows low-level modules (like database.py) to log to audit
without importing activity_producer directly, avoiding circular dependencies.

Usage:
    # In database.py (at module level, no lazy imports)
    from apps.core.audit_hooks import audit_hooks

    # In event handler
    await audit_hooks.log_db_invalidation(reason)

    # In server.py or activity.py (after activity_producer is ready)
    audit_hooks.register_db_callback(activity_producer.log_action)
"""

from collections.abc import Callable, Coroutine


class AuditHooks:
    """Singleton for managing audit callbacks from low-level modules."""

    def __init__(self):
        self._db_invalidation_callback: Callable[..., Coroutine] | None = None

    def register_db_callback(
        self,
        callback: Callable[..., Coroutine],
    ) -> None:
        """Register callback for database connection invalidation events.

        Args:
            callback: Async function matching activity_producer.log_action signature
        """
        self._db_invalidation_callback = callback

    async def log_db_invalidation(self, reason: str) -> None:
        """Log database connection invalidation to audit.

        Args:
            reason: Reason for connection invalidation
        """
        if self._db_invalidation_callback:
            await self._db_invalidation_callback(
                action="warning",
                entity_type="database",
                details=f"Database connection invalidated: {reason}",
                source="system",
                severity="warning",
            )


# Singleton instance
audit_hooks = AuditHooks()
