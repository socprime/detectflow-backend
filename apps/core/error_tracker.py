"""Error tracking for anti-flood audit logging.

Prevents spamming audit logs with repeated infrastructure errors.

Error Key Naming Convention:
    - Use lowercase with underscores
    - Format: {subsystem}_{operation}[_{entity_id}]
    - Examples:
        - "kafka_connect_metrics" - Kafka connection for metrics
        - "flink_create_{pipeline_id}" - Flink deployment creation
        - "db_connection_invalidated" - Database connection issues
        - "kafka_sync_filters_{pipeline_id}" - Filter sync to Kafka

Usage:
    from apps.core.error_tracker import ErrorTracker

    # In error handler:
    if ErrorTracker.should_log("kafka_connect_metrics"):
        await activity_producer.log_action(...)

    # For entity-specific errors:
    if ErrorTracker.should_log(f"kafka_sync_filters_{pipeline_id}"):
        await activity_producer.log_action(...)
"""

import threading
from datetime import UTC, datetime
from typing import ClassVar


class ErrorTracker:
    """Track recent errors to prevent audit log flooding.

    Only logs the same error once per cooldown period (default 5 minutes).
    Thread-safe implementation using threading.Lock.
    """

    # Class-level storage for error timestamps
    _last_logged: ClassVar[dict[str, datetime]] = {}
    _lock: ClassVar[threading.Lock] = threading.Lock()
    COOLDOWN_SECONDS: ClassVar[int] = 300  # 5 minutes

    @classmethod
    def should_log(cls, error_key: str) -> bool:
        """Check if this error should be logged (not in cooldown).

        Thread-safe check and update of last logged time.

        Args:
            error_key: Unique key for the error (e.g., "kafka_connect_metrics")

        Returns:
            True if error should be logged, False if in cooldown
        """
        now = datetime.now(UTC)

        with cls._lock:
            last_time = cls._last_logged.get(error_key)

            if last_time is None or (now - last_time).total_seconds() >= cls.COOLDOWN_SECONDS:
                cls._last_logged[error_key] = now
                return True
            return False

    @classmethod
    def reset(cls, error_key: str | None = None) -> None:
        """Reset error tracking (useful for tests).

        Args:
            error_key: Specific key to reset, or None to reset all
        """
        with cls._lock:
            if error_key:
                cls._last_logged.pop(error_key, None)
            else:
                cls._last_logged.clear()
