from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text

from apps.core.database import engine
from apps.core.enums import HealthCheckStatus
from apps.services.health_check.platforms.base import BaseHealthChecker


def _get_alembic_config_path() -> Path:
    """Return path to alembic.ini at project root (from this file: parents[4])."""
    project_root = Path(__file__).resolve().parents[4]
    return project_root / "alembic.ini"


class PostgreSQLHealthChecker(BaseHealthChecker):
    """Health checker for PostgreSQL: connection and schema relevance (Alembic head)."""

    def __init__(self) -> None:
        """Register checks: Database Connection, Database Relevance Check."""
        self.checks = {
            "Database Connection": self._check_database_connection,
            "Database Relevance Check": self._check_database_relevance,
        }

    async def _check_database_connection(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check if the database is reachable (execute SELECT 1).

        Returns:
            (OPERATIONAL, descriptions) on success; exceptions propagate to base.
        """
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return HealthCheckStatus.OPERATIONAL, ["Connected to the database"]

    async def _check_database_relevance(self) -> tuple[HealthCheckStatus, list[str]]:
        """Check that DB schema matches app: alembic_version.revision == migration head.

        Returns:
            (OPERATIONAL, msg) if DB is at head; (ERROR, msg) if missing/alembic.ini
            not found/no head, or schema behind (suggests 'alembic upgrade head').
        """
        async with engine.connect() as conn:
            result = await conn.execute(text("SELECT version_num FROM alembic_version"))
            rows = result.fetchall()
        if not rows:
            return HealthCheckStatus.ERROR, ["alembic_version missing; DB may be empty or not migrated"]
        if len(rows) > 1:
            return HealthCheckStatus.ERROR, [
                "Multiple rows in alembic_version; schema state is ambiguous. Ensure single revision."
            ]
        db_revision = rows[0][0]
        config_path = _get_alembic_config_path()
        if not config_path.exists():
            return HealthCheckStatus.ERROR, ["alembic.ini not found; cannot determine expected revision"]

        config = Config(str(config_path))
        script = ScriptDirectory.from_config(config)
        head_revision = script.get_current_head()
        if head_revision is None:
            return HealthCheckStatus.ERROR, ["No migration head in project"]

        is_relevant = db_revision == head_revision
        if is_relevant:
            return HealthCheckStatus.OPERATIONAL, ["DB schema is relevant (revision matches head)"]
        return HealthCheckStatus.ERROR, [
            f"DB schema is behind: DB at {db_revision}, head is {head_revision}. Run: alembic upgrade head"
        ]
