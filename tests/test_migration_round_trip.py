"""Behavioural round-trip of the singleton -> ``Default`` correlation pipeline migration.

Opt-in: this is the only test in the suite that needs a real Postgres, so it is skipped unless
``MIGRATION_TEST_DATABASE_URL`` points at a **disposable** database. It drops and recreates the
public schema on every run.

    MIGRATION_TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/migration_test \\
        uv run pytest tests/test_migration_round_trip.py -v

What it pins that the offline text assertions cannot: that the seeded values actually land, that
membership fans a shared destination-topic name out to every pipeline writing it, that downgrade
restores the pre-state with ``attack_chain_configuration`` intact, and that upgrade is replayable.
"""

import os
import subprocess
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import sqlalchemy as sa

DATABASE_URL = os.getenv("MIGRATION_TEST_DATABASE_URL", "")
MIGRATION_REVISION = "c7e1f04b93aa"
PARENT_REVISION = "dcb0e7251d37"
DEFAULT_PIPELINE_ID = UUID("00000000-0000-5000-8000-000000000001")
PROJECT_ROOT = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="MIGRATION_TEST_DATABASE_URL is not set; this test needs a disposable Postgres",
)


def _alembic(*args: str) -> None:
    """Run alembic against the throwaway database, failing loudly on a non-zero exit."""
    env = {**os.environ, "DATABASE_URL": DATABASE_URL}
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"alembic {' '.join(args)} failed:\n{result.stdout}\n{result.stderr}")


@pytest.fixture
def engine():
    engine = sa.create_engine(DATABASE_URL.replace("postgresql+asyncpg", "postgresql"), future=True)
    with engine.begin() as conn:
        conn.execute(sa.text("DROP SCHEMA public CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA public"))
    _alembic("upgrade", PARENT_REVISION)
    yield engine
    engine.dispose()


def _seed_pipelines(conn, count: int, *, topics: list[str]) -> list[UUID]:
    """Detection pipelines, all still on the legacy shared tagged-events topic (topic IS NULL)."""
    ids = [uuid4() for _ in range(count)]
    for index, (pipeline_id, topic) in enumerate(zip(ids, topics, strict=True)):
        conn.execute(
            sa.text(
                "INSERT INTO pipelines (id, name, source_topics, destination_topic, enabled) "
                "VALUES (:id, :name, '{}', :topic, true)"
            ),
            {"id": pipeline_id, "name": f"P{index}", "topic": topic},
        )
    return ids


def _seed_singleton(conn, **overrides) -> None:
    values = {
        "threshold_pct": 75,
        "correlation_window_seconds": 600,
        "monitored_topics": ["out-a", "out-b"],
        "auto_add_new_topics": True,
        "jobs_status": "starting",
        **overrides,
    }
    conn.execute(
        sa.text(
            "INSERT INTO attack_chain_configuration "
            "(id, threshold_pct, correlation_window_seconds, monitored_topics, auto_add_new_topics, jobs_status) "
            "VALUES (1, :threshold_pct, :correlation_window_seconds, :monitored_topics, "
            ":auto_add_new_topics, :jobs_status)"
        ),
        values,
    )


def _seed_chains(conn, count: int) -> list[UUID]:
    """Two chains and the rows their NOT NULL foreign keys require."""
    conn.execute(
        sa.text("INSERT INTO active_threats (tdm_id, name) VALUES ('AT-1', 'Threat One')"),
    )
    hos_id = uuid4()
    conn.execute(
        sa.text("INSERT INTO high_order_sigmas (id, tdm_id, name) VALUES (:id, 'HOS-1', 'Chain rule')"),
        {"id": hos_id},
    )
    chain_ids = [uuid4() for _ in range(count)]
    for index, chain_id in enumerate(chain_ids):
        conn.execute(
            sa.text(
                "INSERT INTO attack_chains "
                "(id, hos_rule_id, active_threat_id, chain_type, hostname, status, first_seen, last_seen, "
                " window_start, coverage) "
                "VALUES (:id, :hos, 'AT-1', 'HOS', :host, 'new', now(), now(), now(), 100.00)"
            ),
            {"id": chain_id, "hos": hos_id, "host": f"host-{index}"},
        )
    return chain_ids


def _default_row(conn):
    return (
        conn.execute(sa.text("SELECT * FROM correlation_pipelines WHERE id = :id"), {"id": DEFAULT_PIPELINE_ID})
        .mappings()
        .first()
    )


def _member_count(conn) -> int:
    return conn.execute(
        sa.text("SELECT count(*) FROM correlation_pipeline_members WHERE correlation_pipeline_id = :id"),
        {"id": DEFAULT_PIPELINE_ID},
    ).scalar_one()


class TestFullMigration:
    def test_upgrade_creates_default_attributes_chains_and_round_trips(self, engine) -> None:
        with engine.begin() as conn:
            # "out-a" is written by TWO detection pipelines: the old in-event filter matched the
            # topic NAME, so both were in fact correlated and both must become members.
            pipeline_ids = _seed_pipelines(conn, 3, topics=["out-a", "out-a", "out-b"])
            _seed_singleton(conn)
            chain_ids = _seed_chains(conn, 2)

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            row = _default_row(conn)
            assert row is not None
            assert row["name"] == "Default"
            assert row["jobs_status"] == "stopped"  # constraint 5
            assert row["threshold_pct"] == 75
            assert row["correlation_window_seconds"] is None  # constraint 3 / D3
            assert row["auto_add_new_topics"] is True
            assert row["correlation_taskmanager_memory_mb"] == 4096  # constraint 9
            assert row["correlation_taskmanager_cpu"] == 3.0
            assert row["agent_taskmanager_memory_mb"] == 3072
            assert row["correlation_deployment_name"] == f"corr-{DEFAULT_PIPELINE_ID}"
            assert row["agent_deployment_name"] == f"agent-{DEFAULT_PIPELINE_ID}"

            assert _member_count(conn) == 3  # constraint 4: the shared name fanned out
            assert (
                conn.execute(sa.text("SELECT count(*) FROM correlation_pipeline_excluded_active_threats")).scalar_one()
                == 0
            )

            attributed = conn.execute(
                sa.text("SELECT count(*) FROM attack_chains WHERE correlation_pipeline_id = :id"),
                {"id": DEFAULT_PIPELINE_ID},
            ).scalar_one()
            assert attributed == len(chain_ids)

            # This migration assigns no topics: that is the assign endpoint's job, per pipeline.
            still_null = conn.execute(
                sa.text("SELECT count(*) FROM pipelines WHERE tagged_events_topic IS NULL")
            ).scalar_one()
            assert still_null == len(pipeline_ids)

            # The singleton survives as the rollback source.
            singleton = (
                conn.execute(sa.text("SELECT * FROM attack_chain_configuration WHERE id = 1")).mappings().first()
            )
            assert singleton is not None
            assert singleton["correlation_window_seconds"] == 600

        _alembic("downgrade", "-1")

        with engine.begin() as conn:
            assert _default_row(conn) is None
            assert _member_count(conn) == 0
            orphaned = conn.execute(
                sa.text("SELECT count(*) FROM attack_chains WHERE correlation_pipeline_id IS NULL")
            ).scalar_one()
            assert orphaned == len(chain_ids)  # detached, never deleted
            assert conn.execute(sa.text("SELECT count(*) FROM attack_chain_configuration")).scalar_one() == 1

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            assert _default_row(conn) is not None
            assert _member_count(conn) == 3

        # Replaying an already-applied migration must be a no-op, not a duplicate-key error.
        _alembic("downgrade", "-1")
        _alembic("upgrade", MIGRATION_REVISION)
        _alembic("upgrade", MIGRATION_REVISION)


class TestEdgeCases:
    def test_no_monitored_topics_and_no_auto_add_gives_a_member_less_default(self, engine) -> None:
        """Still seeded ``stopped``: a member-less pipeline can never start (constraint 5)."""
        with engine.begin() as conn:
            _seed_pipelines(conn, 1, topics=["out-a"])
            _seed_singleton(conn, monitored_topics=[], auto_add_new_topics=False)

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            row = _default_row(conn)
            assert row is not None
            assert row["jobs_status"] == "stopped"
            assert _member_count(conn) == 0

    def test_auto_add_alone_takes_every_enabled_pipeline(self, engine) -> None:
        """auto_add was applied at start, not at save, so an empty list still meant "all enabled"."""
        with engine.begin() as conn:
            _seed_pipelines(conn, 2, topics=["out-a", "out-b"])
            conn.execute(sa.text("UPDATE pipelines SET enabled = false WHERE destination_topic = 'out-b'"))
            _seed_singleton(conn, monitored_topics=[], auto_add_new_topics=True)

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            assert _member_count(conn) == 1

    def test_fresh_install_without_a_singleton_row_is_a_no_op(self, engine) -> None:
        with engine.begin() as conn:
            _seed_pipelines(conn, 1, topics=["out-a"])

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            assert conn.execute(sa.text("SELECT count(*) FROM correlation_pipelines")).scalar_one() == 0

    def test_a_hand_made_default_does_not_break_the_deploy(self, engine) -> None:
        """UNIQUE (name) would abort the migration; the collision falls back to a suffixed name."""
        with engine.begin() as conn:
            _seed_pipelines(conn, 1, topics=["out-a"])
            _seed_singleton(conn)
            conn.execute(
                sa.text("INSERT INTO correlation_pipelines (id, name) VALUES (:id, 'Default')"),
                {"id": uuid4()},
            )

        _alembic("upgrade", MIGRATION_REVISION)

        with engine.begin() as conn:
            row = _default_row(conn)
            assert row is not None
            assert row["name"] == "Default (migrated singleton)"
