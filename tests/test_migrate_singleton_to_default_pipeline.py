"""Offline guards for the singleton -> ``Default`` correlation pipeline migration.

Tests here run without a database (``tests/conftest.py``), so they load the migration module and
assert on its constants and SQL text. That is enough to pin the decisions that are expensive to get
wrong and invisible in a green test run: the window migrating as NULL, the seed status, the
resource numbers, and the fact that nothing is dropped or made NOT NULL.

The behavioural half — that running it against Postgres produces those rows and round-trips — is
``tests/test_migration_round_trip.py``, which is opt-in because it needs a real database.
"""

import importlib.util
import re
from pathlib import Path
from uuid import UUID

import pytest
import sqlalchemy

MIGRATION_FILENAME = "c7e1f04b93aa_migrate_singleton_config_to_default_pipeline.py"

# DNS-1123 label: lowercase alphanumeric or '-', start/end alphanumeric, <= 63 chars. The generated
# deployment names go straight into Kubernetes object names.
_DNS_1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")


def _load_migration_module():
    """Load the migration by path, the way this repo unit-tests migrations offline."""
    module_path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / MIGRATION_FILENAME
    spec = importlib.util.spec_from_file_location("test_default_pipeline_migration_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def migration():
    return _load_migration_module()


@pytest.fixture(scope="module")
def source() -> str:
    return (Path(__file__).resolve().parents[1] / "alembic" / "versions" / MIGRATION_FILENAME).read_text()


class TestDefaultPipelineId:
    def test_is_a_valid_uuid(self, migration) -> None:
        """Deterministic, so re-running the migration is a no-op and downgrade can target one row."""
        assert UUID(migration.DEFAULT_CORRELATION_PIPELINE_ID)

    @pytest.mark.parametrize("prefix", ["corr-", "agent-"])
    def test_generated_deployment_names_are_dns_1123_labels(self, migration, prefix: str) -> None:
        name = f"{prefix}{migration.DEFAULT_CORRELATION_PIPELINE_ID}"

        assert _DNS_1123_LABEL.match(name), name
        assert len(name) <= 63

    def test_deployment_name_prefixes_match_the_constants_module(self, migration) -> None:
        """The migration cannot import apps.*, so the prefixes are duplicated — pin them together."""
        from apps.core.constants import AGENT_DEPLOYMENT_PREFIX, CORRELATION_DEPLOYMENT_PREFIX

        assert migration._CORRELATION_DEPLOYMENT_PREFIX == CORRELATION_DEPLOYMENT_PREFIX
        assert migration._AGENT_DEPLOYMENT_PREFIX == AGENT_DEPLOYMENT_PREFIX


class TestSeededValues:
    def test_correlation_window_is_written_as_null(self, migration) -> None:
        """D3: the stored integer is NOT carried forward.

        The data plane never read that value, so today's real behaviour is "each HoS rule's
        detection.timeframe wins" — which is what NULL encodes. Carrying 300 across would turn a
        dead slider into a live override and silently re-bucket every chain.
        """
        insert = migration._INSERT_PIPELINE
        window_line = next(line for line in insert.splitlines() if "NULL," in line)

        assert "correlation_window_seconds" in insert
        assert window_line.strip().startswith("NULL,")
        assert "c.correlation_window_seconds" not in insert

    def test_jobs_status_is_seeded_stopped_with_no_starting_branch(self, migration) -> None:
        """Constraint 5: no member owns a tagged-events topic yet, so a started job would crash-loop."""
        assert "'stopped'," in migration._INSERT_PIPELINE
        assert "starting" not in migration._INSERT_PIPELINE
        assert "c.jobs_status" not in migration._INSERT_PIPELINE

    def test_resource_literals_are_the_production_numbers(self, migration) -> None:
        """Constraint 9: 4g / 3.0 correlation and 3g / 1.0 agent, never the mock's 3g / 1.0 pair."""
        assert "1, 4096, 3.0, 2048, 1.0," in migration._INSERT_PIPELINE
        assert "1, 3072, 1.0, 2048, 1.0," in migration._INSERT_PIPELINE

    def test_threshold_and_auto_add_are_carried_from_the_singleton(self, migration) -> None:
        assert "c.threshold_pct," in migration._INSERT_PIPELINE
        assert "c.auto_add_new_topics," in migration._INSERT_PIPELINE


class TestIdempotency:
    def test_every_insert_tolerates_a_re_run(self, migration) -> None:
        inserts = (
            migration._INSERT_PIPELINE,
            migration._INSERT_MEMBERS_FROM_MONITORED_TOPICS,
            migration._INSERT_MEMBERS_FROM_AUTO_ADD,
        )

        for statement in inserts:
            assert "ON CONFLICT" in statement and "DO NOTHING" in statement

    def test_chain_attribution_only_touches_unattributed_rows(self, migration) -> None:
        """Re-running must not re-stamp a chain that a later pipeline delete detached on purpose."""
        assert "WHERE correlation_pipeline_id IS NULL" in migration._ATTRIBUTE_CHAINS

    def test_upgrade_is_guarded_by_the_singleton_row(self, source: str) -> None:
        """A fresh install has no singleton row and must come out of upgrade() untouched."""
        assert "SELECT 1 FROM attack_chain_configuration WHERE id = 1" in source
        assert "return" in source.split("def upgrade")[1].split("conn.execute(sa.text(_INSERT_PIPELINE)")[0]


class TestWhatItMustNotDo:
    def test_nothing_is_made_not_null(self, source: str) -> None:
        """Constraint 2: ``attack_chains.correlation_pipeline_id`` stays nullable.

        A NOT NULL violation inside the single serialised chains-analysed consumer loop is a poison
        record that stalls ingestion for every customer. Attribution is enforced by a monitoring
        query, not by a constraint.
        """
        assert "SET NOT NULL" not in source
        assert "nullable=False" not in source

    def test_the_singleton_table_survives(self, source: str) -> None:
        """It is the only surviving copy of the pre-migration window / monitored topics."""
        assert "drop_table" not in source
        assert "DROP TABLE" not in source.upper().replace("DROP TABLE IF EXISTS", "")

    def test_no_active_threat_row_is_written(self, migration) -> None:
        """D8: monitoring is opt-out, so a pipeline that excludes nothing monitors everything.

        Seeding "the current enabled set" would materialise an allow-list and re-introduce exactly
        the trap the deny-list was chosen to remove.
        """
        written = "".join(
            (
                migration._INSERT_PIPELINE,
                migration._INSERT_MEMBERS_FROM_MONITORED_TOPICS,
                migration._INSERT_MEMBERS_FROM_AUTO_ADD,
                migration._ATTRIBUTE_CHAINS,
            )
        )

        assert "active_threat" not in written

    def test_no_tagged_events_topic_is_assigned(self, migration) -> None:
        """The assign endpoint is the only sanctioned writer; it also provisions the topic.

        Asserted on the executed SQL rather than the file: the module docstring says the same thing
        in prose, and a doc sentence must not be able to fail this test.
        """
        written = "".join(
            (
                migration._INSERT_PIPELINE,
                migration._INSERT_MEMBERS_FROM_MONITORED_TOPICS,
                migration._INSERT_MEMBERS_FROM_AUTO_ADD,
                migration._ATTRIBUTE_CHAINS,
            )
        )

        assert "tagged_events_topic" not in written


class TestDowngrade:
    def test_deletes_only_rows_keyed_by_the_deterministic_id(self, source: str) -> None:
        body = source.split("def downgrade")[1]

        assert body.count("CAST(:pid AS uuid)") == 3
        assert "DELETE FROM correlation_pipelines WHERE id = CAST(:pid AS uuid)" in body
        assert "DELETE FROM correlation_pipeline_members WHERE correlation_pipeline_id = CAST(:pid AS uuid)" in body

    def test_detaches_chains_instead_of_deleting_them(self, source: str) -> None:
        """Chains carry analyst-owned status and comments; downgrade must not destroy them."""
        body = source.split("def downgrade")[1]

        assert "UPDATE attack_chains SET correlation_pipeline_id = NULL" in body
        assert "DELETE FROM attack_chains" not in body

    def test_does_not_drop_the_singleton_table(self, source: str) -> None:
        body = source.split("def downgrade")[1]

        assert "attack_chain_configuration" in body  # only the COMMENT is reverted
        assert "drop_table" not in body


class TestRevisionChain:
    def test_declares_a_single_parent(self, migration) -> None:
        assert migration.down_revision == "dcb0e7251d37"
        assert migration.revision == "c7e1f04b93aa"
        assert migration.branch_labels is None


class TestBindParameters:
    """``text()`` finds ``:name`` with a regex, so SQL COMMENTS are scanned too.

    A line reference like ``-- see apps/core/settings.py, :186-188`` becomes a bind parameter
    named ``186``; only ``:pid`` is ever supplied, so the upgrade dies with
    ``A value is required for bind parameter '186'`` — at deploy time, inside the entrypoint's
    ``alembic upgrade heads``, which crash-loops the pod. Nothing here needs a database, so the
    same failure is one assertion away instead of one deploy away.
    """

    def test_no_sql_constant_binds_anything_but_pid(self, migration) -> None:
        offenders: dict[str, list[str]] = {}
        for name, value in vars(migration).items():
            if not isinstance(value, str) or not re.search(r"\b(INSERT|UPDATE|DELETE|SELECT|COMMENT)\b", value):
                continue
            unexpected = sorted(set(sqlalchemy.text(value).compile().binds) - {"pid"})
            if unexpected:
                offenders[name] = unexpected

        assert not offenders, f"unintended bind parameters (a ':NNN' in a comment?): {offenders}"


def test_no_migration_writes_a_colon_number_token() -> None:
    """The same trap, across every migration in the tree — not just this one.

    Matches a bare ``:`` before a digit, which in SQL text is a bind parameter and in prose is a
    line number someone meant as documentation. Python slices (``body[:500]``) and paths
    (``settings.py:177``) are excluded: their colon follows a word or bracket character, which is
    exactly what SQLAlchemy's own regex refuses to treat as a bind.
    """
    versions = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    pattern = re.compile(r"(?<![\[\w:$]):\d")
    offenders = [
        f"{path.name}:{number}"
        for path in sorted(versions.glob("*.py"))
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if pattern.search(line)
    ]

    assert not offenders, "':NNN' becomes a bind parameter when the SQL reaches text(): " + ", ".join(offenders)


def test_every_revision_id_in_the_tree_is_unique() -> None:
    """Hand-picked revision ids collide, and alembic degrades quietly when they do.

    This repo writes migrations by hand, so ids like ``a1b2c3d4e5f6`` get chosen twice. Alembic
    answers a duplicate with a ``UserWarning`` and then reports **two heads**, which the Postgres
    health check turns into a startup ERROR — a long way from the actual cause. Caught here instead:
    the same scan alembic does, one assertion.
    """
    versions = Path(__file__).resolve().parents[1] / "alembic" / "versions"
    seen: dict[str, str] = {}
    duplicates: list[str] = []

    for path in sorted(versions.glob("*.py")):
        for line in path.read_text().splitlines():
            if line.startswith("revision = "):
                revision_id = line.split("=", 1)[1].strip().strip("\"'")
                if revision_id in seen:
                    duplicates.append(f"{revision_id}: {seen[revision_id]} and {path.name}")
                seen[revision_id] = path.name
                break

    assert not duplicates, "duplicate alembic revision ids: " + "; ".join(duplicates)
