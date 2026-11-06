"""migrate singleton config to default correlation pipeline

Moves the one ``attack_chain_configuration`` row onto a first-class ``correlation_pipelines`` row
named ``Default``, derives its membership from the singleton's ``monitored_topics``, and attributes
every existing chain to it.

Deliberately NOT done here, each for a stated reason:

* ``attack_chain_configuration`` is **kept**. It is the only surviving record of the pre-migration
  ``correlation_window_seconds`` / ``monitored_topics`` and the cheapest rollback source; a
  follow-up drops it one release later.
* ``correlation_window_seconds`` migrates as **NULL**, not the stored integer. The data plane has
  never read that value (published in the chains-config envelope, dropped on the floor), so today's
  real behaviour is "each HoS rule's ``detection.timeframe`` wins" — which is exactly what NULL
  encodes. Carrying ``300`` forward would turn a dead slider into a live override and silently
  re-bucket every chain.
* ``jobs_status`` seeds as **'stopped'**, never 'starting'. At migration time no member owns a
  tagged-events topic yet, so the rendered ``KAFKA_INPUT_TOPICS`` cannot be trusted to be the
  members' own topics; the runbook's explicit ``POST /run`` is the only start. The legacy
  ``correlation-job`` / ``agent-job`` pods keep running untouched throughout.
* **No Active Threat rows.** Threat monitoring is opt-out: monitored = NOT is_blocked AND not in
  the pipeline's exclusion deny-list. A pipeline that excludes nothing monitors every non-blocked
  threat, so ``Default`` needs zero rows in ``correlation_pipeline_excluded_active_threats``.
* **No ``tagged_events_topic`` assignment.** NULL means "still on the legacy shared topic"; the
  assign endpoint is the only sanctioned writer and the only place a customer's detection pipeline
  restarts. The runbook calls it, per pipeline, in a change window.

Resource columns are seeded with literals rather than left NULL. NULL inherits
``platform_resource_defaults()`` at render time, which resolves to the same numbers today — the
literals pin them to the row so a later change to the platform baseline cannot silently shrink the
one runtime that is carrying production traffic.

Revision ID: c7e1f04b93aa
Revises: dcb0e7251d37
Create Date: 2026-08-12

"""

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c7e1f04b93aa"
down_revision = "dcb0e7251d37"
branch_labels = None
depends_on = None

# Deterministic id: makes the migration idempotent, keeps the generated deployment names
# (corr-<id> / agent-<id>) and topic names stable across re-runs, and lets downgrade target
# exactly the row this migration created.
DEFAULT_CORRELATION_PIPELINE_ID = "00000000-0000-5000-8000-000000000001"

# Deployment-name prefixes. Duplicated as literals on purpose: a migration must not import
# ``apps.*`` (settings would be constructed at upgrade time, in an environment that may not carry
# them). Mirrors CORRELATION_DEPLOYMENT_PREFIX / AGENT_DEPLOYMENT_PREFIX in apps/core/constants.py.
_CORRELATION_DEPLOYMENT_PREFIX = "corr-"
_AGENT_DEPLOYMENT_PREFIX = "agent-"

_INSERT_PIPELINE = f"""
    INSERT INTO correlation_pipelines (
        id, name, jobs_status, threshold_pct, correlation_window_seconds, auto_add_new_topics,
        correlation_parallelism, correlation_taskmanager_memory_mb, correlation_taskmanager_cpu,
        correlation_jobmanager_memory_mb, correlation_jobmanager_cpu,
        agent_parallelism, agent_taskmanager_memory_mb, agent_taskmanager_cpu,
        agent_jobmanager_memory_mb, agent_jobmanager_cpu,
        correlation_deployment_name, agent_deployment_name, created_at, updated_at, updated_by
    )
    SELECT CAST(:pid AS uuid),
           -- UNIQUE (name), and the rebind task resolves this row BY NAME. A pre-existing
           -- 'Default' means an operator created one by hand: keep both rather than fail the
           -- deploy, and let the runbook's 1.2 check surface the collision.
           CASE WHEN EXISTS (SELECT 1 FROM correlation_pipelines WHERE name = 'Default')
                THEN 'Default (migrated singleton)' ELSE 'Default' END,
           'stopped',
           c.threshold_pct,
           NULL,
           c.auto_add_new_topics,
           -- Defaults mirrored from apps/core/settings.py (correlation_* resources and
           -- flink_jobmanager_cpu). The correlation side's 4g / 3.0 is Pemja/Beam off-heap
           -- headroom, reinforced by kubernetes.taskmanager.memory.limit-factor "1.75" in the
           -- deployment template. Keep line references out of this SQL: text() scans comments
           -- too, and a colon-prefixed token there becomes a bind parameter nothing fills.
           1, 4096, 3.0, 2048, 1.0,
           -- Agent-side defaults, likewise from apps/core/settings.py.
           1, 3072, 1.0, 2048, 1.0,
           '{_CORRELATION_DEPLOYMENT_PREFIX}' || :pid, '{_AGENT_DEPLOYMENT_PREFIX}' || :pid,
           now(), c.updated_at, c.updated_by
    FROM attack_chain_configuration c
    WHERE c.id = 1
    ON CONFLICT (id) DO NOTHING
"""

_INSERT_MEMBERS_FROM_MONITORED_TOPICS = """
    INSERT INTO correlation_pipeline_members (correlation_pipeline_id, pipeline_id)
    SELECT CAST(:pid AS uuid), p.id
    FROM pipelines p, attack_chain_configuration c
    WHERE c.id = 1 AND p.destination_topic = ANY (c.monitored_topics)
    ON CONFLICT DO NOTHING
"""

_INSERT_MEMBERS_FROM_AUTO_ADD = """
    INSERT INTO correlation_pipeline_members (correlation_pipeline_id, pipeline_id)
    SELECT CAST(:pid AS uuid), p.id
    FROM pipelines p, attack_chain_configuration c
    WHERE c.id = 1 AND c.auto_add_new_topics IS TRUE AND p.enabled IS TRUE
    ON CONFLICT DO NOTHING
"""

_ATTRIBUTE_CHAINS = """
    UPDATE attack_chains
    SET correlation_pipeline_id = CAST(:pid AS uuid)
    WHERE correlation_pipeline_id IS NULL
"""

_DEPRECATION_COMMENT = (
    "COMMENT ON TABLE attack_chain_configuration IS "
    "'DEPRECATED: superseded by correlation_pipelines. Retained one release for rollback and to "
    "preserve the pre-migration correlation_window_seconds / monitored_topics.'"
)


def upgrade() -> None:
    conn = op.get_bind()
    pid = {"pid": DEFAULT_CORRELATION_PIPELINE_ID}

    # Fresh install / already migrated -> no-op. Every statement below is re-runnable anyway; this
    # guard is what makes a database that never had the singleton skip the whole thing.
    if conn.execute(sa.text("SELECT 1 FROM attack_chain_configuration WHERE id = 1")).first() is None:
        return

    conn.execute(sa.text(_INSERT_PIPELINE), pid)

    for stmt in (
        # Membership is keyed by pipeline_id, and one monitored topic NAME fans out to EVERY
        # pipeline writing it: pipelines.destination_topic has no unique constraint and the old
        # in-event filter matched the name, so every one of those pipelines was in fact correlated.
        # That is the faithful mapping, not a data-quality error, and it does not collide with
        # UNIQUE (pipeline_id) because Default is the only correlation pipeline at migration time.
        _INSERT_MEMBERS_FROM_MONITORED_TOPICS,
        # auto_add_new_topics was applied at start_runtime, not at save, so a never-started runtime
        # can hold an empty monitored_topics while still meaning "every enabled pipeline".
        _INSERT_MEMBERS_FROM_AUTO_ADD,
        # Every chain in the table came from the one runtime this row replaces.
        _ATTRIBUTE_CHAINS,
    ):
        conn.execute(sa.text(stmt), pid)

    op.execute(_DEPRECATION_COMMENT)


def downgrade() -> None:
    conn = op.get_bind()
    pid = {"pid": DEFAULT_CORRELATION_PIPELINE_ID}
    for stmt in (
        # Chains are detached, never deleted: the rows carry analyst-owned status and comments.
        "UPDATE attack_chains SET correlation_pipeline_id = NULL WHERE correlation_pipeline_id = CAST(:pid AS uuid)",
        "DELETE FROM correlation_pipeline_members WHERE correlation_pipeline_id = CAST(:pid AS uuid)",
        # correlation_pipeline_excluded_active_threats cascades on correlation_pipeline_id, and
        # this migration writes no rows there anyway, so deleting the pipeline is enough.
        "DELETE FROM correlation_pipelines WHERE id = CAST(:pid AS uuid)",
    ):
        conn.execute(sa.text(stmt), pid)
    op.execute("COMMENT ON TABLE attack_chain_configuration IS NULL")
