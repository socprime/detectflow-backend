"""Flink configuration manager.

This module provides business logic for managing Flink resource configuration:
- Parameter schema documentation for UI
- Config resolution with priority chain
"""

from apps.core.models import Pipeline
from apps.core.schemas import (
    FlinkCategoryInfo,
    FlinkDefaultsResponse,
    FlinkDefaultsSchemaResponse,
    FlinkParameterInfo,
    FlinkResourceConfig,
)

# =============================================================================
# Parameter Schema (for UI documentation endpoint)
# =============================================================================

FLINK_PARAMETERS: list[FlinkParameterInfo] = [
    # === Resources ===
    FlinkParameterInfo(
        name="parallelism",
        type="integer",
        default=1,
        min=1,
        max=128,
        title="Parallelism",
        description="Number of parallel workers processing events. Higher values increase throughput.",
        impact="restart",
        category="resources",
        tips=[
            "Higher = more throughput, but must match your Kafka partition count",
            "Going above partition count wastes resources",
            "Doubling parallelism roughly doubles throughput (if you have enough partitions)",
        ],
    ),
    FlinkParameterInfo(
        name="taskmanager_memory_mb",
        type="integer",
        default=2048,
        min=1536,
        max=131072,
        unit="MB",
        title="TaskManager Memory",
        description=(
            "Total memory per worker pod, split between the Flink JVM and the separate "
            "Python/Polars matcher process. More rules and events require more memory."
        ),
        impact="restart",
        category="resources",
        tips=[
            "2 GB (default) handles most workloads",
            "Increase if pipeline keeps crashing or restarting (OOMKilled)",
            "More rules and longer windows = more memory needed",
        ],
    ),
    FlinkParameterInfo(
        name="taskmanager_cpu",
        type="number",
        default=1.5,
        min=1.0,
        max=16.0,
        unit="cores",
        title="TaskManager CPU",
        description="CPU cores per worker. Each worker handles one parallelism slot.",
        impact="restart",
        category="resources",
        tips=[
            "1.5 cores (default) — headroom for matching + Kafka fetch on one slot",
            "1.0 often CPU-bounds the matcher under load; raise further only if metrics show it",
            "parallelism=N creates N workers, each with this many cores",
        ],
    ),
    FlinkParameterInfo(
        name="jobmanager_memory_mb",
        type="integer",
        default=2048,
        min=512,
        max=32768,
        unit="MB",
        title="Job Manager Memory",
        description="Memory for the JobManager, which coordinates the workers and checkpoints.",
        impact="restart",
        category="resources",
        tips=[
            "2 GB (default) is enough for a single-job deployment",
            "Raise it only if the JobManager itself is OOM-killed, not for throughput",
        ],
    ),
    FlinkParameterInfo(
        name="jobmanager_cpu",
        type="number",
        default=1.0,
        min=0.5,
        max=16.0,
        unit="cores",
        title="Job Manager CPU",
        description="CPU cores for the JobManager. Coordination work only — it processes no events.",
        impact="restart",
        category="resources",
        tips=["1 core (default) is sufficient", "Scaling workers does not require scaling this"],
    ),
    # === Processing ===
    FlinkParameterInfo(
        name="window_size_sec",
        type="integer",
        default=30,
        min=10,
        max=120,
        unit="seconds",
        title="Window Size",
        description="How often events are processed in batches. Affects detection latency.",
        impact="restart",
        category="processing",
        tips=[
            "30 seconds (default) — balanced latency and throughput",
            "Lower (10-15s) — faster detection, slightly less throughput",
            "Higher (60s) — can cause lag during traffic spikes, not recommended",
        ],
    ),
    FlinkParameterInfo(
        name="checkpoint_interval_sec",
        type="integer",
        default=60,
        min=30,
        max=600,
        unit="seconds",
        title="Checkpoint Interval",
        description="How often pipeline state is saved. Affects recovery time after crashes.",
        impact="restart",
        category="processing",
        tips=[
            "60 seconds (default) — good balance for most pipelines",
            "Lower = faster recovery, more disk usage",
            "Higher = slower recovery, less disk usage",
        ],
    ),
    # === Autoscaler ===
    FlinkParameterInfo(
        name="autoscaler_enabled",
        type="boolean",
        default=False,
        title="Enable Autoscaling",
        description="Automatically add or remove workers based on load.",
        impact="restart",
        category="autoscaler",
        tips=[
            "Enable for unpredictable or variable traffic patterns",
            "Scales up when overloaded, scales down when idle",
            "Reacts to sustained load changes, ignores short spikes",
        ],
    ),
    FlinkParameterInfo(
        name="autoscaler_min_parallelism",
        type="integer",
        default=1,
        min=1,
        max=24,
        title="Min Parallelism",
        description="Minimum workers always running. Affects cold start and baseline cost.",
        impact="restart",
        category="autoscaler",
        requires="autoscaler_enabled",
        tips=[
            "Higher = faster response to traffic spikes, but higher cost when idle",
            "Lower = saves resources, but slower to react to sudden load",
        ],
    ),
    FlinkParameterInfo(
        name="autoscaler_max_parallelism",
        type="integer",
        default=24,
        min=1,
        max=720,
        title="Max Parallelism",
        description="Maximum workers during peak load. Limits resource usage.",
        impact="restart",
        category="autoscaler",
        requires="autoscaler_enabled",
        tips=[
            "Set to your Kafka partition count — going higher won't help",
            "Higher = handles bigger spikes, but reserves more cluster resources",
        ],
    ),
    # === Correlation (one correlation pipeline's chain-forming tuning) ===
    # Every parameter below is `hot_reload` because its only transport is the pipeline's
    # chains-config topic, which the running matcher reads from broadcast state. That must stay in
    # step with CHAINS_CONFIG_HOT_FIELDS in apps/managers/correlation_pipelines.py — a test asserts
    # the two agree, because `impact` is what greys the field out in the UI while a job is up.
    FlinkParameterInfo(
        name="threshold_pct",
        type="integer",
        default=60,
        min=50,
        max=100,
        unit="%",
        title="Threshold for chain forming",
        description="Minimum share of a high-order Sigma rule's steps that must match before a chain forms.",
        impact="hot_reload",
        category="correlation",
        tips=["Multiples of 5 only", "Lower = more chains, more noise", "Applies with no restart"],
    ),
    FlinkParameterInfo(
        name="correlation_window_seconds",
        type="integer",
        default=None,
        nullable=True,
        min=30,
        max=10800,
        unit="seconds",
        title="Correlation Window",
        description=(
            "Explicit correlation window for this pipeline. Null inherits each high-order Sigma "
            "rule's own detection timeframe."
        ),
        impact="hot_reload",
        category="correlation",
        tips=[
            "30 seconds to 3 hours",
            "Leave empty to inherit the rule's timeframe",
            "Changing it re-buckets chain ids: in-flight chains close under the old window",
        ],
    ),
    # The create/edit form shows the name pattern only while auto-add is on.
    # `requires` already models exactly that (it drives the autoscaler sub-fields today), so
    # the pair below needs no new schema field.
    FlinkParameterInfo(
        name="auto_add_new_pipelines",
        type="boolean",
        default=False,
        title="Auto-add new pipelines",
        description="Automatically add newly created detection pipelines to this correlation pipeline's membership.",
        impact="hot_reload",
        category="correlation",
        tips=["Off by default", "Membership changes restart only the correlation job"],
    ),
    FlinkParameterInfo(
        name="auto_add_pipeline_name_pattern",
        type="string",
        default=None,
        nullable=True,
        title="Pipeline name pattern",
        requires="auto_add_new_pipelines",
        description=(
            "Shell-style glob matched case-sensitively against a new detection pipeline's "
            "name. Null or empty matches every new pipeline."
        ),
        impact="hot_reload",
        category="correlation",
        tips=[
            "Glob, not regex — e.g. `cust1-*`",
            "Empty means every new pipeline",
            "Max 255 characters; spaces are allowed (pipeline names can contain them)",
        ],
    ),
    # === Correlation Flink job ===
    FlinkParameterInfo(
        name="correlation_parallelism",
        type="integer",
        default=1,
        min=1,
        max=64,
        title="Parallelism",
        description="Parallel correlation workers. Chains are keyed by hostname, so this is bounded by host spread.",
        impact="restart",
        category="correlation_job",
        tips=[
            "1 (default) handles a single tenant's tagged-event volume",
            "Must not exceed the member tagged-events topics' combined partition count",
        ],
    ),
    FlinkParameterInfo(
        name="correlation_taskmanager_memory_mb",
        type="integer",
        default=4096,
        min=2048,
        max=131072,
        unit="MB",
        title="Task Manager Memory",
        description=(
            "Memory per correlation worker. The Python worker's off-heap memory is not counted in "
            "the JVM process size, so this runs higher than a detection pipeline's."
        ),
        impact="restart",
        category="correlation_job",
        tips=["4 GB is the validated production value", "Below 3 GB the Python worker can stall checkpoints"],
    ),
    FlinkParameterInfo(
        name="correlation_taskmanager_cpu",
        type="number",
        default=3.0,
        min=0.5,
        max=32.0,
        unit="cores",
        title="Task Manager CPU",
        description="CPU cores per correlation worker, shared between the JVM and the embedded Python interpreter.",
        impact="restart",
        category="correlation_job",
        tips=["3 cores is the validated production value", "1 core starves the Python worker and stalls checkpoints"],
    ),
    FlinkParameterInfo(
        name="correlation_jobmanager_memory_mb",
        type="integer",
        default=2048,
        min=512,
        max=32768,
        unit="MB",
        title="Job Manager Memory",
        description="Memory for the correlation job's JobManager (coordination and checkpointing only).",
        impact="restart",
        category="correlation_job",
        tips=["2 GB (default) is enough", "Raise only if the JobManager itself is OOM-killed"],
    ),
    FlinkParameterInfo(
        name="correlation_jobmanager_cpu",
        type="number",
        default=1.0,
        min=0.5,
        max=16.0,
        unit="cores",
        title="Job Manager CPU",
        description="CPU cores for the correlation job's JobManager. It processes no events.",
        impact="restart",
        category="correlation_job",
        tips=["1 core (default) is sufficient", "Scaling workers does not require scaling this"],
    ),
    # === Agentic (LLM enrichment) Flink job ===
    FlinkParameterInfo(
        name="agent_parallelism",
        type="integer",
        default=1,
        min=1,
        max=64,
        title="Parallelism",
        description="Parallel LLM-enrichment workers. Bounded by the chains-raw partition count.",
        impact="restart",
        category="agent_job",
        tips=[
            "1 (default) matches the per-pipeline chains-raw topic",
            "Raising it multiplies the LLM call rate — the token bucket is per worker",
        ],
    ),
    FlinkParameterInfo(
        name="agent_taskmanager_memory_mb",
        type="integer",
        default=3072,
        min=1024,
        max=131072,
        unit="MB",
        title="Task Manager Memory",
        description="Memory per agent worker. The embedded interpreter and prompt buffers dominate it.",
        impact="restart",
        category="agent_job",
        tips=["3 GB (default) is the validated value", "Chains are small; this rarely needs raising"],
    ),
    FlinkParameterInfo(
        name="agent_taskmanager_cpu",
        type="number",
        default=1.0,
        min=0.5,
        max=32.0,
        unit="cores",
        title="Task Manager CPU",
        description="CPU cores per agent worker. The job spends most of its time waiting on the LLM endpoint.",
        impact="restart",
        category="agent_job",
        tips=["1 core (default) is sufficient", "Latency here is the provider's, not the CPU's"],
    ),
    FlinkParameterInfo(
        name="agent_jobmanager_memory_mb",
        type="integer",
        default=2048,
        min=512,
        max=32768,
        unit="MB",
        title="Job Manager Memory",
        description="Memory for the agent job's JobManager (coordination and checkpointing only).",
        impact="restart",
        category="agent_job",
        tips=["2 GB (default) is enough", "Raise only if the JobManager itself is OOM-killed"],
    ),
    FlinkParameterInfo(
        name="agent_jobmanager_cpu",
        type="number",
        default=1.0,
        min=0.5,
        max=16.0,
        unit="cores",
        title="Job Manager CPU",
        description="CPU cores for the agent job's JobManager. It processes no events.",
        impact="restart",
        category="agent_job",
        tips=["1 core (default) is sufficient", "Scaling workers does not require scaling this"],
    ),
]

FLINK_CATEGORIES: dict[str, FlinkCategoryInfo] = {
    "resources": FlinkCategoryInfo(
        title="Resources",
        description="Resource allocation for Flink workers",
        order=1,
    ),
    "processing": FlinkCategoryInfo(
        title="Processing",
        description="Event processing and state management settings",
        order=2,
    ),
    "autoscaler": FlinkCategoryInfo(
        title="Autoscaling",
        description="Automatic parallelism adjustment based on load",
        order=3,
    ),
    "correlation": FlinkCategoryInfo(
        title="Correlation",
        description="Chain-forming tuning for one correlation pipeline (hot-reloaded via chains-config)",
        order=4,
    ),
    "correlation_job": FlinkCategoryInfo(
        title="Correlation Flink Job",
        description="Compute budget for one correlation pipeline's correlation job",
        order=5,
    ),
    "agent_job": FlinkCategoryInfo(
        title="Agentic Flink Job",
        description="Compute budget for one correlation pipeline's agentic (LLM enrichment) job",
        order=6,
    ),
}

IMPACT_DESCRIPTIONS: dict[str, str] = {
    "restart": "Changing this value requires pipeline restart (~30-60 seconds downtime)",
    "hot_reload": "Changes apply immediately without pipeline restart",
}


class FlinkConfigManager:
    """Manager for Flink resource configuration.

    Handles:
    - Config resolution with priority chain
    - Parameter schema for UI
    - Auto-calculation of derived values
    """

    @staticmethod
    def get_schema() -> FlinkDefaultsSchemaResponse:
        """Get full parameter schema for UI documentation endpoint."""
        return FlinkDefaultsSchemaResponse(
            parameters=FLINK_PARAMETERS,
            categories=FLINK_CATEGORIES,
            impact_descriptions=IMPACT_DESCRIPTIONS,
        )

    @staticmethod
    def resolve_config(
        request_config: FlinkResourceConfig | None,
        pipeline: Pipeline | None,
        defaults: FlinkDefaultsResponse,
    ) -> FlinkDefaultsResponse:
        """Resolve final config with priority chain.

        Priority (highest to lowest):
        1. Request values (if provided and not None)
        2. Existing pipeline values (if not None)
        3. Global defaults from database
        4. System defaults (built into FlinkDefaultsResponse)

        Args:
            request_config: Optional config from API request
            pipeline: Optional existing pipeline (for updates)
            defaults: Global defaults from database

        Note:
            Detection pipelines only. Correlation-pipeline parameters are documented in
            ``FLINK_PARAMETERS`` for the UI, but they are resolved by
            ``CorrelationPipelinesManager`` against its own row + ``settings`` fallbacks — the two
            entities have different columns, so one resolver for both would have to fake half of
            each.

        Returns:
            Resolved configuration with all values filled
        """
        result: dict = {}

        # Field mapping: schema field -> pipeline model field
        field_mapping = {
            "parallelism": "parallelism",
            "taskmanager_memory_mb": "taskmanager_memory_mb",
            "taskmanager_cpu": "taskmanager_cpu",
            "window_size_sec": "window_size_sec",
            "checkpoint_interval_sec": "checkpoint_interval_sec",
            "autoscaler_enabled": "autoscaler_enabled",
            "autoscaler_min_parallelism": "autoscaler_min_parallelism",
            "autoscaler_max_parallelism": "autoscaler_max_parallelism",
        }

        for schema_field, model_field in field_mapping.items():
            value = None

            # Priority 1: Request config
            if request_config is not None:
                req_value = getattr(request_config, schema_field, None)
                if req_value is not None:
                    value = req_value

            # Priority 2: Existing pipeline values
            if value is None and pipeline is not None:
                pipeline_value = getattr(pipeline, model_field, None)
                if pipeline_value is not None:
                    value = pipeline_value

            # Priority 3: Global defaults
            if value is None:
                value = getattr(defaults, schema_field)

            result[schema_field] = value

        return FlinkDefaultsResponse(**result)
