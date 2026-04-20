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
        default=2,
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
        min=1024,
        max=131072,
        unit="MB",
        title="TaskManager Memory",
        description="Memory per worker. More rules and events require more memory.",
        impact="restart",
        category="resources",
        tips=[
            "2 GB (default) handles most workloads",
            "Increase if pipeline keeps crashing or restarting",
            "More rules = more memory needed",
        ],
    ),
    FlinkParameterInfo(
        name="taskmanager_cpu",
        type="number",
        default=1.0,
        min=0.5,
        max=16.0,
        unit="cores",
        title="TaskManager CPU",
        description="CPU cores per worker. Each worker handles one parallelism slot.",
        impact="restart",
        category="resources",
        tips=[
            "1 core (default) is sufficient for most workloads",
            "Increase only if CPU is the bottleneck (check metrics first)",
            "parallelism=N creates N workers, each with this many cores",
        ],
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
