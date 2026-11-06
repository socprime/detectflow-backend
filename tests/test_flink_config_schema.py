"""``GET /settings/flink-defaults/schema`` is the single source of ranges, defaults and tooltips.

"defaults and tooltips come from the backend" is a contract, not a nicety: if a
parameter reaches the UI without a default, bounds or tips, the form invents them — and an invented
correlation default silently regresses a deliberately tuned production value.
"""

from apps.core.schemas import validate_auto_add_pipeline_name_pattern
from apps.core.settings import settings
from apps.managers.correlation_pipelines import (
    AGENT_RESTART_FIELDS,
    CHAINS_CONFIG_HOT_FIELDS,
    CORRELATION_RESTART_FIELDS,
    memory_mb,
)
from apps.managers.flink_config import (
    FLINK_CATEGORIES,
    FLINK_PARAMETERS,
    IMPACT_DESCRIPTIONS,
    FlinkConfigManager,
)

BY_NAME = {parameter.name: parameter for parameter in FLINK_PARAMETERS}


def test_every_category_and_impact_is_declared() -> None:
    for parameter in FLINK_PARAMETERS:
        assert parameter.category in FLINK_CATEGORIES, parameter.name
        assert parameter.impact in IMPACT_DESCRIPTIONS, parameter.name


def test_every_parameter_is_documented_for_the_form() -> None:
    """default (possibly null), title, description, impact and tips — plus min/max when numeric."""
    for parameter in FLINK_PARAMETERS:
        assert parameter.title, parameter.name
        assert parameter.description, parameter.name
        assert parameter.tips, parameter.name
        if parameter.default is None:
            assert parameter.nullable, f"{parameter.name}: a null default must be declared nullable"
        if parameter.type in {"integer", "number"}:
            assert parameter.min is not None, parameter.name
            assert parameter.max is not None, parameter.name


def test_the_three_correlation_categories_exist_and_sort_after_the_pipeline_ones() -> None:
    for key in ("correlation", "correlation_job", "agent_job"):
        assert key in FLINK_CATEGORIES
    orders = [FLINK_CATEGORIES[key].order for key in ("correlation", "correlation_job", "agent_job")]
    assert orders == sorted(orders)
    assert min(orders) > FLINK_CATEGORIES["autoscaler"].order


def test_correlation_defaults_match_the_production_settings_not_the_mock() -> None:
    """The mock says 3g/1.0 for the correlation job; production is 4g/3.0 and stays that way.

    Those numbers are Python-worker off-heap headroom, not a UI preference — a mock-driven
    regression must fail the build here rather than starve a customer's correlation job.
    """
    assert BY_NAME["correlation_taskmanager_memory_mb"].default == memory_mb(
        settings.correlation_job_taskmanager_memory, default_mb=4096
    )
    assert BY_NAME["correlation_taskmanager_memory_mb"].default == 4096
    assert BY_NAME["correlation_taskmanager_cpu"].default == settings.correlation_job_taskmanager_cpu == 3.0
    assert BY_NAME["correlation_jobmanager_memory_mb"].default == 2048
    assert BY_NAME["correlation_jobmanager_cpu"].default == settings.flink_jobmanager_cpu == 1.0
    assert BY_NAME["agent_taskmanager_memory_mb"].default == 3072
    assert BY_NAME["agent_taskmanager_cpu"].default == settings.agent_job_taskmanager_cpu == 1.0
    assert BY_NAME["agent_jobmanager_memory_mb"].default == 2048
    assert BY_NAME["agent_jobmanager_cpu"].default == 1.0


def test_jobmanager_parameters_exist_for_detection_pipelines_too() -> None:
    """The mock shows them in both runtime panels; they were missing from the schema entirely."""
    assert BY_NAME["jobmanager_memory_mb"].category == "resources"
    assert BY_NAME["jobmanager_cpu"].category == "resources"


def test_correlation_window_is_nullable_with_a_null_default() -> None:
    """The UI needs an explicit "Inherit" state — null is a value here, not an omission."""
    window = BY_NAME["correlation_window_seconds"]
    assert window.nullable is True
    assert window.default is None
    assert (window.min, window.max) == (30, 10800)


def test_pipeline_name_pattern_is_a_string_gated_on_the_auto_add_toggle() -> None:
    pattern = BY_NAME["auto_add_pipeline_name_pattern"]
    assert pattern.type == "string"
    assert pattern.nullable is True
    assert pattern.requires == "auto_add_new_pipelines"
    assert "auto_add_new_pipelines" in BY_NAME
    # What the tips advertise as valid must actually pass the schema task's validator.
    assert validate_auto_add_pipeline_name_pattern("cust1-*") == "cust1-*"
    assert validate_auto_add_pipeline_name_pattern("Customer *") == "Customer *"
    assert validate_auto_add_pipeline_name_pattern("") is None


def test_impact_agrees_with_the_managers_edit_safety_matrix() -> None:
    """``impact`` is what greys a field out on a running job — it cannot disagree with the matrix."""
    restart_fields = CORRELATION_RESTART_FIELDS | AGENT_RESTART_FIELDS
    for parameter in FLINK_PARAMETERS:
        if parameter.category == "correlation":
            assert parameter.impact == "hot_reload", parameter.name
            assert parameter.name in CHAINS_CONFIG_HOT_FIELDS or parameter.name == "auto_add_pipeline_name_pattern"
        if parameter.category in {"correlation_job", "agent_job"}:
            assert parameter.impact == "restart", parameter.name
            assert parameter.name in restart_fields, parameter.name


def test_pre_existing_parameters_still_validate_after_the_model_widening() -> None:
    """The widening (string type, nullable default, three categories) is additive."""
    legacy = [p for p in FLINK_PARAMETERS if p.category in {"resources", "processing", "autoscaler"}]
    assert len(legacy) >= 8
    for parameter in legacy:
        assert parameter.default is not None
        assert parameter.nullable is False
        assert parameter.type in {"integer", "number", "boolean"}


def test_schema_endpoint_payload_carries_everything_the_form_needs() -> None:
    schema = FlinkConfigManager.get_schema()
    names = {parameter.name for parameter in schema.parameters}
    assert {
        "threshold_pct",
        "correlation_window_seconds",
        "auto_add_new_pipelines",
        "auto_add_pipeline_name_pattern",
    } <= names
    assert {"correlation_parallelism", "correlation_taskmanager_cpu", "agent_parallelism"} <= names
    assert {"correlation", "correlation_job", "agent_job"} <= set(schema.categories)
    assert set(schema.impact_descriptions) == {"restart", "hot_reload"}


def test_parameter_names_are_unique() -> None:
    names = [parameter.name for parameter in FLINK_PARAMETERS]
    assert len(names) == len(set(names)), "duplicate parameter name in FLINK_PARAMETERS"
