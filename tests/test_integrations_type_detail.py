"""Guards for the type-detail ``config_schema`` contract served to the frontend.

``GET /integrations/types/{type}`` prepends a required form-only ``profile_name`` property to
every provider's config schema (see ``apps/integrations/base.with_profile_name_field``); the
manager maps it to the ``integrations.name`` column and never stores it in the config JSONB.
The mapping itself is pinned in ``tests/test_integration_profile_name.py`` — this file keeps
the registry-wide reserved-key guard.
"""

from apps.integrations.base import (
    PROFILE_NAME_KEY,
    with_profile_name_field,
    with_required_config_fields,
)
from apps.integrations.providers.elastic_stack import ElasticStackProvider
from apps.integrations.providers.microsoft_sentinel import MicrosoftSentinelProvider
from apps.integrations.providers.opensearch import OpenSearchProvider
from apps.integrations.providers.splunk import SplunkProvider
from apps.integrations.registry import IntegrationRegistry


def test_no_provider_config_defines_reserved_profile_name_field() -> None:
    """``profile_name`` is reserved for the injected form slot (and stripped from config).

    A provider config field called ``profile_name`` would collide with the injected property
    and be silently dropped by ``IntegrationsManager._split_profile_name`` — pick another
    field name.
    """
    for integration_type, provider_cls in IntegrationRegistry.get_all().items():
        assert PROFILE_NAME_KEY not in provider_cls.get_config_schema().model_fields, integration_type


def test_required_config_fields_reference_real_schema_fields() -> None:
    """Every field a provider declares ``required_config_fields`` must exist on its config schema.

    Guards against a typo leaving the form asking for a field the provider doesn't have.
    """
    for integration_type, provider_cls in IntegrationRegistry.get_all().items():
        fields = provider_cls.get_config_schema().model_fields
        for name in provider_cls.required_config_fields():
            assert name in fields, f"{integration_type}: required field {name!r} not in config schema"


def test_sentinel_view_in_siem_fields_are_required_but_keep_defaults() -> None:
    """Sentinel's View-in-SIEM essentials render required while staying pre-filled.

    The required asterisk is layered onto the JSON Schema (the fields keep their Pydantic
    defaults for form pre-fill), mirroring the injected ``profile_name`` slot.
    """
    schema = with_required_config_fields(
        with_profile_name_field(MicrosoftSentinelProvider.get_config_schema().model_json_schema()),
        MicrosoftSentinelProvider.required_config_fields(),
    )
    required = schema["required"]
    for field in ("sentinel_url", "table_names", "host_name_fields"):
        assert field in required
    # host_name_fields keeps its default so the form still pre-fills it despite being required
    assert "default" in schema["properties"]["host_name_fields"]
    # form-only profile_name slot stays required and first
    assert required[0] == PROFILE_NAME_KEY
    # the optional remapped-id field is not forced required
    assert "remapped_sigma_rule_id_field" not in required


# Per provider: (form-required fields IN ORDER, fields that must stay optional). ``profile_name``
# is injected separately by ``with_profile_name_field`` and is always ``required[0]``.
_REQUIRED_BY_PROVIDER = {
    MicrosoftSentinelProvider: (
        ("sentinel_url", "table_names", "host_name_fields"),
        ("remapped_sigma_rule_id_field",),
    ),
    SplunkProvider: (
        ("splunk_url", "index_names", "host_name_fields"),
        ("remapped_sigma_rule_id_field", "custom_search_app"),
    ),
    OpenSearchProvider: (
        ("opensearch_url", "index_pattern_id", "host_name_fields"),
        ("remapped_sigma_rule_id_field", "tenant"),
    ),
    ElasticStackProvider: (
        ("kibana_url", "index_pattern_id", "host_name_fields"),
        ("remapped_sigma_rule_id_field", "kibana_space_id"),
    ),
}


def test_served_schema_required_list_per_provider() -> None:
    """End-to-end: served ``config_schema['required']`` is ``[profile_name, *required_config_fields]``.

    Runs the same two-step post-processing the manager applies in ``get_type_detail``
    (``with_required_config_fields(with_profile_name_field(...), required_config_fields())``) for
    every View-in-SIEM provider — not just Sentinel — asserting the order (profile_name first) and
    that each provider's optional fields never leak into ``required``.
    """
    for provider_cls, (required_fields, optional_fields) in _REQUIRED_BY_PROVIDER.items():
        schema = with_required_config_fields(
            with_profile_name_field(provider_cls.get_config_schema().model_json_schema()),
            provider_cls.required_config_fields(),
        )
        assert schema["required"] == [PROFILE_NAME_KEY, *required_fields], provider_cls.integration_type
        for optional in optional_fields:
            assert optional not in schema["required"], f"{provider_cls.integration_type}: {optional}"


def test_required_config_fields_stay_optional_at_model_layer() -> None:
    """Form-required fields are a JSON-Schema hint only — the Pydantic model stays lenient.

    The whole "required is a form gate, not a server constraint" design hinges on this: a config
    omitting every required field must still ``model_validate`` (the models keep defaults so the
    form pre-fills them and capability-subset profiles can save). Fails loudly if anyone tightens a
    model field to be truly required (e.g. drops its default).
    """
    for provider_cls in _REQUIRED_BY_PROVIDER:
        config_cls = provider_cls.get_config_schema()
        dumped = config_cls.model_validate({}).model_dump()  # no required field supplied -> must not raise
        for field in provider_cls.required_config_fields():
            assert field in dumped, f"{provider_cls.integration_type}: {field} lost its default"


# Console-URL config field per provider — kept a lenient ``str`` (see test below).
_URL_FIELD_BY_TYPE = {
    "microsoft_sentinel": "sentinel_url",
    "splunk": "splunk_url",
    "opensearch": "opensearch_url",
    "elastic_stack": "kibana_url",
}


def test_url_fields_stay_lenient_strings() -> None:
    """URL config fields keep type ``string`` and carry no input-validation hint.

    The backend ships no ``format``/URL validator: hard URL validation lives at call time
    (``_resolve_console_base``) so a partial/on-prem/imperfect URL can still save and degrade
    gracefully to a query preview (the ``*_invalid_url_keeps_query_preview`` tests). How to validate
    URL shape in the form is the frontend's concern, so the schema advertises no ``format``.
    """
    for integration_type, field in _URL_FIELD_BY_TYPE.items():
        config_cls = IntegrationRegistry.get_all()[integration_type].get_config_schema()
        props = config_cls.model_json_schema()["properties"][field]
        assert props["type"] == "string", integration_type
        assert "format" not in props and "pattern" not in props, integration_type
        # scheme-less host:port must not raise at the model layer (graceful-degradation contract)
        config_cls.model_validate({field: "console.example.com:5601"})


def test_remapped_sigma_rule_id_field_carries_no_validation_hint() -> None:
    """The field-name slot exposes no ``pattern`` hint — the server gate is the sole validator.

    ``validate_field_name`` (the anti-injection gate, covered in ``tests/test_view_in_siem.py``)
    enforces the shape on every write; the schema deliberately does not mirror that regex, so there
    is no client/server regex drift to keep in sync. The form decides how to surface the rule.
    """
    for integration_type in ("microsoft_sentinel", "splunk", "opensearch", "elastic_stack"):
        schema = IntegrationRegistry.get_all()[integration_type].get_config_schema().model_json_schema()
        assert "pattern" not in schema["properties"]["remapped_sigma_rule_id_field"], integration_type


def test_with_required_config_fields_dedups_and_does_not_mutate() -> None:
    """``with_required_config_fields`` appends only new names, preserves order, and copies the input."""
    original = {"required": [PROFILE_NAME_KEY, "host_name_fields"]}
    result = with_required_config_fields(original, ("host_name_fields", "splunk_url"))
    # existing order preserved, only the genuinely new field appended, no duplicate
    assert result["required"] == [PROFILE_NAME_KEY, "host_name_fields", "splunk_url"]
    assert result["required"].count("host_name_fields") == 1
    # input not mutated (shallow copy)
    assert original["required"] == [PROFILE_NAME_KEY, "host_name_fields"]
    assert result is not original
