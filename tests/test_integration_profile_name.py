"""Tests for the integration-level ``profile_name`` form field.

``profile_name`` is surfaced in every provider's config form (injected into ``config_schema`` by
``with_profile_name_field``) but is **not** provider config — the manager maps it to the
``integrations.name`` column on write and re-injects it from there on read, so it is never stored
in the config JSONB. These tests pin that mapping across get_type_detail / create / update / GET.
"""

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import apps.integrations.providers  # noqa: F401  -- register providers for registry lookups
from apps.core.exceptions import BadRequestError
from apps.core.schemas import IntegrationCreateRequest, IntegrationUpdateRequest
from apps.integrations.base import PROFILE_NAME_KEY, with_profile_name_field
from apps.managers.integrations import IntegrationsManager

USER = SimpleNamespace(id=uuid.uuid4(), email="tester@example.com")


# ---------------------------------------------------------------------------
# with_profile_name_field — schema injection
# ---------------------------------------------------------------------------


def test_with_profile_name_field_prepends_required_first_field():
    schema = with_profile_name_field({"properties": {"splunk_url": {"type": "string", "ui_order": 1}}, "required": []})
    prop = schema["properties"][PROFILE_NAME_KEY]
    assert prop["type"] == "string"
    assert prop["title"] == "Profile Name"
    assert prop["ui_order"] == 0  # sorts before every provider field (ui_order >= 1)
    # Design: Profile Name renders without a tooltip — no description, placeholder only.
    assert "description" not in prop and prop["placeholder"]
    assert schema["required"][0] == PROFILE_NAME_KEY
    # Existing properties are preserved.
    assert "splunk_url" in schema["properties"]


def test_with_profile_name_field_does_not_mutate_input_or_duplicate_required():
    original = {"properties": {"x": {"type": "string"}}, "required": [PROFILE_NAME_KEY, "x"]}
    schema = with_profile_name_field(original)
    assert original["properties"] == {"x": {"type": "string"}}  # input untouched
    assert schema["required"].count(PROFILE_NAME_KEY) == 1  # no duplicate


@pytest.mark.parametrize("integration_type", ["microsoft_sentinel", "splunk", "elastic_stack", "opensearch", "github"])
async def test_get_type_detail_exposes_profile_name_for_every_provider(integration_type):
    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    mgr.dao.count_by_type.return_value = {}
    detail = await mgr.get_type_detail(integration_type)
    props = detail["config_schema"]["properties"]
    assert PROFILE_NAME_KEY in props
    assert props[PROFILE_NAME_KEY]["ui_order"] == 0
    assert PROFILE_NAME_KEY in detail["config_schema"]["required"]


# ---------------------------------------------------------------------------
# _split_profile_name helper
# ---------------------------------------------------------------------------


def test_split_profile_name_extracts_and_strips():
    config, name = IntegrationsManager._split_profile_name({"profile_name": "  Prod  ", "splunk_url": "https://x"})
    assert name == "Prod"
    assert config == {"splunk_url": "https://x"}  # profile_name removed, not stored in config


def test_split_profile_name_absent_or_non_string():
    assert IntegrationsManager._split_profile_name({"splunk_url": "x"}) == ({"splunk_url": "x"}, "")
    assert IntegrationsManager._split_profile_name({"profile_name": 123}) == ({}, "")
    assert IntegrationsManager._split_profile_name(None) == ({}, "")


# ---------------------------------------------------------------------------
# create — name derived from config.profile_name (mapped to the name column)
# ---------------------------------------------------------------------------


def _manager() -> IntegrationsManager:
    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    mgr.dao.count_for_integration_type.return_value = 0
    created = SimpleNamespace(id=uuid.uuid4(), name="", integration_type="splunk", config={})
    mgr.dao.create.return_value = created
    mgr.check_connection = AsyncMock()
    mgr.get_by_id = AsyncMock(return_value=created)
    return mgr


async def test_create_derives_name_from_config_profile_name(activity_log):
    mgr = _manager()
    req = IntegrationCreateRequest(
        integration_type="splunk",
        config={"profile_name": "Prod Splunk", "splunk_url": "https://splunk.example.com:8000"},
    )
    await mgr.create(req, USER)
    kwargs = mgr.dao.create.await_args.kwargs
    assert kwargs["name"] == "Prod Splunk"
    # profile_name is NOT persisted into the config JSONB.
    assert PROFILE_NAME_KEY not in kwargs["config"]
    assert kwargs["config"]["splunk_url"] == "https://splunk.example.com:8000"


async def test_create_top_level_name_wins_over_config(activity_log):
    mgr = _manager()
    req = IntegrationCreateRequest(
        name="Explicit",
        integration_type="splunk",
        config={"profile_name": "FromConfig", "splunk_url": "https://x:8000"},
    )
    await mgr.create(req, USER)
    assert mgr.dao.create.await_args.kwargs["name"] == "Explicit"


async def test_create_without_any_name_raises(activity_log):
    mgr = _manager()
    req = IntegrationCreateRequest(integration_type="splunk", config={"splunk_url": "https://x:8000"})
    with pytest.raises(BadRequestError):
        await mgr.create(req, USER)


# ---------------------------------------------------------------------------
# update — config.profile_name updates the name column, not the config JSONB
# ---------------------------------------------------------------------------


async def test_update_config_profile_name_sets_name(activity_log):
    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    existing = SimpleNamespace(
        id=uuid.uuid4(), name="Old", integration_type="splunk", config={"splunk_url": "https://x:8000"}
    )
    mgr.get_by_id = AsyncMock(return_value=existing)
    mgr.check_connection = AsyncMock()  # else it issues its own dao.update (status) we'd capture instead
    updated = SimpleNamespace(id=existing.id, name="Renamed", integration_type="splunk", config={})
    mgr.dao.update.return_value = updated
    req = IntegrationUpdateRequest(config={"profile_name": "Renamed", "splunk_url": "https://x:8000"})
    await mgr.update(existing.id, req, USER)
    kwargs = mgr.dao.update.await_args.kwargs
    assert kwargs["name"] == "Renamed"
    assert PROFILE_NAME_KEY not in kwargs["config"]


# ---------------------------------------------------------------------------
# GET detail — profile_name re-injected from the name column
# ---------------------------------------------------------------------------


async def test_get_detail_reinjects_profile_name(api_client, monkeypatch):
    integration_id = uuid.uuid4()
    integration = SimpleNamespace(
        id=integration_id,
        name="My Splunk",
        integration_type="splunk",
        category="siem",
        status="connected",
        status_message="ok",
        config={"splunk_url": "https://splunk.example.com:8000", "token": "secret"},
        last_checked_at=None,
        created=datetime(2026, 6, 1, tzinfo=UTC),
        updated=datetime(2026, 6, 1, tzinfo=UTC),
    )
    monkeypatch.setattr("apps.managers.integrations.IntegrationsManager.get_by_id", AsyncMock(return_value=integration))
    response = await api_client.get(f"/api/v1/integrations/{integration_id}")
    assert response.status_code == 200
    config = response.json()["config"]
    assert config[PROFILE_NAME_KEY] == "My Splunk"
    assert config["token"] == "******"  # masking still applies
