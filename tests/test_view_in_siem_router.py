"""Router-level tests for ``POST /api/v1/integrations/{id}/view-in-siem`` (ASGI, no infra).

First HTTP-contract coverage in the repo: pins the app-owned 422 body validation
(``IntegrationViewInSiemRequest``: required ``chain_id`` UUID, optional ``sigma_rule_id`` UUID),
auth enforcement, and the router→manager→``ViewInSiemResult`` serialization
(``IntegrationViewInSiemResponse``). The 400/404 mapping itself is framework-owned (domain
exceptions subclass ``HTTPException``) and pinned lightly. The manager method is patched at the
class; auth/DB dependencies come overridden from the shared ``api_client`` fixture
(``tests/conftest.py``) — the seed pattern for future router tests.
"""

import uuid
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock

import httpx
import pytest

from apps.core.exceptions import BadRequestError, NotFoundError
from apps.integrations.base import ViewInSiemResult
from server import app

INTEGRATION_ID = uuid.uuid4()
ENDPOINT = f"/api/v1/integrations/{INTEGRATION_ID}/view-in-siem"
MANAGER_METHOD = "apps.managers.integrations.IntegrationsManager.build_view_in_siem_url"


def _body(**overrides) -> dict:
    body: dict = {"chain_id": str(uuid.uuid4())}
    body.update(overrides)
    return body


def _patch_manager(monkeypatch, **mock_kwargs) -> AsyncMock:
    mock = AsyncMock(**mock_kwargs)
    monkeypatch.setattr(MANAGER_METHOD, mock)
    return mock


# ---------------------------------------------------------------------------
# 200 — result serialization (router validates result.model_dump())
# ---------------------------------------------------------------------------


async def test_200_happy_path_serializes_view_in_siem_result(api_client, monkeypatch):
    result = ViewInSiemResult(status=True, url="https://siem.example.com/deep-link", query="q1")
    mock = _patch_manager(monkeypatch, return_value=result)
    response = await api_client.post(ENDPOINT, json=_body())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] is True
    assert data["url"] == "https://siem.example.com/deep-link"
    assert data["query"] == "q1"
    mock.assert_awaited_once()


async def test_200_provider_failure_is_status_false_not_an_error(api_client, monkeypatch):
    # Result→HTTP contract: provider failures are 200 with status:false (not 4xx/5xx).
    _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=False, message="Invalid config: x"))
    response = await api_client.post(ENDPOINT, json=_body())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] is False
    assert "Invalid config" in data["message"]
    assert data["url"] is None


async def test_request_body_is_forwarded_to_manager(api_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True, url="http://x", query="q"))
    chain_id = uuid.uuid4()
    sigma_rule_id = uuid.uuid4()
    response = await api_client.post(ENDPOINT, json=_body(chain_id=str(chain_id), sigma_rule_id=str(sigma_rule_id)))
    assert response.status_code == 200
    kwargs = mock.await_args.kwargs
    assert kwargs["chain_id"] == chain_id
    assert kwargs["sigma_rule_id"] == sigma_rule_id


# ---------------------------------------------------------------------------
# 400 / 404 — domain exceptions raised in the manager
# ---------------------------------------------------------------------------


async def test_400_when_type_lacks_view_in_siem(api_client, monkeypatch):
    _patch_manager(monkeypatch, side_effect=BadRequestError("Integration type 'github' does not support view in SIEM"))
    response = await api_client.post(ENDPOINT, json=_body())
    assert response.status_code == 400
    assert "does not support view in SIEM" in response.json()["detail"]


async def test_404_when_chain_is_missing(api_client, monkeypatch):
    _patch_manager(monkeypatch, side_effect=NotFoundError("Attack chain with id x not found"))
    response = await api_client.post(ENDPOINT, json=_body())
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]


# ---------------------------------------------------------------------------
# 422 — app-owned body schema (IntegrationViewInSiemRequest)
# ---------------------------------------------------------------------------


async def test_422_when_chain_id_is_not_a_uuid(api_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True))
    response = await api_client.post(ENDPOINT, json=_body(chain_id="not-a-uuid"))
    assert response.status_code == 422
    mock.assert_not_awaited()


async def test_422_when_chain_id_is_missing(api_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True))
    response = await api_client.post(ENDPOINT, json={})
    assert response.status_code == 422
    mock.assert_not_awaited()


async def test_422_when_sigma_rule_id_is_not_a_uuid(api_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True))
    response = await api_client.post(ENDPOINT, json=_body(sigma_rule_id="not-a-uuid"))
    assert response.status_code == 422
    mock.assert_not_awaited()


async def test_422_when_integration_id_path_param_is_not_a_uuid(api_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True))
    response = await api_client.post("/api/v1/integrations/not-a-uuid/view-in-siem", json=_body())
    assert response.status_code == 422
    mock.assert_not_awaited()


# ---------------------------------------------------------------------------
# Auth — the dependency rejects requests without a Bearer token
# ---------------------------------------------------------------------------


@pytest.fixture
async def anonymous_client() -> AsyncIterator[httpx.AsyncClient]:
    """ASGI client with NO dependency overrides — auth runs for real (HTTPBearer)."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


async def test_unauthenticated_request_is_rejected(anonymous_client, monkeypatch):
    mock = _patch_manager(monkeypatch, return_value=ViewInSiemResult(status=True))
    response = await anonymous_client.post(ENDPOINT, json=_body())
    # HTTPBearer rejects a missing Authorization header before the endpoint body runs.
    assert response.status_code in (401, 403)
    mock.assert_not_awaited()
