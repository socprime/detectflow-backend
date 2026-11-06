"""Tests for attack-chains list SSE invalidation."""

import asyncio
import json

import pytest

from apps.managers.attack_chains import AttackChainsManager
from apps.routers import attack_chains as attack_chains_router
from apps.services.attack_chains_list_invalidation import AttackChainsListInvalidationService


class TestAttackChainsListInvalidationService:
    def setup_method(self) -> None:
        self.service = AttackChainsListInvalidationService()

    def test_notify_changed_increments_revision(self) -> None:
        assert self.service.revision == 0
        assert self.service.notify_changed() == 1
        assert self.service.revision == 1
        assert self.service.notify_changed() == 2

    @pytest.mark.asyncio
    async def test_subscriber_receives_revision_on_notify(self) -> None:
        queue = await self.service.subscribe()
        revision = self.service.notify_changed()
        assert await asyncio.wait_for(queue.get(), timeout=1.0) == revision

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(self) -> None:
        queue = await self.service.subscribe()
        await self.service.unsubscribe(queue)
        self.service.notify_changed()
        assert queue.empty()


def _make_request(monkeypatch, disconnect_fn):
    from fastapi import Request
    from starlette.requests import Request as StarletteRequest

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/v1/attack_chains/chains/stream",
        "headers": [],
        "query_string": b"",
    }
    request = Request(StarletteRequest(scope, receive=lambda: asyncio.sleep(0)))  # type: ignore[arg-type]
    monkeypatch.setattr(request, "is_disconnected", disconnect_fn)
    return request


@pytest.mark.asyncio
async def test_list_attack_chains_includes_list_revision(api_client, monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    service._revision = 9  # noqa: SLF001
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)

    async def fake_list_chains(self, pagination, group_by, **kwargs):
        return {"default": []}, 0, None

    monkeypatch.setattr(AttackChainsManager, "list_chains", fake_list_chains)

    response = await api_client.get("/api/v1/attack_chains/chains")
    assert response.status_code == 200
    body = response.json()
    assert body["list_revision"] == 9
    assert body["groups"] == {"default": []}


@pytest.mark.asyncio
async def test_list_revision_captured_before_query(api_client, monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    service._revision = 4  # noqa: SLF001
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)

    async def fake_list_chains(self, pagination, group_by, **kwargs):
        service.notify_changed()
        return {"default": []}, 0, None

    monkeypatch.setattr(AttackChainsManager, "list_chains", fake_list_chains)

    response = await api_client.get("/api/v1/attack_chains/chains")
    assert response.status_code == 200
    assert response.json()["list_revision"] == 4


@pytest.mark.asyncio
async def test_attack_chains_list_sse_generator_emits_connection_established(monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    service._revision = 7  # noqa: SLF001
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)

    async def disconnected() -> bool:
        return True

    request = _make_request(monkeypatch, disconnected)

    gen = attack_chains_router._attack_chains_list_sse_generator(request)
    first = await anext(gen)
    payload = json.loads(first.removeprefix("data: ").strip())
    assert payload == {
        "type": "connection_established",
        "revision": 7,
        "timestamp": payload["timestamp"],
    }

    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_attack_chains_list_sse_generator_emits_changed_event(monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)
    monkeypatch.setattr(
        "apps.routers.attack_chains.settings.attack_chains_list_sse_heartbeat_seconds",
        30.0,
    )

    should_disconnect = False

    async def maybe_disconnect() -> bool:
        return should_disconnect

    request = _make_request(monkeypatch, maybe_disconnect)

    gen = attack_chains_router._attack_chains_list_sse_generator(request)
    await anext(gen)  # connection_established

    changed_task = asyncio.create_task(anext(gen))
    service.notify_changed()
    changed = await changed_task
    payload = json.loads(changed.removeprefix("data: ").strip())
    assert payload["type"] == "attack_chains_changed"
    assert payload["revision"] == 1

    should_disconnect = True
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_attack_chains_list_sse_generator_notify_during_subscribe_not_missed(monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)
    monkeypatch.setattr(
        "apps.routers.attack_chains.settings.attack_chains_list_sse_heartbeat_seconds",
        30.0,
    )

    async def subscribe_and_notify() -> asyncio.Queue[int]:
        queue = await AttackChainsListInvalidationService.subscribe(service)
        service.notify_changed()
        return queue

    monkeypatch.setattr(service, "subscribe", subscribe_and_notify)

    should_disconnect = False

    async def maybe_disconnect() -> bool:
        return should_disconnect

    request = _make_request(monkeypatch, maybe_disconnect)

    gen = attack_chains_router._attack_chains_list_sse_generator(request)
    established = json.loads((await anext(gen)).removeprefix("data: ").strip())
    assert established["type"] == "connection_established"
    assert established["revision"] == 1

    changed = json.loads((await anext(gen)).removeprefix("data: ").strip())
    assert changed == {
        "type": "attack_chains_changed",
        "revision": 1,
        "timestamp": changed["timestamp"],
    }

    should_disconnect = True
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


@pytest.mark.asyncio
async def test_attack_chains_list_sse_generator_emits_heartbeat(monkeypatch) -> None:
    service = AttackChainsListInvalidationService()
    service._revision = 3  # noqa: SLF001
    monkeypatch.setattr(attack_chains_router, "attack_chains_list_invalidation_service", service)
    monkeypatch.setattr(
        "apps.routers.attack_chains.settings.attack_chains_list_sse_heartbeat_seconds",
        0.01,
    )

    should_disconnect = False

    async def maybe_disconnect() -> bool:
        return should_disconnect

    request = _make_request(monkeypatch, maybe_disconnect)

    gen = attack_chains_router._attack_chains_list_sse_generator(request)
    await anext(gen)  # connection_established

    heartbeat = json.loads((await anext(gen)).removeprefix("data: ").strip())
    assert heartbeat == {
        "type": "heartbeat",
        "revision": 3,
        "timestamp": heartbeat["timestamp"],
    }

    should_disconnect = True
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
