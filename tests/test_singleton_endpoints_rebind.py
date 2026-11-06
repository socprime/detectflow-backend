"""HTTP contract of the four remaining deprecated singleton operations, now bound to ``Default``.

Router-level only: the managers are patched, so what is pinned here is the wire. The point of the
rebind is that the wire did **not** move — the frontend has not cut over yet — while everything
behind it did:

* the response shapes and status codes are unchanged (runtime start/stop stay ``200``; the
  per-pipeline pair's ``202`` is deliberately not propagated back here),
* every operation resolves ``Default`` through ``CorrelationPipelinesManager`` and 404s without it.

The projection rules themselves (window default) are pinned in ``test_correlation_job_manager.py``.
Membership is ``PATCH /correlation_pipelines/{id}``; ``monitored_topics`` on this route is a 422.
The platform-wide graph is ``GET /api/v1/correlation_dashboard/snapshot`` (REST) and
``GET /api/v1/correlation_dashboard/stream`` (SSE).
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from apps.core.exceptions import NotFoundError
from apps.core.schemas import AttackChainConfigurationResponse, AttackChainsRuntimeResponse

CONFIGURATION = "/api/v1/correlation_job/configuration"
START = "/api/v1/attack_chains/runtime/start"
STOP = "/api/v1/attack_chains/runtime/stop"

_NO_DEFAULT = NotFoundError("No default correlation pipeline; use /api/v1/correlation_pipelines")


def _configuration() -> AttackChainConfigurationResponse:
    return AttackChainConfigurationResponse(
        id=1,
        threshold_pct=60,
        correlation_window_seconds=300,
        auto_add_new_pipelines=False,
        updated_at=datetime(2026, 8, 1, tzinfo=UTC),
        updated_by=None,
    )


def _runtime_result(status: str) -> AttackChainsRuntimeResponse:
    return AttackChainsRuntimeResponse(status=status, correlation_job={"operation": "created"}, agent_job={})


@pytest.fixture
def attack_chains_manager():
    """Patch ``AttackChainsManager`` in both deprecated routers with one instance."""
    manager = AsyncMock()
    with (
        patch("apps.routers.correlation_job.AttackChainsManager", return_value=manager),
        patch("apps.routers.attack_chains_runtime.AttackChainsManager", return_value=manager),
    ):
        yield manager


class TestConfiguration:
    async def test_get_returns_the_legacy_shape(self, api_client, attack_chains_manager) -> None:
        attack_chains_manager.get_configuration.return_value = _configuration()

        response = await api_client.get(CONFIGURATION)

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == 1
        assert body["correlation_window_seconds"] == 300
        assert "monitored_topics" not in body

    async def test_patch_returns_the_legacy_shape(self, api_client, attack_chains_manager) -> None:
        attack_chains_manager.update.return_value = _configuration()

        response = await api_client.patch(CONFIGURATION, json={"threshold_pct": 75})

        assert response.status_code == 200
        assert response.json()["threshold_pct"] == 60
        attack_chains_manager.update.assert_awaited_once()

    async def test_patch_with_monitored_topics_is_422(self, api_client, attack_chains_manager) -> None:
        response = await api_client.patch(
            CONFIGURATION, json={"monitored_topics": [{"name": "output-logs", "enabled": True}]}
        )

        assert response.status_code == 422
        attack_chains_manager.update.assert_not_awaited()

    @pytest.mark.parametrize("method", ["get", "patch"])
    async def test_no_default_pipeline_is_a_404(self, api_client, attack_chains_manager, method: str) -> None:
        attack_chains_manager.get_configuration.side_effect = _NO_DEFAULT
        attack_chains_manager.update.side_effect = _NO_DEFAULT

        response = await getattr(api_client, method)(
            CONFIGURATION, **({"json": {"threshold_pct": 75}} if method == "patch" else {})
        )

        assert response.status_code == 404
        assert "/api/v1/correlation_pipelines" in response.text


class TestRuntimeStartStop:
    @pytest.mark.parametrize(("url", "shim"), [(START, "start_runtime"), (STOP, "stop_runtime")])
    async def test_still_200_with_the_unchanged_body(self, api_client, attack_chains_manager, url, shim) -> None:
        """The per-pipeline pair answers 202; this surface must not start reporting one."""
        getattr(attack_chains_manager, shim).return_value = _runtime_result("starting")

        response = await api_client.post(url)

        assert response.status_code == 200
        assert response.json() == {
            "status": "starting",
            "correlation_job": {"operation": "created"},
            "agent_job": {},
        }

    @pytest.mark.parametrize(("url", "shim"), [(START, "start_runtime"), (STOP, "stop_runtime")])
    async def test_no_default_pipeline_is_a_404(self, api_client, attack_chains_manager, url, shim) -> None:
        getattr(attack_chains_manager, shim).side_effect = _NO_DEFAULT

        response = await api_client.post(url)

        assert response.status_code == 404
        assert "/api/v1/correlation_pipelines" in response.text


class TestQuarantine:
    def test_no_module_reads_the_deprecated_singleton_row(self) -> None:
        """``attack_chain_configuration`` survives one release as a rollback record only.

        Its last two readers were ``GET`` / ``PATCH /correlation_job/configuration``; after the
        rebind the accessors exist but nothing calls them, so the table cannot drift away from the
        state the migration left it in.
        """
        import inspect
        import pathlib

        from apps.managers import attack_chains

        assert "ensure_configuration_singleton" not in inspect.getsource(attack_chains)

        # The two accessors survive alongside the table they read; what must not exist is a caller.
        callers = [
            f"{path}:{number}"
            for path in pathlib.Path("apps").rglob("*.py")
            if path.name != "attack_chains.py" or path.parent.name != "postgre"
            for number, line in enumerate(path.read_text().splitlines(), start=1)
            if "configuration_singleton(" in line
        ]
        assert callers == [], callers
