"""HTTP contract of ``/api/v1/correlation_pipelines`` (frontend contract).

Router-level only: the manager is patched, so what is pinned here is the wire — status codes, the
thin-router boundary, how query parameters reach the manager, and which domain exception becomes
which HTTP code. The manager's own behaviour lives in ``test_correlation_pipelines_manager.py`` and
``test_correlation_pipeline_duplicate.py``.
"""

import inspect
import pathlib
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from apps.core.auth import get_current_active_user, require_write_access
from apps.core.enums import UserRole
from apps.core.exceptions import ConflictError, NotFoundError
from apps.core.schemas import (
    AttackChainsRuntimeResponse,
    CorrelationJobGraphData,
    CorrelationPipelineJobResources,
    CorrelationPipelineListResponse,
    CorrelationPipelineResponse,
)
from apps.managers.correlation_dashboard import get_correlation_dashboard_snapshot
from apps.routers import correlation_pipelines as correlation_pipelines_router
from server import app

BASE = "/api/v1/correlation_pipelines"
DASHBOARD = "/api/v1/correlation_dashboard"
PIPELINE_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
COPY_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")


def _response(pipeline_id: uuid.UUID = PIPELINE_ID, name: str = "tenant-a", **overrides) -> CorrelationPipelineResponse:
    payload = {
        "id": pipeline_id,
        "name": name,
        "jobs_status": "stopped",
        "runtime_state": None,
        "members": [],
        "attack_chains_count": 0,
        "created_at": datetime(2026, 8, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 8, 1, tzinfo=UTC),
        "threshold_pct": 60,
        "correlation_window_seconds": None,
        "auto_add_new_pipelines": False,
        "auto_add_pipeline_name_pattern": None,
        "correlation_resources": CorrelationPipelineJobResources(),
        "agent_resources": CorrelationPipelineJobResources(),
        "correlation_deployment_name": f"corr-{pipeline_id}",
        "agent_deployment_name": f"agent-{pipeline_id}",
        "updated_by": None,
    }
    payload.update(overrides)
    return CorrelationPipelineResponse(**payload)


def _runtime(status: str = "starting") -> AttackChainsRuntimeResponse:
    return AttackChainsRuntimeResponse(
        status=status,
        correlation_job={"deployment_name": f"corr-{PIPELINE_ID}", "operation": "created"},
        agent_job={"deployment_name": f"agent-{PIPELINE_ID}", "operation": "created"},
    )


@pytest.fixture
def manager() -> AsyncMock:
    """Patch the manager the router constructs; returns the instance the routes will call."""
    instance = AsyncMock()
    instance.list_pipelines.return_value = CorrelationPipelineListResponse(
        total=1, limit=10, offset=0, sort="name", order="asc", data=[]
    )
    instance.get.return_value = _response()
    instance.create.return_value = _response()
    instance.update.return_value = _response()
    instance.delete.return_value = []
    instance.duplicate.return_value = _response(COPY_ID, "tenant-a (Copy)")
    instance.start.return_value = _runtime("starting")
    instance.stop.return_value = _runtime("stopping")
    with patch("apps.routers.correlation_pipelines.CorrelationPipelinesManager", return_value=instance):
        yield instance


class TestStatusCodes:
    async def test_list_is_200(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.get(BASE)
        assert response.status_code == 200
        assert response.json()["data"] == []

    async def test_get_is_200(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.get(f"{BASE}/{PIPELINE_ID}")
        assert response.status_code == 200
        assert response.json()["id"] == str(PIPELINE_ID)
        assert response.json()["destination_topics"] == []

    async def test_create_is_201(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.post(BASE, json={"name": "tenant-a"})
        assert response.status_code == 201

    async def test_patch_is_200(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.patch(f"{BASE}/{PIPELINE_ID}", json={"threshold_pct": 75})
        assert response.status_code == 200

    async def test_delete_is_204_with_no_body(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.delete(f"{BASE}/{PIPELINE_ID}")
        assert response.status_code == 204
        assert response.content == b""

    async def test_run_and_stop_are_202(self, api_client, manager: AsyncMock) -> None:
        """202, not the singleton pair's 200: the CRDs are submitted, not yet RUNNING."""
        run = await api_client.post(f"{BASE}/{PIPELINE_ID}/run")
        stop = await api_client.post(f"{BASE}/{PIPELINE_ID}/stop")
        assert run.status_code == 202
        assert run.json()["status"] == "starting"
        assert stop.status_code == 202
        assert stop.json()["status"] == "stopping"

    async def test_dashboard_snapshot_is_200(self, api_client, manager: AsyncMock) -> None:
        snapshot = CorrelationJobGraphData(blocks=[])
        with patch(
            "apps.routers.correlation_pipelines.get_correlation_dashboard_snapshot",
            new=AsyncMock(return_value=snapshot),
        ) as get_snapshot:
            response = await api_client.get(f"{DASHBOARD}/snapshot")
        assert response.status_code == 200
        assert response.json() == {"blocks": []}
        get_snapshot.assert_awaited_once_with()


class TestCreateAndRunIsTwoCalls:
    async def test_create_never_starts_the_pipeline(self, api_client, manager: AsyncMock) -> None:
        """ "Create & Run" is POST "" then POST /{id}/run — never a run_after_create flag."""
        await api_client.post(BASE, json={"name": "tenant-a"})

        manager.create.assert_awaited_once()
        manager.start.assert_not_awaited()

    async def test_create_rejects_a_run_after_create_flag(self, api_client, manager: AsyncMock) -> None:
        """The create request model is extra="forbid", so the flag cannot be smuggled in."""
        response = await api_client.post(BASE, json={"name": "tenant-a", "run_after_create": True})

        assert response.status_code == 422
        manager.create.assert_not_awaited()


class TestQueryParameters:
    async def test_confirm_restart_reaches_the_manager_as_a_keyword(self, api_client, manager: AsyncMock) -> None:
        await api_client.patch(f"{BASE}/{PIPELINE_ID}?confirm_restart=true", json={"threshold_pct": 75})

        assert manager.update.await_args.kwargs["confirm_restart"] is True

    async def test_confirm_restart_defaults_to_false(self, api_client, manager: AsyncMock) -> None:
        await api_client.patch(f"{BASE}/{PIPELINE_ID}", json={"threshold_pct": 75})

        assert manager.update.await_args.kwargs["confirm_restart"] is False

    async def test_list_passes_typed_pagination(self, api_client, manager: AsyncMock) -> None:
        await api_client.get(f"{BASE}?limit=25&offset=50&sort=attack_chains&order=desc&search=tenant")

        pagination = manager.list_pipelines.await_args.args[0]
        assert pagination == {
            "limit": 25,
            "offset": 50,
            "sort": "attack_chains",
            "order": "desc",
            "search": "tenant",
            "skip": 50,
        }

    async def test_unknown_sort_field_is_422(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.get(f"{BASE}?sort=nonsense")
        assert response.status_code == 422


class TestErrorMapping:
    async def test_not_found_is_404(self, api_client, manager: AsyncMock) -> None:
        manager.get.side_effect = NotFoundError("Correlation pipeline with id x not found")
        response = await api_client.get(f"{BASE}/{PIPELINE_ID}")
        assert response.status_code == 404

    async def test_conflict_is_409(self, api_client, manager: AsyncMock) -> None:
        manager.update.side_effect = ConflictError("restarts a running job")
        response = await api_client.patch(f"{BASE}/{PIPELINE_ID}", json={"agent_resources": {"parallelism": 4}})
        assert response.status_code == 409

    async def test_unknown_body_field_is_422(self, api_client, manager: AsyncMock) -> None:
        """Proves the schema task's extra="forbid" survives to the wire."""
        response = await api_client.patch(f"{BASE}/{PIPELINE_ID}", json={"threshold_pct": 75, "nope": 1})
        assert response.status_code == 422
        manager.update.assert_not_awaited()


class TestDuplicateRoute:
    async def test_returns_201_with_the_new_id(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.post(f"{BASE}/{PIPELINE_ID}/duplicate")

        assert response.status_code == 201
        body = response.json()
        assert body["id"] == str(COPY_ID) != str(PIPELINE_ID)
        assert body["jobs_status"] == "stopped"
        assert body["name"].endswith("(Copy)")
        manager.duplicate.assert_awaited_once()

    async def test_takes_no_body_and_no_query_parameters(self, api_client, manager: AsyncMock) -> None:
        """A body is the seam through which "duplicate but change one thing" creeps in."""
        route = next(
            r for r in app.routes if getattr(r, "path", None) == f"{BASE}/{{correlation_pipeline_id}}/duplicate"
        )
        parameter_names = {p.name for p in route.dependant.query_params}
        assert parameter_names == set()
        assert route.dependant.body_params == []

    async def test_ignores_a_body_that_is_sent_anyway(self, api_client, manager: AsyncMock) -> None:
        response = await api_client.post(f"{BASE}/{PIPELINE_ID}/duplicate", json={"name": "chosen"})

        assert response.status_code == 201
        assert manager.duplicate.await_args.args[0] == PIPELINE_ID

    async def test_name_exhaustion_is_409(self, api_client, manager: AsyncMock) -> None:
        manager.duplicate.side_effect = ConflictError("Could not derive a free name")
        response = await api_client.post(f"{BASE}/{PIPELINE_ID}/duplicate")
        assert response.status_code == 409

    async def test_missing_source_is_404(self, api_client, manager: AsyncMock) -> None:
        manager.duplicate.side_effect = NotFoundError("not found")
        response = await api_client.post(f"{BASE}/{PIPELINE_ID}/duplicate")
        assert response.status_code == 404


class TestRbac:
    @pytest.fixture
    def viewer(self):
        """Run the real ``require_write_access`` against a viewer-shaped user."""
        user = SimpleNamespace(id=uuid.uuid4(), email="viewer@example.com", is_active=True, role=UserRole.VIEWER)
        app.dependency_overrides.pop(require_write_access, None)
        app.dependency_overrides[get_current_active_user] = lambda: user
        yield user

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            ("post", BASE),
            ("patch", f"{BASE}/{PIPELINE_ID}"),
            ("delete", f"{BASE}/{PIPELINE_ID}"),
            ("post", f"{BASE}/{PIPELINE_ID}/duplicate"),
            ("post", f"{BASE}/{PIPELINE_ID}/run"),
            ("post", f"{BASE}/{PIPELINE_ID}/stop"),
        ],
    )
    async def test_viewer_is_forbidden_on_every_mutating_route(
        self, api_client, manager: AsyncMock, viewer, method: str, path: str
    ) -> None:
        """Starting a Flink job is a side effect, not a read — run/stop are write routes."""
        response = await api_client.request(method, path, json={"name": "x"} if method != "delete" else None)
        assert response.status_code == 403

    async def test_viewer_can_still_read(self, api_client, manager: AsyncMock, viewer) -> None:
        assert (await api_client.get(BASE)).status_code == 200
        assert (await api_client.get(f"{BASE}/{PIPELINE_ID}")).status_code == 200
        with patch(
            "apps.routers.correlation_pipelines.get_correlation_dashboard_snapshot",
            new=AsyncMock(return_value=CorrelationJobGraphData(blocks=[])),
        ):
            assert (await api_client.get(f"{DASHBOARD}/snapshot")).status_code == 200


class TestThinRouter:
    def test_router_holds_no_infrastructure(self) -> None:
        """Constraint 1: no Kafka, Kubernetes or activity logging in the router module.

        Checked as code tokens rather than as a plain substring search for "kubernetes": the
        documented ``400`` description legitimately names ``FLINK_PROVIDER=kubernetes``, which is
        the error the manager raises, not a provider call.
        """
        source = pathlib.Path(inspect.getfile(correlation_pipelines_router)).read_text()
        for forbidden in (
            "activity_producer",
            "get_flink_provider",
            "KubernetesFlinkProvider",
            "import kubernetes",
            "AdminClient",
            "chains_config_producer",
        ):
            assert forbidden not in source, f"{forbidden} must not appear in the router"


class TestDashboardStream:
    async def test_the_generator_always_releases_its_subscription(self) -> None:
        """A disconnect after the first frame still unsubscribes (refresher can stop)."""
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
        graph = CorrelationJobGraphData(blocks=[])
        broadcaster = AsyncMock()
        broadcaster.get_cached = lambda: (graph, None)

        with (
            patch.object(correlation_pipelines_router, "correlation_dashboard_broadcaster", broadcaster),
            patch.object(correlation_pipelines_router.settings, "dashboard_broadcast_interval_seconds", 0.01),
        ):
            frames: list[str] = [
                frame async for frame in correlation_pipelines_router._correlation_dashboard_sse_generator(request)
            ]

        assert len(frames) == 1
        assert "correlation_dashboard_update" in frames[0]
        broadcaster.subscribe.assert_awaited_once()
        broadcaster.unsubscribe.assert_awaited_once()

    async def test_generator_aclose_still_unsubscribes(self) -> None:
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        broadcaster = AsyncMock()
        broadcaster.get_cached = lambda: (CorrelationJobGraphData(blocks=[]), None)

        with (
            patch.object(correlation_pipelines_router, "correlation_dashboard_broadcaster", broadcaster),
            patch.object(correlation_pipelines_router.settings, "dashboard_broadcast_interval_seconds", 60.0),
        ):
            gen = correlation_pipelines_router._correlation_dashboard_sse_generator(request)
            await gen.__anext__()
            await gen.aclose()

        broadcaster.subscribe.assert_awaited_once()
        broadcaster.unsubscribe.assert_awaited_once()


class TestDashboardSnapshotCache:
    async def test_reuses_the_live_sse_tick(self) -> None:
        cached = CorrelationJobGraphData(blocks=[])
        fake = SimpleNamespace(is_running=True, get_cached=lambda: (cached, None))
        with (
            patch("apps.managers.correlation_dashboard.correlation_dashboard_broadcaster", fake),
            patch(
                "apps.managers.correlation_dashboard.compute_correlation_dashboard",
                new=AsyncMock(),
            ) as compute,
        ):
            result = await get_correlation_dashboard_snapshot()
        assert result is cached
        compute.assert_not_awaited()

    async def test_computes_when_the_stream_is_idle(self) -> None:
        graph = CorrelationJobGraphData(blocks=[])
        fake = SimpleNamespace(is_running=False, get_cached=lambda: (None, None))
        with (
            patch("apps.managers.correlation_dashboard.correlation_dashboard_broadcaster", fake),
            patch(
                "apps.managers.correlation_dashboard.compute_correlation_dashboard",
                new=AsyncMock(return_value=graph),
            ) as compute,
        ):
            result = await get_correlation_dashboard_snapshot()
        assert result is graph
        compute.assert_awaited_once_with()

    async def test_computes_when_the_cached_tick_failed(self) -> None:
        graph = CorrelationJobGraphData(blocks=[])
        fake = SimpleNamespace(is_running=True, get_cached=lambda: (None, RuntimeError("tick")))
        with (
            patch("apps.managers.correlation_dashboard.correlation_dashboard_broadcaster", fake),
            patch(
                "apps.managers.correlation_dashboard.compute_correlation_dashboard",
                new=AsyncMock(return_value=graph),
            ) as compute,
        ):
            result = await get_correlation_dashboard_snapshot()
        assert result is graph
        compute.assert_awaited_once_with()
