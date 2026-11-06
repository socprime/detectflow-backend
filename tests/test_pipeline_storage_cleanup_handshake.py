"""Never `rm -rf` a detection pipeline's state while its TaskManager still holds the mount.

The cleanup is a busybox Job that deletes the pipeline's state directory, and since §22 the Flink
pods bind-mount **that directory itself** through `subPath` rather than its parent. Running it under
a live mount leaves the TaskManager writing into an orphaned inode: no error, no data, and nothing
visible until the pod restarts into an empty state root.

So the order is delete → wait for termination → wipe, and a wait that does not confirm is a *skip*,
never a "clean anyway". A cleanup that never runs costs a replay of stale checkpoints; one that runs
against a live mount costs the state. The correlation runtime has always worked this way
(``teardown_deployment_pair``); these tests pin the detection half, which did not.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from apps.managers import pipeline_orchestrator as po_module
from apps.managers.pipeline_orchestrator import PipelineChanges, PipelineOrchestrator
from apps.providers.base import StorageCleanupOutcome

PIPELINE_ID = UUID("aaaaaaaa-0000-4000-8000-000000000001")


def _user() -> SimpleNamespace:
    return SimpleNamespace(id=UUID("550e8400-e29b-41d4-a716-446655440000"), email="admin@example.com")


def _orchestrator(monkeypatch) -> PipelineOrchestrator:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = SimpleNamespace(commit=AsyncMock())
    orchestrator._flink = AsyncMock()
    orchestrator._flink.create_deployment = AsyncMock(return_value=SimpleNamespace(success=True, deployment_name="d"))
    orchestrator._flink.update_deployment = AsyncMock(return_value=SimpleNamespace(success=True, deployment_name="d"))
    orchestrator._build_deployment_config = lambda _pipeline: SimpleNamespace(pipeline_id=str(PIPELINE_ID))
    orchestrator._log_flink_warning = AsyncMock()
    orchestrator._log_k8s_warning = AsyncMock()
    # Beyond the handshake under test: the restart scenario resyncs rules/filters afterwards.
    orchestrator._sync_kafka_data_on_enable = AsyncMock()
    orchestrator._flink.cleanup_storage = AsyncMock(side_effect=lambda key: StorageCleanupOutcome(key, "succeeded"))
    monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
    return orchestrator


def _changes(*, cleanup_needed: bool) -> PipelineChanges:
    return PipelineChanges(
        update_data={},
        flink_restart_needed=True,
        cleanup_needed=cleanup_needed,
        filters_changed=False,
        log_source_changed=False,
        old_log_source_id=None,
        new_log_source_id=None,
        custom_fields_changed=False,
        old_custom_fields=None,
        new_custom_fields=None,
    )


class TestTerminateThenCleanup:
    @pytest.mark.asyncio
    async def test_state_is_wiped_only_after_the_deployment_is_gone(self, monkeypatch) -> None:
        order: list[str] = []
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock(side_effect=lambda _id: order.append("deleted"))
        orchestrator._flink.cleanup_storage = AsyncMock(
            side_effect=lambda key: order.append("cleaned") or StorageCleanupOutcome(key, "succeeded")
        )

        assert await orchestrator._terminate_then_cleanup(PIPELINE_ID, operation="delete") == "ok"

        assert order == ["deleted", "cleaned"]

    @pytest.mark.asyncio
    async def test_a_wait_that_never_confirms_leaves_the_state_alone(self, monkeypatch) -> None:
        """``delete_deployment(wait=True)`` raises when the CR outlives the timeout — its pods may
        still hold the mount, so the busybox Job must not be dispatched."""
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock(side_effect=Exception("Timeout waiting for deletion"))
        orchestrator._flink.cleanup_storage = AsyncMock()

        outcome = await orchestrator._terminate_then_cleanup(PIPELINE_ID, operation="delete")

        assert outcome == "skipped: wait_timeout"
        orchestrator._flink.cleanup_storage.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_skip_is_recorded_never_silently_swallowed(self, monkeypatch) -> None:
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock(side_effect=Exception("Timeout waiting for deletion"))

        await orchestrator._terminate_then_cleanup(PIPELINE_ID, operation="delete")

        orchestrator._log_flink_warning.assert_awaited_once()
        entry = po_module.activity_producer.log_action.await_args.kwargs
        assert entry["severity"] == "warning"
        assert "cleanup skipped" in entry["details"].lower()

    @pytest.mark.asyncio
    async def test_a_failed_cleanup_is_reported_but_not_fatal(self, monkeypatch) -> None:
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock()
        orchestrator._flink.cleanup_storage = AsyncMock(side_effect=Exception("Job creation failed"))

        assert await orchestrator._terminate_then_cleanup(PIPELINE_ID, operation="delete") == "error"

        orchestrator._log_k8s_warning.assert_awaited_once()


class TestSourceTopicsChangeIsAColdStart:
    """``source_topics`` changed ⇒ the checkpointed offsets are invalid ⇒ the state must really go.

    It used to wipe the state while the deployment stayed up, which got neither half: the mount was
    corrupted AND the reset did not take, because a `last-state` upgrade restores the source's
    assigned splits from the checkpoint rather than from the rendered topic list.
    """

    @pytest.mark.asyncio
    async def test_it_deletes_wipes_and_recreates(self, monkeypatch) -> None:
        order: list[str] = []
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock(side_effect=lambda _id: order.append("deleted"))
        orchestrator._flink.cleanup_storage = AsyncMock(
            side_effect=lambda key: order.append("cleaned") or StorageCleanupOutcome(key, "succeeded")
        )
        orchestrator._flink.create_deployment = AsyncMock(
            side_effect=lambda _c: order.append("created") or SimpleNamespace(success=True, deployment_name="d")
        )

        await orchestrator._handle_restart_scenario(
            PIPELINE_ID, SimpleNamespace(id=PIPELINE_ID, name="p1"), _changes(cleanup_needed=True)
        )

        assert order == ["deleted", "cleaned", "created"]
        # `update_deployment` would 404 on a CR we just deleted.
        orchestrator._flink.update_deployment.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_ordinary_config_change_still_rolls_in_place(self, monkeypatch) -> None:
        """Only `source_topics` invalidates offsets; everything else keeps its checkpoints."""
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock()
        orchestrator._flink.cleanup_storage = AsyncMock()

        await orchestrator._handle_restart_scenario(
            PIPELINE_ID, SimpleNamespace(id=PIPELINE_ID, name="p1"), _changes(cleanup_needed=False)
        )

        orchestrator._flink.delete_deployment.assert_not_awaited()
        orchestrator._flink.cleanup_storage.assert_not_awaited()
        orchestrator._flink.update_deployment.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_failed_teardown_falls_back_to_the_in_place_upgrade(self, monkeypatch) -> None:
        """The pipeline must not be left down because its state could not be wiped.

        The offsets are then NOT reset — the skip is already recorded as a warning activity entry
        by the handshake, which is the signal an operator acts on.
        """
        orchestrator = _orchestrator(monkeypatch)
        orchestrator._flink.delete_deployment = AsyncMock(side_effect=Exception("Timeout waiting for deletion"))
        orchestrator._flink.cleanup_storage = AsyncMock()

        await orchestrator._handle_restart_scenario(
            PIPELINE_ID, SimpleNamespace(id=PIPELINE_ID, name="p1"), _changes(cleanup_needed=True)
        )

        orchestrator._flink.cleanup_storage.assert_not_awaited()
        orchestrator._flink.create_deployment.assert_not_awaited()
        orchestrator._flink.update_deployment.assert_awaited_once()
