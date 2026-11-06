"""Detection-pipeline lifecycle → correlation-pipeline membership drift.

The pre-epic hook restarted the ONE singleton correlation job whenever the set of enabled
pipelines changed. With per-pipeline tagged-events topics that is both too broad and too narrow:

* too broad — enable/disable no longer changes any correlation job's ``KAFKA_INPUT_TOPICS``
  (a member's topic exists from creation), so two restarts per toggle were pure downtime;
* too narrow — a member re-pointing its ``destination_topic`` is SIEM output only. Correlation
  isolation is membership → tagged-events; destination edits must not republish or restart.

Everything correlation-side is non-critical by contract (hard constraint 3). A detection
pipeline may belong to several correlation jobs; the fan-out loops every owner.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from apps.managers import pipeline_orchestrator as po_module
from apps.managers.pipeline_orchestrator import PipelineChanges, PipelineOrchestrator

PIPELINE_ID = uuid4()
CORRELATION_ID = uuid4()
OTHER_CORRELATION_ID = uuid4()


def _user() -> SimpleNamespace:
    return SimpleNamespace(email="admin@example.com")


def _bare_orchestrator() -> PipelineOrchestrator:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = AsyncMock()
    return orchestrator


def _no_op_changes(**overrides) -> PipelineChanges:
    base = {
        "update_data": {},
        "flink_restart_needed": False,
        "cleanup_needed": False,
        "filters_changed": False,
        "log_source_changed": False,
        "old_log_source_id": None,
        "new_log_source_id": None,
        "custom_fields_changed": False,
        "old_custom_fields": None,
        "new_custom_fields": None,
    }
    return PipelineChanges(**{**base, **overrides})


class TestMembershipLookup:
    @pytest.mark.asyncio
    async def test_membership_lookup_restarts_every_owning_correlation_pipeline(self) -> None:
        orchestrator = _bare_orchestrator()
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = [CORRELATION_ID, OTHER_CORRELATION_ID]

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            await orchestrator._sync_correlation_pipelines_non_critical(_user(), PIPELINE_ID, reason="test")

        manager.get_ids_by_member.assert_awaited_once_with(PIPELINE_ID)
        assert manager.restart_if_running.await_count == 2
        assert [call.args[0] for call in manager.restart_if_running.await_args_list] == [
            CORRELATION_ID,
            OTHER_CORRELATION_ID,
        ]

    @pytest.mark.asyncio
    async def test_an_unowned_pipeline_restarts_nothing(self) -> None:
        orchestrator = _bare_orchestrator()
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = []

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            await orchestrator._sync_correlation_pipelines_non_critical(_user(), PIPELINE_ID, reason="test")

        manager.restart_if_running.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_correlation_failure_does_not_fail_pipeline_operation(self) -> None:
        """Hard constraint 3, both layers: the lookup and the per-pipeline refresh."""
        orchestrator = _bare_orchestrator()

        lookup_failed = AsyncMock()
        lookup_failed.get_ids_by_member.side_effect = RuntimeError("DB down")
        with patch.object(po_module, "CorrelationPipelinesManager", return_value=lookup_failed):
            await orchestrator._sync_correlation_pipelines_non_critical(_user(), PIPELINE_ID, reason="test")

        restart_failed = AsyncMock()
        restart_failed.get_ids_by_member.return_value = [CORRELATION_ID]
        restart_failed.restart_if_running.side_effect = RuntimeError("k8s down")
        with patch.object(po_module, "CorrelationPipelinesManager", return_value=restart_failed):
            await orchestrator._sync_correlation_pipelines_non_critical(_user(), PIPELINE_ID, reason="test")


class TestCreate:
    @staticmethod
    def _orchestrator(monkeypatch, new_pipeline) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(refresh=AsyncMock(), commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(create=AsyncMock(return_value=new_pipeline), update=AsyncMock())
        orchestrator.config_dao = SimpleNamespace(get_flink_defaults=AsyncMock(return_value={}))
        orchestrator._validate_resources = AsyncMock()
        orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_creation=AsyncMock())
        orchestrator._provision_tagged_events_topic = AsyncMock()
        orchestrator._create_flink_deployment = AsyncMock()
        monkeypatch.setattr(
            po_module.FlinkConfigManager,
            "resolve_config",
            staticmethod(
                lambda **_kw: SimpleNamespace(
                    parallelism=1,
                    taskmanager_memory_mb=1024,
                    taskmanager_cpu=1.0,
                    window_size_sec=60,
                    checkpoint_interval_sec=60,
                    autoscaler_enabled=False,
                    autoscaler_min_parallelism=1,
                    autoscaler_max_parallelism=1,
                )
            ),
        )
        monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @staticmethod
    def _request(*, enabled: bool) -> SimpleNamespace:
        return SimpleNamespace(
            name="p1",
            source_topics=["t1"],
            destination_topic="d1",
            destinations=None,
            log_source_id=str(uuid4()),
            enabled=enabled,
            save_untagged=False,
            apply_parser_to_output_events=False,
            filters=[],
            custom_fields=None,
            repository_ids=None,
            resources=None,
            kafka_starting_offset_earliest=False,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("enabled", [True, False])
    async def test_create_auto_adds_to_matching_auto_add_correlation_pipelines_only(
        self, monkeypatch, enabled: bool
    ) -> None:
        """Auto-add matches the detection pipeline's name, not whether the matcher is running.

        Which correlation pipelines match (``auto_add_new_pipelines`` + the name glob) belongs to
        ``CorrelationPipelinesManager.auto_add_member``; this pins
        that the create path delegates to it, once, for the new pipeline.
        """
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline)
        manager = AsyncMock()
        manager.auto_add_member.return_value = [CORRELATION_ID]

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            await orchestrator.create_pipeline(self._request(enabled=enabled), _user())

        manager.auto_add_member.assert_awaited_once()
        assert manager.auto_add_member.await_args.args[0] == PIPELINE_ID

    @pytest.mark.asyncio
    async def test_create_restarts_only_the_owning_correlation_pipeline(self, monkeypatch) -> None:
        """The restart rides ``auto_add_member`` (restart-if-running), never a global sweep."""
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline)
        manager = AsyncMock()

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            await orchestrator.create_pipeline(self._request(enabled=True), _user())

        manager.restart_if_running.assert_not_awaited()
        assert not hasattr(orchestrator, "_sync_correlation_job_topics_non_critical")

    @pytest.mark.asyncio
    async def test_auto_add_failure_does_not_fail_the_create(self, monkeypatch) -> None:
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline)
        manager = AsyncMock()
        manager.auto_add_member.side_effect = RuntimeError("k8s down")

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            response = await orchestrator.create_pipeline(self._request(enabled=True), _user())

        assert response.status is True


class TestDuplicate:
    @staticmethod
    def _orchestrator(monkeypatch, source, new_pipeline) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(refresh=AsyncMock(), commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(
            get_by_id=AsyncMock(return_value=source),
            create=AsyncMock(return_value=new_pipeline),
        )
        orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_duplication=AsyncMock())
        orchestrator._provision_tagged_events_topic = AsyncMock()
        monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @staticmethod
    def _source() -> SimpleNamespace:
        return SimpleNamespace(
            id=uuid4(),
            name="p1",
            source_topics=["t1"],
            destination_topic="d1",
            destinations=[{"topic": "d1"}],
            log_source_id=uuid4(),
            save_untagged=False,
            apply_parser_to_output_events=False,
            filters=[],
            custom_fields=None,
            parallelism=1,
            taskmanager_memory_mb=1024,
            taskmanager_cpu=1.0,
            window_size_sec=60,
            checkpoint_interval_sec=60,
            autoscaler_enabled=False,
            autoscaler_min_parallelism=1,
            autoscaler_max_parallelism=1,
        )

    @pytest.mark.asyncio
    async def test_duplicate_auto_adds_even_though_the_copy_is_disabled(self, monkeypatch) -> None:
        """Auto-add matches the copy's name, not whether the matcher is running — a copy is always disabled."""
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1 (Copy)")
        orchestrator = self._orchestrator(monkeypatch, self._source(), new_pipeline)
        manager = AsyncMock()
        manager.auto_add_member.return_value = [CORRELATION_ID]
        user = _user()

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            await orchestrator.duplicate_pipeline(orchestrator.pipeline_repo.get_by_id.return_value.id, user)

        manager.auto_add_member.assert_awaited_once_with(PIPELINE_ID, user)
        orchestrator._provision_tagged_events_topic.assert_awaited_once_with(new_pipeline)

    @pytest.mark.asyncio
    async def test_auto_add_failure_does_not_fail_the_duplicate(self, monkeypatch) -> None:
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1 (Copy)")
        orchestrator = self._orchestrator(monkeypatch, self._source(), new_pipeline)
        manager = AsyncMock()
        manager.auto_add_member.side_effect = RuntimeError("k8s down")

        with patch.object(po_module, "CorrelationPipelinesManager", return_value=manager):
            response = await orchestrator.duplicate_pipeline(
                orchestrator.pipeline_repo.get_by_id.return_value.id, _user()
            )

        assert response.status is True


class TestUpdate:
    @staticmethod
    def _orchestrator(monkeypatch, *, updated) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(
            get_with_relations=AsyncMock(return_value=updated),
            get_repository_selections=AsyncMock(return_value={}),
            update=AsyncMock(return_value=updated),
            get_by_id=AsyncMock(return_value=updated),
        )
        for scenario in (
            "_handle_disable_scenario",
            "_handle_enable_scenario",
            "_handle_stays_disabled_scenario",
            "_handle_restart_scenario",
            "_handle_hot_reload_scenario",
            "_log_update_activity",
        ):
            setattr(orchestrator, scenario, AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("old_enabled", "new_enabled"),
        [(True, False), (False, True)],
    )
    async def test_enable_disable_does_not_restart_any_correlation_pipeline(
        self, monkeypatch, old_enabled: bool, new_enabled: bool
    ) -> None:
        """The removed ``pipeline_update_active_set`` half: two restarts per toggle, for nothing.

        A member's tagged-events topic exists from creation, so a disabled pipeline just stops
        producing into a topic that still exists — the correlation input list is unchanged.
        """
        existing = SimpleNamespace(id=PIPELINE_ID, name="p1", enabled=old_enabled, filters=[])
        updated = SimpleNamespace(id=PIPELINE_ID, name="p1", enabled=new_enabled, filters=[])
        orchestrator = self._orchestrator(monkeypatch, updated=updated)
        orchestrator.pipeline_repo.get_with_relations = AsyncMock(return_value=existing)
        sync = AsyncMock()
        orchestrator._sync_correlation_pipelines_non_critical = sync

        monkeypatch.setattr(
            po_module,
            "detect_pipeline_changes",
            lambda _request, _existing: _no_op_changes(update_data={"enabled": new_enabled}),
        )
        await orchestrator.update_pipeline(PIPELINE_ID, SimpleNamespace(repository_ids=None, filters=None), _user())

        sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_destination_topic_change_does_not_touch_correlation(self, monkeypatch) -> None:
        """Destination topics are SIEM output. Isolation is membership → tagged-events."""
        pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1", enabled=True, filters=[])
        orchestrator = self._orchestrator(monkeypatch, updated=pipeline)
        sync = AsyncMock()
        orchestrator._sync_correlation_pipelines_non_critical = sync

        monkeypatch.setattr(
            po_module,
            "detect_pipeline_changes",
            lambda _request, _existing: _no_op_changes(update_data={"destination_topic": "new-out"}),
        )
        await orchestrator.update_pipeline(PIPELINE_ID, SimpleNamespace(repository_ids=None, filters=None), _user())

        sync.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_tagged_events_topic_change_cold_starts_the_owning_correlation_pipeline(self, monkeypatch) -> None:
        """The assign path's own trigger, and it changes KAFKA_INPUT_TOPICS by SUBTRACTION.

        The member stops feeding the topic it fed before — usually the shared legacy one, which
        carries other customers' events. A re-render would leave that topic assigned to the restored
        source and the job would keep reading it, so this call must ask for a cold start.
        """
        pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1", enabled=True, filters=[])
        orchestrator = self._orchestrator(monkeypatch, updated=pipeline)
        sync = AsyncMock()
        orchestrator._sync_correlation_pipelines_non_critical = sync

        monkeypatch.setattr(
            po_module,
            "detect_pipeline_changes",
            lambda _request, _existing: _no_op_changes(
                update_data={"tagged_events_topic": "detectflow.tagged-events.x"}, flink_restart_needed=True
            ),
        )
        await orchestrator.update_pipeline(PIPELINE_ID, SimpleNamespace(repository_ids=None, filters=None), _user())

        sync.assert_awaited_once()
        assert sync.await_args.kwargs["reason"] == "pipeline_tagged_topic_assigned"
        assert sync.await_args.kwargs["cold_start"] is True


class TestDelete:
    @staticmethod
    def _orchestrator(monkeypatch, existing, order: list[str]) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(
            get_by_id=AsyncMock(return_value=existing),
            delete=AsyncMock(side_effect=lambda _id: order.append("row_deleted") or True),
        )
        orchestrator._flink = AsyncMock()
        orchestrator._delete_from_kafka = AsyncMock()
        monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @staticmethod
    def _existing(*, enabled: bool = True, topic: str | None = "detectflow.tagged-events.x") -> SimpleNamespace:
        return SimpleNamespace(id=PIPELINE_ID, name="p1", enabled=enabled, tagged_events_topic=topic, filters=[])

    @pytest.mark.asyncio
    async def test_delete_captures_membership_before_cascade_and_retires_topic_after_restart(self, monkeypatch) -> None:
        """Both orderings are load-bearing.

        The ``ON DELETE CASCADE`` on ``correlation_pipeline_members`` destroys the join row, so the
        lookup must precede the delete; and retiring a topic a running correlation job still
        subscribes to produces UNKNOWN_TOPIC_OR_PARTITION churn, so it must follow the re-render.
        """
        order: list[str] = []
        orchestrator = self._orchestrator(monkeypatch, self._existing(), order)

        manager = AsyncMock()
        manager.get_ids_by_member.side_effect = lambda _id: order.append("membership_read") or [CORRELATION_ID]
        manager.restart_if_running.side_effect = lambda *_a, **_kw: order.append("correlation_restarted")
        lifecycle = AsyncMock()
        lifecycle.retire_pipeline_topics.side_effect = lambda _id: order.append("topic_retired") or []

        with (
            patch.object(po_module, "CorrelationPipelinesManager", return_value=manager),
            patch.object(po_module, "topic_lifecycle_service", lifecycle),
        ):
            await orchestrator.delete_pipeline(PIPELINE_ID, _user())

        assert order == ["membership_read", "row_deleted", "correlation_restarted", "topic_retired"]
        # Deleting a member IS a removal: a re-rendered job would keep consuming the departed
        # pipeline's tagged-events topic from its restored splits.
        assert manager.restart_if_running.await_args.kwargs["cold_start"] is True

    @pytest.mark.asyncio
    async def test_delete_of_a_disabled_pipeline_still_refreshes_correlation(self, monkeypatch) -> None:
        """The hook is unconditional now: a disabled member still occupies the input topic list."""
        orchestrator = self._orchestrator(monkeypatch, self._existing(enabled=False), [])
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = [CORRELATION_ID]

        with (
            patch.object(po_module, "CorrelationPipelinesManager", return_value=manager),
            patch.object(po_module, "topic_lifecycle_service", AsyncMock()),
        ):
            await orchestrator.delete_pipeline(PIPELINE_ID, _user())

        manager.restart_if_running.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_membership_is_cascade_removed_and_logged_never_blocked(self, monkeypatch) -> None:
        """Deliverable 6: a correlation pipeline is a view; deleting the asset is not an error."""
        orchestrator = self._orchestrator(monkeypatch, self._existing(), [])
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = [CORRELATION_ID]
        log_action = AsyncMock()
        monkeypatch.setattr(po_module.activity_producer, "log_action", log_action)

        with (
            patch.object(po_module, "CorrelationPipelinesManager", return_value=manager),
            patch.object(po_module, "topic_lifecycle_service", AsyncMock()),
        ):
            response = await orchestrator.delete_pipeline(PIPELINE_ID, _user())

        assert response.status is True
        detach_entries = [
            call for call in log_action.await_args_list if call.kwargs.get("entity_type") == "correlation_pipeline"
        ]
        assert len(detach_entries) == 1
        assert detach_entries[0].kwargs["entity_id"] == str(CORRELATION_ID)
        assert "Removed member pipeline p1 (deleted)" == detach_entries[0].kwargs["details"]

    @pytest.mark.asyncio
    async def test_a_legacy_pipeline_retires_no_topic(self, monkeypatch) -> None:
        """NULL means it never owned one — the shared legacy topic is never retired."""
        orchestrator = self._orchestrator(monkeypatch, self._existing(topic=None), [])
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = []
        lifecycle = AsyncMock()

        with (
            patch.object(po_module, "CorrelationPipelinesManager", return_value=manager),
            patch.object(po_module, "topic_lifecycle_service", lifecycle),
        ):
            await orchestrator.delete_pipeline(PIPELINE_ID, _user())

        lifecycle.retire_pipeline_topics.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_correlation_failure_does_not_fail_the_delete(self, monkeypatch) -> None:
        orchestrator = self._orchestrator(monkeypatch, self._existing(), [])
        manager = AsyncMock()
        manager.get_ids_by_member.return_value = [CORRELATION_ID, OTHER_CORRELATION_ID]
        manager.restart_if_running.side_effect = RuntimeError("k8s down")
        lifecycle = AsyncMock()
        lifecycle.retire_pipeline_topics.side_effect = RuntimeError("broker down")

        with (
            patch.object(po_module, "CorrelationPipelinesManager", return_value=manager),
            patch.object(po_module, "topic_lifecycle_service", lifecycle),
        ):
            response = await orchestrator.delete_pipeline(PIPELINE_ID, _user())

        assert response.status is True
        # One failing correlation pipeline must not stop the next from being refreshed.
        assert manager.restart_if_running.await_count == 2
