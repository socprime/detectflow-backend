"""Per-detection-pipeline tagged-events topic: provisioning, immutability and the NULL fallback.

Epic Decision 1 replaces the one shared ``detectflow.tagged-events`` topic with
``detectflow.tagged-events.<pipeline_id>``, so two customers' tagged events stop sharing a Kafka
topic. Three invariants are pinned here:

* the topic is created **inside the create/duplicate transaction**, before any other side effect, and a
  broker failure rolls the whole request back — never a pipeline row pointing at a topic that does
  not exist;
* the name is immutable for the pipeline's lifetime (it is baked into the matcher's deploy env and
  into the owning correlation pipeline's ``KAFKA_INPUT_TOPICS``), so ``PipelineUpdateRequest``
  carries no such field — the assign endpoint is the only writer;
* ``tagged_events_topic IS NULL`` is load-bearing: a pre-epic pipeline keeps rendering the shared
  legacy topic, on every provider.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from apps.core.exceptions import ServiceUnavailableError
from apps.core.schemas import PipelineUpdateRequest
from apps.core.settings import settings
from apps.managers import pipeline_orchestrator as po_module
from apps.managers.pipeline_orchestrator import PipelineOrchestrator
from apps.modules.kafka.topic_lifecycle import TopicProvisioningError
from apps.providers.base import FlinkDeploymentConfig

PIPELINE_ID = uuid4()


def _flink_config_stub() -> SimpleNamespace:
    return SimpleNamespace(
        parallelism=1,
        taskmanager_memory_mb=1024,
        taskmanager_cpu=1.0,
        window_size_sec=60,
        checkpoint_interval_sec=60,
        autoscaler_enabled=False,
        autoscaler_min_parallelism=1,
        autoscaler_max_parallelism=1,
    )


def _create_request() -> SimpleNamespace:
    return SimpleNamespace(
        name="p1",
        source_topics=["t1"],
        destination_topic="d1",
        destinations=None,
        log_source_id=str(uuid4()),
        enabled=False,  # skip the Flink deployment path
        save_untagged=False,
        apply_parser_to_output_events=False,
        filters=[],
        custom_fields=None,
        repository_ids=None,
        resources=None,
        kafka_starting_offset_earliest=False,
    )


def _orchestrator(monkeypatch, *, new_pipeline: SimpleNamespace) -> PipelineOrchestrator:
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator.db = SimpleNamespace(refresh=AsyncMock(), commit=AsyncMock())
    orchestrator.pipeline_repo = SimpleNamespace(
        create=AsyncMock(return_value=new_pipeline),
        update=AsyncMock(),
        delete=AsyncMock(),
    )
    orchestrator.config_dao = SimpleNamespace(get_flink_defaults=AsyncMock(return_value={}))
    orchestrator._validate_resources = AsyncMock()
    orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_creation=AsyncMock())
    orchestrator._auto_add_to_correlation_pipelines_non_critical = AsyncMock()

    monkeypatch.setattr(
        po_module.FlinkConfigManager, "resolve_config", staticmethod(lambda **_kw: _flink_config_stub())
    )
    monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
    monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
    return orchestrator


class TestCreateProvisionsTopic:
    @pytest.mark.asyncio
    async def test_create_pipeline_provisions_and_pins_topic(self, monkeypatch) -> None:
        new_pipeline = SimpleNamespace(
            id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1", destinations=[{"topic": "d1"}]
        )
        orchestrator = _orchestrator(monkeypatch, new_pipeline=new_pipeline)
        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.return_value = topic

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            response = await orchestrator.create_pipeline(_create_request(), SimpleNamespace(email="t@example.com"))

        assert response.status is True
        lifecycle.ensure_pipeline_topics.assert_awaited_once_with(PIPELINE_ID)
        assert orchestrator.pipeline_repo.update.await_args.kwargs == {"tagged_events_topic": topic}
        assert orchestrator.pipeline_repo.update.await_args.args[0] == PIPELINE_ID
        # The caller holds the pre-update instance; the deployment render reads it off that object.
        assert new_pipeline.tagged_events_topic == topic

    @pytest.mark.asyncio
    async def test_create_rolls_back_pipeline_when_topic_provisioning_fails(self, monkeypatch) -> None:
        """The enclosing transaction is the rollback — no compensating delete, no early commit."""
        new_pipeline = SimpleNamespace(
            id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1", destinations=[{"topic": "d1"}]
        )
        orchestrator = _orchestrator(monkeypatch, new_pipeline=new_pipeline)
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.side_effect = TopicProvisioningError("broker down")

        with (
            patch.object(po_module, "topic_lifecycle_service", lifecycle),
            pytest.raises(ServiceUnavailableError) as excinfo,
        ):
            await orchestrator.create_pipeline(_create_request(), SimpleNamespace(email="t@example.com"))

        assert excinfo.value.status_code == 503
        assert "p1" in excinfo.value.detail
        orchestrator.db.commit.assert_not_awaited()
        orchestrator.pipeline_repo.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provisioning_precedes_every_other_side_effect(self, monkeypatch) -> None:
        """A failure must emit the fewest side effects: topic first, then repositories/rules."""
        order: list[str] = []
        new_pipeline = SimpleNamespace(
            id=PIPELINE_ID, name="p1", source_topics=["t1"], destination_topic="d1", destinations=[{"topic": "d1"}]
        )
        orchestrator = _orchestrator(monkeypatch, new_pipeline=new_pipeline)
        orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(
            handle_pipeline_creation=AsyncMock(side_effect=lambda **_kw: order.append("rules"))
        )
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.side_effect = lambda _id: order.append("topic") or "t"

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            await orchestrator.create_pipeline(_create_request(), SimpleNamespace(email="t@example.com"))

        assert order == ["topic", "rules"]


class TestDuplicateProvisionsTopic:
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

    @staticmethod
    def _orchestrator(monkeypatch, *, new_pipeline: SimpleNamespace) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(refresh=AsyncMock(), commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(
            get_by_id=AsyncMock(return_value=TestDuplicateProvisionsTopic._source()),
            create=AsyncMock(return_value=new_pipeline),
            update=AsyncMock(),
            delete=AsyncMock(),
        )
        orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(handle_pipeline_duplication=AsyncMock())
        orchestrator._auto_add_to_correlation_pipelines_non_critical = AsyncMock()
        monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @pytest.mark.asyncio
    async def test_duplicate_pipeline_provisions_and_pins_topic(self, monkeypatch) -> None:
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1 (Copy)")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline=new_pipeline)
        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.return_value = topic

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            response = await orchestrator.duplicate_pipeline(
                orchestrator.pipeline_repo.get_by_id.return_value.id, SimpleNamespace(email="t@example.com")
            )

        assert response.status is True
        lifecycle.ensure_pipeline_topics.assert_awaited_once_with(PIPELINE_ID)
        assert orchestrator.pipeline_repo.update.await_args.kwargs == {"tagged_events_topic": topic}
        assert new_pipeline.tagged_events_topic == topic

    @pytest.mark.asyncio
    async def test_duplicate_rolls_back_pipeline_when_topic_provisioning_fails(self, monkeypatch) -> None:
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1 (Copy)")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline=new_pipeline)
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.side_effect = TopicProvisioningError("broker down")

        with (
            patch.object(po_module, "topic_lifecycle_service", lifecycle),
            pytest.raises(ServiceUnavailableError) as excinfo,
        ):
            await orchestrator.duplicate_pipeline(
                orchestrator.pipeline_repo.get_by_id.return_value.id, SimpleNamespace(email="t@example.com")
            )

        assert excinfo.value.status_code == 503
        assert "p1 (Copy)" in excinfo.value.detail
        orchestrator.db.commit.assert_not_awaited()
        orchestrator.pipeline_repo.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_provisioning_precedes_every_other_side_effect(self, monkeypatch) -> None:
        order: list[str] = []
        new_pipeline = SimpleNamespace(id=PIPELINE_ID, name="p1 (Copy)")
        orchestrator = self._orchestrator(monkeypatch, new_pipeline=new_pipeline)
        orchestrator._get_rules_orchestrator = lambda: SimpleNamespace(
            handle_pipeline_duplication=AsyncMock(side_effect=lambda *_args: order.append("rules"))
        )
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.side_effect = lambda _id: order.append("topic") or "t"

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            await orchestrator.duplicate_pipeline(
                orchestrator.pipeline_repo.get_by_id.return_value.id, SimpleNamespace(email="t@example.com")
            )

        assert order == ["topic", "rules"]


class TestImmutability:
    def test_update_request_has_no_tagged_events_topic_field(self) -> None:
        """Hard constraint 1: renaming the topic orphans committed offsets on both sides."""
        assert "tagged_events_topic" not in PipelineUpdateRequest.model_fields

    def test_flink_restart_fields_carries_it_but_cleanup_does_not(self) -> None:
        assert "tagged_events_topic" in po_module.FLINK_RESTART_FIELDS
        assert "tagged_events_topic" not in po_module.CLEANUP_TRIGGER_FIELDS


class TestSharedTopicFallback:
    """Hard constraint 5: a pre-epic pipeline (NULL column) still renders the shared legacy topic."""

    @staticmethod
    def _config(tagged_events_topic: str | None) -> FlinkDeploymentConfig:
        return FlinkDeploymentConfig(
            pipeline_id=str(PIPELINE_ID),
            pipeline_name="p1",
            input_topics=["in"],
            output_topic="out",
            tagged_events_topic=tagged_events_topic,
        )

    @staticmethod
    def _rendered_topic(provider, method: str, config: FlinkDeploymentConfig) -> str:
        """Render through the real provider method with the template + K8s API stubbed out."""
        template = MagicMock()
        template.render.return_value = "{}"
        provider.jinja_env = MagicMock()
        provider.jinja_env.get_template.return_value = template
        getattr(provider, method)(config)
        return template.render.call_args.kwargs["tagged_events_topic"]

    @pytest.mark.parametrize("method", ["_create_flink_deployment", "_update_flink_deployment"])
    def test_kubernetes_renders_shared_topic_when_null(self, method: str) -> None:
        from apps.providers.kubernetes import KubernetesFlinkProvider

        provider = KubernetesFlinkProvider.__new__(KubernetesFlinkProvider)
        provider.namespace = "security"
        provider.flink_image = "image:tag"
        provider.image_pull_policy = "IfNotPresent"
        provider.default_config = {"jobmanager_memory": "1024m", "jobmanager_cpu": 1.0}
        provider.api = MagicMock()
        provider._calculate_autoscaler_quotas = MagicMock(return_value=("1", "1024m"))

        assert self._rendered_topic(provider, method, self._config(None)) == settings.kafka_tagged_events_topic
        assert self._rendered_topic(provider, method, self._config("detectflow.tagged-events.x")) == (
            "detectflow.tagged-events.x"
        )

    def test_cmf_renders_shared_topic_when_null(self) -> None:
        """CMF-hosted detection pipelines are not exempt from per-pipeline isolation."""
        from apps.providers.cmf import CMFFlinkProvider

        provider = CMFFlinkProvider.__new__(CMFFlinkProvider)
        provider.flink_image = "image:tag"
        provider.default_config = {"jobmanager_memory": "1024m", "jobmanager_cpu": 1.0}

        assert self._rendered_topic(provider, "_render_manifest", self._config(None)) == (
            settings.kafka_tagged_events_topic
        )
        assert self._rendered_topic(provider, "_render_manifest", self._config("detectflow.tagged-events.x")) == (
            "detectflow.tagged-events.x"
        )

    def test_deployment_config_carries_the_column(self) -> None:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        pipeline = SimpleNamespace(
            id=PIPELINE_ID,
            name="p1",
            source_topics=["in"],
            destination_topic="out",
            tagged_events_topic="detectflow.tagged-events.x",
            parallelism=1,
            save_untagged=False,
            apply_parser_to_output_events=False,
            taskmanager_memory_mb=1024,
            taskmanager_cpu=1.0,
            window_size_sec=60,
            checkpoint_interval_sec=60,
            autoscaler_enabled=False,
            autoscaler_min_parallelism=1,
            autoscaler_max_parallelism=1,
            rules_topic=None,
            metrics_topic=None,
        )

        assert orchestrator._build_deployment_config(pipeline).tagged_events_topic == "detectflow.tagged-events.x"


class TestTopicPickerHidesGeneratedTopics:
    """The pickers fed by ``/topics`` and ``/parsers/run`` must not grow with the tenant count."""

    @staticmethod
    def _listed(names: list[str]) -> list[str]:
        from apps.modules.kafka.topics import KafkaTopicsService

        service = KafkaTopicsService.__new__(KafkaTopicsService)
        metadata = MagicMock()
        metadata.topics = dict.fromkeys(names, object())
        admin = MagicMock()
        admin.list_topics.return_value = metadata
        service._get_admin_client = lambda: admin
        service.bootstrap_servers = "broker:9092"
        return service._get_topics_sync()

    def test_per_entity_topics_are_hidden(self) -> None:
        entity_id = str(uuid4())
        listed = self._listed(
            [
                "customer-events",
                f"{settings.kafka_tagged_events_topic}.{entity_id}",
                f"{settings.kafka_chains_raw_topic}.{entity_id}",
                f"{settings.kafka_chains_analysed_topic}.{entity_id}",
                f"{settings.kafka_chains_config_topic}.{entity_id}",
                "__consumer_offsets",
            ]
        )

        assert listed == ["customer-events"]

    def test_the_bare_shared_topics_stay_visible(self) -> None:
        """Scoped to the dynamic namespace only — existing picker contents are unchanged."""
        shared = [
            settings.kafka_tagged_events_topic,
            settings.kafka_chains_raw_topic,
            settings.kafka_sigma_rules_topic,
        ]

        assert self._listed([*shared, "customer-events"]) == sorted([*shared, "customer-events"])


class TestAssignEndpoint:
    """Deliverable 4 — the ONE operation that restarts a customer's matcher."""

    @staticmethod
    def _orchestrator(monkeypatch, existing) -> PipelineOrchestrator:
        orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
        orchestrator.db = SimpleNamespace(commit=AsyncMock())
        orchestrator.pipeline_repo = SimpleNamespace(
            get_by_id=AsyncMock(return_value=existing),
            update=AsyncMock(),
        )
        orchestrator._handle_restart_scenario = AsyncMock()
        orchestrator._sync_correlation_pipelines_non_critical = AsyncMock()
        monkeypatch.setattr(po_module.activity_producer, "log_action", AsyncMock())
        monkeypatch.setattr(po_module.dashboard_service, "invalidate_pipeline_cache", lambda pipeline_id=None: None)
        return orchestrator

    @staticmethod
    def _existing(*, enabled: bool = True, topic: str | None = None) -> SimpleNamespace:
        return SimpleNamespace(
            id=PIPELINE_ID,
            name="p1",
            enabled=enabled,
            tagged_events_topic=topic,
            log_source_id=uuid4(),
            custom_fields=None,
        )

    @pytest.mark.asyncio
    async def test_requires_explicit_confirmation(self, monkeypatch) -> None:
        from apps.core.exceptions import BadRequestError

        orchestrator = self._orchestrator(monkeypatch, self._existing())

        with pytest.raises(BadRequestError):
            await orchestrator.assign_tagged_events_topic(
                PIPELINE_ID, SimpleNamespace(confirm_restart=False), SimpleNamespace(email="t@example.com")
            )

        orchestrator.pipeline_repo.get_by_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_assigns_and_restarts_an_enabled_pipeline(self, monkeypatch) -> None:
        existing = self._existing(enabled=True)
        orchestrator = self._orchestrator(monkeypatch, existing)
        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.return_value = topic

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            await orchestrator.assign_tagged_events_topic(
                PIPELINE_ID, SimpleNamespace(confirm_restart=True), SimpleNamespace(email="t@example.com")
            )

        assert orchestrator.pipeline_repo.update.await_args.kwargs == {"tagged_events_topic": topic}
        # One restart implementation for the whole orchestrator.
        orchestrator._handle_restart_scenario.assert_awaited_once()
        changes = orchestrator._handle_restart_scenario.await_args.args[2]
        assert changes.update_data == {"tagged_events_topic": topic}
        assert changes.flink_restart_needed is True
        assert changes.cleanup_needed is False
        orchestrator._sync_correlation_pipelines_non_critical.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_does_not_restart_a_disabled_pipeline(self, monkeypatch) -> None:
        orchestrator = self._orchestrator(monkeypatch, self._existing(enabled=False))
        lifecycle = AsyncMock()
        lifecycle.ensure_pipeline_topics.return_value = "detectflow.tagged-events.x"

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            await orchestrator.assign_tagged_events_topic(
                PIPELINE_ID, SimpleNamespace(confirm_restart=True), SimpleNamespace(email="t@example.com")
            )

        orchestrator._handle_restart_scenario.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_is_idempotent_and_never_restarts_twice(self, monkeypatch) -> None:
        orchestrator = self._orchestrator(monkeypatch, self._existing(topic="detectflow.tagged-events.already"))
        lifecycle = AsyncMock()

        with patch.object(po_module, "topic_lifecycle_service", lifecycle):
            response = await orchestrator.assign_tagged_events_topic(
                PIPELINE_ID, SimpleNamespace(confirm_restart=True), SimpleNamespace(email="t@example.com")
            )

        assert response.status is True
        lifecycle.ensure_pipeline_topics.assert_not_awaited()
        orchestrator._handle_restart_scenario.assert_not_awaited()
        orchestrator.pipeline_repo.update.assert_not_awaited()
