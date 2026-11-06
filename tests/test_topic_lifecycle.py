"""Unit tests for `apps/modules/kafka/topic_lifecycle.py` — mocked AdminClient, no broker.

Covers the two things that can lose customer data: composing a topic name for the wrong entity
(or for a shared topic), and deleting a topic somebody still reads.
"""

from concurrent.futures import Future
from types import SimpleNamespace

import pytest
from confluent_kafka import ConsumerGroupState, KafkaError, KafkaException

PIPELINE_ID = "11111111-2222-3333-4444-555555555555"
CORRELATION_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _future(result=None, exception: Exception | None = None) -> Future:
    future: Future = Future()
    if exception is not None:
        future.set_exception(exception)
    else:
        future.set_result(result)
    return future


def _kafka_exception(name: str) -> KafkaException:
    """A KafkaException whose string carries the broker error name the code matches on."""
    return KafkaException(KafkaError(KafkaError._ALL_BROKERS_DOWN, name, fatal=False))


class FakeAdminClient:
    """Minimal AdminClient stand-in recording every admin call the lifecycle service makes."""

    def __init__(
        self,
        existing: dict[str, int] | None = None,
        create_errors: dict[str, Exception] | None = None,
        groups: list[SimpleNamespace] | None = None,
        committed: dict[str, dict[int, int]] | None = None,
        committed_topic: str = "",
        end_offsets: dict[int, int] | None = None,
    ) -> None:
        self.existing = existing or {}  # topic -> partition count
        self.create_errors = create_errors or {}
        self.groups = groups or []
        self.committed = committed or {}  # group -> {partition: offset} on `committed_topic`
        self.committed_topic = committed_topic
        self.end_offsets = end_offsets or {}
        self.created: list = []
        self.altered: list = []
        self.deleted: list[str] = []
        self.offset_polls = 0

    # --- metadata -------------------------------------------------------
    def list_topics(self, topic: str | None = None, timeout: int = 10):
        names = [topic] if topic else list(self.existing)
        topics = {
            name: SimpleNamespace(partitions={i: object() for i in range(self.existing[name])})
            for name in names
            if name in self.existing
        }
        return SimpleNamespace(topics=topics)

    # --- topics ---------------------------------------------------------
    def create_topics(self, new_topics, request_timeout: int = 30):
        self.created.extend(new_topics)
        return {new_topic.topic: _future(exception=self.create_errors.get(new_topic.topic)) for new_topic in new_topics}

    def delete_topics(self, topics, request_timeout: int = 30):
        self.deleted.extend(topics)
        return {topic: _future(None) for topic in topics}

    def incremental_alter_configs(self, resources, request_timeout: int = 30):
        self.altered.extend(resources)
        return {resource: _future(None) for resource in resources}

    # --- consumer groups ------------------------------------------------
    def list_consumer_groups(self, request_timeout: int = 30):
        return _future(SimpleNamespace(valid=self.groups, errors=[]))

    def list_consumer_group_offsets(self, requests, request_timeout: int = 30):
        self.offset_polls += 1
        return {
            request.group_id: _future(
                SimpleNamespace(
                    topic_partitions=[
                        SimpleNamespace(topic=self.committed_topic, partition=partition, offset=offset)
                        for partition, offset in self.committed.get(request.group_id, {}).items()
                    ]
                )
            )
            for request in requests
        }

    def list_offsets(self, request, request_timeout: int = 30):
        return {
            topic_partition: _future(SimpleNamespace(offset=self.end_offsets.get(topic_partition.partition, 0)))
            for topic_partition in request
        }


def _group(group_id: str, state=ConsumerGroupState.EMPTY) -> SimpleNamespace:
    return SimpleNamespace(group_id=group_id, state=state)


@pytest.fixture
def service(setup_test_env):
    """A lifecycle service instance with no AdminClient wired yet."""
    from apps.modules.kafka.topic_lifecycle import KafkaTopicLifecycleService

    return KafkaTopicLifecycleService()


def _wire(service, admin: FakeAdminClient) -> FakeAdminClient:
    service._admin_client = admin
    return admin


class TestTopicNames:
    """Name composition: one builder, and nothing illegal reaches a broker."""

    @pytest.mark.parametrize("entity_id", ["a/b", "", " ", ".", "...", "x" * 300])
    def test_illegal_entity_ids_raise(self, entity_id: str, setup_test_env):
        from apps.modules.kafka.topic_lifecycle import TopicProvisioningError, tagged_events_topic_for

        with pytest.raises(TopicProvisioningError):
            tagged_events_topic_for(entity_id)

    def test_wrappers_match_the_pure_builders(self, setup_test_env):
        """The wrappers only inject the settings prefix — no second name implementation."""
        from apps.core import constants
        from apps.core.settings import settings
        from apps.modules.kafka import topic_lifecycle

        assert topic_lifecycle.tagged_events_topic_for(PIPELINE_ID) == constants.tagged_events_topic(
            settings.kafka_tagged_events_topic, PIPELINE_ID
        )
        assert topic_lifecycle.chains_config_topic_for(CORRELATION_ID) == constants.chains_config_topic(
            settings.kafka_chains_config_topic, CORRELATION_ID
        )
        assert topic_lifecycle.chains_raw_topic_for(CORRELATION_ID) == constants.chains_raw_topic(
            settings.kafka_chains_raw_topic, CORRELATION_ID
        )
        assert topic_lifecycle.chains_analysed_topic_for(CORRELATION_ID) == constants.chains_analysed_topic(
            settings.kafka_chains_analysed_topic, CORRELATION_ID
        )

    def test_shared_topic_name_is_refused(self, setup_test_env):
        from apps.core.settings import settings
        from apps.modules.kafka.topic_lifecycle import TopicProvisioningError, validate_topic_name

        with pytest.raises(TopicProvisioningError):
            validate_topic_name(settings.kafka_active_threats_topic)


class TestEnsure:
    """`ensure_*` is idempotent and fails fast."""

    async def test_creates_missing_topics_with_the_declared_spec(self, service):
        from apps.core.settings import settings

        admin = _wire(service, FakeAdminClient(existing={}))

        names = await service.ensure_correlation_pipeline_topics(CORRELATION_ID)

        assert names == [
            f"{settings.kafka_chains_config_topic}.{CORRELATION_ID}",
            f"{settings.kafka_chains_raw_topic}.{CORRELATION_ID}",
            f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}",
        ]
        assert [t.topic for t in admin.created] == names
        by_name = {t.topic: t for t in admin.created}
        config_topic = by_name[names[0]]
        assert config_topic.num_partitions == 1
        assert config_topic.config == {"cleanup.policy": "compact", "retention.ms": "-1"}
        raw_topic = by_name[names[1]]
        assert raw_topic.num_partitions == settings.kafka_correlation_chains_partitions
        assert raw_topic.config == {"cleanup.policy": "delete", "retention.ms": "604800000"}

    async def test_existing_topic_is_a_no_op(self, service):
        from apps.core.settings import settings

        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        admin = _wire(service, FakeAdminClient(existing={topic: 6}))

        assert await service.ensure_pipeline_topics(PIPELINE_ID) == topic
        assert admin.created == []

    async def test_topic_already_exists_is_success(self, service):
        from apps.core.settings import settings

        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        admin = _wire(service, FakeAdminClient(create_errors={topic: _kafka_exception("TOPIC_ALREADY_EXISTS")}))

        assert await service.ensure_pipeline_topics(PIPELINE_ID) == topic
        assert [t.topic for t in admin.created] == [topic]

    async def test_other_kafka_errors_become_topic_provisioning_error(self, service):
        from apps.core.settings import settings
        from apps.modules.kafka.topic_lifecycle import TopicProvisioningError

        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        _wire(service, FakeAdminClient(create_errors={topic: _kafka_exception("POLICY_VIOLATION")}))

        with pytest.raises(TopicProvisioningError):
            await service.ensure_pipeline_topics(PIPELINE_ID)

    async def test_reconcile_provisions_every_row_in_one_pass(self, service):
        admin = _wire(service, FakeAdminClient(existing={}))

        created = await service.reconcile([PIPELINE_ID], [CORRELATION_ID])

        assert len(created) == 4
        assert len(admin.created) == 4  # one create_topics call, four NewTopics


class TestRetire:
    """The policy matrix, and every guard that stands between a delete and customer data."""

    async def test_refuses_shared_topics(self, service):
        from apps.core.settings import settings
        from apps.modules.kafka.topic_lifecycle import TopicProvisioningError

        for shared in (
            settings.kafka_active_threats_topic,
            settings.kafka_hos_rules_topic,
            settings.kafka_activity_topic,
            settings.kafka_chains_raw_topic,
        ):
            with pytest.raises(TopicProvisioningError):
                service._assert_retirable(shared)

    async def test_keep_issues_no_admin_call(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "keep")
        topic = f"{settings.kafka_tagged_events_topic}.{PIPELINE_ID}"
        admin = _wire(service, FakeAdminClient(existing={topic: 6}))

        outcomes = await service.retire_pipeline_topics(PIPELINE_ID)

        assert [o.action for o in outcomes] == ["kept"]
        assert admin.altered == [] and admin.deleted == []

    async def test_retire_sets_both_cleanup_policy_and_retention(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "retire")
        topics = {
            f"{settings.kafka_chains_config_topic}.{CORRELATION_ID}": 1,
            f"{settings.kafka_chains_raw_topic}.{CORRELATION_ID}": 3,
            f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}": 3,
        }
        admin = _wire(service, FakeAdminClient(existing=topics))

        outcomes = await service.retire_correlation_pipeline_topics(CORRELATION_ID)

        assert [o.action for o in outcomes] == ["retired", "retired", "retired"]
        assert admin.deleted == []
        assert len(admin.altered) == 3
        for resource in admin.altered:
            applied = {entry.name: entry.value for entry in resource.incremental_configs}
            assert applied == {
                "cleanup.policy": "delete",  # compaction must go, or retention purges nothing
                "retention.ms": str(settings.kafka_retired_topic_retention_ms),
            }

    async def test_delete_removes_the_topics_when_nothing_consumes_them(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        topics = {
            f"{settings.kafka_chains_config_topic}.{CORRELATION_ID}": 1,
            f"{settings.kafka_chains_raw_topic}.{CORRELATION_ID}": 3,
            f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}": 3,
        }
        admin = _wire(service, FakeAdminClient(existing=topics))

        outcomes = await service.retire_correlation_pipeline_topics(CORRELATION_ID)

        assert [o.action for o in outcomes] == ["deleted", "deleted", "deleted"]
        assert sorted(admin.deleted) == sorted(topics)
        assert admin.altered == []

    async def test_missing_topic_is_reported_not_created(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        admin = _wire(service, FakeAdminClient(existing={}))

        outcomes = await service.retire_pipeline_topics(PIPELINE_ID)

        assert [(o.action, o.reason) for o in outcomes] == [("kept", "topic does not exist")]
        assert admin.deleted == [] and admin.altered == []

    async def test_live_consumer_group_downgrades_delete_to_retire(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        topic = f"{settings.kafka_chains_raw_topic}.{CORRELATION_ID}"
        admin = _wire(
            service,
            FakeAdminClient(
                existing={topic: 3},
                groups=[_group("agent-job-chains-raw", ConsumerGroupState.STABLE)],
                committed={"agent-job-chains-raw": {0: 100}},
                committed_topic=topic,
                end_offsets={0: 100},
            ),
        )

        outcomes = await service._retire([_spec_for(topic, 3)])

        assert outcomes[0].action == "retired"
        assert "agent-job-chains-raw" in outcomes[0].reason
        assert admin.deleted == []
        assert len(admin.altered) == 1

    async def test_unconsumed_lag_downgrades_delete_to_retire(self, service, monkeypatch):
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        topic = f"{settings.kafka_chains_raw_topic}.{CORRELATION_ID}"
        admin = _wire(
            service,
            FakeAdminClient(
                existing={topic: 3},
                groups=[_group("agent-job-chains-raw", ConsumerGroupState.EMPTY)],
                committed={"agent-job-chains-raw": {0: 40}},
                committed_topic=topic,
                end_offsets={0: 100},
            ),
        )

        outcomes = await service._retire([_spec_for(topic, 3)])

        assert outcomes[0].action == "retired"
        assert "60 unconsumed record(s)" in outcomes[0].reason
        assert admin.deleted == []

    async def test_chains_analysed_waits_for_the_drain_then_downgrades(self, service, monkeypatch):
        """The control plane's own group is judged by lag, and lag is waited on — then retired."""
        from apps.core.settings import settings
        from apps.modules.kafka import topic_lifecycle

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        monkeypatch.setattr(settings, "kafka_topic_delete_drain_timeout_seconds", 5)
        topic = f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}"
        admin = _wire(
            service,
            FakeAdminClient(
                existing={topic: 3},
                groups=[_group(settings.kafka_chains_analysed_consumer_group, ConsumerGroupState.STABLE)],
                committed={settings.kafka_chains_analysed_consumer_group: {0: 10}},
                committed_topic=topic,
                end_offsets={0: 25},
            ),
        )
        ticks = iter(range(0, 100, 3))  # 3s per look — the 5s budget runs out on the third
        monkeypatch.setattr(topic_lifecycle.time, "monotonic", lambda: float(next(ticks)))
        monkeypatch.setattr(topic_lifecycle.time, "sleep", lambda _seconds: None)

        outcomes = await service._retire([_spec_for(topic, 3)])

        assert outcomes[0].action == "retired"
        assert "15 unconsumed record(s)" in outcomes[0].reason
        assert admin.deleted == []
        assert admin.offset_polls > 1  # it polled rather than refusing on the first look

    async def test_chains_analysed_is_deleted_once_drained(self, service, monkeypatch):
        """A caught-up control plane does not block: liveness alone never blocks its own group."""
        from apps.core.settings import settings

        monkeypatch.setattr(settings, "kafka_topic_delete_policy", "delete")
        topic = f"{settings.kafka_chains_analysed_topic}.{CORRELATION_ID}"
        admin = _wire(
            service,
            FakeAdminClient(
                existing={topic: 3},
                groups=[_group(settings.kafka_chains_analysed_consumer_group, ConsumerGroupState.STABLE)],
                committed={settings.kafka_chains_analysed_consumer_group: {0: 25}},
                committed_topic=topic,
                end_offsets={0: 25},
            ),
        )

        outcomes = await service._retire([_spec_for(topic, 3)])

        assert outcomes[0].action == "deleted"
        assert admin.deleted == [topic]


def _spec_for(name: str, partitions: int):
    from apps.modules.kafka.topic_lifecycle import TopicSpec

    return TopicSpec(name=name, partitions=partitions, cleanup_policy="delete", retention_ms=604_800_000)
