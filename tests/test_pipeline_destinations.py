"""Tests for multi-destination pipeline output (match-node contract)."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader

from apps.core.schemas import (
    MAX_OUTPUT_DESTINATIONS,
    OUTPUT_MODE_ACCEPTS,
    PipelineCreateRequest,
    PipelineUpdateRequest,
    destination_accepts,
    destination_topics_from_pipeline,
    get_reserved_topics,
    pipeline_destinations,
)
from apps.core.settings import settings
from apps.managers import dashboard as dashboard_module
from apps.managers.dashboard import DashboardService, DashboardTopology
from apps.managers.pipeline_orchestrator import FLINK_RESTART_FIELDS, detect_pipeline_changes
from apps.modules.kafka.chains import KafkaChainsConfigProducer
from apps.providers.base import FlinkDeploymentConfig
from apps.providers.matcher_runtime_tuning import resolve_matcher_runtime


class TestPipelineDestinationsSchema:
    def test_create_with_destinations(self):
        request = PipelineCreateRequest(
            name="multi",
            source_topics=["windows-events"],
            destinations=[
                {"topic": "windows-detections", "output_mode": "matched_only"},
                {
                    "topic": "windows-detections-all",
                    "output_mode": "all_events",
                    "apply_parser_to_output_events": True,
                },
            ],
        )
        assert len(request.destinations) == 2
        assert request.destination_topic == "windows-detections"
        assert request.save_untagged is False
        assert request.apply_parser_to_output_events is False

    def test_create_legacy_shim_synthesizes_destinations(self):
        request = PipelineCreateRequest(
            name="legacy",
            source_topics=["input-logs"],
            destination_topic="output-logs",
            save_untagged=True,
            apply_parser_to_output_events=True,
        )
        assert len(request.destinations) == 1
        assert request.destinations[0].topic == "output-logs"
        assert request.destinations[0].output_mode == "all_events"
        assert request.destinations[0].apply_parser_to_output_events is True

    def test_duplicate_destination_topics_fail(self):
        with pytest.raises(ValueError, match="Duplicate destination topic"):
            PipelineCreateRequest(
                name="dup",
                source_topics=["in"],
                destinations=[
                    {"topic": "same"},
                    {"topic": "same"},
                ],
            )

    def test_destination_in_sources_fails(self):
        with pytest.raises(ValueError, match="cannot be the same as a source topic"):
            PipelineCreateRequest(
                name="loop",
                source_topics=["shared"],
                destinations=[{"topic": "shared"}],
            )

    def test_too_many_destinations_fail(self):
        with pytest.raises(ValueError):
            PipelineCreateRequest(
                name="too-many",
                source_topics=["in"],
                destinations=[{"topic": f"out-{i}"} for i in range(MAX_OUTPUT_DESTINATIONS + 1)],
            )

    def test_reserved_destination_fails(self):
        reserved = next(iter(get_reserved_topics()))
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="reserved",
                source_topics=["in"],
                destinations=[{"topic": reserved}],
            )

    def test_tagged_events_system_topic_rejected_in_any_position(self):
        """``detectflow.tagged-events`` is the shared attack-chains tech topic.

        The match node writes it from its own side output; a pipeline destination pointed at
        it would interleave raw pipeline output with tagged-event envelopes and break the
        correlation-job contract. Reserved in EVERY destination slot, not just the first.
        """
        with pytest.raises(ValueError, match="reserved system topic"):
            PipelineCreateRequest(
                name="tagged",
                source_topics=["in"],
                destinations=[{"topic": "out"}, {"topic": settings.kafka_tagged_events_topic}],
            )

    def test_create_requires_destinations_or_legacy(self):
        with pytest.raises(ValueError, match="Either destinations or destination_topic"):
            PipelineCreateRequest(name="missing", source_topics=["in"])

    def test_update_keeps_fields_as_sent(self):
        """The update schema validates only — the merge with stored state is the orchestrator's."""
        request = PipelineUpdateRequest(
            destinations=[
                {"topic": "a", "output_mode": "all_events", "apply_parser_to_output_events": True},
                {"topic": "b"},
            ]
        )
        assert [d.topic for d in request.destinations] == ["a", "b"]
        assert request.destination_topic is None
        assert request.save_untagged is None
        assert request.apply_parser_to_output_events is None

    def test_update_rejects_invalid_destinations(self):
        with pytest.raises(ValueError, match="Duplicate destination topic"):
            PipelineUpdateRequest(destinations=[{"topic": "same"}, {"topic": "same"}])

    def test_unmatched_only_destination_accepted(self):
        request = PipelineCreateRequest(
            name="noise-router",
            source_topics=["raw"],
            destinations=[{"topic": "noise", "output_mode": "unmatched_only"}],
        )
        assert request.destinations[0].output_mode == "unmatched_only"
        # Legacy mirror: unmatched events DO reach this topic, so save_untagged is true
        assert request.save_untagged is True
        assert request.destination_topic == "noise"

    def test_unknown_output_mode_rejected(self):
        with pytest.raises(ValueError):
            PipelineCreateRequest(
                name="bad",
                source_topics=["raw"],
                destinations=[{"topic": "out", "output_mode": "everything_please"}],
            )

    def test_severities_default_empty_and_canonicalized(self):
        request = PipelineCreateRequest(
            name="sev",
            source_topics=["raw"],
            destinations=[
                {"topic": "hot", "severities": ["critical", "high", "critical"]},
                {"topic": "all"},
            ],
        )
        assert request.destinations[0].severities == ["high", "critical"]
        assert request.destinations[1].severities == []

    def test_severities_cleared_outside_matched_only(self):
        request = PipelineCreateRequest(
            name="sev",
            source_topics=["raw"],
            destinations=[
                {"topic": "all", "output_mode": "all_events", "severities": ["high"]},
                {"topic": "noise", "output_mode": "unmatched_only", "severities": ["high"]},
            ],
        )
        assert [d.severities for d in request.destinations] == [[], []]

    def test_unknown_severity_rejected(self):
        with pytest.raises(ValueError):
            PipelineCreateRequest(
                name="bad",
                source_topics=["raw"],
                destinations=[{"topic": "out", "severities": ["urgent"]}],
            )

    def test_output_mode_accepts_matrix(self):
        """``unmatched_only`` is ``all_events`` minus the matches — pin the whole table.

        Must stay identical to ``OUTPUT_MODE_ACCEPTS`` in the match node, which is what
        actually routes events (detectflow-matchnode ``app/config/settings.py``).
        """
        assert [(destination_accepts(m, True), destination_accepts(m, False)) for m in OUTPUT_MODE_ACCEPTS] == [
            (True, True),  # all_events
            (True, False),  # matched_only
            (False, True),  # unmatched_only
        ]


class TestDetectPipelineChangesDestinations:
    def _existing(self, **overrides):
        base = {
            "name": "p",
            "source_topics": ["in"],
            "destination_topic": "out",
            "save_untagged": False,
            "apply_parser_to_output_events": False,
            "destinations": [
                {
                    "topic": "out",
                    "output_mode": "matched_only",
                    "apply_parser_to_output_events": False,
                }
            ],
            "filters": [],
            "log_source_id": uuid4(),
            "custom_fields": None,
            "enabled": True,
        }
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_destinations_change_triggers_restart(self):
        existing = self._existing()
        request = PipelineUpdateRequest(
            destinations=[
                {"topic": "out"},
                {"topic": "out-all", "output_mode": "all_events"},
            ]
        )
        changes = detect_pipeline_changes(request, existing)
        assert changes.flink_restart_needed
        assert "destinations" in changes.update_data
        assert changes.update_data["destination_topic"] == "out"
        assert "destinations" in FLINK_RESTART_FIELDS

    def _existing_multi(self):
        return self._existing(
            destinations=[
                {"topic": "out", "output_mode": "matched_only", "apply_parser_to_output_events": False},
                {"topic": "out2", "output_mode": "matched_only", "apply_parser_to_output_events": False},
            ]
        )

    def test_legacy_save_untagged_updates_first_destination(self):
        request = PipelineUpdateRequest(save_untagged=True)
        changes = detect_pipeline_changes(request, self._existing_multi())
        assert changes.flink_restart_needed
        assert changes.update_data["destinations"][0]["output_mode"] == "all_events"
        assert changes.update_data["destinations"][1]["topic"] == "out2"
        assert changes.update_data["save_untagged"] is True

    def test_legacy_destination_topic_keeps_extra_destinations(self):
        """A legacy client renaming the primary topic must not drop destinations it can't see."""
        request = PipelineUpdateRequest(destination_topic="out-renamed")
        changes = detect_pipeline_changes(request, self._existing_multi())
        assert [d["topic"] for d in changes.update_data["destinations"]] == ["out-renamed", "out2"]
        assert changes.update_data["destination_topic"] == "out-renamed"

    def test_unchanged_destinations_do_not_trigger_restart(self):
        request = PipelineUpdateRequest(name="renamed")
        changes = detect_pipeline_changes(request, self._existing_multi())
        assert "destinations" not in changes.update_data
        assert not changes.flink_restart_needed

    def test_resubmitting_identical_destinations_is_a_no_op(self):
        existing = self._existing_multi()
        request = PipelineUpdateRequest(destinations=existing.destinations)
        changes = detect_pipeline_changes(request, existing)
        assert "destinations" not in changes.update_data

    def test_severities_change_triggers_restart(self):
        existing = self._existing_multi()
        request = PipelineUpdateRequest(destinations=[{"topic": "out", "severities": ["high"]}, {"topic": "out2"}])
        changes = detect_pipeline_changes(request, existing)
        assert changes.flink_restart_needed
        assert changes.update_data["destinations"][0]["severities"] == ["high"]

    def test_same_severities_in_other_order_is_a_no_op(self):
        existing = self._existing(
            destinations=[{"topic": "out", "output_mode": "matched_only", "severities": ["high", "low"]}]
        )
        request = PipelineUpdateRequest(destinations=[{"topic": "out", "severities": ["high", "low", "high"]}])
        changes = detect_pipeline_changes(request, existing)
        assert "destinations" not in changes.update_data

    def test_new_source_topic_colliding_with_untouched_destination_fails(self):
        request = PipelineUpdateRequest(source_topics=["out2"])
        with pytest.raises(ValueError, match="cannot be the same as a source topic"):
            detect_pipeline_changes(request, self._existing_multi())


class TestAttackChainsMonitoredTopics:
    """Destination topics remain SIEM output; they are not correlation identity.

    correlation-job isolates by membership → dedicated tagged-events topics. ``chains-config``
    does not carry destination names. Auto-add globs the detection pipeline's **name**, not its
    destinations (control-plane only — see ``test_correlation_pipeline_republish.py``).
    """

    async def test_chains_config_does_not_carry_destination_topics(self):
        """The matcher does not filter by destination names."""
        row = SimpleNamespace(
            threshold_pct=70,
            correlation_window_seconds=3600,
            monitored_topics=["win-all-parsed"],
            updated_at=None,
            auto_add_new_pipelines=False,
        )
        envelope = KafkaChainsConfigProducer.build_singleton_envelope(
            KafkaChainsConfigProducer.__new__(KafkaChainsConfigProducer), row
        )
        assert "monitored_topics" not in envelope
        assert "auto_add_new_pipelines" not in envelope


class TestDashboardDestinationEps:
    """Per-destination EPS attribution: the Flink gauges are per-pipeline, not per-sink."""

    def _graph(self, destinations, tagged_eps=4.0, untagged_eps=6.0, input_eps=10.0):
        service = DashboardService()
        pipeline_id = uuid4()
        topology = DashboardTopology(
            source_topics=["in"],
            dest_topics=sorted(d["topic"] for d in destinations),
            repositories=[],
            total_rules=0,
            pipelines_count=1,
            enabled_pipelines=[{"id": pipeline_id, "source_topics": ["in"], "destinations": destinations}],
            pipelines_data=[],
            fetched_at=0.0,
        )
        entry = SimpleNamespace(input_eps=input_eps, tagged_eps=tagged_eps, untagged_eps=untagged_eps, metrics=None)
        with patch.object(dashboard_module.flink_metrics_poller, "get_cache_entry", return_value=entry):
            graph = service._assemble_graph_data(topology)
        return {d.name: d for d in graph.destination_topics}, graph

    def _all_three_modes(self):
        return self._graph(
            [
                {"topic": "matched", "output_mode": "matched_only"},
                {"topic": "everything", "output_mode": "all_events"},
                {"topic": "noise", "output_mode": "unmatched_only"},
            ]
        )

    def test_tagged_eps_skips_unmatched_only_destinations(self):
        by_topic, graph = self._all_three_modes()
        assert by_topic["matched"].tagged_eps == 4.0
        assert by_topic["everything"].tagged_eps == 4.0
        assert by_topic["noise"].tagged_eps == 0.0  # receives no detections
        # Totals stay per-pipeline: a fan-out does not inflate global throughput
        assert graph.total_tagged_eps == 4.0
        assert graph.total_untagged_eps == 6.0

    def test_untagged_eps_skips_matched_only_destinations(self):
        by_topic, _ = self._all_three_modes()
        assert by_topic["matched"].untagged_eps == 0.0
        assert by_topic["everything"].untagged_eps == 6.0
        assert by_topic["noise"].untagged_eps == 6.0  # all_events minus the matches


class TestDeploymentConfigDestinations:
    def test_kafka_output_destinations_json_compact(self):
        destinations = [
            {
                "topic": "detections-matched",
                "output_mode": "matched_only",
                "apply_parser_to_output_events": False,
            },
            {
                "topic": "detections-all-parsed",
                "output_mode": "all_events",
                "apply_parser_to_output_events": True,
                "severities": ["high", "critical"],
            },
        ]
        config = FlinkDeploymentConfig(
            pipeline_id=str(uuid4()),
            pipeline_name="test",
            input_topics=["in"],
            output_topic="detections-matched",
            output_destinations=destinations,
            output_mode="matched_only",
            apply_parser_to_output_events=False,
        )
        payload = config.kafka_output_destinations_json()
        assert " " not in payload
        assert json.loads(payload) == destinations

    def test_flink_template_injects_kafka_output_destinations(self):
        templates_dir = Path(__file__).resolve().parents[1] / "apps" / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)))
        template = env.get_template("flink-deployment.yaml.j2")
        destinations = [
            {"topic": "dest_topic_test", "output_mode": "matched_only", "apply_parser_to_output_events": True}
        ]
        destinations_json = json.dumps(destinations, separators=(",", ":"))
        rendered = template.render(
            deployment_name="pipeline-test",
            namespace="security",
            pipeline_id="abc",
            job_id="abc",
            input_topics=["in"],
            output_topic="dest_topic_test",
            kafka_output_destinations=destinations_json,
            output_mode="matched_only",
            apply_parser_to_output_events=True,
            parallelism=1,
            flink_image="flink:latest",
            image_pull_policy="IfNotPresent",
            taskmanager_memory="2048m",
            taskmanager_process_memory="1536m",
            taskmanager_memory_limit_factor="1.5",
            taskmanager_cpu=1.0,
            runtime=resolve_matcher_runtime(),
            kafka_starting_offset="latest",
            window_size_sec=30,
            checkpoint_interval_sec=60,
            autoscaler_enabled=False,
            autoscaler_min_parallelism=1,
            autoscaler_max_parallelism=24,
            rules_topic="detectflow.sigma-rules",
            metrics_topic="detectflow.rule-metrics",
            tagged_events_topic="detectflow.tagged-events",
            jobmanager_memory="1024m",
            jobmanager_cpu=0.5,
            taskmanager_replicas=1,
            autoscaler_quota_cpu=1,
            autoscaler_quota_memory="2Gi",
            kafka_auth_method="PLAINTEXT",
            node_selector_key="node",
            node_selector_value="flink",
            checkpoints_pvc="flink-checkpoints-pvc",
            ha_pvc="flink-ha-pvc",
            savepoints_pvc="flink-savepoints-pvc",
        )
        manifest = yaml.safe_load(rendered)
        jm_env = {
            e["name"]: e["value"] for e in manifest["spec"]["jobManager"]["podTemplate"]["spec"]["containers"][0]["env"]
        }
        assert jm_env["KAFKA_OUTPUT_DESTINATIONS"] == destinations_json
        assert json.loads(jm_env["KAFKA_OUTPUT_DESTINATIONS"]) == destinations
        assert jm_env["KAFKA_OUTPUT_TOPIC"] == "dest_topic_test"  # legacy shim retained

    def test_cmf_template_injects_kafka_output_destinations(self):
        templates_dir = Path(__file__).resolve().parents[1] / "apps" / "templates"
        env = Environment(loader=FileSystemLoader(str(templates_dir)))
        template = env.get_template("cmf-application.json.j2")
        destinations_json = json.dumps(
            [{"topic": "out", "output_mode": "matched_only", "apply_parser_to_output_events": False}],
            separators=(",", ":"),
        )
        rendered = template.render(
            app_name="pipeline-test",
            pipeline_id="abc",
            job_id="abc",
            input_topics=["in"],
            output_topic="out",
            kafka_output_destinations=destinations_json,
            output_mode="matched_only",
            apply_parser_to_output_events=False,
            parallelism=1,
            flink_image="flink:latest",
            image_pull_policy="IfNotPresent",
            taskmanager_memory="1536m",
            taskmanager_memory_limit_factor="1.5",
            taskmanager_cpu=1.0,
            runtime=resolve_matcher_runtime(),
            kafka_starting_offset="latest",
            window_size_sec=30,
            checkpoint_interval_sec=60,
            rules_topic="detectflow.sigma-rules",
            metrics_topic="detectflow.rule-metrics",
            tagged_events_topic="detectflow.tagged-events",
            jobmanager_memory="1024m",
            jobmanager_cpu=0.5,
            kafka_bootstrap_servers="kafka:9092",
            kafka_auth_method="PLAINTEXT",
        )
        parsed = json.loads(rendered)
        env_vars = {e["name"]: e["value"] for e in parsed["spec"]["podTemplate"]["spec"]["containers"][0]["env"]}
        assert env_vars["KAFKA_OUTPUT_DESTINATIONS"] == destinations_json


class TestReadingPipelineDestinations:
    def test_empty_destinations_synthesizes_from_legacy(self):
        pipeline = SimpleNamespace(
            destinations=[],
            destination_topic="legacy-out",
            save_untagged=True,
            apply_parser_to_output_events=False,
        )
        assert pipeline_destinations(pipeline) == [
            {
                "topic": "legacy-out",
                "output_mode": "all_events",
                "apply_parser_to_output_events": False,
            }
        ]

    def test_destination_topics_order_preserved(self):
        pipeline = SimpleNamespace(
            destinations=[{"topic": "b"}, {"topic": "a"}],
            destination_topic="b",
        )
        assert destination_topics_from_pipeline(pipeline) == ["b", "a"]

    def test_destination_topics_falls_back_to_legacy_column(self):
        """Rows selected without the JSONB column (or written pre-migration) still resolve."""
        pipeline = SimpleNamespace(destinations=None, destination_topic="legacy-out")
        assert destination_topics_from_pipeline(pipeline) == ["legacy-out"]
