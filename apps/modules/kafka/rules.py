import asyncio
import json
import re
from concurrent.futures import ThreadPoolExecutor

import yaml
from confluent_kafka import Producer

from apps.clients.tdm_api import TdmRule
from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import Rule
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient

logger = get_logger(__name__)


class KafkaRulesSyncService(BaseKafkaSyncClient):
    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-sync")

    def __init__(self):
        super().__init__(client_id="admin-panel-backend")
        self.rules_topic = settings.kafka_sigma_rules_topic

        # Add producer-specific config
        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",  # Wait for all replicas
                "compression.type": "gzip",
            }
        )
        self.producer = Producer(self.producer_config)

    def _get_default_client_id(self) -> str:
        """Return default client ID for rules sync service."""
        return "admin-panel-backend"

    async def send_rules(
        self,
        pipeline_id: str,
        rules: list[Rule | TdmRule],
        batch_size: int = 100,
    ) -> None:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._send_rules_sync, pipeline_id, rules, batch_size
        )

    async def delete_rules(self, pipeline_id: str, rule_ids: list[str]) -> bool:
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self._delete_rules_sync, pipeline_id, rule_ids
        )

    def _send_rules_sync(
        self,
        pipeline_id: str,
        rules: list[Rule | TdmRule],
        batch_size: int = 100,
    ) -> None:
        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver message: {err}")
                # Track error for audit logging (will be logged async later)
                if ErrorTracker.should_log(f"kafka_rules_deliver_{pipeline_id}"):
                    # Note: Cannot await in sync callback, error tracked via ErrorTracker
                    logger.error(
                        "Rule delivery failure tracked for audit",
                        extra={"pipeline_id": pipeline_id, "error": str(err)},
                    )
                raise RuntimeError(f"Failed to deliver message: {err}")
            else:
                logger.debug(f"Rule delivered to Kafka: key={msg.key()}")

        # Filter out unsupported rules 
        # Only Rule model has is_supported attribute, TdmRule always passes through
        supported_rules = []
        skipped_count = 0
        for rule in rules:
            if isinstance(rule, Rule) and hasattr(rule, "is_supported") and not rule.is_supported:
                logger.debug(f"Skipping unsupported rule {rule.id}: {rule.unsupported_reason}")
                skipped_count += 1
            else:
                supported_rules.append(rule)

        if skipped_count > 0:
            logger.info(f"Filtered out {skipped_count} unsupported rules for pipeline {pipeline_id}")

        logger.info(f"Sending {len(supported_rules)} rules to Kafka for pipeline {pipeline_id}")

        for idx, rule in enumerate(supported_rules, start=1):
            key_bytes, value_bytes = self._prepare_message(pipeline_id, rule)

            logger.debug(f"Producing rule {rule.id} to topic {self.rules_topic}, key={key_bytes}")

            self.producer.produce(
                topic=self.rules_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

            if idx % batch_size == 0:
                self.producer.flush(timeout=10)

        self.producer.flush(timeout=10)
        logger.info(f"Successfully sent {len(supported_rules)} rules to Kafka for pipeline {pipeline_id}")

    def _delete_rules_sync(self, pipeline_id: str, rule_ids: list[str]) -> None:
        if not rule_ids:
            logger.info(f"No rules to delete for pipeline {pipeline_id}")
            return

        logger.info(f"Deleting {len(rule_ids)} rules from Kafka for pipeline {pipeline_id}")

        def on_delivery(err, msg):
            if err:
                logger.error(f"Failed to deliver delete message: {err}")
                # Track error for audit logging
                if ErrorTracker.should_log(f"kafka_rules_delete_{pipeline_id}"):
                    logger.error(
                        "Rule delete delivery failure tracked for audit",
                        extra={"pipeline_id": pipeline_id, "error": str(err)},
                    )
                raise RuntimeError(f"Failed to deliver message: {err}")
            else:
                logger.debug(f"Delete message delivered: key={msg.key()}")

        # Phase 1: Send logical delete messages (empty sigma.text)
        for rule_id in rule_ids:
            key_bytes, value_bytes = self._prepare_logical_delete_message(pipeline_id, rule_id)
            self.producer.produce(
                topic=self.rules_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

        self.producer.flush(timeout=10)
        logger.info(f"Sent {len(rule_ids)} logical delete messages for pipeline {pipeline_id}")

        # Phase 2: Send tombstones for Kafka log compaction
        for rule_id in rule_ids:
            key_bytes, value_bytes = self._prepare_tombstone_message(pipeline_id, rule_id)
            self.producer.produce(
                topic=self.rules_topic,
                key=key_bytes,
                value=value_bytes,
                callback=on_delivery,
            )

        self.producer.flush(timeout=10)
        logger.info(f"Sent {len(rule_ids)} tombstone messages for pipeline {pipeline_id}")

    def _prepare_message(self, pipeline_id: str, rule: Rule | TdmRule) -> tuple[bytes, bytes]:
        # Get level and tags - try TDM API data first, fallback to YAML parsing
        if isinstance(rule, TdmRule):
            # TdmRule - use TDM API data if available
            level = rule.level if rule.level else None
            tags = rule.tags if rule.tags and rule.tags.get("technique") else None

            # Fallback to YAML parsing if TDM API didn't provide complete data
            if not level or not tags:
                parsed_level, parsed_tags = self._parse_sigma_metadata(rule.body)
                level = level or parsed_level
                tags = tags or parsed_tags
        else:
            # Rule model - parse level and tags from YAML body
            level, tags = self._parse_sigma_metadata(rule.body)

        kafka_rule = {
            "job_id": pipeline_id,
            "case": {"id": str(rule.id), "name": rule.name},
            "sigma": {"text": rule.body, "level": level},
            "tags": tags,
        }

        case_id = str(rule.id)
        key_bytes = self._get_composite_key(pipeline_id, case_id)
        value_bytes = json.dumps(kafka_rule, ensure_ascii=False, default=str).encode("utf-8")

        return key_bytes, value_bytes

    def _parse_sigma_metadata(self, body: str) -> tuple[str, dict]:
        """Parse level and tags from sigma YAML body for Rule model."""
        try:
            parsed = yaml.safe_load(body)
            if not isinstance(parsed, dict):
                return "medium", {"technique": []}

            level = parsed.get("level", "medium")
            yaml_tags = parsed.get("tags", [])

            # Convert MITRE ATT&CK technique tags to Flink format
            # e.g., "attack.t1216" -> {"id": "T1216"}
            techniques = []
            technique_pattern = re.compile(r"^attack\.t(\d+(?:\.\d+)?)$", re.IGNORECASE)

            for tag in yaml_tags:
                if isinstance(tag, str):
                    match = technique_pattern.match(tag)
                    if match:
                        technique_id = f"T{match.group(1).upper()}"
                        techniques.append({"id": technique_id})

            return level, {"technique": techniques}
        except Exception as e:
            logger.warning(f"Failed to parse sigma YAML metadata: {e}")
            return "medium", {"technique": []}

    def _prepare_logical_delete_message(self, pipeline_id: str, rule_id: str) -> tuple[bytes, bytes]:
        logical_delete_message = {
            "job_id": pipeline_id,
            "case": {"id": rule_id},
            "sigma": {"text": ""},
        }
        key_bytes = self._get_composite_key(pipeline_id, rule_id)
        value_bytes = json.dumps(logical_delete_message, ensure_ascii=False, default=str).encode("utf-8")
        return key_bytes, value_bytes

    def _prepare_tombstone_message(self, pipeline_id: str, rule_id: str) -> tuple[bytes, None]:
        key_bytes = self._get_composite_key(pipeline_id, rule_id)
        return key_bytes, None

    def _get_composite_key(self, pipeline_id: str, case_id: str) -> bytes:
        composite_key = f"{pipeline_id}:{case_id}"
        return composite_key.encode("utf-8")
