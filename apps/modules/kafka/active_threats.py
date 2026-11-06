import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import yaml
from confluent_kafka import Producer

from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.models import ActiveThreat, HighOrderSigma
from apps.core.settings import settings
from apps.modules.kafka.base import BaseKafkaSyncClient
from apps.services.mitre_attack import mitre_attack

logger = get_logger(__name__)


class KafkaActiveThreatsSyncService(BaseKafkaSyncClient):
    _executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="kafka-active-threats-sync")

    def __init__(self):
        super().__init__(client_id="admin-panel-backend-active-threats")
        self.active_threats_topic = settings.kafka_active_threats_topic
        self.hos_topic = settings.kafka_hos_rules_topic
        self.producer_config = self._config.copy()
        self.producer_config.update(
            {
                "acks": "all",
                "compression.type": "gzip",
            }
        )
        self.producer = Producer(self.producer_config)

    def _get_default_client_id(self) -> str:
        return "admin-panel-backend-active-threats"

    async def send_active_threats(self, active_threats: list[ActiveThreat]) -> None:
        await asyncio.get_running_loop().run_in_executor(self._executor, self._send_active_threats_sync, active_threats)

    async def delete_active_threats(self, threat_ids: list[str]) -> None:
        await asyncio.get_running_loop().run_in_executor(self._executor, self._delete_active_threats_sync, threat_ids)

    async def send_hos(
        self,
        hos_rows: list[HighOrderSigma],
        active_threat_ids_by_hos_tdm_id: dict[str, list[str]],
    ) -> None:
        await asyncio.get_running_loop().run_in_executor(
            self._executor,
            self._send_hos_sync,
            hos_rows,
            active_threat_ids_by_hos_tdm_id,
        )

    async def delete_hos(self, hos_tdm_ids: list[str]) -> None:
        await asyncio.get_running_loop().run_in_executor(self._executor, self._delete_hos_sync, hos_tdm_ids)

    def _send_active_threats_sync(self, active_threats: list[ActiveThreat]) -> None:
        if not active_threats:
            return
        logger.info("Sending %s active threats to Kafka", len(active_threats))
        for threat in active_threats:
            key_bytes = self._encode_key(threat.tdm_id)
            payload = self._build_active_threat_payload(threat)
            value_bytes = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.producer.produce(
                topic=self.active_threats_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("active_threats", threat.tdm_id),
            )
        self.producer.flush(timeout=10)

    def _delete_active_threats_sync(self, threat_ids: list[str]) -> None:
        if not threat_ids:
            return
        logger.info("Deleting %s active threats from Kafka", len(threat_ids))

        # Phase 1: send logical delete payloads for clients that do not consume tombstones.
        for threat_id in threat_ids:
            key_bytes, value_bytes = self._prepare_active_threat_logical_delete_message(threat_id)
            self.producer.produce(
                topic=self.active_threats_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("active_threats", threat_id),
            )
        self.producer.flush(timeout=10)

        # Phase 2: send tombstones for Kafka compaction.
        for threat_id in threat_ids:
            key_bytes, value_bytes = self._prepare_tombstone_message(threat_id)
            self.producer.produce(
                topic=self.active_threats_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("active_threats", threat_id),
            )
        self.producer.flush(timeout=10)

    def _send_hos_sync(
        self,
        hos_rows: list[HighOrderSigma],
        active_threat_ids_by_hos_tdm_id: dict[str, list[str]],
    ) -> None:
        if not hos_rows:
            return
        logger.info("Sending %s HOS rules to Kafka", len(hos_rows))
        for hos in hos_rows:
            active_threat_ids = active_threat_ids_by_hos_tdm_id.get(hos.tdm_id, [])
            key_bytes = self._encode_key(hos.tdm_id)
            payload = self._build_hos_payload(hos, active_threat_ids)
            value_bytes = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.producer.produce(
                topic=self.hos_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("hos", hos.tdm_id),
            )
        self.producer.flush(timeout=10)

    def _delete_hos_sync(self, hos_tdm_ids: list[str]) -> None:
        if not hos_tdm_ids:
            return
        logger.info("Deleting %s HOS rules from Kafka", len(hos_tdm_ids))

        # Phase 1: send logical delete payloads for clients that do not consume tombstones.
        for hos_tdm_id in hos_tdm_ids:
            key_bytes, value_bytes = self._prepare_hos_logical_delete_message(hos_tdm_id)
            self.producer.produce(
                topic=self.hos_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("hos", hos_tdm_id),
            )
        self.producer.flush(timeout=10)

        # Phase 2: send tombstones for Kafka compaction.
        for hos_tdm_id in hos_tdm_ids:
            key_bytes, value_bytes = self._prepare_tombstone_message(hos_tdm_id)
            self.producer.produce(
                topic=self.hos_topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_callback("hos", hos_tdm_id),
            )
        self.producer.flush(timeout=10)

    def _delivery_callback(self, entity: str, entity_id: str):
        def on_delivery(err, _):
            if err:
                logger.error("Failed to deliver Kafka message for %s %s: %s", entity, entity_id, err)
                if ErrorTracker.should_log(f"kafka_{entity}_{entity_id}"):
                    logger.error(
                        "Kafka delivery failure tracked for audit",
                        extra={"entity": entity, "entity_id": entity_id, "error": str(err)},
                    )
                raise RuntimeError(f"Failed to deliver message: {err}")

        return on_delivery

    @staticmethod
    def _encode_key(key: str) -> bytes:
        return key.encode("utf-8")

    @staticmethod
    def _format_datetime(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _format_date(value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.date().isoformat()

    @staticmethod
    def _threat_actor_refs(values: list[str] | None) -> list[dict[str, str]]:
        """Wire ``{id, name}`` objects for correlation-job / agent-job ``ChainRaw.ThreatActor``.

        Names come from the cached enterprise ATT&CK bundle; unknown or legacy values keep the
        raw string as both ``id`` and ``name`` (same fallback as HTTP ``MitreIdNameRef``).
        """
        return [mitre_attack.resolve_group_ref(raw)._asdict() for raw in (values or [])]

    def _build_active_threat_payload(self, threat: ActiveThreat) -> dict:
        return {
            "id": threat.tdm_id,
            "name": threat.name,
            "threat_type": ",".join(threat.threat_types or []),
            "severity": threat.severity,
            "industries": list(threat.industries or []),
            "geographies": list(threat.geographies or []),
            "threat_actors": self._threat_actor_refs(threat.threat_actors),
            "summary": threat.summary,
            "investigation": threat.investigation,
            "mitigation": threat.mitigation,
            "response": threat.response,
            "release_date": self._format_date(threat.tdm_created),
            # D8's global conjunct, on the SHARED catalogue topic. The wire key stays `enabled` —
            # the matcher's two `at.enabled` gates are live consumers of it — and only the source of
            # the value moved to `is_blocked`. A second `is_blocked` key would be two keys for one
            # fact, which is how consumers diverge; retiring `enabled` needs its own coordinated task.
            "enabled": not bool(threat.is_blocked),
            "source": "socprime_tdm",
            "updated_at": self._format_datetime(threat.tdm_updated or threat.updated),
        }

    def _build_hos_payload(self, hos: HighOrderSigma, active_threat_ids: list[str]) -> dict:
        parsed = self._parse_hos_yaml(hos.body)
        return {
            "id": hos.tdm_id,
            "active_threat_id": active_threat_ids[0] if active_threat_ids else None,
            "title": parsed.get("title") or hos.name,
            "description": parsed.get("description"),
            "level": str(parsed.get("level")).upper() if parsed.get("level") else None,
            "status_lifecycle": parsed.get("status"),
            "author": parsed.get("author"),
            "references": parsed.get("references") if isinstance(parsed.get("references"), list) else [],
            "tags": parsed.get("tags") if isinstance(parsed.get("tags"), list) else [],
            "yaml_text": hos.body,
            "updated_at": self._format_datetime(hos.tdm_updated or hos.updated),
            "is_user_edited": bool(getattr(hos, "is_user_edited", False)),
        }

    def _prepare_active_threat_logical_delete_message(self, threat_id: str) -> tuple[bytes, bytes]:
        payload = {
            "id": threat_id,
            "deleted": True,
        }
        return self._encode_key(threat_id), json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _prepare_hos_logical_delete_message(self, hos_tdm_id: str) -> tuple[bytes, bytes]:
        payload = {
            "id": hos_tdm_id,
            "deleted": True,
        }
        return self._encode_key(hos_tdm_id), json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def _prepare_tombstone_message(self, entity_id: str) -> tuple[bytes, None]:
        return self._encode_key(entity_id), None

    @staticmethod
    def _parse_hos_yaml(body: str | None) -> dict:
        if not body:
            return {}
        try:
            parsed = yaml.safe_load(body)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}


_kafka_active_threats_service: KafkaActiveThreatsSyncService | None = None


def get_kafka_active_threats_service() -> KafkaActiveThreatsSyncService:
    global _kafka_active_threats_service
    if _kafka_active_threats_service is None:
        _kafka_active_threats_service = KafkaActiveThreatsSyncService()
    return _kafka_active_threats_service
