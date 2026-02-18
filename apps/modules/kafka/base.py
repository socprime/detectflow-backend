"""Base classes and utilities for Kafka operations."""

import ssl
from abc import ABC, abstractmethod
from collections.abc import Callable

from apps.core.logger import get_logger
from apps.core.settings import settings

logger = get_logger(__name__)


class KafkaConfigBuilder:
    """Utility class for building Kafka configurations."""

    @staticmethod
    def build_confluent_config(
        client_id: str = "admin-panel-kafka-client",
    ) -> dict:
        """Build configuration for confluent_kafka clients.

        Supports AdminClient, Producer, Consumer.

        Args:
            client_id: Client identifier for Kafka.

        Returns:
            Dictionary with Kafka configuration for confluent_kafka.

        Raises:
            ValueError: If authentication method is invalid or
                required credentials are missing.
        """
        config = {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": client_id,
        }

        if settings.kafka_auth_method == "SASL":
            api_key = settings.kafka_api_key
            api_secret = settings.kafka_api_secret

            if not api_key or not api_secret:
                raise ValueError("KAFKA_API_KEY and KAFKA_API_SECRET required for SASL auth")

            sasl_config = {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": api_key,
                "sasl.password": api_secret,
            }
            # Disable hostname verification if configured
            if not settings.kafka_ssl_check_hostname:
                sasl_config["ssl.endpoint.identification.algorithm"] = "none"
            config.update(sasl_config)
        elif settings.kafka_auth_method == "SSL":
            ssl_config = {
                "security.protocol": "SSL",
                "ssl.ca.location": settings.kafka_ssl_ca_location,
                "ssl.certificate.location": settings.kafka_ssl_certificate_location,
                "ssl.key.location": settings.kafka_ssl_key_location,
            }
            # Disable hostname verification if configured
            if not settings.kafka_ssl_check_hostname:
                ssl_config["ssl.endpoint.identification.algorithm"] = "none"
            config.update(ssl_config)
        elif settings.kafka_auth_method == "PLAINTEXT":
            config["security.protocol"] = "PLAINTEXT"
        else:
            raise ValueError(f"Invalid authentication method for Kafka: {settings.kafka_auth_method}")

        return config

    @staticmethod
    def build_aiokafka_config(
        group_id: str | None = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        value_deserializer: Callable | None = None,
        key_deserializer: Callable | None = None,
    ) -> dict:
        """Build configuration for aiokafka clients.

        Supports AIOKafkaConsumer.

        Args:
            group_id: Consumer group ID. If None, will be generated.
            auto_offset_reset: Offset reset policy ("earliest" or "latest").
            enable_auto_commit: Whether to enable auto-commit.
            value_deserializer: Function to deserialize message values.
            key_deserializer: Function to deserialize message keys.

        Returns:
            Dictionary with Kafka configuration for aiokafka.

        Raises:
            ValueError: If authentication method is invalid or
                required credentials are missing.
        """
        config = {
            "bootstrap_servers": settings.kafka_bootstrap_servers,
            "group_id": group_id or "admin-panel-kafka-consumer",
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": enable_auto_commit,
        }

        if value_deserializer:
            config["value_deserializer"] = value_deserializer
        if key_deserializer:
            config["key_deserializer"] = key_deserializer

        if settings.kafka_auth_method == "SASL":
            api_key = settings.kafka_api_key
            api_secret = settings.kafka_api_secret

            if not api_key or not api_secret:
                raise ValueError("KAFKA_API_KEY and KAFKA_API_SECRET required for SASL auth")

            ssl_context = ssl.create_default_context()
            if not settings.kafka_ssl_check_hostname:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            config.update(
                {
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "PLAIN",
                    "sasl_plain_username": api_key,
                    "sasl_plain_password": api_secret,
                    "ssl_context": ssl_context,
                }
            )
        elif settings.kafka_auth_method == "SSL":
            ssl_context = ssl.create_default_context()
            if not settings.kafka_ssl_check_hostname:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            if settings.kafka_ssl_ca_location:
                ssl_context.load_verify_locations(settings.kafka_ssl_ca_location)
            if settings.kafka_ssl_certificate_location and settings.kafka_ssl_key_location:
                ssl_context.load_cert_chain(
                    settings.kafka_ssl_certificate_location,
                    settings.kafka_ssl_key_location,
                )

            config.update(
                {
                    "security_protocol": "SSL",
                    "ssl_context": ssl_context,
                }
            )
        # PLAINTEXT - no additional config needed

        return config


class BaseKafkaSyncClient(ABC):
    """Base class for synchronous Kafka clients (confluent_kafka)."""

    def __init__(self, client_id: str | None = None):
        """Initialize base Kafka sync client.

        Args:
            client_id: Client identifier for Kafka. If None, uses default.
        """
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.auth_method = settings.kafka_auth_method
        self._client_id = client_id or self._get_default_client_id()
        self._config = KafkaConfigBuilder.build_confluent_config(self._client_id)

    @abstractmethod
    def _get_default_client_id(self) -> str:
        """Return default client ID for this client type."""
        pass


class BaseKafkaAsyncClient(ABC):
    """Base class for asynchronous Kafka clients (aiokafka)."""

    def __init__(
        self,
        group_id: str | None = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
    ):
        """Initialize base Kafka async client.

        Args:
            group_id: Consumer group ID. If None, uses default.
            auto_offset_reset: Offset reset policy ("earliest" or "latest").
            enable_auto_commit: Whether to enable auto-commit.
        """
        self.bootstrap_servers = settings.kafka_bootstrap_servers
        self.auth_method = settings.kafka_auth_method
        self._group_id = group_id or self._get_default_group_id()
        self._config = KafkaConfigBuilder.build_aiokafka_config(
            group_id=self._group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            value_deserializer=(lambda m: m.decode("utf-8") if m else None),
            key_deserializer=(lambda k: k.decode("utf-8") if k else None),
        )

    @abstractmethod
    def _get_default_group_id(self) -> str:
        """Return default group ID for this client type."""
        pass
