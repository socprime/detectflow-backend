from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Logging settings
    log_level: str = "INFO"  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    # Database settings
    database_url: str
    database_pool_size: int = 10  # Permanent connections in pool
    database_max_overflow: int = 20  # Extra connections when pool exhausted (total max: 30)
    database_pool_recycle: int = 1800  # Recycle connections after 30 min (prevents stale connections)
    database_pool_timeout: int = 30  # Timeout waiting for connection from pool
    database_echo: bool = False  # Log SQL queries (disabled by default for performance)
    tdm_api_base_url: str = "https://api.tdm.socprime.com"
    tdm_hostname: str = "tdm.socprime.com"

    # Kafka settings
    kafka_bootstrap_servers: str
    kafka_auth_method: Literal["PLAINTEXT", "SASL", "SSL"] = "PLAINTEXT"
    kafka_api_key: str | None = None
    kafka_api_secret: str | None = None
    kafka_ssl_ca_location: str | None = None
    kafka_ssl_certificate_location: str | None = None
    kafka_ssl_key_location: str | None = None
    kafka_ssl_check_hostname: bool = True  # Set to false to disable SSL hostname verification
    kafka_sigma_rules_topic: str = "sigma-rules"
    kafka_activity_topic: str = "etl-activity"
    kafka_activity_consumer_group: str = "admin-panel-activity"
    kafka_metrics_topic: str = "rule-statistics"  # Topic for per-rule metrics from Flink
    kafka_metrics_consumer_group: str = "admin-panel-metrics"

    # Kafka topic initialization settings (used by entrypoint.sh)
    kafka_default_partitions: int = 1  # Default partitions for new topics
    kafka_default_replication_factor: int = 2  # Default replication factor (set to 2+ in production)

    # Dashboard settings
    dashboard_broadcast_interval_seconds: float = 2.0
    audit_logs_retention_days: int = 30

    # Flink metrics polling interval (seconds)
    flink_metrics_poll_interval: float = 5.0

    # Sync settings
    enable_auto_sync: bool = True  # Enable automatic scheduled sync (default: enabled)
    sync_api_repos_interval_minutes: int = 5
    sync_api_repos_timeout_seconds: int = 600  # Timeout for sync operation (default: 10 minutes)

    # Auth settings
    jwt_secret_key: str = "your-secret-key-change-in-production"
    jwt_refresh_secret_key: str = "your-refresh-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7

    # Kubernetes settings
    kubernetes_namespace: str = "security"  # K8s namespace for Flink deployments

    # Flink deployment settings
    flink_image: str = "flink-sigma-detector:latest"  # Docker image for Flink jobs
    image_pull_policy: str = "Always"  # Image pull policy (Always, IfNotPresent, Never)

    # Flink resource settings
    flink_taskmanager_cpu: float = 2.0  # CPU cores per TaskManager
    flink_taskmanager_memory_gb: float = 4.0  # Memory in GB per TaskManager
    flink_jobmanager_cpu: float = 1.0  # CPU cores for JobManager
    flink_jobmanager_memory_gb: float = 2.0  # Memory in GB for JobManager
    flink_taskmanager_slots: int = 4  # Slots per TaskManager (parallelism per replica)

    # Flink autoscaler settings (None = auto-calculated from TaskManager resources)
    autoscaler_quota_cpu: float | None = None  # CPU quota for autoscaler
    autoscaler_quota_memory_gb: float | None = None  # Memory quota in GB for autoscaler

    # Flink node selector for pod scheduling (key=value for nodeSelector in pod specs)
    flink_node_selector_key: str = "spec-node-ns-pod-disk"
    flink_node_selector_value: str = "pvc-security"


# Create a singleton instance
settings = Settings()
