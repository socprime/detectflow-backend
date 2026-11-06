from typing import Literal, Self

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from apps.core.constants import SOCPRIME_TDM_API_BASE_URL
from apps.core.version import get_version


class Settings(BaseSettings):
    """Application settings loaded from environment variables and .env file"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application version (from git tag / VERSION file)
    detectflow_backend_version: str = get_version()

    # Logging settings
    log_level: str = "INFO"  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    # API documentation (Swagger UI / ReDoc / OpenAPI JSON). Off by default in OSS.
    disable_api_docs: bool = True

    # Browser origins allowed to call the API cross-origin, comma-separated
    # ("scheme://host[:port]", no trailing slash). Empty (default) means same-origin only —
    # correct whenever the frontend calls the API on its own host (e.g. via reverse-proxy path
    # routing), which is the default deployment shape. Set explicitly only when the frontend is
    # served from a different origin than the API (e.g. local dev with split ports). "*" is
    # dropped rather than honoured: auth cookies make every request credentialed, and a wildcard
    # origin would let any site the user visits drive their session.
    cors_allow_origins: str = ""

    # Database settings
    database_url: str
    database_pool_size: int = 20  # Permanent connections in pool
    database_max_overflow: int = 30  # Extra connections when pool exhausted (total max: 50)
    database_pool_recycle: int = 1800  # Recycle connections after 30 min (prevents stale connections)
    database_pool_timeout: int = 30  # Timeout waiting for connection from pool
    database_echo: bool = False  # Log SQL queries (disabled by default for performance)
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
    kafka_sigma_rules_topic: str = "detectflow.sigma-rules"
    kafka_active_threats_topic: str = "detectflow.active-threats"
    kafka_hos_rules_topic: str = "detectflow.hos-rules"
    kafka_tagged_events_topic: str = "detectflow.tagged-events"
    kafka_activity_topic: str = "detectflow.activity"
    kafka_activity_consumer_group: str = "admin-panel-activity"
    kafka_metrics_topic: str = "detectflow.rule-metrics"  # Topic for per-rule metrics from Flink
    kafka_metrics_consumer_group: str = "admin-panel-metrics"

    # Detectflow correlation: compacted chains-config broadcast (key=global)
    kafka_chains_config_topic: str = "detectflow.chains-config"
    # Detectflow: raw chains from correlation-job → agent-job (key=chain_id)
    kafka_chains_raw_topic: str = "detectflow.chains-raw"
    # Detectflow: enriched chains from correlation + LLM (key=chain_id)
    kafka_chains_analysed_topic: str = "detectflow.chains-analysed"
    kafka_chains_analysed_consumer_group: str = "admin-panel-chains-analysed"
    kafka_chains_analysed_enabled: bool = True
    kafka_chains_analysed_auto_offset_reset: Literal["earliest", "latest"] = "earliest"

    # Poison-record policy for detectflow.chains-analysed. One consumer serves every correlation
    # pipeline, so a record that can never apply must not hold its partition: it is retried with
    # exponential backoff, then parked in chains_analysed_dead_letters and skipped.
    kafka_chains_analysed_max_attempts: int = Field(default=5, ge=1)
    kafka_chains_analysed_retry_backoff_base_seconds: float = Field(default=0.5, gt=0)
    kafka_chains_analysed_retry_backoff_max_seconds: float = Field(default=30.0, gt=0)
    # Wall-clock ceiling on retrying ONE record; also the grace period before a loop stalled on
    # infrastructure is reported unhealthy.
    kafka_chains_analysed_retry_deadline_seconds: float = Field(default=120.0, gt=0)
    kafka_chains_analysed_dead_letter_max_payload_bytes: int = Field(default=262144, ge=1024)
    kafka_chains_analysed_dead_letter_retention_days: int = Field(default=30, ge=1)
    # Liveness bound for the poll loop (NOT message age — chains are rare by design).
    kafka_chains_analysed_poll_stale_seconds: int = Field(default=60, ge=10)

    # Per-correlation-pipeline analysed-chain topics: detectflow.chains-analysed.<corr_id>.
    # One consumer, one pattern subscription — new pipelines are picked up on metadata refresh.
    kafka_chains_analysed_topic_pattern_enabled: bool = True
    # Also consume the pre-epic singleton topic while it still holds unconsumed records.
    kafka_chains_analysed_include_legacy_topic: bool = True
    # Discovery bound for a newly created pipeline's topic, and the ONLY consumer that deviates
    # from aiokafka's 300s default. A pattern subscription refetches FULL-cluster metadata every
    # interval, and the payload grows with every per-pipeline topic the epic adds. 60s is well
    # inside the correlation window a new pipeline needs before it can emit anything, and nothing
    # is lost meanwhile (auto_offset_reset=earliest). Do not go below the floor.
    kafka_chains_analysed_metadata_max_age_ms: int = Field(default=60000, ge=30000)

    # Kafka topic initialization settings (used by entrypoint.sh)
    kafka_default_partitions: int = 1  # Default partitions for new topics
    kafka_default_replication_factor: int = 2  # Default replication factor (set to 2+ in production)
    # Partition count for the tagged-events tech topic. Defaults to 12 so
    # correlation-job can consume 12-way in parallel. Tunable per cluster.
    kafka_tagged_events_partitions: int = 12
    # Per-detection-pipeline tagged-events topic partitions (detectflow.tagged-events.<pipeline_id>).
    # 6 is MORE headroom than before, not less: the shared 12-partition topic carried every
    # pipeline's traffic, and a correlation pipeline consumes the sum over its member topics.
    kafka_pipeline_tagged_events_partitions: int = 6
    # Per-correlation-pipeline chains-raw / chains-analysed partitions. Bounded by
    # agent_job_parallelism (default 1); 3 leaves room to scale an agent job without repartition.
    kafka_correlation_chains_partitions: int = 3
    # What happens to a per-entity topic when its owning row is deleted.
    #   keep   — leave the topic untouched (external archival / strict retention regimes)
    #   retire — set cleanup.policy=delete + retention.ms=<retire window>, let the broker age it out
    #   delete — immediate AdminClient.delete_topics (irreversible)
    kafka_topic_delete_policy: Literal["keep", "retire", "delete"] = "retire"
    kafka_retired_topic_retention_ms: int = 86_400_000  # 24h grace window for "retire"
    # Bound on the pre-delete drain wait for a chains-analysed topic.
    kafka_topic_delete_drain_timeout_seconds: int = 30

    # Dashboard settings
    # Shared by the detection dashboard SSE and the platform-wide correlation graph stream.
    dashboard_broadcast_interval_seconds: float = 2.0
    # TTL for the dashboard's cached DB topology (topics/repos/rule counts/pipeline rows).
    # Warm SSE ticks within the TTL reuse the snapshot and touch no DB; pipeline CRUD
    # invalidates it immediately, repo/rule sync changes surface within this bound.
    dashboard_topology_ttl_seconds: float = 30.0
    # Concurrent Kubernetes reads when observing many correlation-pipeline runtimes (list,
    # detail, Active Threat pipeline chips). Never one API-server call per pipeline.
    correlation_dashboard_k8s_concurrency: int = 4
    attack_chains_list_sse_heartbeat_seconds: float = 10.0
    audit_logs_retention_days: int = 30

    # Flink metrics polling interval (seconds)
    flink_metrics_poll_interval: float = 5.0

    # Sync settings
    enable_auto_sync: bool = True  # Enable automatic scheduled sync (default: enabled)
    sync_api_repos_interval_minutes: int = 5
    sync_api_repos_timeout_seconds: int = 600  # Timeout for sync operation (default: 10 minutes)
    sync_active_threats_enabled: bool = True
    sync_active_threats_interval_minutes: int = 10
    sync_active_threats_timeout_seconds: int = 600
    # Interval (hours) for the scheduled full (non-incremental) Active Threat/HOS/rule re-sync.
    sync_active_threats_full_refresh_interval_hours: int = 6
    sync_active_threats_retention_days: int = -1  # <= 0 disables AT retention cleanup
    sync_delete_at_missing_in_tdm: bool = False
    sync_delete_orphan_hos: bool = False
    sync_delete_unreferenced_system_rules: bool = False

    # GitLab integration
    gitlab_api_timeout_seconds: float = 30.0  # Per-request timeout for GitLab REST API calls

    # Health check schedule (runs when scheduler is started, i.e. when enable_auto_sync is True)
    health_check_interval_minutes: int = 5  # Interval for scheduled health_check check_all

    # Rule versioning
    rule_version_retention: int = Field(default=10, ge=2)  # Keep last N versions per rule in rule_content table
    # When true, API responses omit Sigma rule bodies for every repository type except
    # ``local`` and ``external`` (opensource GitHub catalog). Kafka / Flink sync is unchanged.
    hide_restricted_rule_bodies: bool = False

    # Auth settings
    jwt_secret_key: str = "your-secret-key-change-in-production"
    jwt_refresh_secret_key: str = "your-refresh-secret-key-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7

    # Kubernetes settings
    kubernetes_namespace: str = "security"  # K8s namespace for Flink deployments
    # Name of the LEGACY single-instance correlation runtime, the one that predates per-customer
    # correlation pipelines. Per-pipeline deployments are named by apps/core/constants.py
    # (corr-<uuid> / agent-<uuid>) and never read these two settings.
    correlation_job_deployment_name: str = "correlation-job"
    correlation_job_image: str = "detectflow-correlation-job:latest"
    correlation_job_parallelism: int = 1
    # PyFlink + RocksDB needs real CPU and memory headroom for the Python worker in BOTH
    # modes — the worker's off-heap RSS is not counted in taskmanager.memory.process.size
    # whether it runs in-JVM (thread mode / Pemja, the default since 2026-06) or as a
    # separate Beam SDK-harness process (CORRELATION_PYTHON_MODE=process, which only adds
    # to the footprint). Without the headroom checkpoints stall/time out. 1 core is far too
    # low: budget 2-4 cores per slot and enough process memory that the off-heap Python
    # worker has room beyond JVM + RocksDB managed memory. These are per-cluster physics,
    # not per-pipeline tuning; a correlation pipeline overrides them only deliberately.
    correlation_job_taskmanager_memory: str = "4g"
    correlation_job_taskmanager_cpu: float = 3.0
    correlation_job_jobmanager_memory: str = "2g"
    correlation_job_jobmanager_cpu: float = 1.0
    # Subscription universe of the LEGACY single-instance correlation runtime, inlined as the
    # KAFKA_INPUT_TOPICS env on its FlinkDeployment containers. A correlation pipeline builds
    # that list from its members' `pipelines.tagged_events_topic` instead (epic D1); this value
    # is what the legacy deployment keeps using until the migration adopts it. Matches
    # `kafka_tagged_events_topic` by default.
    correlation_job_input_topic: str = "detectflow.tagged-events"

    # Agent-job FlinkDeployment. Thread mode (Pemja) per architecture §1 — must
    # not share a TM pool with process-mode TMs; agent_node_selector_* lets a
    # dedicated pool be carved out later. None ⇒ inherit flink_node_selector_*.
    agent_job_deployment_name: str = "agent-job"
    agent_job_image: str = "detectflow-agent-job:latest"
    agent_job_parallelism: int = 1
    agent_job_taskmanager_memory: str = "3g"
    agent_job_taskmanager_cpu: float = 1.0
    agent_job_jobmanager_memory: str = "2g"
    agent_job_jobmanager_cpu: float = 1.0
    agent_node_selector_key: str | None = None
    agent_node_selector_value: str | None = None

    # Window-based "degraded" health signal for the Attack Chains runtime view.
    # Derived from recent warning/error Flink activity events already stored in
    # audit_logs (no extra Kafka consumer). Window slides with now() so the
    # signal self-heals once errors stop arriving.
    attack_chains_health_window_seconds: int = 600

    # agent-job-specific LLM config (envFrom). Per-job — correlation-job does
    # NOT mount these (no LLM there).
    detectflow_agent_configmap_name: str = "detectflow-agent-config"
    detectflow_agent_secret_name: str = "detectflow-agent-secrets"
    # Private-CA bundle for an LLM endpoint that serves a self-signed / internal
    # certificate. The bundle lives in `detectflow_agent_secret_name` under the
    # key that equals the *file name* of this path; agent-job pods mount it at
    # `dirname(path)` so the agent's httpx client (SSL_CERT_FILE) trusts it.
    # Leave unset — the path is then read from SSL_CERT_FILE in the agent
    # ConfigMap, which is the value the container itself sees. Set it only to
    # override that (it is inlined into the pod env, beating the ConfigMap copy).
    detectflow_agent_ssl_cert_file: str | None = None
    # Convention used when that ConfigMap cannot be read (the backend is usually
    # not granted `get configmaps` in the Flink namespace). The key is then
    # projected *optionally* and SSL_CERT_FILE is NOT inlined, so a deployment
    # without a private CA is unaffected while one that follows the convention
    # works with no extra RBAC. Empty disables the fallback.
    detectflow_agent_ssl_cert_file_default: str = "/etc/ssl/agent-ca/ca.pem"
    # Shared cluster infra (KAFKA_BOOTSTRAP_SERVERS, KAFKA_AUTH_METHOD, log level
    # defaults). Reused from sigma-detector — both correlation-job and agent-job
    # mount these via envFrom, the same way sigma-detector does
    # (apps/templates/flink-deployment.yaml.j2).
    flink_config_configmap_name: str = "flink-config"
    flink_sigma_config_secret_name: str = "flink-sigma-config"
    detectflow_checkpoint_interval_sec: int = 60

    # Flink deployment settings
    flink_image: str = "flink-sigma-detector:latest"  # Docker image for Flink jobs (K8s provider)
    flink_image_cmf: str | None = None  # Docker image for CMF provider (falls back to flink_image)
    image_pull_policy: str = "Always"  # Image pull policy (Always, IfNotPresent, Never)

    # Flink resource settings (per TaskManager; each TM = one parallelism unit)
    flink_taskmanager_cpu: float = 1.5  # CPU cores per TaskManager
    flink_taskmanager_memory_gb: float = 2.0  # Memory in GB per TaskManager (JVM + Python)
    flink_jobmanager_cpu: float = 1.0  # CPU cores for JobManager
    flink_jobmanager_memory_gb: float = 2.0  # Memory in GB for JobManager

    # PyFlink TaskManager JVM/pod memory split (K8s / CMF providers).
    #
    # The configured TaskManager memory is the pod limit (not silently increased). Process
    # mode prefers leaving process_native_headroom_min_mb outside process.size for Python
    # workers; if that does not fit, headroom is compressed inside the configured total and
    # the JVM is floored at process_memory_min_mb (1.25 GiB). Thread mode gives nearly all
    # memory to process.size and keeps only native_headroom_min_mb outside for off-heap.
    flink_taskmanager_process_memory_base_mb: int = 1536
    flink_taskmanager_process_memory_growth_fraction: float = 0.1
    flink_taskmanager_process_memory_min_mb: int = 1280  # 1.25 GiB — below this RocksDB/process runs starve
    flink_taskmanager_process_native_headroom_min_mb: int = 1536
    flink_taskmanager_native_headroom_min_mb: int = 512  # thread-mode off-heap reserve

    # Matcher runtime switches (rendered into FlinkDeployment + companion memory knobs).
    # Keep in lockstep with the matcher repo defaults (process + rocksdb).
    matcher_python_execution_mode: Literal["process", "thread"] = "process"
    matcher_state_backend: Literal["rocksdb", "memory"] = "rocksdb"

    # Flink autoscaler settings (None = auto-calculated from TaskManager resources)
    autoscaler_quota_cpu: float | None = None  # CPU quota for autoscaler
    autoscaler_quota_memory_gb: float | None = None  # Memory quota in GB for autoscaler

    # Flink node selector for pod scheduling (key=value for nodeSelector in pod specs)
    # Examples: app=detectflow-prod, app=detectflow-demo
    flink_node_selector_key: str = "app"
    flink_node_selector_value: str = "detectflow-prod"

    # Flink PVC names for state storage
    flink_checkpoints_pvc: str = "flink-checkpoints-pvc"
    flink_ha_pvc: str = "flink-ha-pvc"
    flink_savepoints_pvc: str = "flink-savepoints-pvc"

    # How long to watch the storage-cleanup Job before reporting `timed_out`. The Job only removes
    # three directories, so this bounds a pod that never schedules (node selector, quota) rather
    # than slow work. A timeout is never fatal — the state simply survives, visibly.
    flink_cleanup_job_timeout_seconds: int = 120

    # Flink Provider selection (kubernetes or cmf)
    flink_provider: Literal["kubernetes", "cmf"] = "kubernetes"

    # CMF (Confluent Manager for Apache Flink) settings
    cmf_url: str | None = None  # CMF API URL (required when flink_provider="cmf")
    cmf_environment: str | None = None  # CMF environment name (required when flink_provider="cmf")
    cmf_namespace: str | None = None  # K8s namespace where CMF deploys Flink apps (required when flink_provider="cmf")
    cmf_client_cert_path: str | None = None  # mTLS client certificate (optional)
    cmf_client_key_path: str | None = None  # mTLS client key (optional)
    cmf_ca_cert_path: str | None = None  # CA certificate (optional)

    # TDM settings
    tdm_api_base_url: str = SOCPRIME_TDM_API_BASE_URL

    # MITRE ATT&CK enterprise STIX bundle. The Docker image bakes the bundle in at
    # build time so it is NOT fetched from raw.githubusercontent.com on startup.
    # When the file is missing (e.g. local dev), the bundle is fetched from
    # MITRE/CTI over the network on first load instead.
    mitre_attack_data_path: str = "data/enterprise-attack.json"
    mitre_attack_cti_branch: str = "master"

    @property
    def cors_allow_origins_list(self) -> list[str]:
        """Parse `cors_allow_origins` into exact origins, discarding blanks and any wildcard."""
        origins = (origin.strip().rstrip("/") for origin in self.cors_allow_origins.split(","))
        return [origin for origin in origins if origin and origin != "*"]

    @model_validator(mode="after")
    def validate_cmf_settings(self) -> Self:
        """Validate that CMF settings are provided when using CMF provider."""
        if self.flink_provider == "cmf":
            missing = []
            if not self.cmf_url:
                missing.append("CMF_URL")
            if not self.cmf_environment:
                missing.append("CMF_ENVIRONMENT")
            if not self.cmf_namespace:
                missing.append("CMF_NAMESPACE")
            if missing:
                raise ValueError(f"CMF provider requires: {', '.join(missing)}")
        return self


# Create a singleton instance
settings = Settings()
