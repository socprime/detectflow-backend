"""Unit tests for apps/core/settings.py."""

import os

import pytest


class TestSettings:
    """Tests for Settings configuration."""

    def test_settings_loads_from_env(self, setup_test_env):
        """Settings should load from environment variables."""
        from apps.core.settings import Settings

        settings = Settings()
        assert settings.database_url == os.environ["DATABASE_URL"]
        assert settings.kafka_bootstrap_servers == os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    def test_default_values(self, setup_test_env):
        """Settings should have correct default values for non-env fields."""
        from apps.core.settings import Settings

        settings = Settings()

        # Database defaults (these are not overridden by env)
        assert settings.database_pool_size == 20
        assert settings.database_max_overflow == 30
        assert settings.database_pool_recycle == 1800
        assert settings.database_pool_timeout == 30
        assert settings.database_echo is False

        # Auth defaults
        assert settings.jwt_algorithm == "HS256"

        # These have defaults, but may be overridden by env
        assert settings.kafka_auth_method in ["PLAINTEXT", "SASL", "SSL"]
        assert isinstance(settings.jwt_access_token_expire_minutes, int)
        assert isinstance(settings.dashboard_broadcast_interval_seconds, float)
        assert isinstance(settings.attack_chains_list_sse_heartbeat_seconds, float)

    def test_kafka_auth_methods(self, setup_test_env):
        """Kafka auth method should accept valid values."""
        from apps.core.settings import Settings

        os.environ["KAFKA_AUTH_METHOD"] = "PLAINTEXT"
        settings = Settings()
        assert settings.kafka_auth_method == "PLAINTEXT"

    def test_case_insensitive(self, setup_test_env):
        """Settings should be case insensitive."""
        os.environ["database_url"] = "postgresql://test:test@localhost:5432/test_db"
        from apps.core.settings import Settings

        settings = Settings()
        assert "postgresql" in settings.database_url


class TestTopicLifecycleSettings:
    """Defaults for the per-entity topic lifecycle (read from the field defaults, not the env)."""

    def test_topic_lifecycle_defaults(self, setup_test_env):
        """Partition counts, delete policy and the retire grace window ship with safe defaults."""
        from apps.core.settings import Settings

        defaults = {name: field.default for name, field in Settings.model_fields.items()}
        assert defaults["kafka_pipeline_tagged_events_partitions"] == 6
        assert defaults["kafka_correlation_chains_partitions"] == 3
        # Deleting a topic is irreversible, so it is never the default.
        assert defaults["kafka_topic_delete_policy"] == "retire"
        assert defaults["kafka_retired_topic_retention_ms"] == 86_400_000  # 24h undo window
        assert defaults["kafka_topic_delete_drain_timeout_seconds"] == 30

    def test_topic_delete_policy_rejects_unknown_values(self, setup_test_env):
        """The policy is a Literal — a typo must fail loudly, not silently delete or keep."""
        import os

        import pydantic

        from apps.core.settings import Settings

        os.environ["KAFKA_TOPIC_DELETE_POLICY"] = "purge"
        try:
            with pytest.raises(pydantic.ValidationError):
                Settings()
        finally:
            del os.environ["KAFKA_TOPIC_DELETE_POLICY"]


class TestChainsAnalysedPoisonSettings:
    """Defaults and bounds for the chains-analysed poison-record policy."""

    def test_chains_analysed_poison_defaults(self, setup_test_env):
        """The retry budget, park limits and poll-staleness bound ship with safe defaults."""
        from apps.core.settings import Settings

        defaults = {name: field.default for name, field in Settings.model_fields.items()}
        assert defaults["kafka_chains_analysed_max_attempts"] == 5
        assert defaults["kafka_chains_analysed_retry_backoff_base_seconds"] == 0.5
        assert defaults["kafka_chains_analysed_retry_backoff_max_seconds"] == 30.0
        assert defaults["kafka_chains_analysed_retry_deadline_seconds"] == 120.0
        assert defaults["kafka_chains_analysed_dead_letter_max_payload_bytes"] == 262_144
        assert defaults["kafka_chains_analysed_dead_letter_retention_days"] == 30
        # Poll-loop liveness, NOT message age — chains are rare by design.
        assert defaults["kafka_chains_analysed_poll_stale_seconds"] == 60

    @pytest.mark.parametrize(
        ("env_var", "bad_value"),
        [
            ("KAFKA_CHAINS_ANALYSED_MAX_ATTEMPTS", "0"),
            ("KAFKA_CHAINS_ANALYSED_RETRY_BACKOFF_BASE_SECONDS", "0"),
            ("KAFKA_CHAINS_ANALYSED_RETRY_BACKOFF_MAX_SECONDS", "-1"),
            ("KAFKA_CHAINS_ANALYSED_RETRY_DEADLINE_SECONDS", "0"),
            ("KAFKA_CHAINS_ANALYSED_DEAD_LETTER_MAX_PAYLOAD_BYTES", "512"),
            ("KAFKA_CHAINS_ANALYSED_DEAD_LETTER_RETENTION_DAYS", "0"),
            ("KAFKA_CHAINS_ANALYSED_POLL_STALE_SECONDS", "5"),
        ],
    )
    def test_chains_analysed_poison_bounds_reject_out_of_range(self, setup_test_env, env_var, bad_value):
        """A zero/negative budget would either park everything or never park — fail loudly instead."""
        import os

        import pydantic

        from apps.core.settings import Settings

        os.environ[env_var] = bad_value
        try:
            with pytest.raises(pydantic.ValidationError):
                Settings()
        finally:
            del os.environ[env_var]


class TestChainsAnalysedSubscriptionSettings:
    """Defaults and bounds for the pattern subscription over the per-pipeline analysed-chain topics."""

    def test_subscription_defaults(self, setup_test_env):
        """Pattern discovery is on, the legacy singleton is still consumed, refresh is 60s."""
        from apps.core.settings import Settings

        defaults = {name: field.default for name, field in Settings.model_fields.items()}
        assert defaults["kafka_chains_analysed_topic_pattern_enabled"] is True
        assert defaults["kafka_chains_analysed_include_legacy_topic"] is True
        assert defaults["kafka_chains_analysed_metadata_max_age_ms"] == 60000

    def test_metadata_refresh_has_a_floor(self, setup_test_env):
        """Every refresh is a FULL-cluster MetadataRequest; a faster one buys nothing but traffic."""
        import os

        import pydantic

        from apps.core.settings import Settings

        os.environ["KAFKA_CHAINS_ANALYSED_METADATA_MAX_AGE_MS"] = "1000"
        try:
            with pytest.raises(pydantic.ValidationError):
                Settings()
        finally:
            del os.environ["KAFKA_CHAINS_ANALYSED_METADATA_MAX_AGE_MS"]


class TestSettingsValidation:
    """Tests for Settings validation."""

    def test_required_fields_present(self, setup_test_env):
        """Required fields should be present."""
        from apps.core.settings import Settings

        settings = Settings()
        # These are required and should not be None
        assert settings.database_url is not None
        assert settings.kafka_bootstrap_servers is not None

    def test_optional_fields_can_be_none(self, setup_test_env):
        """Optional fields can be None."""
        from apps.core.settings import Settings

        settings = Settings()
        # These are optional
        assert settings.kafka_api_key is None or isinstance(settings.kafka_api_key, str)
        assert settings.kafka_api_secret is None or isinstance(settings.kafka_api_secret, str)
