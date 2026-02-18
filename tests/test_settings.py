"""Unit tests for apps/core/settings.py."""

import os


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
        assert settings.database_pool_size == 10
        assert settings.database_max_overflow == 20
        assert settings.database_pool_recycle == 1800
        assert settings.database_pool_timeout == 30
        assert settings.database_echo is False

        # Auth defaults
        assert settings.jwt_algorithm == "HS256"

        # These have defaults, but may be overridden by env
        assert settings.kafka_auth_method in ["PLAINTEXT", "SASL", "SSL"]
        assert isinstance(settings.jwt_access_token_expire_minutes, int)
        assert isinstance(settings.dashboard_broadcast_interval_seconds, float)

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
