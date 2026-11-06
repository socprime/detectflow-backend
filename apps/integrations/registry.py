from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from apps.core.logger import get_logger

if TYPE_CHECKING:
    from apps.integrations.base import BaseIntegrationProvider

logger = get_logger(__name__)


class IntegrationRegistry:
    """Singleton registry for integration providers.

    Stores provider *classes* (not instances) keyed by integration_type string.
    Provider classes are registered via the @register decorator.

    Usage:
        @IntegrationRegistry.register
        class SplunkProvider(BaseIntegrationProvider):
            integration_type = "splunk"
            ...

    Access:
        provider_cls = IntegrationRegistry.get("splunk")
        all_providers = IntegrationRegistry.get_all()
    """

    _providers: ClassVar[dict[str, type[BaseIntegrationProvider]]] = {}
    _initialized: ClassVar[bool] = False

    @classmethod
    def register(cls, provider_cls: type[BaseIntegrationProvider]) -> type[BaseIntegrationProvider]:
        """Decorator to register a provider class."""
        key = provider_cls.integration_type
        if key in cls._providers:
            raise ValueError(f"Integration type '{key}' is already registered")
        cls._providers[key] = provider_cls
        logger.info("Registered integration provider: %s", key)
        return provider_cls

    @classmethod
    def get(cls, integration_type: str) -> type[BaseIntegrationProvider]:
        """Get provider class by type key.

        Raises:
            KeyError: If integration_type is not registered.
        """
        cls._ensure_initialized()
        if integration_type not in cls._providers:
            raise KeyError(f"Unknown integration type: {integration_type}")
        return cls._providers[integration_type]

    @classmethod
    def get_all(cls) -> dict[str, type[BaseIntegrationProvider]]:
        """Get all registered provider classes."""
        cls._ensure_initialized()
        return dict(cls._providers)

    @classmethod
    def get_by_category(cls, category: str) -> dict[str, type[BaseIntegrationProvider]]:
        """Get providers filtered by category."""
        cls._ensure_initialized()
        return {key: prov for key, prov in cls._providers.items() if prov.category == category}

    @classmethod
    def _ensure_initialized(cls) -> None:
        """Ensure providers module has been imported (auto-discovery)."""
        if not cls._initialized:
            cls._discover_providers()
            cls._initialized = True

    @classmethod
    def _discover_providers(cls) -> None:
        """Import the providers package to trigger all @register decorators."""
        import apps.integrations.providers  # noqa: F401
