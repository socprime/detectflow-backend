"""Flink provider factory.

This module provides a factory function for creating the configured
Flink provider based on environment settings.
"""

from functools import lru_cache

from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.providers.base import FlinkProvider
from apps.providers.cmf import CMFFlinkProvider
from apps.providers.kubernetes import KubernetesFlinkProvider

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_flink_provider() -> FlinkProvider:
    """Get the configured Flink provider (singleton).

    Returns a cached singleton instance of the appropriate FlinkProvider
    based on the FLINK_PROVIDER environment variable:
    - "kubernetes" (default): Uses Flink Kubernetes Operator
    - "cmf": Uses Confluent Manager for Apache Flink

    Returns:
        FlinkProvider instance

    Example:
        from apps.providers import get_flink_provider

        provider = get_flink_provider()
        result = await provider.create_deployment(config)
    """
    provider_type = settings.flink_provider

    logger.info(
        "Initializing Flink provider",
        extra={"provider_type": provider_type},
    )

    if provider_type == "cmf":
        logger.info("Using CMF Flink provider")
        return CMFFlinkProvider()

    logger.info("Using Kubernetes Flink provider")
    return KubernetesFlinkProvider()


def clear_provider_cache() -> None:
    """Clear the cached provider instance.

    Useful for testing or when settings change.
    """
    get_flink_provider.cache_clear()
    logger.info("Flink provider cache cleared")
