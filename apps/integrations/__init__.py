"""Public exports for the integration provider subsystem.

Re-exports the registry, base provider class, connection/deploy/search result
models, optional capability mixins (:class:`~apps.integrations.base.DeployableIntegration`,
:class:`~apps.integrations.base.SearchableIntegration`), and the
:class:`~apps.integrations.registry.IntegrationRegistry` singleton.
"""

from apps.integrations.base import (
    BaseIntegrationProvider,
    ConnectionResult,
    DeployableIntegration,
    DeployResult,
    SearchableIntegration,
    SearchResult,
)
from apps.integrations.registry import IntegrationRegistry

__all__ = [
    "BaseIntegrationProvider",
    "ConnectionResult",
    "DeployResult",
    "DeployableIntegration",
    "IntegrationRegistry",
    "SearchResult",
    "SearchableIntegration",
]
