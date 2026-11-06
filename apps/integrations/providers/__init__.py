"""Integration providers auto-discovery.

Importing this package triggers all @IntegrationRegistry.register decorators.
To add a new provider: create a file in this directory and add its import below.
"""

from apps.integrations.providers import (
    elastic_stack,
    github,
    gitlab,
    microsoft_sentinel,
    opensearch,
    socprime_tdm,
    splunk,
)

__all__ = [
    "elastic_stack",
    "github",
    "gitlab",
    "microsoft_sentinel",
    "opensearch",
    "socprime_tdm",
    "splunk",
]
