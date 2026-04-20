"""Registry of platform health checkers.

PLATFORMS maps platform name (e.g. 'PostgreSQL', 'Kafka') to checker instance.
Used by HealthCheckService to run checks and persist results.
"""

from apps.services.health_check.platforms.cloud_repositories import CloudRepositoriesHealthChecker
from apps.services.health_check.platforms.kafka import KafkaHealthChecker
from apps.services.health_check.platforms.postgresql import PostgreSQLHealthChecker

PLATFORMS = {
    "PostgreSQL": PostgreSQLHealthChecker(),
    "Cloud Repositories": CloudRepositoriesHealthChecker(),
    "Kafka": KafkaHealthChecker(),
}
