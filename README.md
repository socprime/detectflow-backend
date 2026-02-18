# DetectFlow Backend
[![Version](https://img.shields.io/badge/version-0.9.2-blue.svg)](VERSION)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.121.3-green.svg)](https://fastapi.tiangolo.com/)

Backend API for managing real-time pipelines with Sigma detection rules, built on Apache Flink and Kafka.

## Overview

This service provides a REST API for:
- Creating and managing pipelines that process events from Kafka
- Applying Sigma detection rules for security event tagging
- Real-time dashboard with SSE streaming
- SOC Prime Platform integration (API key required)
- GitHub public open-source repositories, including SigmaHQ, Microsoft, Splunk, and Elastic for rule synchronization

## Features

- **Pipeline Management** - Create, configure, and monitor Flink-based ETL pipelines
- **Real-time Dashboard** - SSE streaming for live metrics and pipeline status
- **Detection Rules** - Sigma rule management
- **Log Source Configuration** - Parser scripts and field mappings
- **AI-assisted Mapping** - Generate field mappings using AI prompts
- **JWT Authentication** - Access + refresh token rotation for security
- **Database Migrations** - Alembic for schema version control
- **Kubernetes Native** - Automatic FlinkDeployment CRD management
- **Flink Health Monitoring** - Automatic pipeline status tracking

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 14+
- Kafka cluster
- Kubernetes cluster with Flink Operator

### Installation

```bash
# Clone repository
git clone <repository-url>
cd admin-panel-backend

# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Initialize database
uv run python init_database.py

# Run database migrations
uv run alembic upgrade head

# Run server
uv run python server.py
```

## API Documentation

### Authentication

All endpoints (except `/health`) require JWT Bearer token authentication.

```bash
# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "admin@example.com", "password": "password"}'

# Use token
curl http://localhost:8000/api/v1/pipeline \
  -H "Authorization: Bearer <token>"
```


## Configuration

### Environment Variables

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/etl_admin  # (required) PostgreSQL connection string

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092  # (required) Kafka broker addresses
KAFKA_AUTH_METHOD=PLAINTEXT  # (optional, default: PLAINTEXT) Authentication method (PLAINTEXT, SASL, SSL)
KAFKA_API_KEY=  # (optional) For SASL authentication
KAFKA_API_SECRET=  # (optional) For SASL authentication
KAFKA_SSL_CA_LOCATION=  # (optional) Path to CA certificate for SSL
KAFKA_SSL_CERTIFICATE_LOCATION=  # (optional) Path to client certificate for SSL
KAFKA_SSL_KEY_LOCATION=  # (optional) Path to client private key for SSL
KAFKA_SSL_CHECK_HOSTNAME=true  # (optional, default: true) Enable SSL hostname verification
KAFKA_SIGMA_RULES_TOPIC=sigma-rules  # (optional, default: sigma-rules) Topic for rules, filters, parsers (compacted)
KAFKA_METRICS_TOPIC=rule-statistics  # (optional, default: rule-statistics) Topic for pipeline metrics
KAFKA_METRICS_CONSUMER_GROUP=admin-panel-metrics  # (optional, default: admin-panel-metrics)
KAFKA_ACTIVITY_TOPIC=etl-activity  # (optional, default: etl-activity) Topic for activity/audit logs
KAFKA_ACTIVITY_CONSUMER_GROUP=admin-panel-activity  # (optional, default: admin-panel-activity)
KAFKA_DEFAULT_PARTITIONS=1  # (optional, default: 1) Default partitions for new topics
KAFKA_DEFAULT_REPLICATION_FACTOR=2  # (optional, default: 2) Default replication factor for new topics

# Kubernetes
KUBERNETES_NAMESPACE=security  # (optional, default: security) K8s namespace for Flink deployments
FLINK_IMAGE=flink-sigma-detector:latest  # (optional, default: flink-sigma-detector:latest)
IMAGE_PULL_POLICY=Always  # (optional, default: Always) Image pull policy (Always, IfNotPresent, Never)

# Flink resources
FLINK_TASKMANAGER_CPU=2.0  # (optional, default: 2.0) CPU cores per TaskManager
FLINK_TASKMANAGER_MEMORY_GB=4  # (optional, default: 4) Memory per TaskManager in GB
FLINK_TASKMANAGER_SLOTS=4  # (optional, default: 4) Number of slots per TaskManager
FLINK_JOBMANAGER_CPU=1.0  # (optional, default: 1.0) CPU cores for JobManager
FLINK_JOBMANAGER_MEMORY_GB=2  # (optional, default: 2) Memory for JobManager in GB
FLINK_METRICS_POLL_INTERVAL=5.0  # (optional, default: 5.0) Flink metrics polling interval (seconds)
AUTOSCALER_QUOTA_CPU=  # (optional) CPU quota per pipeline; if unset, auto-calculated from TaskManager
AUTOSCALER_QUOTA_MEMORY_GB=  # (optional) Memory quota per pipeline in GB; if unset, auto-calculated

# Dashboard
DASHBOARD_BROADCAST_INTERVAL_SECONDS=2.0  # (optional, default: 2.0) SSE broadcast interval (seconds)
AUDIT_LOGS_RETENTION_DAYS=30  # (optional, default: 30) Audit logs retention period (days)

# Sync
ENABLE_AUTO_SYNC=true  # (optional, default: true) Enable automatic repository sync
SYNC_API_REPOS_INTERVAL_MINUTES=5  # (optional, default: 5) Sync interval in minutes
SYNC_API_REPOS_TIMEOUT_SECONDS=600  # (optional, default: 600) Sync operation timeout (seconds)

# Auth (JWT with refresh token rotation)
JWT_SECRET_KEY=your-secret-key  # (required in production) Secret key for access tokens
JWT_REFRESH_SECRET_KEY=your-refresh-secret-key  # (required in production) Secret key for refresh tokens
JWT_ALGORITHM=HS256  # (optional, default: HS256) JWT signing algorithm
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=15  # (optional, default: 15) Access token expiration (minutes)
JWT_REFRESH_TOKEN_EXPIRE_DAYS=7  # (optional, default: 7) Refresh token expiration (days)

# TDM API
TDM_API_BASE_URL=https://api.tdm.socprime.com  # (optional, default: https://api.tdm.socprime.com) SOCPrime TDM API base URL
TDM_HOSTNAME=tdm.socprime.com  # (optional, default: tdm.socprime.com)

# Logging
LOG_LEVEL=INFO  # (optional, default: INFO) Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
```

See `.env.example` for complete configuration reference.

## Development

### Project Structure

```
admin-panel-backend/
├── apps/
│   ├── core/           # Core modules (auth, database, schemas)
│   ├── routers/        # API endpoints
│   ├── managers/       # Business logic orchestrators
│   ├── modules/        # External integrations (Kafka, PostgreSQL)
│   ├── clients/        # External API clients (TDM)
│   └── services/       # Background services
├── k8s/                # Kubernetes manifests
├── server.py           # Application entry point
└── init_database.py    # Database initialization
```

### Running Tests

```bash
uv run pytest
```

### Database Migrations

```bash
# Apply all migrations
uv run alembic upgrade head

# Create new migration
uv run alembic revision --autogenerate -m "description"

# Rollback last migration
uv run alembic downgrade -1
```

## Deployment

### Docker

```bash
docker build -t etl-admin-panel .
docker run -p 8000:8000 --env-file .env etl-admin-panel
```

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Frontend  │────▶│  Admin Panel │────▶│  PostgreSQL │
└─────────────┘     │     API      │     └─────────────┘
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        ┌──────────┐ ┌──────────┐ ┌──────────┐
        │  Kafka   │ │   K8s    │ │ SOCPrime │
        │ Metrics  │ │  Flink   │ │ TDM API  │
        └──────────┘ └──────────┘ └──────────┘
```

## How Pipelines Work

### Pipeline Lifecycle

```
1. CREATE PIPELINE
   │
   ├─► Save config to PostgreSQL
   │
   └─► If enabled=true:
       └─► Create FlinkDeployment CRD in Kubernetes
           └─► Flink Operator spawns JobManager + TaskManagers

2. FLINK JOB STARTS
   │
   ├─► Consume events from source_topic (Kafka)
   │
   ├─► Consume rules/filters/parsers from sigma-rules topic (compacted)
   │
   ├─► Apply Sigma detection rules to events
   │
   └─► Write tagged events to destination_topic

3. REAL-TIME UPDATES (Hot-Reload)
   │
   ├─► Update rule/filter/parser
   │
   └─► Admin Panel publishes to sigma-rules topic
       └─► Flink picks up changes WITHOUT restart
```

### Hot-Reload vs Restart

| Change Type | Action | Downtime |
|-------------|--------|----------|
| Rules (add/update/delete) | Kafka publish | None (hot-reload) |
| Filters | Kafka publish | None (hot-reload) |
| Log Source (parser/mapping) | Kafka publish | None (hot-reload) |
| Custom Fields | Kafka publish | None (hot-reload) |
| Source/Destination Topic | Recreate FlinkDeployment | ~30s |
| Parallelism | Recreate FlinkDeployment | ~30s |
| Enable/Disable | Create/Delete FlinkDeployment | ~30s |

### Kafka Topics

| Topic | Purpose | Config |
|-------|---------|--------|
| `{source_topic}` | Input events | Regular |
| `{destination_topic}` | Tagged events output | Regular |
| `sigma-rules` | Rules, filters, parsers | Compacted, infinite retention |
| `sigma-matcher-metrics` | Pipeline metrics | Regular, 7 day retention |

### Metrics Flow

```
Flink Job                    Admin Panel
    │                            │
    │  metrics (every 30s)       │
    ├───────────────────────────►│ Kafka Consumer
    │  sigma-matcher-metrics     │     │
    │                            │     ▼
    │                            │ Dashboard Service
    │                            │     │
    │                            │     ▼
    │                            │ SSE Stream ──► Frontend
    │                            │     │
    │                            │     ▼
    │                            │ PostgreSQL (history)
```

**Key Metrics:**
- `window_total_events` - Events processed per window
- `window_matched_events` - Events that matched rules
- `window_match_rate_percent` - Detection rate
- `on_timer_duration_seconds` - Processing latency

### Resource Management

Kubernetes enforces resource limits at multiple levels:

1. **ResourceQuota** - Namespace-wide hard limits
2. **LimitRange** - Per-container defaults
3. **Flink Autoscaler** - Per-pipeline quotas


## Tech Stack

- **Framework**: FastAPI 0.121+
- **Database**: PostgreSQL + SQLAlchemy 2.0 (AsyncIO)
- **Message Queue**: Apache Kafka
- **Stream Processing**: Apache Flink
- **Container Orchestration**: Kubernetes
- **Authentication**: JWT (PyJWT)


---

**Version**: 0.9.2
