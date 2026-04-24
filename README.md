# DetectFlow Backend
[![Version](https://img.shields.io/badge/version-1.1.0-blue.svg)](VERSION)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.121.3-green.svg)](https://fastapi.tiangolo.com/)

Backend API for managing real-time pipelines with Sigma detection rules, built on Apache Flink and Kafka.
This project is a component of SOC Prime DetectFlow OSS. See its [README](https://github.com/socprime/detectflow-main) for more details and instructions.

## Overview

This service provides a REST API for:
- Creating and managing pipelines that process events from Kafka
- Applying Sigma detection rules for security event tagging
- Real-time dashboard with SSE streaming
- SOC Prime Platform integration (API key required)
- GitHub public open-source repositories, including SigmaHQ, Microsoft, Splunk, and Elastic for rule synchronization

## Features

- **Pipeline Management** - Create, configure, and monitor Flink-based ETL pipelines
- **Pluggable Flink Provider** - Choose between Kubernetes (Flink Operator) or CMF (Confluent Manager for Apache Flink)
- **Real-time Dashboard** - SSE streaming for live metrics and pipeline status
- **Detection Rules** - Sigma rule management
- **Log Source Configuration** - Parser scripts and field mappings
- **AI-assisted Mapping** - Generate field mappings using AI prompts
- **JWT Authentication** - Access + refresh token rotation for security
- **Database Migrations** - Alembic for schema version control
- **Kubernetes Native** - Automatic FlinkDeployment CRD management
- **Flink Health Monitoring** - Automatic pipeline status tracking
- **Platform Health Checks** - PostgreSQL, Kafka, and Cloud Repositories status with scheduled checks

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 14+
- Kafka cluster
- One of the following Flink runtimes:
  - Kubernetes cluster with Flink Operator (default provider)
  - Confluent Manager for Apache Flink (CMF) endpoint

### Installation

```bash
# Clone repository
git clone <repository-url>
cd detectflow-backend

# Install dependencies
uv sync

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Run database migrations (creates schema on first run)
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
DATABASE_POOL_SIZE=20  # (optional, default: 20) Permanent connections in pool
DATABASE_MAX_OVERFLOW=30  # (optional, default: 30) Extra connections when pool exhausted
DATABASE_POOL_RECYCLE=1800  # (optional, default: 1800) Recycle connections after N seconds
DATABASE_POOL_TIMEOUT=30  # (optional, default: 30) Timeout waiting for connection from pool
DATABASE_ECHO=false  # (optional, default: false) Log SQL queries

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

# Flink provider selection
FLINK_PROVIDER=kubernetes  # (optional, default: kubernetes) Flink runtime: "kubernetes" or "cmf"

# Kubernetes (used when FLINK_PROVIDER=kubernetes)
KUBERNETES_NAMESPACE=security  # (optional, default: security) K8s namespace for Flink deployments
FLINK_IMAGE=flink-sigma-detector:latest  # (optional, default: flink-sigma-detector:latest)
FLINK_IMAGE_CMF=  # (optional) Docker image override for CMF provider (falls back to FLINK_IMAGE)
IMAGE_PULL_POLICY=Always  # (optional, default: Always) Image pull policy (Always, IfNotPresent, Never)
FLINK_NODE_SELECTOR_KEY=app  # (optional, default: app) nodeSelector key for Flink pods
FLINK_NODE_SELECTOR_VALUE=detectflow-prod  # (optional, default: detectflow-prod) nodeSelector value

# Flink state storage PVCs (Kubernetes provider)
FLINK_CHECKPOINTS_PVC=flink-checkpoints-pvc  # (optional) PVC for Flink checkpoints
FLINK_HA_PVC=flink-ha-pvc  # (optional) PVC for Flink HA metadata
FLINK_SAVEPOINTS_PVC=flink-savepoints-pvc  # (optional) PVC for Flink savepoints

# CMF — Confluent Manager for Apache Flink (required when FLINK_PROVIDER=cmf)
CMF_URL=  # (required for CMF) CMF API URL, e.g. http://cmf-service.confluent:80
CMF_ENVIRONMENT=  # (required for CMF) CMF environment name
CMF_NAMESPACE=  # (required for CMF) K8s namespace where CMF deploys Flink apps
CMF_CLIENT_CERT_PATH=  # (optional) mTLS client certificate
CMF_CLIENT_KEY_PATH=  # (optional) mTLS client key
CMF_CA_CERT_PATH=  # (optional) CA certificate

# Flink resources
FLINK_TASKMANAGER_CPU=1.0  # (optional, default: 1.0) CPU cores per TaskManager
FLINK_TASKMANAGER_MEMORY_GB=2  # (optional, default: 2) Memory per TaskManager in GB
FLINK_JOBMANAGER_CPU=1.0  # (optional, default: 1.0) CPU cores for JobManager
FLINK_JOBMANAGER_MEMORY_GB=2  # (optional, default: 2) Memory for JobManager in GB
FLINK_METRICS_POLL_INTERVAL=5.0  # (optional, default: 5.0) Flink metrics polling interval (seconds)
AUTOSCALER_QUOTA_CPU=  # (optional) CPU quota per pipeline; if unset, auto-calculated from TaskManager
AUTOSCALER_QUOTA_MEMORY_GB=  # (optional) Memory quota per pipeline in GB; if unset, auto-calculated

# Dashboard
DASHBOARD_BROADCAST_INTERVAL_SECONDS=2.0  # (optional, default: 2.0) SSE broadcast interval (seconds)
AUDIT_LOGS_RETENTION_DAYS=30  # (optional, default: 30) Audit logs retention period (days)

# Sync / scheduled tasks
ENABLE_AUTO_SYNC=true  # (optional, default: true) Enable scheduler (repo sync + health checks)
SYNC_API_REPOS_INTERVAL_MINUTES=5  # (optional, default: 5) Repository sync interval in minutes
SYNC_API_REPOS_TIMEOUT_SECONDS=600  # (optional, default: 600) Sync operation timeout (seconds)
HEALTH_CHECK_INTERVAL_MINUTES=5  # (optional, default: 5) Scheduled health_check check_all interval

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
detectflow-backend/
├── apps/
│   ├── core/           # Core modules (auth, database, schemas, version)
│   ├── routers/        # API endpoints
│   ├── managers/       # Business logic orchestrators
│   ├── modules/        # External integrations (Kafka, PostgreSQL)
│   ├── clients/        # External API clients (TDM, CMF, Flink REST)
│   ├── providers/      # Flink runtime providers (Kubernetes, CMF)
│   ├── services/       # Background services (scheduler, health checks, metrics poller)
│   └── templates/      # Jinja2 templates (FlinkDeployment, CMF application)
├── alembic/            # Database migrations
├── k8s/                # Kubernetes manifests
├── scripts/            # Operational scripts (health_check, …)
└── server.py           # Application entry point
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
docker build -t detectflow-backend .
docker run -p 8000:8000 --env-file .env detectflow-backend
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

**Version**: 1.1.0
