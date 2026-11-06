# Prime Detect Backend
[![Version](https://img.shields.io/badge/version-2.3.2-blue.svg)](pyproject.toml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.121%2B-green.svg)](https://fastapi.tiangolo.com/)

Backend API for managing real-time detection pipelines with Sigma rules, built on Apache Flink and Kafka.
This project is a component of Prime Detect OSS by SOC Prime. See its [README](https://github.com/socprime/detectflow-main) for more details and instructions.

## Features

- **Pipeline management** — create, configure and monitor Flink detection pipelines running [Prime Detect Match Node](https://github.com/socprime/detectflow-matchnode); each pipeline writes to up to 10 output topics with an optional Sigma severity filter.
- **Flink runtimes** — Kubernetes (Flink Operator) or Confluent Manager for Apache Flink (CMF).
- **Detection content** — Sigma rules, filters, log source parsers and field mappings, hot-reloaded into running pipelines without a restart; AI-assisted field mapping (requires the SOC Prime Platform integration).
- **Rule repositories** — sync from SOC Prime Platform (API key required), public GitHub repositories (SigmaHQ, Microsoft Sentinel, Splunk, Elastic) and your own GitLab repositories.
- **Attack Chains** — Active Threats and High-Order Sigma catalogs, correlation pipelines and the AI agent job that turn tagged events into attack chains.
- **SIEM links** — open an attack chain's events in Microsoft Sentinel, Splunk, Elastic Stack or OpenSearch ("view in SIEM").
- **Real-time dashboard** — live metrics and pipeline status over SSE.
- **Health checks** — PostgreSQL, Kafka and rule repositories, on demand and on a schedule.

## Requirements

- Python 3.12+
- PostgreSQL 14+
- Kafka cluster
- One of the Flink runtimes:
  - Kubernetes cluster with Flink Operator (default)
  - Confluent Manager for Apache Flink (CMF)

## Quick Start

```bash
uv sync
cp .env.example .env    # then fill in the settings below

uv run alembic upgrade heads                       # apply migrations
uv run python -m apps.modules.kafka.topic_init     # create Kafka topics
uv run python -m apps.modules.postgre.init_admin   # create the first admin user

uv run uvicorn server:app --host 0.0.0.0 --port 8000
```

The first admin is `admin@soc.local` / `admin`; the password must be changed on first login.

- **API**: http://localhost:8000
- **Swagger UI**: http://localhost:8000/docs (off by default; enable with `DISABLE_API_DOCS=false`)

## Configuration

Copy `.env.example` to `.env` and fill in:

- **Database**: `DATABASE_URL` (required), e.g. `postgresql+asyncpg://user:pass@localhost:5432/detectflow`.
- **Kafka**: `KAFKA_BOOTSTRAP_SERVERS` (required), `KAFKA_AUTH_METHOD` (`PLAINTEXT` / `SASL` / `SSL`) and the matching credentials (`KAFKA_API_KEY` / `KAFKA_API_SECRET` for SASL, `KAFKA_SSL_*` certificate paths for SSL).
- **Auth**: `JWT_SECRET_KEY`, `JWT_REFRESH_SECRET_KEY` — always set your own in production.
- **Flink runtime**: `FLINK_PROVIDER` (`kubernetes` / `cmf`).
  - Kubernetes: `KUBERNETES_NAMESPACE`, `FLINK_IMAGE`.
  - CMF: `CMF_URL`, `CMF_ENVIRONMENT`, `CMF_NAMESPACE`, `FLINK_IMAGE_CMF`.
- **Attack Chains images**: `CORRELATION_JOB_IMAGE`, `AGENT_JOB_IMAGE` (Kubernetes).

All other settings have sensible defaults; see the comments in `.env.example`.

## Running with Docker

```bash
docker build -t detectflow-backend .
docker run -p 8000:8000 --env-file .env detectflow-backend
```

The container entrypoint applies migrations, creates Kafka topics and the first admin user, then starts the API.

## Project Structure

```
├── apps/
│   ├── core/           # Settings, auth, database, schemas
│   ├── routers/        # API endpoints
│   ├── managers/       # Business logic
│   ├── repositories/   # Database access
│   ├── modules/        # Kafka and PostgreSQL helpers
│   ├── clients/        # External API clients (Flink REST, CMF, GitHub, GitLab)
│   ├── providers/      # Flink runtime providers (Kubernetes, CMF)
│   ├── integrations/   # Integration providers (SOC Prime Platform, GitHub, GitLab, SIEMs)
│   ├── services/       # Background services (scheduler, health checks, metrics)
│   └── templates/      # Flink deployment templates
├── alembic/            # Database migrations
├── content/            # Bundled SOC Prime content (Active Threats, HoS, rules), see content/README.md
├── scripts/            # health_check.py, reset_password.py
├── tests/
├── entrypoint.sh       # Container bootstrap
└── server.py           # Application entry point
```

## How Pipelines Work

1. **Create a pipeline** — the configuration is saved to PostgreSQL; an enabled pipeline gets a Flink deployment.
2. **The job runs** — it reads events from the source topic, applies the Sigma rules, filters and parsers from `detectflow.sigma-rules`, and writes results to the pipeline's output topics.
3. **Content changes** — rules, filters, parsers and mappings are published to `detectflow.sigma-rules` and picked up by running jobs without a restart. Changing topics, parallelism or enabling/disabling a pipeline redeploys it (~30s).
