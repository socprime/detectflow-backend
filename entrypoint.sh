#!/bin/bash
set -e

echo "=== DetectFlow Backend Startup ==="

# Run database migrations
echo "Running database migrations..."
alembic upgrade heads
echo "Migrations completed!"

# Initialize Kafka topics
echo "Initializing Kafka topics..."
python -m apps.modules.kafka.topic_init
echo "Kafka topics initialized!"

# Create default admin user if no users exist
python -m apps.modules.postgre.init_admin

# Load the bundled SOC Prime content (detection_content/) once per snapshot version. Never fails startup.
python -m apps.modules.postgre.init_content

# Start the server.
# UVICORN_WORKERS defaults to 1 — identical behavior to the classic single-process
# deployment (right default for installs with a handful of users).
# For high-traffic instances set UVICORN_WORKERS to the CPU count: bcrypt logins
# and JSON serialization are CPU-bound, so worker parallelism = cores. Singleton
# background services (pollers, scheduler, metrics consumer, ...) run in ONE
# elected worker only — see apps/core/leader.py.
# NOTE: the SQLAlchemy pool is per worker: total DB connections can reach
# workers × (database_pool_size + database_max_overflow). Lower DATABASE_POOL_SIZE /
# DATABASE_MAX_OVERFLOW accordingly when raising the worker count.
WORKERS="${UVICORN_WORKERS:-1}"
echo "Starting uvicorn server with ${WORKERS} worker(s)..."
exec uvicorn server:app --host 0.0.0.0 --port 8000 --workers "${WORKERS}"
