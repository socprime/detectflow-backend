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

# Start the server
echo "Starting uvicorn server..."
exec uvicorn server:app --host 0.0.0.0 --port 8000
