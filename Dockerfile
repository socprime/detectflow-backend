# DetectFlow Backend - Dockerfile
# syntax=docker/dockerfile:1
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        postgresql-client \
        netcat-openbsd \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Create non-root user BEFORE installing dependencies
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app

# Set environment variables for uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

RUN python -m pip install --no-cache-dir setuptools==82.0.0

# Copy dependency files first (for better caching)
COPY --chown=appuser:appuser pyproject.toml uv.lock ./

# Switch to non-root user for installing dependencies
USER appuser

# Install Python dependencies using uv
RUN uv sync --frozen --no-dev --no-install-project

# Copy application code
COPY --chown=appuser:appuser . .

# Install the project itself
RUN uv sync --frozen --no-dev

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')" || exit 1

# Copy entrypoint
COPY --chown=appuser:appuser entrypoint.sh /app/entrypoint.sh

# Switch to root temporarily to make entrypoint executable
USER root
RUN chmod +x /app/entrypoint.sh
USER appuser

# Run via entrypoint (migrations + seed + server)
ENTRYPOINT ["/app/entrypoint.sh"]
