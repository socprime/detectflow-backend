import asyncio
from contextlib import asynccontextmanager

from apps.managers.sigma_validation_service import SigmaValidationService
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from apps.clients.tdm_api import (
    TdmApiBadRequestError,
    TdmApiConnectionError,
    TdmApiError,
    TdmApiNotFoundError,
    TdmApiServerError,
    TdmApiUnauthorizedError,
)
from apps.core.database import AsyncSessionLocal, get_pool_status
from apps.core.logger import get_logger, setup_uvicorn_logging
from apps.core.settings import settings
from apps.managers.activity import activity_service
from apps.managers.dashboard import dashboard_service
from apps.managers.flink_monitor import flink_monitor_service
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.activity_consumer import activity_consumer
from apps.modules.kafka.metrics import metrics_consumer
from apps.modules.postgre.audit import AuditLogDAO
from apps.routers import api_router
from apps.services.flink_metrics_poller import flink_metrics_poller
from apps.services.mapping_watcher import get_mapping_watcher
from apps.services.scheduler import get_scheduler_service

logger = get_logger(__name__)

# Background task for periodic metrics cleanup
_cleanup_task: asyncio.Task | None = None


async def _periodic_cleanup() -> None:
    """Background task that periodically cleans up old audit logs from PostgreSQL."""
    cleanup_interval = 6 * 3600  # Run every 6 hours

    while True:
        try:
            await asyncio.sleep(cleanup_interval)

            # Cleanup PostgreSQL audit_logs (old records based on retention)
            try:
                async with AsyncSessionLocal() as db:
                    try:
                        audit_dao = AuditLogDAO(db)
                        deleted = await audit_dao.cleanup_old(days=settings.audit_logs_retention_days)
                        await db.commit()
                        if deleted > 0:
                            logger.info(f"Audit logs cleanup: deleted {deleted} old records")
                    except Exception:
                        await db.rollback()
                        raise
            except Exception as e:
                logger.error(f"Error in audit logs cleanup: {e}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown."""
    global _cleanup_task

    # Filter out health check logs
    setup_uvicorn_logging()

    logger.info("Starting application services...")

    # Check and update rule loader module version
    try:
        async with AsyncSessionLocal() as db:
            validation_service = SigmaValidationService(db)
            version_updated = await validation_service.check_and_update_module_version()
            if version_updated:
                logger.info("Rule loader module version updated, pipelines marked for restart")
    except Exception as e:
        logger.error(f"Failed to check rule loader module version: {e}")
        # Continue startup - graceful degradation

    # Start Flink metrics poller (handles pipeline totals: events_tagged/events_untagged)
    try:
        await flink_metrics_poller.start()
        logger.info("Flink metrics poller started")
    except Exception as e:
        logger.error(f"Failed to start Flink metrics poller: {e}")
        # Continue without poller - graceful degradation

    # Start Kafka metrics consumer (handles per-rule match counters only)
    # This is separate from Flink poller to track which rules matched which events
    if settings.kafka_metrics_topic:
        try:
            await metrics_consumer.start()
            logger.info(
                "Kafka metrics consumer started",
                extra={"topic": settings.kafka_metrics_topic},
            )
        except Exception as e:
            logger.error(f"Failed to start Kafka metrics consumer: {e}")
            # Continue without consumer - graceful degradation
    else:
        logger.info("Kafka metrics consumer disabled (no metrics topic configured)")

    # Start Kafka activity consumer
    try:
        await activity_consumer.start()

        # Register callback to update ActivityService with recent activity
        activity_consumer.add_activity_callback(activity_service.on_activity_received)

        logger.info("Kafka activity consumer started")
    except Exception as e:
        logger.error(f"Failed to start activity consumer: {e}")
        # Continue without activity consumer - graceful degradation

    # Start dashboard service
    try:
        await dashboard_service.start()
        logger.info("Dashboard service started")
    except Exception as e:
        logger.error(f"Failed to start dashboard service: {e}")

    # Start Flink monitor service (pipeline health monitoring)
    try:
        await flink_monitor_service.start()
        logger.info("Flink monitor service started")
    except Exception as e:
        logger.error(f"Failed to start Flink monitor service: {e}")

    # Start periodic cleanup task
    _cleanup_task = asyncio.create_task(_periodic_cleanup())
    logger.info("Periodic cleanup task started")

    scheduler = get_scheduler_service()
    scheduler.start()

    mapping_watcher = get_mapping_watcher()
    mapping_watcher.start()

    logger.info("Application startup complete")

    yield  # Application is running

    # Shutdown
    logger.info("Shutting down application services...")

    # Cancel cleanup task
    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass

    # Stop dashboard service
    await dashboard_service.stop()

    # Stop Flink monitor service
    await flink_monitor_service.stop()

    # Stop Flink metrics poller
    await flink_metrics_poller.stop()

    # Stop Kafka metrics consumer
    await metrics_consumer.stop()

    # Stop Kafka activity consumer
    await activity_consumer.stop()

    # Flush activity producer
    activity_producer.close()

    await mapping_watcher.shutdown()

    scheduler.shutdown()

    logger.info("Application shutdown complete")


OPENAPI_TAGS = [
    {
        "name": "Authentication",
        "description": "User authentication and session management. Login, logout, profile updates, and password changes.",
    },
    {
        "name": "Dashboard",
        "description": "Real-time dashboard data via SSE streaming and REST snapshots. Pipeline metrics and system health.",
    },
    {
        "name": "User Management",
        "description": "Admin-only user administration. Create, update, delete users and reset passwords.",
    },
    {
        "name": "Pipelines",
        "description": "ETL pipeline management. Create, configure, and monitor Flink-based data processing pipelines.",
    },
    {
        "name": "Filters",
        "description": "Detection filter management. YAML-based prefilters for event filtering in pipelines.",
    },
    {
        "name": "Log Sources",
        "description": "Log source configuration. Parser scripts and field mappings for event processing.",
    },
    {
        "name": "Topics",
        "description": "Kafka topic discovery. View available topics and their pipeline associations.",
    },
    {
        "name": "Repositories",
        "description": "Rule repository management. Local repositories and SOCPrime TDM API integration.",
    },
    {
        "name": "Rules",
        "description": "Detection rule management. Sigma rules for event tagging in ETL pipelines.",
    },
    {
        "name": "Parsers",
        "description": "Parser testing utilities. Validate parser queries against real Kafka data.",
    },
    {
        "name": "Mapping",
        "description": "Field mapping generation. AI-assisted mapping between parsed events and Sigma fields.",
    },
]

app = FastAPI(
    title="DetectFlow Backend",
    docs_url=None,  # disable Swagger UI at /docs
    redoc_url=None,  # disable ReDoc at /redoc
    description="""
## Overview

Backend API for the ETL Pipelines Admin Panel providing real-time monitoring and management of data processing pipelines.

## Key Features

- **Real-time Dashboard** - SSE streaming for live pipeline metrics
- **Pipeline Management** - Create, configure, and monitor Flink-based ETL pipelines
- **Detection Rules** - Sigma rule management with SOCPrime TDM integration
- **Log Source Configuration** - Parser scripts and field mappings
- **User Management** - Role-based access control (Admin/User)

## Authentication

All endpoints require JWT Bearer token authentication except `/health` and `/`.
Include the token in the `Authorization` header: `Bearer <token>`
""",
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=OPENAPI_TAGS,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api_router)


@app.exception_handler(TdmApiConnectionError)
async def tdm_api_connection_handler(request: Request, exc: TdmApiConnectionError) -> JSONResponse:
    """Handle TDM API connection errors (timeout, DNS, network)."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)},
    )


@app.exception_handler(TdmApiServerError)
async def tdm_api_server_handler(request: Request, exc: TdmApiServerError) -> JSONResponse:
    """Handle TDM API server errors (5xx)."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)},
    )


@app.exception_handler(TdmApiBadRequestError)
async def tdm_api_bad_request_handler(request: Request, exc: TdmApiBadRequestError) -> JSONResponse:
    """Handle TDM API bad request errors (400) - user provided invalid data."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


@app.exception_handler(TdmApiUnauthorizedError)
async def tdm_api_unauthorized_handler(request: Request, exc: TdmApiUnauthorizedError) -> JSONResponse:
    """Handle TDM API unauthorized errors (401/403)."""
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


@app.exception_handler(TdmApiNotFoundError)
async def tdm_api_not_found_handler(request: Request, exc: TdmApiNotFoundError) -> JSONResponse:
    """Handle TDM API not found errors (404) - user requested non-existent resource."""
    # return 400 because usually it occurs because of user provided invalid data
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": str(exc)},
    )


@app.exception_handler(TdmApiError)
async def tdm_api_error_handler(request: Request, exc: TdmApiError) -> JSONResponse:
    """Handle any other TDM API errors - unexpected error from TDM API."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": str(exc)},
    )


@app.get("/")
async def root():
    return {"message": "ETL Admin Panel Backend API"}


@app.get("/health")
async def health_check():
    """Health check endpoint with service status."""
    pool_status = get_pool_status()
    return {
        "status": "healthy",
        "services": {
            "flink_metrics_poller": flink_metrics_poller.is_healthy,
            "kafka_metrics_consumer": metrics_consumer.is_healthy,
            "kafka_activity_consumer": activity_consumer.is_healthy,
            "dashboard": dashboard_service._is_running,
            "flink_monitor": flink_monitor_service._is_running,
        },
        "database_pool": pool_status,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
