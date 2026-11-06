import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from apps.core.database import AsyncSessionLocal, get_pool_status
from apps.core.leader import LeaderElector
from apps.core.logger import get_logger, setup_uvicorn_logging
from apps.core.openapi import OPENAPI_TAGS, api_docs_app_kwargs, generate_unique_id, install_tag_groups
from apps.core.schemas import HealthResponse, RootResponse
from apps.core.settings import settings
from apps.integrations.providers.socprime_tdm import (
    TdmApiBadRequestError,
    TdmApiConnectionError,
    TdmApiError,
    TdmApiNotFoundError,
    TdmApiServerError,
    TdmApiUnauthorizedError,
)
from apps.managers.activity import activity_service
from apps.managers.correlation_dashboard import correlation_dashboard_broadcaster
from apps.managers.dashboard import dashboard_service
from apps.managers.flink_monitor import flink_monitor_service
from apps.managers.sigma_validation_service import SigmaValidationService
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.activity_consumer import activity_consumer
from apps.modules.kafka.chains import chains_analysed_consumer
from apps.modules.kafka.metrics import metrics_consumer
from apps.modules.kafka.topic_lifecycle import tagged_events_topic_for, topic_lifecycle_service
from apps.modules.postgre.attack_chains import ChainsAnalysedDeadLetterDAO
from apps.modules.postgre.audit import AuditLogDAO
from apps.modules.postgre.correlation_pipeline import CorrelationPipelineDAO
from apps.modules.postgre.pipeline import PipelineDAO
from apps.routers import api_router
from apps.services.flink_metrics_poller import flink_metrics_poller
from apps.services.mapping_watcher import get_mapping_watcher
from apps.services.scheduler import get_scheduler_service

logger = get_logger(__name__)

# Background task for periodic metrics cleanup
_cleanup_task: asyncio.Task | None = None

# Elected once per process; only the leader runs the singleton services below.
leader_elector: LeaderElector | None = None


async def _periodic_cleanup() -> None:
    """Background task that periodically prunes old audit logs and parked chains-analysed records."""
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

            # Prune parked chains-analysed poison records. Own try/except so a failure here can
            # never skip the audit-log prune above.
            try:
                async with AsyncSessionLocal() as db:
                    try:
                        dlq_dao = ChainsAnalysedDeadLetterDAO(db)
                        pruned = await dlq_dao.prune(settings.kafka_chains_analysed_dead_letter_retention_days)
                        await db.commit()
                        if pruned > 0:
                            logger.info(f"Chains-analysed dead letters cleanup: deleted {pruned} old records")
                    except Exception:
                        await db.rollback()
                        raise
            except Exception as e:
                logger.error(f"Error in chains-analysed dead letters cleanup: {e}")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")


async def _reconcile_kafka_topics() -> None:
    """Provision the per-entity Kafka topics every persisted row owns.

    ``topic_init`` only creates the static shared topics, and per-entity topics are created by the
    manager that creates the row — so a DB restored against a fresh cluster (or a topic deleted out
    of band) would leave a pipeline pointing at a topic that does not exist. Idempotent: existing
    topics are left untouched, partition drift is warned about, never altered.
    """
    async with AsyncSessionLocal() as db:
        owned_topics = await PipelineDAO(db).get_tagged_events_topics()
        correlation_pipeline_ids = sorted(await CorrelationPipelineDAO(db).list_ids())

    for pipeline_id, stored_topic in owned_topics.items():
        expected = tagged_events_topic_for(pipeline_id)
        if stored_topic != expected:
            logger.warning(
                "Pipeline tagged-events topic does not match its id — reconciling the generated name",
                extra={"pipeline_id": str(pipeline_id), "stored": stored_topic, "expected": expected},
            )

    created = await topic_lifecycle_service.reconcile(owned_topics.keys(), correlation_pipeline_ids)
    logger.info(
        "Kafka topic reconcile complete",
        extra={
            "pipelines": len(owned_topics),
            "correlation_pipelines": len(correlation_pipeline_ids),
            "created": created,
        },
    )


async def _start_singleton_services() -> None:
    """Start the services that must run in exactly ONE worker process.

    With ``uvicorn --workers N`` these would otherwise run N times: the metrics
    consumer subscribes with a unique Kafka group per instance and writes
    counters to Postgres (N× double-counting), the scheduler would fire every
    periodic task N times, etc. The LeaderElector (Postgres advisory lock)
    guarantees a single runner; on leader crash another worker takes over.
    """
    global _cleanup_task

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

    # Reconcile per-entity Kafka topics (never fatal — a dead broker must not block startup).
    # Leader-only: this CREATES topics, so N workers would race on the same AdminClient calls.
    try:
        await _reconcile_kafka_topics()
    except Exception as e:
        logger.error(f"Failed to reconcile per-pipeline Kafka topics: {e}")
        # Continue startup - graceful degradation

    # Start Kafka metrics consumer (handles per-rule match counters only).
    # Unique Kafka group per instance + DB writes → strictly leader-only.
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

    # Start Flink monitor service (pipeline health monitoring)
    try:
        await flink_monitor_service.start()
        logger.info("Flink monitor service started")
    except Exception as e:
        logger.error(f"Failed to start Flink monitor service: {e}")

    # Start periodic cleanup task
    _cleanup_task = asyncio.create_task(_periodic_cleanup())
    logger.info("Periodic cleanup task started")

    try:
        get_scheduler_service().start()
    except Exception as e:
        logger.error(f"Failed to start scheduler service: {e}")

    try:
        get_mapping_watcher().start()
    except Exception as e:
        logger.error(f"Failed to start mapping watcher: {e}")

    logger.info("Singleton services started (this worker is the leader)")


async def _stop_singleton_services() -> None:
    """Stop everything `_start_singleton_services` started (leader shutdown/demotion)."""
    global _cleanup_task

    if _cleanup_task:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except asyncio.CancelledError:
            pass
        _cleanup_task = None

    await flink_monitor_service.stop()

    await metrics_consumer.stop()

    await get_mapping_watcher().shutdown()

    get_scheduler_service().shutdown()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle - startup and shutdown.

    Two service tiers:
    - **per-worker** (started here unconditionally): dashboard SSE service, the
      Flink metrics poller (in-process EPS cache — every worker that serves
      snapshot/SSE must poll, or followers return 0 EPS), the activity consumer
      (unique Kafka group — every worker needs the in-process recent-activity
      cache for its own SSE clients; only the leader persists events to
      audit_logs, see ``set_persist_predicate``), and the chains-analysed
      consumer (shared Kafka group — partitions are balanced across workers,
      each message handled once).
    - **singleton** (leader only, via `_start_singleton_services`): Kafka
      metrics consumer, Flink monitor, scheduler, mapping watcher, periodic
      cleanup.
    """
    global leader_elector

    # Filter out health check logs
    setup_uvicorn_logging()

    logger.info(f"Starting DetectFlow Backend {settings.detectflow_backend_version}")

    if "*" in settings.cors_allow_origins:
        logger.warning("CORS_ALLOW_ORIGINS contains '*' — ignored, credentialed requests need exact origins")
    logger.info(f"CORS allowed origins: {settings.cors_allow_origins_list or 'none (same-origin only)'}")

    # Start Kafka chains-analysed consumer (LLM-enriched attack chains).
    # Shared consumer group: safe on every worker, Kafka shards the partitions.
    if settings.kafka_chains_analysed_enabled and settings.kafka_chains_analysed_topic:
        try:
            await chains_analysed_consumer.start()
            logger.info(
                "Kafka chains-analysed consumer started",
                extra={"pattern": chains_analysed_consumer.pattern or settings.kafka_chains_analysed_topic},
            )
        except Exception as e:
            logger.error(f"Failed to start Kafka chains-analysed consumer: {e}")

    # Elect the singleton-services leader (Postgres advisory lock) BEFORE the
    # activity consumer starts, so in the common single-worker deployment the
    # leader is already known and audit-event persistence is active from the
    # first consumed message. In a single-worker deployment this worker simply
    # wins immediately.
    leader_elector = LeaderElector(on_promote=_start_singleton_services, on_demote=_stop_singleton_services)
    await leader_elector.start()

    # Start Kafka activity consumer (every worker: feeds the in-process
    # recent-activity cache used by this worker's dashboard SSE clients).
    # DB persistence of events is gated to the leader to avoid duplicate rows;
    # before the election resolves, persistence stays ON (better a rare
    # duplicate than a silently dropped audit event).
    try:
        activity_consumer.set_persist_predicate(lambda: leader_elector is None or leader_elector.is_leader)
        await activity_consumer.start()

        # Register callback to update ActivityService with recent activity
        activity_consumer.add_activity_callback(activity_service.on_activity_received)

        logger.info("Kafka activity consumer started")
    except Exception as e:
        logger.error(f"Failed to start activity consumer: {e}")
        # Continue without activity consumer - graceful degradation

    # Start Flink metrics poller (per worker: fills this process's EPS cache
    # that dashboard snapshot/SSE reads). Read-only against Flink REST — safe
    # on every replica; must not be leader-only or followers serve 0 EPS.
    try:
        await flink_metrics_poller.start()
        logger.info("Flink metrics poller started")
    except Exception as e:
        logger.error(f"Failed to start Flink metrics poller: {e}")

    # Start dashboard service (per worker — serves this worker's SSE clients)
    try:
        await dashboard_service.start()
        logger.info("Dashboard service started")
    except Exception as e:
        logger.error(f"Failed to start dashboard service: {e}")

    logger.info("Application startup complete")

    yield  # Application is running

    # Shutdown
    logger.info("Shutting down application services...")

    # Stop the singleton services BEFORE releasing the leader lock: releasing
    # first would let a standby worker start its own metrics consumer while
    # ours is still draining (briefly two writers).
    if leader_elector is not None and leader_elector.is_leader:
        await _stop_singleton_services()
    if leader_elector is not None:
        await leader_elector.stop()

    # Stop dashboard service (cancels its SSE broadcaster refresher)
    await dashboard_service.stop()
    await correlation_dashboard_broadcaster.stop()

    await flink_metrics_poller.stop()

    # Stop Kafka consumers (reverse of startup order)
    await activity_consumer.stop()
    await chains_analysed_consumer.stop()

    # Flush activity producer
    activity_producer.close()

    logger.info("Application shutdown complete")


app = FastAPI(
    title="DetectFlow Backend",
    summary="Control-plane REST API for DetectFlow ETL: Sigma-rule pipelines on Flink, TDM sync, Attack Chains, and a real-time SSE dashboard.",
    description="""
## Overview

Backend API for DetectFlow providing real-time monitoring and management of data processing pipelines.

## Key Features

- **Real-time Dashboard** - SSE streaming for live pipeline metrics
- **Pipeline Management** - Create, configure, and monitor Flink-based ETL pipelines
- **Detection Rules** - Sigma rule management with SOCPrime TDM integration
- **Attack Chains** - correlation runtime, active-threat catalog, and chain review workflow
- **Integrations** - SIEM/EDR profiles with connection checks, rule deployment, and View-in-SIEM links
- **Log Source Configuration** - Parser scripts and field mappings
- **User Management** - Role-based access control (Admin/User)

## Authentication

All endpoints require JWT Bearer token authentication except `GET /`, `GET /health`, and
`POST /api/v1/auth/{login,refresh,logout}`. Include the token in the `Authorization` header:
`Bearer <token>`.

**SSE endpoints** (`/api/v1/dashboard/stream`, `/api/v1/correlation_dashboard/stream`,
`/api/v1/attack_chains/chains/stream`) authenticate
via the HttpOnly `access_token` cookie set by `POST /api/v1/auth/login` (the `CookieAuth` security
scheme), because `EventSource` cannot send an `Authorization` header.

## Errors

All errors use the shape `{"detail": "<message>"}`. Request-validation errors (HTTP 422) use the
FastAPI shape `{"detail": [{"loc": [...], "msg": "...", "type": "..."}]}`.
""",
    version=settings.detectflow_backend_version,
    contact={"name": "SOC Prime", "url": "https://socprime.com"},
    lifespan=lifespan,
    openapi_tags=OPENAPI_TAGS,
    generate_unique_id_function=generate_unique_id,
    swagger_ui_parameters={
        "persistAuthorization": True,
        "displayRequestDuration": True,
        "filter": True,
    },
    **api_docs_app_kwargs(enabled=not settings.disable_api_docs),
)

# CORS middleware. Origins must be listed explicitly (CORS_ALLOW_ORIGINS): responses carry
# auth cookies, so a wildcard origin would expose every session to any site the user visits.
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(api_router)

# Inject ReDoc x-tagGroups + RBAC Requires/403 annotations into OpenAPI
install_tag_groups(app)


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


@app.get("/", response_model=RootResponse, summary="Service banner", tags=["Health Check"])
async def root():
    """Public service banner; use as a minimal liveness probe."""
    return {"message": "DetectFlow Backend API"}


@app.get("/health", response_model=HealthResponse, summary="Service health", tags=["Health Check"])
async def health_check():
    """Public health report: overall status, backend version, background-service flags, and DB pool stats."""
    pool_status = get_pool_status()
    return {
        "status": "healthy",
        "detectflow_backend_version": settings.detectflow_backend_version,
        "services": {
            "is_leader": leader_elector is not None and leader_elector.is_leader,
            "flink_metrics_poller": flink_metrics_poller.is_healthy,
            "kafka_metrics_consumer": metrics_consumer.is_healthy,
            "kafka_chains_analysed_consumer": (
                chains_analysed_consumer.is_healthy
                if settings.kafka_chains_analysed_enabled and settings.kafka_chains_analysed_topic
                else None
            ),
            "kafka_activity_consumer": activity_consumer.is_healthy,
            "dashboard": dashboard_service._is_running,
            "flink_monitor": flink_monitor_service._is_running,
        },
        "database_pool": pool_status,
        # Process-local snapshot only — no Kafka round trip, no DB query (this route is public).
        "kafka_chains_analysed": (
            chains_analysed_consumer.health_snapshot()
            if settings.kafka_chains_analysed_enabled and settings.kafka_chains_analysed_topic
            else None
        ),
        "db_pool": pool_status,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
