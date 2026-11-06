from fastapi import APIRouter

from apps.routers import (
    active_threats,
    attack_chains,
    attack_chains_runtime,
    audit,
    auth,
    correlation_job,
    correlation_pipelines,
    dashboard,
    filters,
    health_check,
    high_order_sigma,
    integrations,
    log_sources,
    mapping,
    parsers,
    pipelines,
    repositories,
    rules,
    settings,
    topics,
    user,
)

api_router = APIRouter()

api_router.include_router(auth.router)
api_router.include_router(active_threats.router)
api_router.include_router(attack_chains.router)
api_router.include_router(attack_chains_runtime.router)
api_router.include_router(correlation_job.router)
api_router.include_router(correlation_pipelines.router)
api_router.include_router(correlation_pipelines.dashboard_router)
api_router.include_router(high_order_sigma.router)
api_router.include_router(audit.router)
api_router.include_router(dashboard.router)
api_router.include_router(pipelines.router)
api_router.include_router(topics.router)
api_router.include_router(filters.router)
api_router.include_router(log_sources.router)
api_router.include_router(repositories.router)
api_router.include_router(rules.router)
api_router.include_router(parsers.router)
api_router.include_router(user.router)
api_router.include_router(mapping.router)
api_router.include_router(health_check.router)
api_router.include_router(integrations.router)
api_router.include_router(settings.router)
