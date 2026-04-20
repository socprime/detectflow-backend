from fastapi import APIRouter

from apps.routers import (
    audit,
    auth,
    dashboard,
    filters,
    health_check,
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
api_router.include_router(settings.router)
