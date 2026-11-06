"""OpenAPI presentation layer: shared error responses, tags, ReDoc tag groups, operationIds.

Single source of truth for how the API documents itself:

- reusable ``responses={}`` building blocks for routers (``errors()``, ``err()``,
  ``UNAUTHORIZED``, ``NOT_FOUND``, ...) — all error bodies are ``ErrorResponse`` (``{"detail": str}``);
- the complete tag catalog (``OPENAPI_TAGS``) and the ReDoc sidebar grouping (``TAG_GROUPS``);
- ``generate_unique_id`` — stable, collision-free operationIds (``<tag_slug>_<function_name>``);
- ``install_tag_groups`` — wraps ``app.openapi`` to inject ``x-tagGroups`` and RBAC annotations
  derived from route auth dependencies (``require_write_access`` / ``get_current_admin_user``);
- ``api_docs_app_kwargs`` — FastAPI docs/OpenAPI URL kwargs gated by ``Settings.disable_api_docs``.

Conventions (enforced by ``tests/test_openapi_contract.py``):

- every tag used by a router MUST exist in ``OPENAPI_TAGS`` and in exactly one ``TAG_GROUPS`` group;
- every documented 4xx/5xx response (except auto-422) MUST carry a model — use the blocks below;
- never override the auto-generated 422 with ``ErrorResponse``: body/query validation errors have
  the ``HTTPValidationError`` shape (``{"detail": [{loc, msg, type}]}``). Document 422 with
  ``ErrorResponse`` only where a domain ``UnprocessableEntityError`` (``{"detail": str}``) is raised;
- write/admin routes get a 403 response and a ``**Requires:** …`` description line injected from
  their FastAPI auth dependency — do not hand-maintain that text on each router.
"""

from collections.abc import Callable
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute

from apps.core.auth import get_current_admin_user, require_write_access
from apps.core.schemas import ErrorResponse

# RBAC OpenAPI annotations (kept in sync with HTTPException detail in apps.core.auth).
RbacLevel = Literal["admin", "write"]

RBAC_DESCRIPTION_LINE: dict[RbacLevel, str] = {
    "admin": "**Requires:** admin access",
    "write": "**Requires:** write access (admin or user)",
}
RBAC_FORBIDDEN_DETAIL: dict[RbacLevel, str] = {
    "admin": "Admin access required",
    "write": "Write access required",
}
_ERROR_RESPONSE_REF: dict[str, Any] = {"$ref": "#/components/schemas/ErrorResponse"}

# =============================================================================
# Reusable error-response blocks for router `responses={}` declarations
# =============================================================================

UNAUTHORIZED = {401: {"model": ErrorResponse, "description": "Not authenticated"}}
FORBIDDEN = {403: {"model": ErrorResponse, "description": "Insufficient permissions"}}
NOT_FOUND = {404: {"model": ErrorResponse, "description": "Resource not found"}}
CONFLICT = {409: {"model": ErrorResponse, "description": "Resource conflict"}}
BAD_REQUEST = {400: {"model": ErrorResponse, "description": "Bad request"}}
BAD_GATEWAY = {502: {"model": ErrorResponse, "description": "Error from upstream service"}}
SERVICE_UNAVAILABLE = {503: {"model": ErrorResponse, "description": "Service temporarily unavailable"}}


def err(code: int, description: str) -> dict[int, dict[str, Any]]:
    """One error-response entry with a custom description and the standard ``ErrorResponse`` body."""
    return {code: {"model": ErrorResponse, "description": description}}


def errors(*blocks: dict[int, dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """Merge error-response blocks into one ``responses={}`` dict; later blocks override by status code.

    Usage::

        responses=errors(UNAUTHORIZED, err(404, "Filter not found"))
    """
    merged: dict[int, dict[str, Any]] = {}
    for block in blocks:
        merged.update(block)
    return merged


# Documents the Set-Cookie side effect of login/refresh/logout in `responses[<code>]["headers"]`.
SET_COOKIE_AUTH = {
    "Set-Cookie": {
        "description": (
            "Sets (or clears, on logout) the HttpOnly `access_token` and `refresh_token` cookies "
            "(Secure, SameSite=Strict). `access_token` authenticates SSE endpoints; "
            "`refresh_token` is used by `POST /api/v1/auth/refresh`."
        ),
        "schema": {"type": "string"},
    }
}

# =============================================================================
# Tag catalog and ReDoc grouping
# =============================================================================

OPENAPI_TAGS = [
    # --- Platform ---
    {
        "name": "Authentication",
        "description": "User authentication and session management. Login, logout, profile updates, and password changes.",
    },
    {
        "name": "User Management",
        "description": "Admin-only user administration. Create, update, delete users and reset passwords.",
    },
    {
        "name": "Audit Logs",
        "description": "Audit trail of user and system actions: filterable list, filter options, and CSV export.",
    },
    {
        "name": "Health Check",
        "description": "Service liveness and integration health: latest persisted platform check results and on-demand re-checks.",
    },
    {
        "name": "Settings",
        "description": "Global Flink defaults applied to new pipelines: read/update values and the editable parameter schema.",
    },
    # --- ETL Pipelines ---
    {
        "name": "Pipelines",
        "description": "ETL pipeline management. Create, configure, and monitor Flink-based data processing pipelines.",
    },
    {
        "name": "Topics",
        "description": "Kafka topic discovery. View available topics and their pipeline associations.",
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
        "name": "Repositories",
        "description": "Rule repository management. Local, SOCPrime TDM API, GitHub, and GitLab integration-owned repositories; sync trigger and status.",
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
    # --- Attack Chains ---
    {
        "name": "Active threats",
        "description": "TDM active-threat catalog: list, detail (high-order Sigmas + deduped linked rules), bulk enable/disable.",
    },
    {
        "name": "Attack chains",
        "description": "Correlated attack chains: paginated/grouped list, list invalidation SSE stream, filter options, detail with MITRE graph, status workflow.",
    },
    {
        "name": "Attack chains runtime",
        "description": "Start/stop the Attack Chains correlation runtime (correlation-job and agent-job Flink deployments).",
    },
    {
        "name": "Correlation job",
        "description": (
            "DEPRECATED singleton correlation configuration. "
            "Superseded by Correlation pipelines. The platform graph is "
            "``/api/v1/correlation_dashboard``."
        ),
    },
    {
        "name": "Correlation pipelines",
        "description": (
            "Per-customer correlation pipelines: CRUD, duplicate, and composite run/stop of the "
            "correlation-job + agent-job pair. The platform-wide graph is "
            "``/api/v1/correlation_dashboard`` (snapshot + SSE)."
        ),
    },
    {
        "name": "High-order Sigmas",
        "description": "High-order Sigma catalog: paginated list, GET by UUID, and rule body updates.",
    },
    # --- Integrations ---
    {
        "name": "Integrations",
        "description": "SIEM/EDR/TDM integration profiles: CRUD, connection checks, rule deployment, search, and View-in-SIEM deep links.",
    },
    # --- Dashboard ---
    {
        "name": "Dashboard",
        "description": "Real-time dashboard data via SSE streaming and REST snapshots. Pipeline metrics and system health.",
    },
]

# ReDoc sidebar grouping (`x-tagGroups` vendor extension); Swagger UI ignores it but
# follows the OPENAPI_TAGS order above, which mirrors the same grouping.
TAG_GROUPS = [
    {"name": "Platform", "tags": ["Authentication", "User Management", "Audit Logs", "Health Check", "Settings"]},
    {
        "name": "ETL Pipelines",
        "tags": ["Pipelines", "Topics", "Filters", "Log Sources", "Repositories", "Rules", "Parsers", "Mapping"],
    },
    {
        "name": "Attack Chains",
        "tags": [
            "Active threats",
            "Attack chains",
            "Attack chains runtime",
            "Correlation job",
            "Correlation pipelines",
            "High-order Sigmas",
        ],
    },
    {"name": "Integrations", "tags": ["Integrations"]},
    {"name": "Dashboard", "tags": ["Dashboard"]},
]

# =============================================================================
# operationIds and schema post-processing
# =============================================================================


def generate_unique_id(route: APIRoute) -> str:
    """operationId = ``<first-tag slug>_<endpoint function name>``.

    Tag-prefixing keeps operationIds unique across routers (e.g. ``get_filter_options``
    exists in both the Audit Logs and Attack chains routers) and yields readable method
    names in generated client SDKs.
    """
    tag = str(route.tags[0]) if route.tags else "default"
    slug = tag.lower().replace(" ", "_").replace("-", "_")
    return f"{slug}_{route.name}"


def _iter_dependant_calls(dependant: Dependant) -> set[Callable[..., Any]]:
    """Collect every callable in a FastAPI dependency tree (including nested deps)."""
    found: set[Callable[..., Any]] = set()
    stack = [dependant]
    while stack:
        current = stack.pop()
        if current.call is not None:
            found.add(current.call)
        stack.extend(current.dependencies)
    return found


def rbac_level_for_route(route: APIRoute) -> RbacLevel | None:
    """Return the RBAC level implied by the route's auth dependencies, if any.

    Admin wins when both markers are present (defensive; routers should not stack them).
    """
    calls = _iter_dependant_calls(route.dependant)
    if get_current_admin_user in calls:
        return "admin"
    if require_write_access in calls:
        return "write"
    return None


def _annotate_operation_rbac(operation: dict[str, Any], level: RbacLevel) -> None:
    """Inject a Requires line and a modeled 403 response onto one OpenAPI operation."""
    requires_line = RBAC_DESCRIPTION_LINE[level]
    description = operation.get("description") or ""
    if requires_line not in description:
        operation["description"] = f"{requires_line}\n\n{description}" if description else requires_line

    responses = operation.setdefault("responses", {})
    responses["403"] = {
        "description": RBAC_FORBIDDEN_DETAIL[level],
        "content": {"application/json": {"schema": dict(_ERROR_RESPONSE_REF)}},
    }


def annotate_rbac_in_schema(app: FastAPI, schema: dict[str, Any]) -> None:
    """Annotate OpenAPI operations from ``require_write_access`` / ``get_current_admin_user``.

    Mutates ``schema`` in place. Matching is by ``path`` + HTTP method against ``APIRoute``
    entries that are included in the schema.
    """
    paths = schema.get("paths") or {}
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        level = rbac_level_for_route(route)
        if level is None:
            continue
        path_item = paths.get(route.path)
        if not path_item:
            continue
        for method in route.methods or ():
            operation = path_item.get(method.lower())
            if isinstance(operation, dict):
                _annotate_operation_rbac(operation, level)


def api_docs_app_kwargs(enabled: bool) -> dict[str, str | None]:
    """Return FastAPI constructor kwargs for Swagger UI, ReDoc, and OpenAPI JSON routes.

    When ``enabled`` is False, all three URLs are ``None`` so FastAPI does not mount
    ``/docs``, ``/redoc``, or ``/openapi.json``.
    """
    if not enabled:
        return {"docs_url": None, "redoc_url": None, "openapi_url": None}
    return {"docs_url": "/docs", "redoc_url": "/redoc", "openapi_url": "/openapi.json"}


def install_tag_groups(app: FastAPI) -> None:
    """Wrap ``app.openapi`` to inject ``x-tagGroups`` and RBAC docs into the schema.

    FastAPI caches the schema on ``app.openapi_schema``; the original ``app.openapi``
    populates that cache, so the extensions are added exactly once and reused afterwards.
    """
    original_openapi = app.openapi

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = original_openapi()
        schema["x-tagGroups"] = TAG_GROUPS
        annotate_rbac_in_schema(app, schema)
        return schema

    app.openapi = custom_openapi
