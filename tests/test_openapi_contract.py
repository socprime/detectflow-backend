"""OpenAPI contract/lint tests — pin the API documentation conventions.

Guards the documentation the same way ``test_kafka_*_payload.py`` guard the Kafka edges:
a docs regression must break a test here, not a consumer of ``/openapi.json``.

Enforced conventions (see ``apps/core/openapi.py``):
- every operation has summary, description, and at least one tag;
- every used tag is declared in the top-level tag catalog with a description and belongs to
  exactly one ReDoc ``x-tagGroups`` group;
- operationIds are unique (``generate_unique_id``);
- every documented non-422 error response carries a JSON schema (``ErrorResponse``);
- every operation outside the public allowlist requires security (Bearer or CookieAuth);
- DELETE operations return 204 No Content;
- 100% of component-schema properties carry a ``description``;
- routes using ``require_write_access`` / ``get_current_admin_user`` document a modeled 403 and a
  ``**Requires:** …`` description line (injected by ``install_tag_groups``).
"""

import pytest
from fastapi.routing import APIRoute

from apps.core.openapi import (
    RBAC_DESCRIPTION_LINE,
    RBAC_FORBIDDEN_DETAIL,
    api_docs_app_kwargs,
    rbac_level_for_route,
)
from apps.core.settings import settings
from server import app

HTTP_METHODS = {"get", "post", "put", "patch", "delete"}

# Operations deliberately served without authentication.
PUBLIC_OPERATIONS = {
    ("/", "get"),
    ("/health", "get"),
    ("/api/v1/auth/login", "post"),
    ("/api/v1/auth/refresh", "post"),
    ("/api/v1/auth/logout", "post"),
}

# FastAPI-internal schemas exempt from the field-description lint.
EXCLUDED_SCHEMAS = {"HTTPValidationError", "ValidationError"}


@pytest.fixture(scope="module")
def spec() -> dict:
    """Build the OpenAPI schema once per module (reset the FastAPI cache first)."""
    app.openapi_schema = None
    return app.openapi()


def iter_operations(spec: dict):
    """Yield (path, method, operation) for every documented operation."""
    for path, path_item in spec["paths"].items():
        for method, operation in path_item.items():
            if method in HTTP_METHODS:
                yield path, method, operation


def test_spec_builds(spec):
    assert spec["openapi"].startswith("3.")
    assert spec["info"]["title"]
    assert spec["info"]["version"]
    assert spec["info"]["summary"]
    assert spec["paths"]


def test_every_operation_documented(spec):
    missing = []
    for path, method, op in iter_operations(spec):
        if not op.get("summary"):
            missing.append(f"{method.upper()} {path}: no summary")
        if not op.get("description"):
            missing.append(f"{method.upper()} {path}: no description")
        if not op.get("tags"):
            missing.append(f"{method.upper()} {path}: no tags")
    assert not missing, "Undocumented operations:\n" + "\n".join(missing)


def test_tags_declared_and_grouped(spec):
    declared = {t["name"]: t for t in spec.get("tags", [])}
    used = {tag for _, _, op in iter_operations(spec) for tag in op.get("tags", [])}

    undeclared = used - declared.keys()
    assert not undeclared, f"Tags used by operations but missing from OPENAPI_TAGS: {sorted(undeclared)}"

    without_description = [name for name, t in declared.items() if not t.get("description")]
    assert not without_description, f"Tags without description: {without_description}"

    groups = spec.get("x-tagGroups")
    assert groups, "x-tagGroups missing from the schema (install_tag_groups not applied)"
    grouped = [tag for group in groups for tag in group["tags"]]
    assert len(grouped) == len(set(grouped)), "A tag appears in more than one x-tagGroups group"
    ungrouped = declared.keys() - set(grouped)
    assert not ungrouped, f"Declared tags missing from x-tagGroups: {sorted(ungrouped)}"
    unknown = set(grouped) - declared.keys()
    assert not unknown, f"x-tagGroups references undeclared tags: {sorted(unknown)}"


def test_operation_ids_unique(spec):
    ids = [op["operationId"] for _, _, op in iter_operations(spec)]
    duplicates = {i for i in ids if ids.count(i) > 1}
    assert not duplicates, f"Duplicate operationIds: {sorted(duplicates)}"


def test_error_responses_have_schema(spec):
    """Every documented >=400 response (except auto-422) must carry a JSON schema."""
    missing = []
    for path, method, op in iter_operations(spec):
        for code, response in op.get("responses", {}).items():
            if not code.isdigit() or int(code) < 400 or code == "422":
                continue
            schema = response.get("content", {}).get("application/json", {}).get("schema")
            if not schema:
                missing.append(f"{method.upper()} {path} -> {code}")
    assert not missing, "Error responses without a schema:\n" + "\n".join(missing)


def test_security_required_outside_allowlist(spec):
    unsecured = [
        f"{method.upper()} {path}"
        for path, method, op in iter_operations(spec)
        if (path, method) not in PUBLIC_OPERATIONS and not op.get("security")
    ]
    assert not unsecured, "Operations without a security requirement:\n" + "\n".join(unsecured)


def test_security_schemes_declared(spec):
    schemes = spec.get("components", {}).get("securitySchemes", {})
    assert "HTTPBearer" in schemes, "Bearer security scheme missing"
    assert "CookieAuth" in schemes, "Cookie security scheme (SSE) missing"
    assert schemes["CookieAuth"]["in"] == "cookie"
    assert schemes["CookieAuth"]["name"] == "access_token"


def test_delete_operations_return_204(spec):
    """DELETE success is 204 No Content and nothing else.

    Checks the full 2xx set (not just "204 documented") so a DELETE that actually returns a
    200 body while merely *documenting* 204 cannot slip through.
    """
    wrong = []
    for path, method, op in iter_operations(spec):
        if method != "delete":
            continue
        success_codes = [c for c in op.get("responses", {}) if c.startswith("2")]
        if success_codes != ["204"]:
            wrong.append(f"DELETE {path}: success responses must be exactly ['204'], got {sorted(success_codes)}")
        for code in success_codes:
            if op["responses"][code].get("content"):
                wrong.append(f"DELETE {path}: {code} must not declare a response body")
    assert not wrong, "DELETE endpoints violating the 204 convention:\n" + "\n".join(wrong)


def iter_schema_properties(schema: dict, prefix: str):
    """Recursively yield (dotted_name, property_schema) including inline nested objects.

    Walks ``properties``, array ``items``, and typed ``additionalProperties`` so inline
    sub-objects can't hide undescribed fields; free-form dicts (``additionalProperties: true``
    or bare ``{"type": "object"}``) are intentionally opaque and yield nothing.
    """
    for prop_name, prop in schema.get("properties", {}).items():
        yield f"{prefix}.{prop_name}", prop
        yield from iter_schema_properties(prop, f"{prefix}.{prop_name}")
    items = schema.get("items")
    if isinstance(items, dict):
        yield from iter_schema_properties(items, f"{prefix}[]")
    additional = schema.get("additionalProperties")
    if isinstance(additional, dict):
        yield from iter_schema_properties(additional, f"{prefix}{{}}")


def test_component_schema_fields_described(spec):
    """100% field-description coverage in component schemas (the lint behind schemas.py)."""
    missing = []
    for name, schema in spec.get("components", {}).get("schemas", {}).items():
        if name in EXCLUDED_SCHEMAS:
            continue
        for dotted, prop in iter_schema_properties(schema, name):
            if "description" not in prop:
                missing.append(dotted)
    assert not missing, f"{len(missing)} schema properties without description:\n" + "\n".join(missing)


def test_api_docs_app_kwargs():
    """DISABLE_API_DOCS gates Swagger / ReDoc / OpenAPI route mounting."""
    assert api_docs_app_kwargs(True) == {
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "openapi_url": "/openapi.json",
    }
    assert api_docs_app_kwargs(False) == {
        "docs_url": None,
        "redoc_url": None,
        "openapi_url": None,
    }


async def test_docs_endpoints_serve(api_client):
    """Docs routes are mounted when DISABLE_API_DOCS is false."""
    if settings.disable_api_docs:
        pytest.skip("API docs are disabled (DISABLE_API_DOCS=true)")
    assert app.docs_url == "/docs"
    assert app.redoc_url == "/redoc"
    assert app.openapi_url == "/openapi.json"
    for url in ("/openapi.json", "/docs", "/redoc"):
        response = await api_client.get(url)
        assert response.status_code == 200, f"{url} returned {response.status_code}"


async def test_sse_endpoints_enforce_cookie_auth(api_client):
    """SSE cookie auth is enforced at runtime, not just declared in the spec.

    The CookieAuth scheme uses ``auto_error=False`` (the 401 is raised manually in
    ``get_current_user_from_cookie``), so the spec's ``security`` entry alone proves nothing —
    this pins the actual 401-without-cookie behavior. ``api_client`` overrides only the Bearer
    dependency, so the cookie path stays live.
    """
    for url in (
        "/api/v1/dashboard/stream",
        "/api/v1/correlation_dashboard/stream",
        "/api/v1/attack_chains/chains/stream",
    ):
        response = await api_client.get(url)
        assert response.status_code == 401, f"{url} without cookie returned {response.status_code}, expected 401"


def test_deprecated_operations_are_exactly_the_singleton_surface(spec):
    """Step A of the singleton retirement: mark, do not move.

    Configuration GET/PATCH and runtime start/stop stay on the legacy paths. The graph dashboard
    is ``GET /api/v1/correlation_dashboard/snapshot`` plus ``/stream`` (SSE).
    """
    deprecated = {(path, method) for path, method, op in iter_operations(spec) if op.get("deprecated")}
    assert deprecated == {
        ("/api/v1/correlation_job/configuration", "get"),
        ("/api/v1/correlation_job/configuration", "patch"),
        ("/api/v1/attack_chains/runtime/start", "post"),
        ("/api/v1/attack_chains/runtime/stop", "post"),
    }
    for path, method in deprecated:
        description = spec["paths"][path][method].get("description") or ""
        assert "**Deprecated.**" in description, f"{method.upper()} {path} has no deprecation banner"
        assert "/api/v1/correlation_pipelines" in description


def test_correlation_pipelines_surface_is_complete(spec):
    """Five collection paths; every mutating one is a write route and run/stop are 202, not 200.

    The platform graph is a sibling resource (``/api/v1/correlation_dashboard``) so it cannot be
    read as ``{correlation_pipeline_id}``.
    """
    paths = {p for p in spec["paths"] if p.startswith("/api/v1/correlation_pipelines")}
    assert paths == {
        "/api/v1/correlation_pipelines",
        "/api/v1/correlation_pipelines/{correlation_pipeline_id}",
        "/api/v1/correlation_pipelines/{correlation_pipeline_id}/duplicate",
        "/api/v1/correlation_pipelines/{correlation_pipeline_id}/run",
        "/api/v1/correlation_pipelines/{correlation_pipeline_id}/stop",
    }
    base = "/api/v1/correlation_pipelines/{correlation_pipeline_id}"
    assert set(spec["paths"][f"{base}/run"]["post"]["responses"]) >= {"202"}
    assert "200" not in spec["paths"][f"{base}/run"]["post"]["responses"]
    assert "200" not in spec["paths"][f"{base}/stop"]["post"]["responses"]
    assert set(spec["paths"][f"{base}/duplicate"]["post"]["responses"]) >= {"201"}
    assert spec["paths"]["/api/v1/correlation_pipelines"]["post"]["responses"].get("201")


def test_correlation_dashboard_surface_is_complete(spec):
    """Snapshot + SSE, neither nested under ``{correlation_pipeline_id}``."""
    paths = {p for p in spec["paths"] if p.startswith("/api/v1/correlation_dashboard")}
    assert paths == {
        "/api/v1/correlation_dashboard/snapshot",
        "/api/v1/correlation_dashboard/stream",
    }
    stream_security = spec["paths"]["/api/v1/correlation_dashboard/stream"]["get"].get("security")
    assert stream_security == [{"CookieAuth": []}]


def test_correlation_pipeline_list_item_exposes_destination_topics(spec):
    """GET /correlation_pipelines rows carry destination_topics for list chips."""
    schema = spec["components"]["schemas"]["CorrelationPipelineListItem"]
    assert "destination_topics" in schema["properties"]
    assert schema["properties"]["destination_topics"]["type"] == "array"
    assert schema["properties"]["destination_topics"]["items"]["type"] == "string"


def test_pipeline_list_and_detail_expose_correlation_pipelines(spec):
    """GET /pipeline rows carry correlation_pipelines chips (id/name refs)."""
    ref = {"$ref": "#/components/schemas/CorrelationPipelineRef"}
    for schema_name in ("PipelineListItem", "PipelineDetailResponse"):
        schema = spec["components"]["schemas"][schema_name]
        assert "correlation_pipelines" in schema["properties"]
        items = schema["properties"]["correlation_pipelines"]["items"]
        assert ref["$ref"] in str(items), f"{schema_name}.correlation_pipelines is not CorrelationPipelineRef"


def test_composite_status_is_never_a_bare_string(spec):
    """The six-value enum must reach the client as an enum, not as free text."""
    enum = spec["components"]["schemas"]["CorrelationPipelineStatus"]
    assert enum["enum"] == ["running", "starting", "stopping", "stopped", "partial", "failed"]
    ref = {"$ref": "#/components/schemas/CorrelationPipelineStatus"}
    for schema_name, field in (
        ("AttackChainsRuntimeStateResponse", "overall"),
        ("AttackChainsRuntimeResponse", "status"),
        ("CorrelationPipelineRuntimeResponse", "status"),
    ):
        prop = spec["components"]["schemas"][schema_name]["properties"][field]
        assert ref["$ref"] in str(prop), f"{schema_name}.{field} is not typed with the status enum"


def test_rbac_dependencies_are_annotated_in_openapi(spec):
    """Every schema-included route with write/admin auth deps gets Requires + modeled 403."""
    missing = []
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        level = rbac_level_for_route(route)
        if level is None:
            continue
        path_item = spec["paths"].get(route.path)
        if not path_item:
            missing.append(f"{route.path}: route not found in OpenAPI paths")
            continue
        requires_line = RBAC_DESCRIPTION_LINE[level]
        forbidden_detail = RBAC_FORBIDDEN_DETAIL[level]
        for method in route.methods or ():
            op = path_item.get(method.lower())
            if not isinstance(op, dict):
                missing.append(f"{method.upper()} {route.path}: operation missing from OpenAPI")
                continue
            if requires_line not in (op.get("description") or ""):
                missing.append(f"{method.upper()} {route.path}: description missing {requires_line!r}")
            response_403 = op.get("responses", {}).get("403")
            if not response_403:
                missing.append(f"{method.upper()} {route.path}: missing 403 response")
                continue
            if response_403.get("description") != forbidden_detail:
                missing.append(
                    f"{method.upper()} {route.path}: 403 description "
                    f"{response_403.get('description')!r} != {forbidden_detail!r}"
                )
            schema = response_403.get("content", {}).get("application/json", {}).get("schema")
            if not schema:
                missing.append(f"{method.upper()} {route.path}: 403 response has no JSON schema")
    assert not missing, "RBAC OpenAPI annotations missing or wrong:\n" + "\n".join(missing)


def test_viewer_safe_routes_are_not_write_annotated(spec):
    """Pinned viewer-safe POSTs must not carry write/admin Requires annotations."""
    viewer_safe = {
        ("/api/v1/rules/validate", "post"),
        ("/api/v1/rules/validate/bulk", "post"),
        ("/api/v1/generate-mapping-prompt", "post"),
        ("/api/v1/get-sigma-fields", "post"),
        ("/api/v1/parsers/run", "post"),
        ("/api/v1/topic-events", "post"),
    }
    unexpected = []
    for path, method in viewer_safe:
        op = spec["paths"][path][method]
        description = op.get("description") or ""
        for line in RBAC_DESCRIPTION_LINE.values():
            if line in description:
                unexpected.append(f"{method.upper()} {path}: unexpected {line!r}")
        if "403" in op.get("responses", {}):
            unexpected.append(f"{method.upper()} {path}: unexpected 403 response")
    assert not unexpected, "Viewer-safe routes incorrectly annotated:\n" + "\n".join(unexpected)


def test_rbac_level_helper_on_known_routes():
    """Sanity-check dependency detection against known admin / write / read routes."""
    by_path_method: dict[tuple[str, str], APIRoute] = {}
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        for method in route.methods or ():
            by_path_method[(route.path, method.lower())] = route

    assert rbac_level_for_route(by_path_method[("/api/v1/users", "get")]) == "admin"
    assert rbac_level_for_route(by_path_method[("/api/v1/filters", "post")]) == "write"
    assert rbac_level_for_route(by_path_method[("/api/v1/filters", "get")]) is None
