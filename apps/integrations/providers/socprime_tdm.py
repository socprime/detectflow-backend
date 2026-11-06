"""SOC Prime Threat Detection Marketplace integration.

HTTP calls to TDM live on :class:`SocPrimeTdmProvider`. Build an instance with
``SocPrimeTdmProvider(config_dict)`` where ``config_dict`` comes from
:meth:`apps.modules.postgre.config.ConfigDAO.get_socprime_tdm_provider_config`
(or any dict matching :class:`SocPrimeTdmConfig`: ``api_key`` only). The TDM REST
base URL is global: :attr:`apps.core.settings.Settings.tdm_api_base_url`
(environment variable ``TDM_API_BASE_URL``, default ``SOCPRIME_TDM_API_BASE_URL``
in ``apps.core.constants``).

:meth:`generate_mapping` polls the async job endpoint up to :data:`TDM_MAPPING_JOB_MAX_POLLS`
(two-second interval) then raises :exc:`TdmApiError` if the job never completes.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator
from datetime import UTC, datetime
from functools import cached_property, wraps
from typing import Any
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel, Field

from apps.core.constants import HTTP_USER_AGENT
from apps.core.enums import IntegrationCategory
from apps.core.settings import settings
from apps.integrations.base import BaseIntegrationProvider, ConnectionResult
from apps.integrations.registry import IntegrationRegistry

MAX_RETRIES = 5
BASE_DELAY = 1.0
# Upper bound for mapping job polling (~30 min at 2s sleep) to avoid unbounded wait.
TDM_MAPPING_JOB_MAX_POLLS = 900
# /v1/active-threats/check-existence accepts 1..500 ids per request.
ACTIVE_THREAT_EXISTENCE_BATCH_SIZE = 500
# /v1/search-sigmas page_size max is 50; we chunk Lucene OR groups to fit one page.
SEARCH_SIGMAS_BATCH_SIZE = 50


def _chunked[T](items: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield lists of up to ``size`` items from ``items``."""
    chunk: list[T] = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _build_lucene_or_query(field: str, values: Iterable[str]) -> str:
    """Build a Lucene ``field: ("v1" OR "v2" ...)`` clause. ``OR`` must be uppercase."""
    quoted = " OR ".join(f'"{v}"' for v in values)
    return f"{field}: ({quoted})"


def _format_search_sigmas_date_start(value: datetime) -> str:
    """Format ``date_start`` for TDM ``search-sigmas`` headers."""
    value = value.astimezone(UTC) if value.tzinfo else value
    return f"{value.year}-{value.month}-{value.day}T{value:%H:%M:%S}"


def _build_search_sigmas_headers(
    field: str,
    values: Iterable[str],
    siem_type: str = "sigma",
    updated_after: datetime | None = None,
) -> dict[str, str]:
    headers = {
        "client_siem_type": siem_type,
        "client_query_string": _build_lucene_or_query(field, values),
    }
    if updated_after is not None:
        headers["date_start"] = _format_search_sigmas_date_start(updated_after)
    return headers


class TdmRepository(BaseModel, extra="ignore"):
    id: str
    name: str
    last_synced_rule_date: datetime | None = None


class TdmRule(BaseModel, extra="ignore"):
    id: str
    name: str
    body: str
    level: str | None = None
    tags: dict[str, Any] = Field(default_factory=dict)
    created: datetime
    updated: datetime


class TdmActiveThreat(BaseModel, extra="ignore"):
    """Active Threat as returned by ``GET /v1/active-threats``.

    ``related_high_order_sigma_rules`` are TDM ``case.id`` values; resolve them with
    :meth:`SocPrimeTdmProvider.search_rules_by_case_ids`.
    ``threat_actors`` / ``techniques`` are MITRE ATT&CK group and technique IDs.
    """

    id: str
    name: str
    created: datetime
    updated: datetime
    summary: str | None = None
    severity: str | None = None
    investigation: str | None = None
    response: str | None = None
    mitigation: str | None = None
    industries: list[str] | None = None
    geographies: list[str] | None = None
    threat_types: list[str] | None = None
    threat_actors: list[str] | None = None
    techniques: list[str] | None = None
    attack_flow: str | None = None
    related_high_order_sigma_rules: list[str] = Field(default_factory=list)
    related_behavioral_rules: list[str] = Field(default_factory=list)


class TdmApiError(Exception):
    """Base error for TDM HTTP client failures (transport, HTTP status, mapping job)."""


class TdmApiBadRequestError(TdmApiError):
    """TDM returned HTTP 400."""


class TdmApiNotFoundError(TdmApiError):
    """TDM returned HTTP 404."""


class TdmApiUnauthorizedError(TdmApiError):
    """Missing/invalid API key or HTTP 401/403."""


class TdmApiConnectionError(TdmApiError):
    """Network timeout or connection failure reaching TDM."""


class TdmApiServerError(TdmApiError):
    """TDM returned HTTP 5xx."""


def retry_on_rate_limit(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Retry on HTTP 429 using ``MAX_RETRIES`` / ``BASE_DELAY``."""

    @wraps(func)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        for attempt in range(MAX_RETRIES + 1):
            try:
                return await func(self, *args, **kwargs)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < MAX_RETRIES:
                    delay = None
                    reset_timestamp = e.response.headers.get("x-rate-limit-reset-timestamp")
                    if reset_timestamp:
                        try:
                            reset_time = float(reset_timestamp)
                            delay = max(0, reset_time - time.time())
                        except (ValueError, TypeError):
                            pass
                    if delay is None:
                        delay = BASE_DELAY * (2**attempt)
                    await asyncio.sleep(delay)
                    continue
                raise
            except (httpx.RequestError, httpx.TimeoutException):
                raise
        raise RuntimeError("Unexpected retry loop exit")

    return wrapper


class SocPrimeTdmConfig(BaseModel):
    """Config schema for SOC Prime Threat Detection Marketplace."""

    api_key: str = Field(
        ...,
        description="Your SOC Prime API key",
        json_schema_extra={
            "sensitive": True,
            "ui_order": 1,
            "placeholder": "Enter your SOC Prime API key",
        },
    )
    push: bool = Field(
        default=True,
        description="Push local rule changes to SOC Prime TDM. When off, TDM changes are still pulled into DetectFlow.",
        json_schema_extra={"ui_order": 2, "section": "Sync Settings"},
    )


@IntegrationRegistry.register
class SocPrimeTdmProvider(BaseIntegrationProvider):
    integration_type = "socprime_tdm"
    singleton_profile = True
    display_name = "Threat Detection Marketplace"
    category = IntegrationCategory.SOC_PRIME
    description = "Access SOC Prime Platform for threat detection content, AI-powered field mapping and translations."
    icon = "socprime"
    sensitive_mask = "..."

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return SocPrimeTdmConfig

    @cached_property
    def _validated_config(self) -> SocPrimeTdmConfig:
        """Parse ``self.config`` once per provider instance (hot paths call HTTP many times)."""
        return SocPrimeTdmConfig.model_validate(self.config)

    @classmethod
    def mask_sensitive_config(cls, config: dict, sensitive_fields: set[str]) -> dict:
        """Mask ``api_key`` for API responses.

        Non-strings and values shorter than 6 characters become ``cls.sensitive_mask``
        (for this provider ``...``). Longer strings become ``first3 + sensitive_mask + last3``.
        """
        masked = dict(config)
        for field in sensitive_fields:
            if field in masked and masked[field]:
                if isinstance(masked[field], str):
                    if len(masked[field]) < 6:
                        masked[field] = cls.sensitive_mask
                    else:
                        masked[field] = masked[field][:3] + cls.sensitive_mask + masked[field][-3:]
                else:
                    masked[field] = cls.sensitive_mask
        return masked

    def _safe_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            return response.json() if response.content else {}
        except Exception:
            return {}

    @retry_on_rate_limit
    async def _make_api_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        cfg = self._validated_config
        if not cfg.api_key:
            raise TdmApiUnauthorizedError("API key is not set")

        url = urljoin(settings.tdm_api_base_url.rstrip("/"), endpoint)
        request_headers = {"client_secret_id": cfg.api_key, "User-Agent": HTTP_USER_AGENT}
        if headers:
            request_headers.update(headers)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method, url, headers=request_headers, params=params, json=json, timeout=30
                )
        except httpx.TimeoutException:
            raise TdmApiConnectionError("TDM API is not responding. Please try again later.")
        except httpx.ConnectError:
            raise TdmApiConnectionError("Cannot connect to TDM API. Please check your network connection.")
        except httpx.RequestError:
            raise TdmApiConnectionError("Network error occurred while connecting to TDM API.")

        status = response.status_code

        if status == 400:
            detail = self._safe_json(response).get("detail", "Bad request")
            raise TdmApiBadRequestError(detail)

        if status == 404:
            detail = self._safe_json(response).get("detail", "Requested resource not found")
            raise TdmApiNotFoundError(detail)

        if status in (401, 403):
            raise TdmApiUnauthorizedError("This API key isn't valid or doesn't have sufficient permissions")

        if status >= 500:
            raise TdmApiServerError("TDM API is temporarily unavailable. Please try again later.")

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if response.status_code == 429:
                raise e
            else:
                raise TdmApiError("Unexpected error from TDM API. Please try again later.")

        return response.json()

    async def get_repositories(self) -> list[TdmRepository]:
        """Fetch DetectFlow-visible custom repositories from TDM."""
        response = await self._make_api_request(
            method="GET",
            endpoint="/v1/custom-repositories",
            params={"select_only_detectflow_repos": "true"},
        )
        return [TdmRepository(**repo) for repo in response]

    async def get_rules(self, repository_id: str, updated_after: datetime | None = None) -> list[TdmRule]:
        """List Sigma rules for a repository; optional incremental filter via ``updated_after``."""
        params: dict[str, str] = {"repo_id": repository_id, "siem_type": "sigma"}
        if updated_after:
            params["updated_after"] = updated_after.isoformat()
        response = await self._make_api_request(method="GET", endpoint="/v1/custom-content", params=params)
        return [self._parse_rule(rule) for rule in response]

    @staticmethod
    def _parse_rule(rule: dict[str, Any]) -> TdmRule:
        tags_data = rule.get("tags", {})
        tags: dict[str, Any] = {"technique": tags_data.get("technique", [])} if tags_data else {}

        res = TdmRule(
            id=rule["case"]["id"],
            name=rule["case"]["name"],
            body=rule["sigma"]["text"],
            level=rule.get("sigma", {}).get("level"),
            tags=tags,
            created=rule["release_date"],
            updated=rule["update_date"],
        )
        if res.created and not res.created.tzinfo:
            res = res.model_copy(update={"created": res.created.replace(tzinfo=UTC)})
        if res.updated and not res.updated.tzinfo:
            res = res.model_copy(update={"updated": res.updated.replace(tzinfo=UTC)})
        return res

    async def update_rule(
        self,
        repository_id: str,
        rule_id: str,
        name: str | None = None,
        body: str | None = None,
    ) -> TdmRule:
        """PATCH a rule in TDM; at least one of ``name`` or ``body`` must be set."""
        params = {"repo_id": repository_id}
        payload: dict[str, str] = {}
        if name:
            payload["rule_name"] = name
        if body:
            payload["rule_text"] = body
            payload["siem_type"] = "sigma"
        if not payload:
            raise ValueError("Nothing to update")
        response = await self._make_api_request(
            method="PATCH", endpoint=f"/v1/custom-content/{rule_id}", params=params, json=payload
        )
        return self._parse_rule(response)

    async def get_rule_ids(self, repository_id: str) -> list[str]:
        """Return rule id strings for a repository (Sigma) without full rule bodies."""
        return await self._make_api_request(
            method="GET",
            endpoint="/v1/custom-content-ids",
            params={"repo_id": repository_id, "siem_type": "sigma"},
        )

    async def get_active_threats(
        self,
        updated_after: datetime,
        updated_before: datetime | None = None,
    ) -> list[TdmActiveThreat]:
        """Fetch Active Threats updated within ``[updated_after, updated_before]``.

        ``updated_after`` must be no more than 90 days old (TDM constraint).
        """
        params: dict[str, str] = {"updated_after": updated_after.isoformat()}
        if updated_before is not None:
            params["updated_before"] = updated_before.isoformat()
        response = await self._make_api_request(
            method="GET",
            endpoint="/v1/active-threats",
            params=params,
        )
        items = response.get("active_threats", []) if isinstance(response, dict) else []
        return [self._parse_active_threat(item) for item in items]

    async def check_active_threats_exist(self, ids: list[str]) -> set[str]:
        """Return the subset of ``ids`` still active in TDM (chunked to 500 per call)."""
        if not ids:
            return set()
        existing: set[str] = set()
        for chunk in _chunked(ids, ACTIVE_THREAT_EXISTENCE_BATCH_SIZE):
            response = await self._make_api_request(
                method="POST",
                endpoint="/v1/active-threats/check-existence",
                json={"ids": chunk},
            )
            active = response.get("active_ids", []) if isinstance(response, dict) else []
            existing.update(active)
        return existing

    async def search_rules_by_case_ids(
        self,
        case_ids: list[str],
        siem_type: str = "sigma",
        updated_after: datetime | None = None,
    ) -> list[TdmRule]:
        """Fetch rules by TDM ``case.id`` values.

        Calls ``GET /v1/search-sigmas`` in chunks to fit one page per request.
        ``siem_type`` is sent as ``client_siem_type`` and defaults to ``sigma``.
        ``updated_after`` is sent as TDM's ``date_start`` header to fetch only
        content updated after that timestamp.
        """
        if not case_ids:
            return []
        rules: list[TdmRule] = []
        for chunk in _chunked(case_ids, SEARCH_SIGMAS_BATCH_SIZE):
            response = await self._make_api_request(
                method="GET",
                endpoint="/v1/search-sigmas",
                params={"unlock_content_for_detect_flow": "true"},
                headers=_build_search_sigmas_headers("case.id", chunk, siem_type, updated_after),
            )
            items = response if isinstance(response, list) else []
            rules.extend(self._parse_rule(item) for item in items)
        rules = self._filter_out_locked_rules(rules)
        return rules

    async def search_rules_by_sigma_uuids(
        self,
        sigma_uuids: list[str],
        siem_type: str = "sigma",
        updated_after: datetime | None = None,
    ) -> list[TdmRule]:
        """Fetch rules by Sigma UUID values.

        Calls ``GET /v1/search-sigmas`` in chunks to fit one page per request.
        ``siem_type`` is sent as ``client_siem_type`` and defaults to ``sigma``.
        ``updated_after`` is sent as TDM's ``date_start`` header to fetch only
        content updated after that timestamp.
        """
        if not sigma_uuids:
            return []
        rules: list[TdmRule] = []
        for chunk in _chunked(sigma_uuids, SEARCH_SIGMAS_BATCH_SIZE):
            response = await self._make_api_request(
                method="GET",
                endpoint="/v1/search-sigmas",
                params={"unlock_content_for_detect_flow": "true"},
                headers=_build_search_sigmas_headers("sigma.uuid", chunk, siem_type, updated_after),
            )
            items = response if isinstance(response, list) else []
            rules.extend(self._parse_rule(item) for item in items)

        rules = self._filter_out_locked_rules(rules)
        return rules

    @staticmethod
    def _filter_out_locked_rules(rules: list[TdmRule]) -> list[TdmRule]:
        return [rule for rule in rules if not rule.body == "This rule is locked"]

    @staticmethod
    def _ensure_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value

    @classmethod
    def _parse_active_threat(cls, item: dict[str, Any]) -> TdmActiveThreat:
        threat = TdmActiveThreat.model_validate(item)
        return threat.model_copy(
            update={
                "created": cls._ensure_utc(threat.created),
                "updated": cls._ensure_utc(threat.updated),
            }
        )

    async def generate_mapping(self, sigma_fields: list[str], event_fields: list[str]) -> str:
        """Start TDM AI field-mapping job and poll until done, error, or timeout.

        Raises:
            TdmApiError: On job error payload or if polling exceeds ``TDM_MAPPING_JOB_MAX_POLLS``.
        """
        res = await self._make_api_request(
            method="POST",
            endpoint="/v1/ai-features-in-uncoder/field-mapping",
            json={"sigma_fields": sigma_fields, "event_fields": event_fields},
        )
        job_id = res["job_id"]

        for _ in range(TDM_MAPPING_JOB_MAX_POLLS):
            res = await self._make_api_request(
                method="GET",
                endpoint=f"/v1/ai-features-in-uncoder/job-result/{job_id}",
            )
            if res["status"] == "done":
                return res["result"]
            if res["status"] == "error":
                raise TdmApiError(res["result"])
            await asyncio.sleep(2)

        raise TdmApiError("TDM mapping job timed out while waiting for result")

    async def check_connection(self) -> ConnectionResult:
        try:
            await self.get_repositories()
            return ConnectionResult(success=True, message="Connected to SOC Prime successfully")
        except TdmApiError as e:
            return ConnectionResult(success=False, message=str(e))
        except Exception as e:
            return ConnectionResult(success=False, message=f"Connection failed: {e!s}")
