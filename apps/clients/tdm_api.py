import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import wraps
from typing import Any
from urllib.parse import urljoin

import httpx
from pydantic import BaseModel

from apps.core.settings import settings

# Retry configuration for rate limiting
MAX_RETRIES = 5
BASE_DELAY = 1.0


class TdmRepository(BaseModel, extra="ignore"):
    id: str
    name: str

    last_synced_rule_date: datetime | None = None


class TdmRule(BaseModel, extra="ignore"):
    id: str
    name: str
    body: str
    level: str | None = None
    tags: dict = {}
    created: datetime
    updated: datetime


class TdmApiError(Exception):
    """Base exception for TDM API errors."""

    pass


class TdmApiBadRequestError(TdmApiError):
    """400 Bad Request."""

    pass


class TdmApiNotFoundError(TdmApiError):
    """404 Not Found."""

    pass


class TdmApiUnauthorizedError(TdmApiError):
    """401/403 Unauthorized."""

    pass


class TdmApiConnectionError(TdmApiError):
    """Connection errors (timeout, DNS, refused, etc.)."""

    pass


class TdmApiServerError(TdmApiError):
    """5xx Server errors."""

    pass


def retry_on_rate_limit(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    """Decorator to retry on HTTP 429 (Too Many Requests) with rate limit handling.
    Uses global constants MAX_RETRIES and BASE_DELAY.
    """

    @wraps(func)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        for attempt in range(MAX_RETRIES + 1):
            try:
                return await func(self, *args, **kwargs)
            except httpx.HTTPStatusError as e:
                # Retry only on 429 (Too Many Requests)
                if e.response.status_code == 429 and attempt < MAX_RETRIES:
                    delay = None

                    # Check for x-rate-limit-reset-timestamp header
                    reset_timestamp = e.response.headers.get("x-rate-limit-reset-timestamp")
                    if reset_timestamp:
                        try:
                            reset_time = float(reset_timestamp)
                            current_time = time.time()
                            delay = max(0, reset_time - current_time)
                        except (ValueError, TypeError):
                            # If timestamp is invalid, fall through to exponential backoff
                            pass

                    # Fall back to exponential backoff if timestamp not available or invalid
                    if delay is None:
                        delay = BASE_DELAY * (2**attempt)

                    await asyncio.sleep(delay)
                    continue
                # Re-raise if not 429 or max retries reached
                raise
            except (httpx.RequestError, httpx.TimeoutException):
                # Re-raise network/connection errors without retry
                raise

        # This should never be reached, but type checker needs it
        raise RuntimeError("Unexpected retry loop exit")

    return wrapper


class TDMAPIClient:
    def __init__(self, api_key: str):
        self._base_url = settings.tdm_api_base_url
        self.api_key = api_key

    def _safe_json(self, response: httpx.Response) -> dict:
        """Safely parse JSON response, return empty dict if body is empty or invalid."""
        try:
            return response.json() if response.content else {}
        except Exception:
            return {}

    @retry_on_rate_limit
    async def _make_api_request(
        self, method: str, endpoint: str, params: dict | None = None, json: dict | None = None
    ) -> dict:
        if not self.api_key:
            raise TdmApiUnauthorizedError("API key is not set")

        url = urljoin(self._base_url, endpoint)
        headers = {"client_secret_id": self.api_key, "User-Agent": "DetectFlow"}

        try:
            async with httpx.AsyncClient(base_url=self._base_url) as client:
                response = await client.request(method, url, headers=headers, params=params, json=json, timeout=30)
        except httpx.TimeoutException:
            raise TdmApiConnectionError("TDM API is not responding. Please try again later.")
        except httpx.ConnectError:
            raise TdmApiConnectionError("Cannot connect to TDM API. Please check your network connection.")
        except httpx.RequestError:
            raise TdmApiConnectionError("Network error occurred while connecting to TDM API. Please try again.")

        # Handle HTTP status codes
        status = response.status_code

        if status == 400:
            detail = self._safe_json(response).get("detail", "Bad request")
            raise TdmApiBadRequestError(detail)

        if status == 404:
            detail = self._safe_json(response).get("detail", "Requested resource not found")
            raise TdmApiNotFoundError(detail)

        if status in [401, 403]:
            raise TdmApiUnauthorizedError("This API key isn't valid or doesn't have sufficient permissions")

        if status >= 500:
            raise TdmApiServerError("TDM API is temporarily unavailable. Please try again later.")

        # Raise for any other 4xx errors
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            raise TdmApiError("Unexpected error from TDM API. Please try again later.")

        return response.json()

    async def get_repositories(self) -> list[TdmRepository]:
        response = await self._make_api_request(
            method="GET",
            endpoint="/v1/custom-repositories",
            params={
                "select_only_detectflow_repos": "true",
            },
        )
        return [TdmRepository(**repo) for repo in response]

    async def get_rules(self, repository_id: str, updated_after: datetime | None = None) -> list[TdmRule]:
        url = urljoin(self._base_url, "/v1/custom-content")
        params = {"repo_id": repository_id, "siem_type": "sigma"}
        if updated_after:
            params["updated_after"] = updated_after.isoformat()
        response = await self._make_api_request(method="GET", endpoint=url, params=params)
        return [self._parse_rule(rule) for rule in response]

    @staticmethod
    def _parse_rule(rule: dict) -> TdmRule:
        # Extract tags, keeping only technique for Flink compatibility
        tags_data = rule.get("tags", {})
        tags = {"technique": tags_data.get("technique", [])} if tags_data else {}

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
            res.created = res.created.replace(tzinfo=UTC)
        if res.updated and not res.updated.tzinfo:
            res.updated = res.updated.replace(tzinfo=UTC)
        return res

    async def update_rule(
        self, repository_id: str, rule_id: str, name: str | None = None, body: str | None = None
    ) -> TdmRule:
        url = urljoin(self._base_url, f"/v1/custom-content/{rule_id}")
        params = {"repo_id": repository_id}
        payload = {}
        if name:
            payload["rule_name"] = name
        if body:
            payload["rule_text"] = body
            payload["siem_type"] = "sigma"
        if not payload:
            raise ValueError("Nothing to update")
        response = await self._make_api_request(method="PATCH", endpoint=url, params=params, json=payload)
        return self._parse_rule(response)

    async def get_rule_ids(self, repository_id: str) -> list[str]:
        return await self._make_api_request(
            method="GET",
            endpoint="/v1/custom-content-ids",
            params={"repo_id": repository_id, "siem_type": "sigma"},
        )

    async def generate_mapping(self, sigma_fields: list[str], event_fields: list[str]) -> str:
        res = await self._make_api_request(
            method="POST",
            endpoint="/v1/ai-features-in-uncoder/field-mapping",
            json={
                "sigma_fields": sigma_fields,
                "event_fields": event_fields,
            },
        )
        job_id = res["job_id"]

        # TODO: make 2 endpoints??
        while True:
            res = await self._make_api_request(
                method="GET",
                endpoint=f"/v1/ai-features-in-uncoder/job-result/{job_id}",
            )
            if res["status"] == "done":
                return res["result"]
            if res["status"] == "error":
                raise TdmApiError(res["result"])
            await asyncio.sleep(2)
