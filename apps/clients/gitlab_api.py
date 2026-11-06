"""GitLab client for Sigma-rule sync via the GitLab REST API (v4).

Two directions, deliberately asymmetric:
  - **Pull** (scheduler): ``pull`` (full) / ``pull_changes`` (incremental via compare API).
  - **Push** (user, per rule): ``push_rule`` / ``delete_rule`` with optimistic locking.

Configuration is supplied by the caller (typically a GitLab integration profile row).
"""

import asyncio
import base64
import ssl
from collections import Counter
from urllib.parse import quote
from uuid import UUID

import httpx
import yaml
from pydantic import BaseModel

from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.utils import coerce_config_bool, normalize_pem_bundle

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 30.0
MAX_CONCURRENT_FETCHES = 8
TREE_PAGE_SIZE = 100
DEFAULT_AUTHOR_NAME = "DetectFlow"
DEFAULT_AUTHOR_EMAIL = "detectflow@soc.local"


class GitlabRule(BaseModel):
    """A Sigma rule as read from / written to GitLab."""

    id: str
    name: str
    body: str
    file_path: str = ""
    last_commit_id: str | None = None


class GitLabApiError(Exception):
    """Raised for GitLab REST API errors (auth, HTTP, config failures)."""


class GitLabApiNotFoundError(Exception):
    """Raised when a project, branch, or path is not found (HTTP 404)."""


class GitLabConflictError(GitLabApiError):
    """Raised when an optimistic-locked write is rejected because the file changed on GitLab."""


class GitLabPullResult(BaseModel):
    rules: list[GitlabRule]
    commit_sha: str | None = None


class GitLabPullChanges(BaseModel):
    upserts: list[GitlabRule] = []
    deletes: list[str] = []
    commit_sha: str | None = None
    full: bool = False


class GitLabApiClient:
    """Sync Sigma rules in and out of a GitLab project using the REST API."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        project_path: str,
        branch: str = "main",
        subpath: str = "",
        author_name: str = DEFAULT_AUTHOR_NAME,
        author_email: str = DEFAULT_AUTHOR_EMAIL,
        timeout: float = DEFAULT_TIMEOUT,
        verify_ssl: bool = True,
        ca_cert: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.project_path = project_path.strip("/")
        self.branch = branch
        self.subpath = subpath.strip("/")
        self.author_name = author_name
        self.author_email = author_email
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.ca_cert = ca_cert.strip()

    @classmethod
    def from_integration_config(
        cls,
        config: dict,
        *,
        author_name: str | None = None,
        author_email: str | None = None,
    ) -> "GitLabApiClient":
        """Build a client from a GitLab integration profile ``config`` JSONB."""
        return cls(
            base_url=str(config.get("base_url") or "https://gitlab.com"),
            token=str(config["token"]),
            project_path=str(config["project_path"]),
            branch=str(config.get("branch") or "main"),
            subpath=str(config.get("subpath") or ""),
            verify_ssl=coerce_config_bool(config.get("verify_ssl"), default=True),
            ca_cert=str(config.get("ca_cert") or ""),
            timeout=settings.gitlab_api_timeout_seconds,
            author_name=author_name or DEFAULT_AUTHOR_NAME,
            author_email=author_email or DEFAULT_AUTHOR_EMAIL,
        )

    def _ssl_verify(self) -> bool | ssl.SSLContext:
        """Resolve the httpx ``verify`` argument from ``verify_ssl`` + optional ``ca_cert``.

        Returns ``False`` when verification is disabled, otherwise an :class:`ssl.SSLContext`.
        When ``ca_cert`` (in-memory PEM) is set it is added **on top of** the default system/public
        trust store — so an operator can supply just a private-CA root, or only the intermediate a
        misconfigured public host fails to send, without losing the built-in public roots. With no
        ``ca_cert`` this is equivalent to httpx's default (``True``).
        """
        if not self.verify_ssl:
            return False
        ctx = ssl.create_default_context()
        if self.ca_cert:
            try:
                ctx.load_verify_locations(cadata=normalize_pem_bundle(self.ca_cert))
            except ssl.SSLError as exc:
                raise GitLabApiError(f"Invalid CA certificate: {exc}") from exc
        return ctx

    def _http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=self.timeout, verify=self._ssl_verify())

    def _project_api_url(self) -> str:
        if not self.token:
            raise GitLabApiError("GitLab token is not configured")
        if not self.project_path:
            raise GitLabApiError("GitLab project path is not configured")
        encoded_project = quote(self.project_path, safe="")
        return f"{self.base_url}/api/v4/projects/{encoded_project}"

    def _headers(self) -> dict[str, str]:
        return {"PRIVATE-TOKEN": self.token}

    def _full_path(self, rel_path: str) -> str:
        rel = rel_path.strip("/")
        return f"{self.subpath}/{rel}" if self.subpath else rel

    def _to_subpath_relative(self, repo_path: str) -> str:
        if self.subpath and repo_path.startswith(f"{self.subpath}/"):
            return repo_path[len(self.subpath) + 1 :]
        return repo_path

    @staticmethod
    def _is_valid_uuid(value: str | None) -> bool:
        if not value:
            return False
        try:
            UUID(value)
            return True
        except (ValueError, TypeError):
            return False

    @classmethod
    def _parse_sigma_rule(cls, content: str, file_path: str, last_commit_id: str | None = None) -> GitlabRule | None:
        try:
            parsed = yaml.safe_load(content)
        except yaml.YAMLError:
            return None
        if not isinstance(parsed, dict) or not parsed.get("title") or not parsed.get("detection"):
            return None
        rule_id = parsed.get("id")
        if not cls._is_valid_uuid(rule_id):
            return None
        return GitlabRule(
            id=rule_id, name=parsed["title"], body=content, file_path=file_path, last_commit_id=last_commit_id
        )

    @staticmethod
    def _matches_include(repo_path: str, include_patterns: list[str], exclude_patterns: list[str]) -> bool:
        if any(token and token in repo_path for token in exclude_patterns):
            return False
        if not include_patterns:
            return True
        return any(
            repo_path.endswith(pattern) if pattern.startswith(".") else pattern in repo_path
            for pattern in include_patterns
        )

    @staticmethod
    def _response_message(response: httpx.Response) -> str:
        try:
            data = response.json()
        except ValueError:
            return response.text
        if isinstance(data, dict):
            return str(data.get("message") or data.get("error") or data)
        return str(data)

    def _raise_for_status(self, response: httpx.Response, url: str) -> None:
        if response.status_code == 404:
            raise GitLabApiNotFoundError(f"GitLab resource not found: {url}")
        if response.status_code in (401, 403):
            raise GitLabApiError(f"GitLab API authorization failed ({response.status_code}) for {url}")
        if response.status_code >= 400:
            raise GitLabApiError(f"GitLab API error {response.status_code} for {url}: {response.text[:300]}")

    async def _request(self, client: httpx.AsyncClient, method: str, url: str, **kwargs) -> httpx.Response:
        try:
            response = await client.request(method, url, headers=self._headers(), **kwargs)
        except httpx.HTTPError as exc:
            raise GitLabApiError(f"GitLab API request failed: {exc}") from exc
        self._raise_for_status(response, url)
        return response

    async def _get_branch_sha(self, client: httpx.AsyncClient) -> str | None:
        url = f"{self._project_api_url()}/repository/branches/{quote(self.branch, safe='')}"
        try:
            response = await self._request(client, "GET", url)
        except GitLabApiNotFoundError:
            return None
        return response.json().get("commit", {}).get("id")

    async def _list_blob_paths(self, client: httpx.AsyncClient) -> list[str]:
        url = f"{self._project_api_url()}/repository/tree"
        paths: list[str] = []
        page = 1
        while True:
            params = {"ref": self.branch, "recursive": "true", "per_page": TREE_PAGE_SIZE, "page": page}
            if self.subpath:
                params["path"] = self.subpath
            response = await self._request(client, "GET", url, params=params)
            for entry in response.json():
                if entry.get("type") == "blob":
                    paths.append(entry["path"])
            next_page = response.headers.get("X-Next-Page")
            if not next_page:
                break
            page = int(next_page)
        return paths

    async def _get_file(
        self, client: httpx.AsyncClient, repo_path: str, ref: str | None = None
    ) -> tuple[str, str | None] | None:
        encoded_path = quote(repo_path, safe="")
        url = f"{self._project_api_url()}/repository/files/{encoded_path}"
        try:
            response = await self._request(client, "GET", url, params={"ref": ref or self.branch})
        except GitLabApiNotFoundError:
            return None
        data = response.json()
        raw = data.get("content") or ""
        if data.get("encoding") == "base64":
            try:
                content = base64.b64decode(raw).decode("utf-8")
            except (ValueError, UnicodeDecodeError):
                return None
        else:
            content = raw
        return content, data.get("last_commit_id")

    def _in_subpath(self, repo_path: str) -> bool:
        return not self.subpath or repo_path.startswith(f"{self.subpath}/")

    async def _fetch_rules(
        self, client: httpx.AsyncClient, repo_paths: list[str], ref: str | None = None
    ) -> list[GitlabRule]:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_FETCHES)

        async def fetch(repo_path: str) -> GitlabRule | None:
            async with semaphore:
                result = await self._get_file(client, repo_path, ref=ref)
            if result is None:
                return None
            content, last_commit_id = result
            return self._parse_sigma_rule(content, self._to_subpath_relative(repo_path), last_commit_id)

        fetched = await asyncio.gather(*(fetch(path) for path in repo_paths))
        return [rule for rule in fetched if rule is not None]

    async def _compare_diffs(self, client: httpx.AsyncClient, from_sha: str, to_sha: str) -> list[dict]:
        url = f"{self._project_api_url()}/repository/compare"
        params = {"from": from_sha, "to": to_sha, "straight": "true"}
        response = await self._request(client, "GET", url, params=params)
        return response.json().get("diffs", [])

    async def get_latest_commit_sha(self) -> str | None:
        async with self._http_client() as client:
            return await self._get_branch_sha(client)

    async def pull(
        self,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> GitLabPullResult:
        include_patterns = include_patterns or [".yml", ".yaml"]
        exclude_patterns = exclude_patterns or []
        async with self._http_client() as client:
            commit_sha = await self._get_branch_sha(client)
            rules = await self._full_read(client, include_patterns, exclude_patterns)

        duplicate_ids = [rule_id for rule_id, count in Counter(r.id for r in rules).items() if count > 1]
        if duplicate_ids:
            logger.warning(
                f"GitLab project {self.project_path} has {len(duplicate_ids)} Sigma id(s) shared by "
                f"multiple files (sync keys on file path, not id): {sorted(duplicate_ids)}"
            )
        logger.info(f"Pulled {len(rules)} Sigma rule(s) from GitLab project {self.project_path} (API)")
        return GitLabPullResult(rules=rules, commit_sha=commit_sha)

    async def pull_changes(
        self,
        from_sha: str | None = None,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> GitLabPullChanges:
        include_patterns = include_patterns or [".yml", ".yaml"]
        exclude_patterns = exclude_patterns or []

        async with self._http_client() as client:
            head_sha = await self._get_branch_sha(client)
            if head_sha is None:
                return GitLabPullChanges(commit_sha=None)
            if from_sha == head_sha:
                logger.info(f"GitLab project {self.project_path}: no changes since {head_sha} (API)")
                return GitLabPullChanges(commit_sha=head_sha)

            if not from_sha:
                rules = await self._full_read(client, include_patterns, exclude_patterns)
                logger.info(f"GitLab project {self.project_path}: full read of {len(rules)} rule(s) (no base SHA)")
                return GitLabPullChanges(upserts=rules, commit_sha=head_sha, full=True)

            try:
                diffs = await self._compare_diffs(client, from_sha, head_sha)
            except GitLabApiNotFoundError:
                logger.warning(
                    f"GitLab project {self.project_path}: base commit {from_sha} unreachable — "
                    f"falling back to a full read"
                )
                rules = await self._full_read(client, include_patterns, exclude_patterns)
                return GitLabPullChanges(upserts=rules, commit_sha=head_sha, full=True)

            changed_paths: set[str] = set()
            deleted_paths: set[str] = set()
            for diff in diffs:
                old_path = diff.get("old_path")
                new_path = diff.get("new_path")
                if diff.get("deleted_file"):
                    deleted_paths.add(old_path)
                elif diff.get("renamed_file"):
                    deleted_paths.add(old_path)
                    changed_paths.add(new_path)
                else:
                    changed_paths.add(new_path)

            def keep(path: str) -> bool:
                return self._in_subpath(path) and self._matches_include(path, include_patterns, exclude_patterns)

            changed_repo_paths = [p for p in changed_paths if keep(p)]
            deletes = sorted(self._to_subpath_relative(p) for p in deleted_paths if keep(p))
            upserts = await self._fetch_rules(client, changed_repo_paths, ref=head_sha)

        logger.info(
            f"GitLab project {self.project_path}: {len(upserts)} changed, {len(deletes)} deleted since {from_sha} (API)"
        )
        return GitLabPullChanges(upserts=upserts, deletes=deletes, commit_sha=head_sha)

    async def _full_read(
        self, client: httpx.AsyncClient, include_patterns: list[str], exclude_patterns: list[str]
    ) -> list[GitlabRule]:
        all_paths = await self._list_blob_paths(client)
        candidate_paths = [p for p in all_paths if self._matches_include(p, include_patterns, exclude_patterns)]
        return await self._fetch_rules(client, candidate_paths)

    async def _write_commit(self, action: dict[str, str], commit_message: str) -> str:
        payload = {
            "branch": self.branch,
            "commit_message": commit_message,
            "author_name": self.author_name,
            "author_email": self.author_email,
            "actions": [action],
        }
        url = f"{self._project_api_url()}/repository/commits"
        async with self._http_client() as client:
            try:
                response = await client.request("POST", url, headers=self._headers(), json=payload)
            except httpx.HTTPError as exc:
                raise GitLabApiError(f"GitLab API request failed: {exc}") from exc

        message = self._response_message(response)
        if response.status_code == 400 and "changed since" in message.lower():
            raise GitLabConflictError(message)
        self._raise_for_status(response, url)
        return response.json().get("id")

    async def push_rule(
        self,
        rule: GitlabRule,
        *,
        last_commit_id: str | None = None,
        force: bool = False,
        commit_message: str | None = None,
    ) -> str:
        full_path = self._full_path(self._sync_path(rule))
        body = rule.body if rule.body.endswith("\n") else rule.body + "\n"
        action: dict[str, str] = {"action": "update", "file_path": full_path, "content": body}
        base = last_commit_id if last_commit_id is not None else rule.last_commit_id
        if not force and base:
            action["last_commit_id"] = base

        commit_sha = await self._write_commit(action, commit_message or f"Update {full_path} via DetectFlow")
        logger.info(f"Pushed rule update {full_path} → commit {commit_sha} (GitLab {self.project_path})")
        return commit_sha

    async def delete_rule(
        self,
        file_path: str,
        *,
        last_commit_id: str | None = None,
        force: bool = False,
        commit_message: str | None = None,
    ) -> str:
        full_path = self._full_path(file_path)
        action: dict[str, str] = {"action": "delete", "file_path": full_path}
        if not force and last_commit_id:
            action["last_commit_id"] = last_commit_id

        commit_sha = await self._write_commit(action, commit_message or f"Delete {full_path} via DetectFlow")
        logger.info(f"Pushed rule delete {full_path} → commit {commit_sha} (GitLab {self.project_path})")
        return commit_sha

    @staticmethod
    def _sync_path(rule: GitlabRule) -> str:
        return rule.file_path if rule.file_path else f"{rule.id}.yml"
