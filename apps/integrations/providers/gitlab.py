from typing import Any

from pydantic import BaseModel, Field, field_validator

from apps.clients.gitlab_api import GitLabApiClient, GitLabApiError
from apps.core.enums import IntegrationCategory
from apps.integrations.base import BaseIntegrationProvider, ConnectionResult, PullableIntegration
from apps.integrations.registry import IntegrationRegistry
from apps.modules.utils import normalize_pem_bundle


class GitLabConfig(BaseModel):
    """Config schema for GitLab integration."""

    base_url: str = Field(
        default="https://gitlab.com",
        description="GitLab host base URL",
        json_schema_extra={"ui_order": 1, "placeholder": "https://gitlab.com", "immutable_after_create": True},
    )
    token: str = Field(
        ...,
        description="Personal, project, or group access token",
        json_schema_extra={"sensitive": True, "ui_order": 2},
    )
    project_path: str = Field(
        ...,
        description="Project path without .git (e.g. group/subgroup/project)",
        json_schema_extra={
            "ui_order": 3,
            "placeholder": "my-group/my-sigma-repo",
            "immutable_after_create": True,
        },
    )
    branch: str = Field(
        default="main",
        description="Branch to pull from and push to",
        json_schema_extra={"ui_order": 4, "immutable_after_create": True},
    )
    subpath: str = Field(
        default="",
        description="Directory within the repo that holds Sigma rules (empty = repo root)",
        json_schema_extra={"ui_order": 5, "placeholder": "rules", "immutable_after_create": True},
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify TLS certificates (disable only as a last resort for self-signed hosts)",
        json_schema_extra={"ui_order": 6},
    )
    ca_cert: str = Field(
        default="",
        description=(
            "Custom CA certificate chain (PEM) used to verify a self-hosted GitLab host behind a "
            "private CA. Preferred over disabling verify_ssl. Leave empty to use the system CA store."
        ),
        json_schema_extra={
            "ui_order": 7,
            "multiline": True,
            "placeholder": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
        },
    )
    auto_pull: bool = Field(
        default=True,
        description="Pull rules automatically on the scheduler interval",
        json_schema_extra={"ui_order": 8},
    )

    @field_validator("base_url")
    @classmethod
    def _validate_base_url(cls, value: str) -> str:
        value = value.strip()
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url must start with http:// or https://")
        return value.rstrip("/")

    @field_validator("project_path")
    @classmethod
    def _validate_project_path(cls, value: str) -> str:
        value = value.strip().strip("/")
        if not value:
            raise ValueError("project_path must not be empty")
        return value

    @field_validator("ca_cert")
    @classmethod
    def _validate_ca_cert(cls, value: str) -> str:
        value = value.strip()
        if not value:
            return value
        if "-----BEGIN CERTIFICATE-----" not in value:
            raise ValueError("ca_cert must be a PEM-encoded certificate")
        # Re-wraps into strict PEM so a bundle whose newlines were flattened by a single-line
        # form field (see GitLabApiClient._ssl_verify) is stored in a form OpenSSL can load.
        return normalize_pem_bundle(value)


@IntegrationRegistry.register
class GitLabProvider(BaseIntegrationProvider, PullableIntegration):
    integration_type = "gitlab"
    display_name = "GitLab"
    category = IntegrationCategory.GIT_HOSTING
    description = "Synchronize Sigma rules with a GitLab project (pull and push)."
    icon = "gitlab"

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return GitLabConfig

    @classmethod
    def immutable_config_fields(cls) -> tuple[str, ...]:
        return ("base_url", "project_path", "branch", "subpath")

    @classmethod
    def sync_identity_values(cls, config: dict[str, Any]) -> dict[str, str]:
        """Normalized GitLab sync identity used to detect forbidden config changes."""
        validated = GitLabConfig.model_validate(config or {})
        return {
            "base_url": validated.base_url.rstrip("/"),
            "project_path": validated.project_path.strip("/"),
            "branch": validated.branch,
            "subpath": validated.subpath.strip("/"),
        }

    @classmethod
    def changed_immutable_config_fields(cls, existing_config: dict, merged_config: dict) -> list[str]:
        old = cls.sync_identity_values(existing_config)
        new = cls.sync_identity_values(merged_config)
        return [field for field in cls.immutable_config_fields() if old[field] != new[field]]

    async def check_connection(self) -> ConnectionResult:
        try:
            client = GitLabApiClient.from_integration_config(self.config)
            commit_sha = await client.get_latest_commit_sha()
            if commit_sha is None:
                return ConnectionResult(
                    success=False,
                    message=f"Branch '{client.branch}' not found or empty in project '{client.project_path}'",
                )
            return ConnectionResult(
                success=True,
                message="Connected to GitLab successfully",
                details={"commit_sha": commit_sha},
            )
        except GitLabApiError as exc:
            return ConnectionResult(success=False, message=str(exc))
