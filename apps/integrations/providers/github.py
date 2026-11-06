from pydantic import BaseModel, Field

from apps.core.enums import IntegrationCategory
from apps.integrations.base import BaseIntegrationProvider, ConnectionResult
from apps.integrations.registry import IntegrationRegistry


class GitHubConfig(BaseModel):
    """Config schema for GitHub integration."""

    personal_access_token: str = Field(
        ...,
        description="GitHub Personal Access Token",
        json_schema_extra={"sensitive": True, "ui_order": 1, "placeholder": "ghp_xxxxxxxxxxxx"},
    )
    username: str = Field(
        default="",
        description="GitHub Username",
        json_schema_extra={"ui_order": 2, "placeholder": "your-username"},
    )


@IntegrationRegistry.register
class GitHubProvider(BaseIntegrationProvider):
    integration_type = "github"
    display_name = "GitHub"
    category = IntegrationCategory.GIT_HOSTING
    description = "Synchronize with a GitHub repository."
    icon = "github"

    @classmethod
    def get_config_schema(cls) -> type[BaseModel]:
        return GitHubConfig

    async def check_connection(self) -> ConnectionResult:
        return ConnectionResult(success=False, message="GitHub integration is not yet implemented")
