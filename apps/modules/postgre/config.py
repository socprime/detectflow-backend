import json
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Config, Integration
from apps.core.schemas import FlinkDefaultsResponse, FlinkDefaultsUpdateRequest
from apps.modules.postgre.base import BaseDAO

# Key for Flink defaults in config table
FLINK_DEFAULTS_KEY = "flink_defaults"


class ConfigDAO(BaseDAO[Config]):
    def __init__(self, session: AsyncSession):
        super().__init__(Config, session)

    async def get_by_key(self, key: str) -> Config | None:
        """Get config entry by key"""
        result = await self.session.execute(select(self.model).where(self.model.key == key))
        return result.scalar_one_or_none()

    async def upsert_by_key(self, key: str, value: str) -> Config:
        """Create or update config entry by key"""
        existing = await self.get_by_key(key)
        if existing:
            await self.update(existing.id, value=value)
            await self.session.refresh(existing)
            return existing
        else:
            return await self.create(key=key, value=value)

    async def get_socprime_tdm_provider_config(self) -> dict[str, Any] | None:
        """Return a shallow copy of the first ``socprime_tdm`` integration ``config`` JSON.

        Used to construct :class:`~apps.integrations.providers.socprime_tdm.SocPrimeTdmProvider`
        (``api_key`` from the profile; TDM REST base URL comes from
        :attr:`apps.core.settings.Settings.tdm_api_base_url`). Returns ``None`` if no row exists,
        ``config`` is empty, or ``api_key`` is missing/falsy.
        """
        result = await self.session.execute(
            select(Integration)
            .where(Integration.integration_type == "socprime_tdm")
            .order_by(Integration.created.asc())
            .limit(1)
        )
        integration = result.scalar_one_or_none()
        if not integration or not integration.config:
            return None
        api_key = integration.config.get("api_key")
        if not api_key:
            return None
        return dict(integration.config)

    async def get_api_key(self) -> str | None:
        """Return only the API key from :meth:`get_socprime_tdm_provider_config` (convenience)."""
        cfg = await self.get_socprime_tdm_provider_config()
        return str(cfg["api_key"]) if cfg else None

    @staticmethod
    def is_push_enabled_config(cfg: dict[str, Any] | None) -> bool:
        """Push-enabled decision for an already-fetched TDM provider config.

        Callers that also need the config (e.g. to build ``SocPrimeTdmProvider``) can fetch it once
        via :meth:`get_socprime_tdm_provider_config` and pass it here instead of re-reading the DB.
        ``None`` (no profile/key) cannot push; otherwise ``config.push`` (default ``True``).
        """
        if not cfg:
            return False  # cannot push without a configured key
        return bool(cfg.get("push", True))

    async def is_tdm_push_enabled(self) -> bool:
        """True when a TDM profile+key exists and its ``config.push`` is enabled (default True)."""
        return self.is_push_enabled_config(await self.get_socprime_tdm_provider_config())

    async def get_tdm_push_reenable_strategy(self) -> str:
        """Return the push re-enable reconciliation strategy (always ``next_sync``).

        ``push_reenable_strategy`` is no longer an exposed TDM config field, so this effectively
        always resolves to ``next_sync``. The plumbing (config read + ``next_save`` handling in the
        reconciler) is kept intact so the ``next_save`` safety mode can be re-enabled later without
        re-wiring the flow.
        """
        cfg = await self.get_socprime_tdm_provider_config()
        return str((cfg or {}).get("push_reenable_strategy") or "next_sync")

    async def get_gitlab_provider_config(self, integration_id: UUID) -> dict[str, Any] | None:
        """Return config for a GitLab integration profile when ``token`` is present."""
        result = await self.session.execute(select(Integration).where(Integration.id == integration_id))
        integration = result.scalar_one_or_none()
        if not integration or integration.integration_type != "gitlab" or not integration.config:
            return None
        token = integration.config.get("token")
        if not token:
            return None
        return dict(integration.config)

    # =========================================================================
    # Flink Defaults Methods
    # =========================================================================

    async def get_flink_defaults(self) -> FlinkDefaultsResponse:
        """Get Flink defaults from config table.

        Returns stored defaults merged with system defaults.
        If no config exists, returns system defaults.
        """
        existing = await self.get_by_key(FLINK_DEFAULTS_KEY)

        if existing:
            try:
                stored_data = json.loads(existing.value)
                # Merge with system defaults (stored values override defaults)
                return FlinkDefaultsResponse(**stored_data)
            except (json.JSONDecodeError, ValueError):
                # Invalid JSON - return system defaults
                pass

        # Return system defaults
        return FlinkDefaultsResponse()

    async def set_flink_defaults(self, update: FlinkDefaultsUpdateRequest) -> FlinkDefaultsResponse:
        """Update Flink defaults (partial update).

        Only provided (non-None) fields are updated.
        Returns the updated defaults.
        """
        # Get current defaults
        current = await self.get_flink_defaults()
        current_dict = current.model_dump()

        # Apply updates (only non-None fields)
        update_dict = update.model_dump(exclude_none=True)
        current_dict.update(update_dict)

        # Save to DB
        await self.upsert_by_key(
            key=FLINK_DEFAULTS_KEY,
            value=json.dumps(current_dict),
        )

        return FlinkDefaultsResponse(**current_dict)
