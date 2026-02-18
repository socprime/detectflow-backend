import base64
import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.models import Config
from apps.core.schemas import FlinkDefaultsResponse, FlinkDefaultsUpdateRequest
from apps.modules.postgre.base import BaseDAO

# TODO: add encryption/decryption of API key or save it in secrets manager

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

    async def get_api_key(self) -> str | None:
        """Get API key from config"""
        existing = await self.get_by_key(key="repository_api_key")
        return self._decode_api_key(existing.value) if existing else None

    async def set_api_key(self, api_key: str) -> None:
        """Set API key in config"""
        await self.upsert_by_key(key="repository_api_key", value=self._encode_api_key(api_key))

    @staticmethod
    def _encode_api_key(api_key: str) -> str:
        """Encode API key"""
        return base64.b64encode(api_key.encode()).decode()

    @staticmethod
    def _decode_api_key(encoded_api_key: str) -> str:
        """Decode API key"""
        return base64.b64decode(encoded_api_key.encode()).decode()

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
