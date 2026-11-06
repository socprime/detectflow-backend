"""Sigma rule validation service.

This service validates Sigma rules against the current rule loader module version.
Rules that cannot be processed by the Flink job are marked as unsupported.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.logger import get_logger
from apps.core.models import Pipeline, Rule, RuleLoaderModuleVersion
from apps.modules.postgre.pipeline_rules import PipelineRulesDAO

logger = get_logger(__name__)

# Graceful degradation: schema-parser sigma_validation may not be available
try:
    import schema_parser
    from schema_parser.sigma_validation import SigmaValidator

    SIGMA_VALIDATOR_AVAILABLE = True
    SCHEMA_PARSER_VERSION = getattr(schema_parser, "__version__", None)
except ImportError:
    SIGMA_VALIDATOR_AVAILABLE = False
    SCHEMA_PARSER_VERSION = None
    SigmaValidator = None
    logger.warning("schema_parser.sigma_validation not available, rules will be marked as supported by default")


@dataclass
class ValidationResult:
    """Result of a single rule validation."""

    is_supported: bool
    reason: str | None = None


def _validate_bodies_sync(bodies: list[str]) -> list[ValidationResult]:
    """Validate rule bodies — pure CPU (YAML parse + Sigma checks), NO event loop.

    Validating a large batch inline in an async handler blocks the event loop
    for the whole run, stalling every concurrent request and SSE stream. Always
    call this via ``asyncio.to_thread`` from async code.

    One ``SigmaValidator`` instance is reused for the whole batch instead of
    constructing a new one per rule.
    """
    if not SIGMA_VALIDATOR_AVAILABLE:
        return [ValidationResult(is_supported=True) for _ in bodies]

    validator = SigmaValidator()
    results: list[ValidationResult] = []
    for body in bodies:
        try:
            result = validator.validate(body)
            if result.is_supported:
                results.append(ValidationResult(is_supported=True))
            else:
                results.append(ValidationResult(is_supported=False, reason=result.unsupported_reason))
        except (ValueError, TypeError) as e:
            # Invalid input types
            logger.warning(f"Invalid rule body type: {e}")
            results.append(ValidationResult(is_supported=False, reason=f"Invalid rule format: {e}"))
        except Exception as e:
            # Unexpected errors - log with traceback for debugging (ISSUE #6 fix)
            logger.exception(f"Unexpected error validating rule: {e}")
            results.append(ValidationResult(is_supported=False, reason=f"Validation error: {type(e).__name__}: {e}"))
    return results


class SigmaValidationService:
    """Service for validating Sigma rules against the rule loader module."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_current_module_version(self) -> str | None:
        """Get current rule loader module version from DB.

        Returns:
            Current version string, or None if no version is set.
        """
        result = await self.db.execute(
            select(RuleLoaderModuleVersion).where(RuleLoaderModuleVersion.is_current.is_(True))
        )
        version_row = result.scalar_one_or_none()
        return version_row.version if version_row else None

    async def validate_rule(
        self,
        rule: Rule,
        force: bool = False,
    ) -> bool:
        """Validate a single rule if needed.

        Skip validation if:
        - rule.validated_with_version == current_version AND force=False

        Args:
            rule: Rule model to validate.
            force: Force re-validation even if already validated with current version.

        Returns:
            True if rule is supported, False otherwise.
        """
        current_version = await self.get_current_module_version()

        # Skip if already validated with current version (unless forced)
        if not force and current_version and rule.validated_with_version == current_version:
            return rule.is_supported

        # Perform validation using schema-parser (in the threadpool — CPU-heavy)
        validation_result = (await asyncio.to_thread(_validate_bodies_sync, [rule.body]))[0]

        # Update rule with validation results
        rule.is_supported = validation_result.is_supported
        rule.unsupported_reason = validation_result.reason
        rule.validated_at = datetime.now(tz=UTC)
        rule.validated_with_version = current_version

        await self.db.flush()

        return validation_result.is_supported

    async def validate_rules_batch(
        self,
        rules: list[Rule],
        force: bool = False,
    ) -> dict[str, bool]:
        """Validate multiple rules, return {rule_id: is_supported}.

        Args:
            rules: List of Rule models to validate.
            force: Force re-validation even if already validated.

        Returns:
            Dictionary mapping rule_id (as string) to is_supported boolean.
        """
        current_version = await self.get_current_module_version()
        results: dict[str, bool] = {}

        # Split: already-validated rules are answered from the DB state; the rest
        # go through the CPU-heavy validator in ONE threadpool call, so the event
        # loop keeps serving requests while thousands of rules are validated.
        to_validate: list[Rule] = []
        for rule in rules:
            if not force and current_version and rule.validated_with_version == current_version:
                results[str(rule.id)] = rule.is_supported
            else:
                to_validate.append(rule)

        if to_validate:
            bodies = [rule.body for rule in to_validate]  # ORM access on the loop, not in the thread
            validation_results = await asyncio.to_thread(_validate_bodies_sync, bodies)

            validated_at = datetime.now(tz=UTC)
            for rule, validation_result in zip(to_validate, validation_results, strict=True):
                rule.is_supported = validation_result.is_supported
                rule.unsupported_reason = validation_result.reason
                rule.validated_at = validated_at
                rule.validated_with_version = current_version
                results[str(rule.id)] = validation_result.is_supported

            await self.db.flush()

        logger.info(
            f"Validated {len(rules)} rules: "
            f"{sum(1 for v in results.values() if v)} supported, "
            f"{sum(1 for v in results.values() if not v)} unsupported"
        )

        return results

    async def on_module_version_update(
        self,
        new_version: str,
    ) -> None:
        """Called when rule loader module version changes.

        1. Insert new version row (is_current=True)
        2. Set previous version is_current=False
        3. UPDATE pipelines SET needs_restart=True WHERE enabled=True

        Args:
            new_version: New version string to set as current.
        """
        logger.info(f"Updating rule loader module version to {new_version}")

        # Set all existing versions to not current
        await self.db.execute(
            update(RuleLoaderModuleVersion).where(RuleLoaderModuleVersion.is_current.is_(True)).values(is_current=False)
        )

        # Insert new version as current
        new_version_row = RuleLoaderModuleVersion(
            version=new_version,
            is_current=True,
        )
        self.db.add(new_version_row)

        # Mark all enabled pipelines as needing restart
        await self.db.execute(update(Pipeline).where(Pipeline.enabled.is_(True)).values(needs_restart=True))

        await self.db.commit()

        logger.info(f"Rule loader module version updated to {new_version}, enabled pipelines marked for restart")

    async def check_and_update_module_version(self) -> bool:
        """Check schema-parser version and update if changed.

        Called on application startup to detect schema-parser upgrades.

        Returns:
            True if version was updated, False if unchanged or unavailable.
        """
        if not SCHEMA_PARSER_VERSION:
            logger.warning("schema_parser.__version__ not found, skipping version check")
            return False

        try:
            current_db_version = await self.get_current_module_version()

            if current_db_version == SCHEMA_PARSER_VERSION:
                logger.info(f"Rule loader module version unchanged: {SCHEMA_PARSER_VERSION}")
                return False

            if current_db_version is None:
                # First time setup - just record the version without marking pipelines for restart
                logger.info(f"Recording initial rule loader module version: {SCHEMA_PARSER_VERSION}")
                new_version_row = RuleLoaderModuleVersion(
                    version=SCHEMA_PARSER_VERSION,
                    is_current=True,
                )
                self.db.add(new_version_row)
                await self.db.commit()
                return False

            # Version changed - trigger full update
            logger.info(f"Rule loader module version changed: {current_db_version} → {SCHEMA_PARSER_VERSION}")
            await self.on_module_version_update(SCHEMA_PARSER_VERSION)
            return True

        except Exception as e:
            logger.error(f"Error checking rule loader module version: {e}")
            return False

    async def clear_pipeline_restart_flag(self, pipeline_id: UUID) -> None:
        """Clear the needs_restart flag for a pipeline after it has been restarted.

        Args:
            pipeline_id: UUID of the pipeline to clear the flag for.
        """
        await self.db.execute(update(Pipeline).where(Pipeline.id == pipeline_id).values(needs_restart=False))
        await self.db.commit()

    async def get_supported_rules_count(self, pipeline_id: UUID) -> tuple[int, int]:
        """Get count of supported and total rules for a pipeline.

        Args:
            pipeline_id: Pipeline UUID.

        Returns:
            Tuple of (supported_count, total_count).
        """
        pipeline_rules_dao = PipelineRulesDAO(self.db)

        # Get all rules for the pipeline
        rules, total = await pipeline_rules_dao.get_by_pipeline(pipeline_id, limit=100000)

        supported_count = sum(1 for pr in rules if pr.rule and pr.rule.is_supported)

        return supported_count, total

    def _perform_validation(self, rule_body: str) -> ValidationResult:
        """Validate one rule body synchronously.

        Kept for compatibility; prefer ``asyncio.to_thread(_validate_bodies_sync, ...)``
        from async code — this call is CPU-heavy and must not run on the event loop.

        Args:
            rule_body: Sigma rule YAML body.

        Returns:
            ValidationResult with is_supported and optional reason.
        """
        return _validate_bodies_sync([rule_body])[0]
