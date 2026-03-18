"""Business logic for Sigma/event fields and mapping generation.

Provides MappingManager for extracting Sigma fields from repositories, sampling
and parsing event fields from Kafka topics, and running mapping generation (Sigma + event fields → TDM API).
"""

import asyncio
from uuid import UUID

import orjson
from schema_parser.core.exceptions import ParseJsonFunctionError, RegexFunctionError
from schema_parser.manager import ParserManager as SchemaParserManager
from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.tdm_api import TDMAPIClient
from apps.core.exceptions import BadRequestError
from apps.modules.kafka.activity import activity_producer
from apps.modules.kafka.parsers import KafkaParsersEventsReader
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import get_fields_from_sigma


class MappingManager:
    """Manager for field mapping generation operations."""

    def __init__(self, db: AsyncSession):
        """Initialize manager with database session."""
        self.db = db
        self.config_dao = ConfigDAO(session=db)
        self.rule_dao = RuleDAO(session=db)

    async def generate_mapping(
        self,
        repository_ids: list[str],
        topics: list[str],
        parser_query: str,
    ) -> str:
        """Generate field mapping using AI assistance.

        Analyzes parsed events from Kafka topics and Sigma rule fields
        from specified repositories to suggest field mappings.

        Args:
            repository_ids: Repository IDs to extract Sigma fields from
            topics: Kafka topics to sample events from
            parser_query: Parser query to apply to events

        Returns:
            Generated field mapping configuration

        Raises:
            BadRequestError: If no fields found or parsing fails
        """
        api_key = await self.config_dao.get_api_key()
        if not api_key:
            raise BadRequestError("SOCPrime API key is not configured")
        tdm_api_client = TDMAPIClient(api_key=api_key)

        # Fetch sigma fields and event fields in parallel
        try:
            sigma_fields, event_fields = await asyncio.gather(
                self._get_sigma_fields(repository_ids),
                self._get_event_fields(topics, parser_query),
                return_exceptions=True,
            )
        except Exception as e:
            # Unexpected error in gather itself (system error)
            await activity_producer.log_action(
                action="error",
                entity_type="mapping",
                details=f"Unexpected error during field extraction: {str(e)}",
                source="system",
                severity="error",
            )
            raise

        # Handle exceptions from gather
        if isinstance(sigma_fields, Exception):
            # BadRequestError = user input issue, other exceptions = system error
            is_user_error = isinstance(sigma_fields, BadRequestError)
            await activity_producer.log_action(
                action="warning" if is_user_error else "error",
                entity_type="mapping",
                details=f"Failed to extract Sigma fields: {str(sigma_fields)}",
                source="user" if is_user_error else "system",
                severity="warning" if is_user_error else "error",
            )
            raise sigma_fields

        if isinstance(event_fields, Exception):
            # BadRequestError = user input issue (invalid parser query), other = system error
            is_user_error = isinstance(event_fields, BadRequestError)
            await activity_producer.log_action(
                action="warning" if is_user_error else "error",
                entity_type="mapping",
                details=f"Failed to parse events: {str(event_fields)}",
                source="user" if is_user_error else "system",
                severity="warning" if is_user_error else "error",
            )
            raise event_fields

        # Validate results - these are user input validation errors
        # (user selected repositories with no rules, or topics with no parseable events)
        if not sigma_fields:
            await activity_producer.log_action(
                action="warning",
                entity_type="mapping",
                details="No Sigma fields found in selected repositories",
                source="user",
                severity="warning",
            )
            raise BadRequestError("It looks like there's no Sigma fields for mapping")

        if not event_fields:
            await activity_producer.log_action(
                action="warning",
                entity_type="mapping",
                details="No event fields found after parsing sample events",
                source="user",
                severity="warning",
            )
            raise BadRequestError("It looks like there's no log event fields for mapping")

        # Generate mapping via TDM API (system error if fails - external service)
        try:
            mapping = await tdm_api_client.generate_mapping(
                sigma_fields=sigma_fields,
                event_fields=event_fields,
            )
        except Exception as e:
            await activity_producer.log_action(
                action="error",
                entity_type="mapping",
                details=f"TDM API mapping generation failed: {str(e)}",
                source="system",
                severity="error",
            )
            raise

        return mapping

    async def get_sigma_fields(self, repository_ids: list[str]) -> list[str]:
        """Get Sigma fields from specified repositories.

        Args:
            repository_ids: Repository IDs to extract Sigma fields from

        Returns:
            Sorted list of unique Sigma field names
        """
        fields = await self._get_sigma_fields(repository_ids)
        return sorted(fields)

    async def generate_mapping_prompt(
        self,
        repository_ids: list[str],
        topics: list[str],
        parser_query: str,
    ) -> str:
        """Generate a prompt for manual mapping generation.

        Args:
            repository_ids: Repository IDs to extract Sigma fields from
            topics: Kafka topics to sample events from
            parser_query: Parser query to apply to events

        Returns:
            Prompt string for AI-assisted mapping

        Raises:
            BadRequestError: If no fields found
        """
        sigma_fields, event_fields = await asyncio.gather(
            self._get_sigma_fields(repository_ids),
            self._get_event_fields(topics, parser_query),
        )

        if not sigma_fields:
            raise BadRequestError("Failed to get fields from Sigma rules")
        if not event_fields:
            raise BadRequestError("Failed to get fields from events")

        prompt = f"""You are a detection engineer. Your task is to generate a Sigma to Event field mapping.

Input:
Sigma fields: fields referenced by Sigma rules.
Event fields: available fields from the target event schema (may include dot-notation).

Output requirements (strict):
1) Output RAW YAML only. Do not use Markdown. Do not add explanations or any extra text.
2) YAML must be a mapping where each key is a sigma field and each value is either:
   - a single event field string, or
   - a list of event field strings ordered best match first.
3) Only include sigma fields that require translation:
   - If a sigma field name matches an event field exactly (case-sensitive), omit it from output.
4) Event fields list may be incomplete (it can be based on a small sample). Prefer mapping to fields that appear
   in the provided Event fields list, but you MAY output additional event field names if they are strongly implied
   by common schemas/conventions and the observed field structure. Do not make wild guesses.
5) If you cannot map a sigma field confidently to any event field, omit it.
6) Prefer semantic equivalence (same meaning), not just string similarity. Consider common schemas/conventions
   (for example: process.*, user.*, host.*, source.*, destination.*, file.*, registry.*, network.*).

Example output (YAML):
sigma_field_one: event.field.one
sigma_field_two:
  - event.field.a
  - event.field.b

Sigma fields:
{" ".join(sigma_fields)}

Event fields:
{" ".join(event_fields)}"""

        return prompt

    async def _get_sigma_fields(self, repository_ids: list[str]) -> list[str]:
        """Extract Sigma fields from rules in specified repositories."""
        if not repository_ids:
            raise BadRequestError("repository_ids cannot be empty")
        uuids: list[UUID] = []
        for i, repo_id in enumerate(repository_ids):
            if not isinstance(repo_id, str) or not repo_id.strip():
                raise BadRequestError(f"Invalid repository_id at index {i}: expected non-empty UUID string")
            try:
                uuids.append(UUID(repo_id.strip()))
            except ValueError:
                raise BadRequestError(f"Invalid repository_id at index {i}: not a valid UUID") from None
        sigma_fields = set()
        rules = await self.rule_dao.get_all_by_repository(repository_ids=uuids)
        for rule in rules:
            sigma_fields.update(get_fields_from_sigma(rule.body))
        return list(sigma_fields)

    async def _get_event_fields(self, topics: list[str], parser_query: str) -> list[str]:
        """Extract event fields by parsing sample events from Kafka."""
        parser_manager = SchemaParserManager()
        kafka_manager = KafkaParsersEventsReader()
        parser_config = parser_manager.query_parser(query=parser_query)
        parsed_events: list[dict] = []

        for topic in topics:
            if topic:
                source_data = await kafka_manager.get_events(topic=topic, limit=50)
                for event in source_data:
                    try:
                        parsed_event = parser_manager.configured_parser(
                            event=orjson.loads(event), parser_config=parser_config, flatten=True
                        )
                    except (ParseJsonFunctionError, RegexFunctionError) as e:
                        raise BadRequestError(f"Failed to parse event: {e}") from e
                    parsed_events.append(parsed_event)

        event_fields = set()
        for event in parsed_events:
            event_fields.update(event.keys())
        return list(event_fields)
