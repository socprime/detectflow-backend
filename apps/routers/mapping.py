"""Field mapping generation endpoints.

This module provides REST API endpoints for generating field mappings
between parsed log data and Sigma rule fields using AI assistance.
"""

import asyncio
from uuid import UUID

import orjson
from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from schema_parser.core.exceptions import ParseJsonFunctionError, RegexFunctionError
from schema_parser.manager import ParserManager as SchemaParserManager
from sqlalchemy.ext.asyncio import AsyncSession

from apps.clients.tdm_api import TDMAPIClient
from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.exceptions import BadRequestError
from apps.core.models import User
from apps.core.schemas import ErrorResponse
from apps.modules.kafka.parsers import KafkaParsersEventsReader
from apps.modules.postgre.config import ConfigDAO
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import get_fields_from_sigma


class GenerateMappingRequest(BaseModel):
    """Request for AI-assisted field mapping generation."""

    repository_ids: list[str] = Field(description="Repository IDs to extract Sigma fields from")
    topics: list[str] = Field(description="Kafka topics to sample events from")
    parser_query: str = Field(description="Parser query to apply to events")


class GenerateMappingResponse(BaseModel):
    """Response with generated field mapping."""

    mapping: str = Field(description="Generated field mapping configuration")


class GenerateMappingPromptResponse(BaseModel):
    prompt: str


class GetSigmaFieldsRequest(BaseModel):
    """Request for getting Sigma fields from repositories."""

    repository_ids: list[str] = Field(description="Repository IDs to extract Sigma fields from")


class GetSigmaFieldsResponse(BaseModel):
    """Response with Sigma fields."""

    sigma_fields: list[str] = Field(description="List of unique Sigma fields from the specified repositories")


router = APIRouter(tags=["Mapping"])


@router.post(
    "/api/v1/generate-mapping",
    response_model=GenerateMappingResponse,
    summary="Generate field mapping",
    responses={
        200: {"description": "Generated field mapping"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def generate_mapping(
    request: GenerateMappingRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Generate field mapping using AI assistance.

    Analyzes parsed events from Kafka topics and Sigma rule fields
    from specified repositories to suggest field mappings.
    """
    config_dao = ConfigDAO(session=db)
    tdm_api_client = TDMAPIClient(api_key=await config_dao.get_api_key())

    sigma_fields, event_fields = await asyncio.gather(
        _get_sigma_fields(request.repository_ids, db),
        _get_event_fields(request.topics, request.parser_query),
    )

    if not sigma_fields:
        raise BadRequestError("Failed to get fields from Sigma rules")
    if not event_fields:
        raise BadRequestError("Failed to get fields from events")

    mapping = await tdm_api_client.generate_mapping(sigma_fields=sigma_fields, event_fields=event_fields)

    return GenerateMappingResponse(mapping=mapping)


@router.post(
    "/api/v1/generate-mapping-prompt",
    response_model=GenerateMappingPromptResponse,
    summary="Generate mapping prompt",
    responses={
        200: {"description": "Generated mapping prompt"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def generate_mapping_prompt(
    request: GenerateMappingRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    sigma_fields, event_fields = await asyncio.gather(
        _get_sigma_fields(request.repository_ids, db),
        _get_event_fields(request.topics, request.parser_query),
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

    return GenerateMappingPromptResponse(prompt=prompt)


@router.post(
    "/api/v1/get-sigma-fields",
    response_model=GetSigmaFieldsResponse,
    summary="Get Sigma fields from repositories",
    responses={
        200: {"description": "List of Sigma fields"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Validation error"},
    },
)
async def get_sigma_fields(
    request: GetSigmaFieldsRequest,
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get Sigma fields from specified repositories.

    Extracts all unique field names referenced by Sigma rules
    in the specified repositories.
    """
    sigma_fields = await _get_sigma_fields(request.repository_ids, db)
    return GetSigmaFieldsResponse(sigma_fields=sorted(sigma_fields))


async def _get_sigma_fields(repository_ids: list[str], db: AsyncSession) -> list[str]:
    rule_dao = RuleDAO(session=db)
    sigma_fields = set()
    rules = await rule_dao.get_all_by_repository(
        repository_ids=[UUID(repository_id) for repository_id in repository_ids]
    )
    for rule in rules:
        sigma_fields.update(get_fields_from_sigma(rule.body))
    return list(sigma_fields)


async def _get_event_fields(topics: list[str], parser_query: str) -> list[str]:
    parser_manager = SchemaParserManager()
    kafka_manager = KafkaParsersEventsReader()
    parser_config = parser_manager.query_parser(query=parser_query)
    parsed_events: list[dict] = []
    for topic in topics:
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
