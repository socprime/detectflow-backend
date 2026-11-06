"""Parser testing endpoints.

This module provides REST API endpoints for testing parser queries
against real Kafka topic data before applying them to log sources.
"""

from fastapi import APIRouter, Depends, HTTPException

from apps.core.auth import get_current_active_user
from apps.core.exceptions import BadRequestError
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.schemas import (
    ParserRunRequest,
    ParserRunResponse,
)
from apps.managers.parser import parser_manager
from apps.modules.kafka.topics import get_topics_from_kafka
from apps.modules.utils import normalize_field_mapping_yaml

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/parsers", tags=["Parsers"])


@router.post(
    "/run",
    response_model=ParserRunResponse,
    summary="Test parser query",
    responses=errors(
        UNAUTHORIZED,
        err(
            400,
            "Invalid mapping YAML, or one or more requested topics do not exist in Kafka",
        ),
    ),
)
async def run_parser(
    parser_run: ParserRunRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Test a parser query against real Kafka topic data.

    Compiles the parser query and runs it over a sample of recent events read live from the
    requested source topics, returning per-event parse results (parsed fields, success flag,
    and any per-event error). Useful for validating a parser before assigning it to a log
    source. Read-only — it does not persist the parser or publish anything to Kafka. When
    `mapping` is provided it is validated as YAML (dictionary of sigma fields to event field
    strings or lists); unquoted `@…` scalars such as `@timestamp` are auto-quoted. Invalid
    mapping or missing Kafka topics returns 400.
    """
    try:
        mapping = normalize_field_mapping_yaml(parser_run.mapping)
    except ValueError as e:
        raise BadRequestError(str(e)) from e

    # Validate that all requested topics exist
    available_topics = await get_topics_from_kafka()
    invalid_topics = [topic for topic in parser_run.source_topic_ids if topic not in available_topics]

    if invalid_topics:
        raise HTTPException(
            status_code=400,
            detail=f"Topics not found: {', '.join(invalid_topics)}",
        )

    test_results = await parser_manager.run_test_parser_query(
        topics=parser_run.source_topic_ids,
        parser_query=parser_run.parser_query,
        field_mapping=mapping,
    )
    return ParserRunResponse(result=test_results)
