"""Parser testing endpoints.

This module provides REST API endpoints for testing parser queries
against real Kafka topic data before applying them to log sources.
"""

from fastapi import APIRouter, Depends, HTTPException

from apps.core.auth import get_current_active_user
from apps.core.logger import get_logger
from apps.core.models import User
from apps.core.schemas import (
    ErrorResponse,
    ParserRunRequest,
    ParserRunResponse,
)
from apps.managers.parser import parser_manager
from apps.modules.kafka.topics import get_topics_from_kafka

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/parsers", tags=["Parsers"])


@router.post(
    "/run",
    response_model=ParserRunResponse,
    summary="Test parser query",
    responses={
        200: {"description": "Parser test results"},
        400: {"description": "One or more topics not found"},
        401: {"model": ErrorResponse, "description": "Not authenticated"},
        422: {"description": "Invalid parser query"},
    },
)
async def run_parser(
    parser_run: ParserRunRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Test a parser query against real Kafka topic data.

    Executes the parser query on sample events from specified topics
    and returns the parsing results. Useful for validating parsers
    before applying them to log sources.
    """
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
        field_mapping=parser_run.mapping,
    )
    return ParserRunResponse(result=test_results)
