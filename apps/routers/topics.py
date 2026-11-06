"""Kafka topics endpoints.

This module provides REST API endpoints for viewing Kafka topics
and their associations with ETL pipelines.
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from apps.core.auth import get_current_active_user
from apps.core.database import get_db
from apps.core.logger import get_logger
from apps.core.models import Pipeline, User
from apps.core.openapi import UNAUTHORIZED, err, errors
from apps.core.schemas import (
    PipelineSimpleResponse,
    TopicDetailResponse,
    TopicEventItem,
    TopicsEventsRequest,
    TopicsResponse,
    destination_topics_from_pipeline,
)
from apps.modules.kafka.parsers import KafkaParsersEventsReader
from apps.modules.kafka.topics import get_topics_from_kafka

router = APIRouter(prefix="/api/v1", tags=["Topics"])
logger = get_logger(__name__)


async def _fetch_pipelines(db: AsyncSession):
    """Fetch pipelines from db with only id, name, source_topics, destinations.

    ``destination_topic`` is selected as the legacy fallback for
    ``destination_topics_from_pipeline``.

    Returns Row objects that can be accessed via attribute notation (e.g., row.id, row.name).
    """
    result = await db.execute(
        select(
            Pipeline.id,
            Pipeline.name,
            Pipeline.source_topics,
            Pipeline.destinations,
            Pipeline.destination_topic,
        )
    )
    return result.all()


@router.get(
    "/topics",
    response_model=TopicsResponse,
    summary="List Kafka topics",
    responses=errors(UNAUTHORIZED),
)
async def get_topics(
    search: str | None = Query(None, description="Filter by topic name (case-insensitive substring)"),
    type_filter: str | None = Query(
        None,
        alias="type",
        description="Filter by how pipelines use the topic: source, destination, or both",
    ),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    limit: int = Query(50, ge=1, le=200, description="Page size"),
    _: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all Kafka topics with their pipeline associations.

    Topic names are fetched live from the Kafka cluster and cross-referenced with the
    pipelines in the database, so each topic shows which pipelines use it as a source,
    destination, or both. Read-only; nothing is published. Results are filterable by name
    and usage type, then paginated.
    """
    topic_names, pipelines = await asyncio.gather(
        get_topics_from_kafka(),
        _fetch_pipelines(db),
    )

    # Build topic info map with pipelines and their types
    # Structure: {topic_name: {pipeline_id: {"name": str, "type": str}}}
    topic_info: dict[str, dict[str, dict]] = {topic_name: {} for topic_name in topic_names}

    # Associate pipelines with topics, tracking how each pipeline uses the topic
    for pipeline in pipelines:
        pipeline_id_str = str(pipeline.id)

        # Handle multiple source topics
        source_topics_list = pipeline.source_topics or []
        for source_topic in source_topics_list:
            if source_topic in topic_info:
                topic_info[source_topic][pipeline_id_str] = {
                    "name": pipeline.name,
                    "type": "source",
                }

        # Handle destination topics (all destinations[].topic)
        for dest_topic in destination_topics_from_pipeline(pipeline):
            if dest_topic not in topic_info:
                continue
            if pipeline_id_str in topic_info[dest_topic]:
                topic_info[dest_topic][pipeline_id_str]["type"] = "both"
            else:
                topic_info[dest_topic][pipeline_id_str] = {
                    "name": pipeline.name,
                    "type": "destination",
                }

    # Determine topic-level type for filtering
    def get_topic_type(pipelines_dict: dict) -> str:
        if not pipelines_dict:
            return "unused"
        types = {p["type"] for p in pipelines_dict.values()}
        if "source" in types and "destination" in types:
            return "both"
        if "both" in types:
            return "both"
        if "source" in types:
            return "source"
        if "destination" in types:
            return "destination"
        return "unused"

    # Build result list with filtering
    all_topics = []
    for topic_name in sorted(topic_info.keys()):
        pipelines_dict = topic_info[topic_name]
        topic_type = get_topic_type(pipelines_dict)

        # Apply search filter
        if search and search.lower() not in topic_name.lower():
            continue

        # Apply type filter (filter by topic-level type)
        if type_filter and topic_type != type_filter:
            continue

        related_pipelines = [
            PipelineSimpleResponse(id=pid, name=pinfo["name"], type=pinfo["type"])
            for pid, pinfo in pipelines_dict.items()
        ]
        all_topics.append(TopicDetailResponse(name=topic_name, pipelines=related_pipelines))

    # Apply pagination
    total = len(all_topics)
    paginated = all_topics[offset : offset + limit]

    return TopicsResponse(data=paginated, total=total, offset=offset, limit=limit)


@router.post(
    "/topic-events",
    response_model=list[TopicEventItem],
    summary="Sample events from topics",
    responses=errors(
        UNAUTHORIZED,
        err(400, "One or more requested topics do not exist in Kafka"),
    ),
)
async def get_topics_events(
    request: TopicsEventsRequest,
    _: User = Depends(get_current_active_user),
):
    """
    Sample recent events from a list of Kafka topics.

    Reads each requested topic directly from Kafka and returns a flat list of
    `TopicEventItem` (each carries the source `topic` name and the raw `event` payload as a
    string). At most **10 events per topic** are returned; topics are read concurrently.
    Read-only — nothing is consumed permanently or published. All requested topics must
    exist, otherwise the request is rejected with 400.
    """
    # Validate that all requested topics exist
    available_topics = await get_topics_from_kafka()
    invalid_topics = [topic for topic in request.topics if topic not in available_topics]

    if invalid_topics:
        raise HTTPException(
            status_code=400,
            detail=f"Topics not found: {', '.join(invalid_topics)}",
        )

    kafka_service = KafkaParsersEventsReader()
    events: list[TopicEventItem] = []

    tasks = [kafka_service.get_events(topic=topic, limit=10) for topic in request.topics]
    results = await asyncio.gather(*tasks)

    for topic, result in zip(request.topics, results, strict=True):
        for event in result:
            events.append(TopicEventItem(topic=topic, event=event))

    return events
