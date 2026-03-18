"""Flink REST API client for metrics retrieval.

This module provides a client for querying Flink JobManager REST API
to retrieve job metrics including consumer lag (pendingRecords).

The client connects to Flink JobManager via Kubernetes Service created
by Flink Operator: {deployment_name}-rest:8081
"""

from dataclasses import dataclass

import httpx

from apps.core.error_tracker import ErrorTracker
from apps.core.logger import get_logger
from apps.core.settings import settings
from apps.modules.kafka.activity import activity_producer

logger = get_logger(__name__)

# Default timeout for Flink API requests (seconds)
DEFAULT_TIMEOUT = 10.0


@dataclass
class FlinkJobMetrics:
    """Flink job metrics from REST API."""

    job_id: str
    state: str
    # Consumer lag (pending records across all subtasks)
    pending_records: int | None = None
    # Custom gauges from Sigma processor (PRIMARY metrics for dashboard)
    input_events_per_second: float = 0.0  # Input events/sec from source
    output_tagged_per_second: float = 0.0  # Tagged events/sec to output
    output_untagged_per_second: float = 0.0  # Untagged events/sec (0 if matched_only)
    # Cumulative counters (for Kafka metrics consumer)
    matched_events: int = 0  # Events that matched at least one Sigma rule
    total_events: int = 0  # Total events processed
    # Timestamp of last processed window (for stale detection)
    last_window_timestamp_ms: int = 0


@dataclass
class FlinkJobStatus:
    """Status of a Flink job from REST API /jobs/overview endpoint."""

    job_id: str
    name: str
    state: str  # RUNNING, FAILED, CANCELED, FINISHED, etc.
    start_time: int | None = None
    duration: int | None = None
    # Task counts by status: {"RUNNING": 2, "FINISHED": 1}
    tasks: dict[str, int] | None = None


class FlinkRestClient:
    """Client for Flink JobManager REST API.

    Connects to Flink JobManager to retrieve job metrics including
    consumer lag (pendingRecords) which indicates Kafka consumer backlog.

    Attributes:
        base_url: Full URL to Flink REST API (e.g., http://flink-xxx-rest:8081)
        timeout: Request timeout in seconds
    """

    def __init__(self, base_url: str, timeout: float = DEFAULT_TIMEOUT):
        """Initialize Flink REST API client.

        Args:
            base_url: Full URL to Flink REST API
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    async def get_jobs(self) -> list[dict]:
        """Get list of all jobs.

        Returns:
            List of job dicts with id and status fields
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/jobs")
            response.raise_for_status()
            return response.json().get("jobs", [])

    async def get_job_status(self) -> FlinkJobStatus | None:
        """Get status of the running job (or first job if none running).

        Uses GET /jobs which returns [{id, status}] - lightweight endpoint.

        Returns:
            FlinkJobStatus for the running job, or None if no jobs found
        """
        try:
            jobs = await self.get_jobs()  # GET /jobs → [{id, status}]
            if not jobs:
                return None

            # Prefer RUNNING job
            for job in jobs:
                if job.get("status") == "RUNNING":
                    return FlinkJobStatus(
                        job_id=job.get("id", ""),
                        name="",  # /jobs doesn't return name
                        state=job.get("status", "UNKNOWN"),
                    )

            # Fallback to first job (may be FAILED, CANCELED, etc.)
            first = jobs[0]
            return FlinkJobStatus(
                job_id=first.get("id", ""),
                name="",
                state=first.get("status", "UNKNOWN"),
            )
        except Exception as e:
            logger.debug(f"Failed to get job status: {e}")
            return None

    async def get_job_details(self, job_id: str) -> dict:
        """Get job details including vertices.

        Args:
            job_id: Flink job ID

        Returns:
            Job details dict
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}/jobs/{job_id}")
            response.raise_for_status()
            return response.json()

    async def get_vertex_metrics(self, job_id: str, vertex_id: str, metrics: list[str]) -> dict[str, float]:
        """Get specific metrics for a vertex.

        Args:
            job_id: Flink job ID
            vertex_id: Vertex (operator) ID
            metrics: List of metric names to retrieve

        Returns:
            Dict mapping metric name to value
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            metrics_param = ",".join(metrics)
            response = await client.get(
                f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/metrics",
                params={"get": metrics_param},
            )
            response.raise_for_status()

            result = {}
            for item in response.json():
                try:
                    result[item["id"]] = float(item["value"])
                except (KeyError, ValueError, TypeError):
                    pass
            return result

    async def get_subtask_metrics(
        self, job_id: str, vertex_id: str, subtask_index: int, metrics: list[str]
    ) -> dict[str, float]:
        """Get specific metrics for a subtask.

        Args:
            job_id: Flink job ID
            vertex_id: Vertex (operator) ID
            subtask_index: Subtask index
            metrics: List of metric names to retrieve

        Returns:
            Dict mapping metric name to value
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            metrics_param = ",".join(metrics)
            response = await client.get(
                f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_index}/metrics",
                params={"get": metrics_param},
            )
            response.raise_for_status()

            result = {}
            for item in response.json():
                try:
                    result[item["id"]] = float(item["value"])
                except (KeyError, ValueError, TypeError):
                    pass
            return result

    async def get_running_job_id(self) -> str | None:
        """Get the ID of the running job.

        Flink generates its own job_id at runtime (different from pipeline_id),
        so we need to query /jobs to find the actual job ID.

        Returns:
            Flink job ID string or None if no running job found
        """
        try:
            jobs = await self.get_jobs()
            for job in jobs:
                if job.get("status") == "RUNNING":
                    return job.get("id")
            return None
        except Exception as e:
            logger.debug(f"Failed to get running job: {e}")
            return None

    async def list_subtask_metrics(self, job_id: str, vertex_id: str, subtask_index: int) -> list[str]:
        """List all available metrics for a subtask.

        Args:
            job_id: Flink job ID
            vertex_id: Vertex (operator) ID
            subtask_index: Subtask index

        Returns:
            List of metric names available for this subtask
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_index}/metrics"
            )
            response.raise_for_status()
            return [item["id"] for item in response.json()]

    async def get_aggregated_subtask_metrics(
        self, job_id: str, vertex_id: str, metrics: list[str], agg: str = "sum"
    ) -> dict[str, float]:
        """Get aggregated metrics across all subtasks for a vertex.

        Uses the /jobs/{jobid}/vertices/{vertexid}/subtasks/metrics endpoint
        with aggregation parameter.

        Args:
            job_id: Flink job ID
            vertex_id: Vertex (operator) ID
            metrics: List of metric names to retrieve (without subtask prefix)
            agg: Aggregation type (sum, min, max, avg)

        Returns:
            Dict mapping metric name to aggregated value
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            metrics_param = ",".join(metrics)
            response = await client.get(
                f"{self.base_url}/jobs/{job_id}/vertices/{vertex_id}/subtasks/metrics",
                params={"get": metrics_param, "agg": agg},
            )
            response.raise_for_status()

            result = {}
            for item in response.json():
                try:
                    # Response format: {"id": "metric_name", "sum": value}
                    metric_id = item["id"]
                    value = item.get(agg, 0.0)
                    result[metric_id] = float(value) if value is not None else 0.0
                except (KeyError, ValueError, TypeError):
                    pass
            return result

    async def get_pending_records(self) -> int | None:
        """Get total pending records (consumer lag) for the running job.

        Finds the Events Source vertex and sums pendingRecords across all subtasks.
        Note: Metric name is prefixed with source name, e.g. Source__Events_Source_(Kafka).pendingRecords

        Returns:
            Total pending records or None if not available
        """
        try:
            # Get actual Flink job_id (different from pipeline_id!)
            job_id = await self.get_running_job_id()
            if job_id is None:
                logger.warning("No running job found")
                return None

            # Get job details to find vertices
            job_details = await self.get_job_details(job_id)
            vertices = job_details.get("vertices", [])

            # Find Events Source vertex (not Rules Source)
            source_vertex = None
            for vertex in vertices:
                name = vertex.get("name", "")
                # Look for Events Source specifically, not Rules Source
                if "events" in name.lower() and "source" in name.lower():
                    source_vertex = vertex
                    break

            # Fallback: any source vertex
            if not source_vertex:
                for vertex in vertices:
                    name = vertex.get("name", "").lower()
                    if "source" in name and "rules" not in name:
                        source_vertex = vertex
                        break

            if not source_vertex:
                logger.warning(f"No Events Source vertex found for job {job_id}")
                return None

            vertex_id = source_vertex["id"]
            parallelism = source_vertex.get("parallelism", 1)

            # Sum pendingRecords across all subtasks
            total_pending = 0
            for subtask_idx in range(parallelism):
                try:
                    # First, list all metrics to find the correct pendingRecords metric name
                    all_metrics = await self.list_subtask_metrics(job_id, vertex_id, subtask_idx)

                    # Find metric ending with .pendingRecords (for Events Source only)
                    pending_metric = None
                    for m in all_metrics:
                        if m.endswith(".pendingRecords") and "Events" in m:
                            pending_metric = m
                            break

                    if pending_metric:
                        metrics = await self.get_subtask_metrics(job_id, vertex_id, subtask_idx, [pending_metric])
                        pending = metrics.get(pending_metric)
                        if pending is not None:
                            total_pending += int(pending)
                except Exception as e:
                    logger.debug(f"Failed to get metrics for subtask {subtask_idx}: {e}")

            return total_pending

        except Exception as e:
            logger.error(f"Failed to get pending records for job {job_id}: {e}")
            if ErrorTracker.should_log(f"flink_api_pending_records_{job_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink_api",
                    entity_id=job_id,
                    details=f"Failed to get pending records: {str(e)}",
                    source="system",
                    severity="error",
                )
            return None

    async def get_job_metrics(self) -> FlinkJobMetrics | None:
        """Get comprehensive metrics for the running job.

        Automatically discovers the running Flink job ID and retrieves:
        - Job state
        - Consumer lag (pending records)
        - Rate gauges from Sigma processor (inputEventsPerSecond, outputTaggedPerSecond, outputUntaggedPerSecond)
        - Cumulative counters (matchedEvents, totalEvents)

        Returns:
            FlinkJobMetrics dataclass or None if failed
        """
        try:
            # Get actual Flink job_id (auto-generated, different from pipeline_id)
            job_id = await self.get_running_job_id()
            if job_id is None:
                logger.debug("No running job found")
                return None

            job_details = await self.get_job_details(job_id)
            state = job_details.get("state", "UNKNOWN")
            vertices = job_details.get("vertices", [])

            # Get pending records (consumer lag)
            pending_records = await self.get_pending_records()

            # Get all metrics from custom Flink gauges in Sigma processor
            (
                matched_events,
                total_events,
                input_eps,
                output_tagged_eps,
                output_untagged_eps,
                last_window_timestamp_ms,
            ) = await self._get_custom_counters(job_id, vertices)

            # EPS values already rounded to 2 decimals by Flink gauges
            return FlinkJobMetrics(
                job_id=job_id,
                state=state,
                pending_records=pending_records,
                input_events_per_second=input_eps,
                output_tagged_per_second=output_tagged_eps,
                output_untagged_per_second=output_untagged_eps,
                matched_events=matched_events,
                total_events=total_events,
                last_window_timestamp_ms=last_window_timestamp_ms,
            )

        except Exception as e:
            logger.error(f"Failed to get job metrics: {e}")
            if ErrorTracker.should_log("flink_api_job_metrics"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink_api",
                    details=f"Failed to get job metrics: {str(e)}",
                    source="system",
                    severity="error",
                )
            return None

    async def _get_custom_counters(
        self, job_id: str, vertices: list[dict]
    ) -> tuple[int, int, float, float, float, int]:
        """Get event counters and rate gauges from Sigma processor vertex.

        Uses custom Flink gauges registered in SigmaMatcherBroadcastFunction.open():
        - matchedEvents, totalEvents: Cumulative counters (for Kafka metrics consumer)
        - inputEventsPerSecond: Input rate gauge (events/sec from source)
        - outputTaggedPerSecond, outputUntaggedPerSecond: Output rate gauges (for dashboard)
        - lastWindowTimestampMs: Timestamp of last processed window (for stale detection)

        Rate gauges represent actual processing rates measured by Flink.
        outputUntaggedPerSecond will be 0 if output_mode == "matched_only".

        Args:
            job_id: Flink job ID
            vertices: List of vertex dicts from job details

        Returns:
            Tuple of (matched_events, total_events, input_eps, tagged_eps, untagged_eps, last_window_timestamp_ms)
        """
        matched_events = 0
        total_events = 0
        input_eps = 0.0
        output_tagged_eps = 0.0
        output_untagged_eps = 0.0
        last_window_timestamp_ms = 0

        try:
            for vertex in vertices:
                name = vertex.get("name", "")
                vertex_id = vertex.get("id", "")

                # Sigma Detection Processor has our metrics
                if "sigma" in name.lower() and "processor" in name.lower():
                    try:
                        # Metrics that need SUM aggregation (counters, rates)
                        sum_metrics = [
                            "Sigma_Detection_Processor.matchedEvents",
                            "Sigma_Detection_Processor.totalEvents",
                            "Sigma_Detection_Processor.inputEventsPerSecond",
                            "Sigma_Detection_Processor.outputTaggedPerSecond",
                            "Sigma_Detection_Processor.outputUntaggedPerSecond",
                        ]

                        # Get aggregated sum across all subtasks for counters and rates
                        aggregated = await self.get_aggregated_subtask_metrics(job_id, vertex_id, sum_metrics, "sum")

                        # Get MAX for timestamp (need most recent, not sum!)
                        timestamp_aggregated = await self.get_aggregated_subtask_metrics(
                            job_id, vertex_id, ["Sigma_Detection_Processor.lastWindowTimestampMs"], "max"
                        )

                        matched_events = int(aggregated.get("Sigma_Detection_Processor.matchedEvents", 0))
                        total_events = int(aggregated.get("Sigma_Detection_Processor.totalEvents", 0))
                        input_eps = aggregated.get("Sigma_Detection_Processor.inputEventsPerSecond", 0.0)
                        output_tagged_eps = aggregated.get("Sigma_Detection_Processor.outputTaggedPerSecond", 0.0)
                        output_untagged_eps = aggregated.get("Sigma_Detection_Processor.outputUntaggedPerSecond", 0.0)
                        last_window_timestamp_ms = int(
                            timestamp_aggregated.get("Sigma_Detection_Processor.lastWindowTimestampMs", 0)
                        )

                        logger.debug(
                            "Got Sigma processor metrics",
                            extra={
                                "matched_events": matched_events,
                                "total_events": total_events,
                                "input_eps": input_eps,
                                "output_tagged_eps": output_tagged_eps,
                                "output_untagged_eps": output_untagged_eps,
                                "last_window_timestamp_ms": last_window_timestamp_ms,
                            },
                        )

                        break  # Found the processor vertex
                    except Exception as e:
                        logger.debug(f"Failed to get custom gauges from Sigma processor: {e}")

        except Exception as e:
            logger.debug(f"Failed to get custom counters: {e}")

        return matched_events, total_events, input_eps, output_tagged_eps, output_untagged_eps, last_window_timestamp_ms


class FlinkMetricsService:
    """Service for retrieving Flink metrics from Kubernetes deployments.

    Combines Kubernetes FlinkDeployment info with Flink REST API metrics.
    """

    def __init__(self, namespace: str | None = None):
        """Initialize metrics service.

        Args:
            namespace: Kubernetes namespace for Flink deployments.
                       Defaults to settings.kubernetes_namespace.
        """
        self.namespace = namespace or settings.kubernetes_namespace

    def _get_rest_url(self, deployment_name: str) -> str:
        """Get Flink REST API URL for a deployment.

        Args:
            deployment_name: FlinkDeployment name

        Returns:
            Full URL to Flink REST API
        """
        # Flink Operator creates Service: {deployment_name}-rest
        service_name = f"{deployment_name}-rest"
        return f"http://{service_name}.{self.namespace}.svc.cluster.local:8081"

    async def get_pipeline_metrics(
        self, pipeline_id: str, deployment_name: str | None = None
    ) -> FlinkJobMetrics | None:
        """Get metrics for a pipeline.

        Args:
            pipeline_id: Pipeline UUID (used to derive deployment name)
            deployment_name: Optional FlinkDeployment name (auto-generated if not provided)

        Returns:
            FlinkJobMetrics or None if failed
        """
        if deployment_name is None:
            deployment_name = f"flink-{pipeline_id.lower()}"

        rest_url = self._get_rest_url(deployment_name)
        client = FlinkRestClient(rest_url)

        try:
            # Auto-discover actual Flink job_id (different from pipeline_id!)
            return await client.get_job_metrics()
        except Exception as e:
            logger.error(f"Failed to get metrics for pipeline {pipeline_id}: {e}")
            if ErrorTracker.should_log(f"flink_api_pipeline_metrics_{pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink_api",
                    entity_id=pipeline_id,
                    details=f"Failed to get metrics for pipeline: {str(e)}",
                    source="system",
                    severity="error",
                )
            return None

    async def get_consumer_lag(self, pipeline_id: str, deployment_name: str | None = None) -> int | None:
        """Get consumer lag (pending records) for a pipeline.

        Args:
            pipeline_id: Pipeline UUID (used to derive deployment name)
            deployment_name: Optional FlinkDeployment name

        Returns:
            Pending records count or None if failed
        """
        if deployment_name is None:
            deployment_name = f"flink-{pipeline_id.lower()}"

        rest_url = self._get_rest_url(deployment_name)
        client = FlinkRestClient(rest_url)

        try:
            # Auto-discover actual Flink job_id
            return await client.get_pending_records()
        except Exception as e:
            logger.error(f"Failed to get consumer lag for pipeline {pipeline_id}: {e}")
            if ErrorTracker.should_log(f"flink_api_consumer_lag_{pipeline_id}"):
                await activity_producer.log_action(
                    action="error",
                    entity_type="flink_api",
                    entity_id=pipeline_id,
                    details=f"Failed to get consumer lag: {str(e)}",
                    source="system",
                    severity="error",
                )
            return None


# Singleton instance
flink_metrics_service = FlinkMetricsService()
