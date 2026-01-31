"""
Task Startup Metrics Module

This module provides data models and collection functionality for tracking and aggregating
Airflow task startup metrics. It supports the airflow-task-startup-optimization feature by
enabling measurement of task startup latency from queue time to execution start time.

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5
"""

import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class TaskStartupMetric:
    """Metrics for a single task startup

    This data class captures the timing information for an individual task instance,
    tracking when it was queued and when it started execution. The startup latency
    is calculated as the difference between these two timestamps.

    Attributes:
        dag_id: The DAG identifier
        task_id: The task identifier within the DAG
        run_id: The unique run identifier for this task instance
        queued_time: Timestamp when the task entered the queue
        start_time: Timestamp when the task began execution
        startup_latency_seconds: Time in seconds from queue to execution start

    Requirements:
        - 7.1: Records timestamp when task enters queue
        - 7.2: Records timestamp when task begins execution
        - 7.3: Calculates task startup latency
    """

    dag_id: str
    task_id: str
    run_id: str
    queued_time: datetime
    start_time: datetime
    startup_latency_seconds: float

    @property
    def startup_latency_ms(self) -> float:
        """Return startup latency in milliseconds

        Returns:
            Startup latency converted to milliseconds
        """
        return self.startup_latency_seconds * 1000


@dataclass
class AggregatedMetrics:
    """Aggregated metrics across multiple tasks

    This data class provides statistical aggregation of task startup metrics,
    including average, median, percentiles, and min/max values. It supports
    the rolling average requirement for monitoring task startup performance.

    Attributes:
        total_tasks: Number of tasks included in aggregation
        avg_startup_latency_seconds: Mean startup latency
        median_startup_latency_seconds: Median startup latency
        p95_startup_latency_seconds: 95th percentile startup latency
        p99_startup_latency_seconds: 99th percentile startup latency
        min_startup_latency_seconds: Minimum startup latency
        max_startup_latency_seconds: Maximum startup latency

    Requirements:
        - 7.5: Maintains rolling average of task startup latency
    """

    total_tasks: int
    avg_startup_latency_seconds: float
    median_startup_latency_seconds: float
    p95_startup_latency_seconds: float
    p99_startup_latency_seconds: float
    min_startup_latency_seconds: float
    max_startup_latency_seconds: float

    @staticmethod
    def from_metrics(metrics: list[TaskStartupMetric]) -> "AggregatedMetrics":
        """Calculate aggregated metrics from list of individual metrics

        This method computes statistical aggregations across a collection of
        task startup metrics, providing insights into overall system performance.

        Args:
            metrics: List of TaskStartupMetric instances to aggregate

        Returns:
            AggregatedMetrics instance with computed statistics

        Raises:
            ValueError: If metrics list is empty
        """
        import statistics

        if not metrics:
            raise ValueError("Cannot aggregate empty metrics list")

        latencies = [m.startup_latency_seconds for m in metrics]
        latencies.sort()

        return AggregatedMetrics(
            total_tasks=len(metrics),
            avg_startup_latency_seconds=statistics.mean(latencies),
            median_startup_latency_seconds=statistics.median(latencies),
            p95_startup_latency_seconds=latencies[int(len(latencies) * 0.95)] if latencies else 0,
            p99_startup_latency_seconds=latencies[int(len(latencies) * 0.99)] if latencies else 0,
            min_startup_latency_seconds=min(latencies) if latencies else 0,
            max_startup_latency_seconds=max(latencies) if latencies else 0,
        )


class TaskStartupMetrics:
    """
    Collects and tracks task startup metrics for Airflow task instances.

    This class provides functionality to record task queue times and start times,
    calculate startup latency, expose metrics via the Airflow Stats API, and
    maintain a rolling average over the last 100 tasks.

    The class uses a singleton pattern to maintain a single instance across
    the application, ensuring consistent metric tracking.

    Attributes:
        _metrics_buffer: Deque storing the last 100 TaskStartupMetric instances
        _logger: Logger instance for logging metrics

    Requirements:
        - 7.1: Records timestamp when task enters queue
        - 7.2: Records timestamp when task begins execution
        - 7.3: Calculates and logs task startup latency
        - 7.4: Exposes metrics via Airflow Stats API
        - 7.5: Maintains rolling average over last 100 tasks
    """

    _instance: Optional["TaskStartupMetrics"] = None
    _metrics_buffer: deque[TaskStartupMetric]
    _logger: logging.Logger

    def __new__(cls):
        """Implement singleton pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._metrics_buffer = deque(maxlen=100)
            cls._instance._logger = logging.getLogger(__name__)
            cls._instance._stats_available = None  # Cache for Stats availability
        return cls._instance

    def _is_stats_available(self) -> bool:
        """
        Check if Airflow Stats API is available.

        Caches the result to avoid repeated import attempts.

        Returns:
            True if Stats API is available, False otherwise
        """
        if self._stats_available is None:
            try:
                from airflow.stats import Stats  # noqa: F401

                self._stats_available = True
            except (ImportError, AttributeError, ValueError):
                # ImportError: airflow not installed
                # AttributeError/ValueError: airflow initialization issues (e.g., Windows)
                self._stats_available = False
        return self._stats_available

    def _emit_metric_gauge(self, metric_name: str, value: float) -> None:
        """
        Emit a gauge metric via Airflow Stats API or log it.

        Args:
            metric_name: Name of the metric
            value: Metric value
        """
        if self._is_stats_available():
            try:
                from airflow.stats import Stats

                Stats.gauge(metric_name, value)
            except Exception as e:
                self._logger.debug(f"Failed to emit gauge metric {metric_name}: {e}")
                self._logger.info(f"Metric: {metric_name} = {value}")
        else:
            self._logger.info(f"Metric: {metric_name} = {value}")

    def _emit_metric_timing(self, metric_name: str, value: float) -> None:
        """
        Emit a timing metric via Airflow Stats API or log it.

        Args:
            metric_name: Name of the metric
            value: Metric value in seconds
        """
        if self._is_stats_available():
            try:
                from airflow.stats import Stats

                Stats.timing(metric_name, value)
            except Exception as e:
                self._logger.debug(f"Failed to emit timing metric {metric_name}: {e}")
                self._logger.info(f"Metric: {metric_name} = {value:.3f}s")
        else:
            self._logger.info(f"Metric: {metric_name} = {value:.3f}s")

    def record_queue_time(
        self, dag_id: str, task_id: str, run_id: str, queued_dttm: datetime
    ) -> None:
        """
        Record when a task enters the queue.

        This method stores the queue timestamp for a task instance. The timestamp
        will be used later to calculate startup latency when the task starts.

        Args:
            dag_id: The DAG identifier
            task_id: The task identifier within the DAG
            run_id: The unique run identifier for this task instance
            queued_dttm: Timestamp when the task entered the queue

        Requirements:
            - 7.1: Records timestamp when task enters queue

        Example:
            >>> metrics = TaskStartupMetrics()
            >>> metrics.record_queue_time(
            ...     dag_id="data_pipeline",
            ...     task_id="extract_data",
            ...     run_id="scheduled__2024-01-01T10:00:00",
            ...     queued_dttm=datetime.now()
            ... )
        """
        # Store queue time in a temporary cache for later matching with start time
        # In a real Airflow integration, this would use task_instance.queued_dttm
        if not hasattr(self, "_queue_times"):
            self._queue_times = {}

        key = f"{dag_id}:{task_id}:{run_id}"
        self._queue_times[key] = queued_dttm

        self._logger.debug(
            f"Recorded queue time for task {dag_id}.{task_id} (run: {run_id}): "
            f"{queued_dttm.isoformat()}"
        )

        # Expose queue time metric via Stats API
        self._emit_metric_gauge(f"task.queue_time.{dag_id}.{task_id}", queued_dttm.timestamp())

    def record_start_time(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        start_date: datetime,
        queued_dttm: datetime | None = None,
    ) -> None:
        """
        Record when a task starts execution and calculate startup latency.

        This method records the task start timestamp, calculates the startup latency
        by comparing with the queue time, logs the latency, exposes it via the Stats
        API, and maintains a rolling average over the last 100 tasks.

        Args:
            dag_id: The DAG identifier
            task_id: The task identifier within the DAG
            run_id: The unique run identifier for this task instance
            start_date: Timestamp when the task began execution
            queued_dttm: Optional queue timestamp (if not provided, will look up from cache)

        Requirements:
            - 7.2: Records timestamp when task begins execution
            - 7.3: Calculates and logs task startup latency
            - 7.4: Exposes metrics via Airflow Stats API
            - 7.5: Maintains rolling average over last 100 tasks

        Example:
            >>> metrics = TaskStartupMetrics()
            >>> metrics.record_start_time(
            ...     dag_id="data_pipeline",
            ...     task_id="extract_data",
            ...     run_id="scheduled__2024-01-01T10:00:00",
            ...     start_date=datetime.now(),
            ...     queued_dttm=datetime.now() - timedelta(seconds=2.5)
            ... )
        """
        # Get queue time from cache if not provided
        if queued_dttm is None:
            if not hasattr(self, "_queue_times"):
                self._queue_times = {}

            key = f"{dag_id}:{task_id}:{run_id}"
            queued_dttm = self._queue_times.get(key)

            if queued_dttm is None:
                self._logger.warning(
                    f"No queue time found for task {dag_id}.{task_id} (run: {run_id}). "
                    "Cannot calculate startup latency."
                )
                return

        # Calculate startup latency
        startup_latency = (start_date - queued_dttm).total_seconds()

        # Create metric object
        metric = TaskStartupMetric(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            queued_time=queued_dttm,
            start_time=start_date,
            startup_latency_seconds=startup_latency,
        )

        # Add to rolling buffer (automatically maintains last 100)
        self._metrics_buffer.append(metric)

        # Log startup latency
        self._logger.info(
            f"Task startup latency for {dag_id}.{task_id} (run: {run_id}): {startup_latency:.3f}s"
        )

        # Expose metrics via Airflow Stats API
        # Task-specific latency
        self._emit_metric_timing(f"task.startup_latency.{dag_id}.{task_id}", startup_latency)

        # Global latency metric
        self._emit_metric_timing("task.startup_latency.all", startup_latency)

        # Rolling average
        if len(self._metrics_buffer) > 0:
            avg_latency = sum(m.startup_latency_seconds for m in self._metrics_buffer) / len(
                self._metrics_buffer
            )
            self._emit_metric_gauge("task.startup_latency.rolling_avg_100", avg_latency)

            self._logger.info(
                f"Rolling average startup latency (last {len(self._metrics_buffer)} tasks): "
                f"{avg_latency:.3f}s"
            )

        # Clean up queue time from cache
        if hasattr(self, "_queue_times"):
            key = f"{dag_id}:{task_id}:{run_id}"
            self._queue_times.pop(key, None)

    def get_rolling_average(self) -> float | None:
        """
        Get the rolling average startup latency over the last 100 tasks.

        Returns:
            Average startup latency in seconds, or None if no metrics available

        Requirements:
            - 7.5: Maintains rolling average over last 100 tasks

        Example:
            >>> metrics = TaskStartupMetrics()
            >>> avg = metrics.get_rolling_average()
            >>> if avg is not None:
            ...     print(f"Average latency: {avg:.3f}s")
        """
        if len(self._metrics_buffer) == 0:
            return None

        return sum(m.startup_latency_seconds for m in self._metrics_buffer) / len(
            self._metrics_buffer
        )

    def get_aggregated_metrics(self) -> AggregatedMetrics | None:
        """
        Get aggregated metrics for all tasks in the rolling buffer.

        Returns:
            AggregatedMetrics instance with statistics, or None if no metrics available

        Requirements:
            - 7.5: Maintains rolling average over last 100 tasks

        Example:
            >>> metrics = TaskStartupMetrics()
            >>> agg = metrics.get_aggregated_metrics()
            >>> if agg is not None:
            ...     print(f"P95 latency: {agg.p95_startup_latency_seconds:.3f}s")
        """
        if len(self._metrics_buffer) == 0:
            return None

        return AggregatedMetrics.from_metrics(list(self._metrics_buffer))

    def clear_metrics(self) -> None:
        """
        Clear all metrics from the rolling buffer.

        This method is primarily useful for testing purposes.
        """
        self._metrics_buffer.clear()
        if hasattr(self, "_queue_times"):
            self._queue_times.clear()
