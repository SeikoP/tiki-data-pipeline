"""
Unit tests for TaskStartupMetric and AggregatedMetrics data classes.

Tests data model functionality, metric calculations, and aggregation logic
for the Airflow task startup optimization feature.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest
from datetime import datetime, timedelta
from common.task_startup_metrics import TaskStartupMetric, AggregatedMetrics


class TestTaskStartupMetric:
    """Test suite for TaskStartupMetric"""

    def test_task_startup_metric_creation(self):
        """Test that TaskStartupMetric can be instantiated with valid data"""
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        start_time = datetime(2024, 1, 1, 10, 0, 5)
        
        metric = TaskStartupMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_123",
            queued_time=queued_time,
            start_time=start_time,
            startup_latency_seconds=5.0
        )
        
        assert metric.dag_id == "test_dag"
        assert metric.task_id == "test_task"
        assert metric.run_id == "test_run_123"
        assert metric.queued_time == queued_time
        assert metric.start_time == start_time
        assert metric.startup_latency_seconds == 5.0

    def test_startup_latency_ms_property(self):
        """Test that startup_latency_ms correctly converts seconds to milliseconds"""
        metric = TaskStartupMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_123",
            queued_time=datetime.now(),
            start_time=datetime.now(),
            startup_latency_seconds=2.5
        )
        
        assert metric.startup_latency_ms == 2500.0

    def test_startup_latency_ms_with_zero(self):
        """Test startup_latency_ms with zero latency"""
        metric = TaskStartupMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_123",
            queued_time=datetime.now(),
            start_time=datetime.now(),
            startup_latency_seconds=0.0
        )
        
        assert metric.startup_latency_ms == 0.0

    def test_startup_latency_ms_with_fractional_seconds(self):
        """Test startup_latency_ms with fractional seconds"""
        metric = TaskStartupMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_123",
            queued_time=datetime.now(),
            start_time=datetime.now(),
            startup_latency_seconds=0.123
        )
        
        assert metric.startup_latency_ms == 123.0

    def test_task_startup_metric_is_dataclass(self):
        """Test that TaskStartupMetric is a dataclass"""
        from dataclasses import is_dataclass
        assert is_dataclass(TaskStartupMetric)


class TestAggregatedMetrics:
    """Test suite for AggregatedMetrics"""

    def test_aggregated_metrics_creation(self):
        """Test that AggregatedMetrics can be instantiated with valid data"""
        metrics = AggregatedMetrics(
            total_tasks=100,
            avg_startup_latency_seconds=2.5,
            median_startup_latency_seconds=2.0,
            p95_startup_latency_seconds=5.0,
            p99_startup_latency_seconds=7.0,
            min_startup_latency_seconds=0.5,
            max_startup_latency_seconds=10.0
        )
        
        assert metrics.total_tasks == 100
        assert metrics.avg_startup_latency_seconds == 2.5
        assert metrics.median_startup_latency_seconds == 2.0
        assert metrics.p95_startup_latency_seconds == 5.0
        assert metrics.p99_startup_latency_seconds == 7.0
        assert metrics.min_startup_latency_seconds == 0.5
        assert metrics.max_startup_latency_seconds == 10.0

    def test_from_metrics_with_single_metric(self):
        """Test from_metrics with a single task metric"""
        metric = TaskStartupMetric(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            queued_time=datetime.now(),
            start_time=datetime.now(),
            startup_latency_seconds=3.0
        )
        
        aggregated = AggregatedMetrics.from_metrics([metric])
        
        assert aggregated.total_tasks == 1
        assert aggregated.avg_startup_latency_seconds == 3.0
        assert aggregated.median_startup_latency_seconds == 3.0
        assert aggregated.min_startup_latency_seconds == 3.0
        assert aggregated.max_startup_latency_seconds == 3.0

    def test_from_metrics_with_multiple_metrics(self):
        """Test from_metrics with multiple task metrics"""
        base_time = datetime.now()
        metrics = [
            TaskStartupMetric(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=i),
                startup_latency_seconds=float(i)
            )
            for i in range(1, 6)  # 1.0, 2.0, 3.0, 4.0, 5.0
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.total_tasks == 5
        assert aggregated.avg_startup_latency_seconds == 3.0
        assert aggregated.median_startup_latency_seconds == 3.0
        assert aggregated.min_startup_latency_seconds == 1.0
        assert aggregated.max_startup_latency_seconds == 5.0

    def test_from_metrics_calculates_percentiles(self):
        """Test that from_metrics correctly calculates p95 and p99"""
        base_time = datetime.now()
        # Create 100 metrics with latencies from 1 to 100
        metrics = [
            TaskStartupMetric(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=i),
                startup_latency_seconds=float(i)
            )
            for i in range(1, 101)
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.total_tasks == 100
        # int(100 * 0.95) = 95, so we get index 95 which is value 96.0
        assert aggregated.p95_startup_latency_seconds == 96.0
        # int(100 * 0.99) = 99, so we get index 99 which is value 100.0
        assert aggregated.p99_startup_latency_seconds == 100.0

    def test_from_metrics_with_empty_list_raises_error(self):
        """Test that from_metrics raises ValueError for empty list"""
        with pytest.raises(ValueError, match="Cannot aggregate empty metrics list"):
            AggregatedMetrics.from_metrics([])

    def test_from_metrics_with_identical_values(self):
        """Test from_metrics when all metrics have the same latency"""
        base_time = datetime.now()
        metrics = [
            TaskStartupMetric(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=2),
                startup_latency_seconds=2.0
            )
            for i in range(10)
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.total_tasks == 10
        assert aggregated.avg_startup_latency_seconds == 2.0
        assert aggregated.median_startup_latency_seconds == 2.0
        assert aggregated.p95_startup_latency_seconds == 2.0
        assert aggregated.p99_startup_latency_seconds == 2.0
        assert aggregated.min_startup_latency_seconds == 2.0
        assert aggregated.max_startup_latency_seconds == 2.0

    def test_from_metrics_with_unsorted_input(self):
        """Test that from_metrics handles unsorted input correctly"""
        base_time = datetime.now()
        metrics = [
            TaskStartupMetric(
                dag_id="test_dag",
                task_id="task_1",
                run_id="run_1",
                queued_time=base_time,
                start_time=base_time,
                startup_latency_seconds=5.0
            ),
            TaskStartupMetric(
                dag_id="test_dag",
                task_id="task_2",
                run_id="run_2",
                queued_time=base_time,
                start_time=base_time,
                startup_latency_seconds=1.0
            ),
            TaskStartupMetric(
                dag_id="test_dag",
                task_id="task_3",
                run_id="run_3",
                queued_time=base_time,
                start_time=base_time,
                startup_latency_seconds=3.0
            ),
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.min_startup_latency_seconds == 1.0
        assert aggregated.max_startup_latency_seconds == 5.0
        assert aggregated.median_startup_latency_seconds == 3.0

    def test_aggregated_metrics_is_dataclass(self):
        """Test that AggregatedMetrics is a dataclass"""
        from dataclasses import is_dataclass
        assert is_dataclass(AggregatedMetrics)


class TestIntegration:
    """Integration tests for TaskStartupMetric and AggregatedMetrics"""

    def test_realistic_task_metrics_scenario(self):
        """Test a realistic scenario with varying task startup latencies"""
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Simulate realistic task startup latencies (in seconds)
        latencies = [1.2, 1.5, 2.0, 2.3, 1.8, 3.5, 2.1, 1.9, 2.5, 4.0]
        
        metrics = [
            TaskStartupMetric(
                dag_id="data_pipeline",
                task_id=f"extract_task_{i}",
                run_id=f"scheduled__2024-01-01T10:00:00+00:00_{i}",
                queued_time=base_time + timedelta(minutes=i),
                start_time=base_time + timedelta(minutes=i, seconds=latency),
                startup_latency_seconds=latency
            )
            for i, latency in enumerate(latencies)
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.total_tasks == 10
        assert aggregated.min_startup_latency_seconds == 1.2
        assert aggregated.max_startup_latency_seconds == 4.0
        assert 2.0 <= aggregated.avg_startup_latency_seconds <= 2.5
        assert 1.8 <= aggregated.median_startup_latency_seconds <= 2.3

    def test_metrics_with_different_dags(self):
        """Test metrics from different DAGs can be aggregated"""
        base_time = datetime.now()
        
        metrics = [
            TaskStartupMetric(
                dag_id="dag_1",
                task_id="task_a",
                run_id="run_1",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=2),
                startup_latency_seconds=2.0
            ),
            TaskStartupMetric(
                dag_id="dag_2",
                task_id="task_b",
                run_id="run_2",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=3),
                startup_latency_seconds=3.0
            ),
            TaskStartupMetric(
                dag_id="dag_3",
                task_id="task_c",
                run_id="run_3",
                queued_time=base_time,
                start_time=base_time + timedelta(seconds=4),
                startup_latency_seconds=4.0
            ),
        ]
        
        aggregated = AggregatedMetrics.from_metrics(metrics)
        
        assert aggregated.total_tasks == 3
        assert aggregated.avg_startup_latency_seconds == 3.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])



class TestTaskStartupMetrics:
    """Test suite for TaskStartupMetrics class"""

    def setup_method(self):
        """Reset singleton instance before each test"""
        from common.task_startup_metrics import TaskStartupMetrics
        TaskStartupMetrics._instance = None

    def test_singleton_pattern(self):
        """Test that TaskStartupMetrics implements singleton pattern"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics1 = TaskStartupMetrics()
        metrics2 = TaskStartupMetrics()
        
        assert metrics1 is metrics2

    def test_record_queue_time(self):
        """Test recording queue time for a task"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Should not raise any exceptions
        metrics.record_queue_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            queued_dttm=queued_time
        )
        
        # Verify queue time is stored
        key = "test_dag:test_task:test_run_1"
        assert hasattr(metrics, '_queue_times')
        assert key in metrics._queue_times
        assert metrics._queue_times[key] == queued_time

    def test_record_start_time_with_queue_time(self):
        """Test recording start time when queue time is provided"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        start_time = datetime(2024, 1, 1, 10, 0, 5)
        
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=start_time,
            queued_dttm=queued_time
        )
        
        # Verify metric was added to buffer
        assert len(metrics._metrics_buffer) == 1
        metric = metrics._metrics_buffer[0]
        assert metric.dag_id == "test_dag"
        assert metric.task_id == "test_task"
        assert metric.run_id == "test_run_1"
        assert metric.startup_latency_seconds == 5.0

    def test_record_start_time_with_cached_queue_time(self):
        """Test recording start time using cached queue time"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        start_time = datetime(2024, 1, 1, 10, 0, 3)
        
        # First record queue time
        metrics.record_queue_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            queued_dttm=queued_time
        )
        
        # Then record start time without providing queue time
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=start_time
        )
        
        # Verify metric was calculated correctly
        assert len(metrics._metrics_buffer) == 1
        metric = metrics._metrics_buffer[0]
        assert metric.startup_latency_seconds == 3.0
        
        # Verify queue time was cleaned up from cache
        key = "test_dag:test_task:test_run_1"
        assert key not in metrics._queue_times

    def test_record_start_time_without_queue_time(self):
        """Test recording start time when no queue time is available"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        start_time = datetime(2024, 1, 1, 10, 0, 5)
        
        # Should log warning but not raise exception
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=start_time
        )
        
        # Verify no metric was added
        assert len(metrics._metrics_buffer) == 0

    def test_rolling_buffer_maintains_100_tasks(self):
        """Test that rolling buffer maintains only last 100 tasks"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Add 150 tasks
        for i in range(150):
            queued_time = base_time + timedelta(minutes=i)
            start_time = queued_time + timedelta(seconds=2)
            
            metrics.record_start_time(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                start_date=start_time,
                queued_dttm=queued_time
            )
        
        # Verify only last 100 are kept
        assert len(metrics._metrics_buffer) == 100
        
        # Verify oldest tasks were removed (first 50)
        task_ids = [m.task_id for m in metrics._metrics_buffer]
        assert "task_0" not in task_ids
        assert "task_49" not in task_ids
        assert "task_50" in task_ids
        assert "task_149" in task_ids

    def test_get_rolling_average_with_metrics(self):
        """Test getting rolling average when metrics exist"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Add tasks with latencies: 1.0, 2.0, 3.0, 4.0, 5.0
        for i in range(1, 6):
            queued_time = base_time + timedelta(minutes=i)
            start_time = queued_time + timedelta(seconds=i)
            
            metrics.record_start_time(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                start_date=start_time,
                queued_dttm=queued_time
            )
        
        avg = metrics.get_rolling_average()
        assert avg is not None
        assert avg == 3.0  # (1+2+3+4+5)/5

    def test_get_rolling_average_without_metrics(self):
        """Test getting rolling average when no metrics exist"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        avg = metrics.get_rolling_average()
        assert avg is None

    def test_get_aggregated_metrics_with_metrics(self):
        """Test getting aggregated metrics when metrics exist"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Add 10 tasks with latencies from 1 to 10
        for i in range(1, 11):
            queued_time = base_time + timedelta(minutes=i)
            start_time = queued_time + timedelta(seconds=i)
            
            metrics.record_start_time(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                start_date=start_time,
                queued_dttm=queued_time
            )
        
        agg = metrics.get_aggregated_metrics()
        assert agg is not None
        assert agg.total_tasks == 10
        assert agg.avg_startup_latency_seconds == 5.5
        assert agg.min_startup_latency_seconds == 1.0
        assert agg.max_startup_latency_seconds == 10.0

    def test_get_aggregated_metrics_without_metrics(self):
        """Test getting aggregated metrics when no metrics exist"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        agg = metrics.get_aggregated_metrics()
        assert agg is None

    def test_clear_metrics(self):
        """Test clearing all metrics"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        # Add some metrics
        for i in range(5):
            queued_time = base_time + timedelta(minutes=i)
            start_time = queued_time + timedelta(seconds=2)
            
            metrics.record_queue_time(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                queued_dttm=queued_time
            )
            
            metrics.record_start_time(
                dag_id="test_dag",
                task_id=f"task_{i}",
                run_id=f"run_{i}",
                start_date=start_time,
                queued_dttm=queued_time
            )
        
        assert len(metrics._metrics_buffer) == 5
        
        # Clear metrics
        metrics.clear_metrics()
        
        assert len(metrics._metrics_buffer) == 0
        assert len(metrics._queue_times) == 0

    def test_multiple_dags_and_tasks(self):
        """Test tracking metrics for multiple DAGs and tasks"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        base_time = datetime(2024, 1, 1, 10, 0, 0)
        
        dags = ["dag_1", "dag_2", "dag_3"]
        tasks = ["task_a", "task_b"]
        
        for dag in dags:
            for task in tasks:
                queued_time = base_time
                start_time = queued_time + timedelta(seconds=2)
                
                metrics.record_start_time(
                    dag_id=dag,
                    task_id=task,
                    run_id=f"run_{dag}_{task}",
                    start_date=start_time,
                    queued_dttm=queued_time
                )
        
        # Should have 6 metrics (3 dags * 2 tasks)
        assert len(metrics._metrics_buffer) == 6
        
        # All should have same latency
        avg = metrics.get_rolling_average()
        assert avg == 2.0

    def test_fractional_latency_seconds(self):
        """Test that fractional latency seconds are handled correctly"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        start_time = datetime(2024, 1, 1, 10, 0, 0, 500000)  # 0.5 seconds later
        
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=start_time,
            queued_dttm=queued_time
        )
        
        assert len(metrics._metrics_buffer) == 1
        metric = metrics._metrics_buffer[0]
        assert metric.startup_latency_seconds == 0.5

    def test_zero_latency(self):
        """Test handling of zero latency (queue and start at same time)"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        same_time = datetime(2024, 1, 1, 10, 0, 0)
        
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=same_time,
            queued_dttm=same_time
        )
        
        assert len(metrics._metrics_buffer) == 1
        metric = metrics._metrics_buffer[0]
        assert metric.startup_latency_seconds == 0.0

    def test_large_latency(self):
        """Test handling of large latency values"""
        from common.task_startup_metrics import TaskStartupMetrics
        
        metrics = TaskStartupMetrics()
        queued_time = datetime(2024, 1, 1, 10, 0, 0)
        start_time = datetime(2024, 1, 1, 11, 0, 0)  # 1 hour later
        
        metrics.record_start_time(
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run_1",
            start_date=start_time,
            queued_dttm=queued_time
        )
        
        assert len(metrics._metrics_buffer) == 1
        metric = metrics._metrics_buffer[0]
        assert metric.startup_latency_seconds == 3600.0  # 1 hour in seconds


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
