"""Before/after comparison report generator for performance optimization.

This module provides comprehensive comparison reports showing performance
improvements between baseline and optimized implementations, including
statistical analysis and data quality metrics.
"""

import statistics
from dataclasses import dataclass

from .metrics_collector import PipelineRunMetrics


@dataclass
class MetricComparison:
    """Comparison of a single metric between baseline and optimized runs.

    Attributes:
        metric_name: Name of the metric
        baseline_value: Baseline metric value
        optimized_value: Optimized metric value
        improvement_percent: Percentage improvement (positive = better)
        baseline_mean: Mean of baseline values (if multiple runs)
        baseline_median: Median of baseline values
        baseline_std_dev: Standard deviation of baseline values
        baseline_p50: 50th percentile of baseline values
        baseline_p95: 95th percentile of baseline values
        baseline_p99: 99th percentile of baseline values
        optimized_mean: Mean of optimized values (if multiple runs)
        optimized_median: Median of optimized values
        optimized_std_dev: Standard deviation of optimized values
        optimized_p50: 50th percentile of optimized values
        optimized_p95: 95th percentile of optimized values
        optimized_p99: 99th percentile of optimized values
    """

    metric_name: str
    baseline_value: float
    optimized_value: float
    improvement_percent: float
    baseline_mean: float | None = None
    baseline_median: float | None = None
    baseline_std_dev: float | None = None
    baseline_p50: float | None = None
    baseline_p95: float | None = None
    baseline_p99: float | None = None
    optimized_mean: float | None = None
    optimized_median: float | None = None
    optimized_std_dev: float | None = None
    optimized_p50: float | None = None
    optimized_p95: float | None = None
    optimized_p99: float | None = None

    def to_dict(self) -> dict:
        """Convert comparison to dictionary for serialization."""
        return {
            "metric_name": self.metric_name,
            "baseline_value": round(self.baseline_value, 2),
            "optimized_value": round(self.optimized_value, 2),
            "improvement_percent": round(self.improvement_percent, 2),
            "baseline_stats": {
                "mean": round(self.baseline_mean, 2) if self.baseline_mean is not None else None,
                "median": round(self.baseline_median, 2)
                if self.baseline_median is not None
                else None,
                "std_dev": round(self.baseline_std_dev, 2)
                if self.baseline_std_dev is not None
                else None,
                "p50": round(self.baseline_p50, 2) if self.baseline_p50 is not None else None,
                "p95": round(self.baseline_p95, 2) if self.baseline_p95 is not None else None,
                "p99": round(self.baseline_p99, 2) if self.baseline_p99 is not None else None,
            },
            "optimized_stats": {
                "mean": round(self.optimized_mean, 2) if self.optimized_mean is not None else None,
                "median": round(self.optimized_median, 2)
                if self.optimized_median is not None
                else None,
                "std_dev": round(self.optimized_std_dev, 2)
                if self.optimized_std_dev is not None
                else None,
                "p50": round(self.optimized_p50, 2) if self.optimized_p50 is not None else None,
                "p95": round(self.optimized_p95, 2) if self.optimized_p95 is not None else None,
                "p99": round(self.optimized_p99, 2) if self.optimized_p99 is not None else None,
            },
        }


@dataclass
class ComparisonReport:
    """Comprehensive before/after comparison report.

    Attributes:
        baseline_runs: List of baseline run IDs
        optimized_runs: List of optimized run IDs
        throughput_comparison: Throughput metric comparison
        latency_comparison: Latency metric comparison
        error_rate_comparison: Error rate comparison
        cpu_comparison: CPU utilization comparison
        memory_comparison: Memory usage comparison
        disk_read_comparison: Disk read comparison
        disk_write_comparison: Disk write comparison
        network_sent_comparison: Network sent comparison
        network_recv_comparison: Network received comparison
        cache_l1_comparison: L1 cache hit rate comparison
        cache_l2_comparison: L2 cache hit rate comparison
        cache_l3_comparison: L3 cache hit rate comparison
        data_completeness_comparison: Data completeness comparison
        overall_improvement_percent: Overall performance improvement
    """

    baseline_runs: list[str]
    optimized_runs: list[str]
    throughput_comparison: MetricComparison
    latency_comparison: MetricComparison
    error_rate_comparison: MetricComparison
    cpu_comparison: MetricComparison
    memory_comparison: MetricComparison
    disk_read_comparison: MetricComparison
    disk_write_comparison: MetricComparison
    network_sent_comparison: MetricComparison
    network_recv_comparison: MetricComparison
    cache_l1_comparison: MetricComparison
    cache_l2_comparison: MetricComparison
    cache_l3_comparison: MetricComparison
    data_completeness_comparison: MetricComparison
    overall_improvement_percent: float

    def to_dict(self) -> dict:
        """Convert report to dictionary for serialization."""
        return {
            "baseline_runs": self.baseline_runs,
            "optimized_runs": self.optimized_runs,
            "overall_improvement_percent": round(self.overall_improvement_percent, 2),
            "comparisons": {
                "throughput": self.throughput_comparison.to_dict(),
                "latency": self.latency_comparison.to_dict(),
                "error_rate": self.error_rate_comparison.to_dict(),
                "cpu": self.cpu_comparison.to_dict(),
                "memory": self.memory_comparison.to_dict(),
                "disk_read": self.disk_read_comparison.to_dict(),
                "disk_write": self.disk_write_comparison.to_dict(),
                "network_sent": self.network_sent_comparison.to_dict(),
                "network_recv": self.network_recv_comparison.to_dict(),
                "cache_l1": self.cache_l1_comparison.to_dict(),
                "cache_l2": self.cache_l2_comparison.to_dict(),
                "cache_l3": self.cache_l3_comparison.to_dict(),
                "data_completeness": self.data_completeness_comparison.to_dict(),
            },
        }


class ComparisonReportGenerator:
    """Generates before/after comparison reports for performance optimization.

    Compares baseline and optimized pipeline runs, calculating statistical
    measures and percentage improvements for all metrics.

    Example:
        >>> generator = ComparisonReportGenerator()
        >>> report = generator.generate_report(baseline_metrics, optimized_metrics)
        >>> print(f"Overall improvement: {report.overall_improvement_percent:.1f}%")
        >>> print(generator.format_report(report))
    """

    def generate_report(
        self,
        baseline_metrics: list[PipelineRunMetrics],
        optimized_metrics: list[PipelineRunMetrics],
    ) -> ComparisonReport:
        """Generate a comprehensive comparison report.

        Args:
            baseline_metrics: List of baseline run metrics
            optimized_metrics: List of optimized run metrics

        Returns:
            ComparisonReport with all comparisons and statistics

        Raises:
            ValueError: If either list is empty
        """
        if not baseline_metrics:
            raise ValueError("baseline_metrics cannot be empty")
        if not optimized_metrics:
            raise ValueError("optimized_metrics cannot be empty")

        # Extract run IDs
        baseline_runs = [m.run_id for m in baseline_metrics]
        optimized_runs = [m.run_id for m in optimized_metrics]

        # Generate comparisons for each metric
        throughput_comparison = self._compare_metric(
            "throughput_products_per_second",
            [m.throughput_products_per_second for m in baseline_metrics],
            [m.throughput_products_per_second for m in optimized_metrics],
            higher_is_better=True,
        )

        latency_comparison = self._compare_metric(
            "latency_per_product_ms",
            [m.latency_per_product_ms for m in baseline_metrics],
            [m.latency_per_product_ms for m in optimized_metrics],
            higher_is_better=False,
        )

        error_rate_comparison = self._compare_metric(
            "error_rate",
            [m.error_rate for m in baseline_metrics],
            [m.error_rate for m in optimized_metrics],
            higher_is_better=False,
        )

        cpu_comparison = self._compare_metric(
            "cpu_utilization_percent",
            [m.cpu_utilization_percent for m in baseline_metrics],
            [m.cpu_utilization_percent for m in optimized_metrics],
            higher_is_better=False,
        )

        memory_comparison = self._compare_metric(
            "memory_peak_mb",
            [m.memory_peak_mb for m in baseline_metrics],
            [m.memory_peak_mb for m in optimized_metrics],
            higher_is_better=False,
        )

        disk_read_comparison = self._compare_metric(
            "disk_read_mb",
            [m.disk_read_mb for m in baseline_metrics],
            [m.disk_read_mb for m in optimized_metrics],
            higher_is_better=False,
        )

        disk_write_comparison = self._compare_metric(
            "disk_write_mb",
            [m.disk_write_mb for m in baseline_metrics],
            [m.disk_write_mb for m in optimized_metrics],
            higher_is_better=False,
        )

        network_sent_comparison = self._compare_metric(
            "network_sent_mb",
            [m.network_sent_mb for m in baseline_metrics],
            [m.network_sent_mb for m in optimized_metrics],
            higher_is_better=False,
        )

        network_recv_comparison = self._compare_metric(
            "network_recv_mb",
            [m.network_recv_mb for m in baseline_metrics],
            [m.network_recv_mb for m in optimized_metrics],
            higher_is_better=False,
        )

        cache_l1_comparison = self._compare_metric(
            "cache_l1_hit_rate",
            [m.cache_l1_hit_rate for m in baseline_metrics],
            [m.cache_l1_hit_rate for m in optimized_metrics],
            higher_is_better=True,
        )

        cache_l2_comparison = self._compare_metric(
            "cache_l2_hit_rate",
            [m.cache_l2_hit_rate for m in baseline_metrics],
            [m.cache_l2_hit_rate for m in optimized_metrics],
            higher_is_better=True,
        )

        cache_l3_comparison = self._compare_metric(
            "cache_l3_hit_rate",
            [m.cache_l3_hit_rate for m in baseline_metrics],
            [m.cache_l3_hit_rate for m in optimized_metrics],
            higher_is_better=True,
        )

        data_completeness_comparison = self._compare_metric(
            "data_completeness_percent",
            [m.data_completeness_percent for m in baseline_metrics],
            [m.data_completeness_percent for m in optimized_metrics],
            higher_is_better=True,
        )

        # Calculate overall improvement (weighted average of key metrics)
        overall_improvement = self._calculate_overall_improvement(
            throughput_comparison, latency_comparison, cpu_comparison, memory_comparison
        )

        return ComparisonReport(
            baseline_runs=baseline_runs,
            optimized_runs=optimized_runs,
            throughput_comparison=throughput_comparison,
            latency_comparison=latency_comparison,
            error_rate_comparison=error_rate_comparison,
            cpu_comparison=cpu_comparison,
            memory_comparison=memory_comparison,
            disk_read_comparison=disk_read_comparison,
            disk_write_comparison=disk_write_comparison,
            network_sent_comparison=network_sent_comparison,
            network_recv_comparison=network_recv_comparison,
            cache_l1_comparison=cache_l1_comparison,
            cache_l2_comparison=cache_l2_comparison,
            cache_l3_comparison=cache_l3_comparison,
            data_completeness_comparison=data_completeness_comparison,
            overall_improvement_percent=overall_improvement,
        )

    def _compare_metric(
        self,
        metric_name: str,
        baseline_values: list[float],
        optimized_values: list[float],
        higher_is_better: bool,
    ) -> MetricComparison:
        """Compare a single metric between baseline and optimized runs.

        Args:
            metric_name: Name of the metric
            baseline_values: List of baseline values
            optimized_values: List of optimized values
            higher_is_better: Whether higher values are better

        Returns:
            MetricComparison with statistics and improvement percentage
        """
        # Calculate statistics for baseline
        baseline_mean = statistics.mean(baseline_values) if baseline_values else 0.0
        baseline_median = statistics.median(baseline_values) if baseline_values else 0.0
        baseline_std_dev = statistics.stdev(baseline_values) if len(baseline_values) > 1 else 0.0
        baseline_p50 = self._percentile(baseline_values, 50) if baseline_values else 0.0
        baseline_p95 = self._percentile(baseline_values, 95) if baseline_values else 0.0
        baseline_p99 = self._percentile(baseline_values, 99) if baseline_values else 0.0

        # Calculate statistics for optimized
        optimized_mean = statistics.mean(optimized_values) if optimized_values else 0.0
        optimized_median = statistics.median(optimized_values) if optimized_values else 0.0
        optimized_std_dev = statistics.stdev(optimized_values) if len(optimized_values) > 1 else 0.0
        optimized_p50 = self._percentile(optimized_values, 50) if optimized_values else 0.0
        optimized_p95 = self._percentile(optimized_values, 95) if optimized_values else 0.0
        optimized_p99 = self._percentile(optimized_values, 99) if optimized_values else 0.0

        # Calculate improvement percentage
        improvement = self._calculate_improvement(baseline_mean, optimized_mean, higher_is_better)

        return MetricComparison(
            metric_name=metric_name,
            baseline_value=baseline_mean,
            optimized_value=optimized_mean,
            improvement_percent=improvement,
            baseline_mean=baseline_mean,
            baseline_median=baseline_median,
            baseline_std_dev=baseline_std_dev,
            baseline_p50=baseline_p50,
            baseline_p95=baseline_p95,
            baseline_p99=baseline_p99,
            optimized_mean=optimized_mean,
            optimized_median=optimized_median,
            optimized_std_dev=optimized_std_dev,
            optimized_p50=optimized_p50,
            optimized_p95=optimized_p95,
            optimized_p99=optimized_p99,
        )

    def _calculate_improvement(
        self, baseline: float, optimized: float, higher_is_better: bool
    ) -> float:
        """Calculate percentage improvement.

        Args:
            baseline: Baseline value
            optimized: Optimized value
            higher_is_better: Whether higher values are better

        Returns:
            Percentage improvement (positive = better)
        """
        if baseline == 0:
            return 0.0

        change_percent = ((optimized - baseline) / baseline) * 100

        # Flip sign if lower is better
        if not higher_is_better:
            change_percent = -change_percent

        return change_percent

    def _percentile(self, values: list[float], percentile: int) -> float:
        """Calculate percentile of values.

        Args:
            values: List of values
            percentile: Percentile to calculate (0-100)

        Returns:
            Percentile value
        """
        if not values:
            return 0.0

        sorted_values = sorted(values)
        index = (percentile / 100) * (len(sorted_values) - 1)

        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower = sorted_values[int(index)]
            upper = sorted_values[int(index) + 1]
            fraction = index - int(index)
            return lower + (upper - lower) * fraction

    def _calculate_overall_improvement(
        self,
        throughput: MetricComparison,
        latency: MetricComparison,
        cpu: MetricComparison,
        memory: MetricComparison,
    ) -> float:
        """Calculate overall improvement as weighted average of key metrics.

        Args:
            throughput: Throughput comparison
            latency: Latency comparison
            cpu: CPU comparison
            memory: Memory comparison

        Returns:
            Overall improvement percentage
        """
        # Weight key metrics (throughput and latency are most important)
        weights = {
            "throughput": 0.4,
            "latency": 0.4,
            "cpu": 0.1,
            "memory": 0.1,
        }

        weighted_sum = (
            throughput.improvement_percent * weights["throughput"]
            + latency.improvement_percent * weights["latency"]
            + cpu.improvement_percent * weights["cpu"]
            + memory.improvement_percent * weights["memory"]
        )

        return weighted_sum

    def format_report(self, report: ComparisonReport) -> str:
        """Generate a human-readable comparison report.

        Args:
            report: ComparisonReport to format

        Returns:
            Formatted string report
        """
        lines = ["=" * 80]
        lines.append("BEFORE/AFTER PERFORMANCE COMPARISON REPORT")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Baseline Runs: {', '.join(report.baseline_runs)}")
        lines.append(f"Optimized Runs: {', '.join(report.optimized_runs)}")
        lines.append("")

        lines.append(f"OVERALL IMPROVEMENT: {report.overall_improvement_percent:+.1f}%")
        lines.append("")

        # Performance metrics
        lines.append("PERFORMANCE METRICS:")
        lines.append("")

        lines.append("Throughput (products/sec):")
        lines.append(self._format_comparison(report.throughput_comparison))
        lines.append("")

        lines.append("Latency (ms/product):")
        lines.append(self._format_comparison(report.latency_comparison))
        lines.append("")

        lines.append("Error Rate:")
        lines.append(self._format_comparison(report.error_rate_comparison, as_percent=True))
        lines.append("")

        # Resource utilization
        lines.append("RESOURCE UTILIZATION:")
        lines.append("")

        lines.append("CPU Utilization (%):")
        lines.append(self._format_comparison(report.cpu_comparison))
        lines.append("")

        lines.append("Peak Memory (MB):")
        lines.append(self._format_comparison(report.memory_comparison))
        lines.append("")

        lines.append("Disk Read (MB):")
        lines.append(self._format_comparison(report.disk_read_comparison))
        lines.append("")

        lines.append("Disk Write (MB):")
        lines.append(self._format_comparison(report.disk_write_comparison))
        lines.append("")

        lines.append("Network Sent (MB):")
        lines.append(self._format_comparison(report.network_sent_comparison))
        lines.append("")

        lines.append("Network Received (MB):")
        lines.append(self._format_comparison(report.network_recv_comparison))
        lines.append("")

        # Cache hit rates
        lines.append("CACHE HIT RATES:")
        lines.append("")

        lines.append("L1 (Memory) Cache:")
        lines.append(self._format_comparison(report.cache_l1_comparison, as_percent=True))
        lines.append("")

        lines.append("L2 (Redis) Cache:")
        lines.append(self._format_comparison(report.cache_l2_comparison, as_percent=True))
        lines.append("")

        lines.append("L3 (File) Cache:")
        lines.append(self._format_comparison(report.cache_l3_comparison, as_percent=True))
        lines.append("")

        # Data quality
        lines.append("DATA QUALITY:")
        lines.append("")

        lines.append("Data Completeness (%):")
        lines.append(self._format_comparison(report.data_completeness_comparison))
        lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)

    def _format_comparison(self, comparison: MetricComparison, as_percent: bool = False) -> str:
        """Format a single metric comparison.

        Args:
            comparison: MetricComparison to format
            as_percent: Whether to display values as percentages

        Returns:
            Formatted string
        """
        multiplier = 100 if as_percent else 1
        suffix = "%" if as_percent else ""

        lines = []
        lines.append(
            f"  Baseline:  Mean={comparison.baseline_mean * multiplier:.2f}{suffix}, "
            f"Median={comparison.baseline_median * multiplier:.2f}{suffix}, "
            f"StdDev={comparison.baseline_std_dev * multiplier:.2f}{suffix}"
        )
        lines.append(
            f"             P50={comparison.baseline_p50 * multiplier:.2f}{suffix}, "
            f"P95={comparison.baseline_p95 * multiplier:.2f}{suffix}, "
            f"P99={comparison.baseline_p99 * multiplier:.2f}{suffix}"
        )
        lines.append(
            f"  Optimized: Mean={comparison.optimized_mean * multiplier:.2f}{suffix}, "
            f"Median={comparison.optimized_median * multiplier:.2f}{suffix}, "
            f"StdDev={comparison.optimized_std_dev * multiplier:.2f}{suffix}"
        )
        lines.append(
            f"             P50={comparison.optimized_p50 * multiplier:.2f}{suffix}, "
            f"P95={comparison.optimized_p95 * multiplier:.2f}{suffix}, "
            f"P99={comparison.optimized_p99 * multiplier:.2f}{suffix}"
        )

        improvement_sign = "+" if comparison.improvement_percent > 0 else ""
        lines.append(f"  Improvement: {improvement_sign}{comparison.improvement_percent:.1f}%")

        return "\n".join(lines)


# Global comparison report generator instance
_global_comparison_generator: ComparisonReportGenerator | None = None


def get_global_comparison_generator() -> ComparisonReportGenerator:
    """Get the global comparison report generator instance.

    Creates the instance if it doesn't exist.
    """
    global _global_comparison_generator
    if _global_comparison_generator is None:
        _global_comparison_generator = ComparisonReportGenerator()
    return _global_comparison_generator
