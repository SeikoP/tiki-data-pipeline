"""Benchmark reproducibility validator for performance testing.

This module validates that benchmarks produce consistent results across
multiple runs, calculating variance and flagging high-variance benchmarks.
"""

import statistics
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from .metrics_collector import PipelineRunMetrics


@dataclass
class ReproducibilityMetrics:
    """Metrics for assessing benchmark reproducibility.
    
    Attributes:
        metric_name: Name of the metric being analyzed
        values: List of values from multiple runs
        mean: Mean value
        median: Median value
        std_dev: Standard deviation
        variance: Variance
        coefficient_of_variation: CV (std_dev / mean)
        min_value: Minimum value
        max_value: Maximum value
        range_percent: Range as percentage of mean
        is_reproducible: Whether variance is acceptable (<10%)
    """
    
    metric_name: str
    values: List[float]
    mean: float
    median: float
    std_dev: float
    variance: float
    coefficient_of_variation: float
    min_value: float
    max_value: float
    range_percent: float
    is_reproducible: bool
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary for serialization."""
        return {
            "metric_name": self.metric_name,
            "values": [round(v, 4) for v in self.values],
            "mean": round(self.mean, 4),
            "median": round(self.median, 4),
            "std_dev": round(self.std_dev, 4),
            "variance": round(self.variance, 4),
            "coefficient_of_variation": round(self.coefficient_of_variation, 4),
            "min_value": round(self.min_value, 4),
            "max_value": round(self.max_value, 4),
            "range_percent": round(self.range_percent, 2),
            "is_reproducible": self.is_reproducible,
        }


@dataclass
class ReproducibilityReport:
    """Report on benchmark reproducibility across multiple runs.
    
    Attributes:
        benchmark_id: Identifier for the benchmark
        run_ids: List of run IDs included in analysis
        num_runs: Number of runs analyzed
        analyzed_at: When the analysis was performed
        metrics: Reproducibility metrics for each performance metric
        overall_reproducible: Whether all metrics are reproducible
        flagged_metrics: List of metrics with high variance
        summary: Human-readable summary
    """
    
    benchmark_id: str
    run_ids: List[str]
    num_runs: int
    analyzed_at: datetime
    metrics: Dict[str, ReproducibilityMetrics]
    overall_reproducible: bool
    flagged_metrics: List[str]
    summary: str
    
    def to_dict(self) -> Dict:
        """Convert report to dictionary for serialization."""
        return {
            "benchmark_id": self.benchmark_id,
            "run_ids": self.run_ids,
            "num_runs": self.num_runs,
            "analyzed_at": self.analyzed_at.isoformat(),
            "metrics": {name: m.to_dict() for name, m in self.metrics.items()},
            "overall_reproducible": self.overall_reproducible,
            "flagged_metrics": self.flagged_metrics,
            "summary": self.summary,
        }


class BenchmarkReproducibilityValidator:
    """Validates benchmark reproducibility across multiple runs.
    
    Runs the same benchmark multiple times and calculates variance in results.
    Flags benchmarks with high variance (>10%) as potentially unreliable.
    
    Example:
        >>> validator = BenchmarkReproducibilityValidator()
        >>> # Run benchmark multiple times
        >>> runs = []
        >>> for i in range(5):
        ...     metrics = run_benchmark(workload)
        ...     runs.append(metrics)
        >>> # Validate reproducibility
        >>> report = validator.validate_reproducibility(
        ...     benchmark_id="benchmark_001",
        ...     runs=runs
        ... )
        >>> if not report.overall_reproducible:
        ...     print(f"Warning: High variance in {report.flagged_metrics}")
    """
    
    # Variance threshold for reproducibility (10%)
    VARIANCE_THRESHOLD = 0.10
    
    def __init__(self, variance_threshold: float = VARIANCE_THRESHOLD):
        """Initialize the reproducibility validator.
        
        Args:
            variance_threshold: Maximum acceptable coefficient of variation (default: 0.10)
        """
        self.variance_threshold = variance_threshold
    
    def validate_reproducibility(
        self,
        benchmark_id: str,
        runs: List[PipelineRunMetrics],
        min_runs: int = 3
    ) -> ReproducibilityReport:
        """Validate reproducibility across multiple benchmark runs.
        
        Args:
            benchmark_id: Identifier for the benchmark
            runs: List of pipeline run metrics from repeated executions
            min_runs: Minimum number of runs required (default: 3)
        
        Returns:
            ReproducibilityReport with variance analysis
        
        Raises:
            ValueError: If fewer than min_runs provided
        """
        if len(runs) < min_runs:
            raise ValueError(
                f"At least {min_runs} runs required for reproducibility analysis, "
                f"got {len(runs)}"
            )
        
        run_ids = [r.run_id for r in runs]
        
        # Analyze each metric
        metrics = {}
        flagged_metrics = []
        
        # Throughput
        throughput_metrics = self._analyze_metric(
            "throughput_products_per_second",
            [r.throughput_products_per_second for r in runs]
        )
        metrics["throughput"] = throughput_metrics
        if not throughput_metrics.is_reproducible:
            flagged_metrics.append("throughput")
        
        # Latency
        latency_metrics = self._analyze_metric(
            "latency_per_product_ms",
            [r.latency_per_product_ms for r in runs]
        )
        metrics["latency"] = latency_metrics
        if not latency_metrics.is_reproducible:
            flagged_metrics.append("latency")
        
        # CPU utilization
        cpu_metrics = self._analyze_metric(
            "cpu_utilization_percent",
            [r.cpu_utilization_percent for r in runs]
        )
        metrics["cpu"] = cpu_metrics
        if not cpu_metrics.is_reproducible:
            flagged_metrics.append("cpu")
        
        # Memory peak
        memory_metrics = self._analyze_metric(
            "memory_peak_mb",
            [r.memory_peak_mb for r in runs]
        )
        metrics["memory"] = memory_metrics
        if not memory_metrics.is_reproducible:
            flagged_metrics.append("memory")
        
        # Error rate
        error_metrics = self._analyze_metric(
            "error_rate",
            [r.error_rate for r in runs]
        )
        metrics["error_rate"] = error_metrics
        if not error_metrics.is_reproducible:
            flagged_metrics.append("error_rate")
        
        # Duration
        duration_metrics = self._analyze_metric(
            "duration_seconds",
            [r.duration_seconds for r in runs]
        )
        metrics["duration"] = duration_metrics
        if not duration_metrics.is_reproducible:
            flagged_metrics.append("duration")
        
        # Overall reproducibility
        overall_reproducible = len(flagged_metrics) == 0
        
        # Generate summary
        summary = self._generate_summary(
            num_runs=len(runs),
            overall_reproducible=overall_reproducible,
            flagged_metrics=flagged_metrics,
            metrics=metrics
        )
        
        return ReproducibilityReport(
            benchmark_id=benchmark_id,
            run_ids=run_ids,
            num_runs=len(runs),
            analyzed_at=datetime.now(),
            metrics=metrics,
            overall_reproducible=overall_reproducible,
            flagged_metrics=flagged_metrics,
            summary=summary,
        )
    
    def _analyze_metric(
        self,
        metric_name: str,
        values: List[float]
    ) -> ReproducibilityMetrics:
        """Analyze reproducibility of a single metric.
        
        Args:
            metric_name: Name of the metric
            values: List of values from multiple runs
        
        Returns:
            ReproducibilityMetrics with variance analysis
        """
        # Filter out zero values for CV calculation
        non_zero_values = [v for v in values if v != 0]
        
        mean = statistics.mean(values)
        median = statistics.median(values)
        std_dev = statistics.stdev(values) if len(values) > 1 else 0.0
        variance = statistics.variance(values) if len(values) > 1 else 0.0
        
        # Coefficient of variation (CV)
        if mean != 0:
            cv = std_dev / abs(mean)
        else:
            cv = 0.0 if std_dev == 0 else float('inf')
        
        min_value = min(values)
        max_value = max(values)
        
        # Range as percentage of mean
        if mean != 0:
            range_percent = ((max_value - min_value) / abs(mean)) * 100
        else:
            range_percent = 0.0
        
        # Check if reproducible (CV within threshold)
        is_reproducible = cv <= self.variance_threshold
        
        return ReproducibilityMetrics(
            metric_name=metric_name,
            values=values,
            mean=mean,
            median=median,
            std_dev=std_dev,
            variance=variance,
            coefficient_of_variation=cv,
            min_value=min_value,
            max_value=max_value,
            range_percent=range_percent,
            is_reproducible=is_reproducible,
        )
    
    def _generate_summary(
        self,
        num_runs: int,
        overall_reproducible: bool,
        flagged_metrics: List[str],
        metrics: Dict[str, ReproducibilityMetrics]
    ) -> str:
        """Generate human-readable summary of reproducibility analysis.
        
        Args:
            num_runs: Number of runs analyzed
            overall_reproducible: Whether all metrics are reproducible
            flagged_metrics: List of metrics with high variance
            metrics: Dictionary of reproducibility metrics
        
        Returns:
            Formatted summary string
        """
        lines = ["=" * 80]
        lines.append("BENCHMARK REPRODUCIBILITY REPORT")
        lines.append("=" * 80)
        lines.append("")
        
        lines.append(f"Number of Runs: {num_runs}")
        lines.append(f"Variance Threshold: {self.variance_threshold * 100:.0f}%")
        lines.append("")
        
        if overall_reproducible:
            lines.append("✓ BENCHMARK IS REPRODUCIBLE")
            lines.append("  All metrics show acceptable variance (<10%)")
        else:
            lines.append("✗ BENCHMARK HAS HIGH VARIANCE")
            lines.append(f"  {len(flagged_metrics)} metric(s) exceed variance threshold:")
            for metric in flagged_metrics:
                lines.append(f"    - {metric}")
        
        lines.append("")
        lines.append("METRIC ANALYSIS:")
        lines.append("")
        
        for name, metric in metrics.items():
            status = "✓" if metric.is_reproducible else "✗"
            lines.append(f"{status} {metric.metric_name}:")
            lines.append(f"    Mean: {metric.mean:.4f}")
            lines.append(f"    Std Dev: {metric.std_dev:.4f}")
            lines.append(f"    CV: {metric.coefficient_of_variation * 100:.2f}%")
            lines.append(f"    Range: {metric.min_value:.4f} - {metric.max_value:.4f} "
                        f"({metric.range_percent:.2f}% of mean)")
            lines.append("")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)
    
    def format_report(self, report: ReproducibilityReport) -> str:
        """Format reproducibility report for display.
        
        Args:
            report: ReproducibilityReport to format
        
        Returns:
            Formatted string
        """
        return report.summary
    
    def compare_reproducibility(
        self,
        report1: ReproducibilityReport,
        report2: ReproducibilityReport
    ) -> Dict:
        """Compare reproducibility between two benchmarks.
        
        Args:
            report1: First reproducibility report
            report2: Second reproducibility report
        
        Returns:
            Dictionary with comparison results
        """
        comparison = {
            "benchmark1_id": report1.benchmark_id,
            "benchmark2_id": report2.benchmark_id,
            "benchmark1_reproducible": report1.overall_reproducible,
            "benchmark2_reproducible": report2.overall_reproducible,
            "metric_comparisons": {},
        }
        
        # Compare each metric
        for metric_name in report1.metrics.keys():
            if metric_name in report2.metrics:
                m1 = report1.metrics[metric_name]
                m2 = report2.metrics[metric_name]
                
                comparison["metric_comparisons"][metric_name] = {
                    "cv1": m1.coefficient_of_variation,
                    "cv2": m2.coefficient_of_variation,
                    "cv_difference": abs(m2.coefficient_of_variation - m1.coefficient_of_variation),
                    "both_reproducible": m1.is_reproducible and m2.is_reproducible,
                }
        
        return comparison
    
    def recommend_min_runs(
        self,
        pilot_runs: List[PipelineRunMetrics],
        target_confidence: float = 0.95
    ) -> int:
        """Recommend minimum number of runs based on pilot data.
        
        Analyzes variance in pilot runs to recommend how many runs are needed
        to achieve target confidence level.
        
        Args:
            pilot_runs: Initial pilot runs (at least 3)
            target_confidence: Target confidence level (default: 0.95)
        
        Returns:
            Recommended minimum number of runs
        """
        if len(pilot_runs) < 3:
            return 5  # Default recommendation
        
        # Analyze throughput variance (key metric)
        throughput_values = [r.throughput_products_per_second for r in pilot_runs]
        
        if not throughput_values or all(v == 0 for v in throughput_values):
            return 5  # Default if no valid data
        
        mean = statistics.mean(throughput_values)
        std_dev = statistics.stdev(throughput_values) if len(throughput_values) > 1 else 0.0
        
        if mean == 0:
            return 5  # Default if mean is zero
        
        cv = std_dev / abs(mean)
        
        # Use rule of thumb: n = (z * cv / margin_of_error)^2
        # For 95% confidence, z = 1.96
        # Target margin of error: 5% of mean
        z = 1.96
        margin_of_error = 0.05
        
        n = ((z * cv) / margin_of_error) ** 2
        
        # Round up and ensure minimum of 3
        recommended = max(3, int(n) + 1)
        
        # Cap at reasonable maximum
        return min(recommended, 20)


# Global reproducibility validator instance
_global_reproducibility_validator: Optional[BenchmarkReproducibilityValidator] = None


def get_global_reproducibility_validator() -> BenchmarkReproducibilityValidator:
    """Get the global reproducibility validator instance.
    
    Creates the instance if it doesn't exist.
    """
    global _global_reproducibility_validator
    if _global_reproducibility_validator is None:
        _global_reproducibility_validator = BenchmarkReproducibilityValidator()
    return _global_reproducibility_validator
