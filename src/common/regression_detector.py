"""Performance regression detection for pipeline runs.

This module provides automated detection of performance regressions by comparing
current metrics against baseline metrics and triggering alerts when degradation
exceeds configured thresholds.
"""

from dataclasses import dataclass
from enum import Enum

from .metrics_collector import PipelineRunMetrics


class RegressionSeverity(Enum):
    """Severity levels for performance regressions."""

    CRITICAL = "CRITICAL"  # >50% degradation
    HIGH = "HIGH"  # 25-50% degradation
    MEDIUM = "MEDIUM"  # 10-25% degradation
    LOW = "LOW"  # 5-10% degradation
    NONE = "NONE"  # <5% degradation


@dataclass
class RegressionAlert:
    """Alert for a detected performance regression.

    Attributes:
        metric_name: Name of the metric that regressed
        severity: Severity level of the regression
        baseline_value: Baseline metric value
        current_value: Current metric value
        degradation_percent: Percentage degradation (positive = worse)
        threshold_percent: Threshold that was exceeded
        description: Human-readable description of the regression
    """

    metric_name: str
    severity: RegressionSeverity
    baseline_value: float
    current_value: float
    degradation_percent: float
    threshold_percent: float
    description: str

    def to_dict(self) -> dict:
        """Convert alert to dictionary for serialization."""
        return {
            "metric_name": self.metric_name,
            "severity": self.severity.value,
            "baseline_value": round(self.baseline_value, 2),
            "current_value": round(self.current_value, 2),
            "degradation_percent": round(self.degradation_percent, 2),
            "threshold_percent": round(self.threshold_percent, 2),
            "description": self.description,
        }


@dataclass
class RegressionReport:
    """Report of all detected regressions for a pipeline run.

    Attributes:
        run_id: ID of the run being compared
        baseline_run_id: ID of the baseline run
        alerts: List of regression alerts
        has_regressions: Whether any regressions were detected
        critical_count: Number of critical regressions
        high_count: Number of high severity regressions
        medium_count: Number of medium severity regressions
        low_count: Number of low severity regressions
    """

    run_id: str
    baseline_run_id: str
    alerts: list[RegressionAlert]
    has_regressions: bool = False
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0

    def __post_init__(self):
        """Calculate counts and has_regressions flag."""
        self.has_regressions = len(self.alerts) > 0
        self.critical_count = sum(
            1 for a in self.alerts if a.severity == RegressionSeverity.CRITICAL
        )
        self.high_count = sum(1 for a in self.alerts if a.severity == RegressionSeverity.HIGH)
        self.medium_count = sum(1 for a in self.alerts if a.severity == RegressionSeverity.MEDIUM)
        self.low_count = sum(1 for a in self.alerts if a.severity == RegressionSeverity.LOW)

    def to_dict(self) -> dict:
        """Convert report to dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "baseline_run_id": self.baseline_run_id,
            "has_regressions": self.has_regressions,
            "critical_count": self.critical_count,
            "high_count": self.high_count,
            "medium_count": self.medium_count,
            "low_count": self.low_count,
            "alerts": [alert.to_dict() for alert in self.alerts],
        }


class RegressionDetector:
    """Detects performance regressions by comparing metrics to baseline.

    Compares current pipeline run metrics against a baseline and generates
    alerts when metrics degrade beyond configured thresholds.

    Example:
        >>> detector = RegressionDetector()
        >>> detector.set_baseline(baseline_metrics)
        >>> report = detector.detect_regressions(current_metrics)
        >>> if report.has_regressions:
        ...     print(f"Found {len(report.alerts)} regressions!")
        ...     for alert in report.alerts:
        ...         print(f"  {alert.metric_name}: {alert.degradation_percent:.1f}% worse")
    """

    def __init__(
        self,
        throughput_threshold: float = 10.0,
        latency_threshold: float = 10.0,
        error_rate_threshold: float = 5.0,
        cpu_threshold: float = 20.0,
        memory_threshold: float = 20.0,
        cache_hit_rate_threshold: float = 5.0,
    ):
        """Initialize the regression detector.

        Args:
            throughput_threshold: Alert if throughput degrades by this % (default: 10%)
            latency_threshold: Alert if latency increases by this % (default: 10%)
            error_rate_threshold: Alert if error rate increases by this % (default: 5%)
            cpu_threshold: Alert if CPU usage increases by this % (default: 20%)
            memory_threshold: Alert if memory usage increases by this % (default: 20%)
            cache_hit_rate_threshold: Alert if cache hit rate drops by this % (default: 5%)
        """
        self._baseline: PipelineRunMetrics | None = None
        self._thresholds = {
            "throughput": throughput_threshold,
            "latency": latency_threshold,
            "error_rate": error_rate_threshold,
            "cpu": cpu_threshold,
            "memory": memory_threshold,
            "cache_hit_rate": cache_hit_rate_threshold,
        }

    def set_baseline(self, baseline_metrics: PipelineRunMetrics) -> None:
        """Set the baseline metrics for comparison.

        Args:
            baseline_metrics: Metrics from the baseline run
        """
        self._baseline = baseline_metrics

    def get_baseline(self) -> PipelineRunMetrics | None:
        """Get the current baseline metrics.

        Returns:
            Baseline metrics or None if not set
        """
        return self._baseline

    def detect_regressions(self, current_metrics: PipelineRunMetrics) -> RegressionReport:
        """Detect regressions by comparing current metrics to baseline.

        Args:
            current_metrics: Metrics from the current run

        Returns:
            RegressionReport with all detected regressions

        Raises:
            ValueError: If baseline has not been set
        """
        if self._baseline is None:
            raise ValueError("Baseline not set. Call set_baseline() first.")

        alerts: list[RegressionAlert] = []

        # Check throughput (lower is worse)
        if self._baseline.throughput_products_per_second > 0:
            degradation = self._calculate_degradation(
                self._baseline.throughput_products_per_second,
                current_metrics.throughput_products_per_second,
                lower_is_worse=True,
            )
            if degradation > self._thresholds["throughput"]:
                alerts.append(
                    RegressionAlert(
                        metric_name="throughput_products_per_second",
                        severity=self._get_severity(degradation),
                        baseline_value=self._baseline.throughput_products_per_second,
                        current_value=current_metrics.throughput_products_per_second,
                        degradation_percent=degradation,
                        threshold_percent=self._thresholds["throughput"],
                        description=f"Throughput decreased by {degradation:.1f}% "
                        f"(from {self._baseline.throughput_products_per_second:.2f} "
                        f"to {current_metrics.throughput_products_per_second:.2f} products/sec)",
                    )
                )

        # Check latency (higher is worse)
        if self._baseline.latency_per_product_ms > 0:
            degradation = self._calculate_degradation(
                self._baseline.latency_per_product_ms,
                current_metrics.latency_per_product_ms,
                lower_is_worse=False,
            )
            if degradation > self._thresholds["latency"]:
                alerts.append(
                    RegressionAlert(
                        metric_name="latency_per_product_ms",
                        severity=self._get_severity(degradation),
                        baseline_value=self._baseline.latency_per_product_ms,
                        current_value=current_metrics.latency_per_product_ms,
                        degradation_percent=degradation,
                        threshold_percent=self._thresholds["latency"],
                        description=f"Latency increased by {degradation:.1f}% "
                        f"(from {self._baseline.latency_per_product_ms:.2f} "
                        f"to {current_metrics.latency_per_product_ms:.2f} ms/product)",
                    )
                )

        # Check error rate (higher is worse)
        degradation = self._calculate_degradation(
            self._baseline.error_rate, current_metrics.error_rate, lower_is_worse=False
        )
        if degradation > self._thresholds["error_rate"]:
            alerts.append(
                RegressionAlert(
                    metric_name="error_rate",
                    severity=self._get_severity(degradation),
                    baseline_value=self._baseline.error_rate,
                    current_value=current_metrics.error_rate,
                    degradation_percent=degradation,
                    threshold_percent=self._thresholds["error_rate"],
                    description=f"Error rate increased by {degradation:.1f}% "
                    f"(from {self._baseline.error_rate * 100:.2f}% "
                    f"to {current_metrics.error_rate * 100:.2f}%)",
                )
            )

        # Check CPU utilization (higher is worse)
        if self._baseline.cpu_utilization_percent > 0:
            degradation = self._calculate_degradation(
                self._baseline.cpu_utilization_percent,
                current_metrics.cpu_utilization_percent,
                lower_is_worse=False,
            )
            if degradation > self._thresholds["cpu"]:
                alerts.append(
                    RegressionAlert(
                        metric_name="cpu_utilization_percent",
                        severity=self._get_severity(degradation),
                        baseline_value=self._baseline.cpu_utilization_percent,
                        current_value=current_metrics.cpu_utilization_percent,
                        degradation_percent=degradation,
                        threshold_percent=self._thresholds["cpu"],
                        description=f"CPU utilization increased by {degradation:.1f}% "
                        f"(from {self._baseline.cpu_utilization_percent:.2f}% "
                        f"to {current_metrics.cpu_utilization_percent:.2f}%)",
                    )
                )

        # Check memory usage (higher is worse)
        if self._baseline.memory_peak_mb > 0:
            degradation = self._calculate_degradation(
                self._baseline.memory_peak_mb, current_metrics.memory_peak_mb, lower_is_worse=False
            )
            if degradation > self._thresholds["memory"]:
                alerts.append(
                    RegressionAlert(
                        metric_name="memory_peak_mb",
                        severity=self._get_severity(degradation),
                        baseline_value=self._baseline.memory_peak_mb,
                        current_value=current_metrics.memory_peak_mb,
                        degradation_percent=degradation,
                        threshold_percent=self._thresholds["memory"],
                        description=f"Peak memory usage increased by {degradation:.1f}% "
                        f"(from {self._baseline.memory_peak_mb:.2f} "
                        f"to {current_metrics.memory_peak_mb:.2f} MB)",
                    )
                )

        # Check cache hit rates (lower is worse)
        for level, baseline_rate, current_rate in [
            ("L1", self._baseline.cache_l1_hit_rate, current_metrics.cache_l1_hit_rate),
            ("L2", self._baseline.cache_l2_hit_rate, current_metrics.cache_l2_hit_rate),
            ("L3", self._baseline.cache_l3_hit_rate, current_metrics.cache_l3_hit_rate),
        ]:
            if baseline_rate > 0:
                degradation = self._calculate_degradation(
                    baseline_rate, current_rate, lower_is_worse=True
                )
                if degradation > self._thresholds["cache_hit_rate"]:
                    alerts.append(
                        RegressionAlert(
                            metric_name=f"cache_{level.lower()}_hit_rate",
                            severity=self._get_severity(degradation),
                            baseline_value=baseline_rate,
                            current_value=current_rate,
                            degradation_percent=degradation,
                            threshold_percent=self._thresholds["cache_hit_rate"],
                            description=f"{level} cache hit rate decreased by {degradation:.1f}% "
                            f"(from {baseline_rate * 100:.2f}% "
                            f"to {current_rate * 100:.2f}%)",
                        )
                    )

        return RegressionReport(
            run_id=current_metrics.run_id, baseline_run_id=self._baseline.run_id, alerts=alerts
        )

    def _calculate_degradation(
        self, baseline: float, current: float, lower_is_worse: bool
    ) -> float:
        """Calculate percentage degradation.

        Args:
            baseline: Baseline value
            current: Current value
            lower_is_worse: If True, lower values are worse (e.g., throughput)
                           If False, higher values are worse (e.g., latency)

        Returns:
            Percentage degradation (positive = worse, negative = better)
        """
        if baseline == 0:
            return 0.0

        change_percent = ((current - baseline) / baseline) * 100

        # Flip sign if lower is worse
        if lower_is_worse:
            change_percent = -change_percent

        return change_percent

    def _get_severity(self, degradation_percent: float) -> RegressionSeverity:
        """Determine severity based on degradation percentage.

        Args:
            degradation_percent: Percentage degradation

        Returns:
            RegressionSeverity level
        """
        if degradation_percent >= 50:
            return RegressionSeverity.CRITICAL
        elif degradation_percent >= 25:
            return RegressionSeverity.HIGH
        elif degradation_percent >= 10:
            return RegressionSeverity.MEDIUM
        elif degradation_percent >= 5:
            return RegressionSeverity.LOW
        else:
            return RegressionSeverity.NONE

    def generate_report(self, regression_report: RegressionReport) -> str:
        """Generate a human-readable regression report.

        Args:
            regression_report: RegressionReport to format

        Returns:
            Formatted string report
        """
        lines = ["=" * 80]
        lines.append("PERFORMANCE REGRESSION REPORT")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Current Run: {regression_report.run_id}")
        lines.append(f"Baseline Run: {regression_report.baseline_run_id}")
        lines.append("")

        if not regression_report.has_regressions:
            lines.append("✅ No performance regressions detected!")
            lines.append("")
            lines.append("=" * 80)
            return "\n".join(lines)

        lines.append(f"⚠️  REGRESSIONS DETECTED: {len(regression_report.alerts)} total")
        lines.append(f"   Critical: {regression_report.critical_count}")
        lines.append(f"   High:     {regression_report.high_count}")
        lines.append(f"   Medium:   {regression_report.medium_count}")
        lines.append(f"   Low:      {regression_report.low_count}")
        lines.append("")

        # Group alerts by severity
        for severity in [
            RegressionSeverity.CRITICAL,
            RegressionSeverity.HIGH,
            RegressionSeverity.MEDIUM,
            RegressionSeverity.LOW,
        ]:
            severity_alerts = [a for a in regression_report.alerts if a.severity == severity]
            if not severity_alerts:
                continue

            lines.append(f"{severity.value} REGRESSIONS:")
            for alert in severity_alerts:
                lines.append(f"  • {alert.description}")
            lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)


# Global regression detector instance
_global_regression_detector: RegressionDetector | None = None


def get_global_regression_detector() -> RegressionDetector:
    """Get the global regression detector instance.

    Creates the instance if it doesn't exist.
    """
    global _global_regression_detector
    if _global_regression_detector is None:
        _global_regression_detector = RegressionDetector()
    return _global_regression_detector
