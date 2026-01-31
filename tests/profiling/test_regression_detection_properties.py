"""Property-based tests for performance regression detection.

These tests verify correctness properties of the RegressionDetector
using property-based testing with hypothesis.

Feature: performance-optimization
Property 16: Performance Regression Detection
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, strategies as st, settings, assume
    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.regression_detector import (
    RegressionDetector,
    RegressionSeverity,
    RegressionAlert,
    RegressionReport
)
from common.metrics_collector import PipelineRunMetrics
from datetime import datetime


# Helper strategies for generating test data
def pipeline_metrics_strategy(run_id_prefix="run"):
    """Strategy for generating PipelineRunMetrics."""
    return st.builds(
        PipelineRunMetrics,
        run_id=st.text(min_size=1, max_size=20, alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd'))).map(lambda x: f"{run_id_prefix}_{x}"),
        start_time=st.just(datetime.now()),
        end_time=st.just(datetime.now()),
        duration_seconds=st.floats(min_value=1.0, max_value=3600.0, allow_nan=False, allow_infinity=False),
        products_processed=st.integers(min_value=1, max_value=100000),
        throughput_products_per_second=st.floats(min_value=0.1, max_value=10000.0, allow_nan=False, allow_infinity=False),
        latency_per_product_ms=st.floats(min_value=0.1, max_value=10000.0, allow_nan=False, allow_infinity=False),
        error_count=st.integers(min_value=0, max_value=1000),
        error_rate=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        cpu_utilization_percent=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        memory_peak_mb=st.floats(min_value=100.0, max_value=65536.0, allow_nan=False, allow_infinity=False),
        memory_avg_mb=st.floats(min_value=100.0, max_value=65536.0, allow_nan=False, allow_infinity=False),
        cache_l1_hit_rate=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        cache_l2_hit_rate=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
        cache_l3_hit_rate=st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False),
    )


# Property 16: Performance Regression Detection
# For any pipeline run where performance metrics degrade beyond configured
# thresholds compared to baseline, the system should generate alerts


def test_property_16_manual_no_regression():
    """Property 16: No Regression - Identical Metrics
    
    **Validates: Requirements 8.4**
    
    When metrics are identical, no regressions should be detected.
    """
    detector = RegressionDetector()
    
    # Create identical baseline and current metrics
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
        cache_l2_hit_rate=0.6,
        cache_l3_hit_rate=0.4,
    )
    
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
        cache_l2_hit_rate=0.6,
        cache_l3_hit_rate=0.4,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert not report.has_regressions
    assert len(report.alerts) == 0
    assert report.critical_count == 0
    assert report.high_count == 0
    assert report.medium_count == 0
    assert report.low_count == 0
    
    print("✅ Property 16: No regression test PASSED")


def test_property_16_manual_throughput_regression():
    """Property 16: Throughput Regression Detection
    
    **Validates: Requirements 8.4**
    
    When throughput degrades beyond threshold, alert should be generated.
    """
    detector = RegressionDetector(throughput_threshold=10.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Throughput drops by 20% (100 -> 80)
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=80.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    assert len(report.alerts) >= 1
    
    # Find throughput alert
    throughput_alerts = [a for a in report.alerts if a.metric_name == "throughput_products_per_second"]
    assert len(throughput_alerts) == 1
    
    alert = throughput_alerts[0]
    assert alert.degradation_percent >= 10.0
    assert alert.severity in [RegressionSeverity.MEDIUM, RegressionSeverity.HIGH]
    
    print("✅ Property 16: Throughput regression test PASSED")


def test_property_16_manual_latency_regression():
    """Property 16: Latency Regression Detection
    
    **Validates: Requirements 8.4**
    
    When latency increases beyond threshold, alert should be generated.
    """
    detector = RegressionDetector(latency_threshold=10.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Latency increases by 30% (10 -> 13)
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=13.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    
    # Find latency alert
    latency_alerts = [a for a in report.alerts if a.metric_name == "latency_per_product_ms"]
    assert len(latency_alerts) == 1
    
    alert = latency_alerts[0]
    assert alert.degradation_percent >= 10.0
    assert alert.severity in [RegressionSeverity.MEDIUM, RegressionSeverity.HIGH]
    
    print("✅ Property 16: Latency regression test PASSED")


def test_property_16_manual_error_rate_regression():
    """Property 16: Error Rate Regression Detection
    
    **Validates: Requirements 8.4**
    
    When error rate increases beyond threshold, alert should be generated.
    """
    detector = RegressionDetector(error_rate_threshold=5.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,  # 1%
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Error rate increases by 60% (0.01 -> 0.016)
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.016,  # 1.6%
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    
    # Find error rate alert
    error_alerts = [a for a in report.alerts if a.metric_name == "error_rate"]
    assert len(error_alerts) == 1
    
    alert = error_alerts[0]
    assert alert.degradation_percent >= 5.0
    # 60% degradation should be CRITICAL (>50%)
    assert alert.severity == RegressionSeverity.CRITICAL
    
    print("✅ Property 16: Error rate regression test PASSED")


def test_property_16_manual_memory_regression():
    """Property 16: Memory Regression Detection
    
    **Validates: Requirements 8.4**
    
    When memory usage increases beyond threshold, alert should be generated.
    """
    detector = RegressionDetector(memory_threshold=20.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Memory increases by 30% (1000 -> 1300)
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1300.0,
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    
    # Find memory alert
    memory_alerts = [a for a in report.alerts if a.metric_name == "memory_peak_mb"]
    assert len(memory_alerts) == 1
    
    alert = memory_alerts[0]
    assert alert.degradation_percent >= 20.0
    assert alert.severity in [RegressionSeverity.MEDIUM, RegressionSeverity.HIGH]
    
    print("✅ Property 16: Memory regression test PASSED")


def test_property_16_manual_cache_hit_rate_regression():
    """Property 16: Cache Hit Rate Regression Detection
    
    **Validates: Requirements 8.4**
    
    When cache hit rate drops beyond threshold, alert should be generated.
    """
    detector = RegressionDetector(cache_hit_rate_threshold=5.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,  # 80%
        cache_l2_hit_rate=0.6,  # 60%
        cache_l3_hit_rate=0.4,  # 40%
    )
    
    # L1 cache hit rate drops by 15% (0.8 -> 0.68)
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.68,
        cache_l2_hit_rate=0.6,
        cache_l3_hit_rate=0.4,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    
    # Find cache hit rate alert
    cache_alerts = [a for a in report.alerts if "cache" in a.metric_name and "hit_rate" in a.metric_name]
    assert len(cache_alerts) >= 1
    
    l1_alerts = [a for a in cache_alerts if "l1" in a.metric_name]
    assert len(l1_alerts) == 1
    
    alert = l1_alerts[0]
    assert alert.degradation_percent >= 5.0
    
    print("✅ Property 16: Cache hit rate regression test PASSED")


def test_property_16_manual_severity_levels():
    """Property 16: Severity Level Classification
    
    **Validates: Requirements 8.4**
    
    Regressions should be classified into correct severity levels.
    """
    detector = RegressionDetector(throughput_threshold=5.0)
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Test LOW severity (5-10% degradation)
    current_low = PipelineRunMetrics(
        run_id="current_low",
        start_time=datetime.now(),
        throughput_products_per_second=93.0,  # 7% drop
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report_low = detector.detect_regressions(current_low)
    assert report_low.has_regressions
    assert report_low.low_count >= 1
    
    # Test MEDIUM severity (10-25% degradation)
    current_medium = PipelineRunMetrics(
        run_id="current_medium",
        start_time=datetime.now(),
        throughput_products_per_second=85.0,  # 15% drop
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    report_medium = detector.detect_regressions(current_medium)
    assert report_medium.has_regressions
    assert report_medium.medium_count >= 1
    
    # Test HIGH severity (25-50% degradation)
    current_high = PipelineRunMetrics(
        run_id="current_high",
        start_time=datetime.now(),
        throughput_products_per_second=65.0,  # 35% drop
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    report_high = detector.detect_regressions(current_high)
    assert report_high.has_regressions
    assert report_high.high_count >= 1
    
    # Test CRITICAL severity (>50% degradation)
    current_critical = PipelineRunMetrics(
        run_id="current_critical",
        start_time=datetime.now(),
        throughput_products_per_second=40.0,  # 60% drop
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    report_critical = detector.detect_regressions(current_critical)
    assert report_critical.has_regressions
    assert report_critical.critical_count >= 1
    
    print("✅ Property 16: Severity level classification test PASSED")


def test_property_16_manual_multiple_regressions():
    """Property 16: Multiple Simultaneous Regressions
    
    **Validates: Requirements 8.4**
    
    Multiple metrics can regress simultaneously and all should be detected.
    """
    detector = RegressionDetector(
        throughput_threshold=10.0,
        latency_threshold=10.0,
        memory_threshold=20.0
    )
    
    baseline = PipelineRunMetrics(
        run_id="baseline",
        start_time=datetime.now(),
        throughput_products_per_second=100.0,
        latency_per_product_ms=10.0,
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1000.0,
        cache_l1_hit_rate=0.8,
    )
    
    # Multiple regressions: throughput down, latency up, memory up
    current = PipelineRunMetrics(
        run_id="current",
        start_time=datetime.now(),
        throughput_products_per_second=80.0,  # 20% drop
        latency_per_product_ms=13.0,  # 30% increase
        error_rate=0.01,
        cpu_utilization_percent=50.0,
        memory_peak_mb=1300.0,  # 30% increase
        cache_l1_hit_rate=0.8,
    )
    
    detector.set_baseline(baseline)
    report = detector.detect_regressions(current)
    
    assert report.has_regressions
    assert len(report.alerts) >= 3
    
    # Verify each regression is detected
    metric_names = {alert.metric_name for alert in report.alerts}
    assert "throughput_products_per_second" in metric_names
    assert "latency_per_product_ms" in metric_names
    assert "memory_peak_mb" in metric_names
    
    print("✅ Property 16: Multiple regressions test PASSED")


# Property-based tests using Hypothesis
if HYPOTHESIS_AVAILABLE:
    
    @given(
        baseline=pipeline_metrics_strategy("baseline"),
        current=pipeline_metrics_strategy("current")
    )
    @settings(max_examples=100, deadline=None)
    def test_property_16_pbt_report_structure(baseline, current):
        """Property 16: Report Structure Validity (PBT)
        
        **Validates: Requirements 8.4**
        
        For any baseline and current metrics, the regression report should have
        valid structure with correct counts and alert properties.
        """
        detector = RegressionDetector()
        detector.set_baseline(baseline)
        report = detector.detect_regressions(current)
        
        # Report structure is valid
        assert isinstance(report, RegressionReport)
        assert report.run_id == current.run_id
        assert report.baseline_run_id == baseline.run_id
        assert isinstance(report.alerts, list)
        
        # Counts match alerts
        total_count = report.critical_count + report.high_count + report.medium_count + report.low_count
        assert total_count == len(report.alerts)
        
        # has_regressions flag is consistent
        assert report.has_regressions == (len(report.alerts) > 0)
        
        # All alerts have valid structure
        for alert in report.alerts:
            assert isinstance(alert, RegressionAlert)
            assert isinstance(alert.metric_name, str)
            assert len(alert.metric_name) > 0
            assert isinstance(alert.severity, RegressionSeverity)
            assert alert.degradation_percent >= 0
            assert alert.threshold_percent >= 0
            assert isinstance(alert.description, str)
            assert len(alert.description) > 0
    
    
    @given(metrics=pipeline_metrics_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_16_pbt_identical_metrics_no_regression(metrics):
        """Property 16: Identical Metrics Produce No Regressions (PBT)
        
        **Validates: Requirements 8.4**
        
        For any metrics, comparing them to themselves should produce no regressions.
        """
        detector = RegressionDetector()
        detector.set_baseline(metrics)
        
        # Create a copy with same values but different run_id
        current = PipelineRunMetrics(
            run_id=f"{metrics.run_id}_copy",
            start_time=metrics.start_time,
            end_time=metrics.end_time,
            duration_seconds=metrics.duration_seconds,
            products_processed=metrics.products_processed,
            throughput_products_per_second=metrics.throughput_products_per_second,
            latency_per_product_ms=metrics.latency_per_product_ms,
            error_count=metrics.error_count,
            error_rate=metrics.error_rate,
            cpu_utilization_percent=metrics.cpu_utilization_percent,
            memory_peak_mb=metrics.memory_peak_mb,
            memory_avg_mb=metrics.memory_avg_mb,
            cache_l1_hit_rate=metrics.cache_l1_hit_rate,
            cache_l2_hit_rate=metrics.cache_l2_hit_rate,
            cache_l3_hit_rate=metrics.cache_l3_hit_rate,
        )
        
        report = detector.detect_regressions(current)
        
        # No regressions for identical metrics
        assert not report.has_regressions
        assert len(report.alerts) == 0
        assert report.critical_count == 0
        assert report.high_count == 0
        assert report.medium_count == 0
        assert report.low_count == 0
    
    
    @given(
        baseline=pipeline_metrics_strategy("baseline"),
        degradation_factor=st.floats(min_value=0.5, max_value=0.95, allow_nan=False, allow_infinity=False)
    )
    @settings(max_examples=100, deadline=None)
    def test_property_16_pbt_throughput_degradation_detected(baseline, degradation_factor):
        """Property 16: Throughput Degradation Detection (PBT)
        
        **Validates: Requirements 8.4**
        
        For any baseline metrics, when throughput degrades significantly,
        a regression should be detected.
        """
        assume(baseline.throughput_products_per_second > 1.0)
        
        detector = RegressionDetector(throughput_threshold=5.0)
        detector.set_baseline(baseline)
        
        # Create current metrics with degraded throughput
        current = PipelineRunMetrics(
            run_id="current",
            start_time=baseline.start_time,
            throughput_products_per_second=baseline.throughput_products_per_second * degradation_factor,
            latency_per_product_ms=baseline.latency_per_product_ms,
            error_rate=baseline.error_rate,
            cpu_utilization_percent=baseline.cpu_utilization_percent,
            memory_peak_mb=baseline.memory_peak_mb,
            cache_l1_hit_rate=baseline.cache_l1_hit_rate,
            cache_l2_hit_rate=baseline.cache_l2_hit_rate,
            cache_l3_hit_rate=baseline.cache_l3_hit_rate,
        )
        
        report = detector.detect_regressions(current)
        
        # Should detect throughput regression
        assert report.has_regressions
        throughput_alerts = [a for a in report.alerts if a.metric_name == "throughput_products_per_second"]
        assert len(throughput_alerts) == 1
        
        alert = throughput_alerts[0]
        assert alert.degradation_percent > 5.0
        assert alert.severity in [RegressionSeverity.LOW, RegressionSeverity.MEDIUM, 
                                  RegressionSeverity.HIGH, RegressionSeverity.CRITICAL]
    
    
    @given(
        baseline=pipeline_metrics_strategy("baseline"),
        improvement_factor=st.floats(min_value=1.05, max_value=2.0, allow_nan=False, allow_infinity=False)
    )
    @settings(max_examples=100, deadline=None)
    def test_property_16_pbt_improvements_not_flagged(baseline, improvement_factor):
        """Property 16: Improvements Not Flagged as Regressions (PBT)
        
        **Validates: Requirements 8.4**
        
        For any baseline metrics, when metrics improve (throughput up, latency down),
        no regressions should be detected.
        """
        assume(baseline.throughput_products_per_second > 1.0)
        assume(baseline.latency_per_product_ms > 1.0)
        assume(baseline.memory_peak_mb > 100.0)
        
        detector = RegressionDetector()
        detector.set_baseline(baseline)
        
        # Create current metrics with improvements
        current = PipelineRunMetrics(
            run_id="current",
            start_time=baseline.start_time,
            throughput_products_per_second=baseline.throughput_products_per_second * improvement_factor,  # Better
            latency_per_product_ms=baseline.latency_per_product_ms / improvement_factor,  # Better
            error_rate=baseline.error_rate / improvement_factor if baseline.error_rate > 0 else 0.0,  # Better
            cpu_utilization_percent=baseline.cpu_utilization_percent / improvement_factor,  # Better
            memory_peak_mb=baseline.memory_peak_mb / improvement_factor,  # Better
            cache_l1_hit_rate=min(1.0, baseline.cache_l1_hit_rate * improvement_factor),  # Better
            cache_l2_hit_rate=min(1.0, baseline.cache_l2_hit_rate * improvement_factor),  # Better
            cache_l3_hit_rate=min(1.0, baseline.cache_l3_hit_rate * improvement_factor),  # Better
        )
        
        report = detector.detect_regressions(current)
        
        # No regressions for improvements
        assert not report.has_regressions
        assert len(report.alerts) == 0


# Run manual tests if executed directly
if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("Running Property 16: Performance Regression Detection Tests")
    print("=" * 80 + "\n")
    
    test_property_16_manual_no_regression()
    test_property_16_manual_throughput_regression()
    test_property_16_manual_latency_regression()
    test_property_16_manual_error_rate_regression()
    test_property_16_manual_memory_regression()
    test_property_16_manual_cache_hit_rate_regression()
    test_property_16_manual_severity_levels()
    test_property_16_manual_multiple_regressions()
    
    if HYPOTHESIS_AVAILABLE:
        print("\nRunning property-based tests with Hypothesis...")
        test_property_16_pbt_report_structure()
        test_property_16_pbt_identical_metrics_no_regression()
        test_property_16_pbt_throughput_degradation_detected()
        test_property_16_pbt_improvements_not_flagged()
        print("✅ All property-based tests PASSED")
    else:
        print("\n⚠️  Hypothesis not available, skipping property-based tests")
    
    print("\n" + "=" * 80)
    print("All Property 16 tests completed successfully!")
    print("=" * 80)
