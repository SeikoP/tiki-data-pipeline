"""Property-based tests for performance metrics collection.

These tests verify correctness properties of the MetricsCollector
using property-based testing with hypothesis.

Feature: performance-optimization
Property 15: Performance Metrics Collection
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

from common.metrics_collector import MetricsCollector, PipelineRunMetrics
from datetime import datetime


# Helper strategies for generating test data
def resource_summary_strategy():
    """Strategy for generating resource summary dictionaries."""
    return st.fixed_dictionaries({
        "cpu_percent": st.fixed_dictionaries({
            "mean": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
            "min": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
            "max": st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False),
        }),
        "memory_rss_mb": st.fixed_dictionaries({
            "mean": st.floats(min_value=0.0, max_value=65536.0, allow_nan=False, allow_infinity=False),
            "min": st.floats(min_value=0.0, max_value=65536.0, allow_nan=False, allow_infinity=False),
            "max": st.floats(min_value=0.0, max_value=65536.0, allow_nan=False, allow_infinity=False),
        }),
        "disk_read_mb": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        "disk_write_mb": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        "network_sent_mb": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
        "network_recv_mb": st.floats(min_value=0.0, max_value=10000.0, allow_nan=False, allow_infinity=False),
    })


# Property 15: Performance Metrics Collection
# For any pipeline run, the system should collect and store all required
# performance metrics including throughput, latency, error rates, CPU utilization,
# memory usage, disk I/O, network I/O, and cache hit rates for all cache levels


def test_property_15_manual_basic():
    """Property 15: Performance Metrics Collection - Basic Manual Test
    
    **Validates: Requirements 8.1, 8.2, 8.5, 8.6**
    
    For any pipeline run, all required metrics should be collected.
    """
    collector = MetricsCollector()
    
    # Start a run
    collector.start_run("test_run_001")
    
    # Record various metrics
    collector.record_products_processed(100)
    collector.record_error()
    collector.record_cache_hit("L1")
    collector.record_cache_miss("L1")
    collector.record_cache_hit("L2")
    collector.record_cache_miss("L2")
    collector.record_cache_hit("L3")
    collector.record_cache_miss("L3")
    
    # Update resource metrics
    resource_summary = {
        "cpu_percent": {"mean": 45.5, "min": 20.0, "max": 80.0},
        "memory_rss_mb": {"mean": 512.0, "min": 400.0, "max": 600.0},
        "disk_read_mb": 100.0,
        "disk_write_mb": 50.0,
        "network_sent_mb": 25.0,
        "network_recv_mb": 75.0,
    }
    collector.update_resource_metrics(resource_summary)
    
    # Set data completeness
    collector.set_data_completeness(85.5)
    
    # End the run
    metrics = collector.end_run()
    
    # Verify all required fields are present and valid
    assert metrics.run_id == "test_run_001"
    assert metrics.start_time is not None
    assert metrics.end_time is not None
    assert metrics.duration_seconds > 0
    
    # Throughput and latency
    assert metrics.products_processed == 100
    assert metrics.throughput_products_per_second > 0
    assert metrics.latency_per_product_ms > 0
    
    # Error metrics
    assert metrics.error_count == 1
    assert 0.0 <= metrics.error_rate <= 1.0
    
    # Resource utilization
    assert metrics.cpu_utilization_percent == 45.5
    assert metrics.memory_peak_mb == 600.0
    assert metrics.memory_avg_mb == 512.0
    assert metrics.disk_read_mb == 100.0
    assert metrics.disk_write_mb == 50.0
    assert metrics.network_sent_mb == 25.0
    assert metrics.network_recv_mb == 75.0
    
    # Cache hit rates
    assert metrics.cache_l1_hits == 1
    assert metrics.cache_l1_misses == 1
    assert metrics.cache_l2_hits == 1
    assert metrics.cache_l2_misses == 1
    assert metrics.cache_l3_hits == 1
    assert metrics.cache_l3_misses == 1
    assert 0.0 <= metrics.cache_l1_hit_rate <= 1.0
    assert 0.0 <= metrics.cache_l2_hit_rate <= 1.0
    assert 0.0 <= metrics.cache_l3_hit_rate <= 1.0
    
    # Data quality
    assert metrics.data_completeness_percent == 85.5
    
    print("✅ Property 15: Basic manual test PASSED")


def test_property_15_manual_multiple_runs():
    """Property 15: Multiple Runs - History Tracking
    
    **Validates: Requirements 8.1, 8.2**
    
    Collector should track multiple runs independently.
    """
    collector = MetricsCollector()
    
    # Run 1
    collector.start_run("run_001")
    collector.record_products_processed(50)
    metrics1 = collector.end_run()
    
    # Run 2
    collector.start_run("run_002")
    collector.record_products_processed(100)
    metrics2 = collector.end_run()
    
    # Verify history
    history = collector.get_run_history()
    assert len(history) == 2
    assert history[0].run_id == "run_001"
    assert history[0].products_processed == 50
    assert history[1].run_id == "run_002"
    assert history[1].products_processed == 100
    
    # Verify latest run
    latest = collector.get_latest_run()
    assert latest.run_id == "run_002"
    assert latest.products_processed == 100
    
    print("✅ Property 15: Multiple runs test PASSED")


def test_property_15_manual_cache_hit_rate_calculation():
    """Property 15: Cache Hit Rate Calculation
    
    **Validates: Requirements 8.5**
    
    Cache hit rates should be calculated correctly.
    """
    collector = MetricsCollector()
    
    collector.start_run("cache_test")
    
    # L1: 8 hits, 2 misses = 80% hit rate
    for _ in range(8):
        collector.record_cache_hit("L1")
    for _ in range(2):
        collector.record_cache_miss("L1")
    
    # L2: 6 hits, 4 misses = 60% hit rate
    for _ in range(6):
        collector.record_cache_hit("L2")
    for _ in range(4):
        collector.record_cache_miss("L2")
    
    # L3: 3 hits, 7 misses = 30% hit rate
    for _ in range(3):
        collector.record_cache_hit("L3")
    for _ in range(7):
        collector.record_cache_miss("L3")
    
    metrics = collector.end_run()
    
    # Verify hit rates
    assert abs(metrics.cache_l1_hit_rate - 0.8) < 0.01
    assert abs(metrics.cache_l2_hit_rate - 0.6) < 0.01
    assert abs(metrics.cache_l3_hit_rate - 0.3) < 0.01
    
    print("✅ Property 15: Cache hit rate calculation test PASSED")


def test_property_15_manual_throughput_latency_calculation():
    """Property 15: Throughput and Latency Calculation
    
    **Validates: Requirements 8.1**
    
    Throughput and latency should be calculated correctly.
    """
    collector = MetricsCollector()
    
    collector.start_run("throughput_test")
    
    # Simulate processing
    import time
    start = time.time()
    collector.record_products_processed(1000)
    time.sleep(0.1)  # Simulate 100ms of work
    
    metrics = collector.end_run()
    
    # Verify calculations
    assert metrics.products_processed == 1000
    assert metrics.duration_seconds >= 0.1
    assert metrics.throughput_products_per_second > 0
    assert metrics.latency_per_product_ms > 0
    
    # Verify relationship: throughput * latency ≈ 1000
    # (products/sec) * (ms/product) / 1000 ≈ 1
    relationship = (metrics.throughput_products_per_second * 
                   metrics.latency_per_product_ms / 1000.0)
    assert abs(relationship - 1.0) < 0.1
    
    print("✅ Property 15: Throughput/latency calculation test PASSED")


def test_property_15_manual_error_rate_calculation():
    """Property 15: Error Rate Calculation
    
    **Validates: Requirements 8.2**
    
    Error rate should be calculated correctly.
    """
    collector = MetricsCollector()
    
    collector.start_run("error_test")
    
    # 10 successful operations, 2 errors = 16.67% error rate
    collector.record_products_processed(10)
    collector.record_error()
    collector.record_error()
    
    metrics = collector.end_run()
    
    # Total operations = 10 + 2 = 12
    # Error rate = 2 / 12 = 0.1667
    assert metrics.error_count == 2
    expected_error_rate = 2.0 / 12.0
    assert abs(metrics.error_rate - expected_error_rate) < 0.01
    
    print("✅ Property 15: Error rate calculation test PASSED")


def test_property_15_manual_serialization():
    """Property 15: Metrics Serialization
    
    **Validates: Requirements 8.6**
    
    Metrics should be serializable to dictionary.
    """
    collector = MetricsCollector()
    
    collector.start_run("serialize_test")
    collector.record_products_processed(100)
    collector.record_cache_hit("L1")
    collector.set_data_completeness(75.0)
    
    metrics = collector.end_run()
    
    # Serialize to dict
    metrics_dict = metrics.to_dict()
    
    # Verify all required keys are present
    required_keys = [
        "run_id", "start_time", "end_time", "duration_seconds",
        "products_processed", "throughput_products_per_second",
        "latency_per_product_ms", "error_count", "error_rate",
        "cpu_utilization_percent", "memory_peak_mb", "memory_avg_mb",
        "disk_read_mb", "disk_write_mb", "network_sent_mb", "network_recv_mb",
        "cache_l1_hits", "cache_l1_misses", "cache_l2_hits", "cache_l2_misses",
        "cache_l3_hits", "cache_l3_misses", "cache_l1_hit_rate",
        "cache_l2_hit_rate", "cache_l3_hit_rate", "data_completeness_percent"
    ]
    
    for key in required_keys:
        assert key in metrics_dict, f"Missing key: {key}"
    
    # Verify types
    assert isinstance(metrics_dict["run_id"], str)
    assert isinstance(metrics_dict["products_processed"], int)
    assert isinstance(metrics_dict["duration_seconds"], (int, float))
    
    print("✅ Property 15: Serialization test PASSED")


# Hypothesis-based property tests (if available)
if HYPOTHESIS_AVAILABLE:
    @given(
        products_processed=st.integers(min_value=1, max_value=100000),
        error_count=st.integers(min_value=0, max_value=1000),
        l1_hits=st.integers(min_value=0, max_value=1000),
        l1_misses=st.integers(min_value=0, max_value=1000),
        l2_hits=st.integers(min_value=0, max_value=1000),
        l2_misses=st.integers(min_value=0, max_value=1000),
        l3_hits=st.integers(min_value=0, max_value=1000),
        l3_misses=st.integers(min_value=0, max_value=1000),
        resource_summary=resource_summary_strategy(),
        completeness=st.floats(min_value=0.0, max_value=100.0, allow_nan=False, allow_infinity=False)
    )
    @settings(max_examples=100, deadline=None)
    def test_property_15_hypothesis(
        products_processed, error_count, l1_hits, l1_misses,
        l2_hits, l2_misses, l3_hits, l3_misses,
        resource_summary, completeness
    ):
        """Property 15: Performance Metrics Collection (Hypothesis)
        
        **Validates: Requirements 8.1, 8.2, 8.5, 8.6**
        
        For any pipeline run, the system should collect and store all required
        performance metrics.
        """
        collector = MetricsCollector()
        
        # Start run
        collector.start_run(f"hypothesis_run_{products_processed}")
        
        # Record metrics
        collector.record_products_processed(products_processed)
        
        for _ in range(error_count):
            collector.record_error()
        
        for _ in range(l1_hits):
            collector.record_cache_hit("L1")
        for _ in range(l1_misses):
            collector.record_cache_miss("L1")
        
        for _ in range(l2_hits):
            collector.record_cache_hit("L2")
        for _ in range(l2_misses):
            collector.record_cache_miss("L2")
        
        for _ in range(l3_hits):
            collector.record_cache_hit("L3")
        for _ in range(l3_misses):
            collector.record_cache_miss("L3")
        
        collector.update_resource_metrics(resource_summary)
        collector.set_data_completeness(completeness)
        
        # End run
        metrics = collector.end_run()
        
        # Verify all required fields are present
        assert metrics.run_id is not None
        assert metrics.start_time is not None
        assert metrics.end_time is not None
        assert metrics.duration_seconds >= 0
        
        # Verify products processed
        assert metrics.products_processed == products_processed
        
        # Verify throughput and latency
        if metrics.duration_seconds > 0:
            assert metrics.throughput_products_per_second >= 0
            assert metrics.latency_per_product_ms >= 0
        
        # Verify error metrics
        assert metrics.error_count == error_count
        assert 0.0 <= metrics.error_rate <= 1.0
        
        # Verify resource metrics
        assert metrics.cpu_utilization_percent >= 0
        assert metrics.memory_peak_mb >= 0
        assert metrics.memory_avg_mb >= 0
        assert metrics.disk_read_mb >= 0
        assert metrics.disk_write_mb >= 0
        assert metrics.network_sent_mb >= 0
        assert metrics.network_recv_mb >= 0
        
        # Verify cache metrics
        assert metrics.cache_l1_hits == l1_hits
        assert metrics.cache_l1_misses == l1_misses
        assert metrics.cache_l2_hits == l2_hits
        assert metrics.cache_l2_misses == l2_misses
        assert metrics.cache_l3_hits == l3_hits
        assert metrics.cache_l3_misses == l3_misses
        
        # Verify cache hit rates
        assert 0.0 <= metrics.cache_l1_hit_rate <= 1.0
        assert 0.0 <= metrics.cache_l2_hit_rate <= 1.0
        assert 0.0 <= metrics.cache_l3_hit_rate <= 1.0
        
        # Verify cache hit rate calculations
        l1_total = l1_hits + l1_misses
        if l1_total > 0:
            expected_l1_rate = l1_hits / l1_total
            assert abs(metrics.cache_l1_hit_rate - expected_l1_rate) < 0.001
        
        l2_total = l2_hits + l2_misses
        if l2_total > 0:
            expected_l2_rate = l2_hits / l2_total
            assert abs(metrics.cache_l2_hit_rate - expected_l2_rate) < 0.001
        
        l3_total = l3_hits + l3_misses
        if l3_total > 0:
            expected_l3_rate = l3_hits / l3_total
            assert abs(metrics.cache_l3_hit_rate - expected_l3_rate) < 0.001
        
        # Verify data completeness
        assert metrics.data_completeness_percent == completeness
        
        # Verify serialization works
        metrics_dict = metrics.to_dict()
        assert isinstance(metrics_dict, dict)
        assert len(metrics_dict) > 0


if __name__ == "__main__":
    print("Running Property 15: Performance Metrics Collection tests...\n")
    
    test_property_15_manual_basic()
    test_property_15_manual_multiple_runs()
    test_property_15_manual_cache_hit_rate_calculation()
    test_property_15_manual_throughput_latency_calculation()
    test_property_15_manual_error_rate_calculation()
    test_property_15_manual_serialization()
    
    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_15_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")
    
    print("\n✅ All Property 15 tests passed!")
