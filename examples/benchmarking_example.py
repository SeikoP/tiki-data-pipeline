"""Example usage of the benchmarking infrastructure.

This script demonstrates how to use the benchmarking system to:
1. Generate standard workloads
2. Capture baseline metrics
3. Validate reproducibility
4. Run comprehensive benchmark suites
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.benchmarking import get_global_benchmarking_system
from common.metrics_collector import MetricsCollector, PipelineRunMetrics
from common.benchmark_workload import BenchmarkWorkload


def simulate_pipeline_run(workload: BenchmarkWorkload) -> PipelineRunMetrics:
    """Simulate a pipeline run for demonstration purposes.
    
    In a real scenario, this would execute the actual pipeline with the workload.
    
    Args:
        workload: Benchmark workload to process
    
    Returns:
        PipelineRunMetrics from the simulated run
    """
    collector = MetricsCollector()
    
    # Start run
    run_id = f"run_{int(time.time())}"
    collector.start_run(run_id)
    
    # Simulate processing
    num_products = len(workload.product_ids)
    
    # Simulate some work
    time.sleep(0.1 * (num_products / 100))  # Scale with workload size
    
    # Record metrics
    collector.record_products_processed(num_products)
    
    # Simulate some cache hits
    for _ in range(num_products // 2):
        collector.record_cache_hit("L1")
    for _ in range(num_products // 4):
        collector.record_cache_miss("L1")
        collector.record_cache_hit("L2")
    
    # Simulate a few errors
    for _ in range(num_products // 100):
        collector.record_error()
    
    # Set data completeness
    collector.set_data_completeness(85.0)
    
    # End run and get metrics
    metrics = collector.end_run()
    
    return metrics


def example_1_generate_workloads():
    """Example 1: Generate standard workloads."""
    print("=" * 80)
    print("EXAMPLE 1: Generate Standard Workloads")
    print("=" * 80)
    print()
    
    system = get_global_benchmarking_system()
    
    # Generate standard workloads (small, medium, large)
    workloads = system.generate_standard_workloads(seed=42)
    
    print(f"Generated {len(workloads)} workloads:")
    for size_name, workload in workloads.items():
        print(f"  - {size_name}: {len(workload.product_ids)} products")
        print(f"    Workload ID: {workload.config.workload_id}")
        print(f"    Checksum: {workload.checksum[:16]}...")
        print(f"    Verified: {workload.verify_checksum()}")
    
    print()
    return workloads


def example_2_capture_baseline(workloads):
    """Example 2: Capture baseline metrics."""
    print("=" * 80)
    print("EXAMPLE 2: Capture Baseline Metrics")
    print("=" * 80)
    print()
    
    system = get_global_benchmarking_system()
    
    # Run baseline benchmark with small workload
    small_workload = workloads["small"]
    
    print(f"Running baseline benchmark with {len(small_workload.product_ids)} products...")
    
    baseline = system.run_baseline_benchmark(
        benchmark_id="example_baseline_001",
        workload=small_workload,
        pipeline_func=simulate_pipeline_run,
        description="Example baseline with small workload"
    )
    
    print()
    print("Baseline captured:")
    print(f"  Baseline ID: {baseline.baseline_id}")
    print(f"  Throughput: {baseline.metrics.throughput_products_per_second:.2f} products/sec")
    print(f"  Latency: {baseline.metrics.latency_per_product_ms:.2f} ms/product")
    print(f"  CPU: {baseline.metrics.cpu_utilization_percent:.1f}%")
    print(f"  Memory: {baseline.metrics.memory_peak_mb:.1f} MB")
    print(f"  Data Completeness: {baseline.metrics.data_completeness_percent:.1f}%")
    
    print()
    return baseline


def example_3_validate_reproducibility(workloads):
    """Example 3: Validate benchmark reproducibility."""
    print("=" * 80)
    print("EXAMPLE 3: Validate Reproducibility")
    print("=" * 80)
    print()
    
    system = get_global_benchmarking_system()
    
    # Validate reproducibility with small workload
    small_workload = workloads["small"]
    
    print(f"Running reproducibility validation with {len(small_workload.product_ids)} products...")
    print("Executing 5 runs...")
    
    report = system.validate_reproducibility(
        benchmark_id="example_reproducibility_001",
        workload=small_workload,
        pipeline_func=simulate_pipeline_run,
        num_runs=5
    )
    
    print()
    print(report.summary)
    
    print()
    return report


def example_4_run_benchmark_suite(workloads):
    """Example 4: Run comprehensive benchmark suite."""
    print("=" * 80)
    print("EXAMPLE 4: Run Benchmark Suite")
    print("=" * 80)
    print()
    
    system = get_global_benchmarking_system()
    
    print("Running benchmark suite across all workload sizes...")
    print()
    
    suite = system.run_benchmark_suite(
        suite_id="example_suite_001",
        workloads=workloads,
        pipeline_func=simulate_pipeline_run,
        description="Example benchmark suite with all workload sizes",
        capture_baseline=True,
        validate_reproducibility=True,
        reproducibility_runs=3
    )
    
    print()
    print(system.generate_suite_report(suite))
    
    print()
    return suite


def main():
    """Run all examples."""
    print()
    print("BENCHMARKING INFRASTRUCTURE EXAMPLES")
    print()
    
    # Example 1: Generate workloads
    workloads = example_1_generate_workloads()
    
    # Example 2: Capture baseline
    baseline = example_2_capture_baseline(workloads)
    
    # Example 3: Validate reproducibility
    report = example_3_validate_reproducibility(workloads)
    
    # Example 4: Run benchmark suite
    suite = example_4_run_benchmark_suite(workloads)
    
    print()
    print("=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)
    print()
    print("Generated files:")
    print("  - Workloads: data/profiling/workloads/")
    print("  - Baselines: data/profiling/baselines/")
    print("  - Suites: data/profiling/suites/")
    print()


if __name__ == "__main__":
    main()
