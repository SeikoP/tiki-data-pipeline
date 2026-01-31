"""Run baseline profiling on the Tiki data pipeline.

This script runs the pipeline with profiling enabled to establish baseline
performance metrics before optimization. It generates comprehensive reports
including:
- Function-level execution times
- Component-level statistics (Crawler, Transformer, Loader)
- System resource utilization
- Bottleneck analysis with recommendations

Usage:
    python scripts/run_baseline_profiling.py [--max-categories N] [--max-pages N]
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.profiling import (
    ProfiledPipelineRunner,
    ProfilingReportGenerator,
    get_global_component_profiler,
    get_global_profiler,
    get_global_resource_monitor,
)
from pipelines.crawl.crawl_products import crawl_products_from_categories


def run_baseline_profiling(
    categories_file: str,
    output_dir: Path,
    max_categories: int = 5,
    max_pages_per_category: int = 2,
    max_workers: int = 5,
    use_selenium: bool = False,
):
    """Run pipeline with profiling enabled.

    Args:
        categories_file: Path to categories JSON file
        output_dir: Directory to save profiling reports
        max_categories: Maximum number of categories to crawl
        max_pages_per_category: Maximum pages per category
        max_workers: Number of worker threads
        use_selenium: Whether to use Selenium for enrichment
    """
    print("=" * 100)
    print("BASELINE PROFILING - TIKI DATA PIPELINE")
    print("=" * 100)
    print()
    print("Configuration:")
    print(f"  📁 Categories file: {categories_file}")
    print(f"  📁 Output directory: {output_dir}")
    print(f"  📊 Max categories: {max_categories}")
    print(f"  📄 Max pages per category: {max_pages_per_category}")
    print(f"  ⚙️  Worker threads: {max_workers}")
    print(f"  🔧 Use Selenium: {use_selenium}")
    print()

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create profiled pipeline runner
    runner = ProfiledPipelineRunner(monitoring_interval=1.0)

    # Start monitoring
    print("▶️  Starting resource monitoring...")
    runner.start_monitoring()
    print()

    # Record start time
    start_time = datetime.now()
    print(f"⏱️  Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # Run pipeline with profiling
        print("▶️  Running pipeline with profiling enabled...")
        print("-" * 100)

        products_output = output_dir / "baseline_products.json"

        crawl_products_from_categories(
            categories_file=categories_file,
            output_file=str(products_output),
            max_categories=max_categories,
            max_pages_per_category=max_pages_per_category,
            max_workers=max_workers,
            use_selenium=use_selenium,
        )

        print("-" * 100)
        print()

    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    finally:
        # Stop monitoring
        print("⏹️  Stopping resource monitoring...")
        runner.stop_monitoring()
        print()

    # Record end time
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"⏱️  Pipeline completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Total duration: {duration:.2f} seconds")
    print()

    # Generate comprehensive report
    print("📊 Generating profiling reports...")
    print()

    # Text report with timeout protection
    try:
        report_file = (
            output_dir / f"baseline_profiling_report_{start_time.strftime('%Y%m%d_%H%M%S')}.txt"
        )
        print(f"  Generating text report to: {report_file}")
        report = runner.generate_report(output_file=str(report_file))
        print(report)
        print()
        print(f"✅ Text report saved to: {report_file}")
        print()
    except Exception as e:
        print(f"⚠️  Could not generate text report: {e}")
        import traceback

        traceback.print_exc()
        print()

    # JSON data export with timeout protection
    try:
        print("  Generating JSON export...")
        generator = ProfilingReportGenerator(
            function_profiler=get_global_profiler(),
            component_profiler=get_global_component_profiler(),
            resource_monitor=get_global_resource_monitor(),
        )

        json_file = (
            output_dir / f"baseline_profiling_data_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        )
        generator.export_json(str(json_file))
        print(f"✅ JSON data exported to: {json_file}")
        print()
    except Exception as e:
        print(f"⚠️  Could not export JSON data: {e}")
        import traceback

        traceback.print_exc()
        print()

    # Generate baseline metrics summary
    try:
        summary = generate_baseline_summary(
            runner=runner,
            duration=duration,
            max_categories=max_categories,
            max_pages_per_category=max_pages_per_category,
        )

        summary_file = (
            output_dir / f"baseline_metrics_summary_{start_time.strftime('%Y%m%d_%H%M%S')}.json"
        )
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print("📊 BASELINE METRICS SUMMARY")
        print("=" * 100)
        print(json.dumps(summary, indent=2))
        print()
        print(f"✅ Summary saved to: {summary_file}")
        print()
    except Exception as e:
        print(f"⚠️  Could not generate baseline summary: {e}")
        print()

    print("=" * 100)
    print("✅ BASELINE PROFILING COMPLETED")
    print("=" * 100)
    print()
    print("Next steps:")
    print("1. Review the profiling report to identify bottlenecks")
    print("2. Prioritize optimizations based on HIGH severity bottlenecks")
    print("3. Implement targeted optimizations")
    print("4. Re-run profiling to measure improvements")
    print()

    return True


def generate_baseline_summary(
    runner: ProfiledPipelineRunner,
    duration: float,
    max_categories: int,
    max_pages_per_category: int,
) -> dict:
    """Generate baseline metrics summary.

    Args:
        runner: Profiled pipeline runner
        duration: Total pipeline duration in seconds
        max_categories: Number of categories crawled
        max_pages_per_category: Pages per category

    Returns:
        Dictionary with baseline metrics
    """
    function_profiler = get_global_profiler()
    component_profiler = get_global_component_profiler()
    resource_monitor = get_global_resource_monitor()

    # Get statistics
    function_stats = function_profiler.get_stats()
    component_stats = component_profiler.get_stats()
    resource_snapshots = resource_monitor.get_snapshots() if resource_monitor else []

    # Calculate throughput (items per second)
    total_items = sum(stats.items_processed for stats in component_stats.values())
    throughput = total_items / duration if duration > 0 else 0

    # Calculate average latency (seconds per item)
    latency = duration / total_items if total_items > 0 else 0

    # Get resource utilization
    avg_cpu = 0
    avg_memory_mb = 0
    peak_memory_mb = 0

    if resource_snapshots:
        avg_cpu = sum(s.cpu_percent for s in resource_snapshots) / len(resource_snapshots)
        avg_memory_mb = sum(s.memory_rss_mb for s in resource_snapshots) / len(resource_snapshots)
        peak_memory_mb = max(s.memory_rss_mb for s in resource_snapshots)

    # Component breakdown
    component_breakdown = {}
    for name, stats in component_stats.items():
        component_breakdown[name] = {
            "execution_time_ms": stats.execution_time_ms,
            "execution_time_seconds": stats.execution_time_ms / 1000,
            "percentage_of_total": (stats.execution_time_ms / (duration * 1000)) * 100
            if duration > 0
            else 0,
            "items_processed": stats.items_processed,
            "throughput_items_per_sec": stats.throughput_items_per_sec,
            "memory_peak_mb": stats.memory_peak_mb,
            "memory_avg_mb": stats.memory_avg_mb,
        }

    # Top 10 slowest functions
    sorted_functions = sorted(
        function_stats.items(), key=lambda x: x[1].total_time_ms, reverse=True
    )[:10]

    slowest_functions = []
    for func_name, stats in sorted_functions:
        slowest_functions.append(
            {
                "function": func_name,
                "total_time_ms": stats.total_time_ms,
                "call_count": stats.call_count,
                "mean_time_ms": stats.mean_time_ms,
                "p95_time_ms": stats.p95_time_ms,
                "p99_time_ms": stats.p99_time_ms,
            }
        )

    return {
        "timestamp": datetime.now().isoformat(),
        "configuration": {
            "max_categories": max_categories,
            "max_pages_per_category": max_pages_per_category,
        },
        "performance_metrics": {
            "total_duration_seconds": duration,
            "total_items_processed": total_items,
            "throughput_items_per_second": throughput,
            "latency_seconds_per_item": latency,
        },
        "resource_utilization": {
            "avg_cpu_percent": avg_cpu,
            "avg_memory_mb": avg_memory_mb,
            "peak_memory_mb": peak_memory_mb,
        },
        "component_breakdown": component_breakdown,
        "slowest_functions": slowest_functions,
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run baseline profiling on Tiki data pipeline")
    parser.add_argument(
        "--categories-file",
        default="data/raw/categories_recursive_optimized.json",
        help="Path to categories JSON file",
    )
    parser.add_argument(
        "--output-dir",
        default="data/profiling/baseline",
        help="Directory to save profiling reports",
    )
    parser.add_argument(
        "--max-categories",
        type=int,
        default=5,
        help="Maximum number of categories to crawl (default: 5)",
    )
    parser.add_argument(
        "--max-pages", type=int, default=2, help="Maximum pages per category (default: 2)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=5, help="Number of worker threads (default: 5)"
    )
    parser.add_argument(
        "--use-selenium",
        action="store_true",
        help="Use Selenium for enrichment (slower but more complete)",
    )

    args = parser.parse_args()

    # Convert output_dir to Path
    output_dir = Path(args.output_dir)

    # Run baseline profiling
    success = run_baseline_profiling(
        categories_file=args.categories_file,
        output_dir=output_dir,
        max_categories=args.max_categories,
        max_pages_per_category=args.max_pages,
        max_workers=args.max_workers,
        use_selenium=args.use_selenium,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
