"""Example demonstrating profiling integration in the pipeline.

This example shows how to use the integrated profiling system to:
1. Profile critical functions (API calls, cache operations)
2. Profile pipeline components (Crawler, Transformer, Loader)
3. Monitor system resources
4. Generate comprehensive profiling reports
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.profiling import (
    ProfiledPipelineRunner,
    ProfilingReportGenerator,
    get_global_profiler,
    get_global_component_profiler,
    get_global_resource_monitor,
)


def simulate_crawler():
    """Simulate crawler operation."""
    print("🔍 Crawling products...")
    time.sleep(0.5)  # Simulate network I/O
    return [{"product_id": f"p{i}", "name": f"Product {i}"} for i in range(100)]


def simulate_transformer(products):
    """Simulate transformer operation."""
    print("🔄 Transforming products...")
    time.sleep(0.3)  # Simulate CPU work
    return [(p, {"transformed": True}) for p in products]


def simulate_loader(transformed_products):
    """Simulate loader operation."""
    print("💾 Loading products to database...")
    time.sleep(0.2)  # Simulate database I/O
    return {"loaded_count": len(transformed_products)}


def run_pipeline():
    """Run the complete pipeline."""
    # Crawl
    products = simulate_crawler()
    
    # Transform
    transformed = simulate_transformer(products)
    
    # Load
    result = simulate_loader(transformed)
    
    return result


def main():
    """Main function demonstrating profiling integration."""
    print("=" * 80)
    print("PROFILING INTEGRATION EXAMPLE")
    print("=" * 80)
    print()
    
    # Create profiled pipeline runner
    runner = ProfiledPipelineRunner(monitoring_interval=0.5)
    
    # Start monitoring
    print("▶️  Starting resource monitoring...")
    runner.start_monitoring()
    print()
    
    # Run pipeline
    print("▶️  Running pipeline...")
    result = run_pipeline()
    print(f"✅ Pipeline completed: {result}")
    print()
    
    # Stop monitoring
    print("⏹️  Stopping resource monitoring...")
    runner.stop_monitoring()
    print()
    
    # Generate comprehensive report
    print("📊 Generating profiling report...")
    report = runner.generate_report()
    print()
    print(report)
    
    # Save report to file
    output_file = Path(__file__).parent / "profiling_report.txt"
    runner.generate_report(output_file=str(output_file))
    print(f"✅ Report saved to: {output_file}")
    print()
    
    # Export JSON data
    print("📊 Exporting profiling data as JSON...")
    generator = ProfilingReportGenerator(
        function_profiler=get_global_profiler(),
        component_profiler=get_global_component_profiler(),
        resource_monitor=get_global_resource_monitor(),
    )
    
    json_file = Path(__file__).parent / "profiling_data.json"
    generator.export_json(str(json_file))
    print(f"✅ JSON data exported to: {json_file}")
    print()
    
    print("=" * 80)
    print("EXAMPLE COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    main()

