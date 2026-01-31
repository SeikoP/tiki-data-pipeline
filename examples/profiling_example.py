"""Example demonstrating the profiling infrastructure.

This script shows how to use the profiling system to identify bottlenecks
in the Tiki data pipeline.
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.profiling import (
    FunctionProfiler,
    ComponentProfiler,
    ResourceMonitor,
    BottleneckAnalyzer,
    profile,
)


# ============================================================================
# Example 1: Function-Level Profiling
# ============================================================================

def example_function_profiling():
    """Demonstrate function-level profiling."""
    print("=" * 80)
    print("EXAMPLE 1: Function-Level Profiling")
    print("=" * 80)
    print()
    
    profiler = FunctionProfiler()
    
    # Profile some functions
    @profiler.profile
    def fast_function():
        time.sleep(0.01)
        return "fast"
    
    @profiler.profile
    def slow_function():
        time.sleep(0.1)
        return "slow"
    
    @profiler.profile
    def medium_function():
        time.sleep(0.05)
        return "medium"
    
    # Call functions multiple times
    print("Calling functions...")
    for _ in range(10):
        fast_function()
        slow_function()
        medium_function()
    
    # Generate report
    print("\n" + profiler.generate_report())


# ============================================================================
# Example 2: Component-Level Profiling
# ============================================================================

def example_component_profiling():
    """Demonstrate component-level profiling."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Component-Level Profiling")
    print("=" * 80)
    print()
    
    profiler = ComponentProfiler()
    
    # Simulate Crawler component
    print("Profiling Crawler component...")
    profiler.start_component("Crawler")
    time.sleep(0.2)  # Simulate crawling
    profiler.sample_memory("Crawler")
    crawler_stats = profiler.end_component("Crawler", items_processed=100)
    
    # Simulate Transformer component
    print("Profiling Transformer component...")
    profiler.start_component("Transformer")
    time.sleep(0.15)  # Simulate transformation
    profiler.sample_memory("Transformer")
    transformer_stats = profiler.end_component("Transformer", items_processed=100)
    
    # Simulate Loader component
    print("Profiling Loader component...")
    profiler.start_component("Loader")
    time.sleep(0.1)  # Simulate loading
    profiler.sample_memory("Loader")
    loader_stats = profiler.end_component("Loader", items_processed=100)
    
    # Generate report
    print("\n" + profiler.generate_report())


# ============================================================================
# Example 3: Resource Monitoring
# ============================================================================

def example_resource_monitoring():
    """Demonstrate resource monitoring."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Resource Monitoring")
    print("=" * 80)
    print()
    
    try:
        monitor = ResourceMonitor()
        
        # Start monitoring
        print("Starting resource monitoring...")
        monitor.start_monitoring(interval_seconds=0.5)
        
        # Simulate some work
        print("Simulating work...")
        for i in range(5):
            # Allocate some memory
            data = [0] * (1000000 * (i + 1))
            time.sleep(0.5)
            del data
        
        # Stop monitoring
        print("Stopping resource monitoring...")
        monitor.stop_monitoring()
        
        # Generate report
        print("\n" + monitor.generate_report())
        
    except ImportError as e:
        print(f"⚠️  Resource monitoring requires psutil: {e}")
        print("Install with: pip install psutil")


# ============================================================================
# Example 4: Bottleneck Analysis
# ============================================================================

def example_bottleneck_analysis():
    """Demonstrate bottleneck analysis."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Bottleneck Analysis")
    print("=" * 80)
    print()
    
    # Create profilers
    func_profiler = FunctionProfiler()
    comp_profiler = ComponentProfiler()
    
    # Profile some functions
    @func_profiler.profile
    def cpu_intensive():
        # Simulate CPU-intensive work
        total = 0
        for i in range(1000000):
            total += i
        return total
    
    @func_profiler.profile
    def io_intensive():
        # Simulate I/O-intensive work
        time.sleep(0.1)
        return "io"
    
    # Call functions
    print("Profiling functions...")
    for _ in range(5):
        cpu_intensive()
        io_intensive()
    
    # Profile components
    print("Profiling components...")
    
    comp_profiler.start_component("Crawler")
    time.sleep(0.3)
    comp_profiler.end_component("Crawler", items_processed=100)
    
    comp_profiler.start_component("Transformer")
    for _ in range(10):
        cpu_intensive()
    comp_profiler.end_component("Transformer", items_processed=100)
    
    comp_profiler.start_component("Loader")
    time.sleep(0.2)
    comp_profiler.end_component("Loader", items_processed=100)
    
    # Analyze bottlenecks
    print("\nAnalyzing bottlenecks...")
    analyzer = BottleneckAnalyzer()
    bottlenecks = analyzer.analyze(
        function_stats=func_profiler.get_stats(),
        component_stats=comp_profiler.get_stats(),
    )
    
    # Generate report
    print("\n" + analyzer.generate_report(bottlenecks))


# ============================================================================
# Example 5: Using Global Profiler with Decorator
# ============================================================================

def example_global_profiler():
    """Demonstrate using the global profiler with decorator."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Global Profiler with Decorator")
    print("=" * 80)
    print()
    
    # Use the convenient @profile decorator
    @profile
    def my_function(n):
        time.sleep(0.01 * n)
        return n * 2
    
    # Call function multiple times
    print("Calling profiled function...")
    for i in range(1, 6):
        result = my_function(i)
        print(f"  my_function({i}) = {result}")
    
    # Get stats from global profiler
    from common.profiling.function_profiler import get_global_profiler
    
    profiler = get_global_profiler()
    print("\n" + profiler.generate_report())


# ============================================================================
# Main
# ============================================================================

def main():
    """Run all examples."""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "PROFILING INFRASTRUCTURE EXAMPLES" + " " * 25 + "║")
    print("╚" + "=" * 78 + "╝")
    print()
    
    # Run examples
    example_function_profiling()
    example_component_profiling()
    example_resource_monitoring()
    example_bottleneck_analysis()
    example_global_profiler()
    
    print("\n" + "=" * 80)
    print("✅ All examples completed!")
    print("=" * 80)
    print()
    print("Next steps:")
    print("1. Integrate profiling into your pipeline code")
    print("2. Run the pipeline with profiling enabled")
    print("3. Analyze the bottleneck report")
    print("4. Prioritize optimizations based on HIGH severity bottlenecks")
    print()


if __name__ == "__main__":
    main()
