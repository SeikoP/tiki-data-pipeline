"""Pipeline runner with integrated profiling and resource monitoring.

This module provides a wrapper for running the entire pipeline with
profiling, resource monitoring, and report generation.
"""

import logging
from collections.abc import Callable
from typing import Any

from .bottleneck_analyzer import get_global_analyzer
from .component_profiler import get_global_component_profiler
from .function_profiler import get_global_profiler
from .resource_monitor import get_global_resource_monitor

logger = logging.getLogger(__name__)


class ProfiledPipelineRunner:
    """Runner that executes pipeline with profiling and monitoring.

    Example:
        >>> runner = ProfiledPipelineRunner()
        >>> runner.start_monitoring()
        >>>
        >>> # Run pipeline stages
        >>> products = crawl_products()
        >>> transformed = transform_products(products)
        >>> load_products(transformed)
        >>>
        >>> runner.stop_monitoring()
        >>> report = runner.generate_report()
        >>> print(report)
    """

    def __init__(self, monitoring_interval: float = 1.0):
        """Initialize the profiled pipeline runner.

        Args:
            monitoring_interval: Resource sampling interval in seconds
        """
        self.monitoring_interval = monitoring_interval
        self.resource_monitor = None
        self.monitoring_active = False

    def start_monitoring(self) -> None:
        """Start resource monitoring at pipeline start."""
        try:
            self.resource_monitor = get_global_resource_monitor()
            self.resource_monitor.start_monitoring(interval_seconds=self.monitoring_interval)
            self.monitoring_active = True
            logger.info("✅ Resource monitoring started")
        except Exception as e:
            logger.warning(f"⚠️  Could not start resource monitoring: {e}")
            self.monitoring_active = False

    def stop_monitoring(self) -> None:
        """Stop resource monitoring at pipeline end."""
        if self.monitoring_active and self.resource_monitor:
            try:
                self.resource_monitor.stop_monitoring()
                logger.info("✅ Resource monitoring stopped")
            except Exception as e:
                logger.warning(f"⚠️  Error stopping resource monitoring: {e}")

    def generate_report(self, output_file: str | None = None) -> str:
        """Generate comprehensive profiling report.

        Args:
            output_file: Optional file path to save the report

        Returns:
            Formatted profiling report string
        """
        print("DEBUG: Starting generate_report()")
        lines = []
        lines.append("=" * 100)
        lines.append("PIPELINE PROFILING REPORT")
        lines.append("=" * 100)
        lines.append("")

        # Function-level profiling
        print("DEBUG: Generating function profiling report...")
        try:
            function_profiler = get_global_profiler()
            lines.append(function_profiler.generate_report())
            lines.append("")
            print("DEBUG: Function profiling report done")
        except Exception as e:
            logger.warning(f"⚠️  Could not generate function profiling report: {e}")
            print(f"DEBUG: Function profiling report failed: {e}")

        # Component-level profiling
        print("DEBUG: Generating component profiling report...")
        try:
            component_profiler = get_global_component_profiler()
            lines.append(component_profiler.generate_report())
            lines.append("")
            print("DEBUG: Component profiling report done")
        except Exception as e:
            logger.warning(f"⚠️  Could not generate component profiling report: {e}")
            print(f"DEBUG: Component profiling report failed: {e}")

        # Resource monitoring
        print(f"DEBUG: Generating resource report (monitoring_active={self.monitoring_active})...")
        if self.monitoring_active and self.resource_monitor:
            try:
                lines.append(self.resource_monitor.generate_report())
                lines.append("")
                print("DEBUG: Resource report done")
            except Exception as e:
                logger.warning(f"⚠️  Could not generate resource monitoring report: {e}")
                print(f"DEBUG: Resource report failed: {e}")
        else:
            print("DEBUG: Skipping resource report (not active or no monitor)")

        # Bottleneck analysis
        print("DEBUG: Starting bottleneck analysis...")
        try:
            print("DEBUG: Getting analyzer...")
            analyzer = get_global_analyzer()
            print("DEBUG: Getting function stats...")
            function_stats = get_global_profiler().get_stats()
            print("DEBUG: Getting component stats...")
            component_stats = get_global_component_profiler().get_stats()

            # Get resource snapshots safely (avoid deadlock)
            print("DEBUG: Getting resource snapshots...")
            resource_snapshots = None
            if self.resource_monitor and not self.monitoring_active:
                try:
                    resource_snapshots = self.resource_monitor.get_snapshots()
                    print(
                        f"DEBUG: Got {len(resource_snapshots) if resource_snapshots else 0} snapshots"
                    )
                except Exception as snap_error:
                    logger.warning(f"⚠️  Could not get resource snapshots: {snap_error}")
                    print(f"DEBUG: Failed to get snapshots: {snap_error}")
            else:
                print("DEBUG: Skipping snapshots (monitor active or no monitor)")

            print("DEBUG: Analyzing bottlenecks...")
            bottlenecks = analyzer.analyze(
                function_stats=function_stats,
                component_stats=component_stats,
                resource_snapshots=resource_snapshots,
            )
            print(f"DEBUG: Found {len(bottlenecks)} bottlenecks")

            print("DEBUG: Generating bottleneck report...")
            lines.append(analyzer.generate_report(bottlenecks))
            lines.append("")
            print("DEBUG: Bottleneck analysis done")
        except Exception as e:
            logger.warning(f"⚠️  Could not generate bottleneck analysis: {e}")
            print(f"DEBUG: Bottleneck analysis failed: {e}")
            import traceback

            logger.warning(traceback.format_exc())
            traceback.print_exc()

        print("DEBUG: Joining report lines...")
        report = "\n".join(lines)
        print("DEBUG: Report joined")

        # Save to file if requested
        if output_file:
            print(f"DEBUG: Saving report to {output_file}...")
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(report)
                logger.info(f"✅ Profiling report saved to: {output_file}")
                print("DEBUG: Report saved successfully")
            except Exception as e:
                logger.error(f"❌ Could not save report to file: {e}")
                print(f"DEBUG: Failed to save report: {e}")

        print("DEBUG: generate_report() completed")
        return report

    def reset_profiling(self) -> None:
        """Reset all profiling data."""
        try:
            get_global_profiler().reset()
            get_global_component_profiler().reset()
            if self.resource_monitor:
                self.resource_monitor.reset()
            logger.info("✅ Profiling data reset")
        except Exception as e:
            logger.warning(f"⚠️  Error resetting profiling data: {e}")


def run_with_profiling(
    pipeline_func: Callable,
    *args,
    monitoring_interval: float = 1.0,
    report_file: str | None = None,
    **kwargs,
) -> Any:
    """Run a pipeline function with profiling and monitoring.

    Args:
        pipeline_func: Pipeline function to execute
        *args: Positional arguments for pipeline_func
        monitoring_interval: Resource sampling interval in seconds
        report_file: Optional file path to save profiling report
        **kwargs: Keyword arguments for pipeline_func

    Returns:
        Result from pipeline_func

    Example:
        >>> def my_pipeline():
        >>>     products = crawl_products()
        >>>     transformed = transform_products(products)
        >>>     load_products(transformed)
        >>>     return transformed
        >>>
        >>> result = run_with_profiling(
        >>>     my_pipeline,
        >>>     report_file="profiling_report.txt"
        >>> )
    """
    runner = ProfiledPipelineRunner(monitoring_interval=monitoring_interval)

    try:
        # Start monitoring
        runner.start_monitoring()

        # Run pipeline
        result = pipeline_func(*args, **kwargs)

        # Stop monitoring
        runner.stop_monitoring()

        # Generate report
        report = runner.generate_report(output_file=report_file)
        logger.info("\n" + report)

        return result

    except Exception as e:
        # Ensure monitoring is stopped even on error
        runner.stop_monitoring()
        raise e


# Global runner instance for convenience
_global_runner: ProfiledPipelineRunner | None = None


def get_global_runner() -> ProfiledPipelineRunner:
    """Get the global pipeline runner instance."""
    global _global_runner
    if _global_runner is None:
        _global_runner = ProfiledPipelineRunner()
    return _global_runner
