"""Profiling report generator for creating comprehensive performance reports.

This module provides utilities for generating detailed profiling reports
including top slowest operations, bottleneck analysis, and recommendations.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from .bottleneck_analyzer import BottleneckAnalyzer
from .component_profiler import ComponentProfiler
from .function_profiler import FunctionProfiler
from .resource_monitor import ResourceMonitor

logger = logging.getLogger(__name__)


class ProfilingReportGenerator:
    """Generator for comprehensive profiling reports.

    Creates reports with:
    - Top 10 slowest operations
    - Execution times and call counts
    - Bottleneck analysis with recommendations
    - Resource usage summary
    - Component performance breakdown
    """

    def __init__(
        self,
        function_profiler: FunctionProfiler | None = None,
        component_profiler: ComponentProfiler | None = None,
        resource_monitor: ResourceMonitor | None = None,
        bottleneck_analyzer: BottleneckAnalyzer | None = None,
    ):
        """Initialize the report generator.

        Args:
            function_profiler: Function-level profiler instance
            component_profiler: Component-level profiler instance
            resource_monitor: Resource monitor instance
            bottleneck_analyzer: Bottleneck analyzer instance
        """
        self.function_profiler = function_profiler
        self.component_profiler = component_profiler
        self.resource_monitor = resource_monitor
        self.bottleneck_analyzer = bottleneck_analyzer or BottleneckAnalyzer()

    def generate_top_slowest_report(self, n: int = 10) -> str:
        """Generate report of top N slowest operations.

        Args:
            n: Number of slowest operations to include

        Returns:
            Formatted report string
        """
        if not self.function_profiler:
            return "No function profiling data available."

        lines = []
        lines.append("=" * 100)
        lines.append(f"TOP {n} SLOWEST OPERATIONS")
        lines.append("=" * 100)
        lines.append("")

        top_slowest = self.function_profiler.get_top_slowest(n)

        if not top_slowest:
            lines.append("No profiling data collected.")
            return "\n".join(lines)

        lines.append(
            f"{'Function':<60} {'Calls':>8} {'Total (ms)':>12} {'Mean (ms)':>12} {'P95 (ms)':>12}"
        )
        lines.append("-" * 100)

        for stats in top_slowest:
            # Truncate long function names
            func_name = stats.function_name
            if len(func_name) > 60:
                func_name = "..." + func_name[-57:]

            lines.append(
                f"{func_name:<60} {stats.call_count:>8} "
                f"{stats.total_time_ms:>12.2f} {stats.mean_time_ms:>12.2f} "
                f"{stats.p95_time_ms:>12.2f}"
            )

        lines.append("")
        lines.append("=" * 100)

        return "\n".join(lines)

    def generate_bottleneck_report(self) -> str:
        """Generate bottleneck analysis report with recommendations.

        Returns:
            Formatted report string
        """
        if not self.bottleneck_analyzer:
            return "No bottleneck analyzer available."

        # Collect profiling data
        function_stats = self.function_profiler.get_stats() if self.function_profiler else None
        component_stats = self.component_profiler.get_stats() if self.component_profiler else None
        resource_snapshots = (
            self.resource_monitor.get_snapshots() if self.resource_monitor else None
        )

        # Analyze bottlenecks
        bottlenecks = self.bottleneck_analyzer.analyze(
            function_stats=function_stats,
            component_stats=component_stats,
            resource_snapshots=resource_snapshots,
        )

        return self.bottleneck_analyzer.generate_report(bottlenecks)

    def generate_component_report(self) -> str:
        """Generate component performance report.

        Returns:
            Formatted report string
        """
        if not self.component_profiler:
            return "No component profiling data available."

        return self.component_profiler.generate_report()

    def generate_resource_report(self) -> str:
        """Generate resource usage report.

        Returns:
            Formatted report string
        """
        if not self.resource_monitor:
            return "No resource monitoring data available."

        return self.resource_monitor.generate_report()

    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive profiling report with all sections.

        Returns:
            Formatted report string
        """
        lines = []
        lines.append("=" * 100)
        lines.append("COMPREHENSIVE PROFILING REPORT")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 100)
        lines.append("")

        # Top slowest operations
        lines.append(self.generate_top_slowest_report(10))
        lines.append("")

        # Component performance
        lines.append(self.generate_component_report())
        lines.append("")

        # Resource usage
        lines.append(self.generate_resource_report())
        lines.append("")

        # Bottleneck analysis
        lines.append(self.generate_bottleneck_report())
        lines.append("")

        return "\n".join(lines)

    def save_report(self, output_file: str, report_type: str = "comprehensive") -> bool:
        """Save profiling report to file.

        Args:
            output_file: Path to output file
            report_type: Type of report ("comprehensive", "slowest", "bottleneck", "component", "resource")

        Returns:
            True if successful, False otherwise
        """
        try:
            # Generate appropriate report
            if report_type == "comprehensive":
                report = self.generate_comprehensive_report()
            elif report_type == "slowest":
                report = self.generate_top_slowest_report()
            elif report_type == "bottleneck":
                report = self.generate_bottleneck_report()
            elif report_type == "component":
                report = self.generate_component_report()
            elif report_type == "resource":
                report = self.generate_resource_report()
            else:
                logger.error(f"Unknown report type: {report_type}")
                return False

            # Save to file
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                f.write(report)

            logger.info(f"✅ Profiling report saved to: {output_file}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to save report: {e}")
            return False

    def export_json(self, output_file: str) -> bool:
        """Export profiling data as JSON.

        Args:
            output_file: Path to output JSON file

        Returns:
            True if successful, False otherwise
        """
        try:
            data = {
                "generated_at": datetime.now().isoformat(),
                "function_stats": {},
                "component_stats": {},
                "resource_summary": {},
                "bottlenecks": [],
            }

            # Export function stats
            if self.function_profiler:
                function_stats = self.function_profiler.get_stats()
                data["function_stats"] = {
                    name: stats.to_dict() for name, stats in function_stats.items()
                }

            # Export component stats
            if self.component_profiler:
                component_stats = self.component_profiler.get_stats()
                data["component_stats"] = {
                    name: stats.to_dict() for name, stats in component_stats.items()
                }

            # Export resource summary
            if self.resource_monitor:
                data["resource_summary"] = self.resource_monitor.get_summary()

            # Export bottlenecks
            if self.bottleneck_analyzer:
                function_stats = (
                    self.function_profiler.get_stats() if self.function_profiler else None
                )
                component_stats = (
                    self.component_profiler.get_stats() if self.component_profiler else None
                )
                resource_snapshots = (
                    self.resource_monitor.get_snapshots() if self.resource_monitor else None
                )

                bottlenecks = self.bottleneck_analyzer.analyze(
                    function_stats=function_stats,
                    component_stats=component_stats,
                    resource_snapshots=resource_snapshots,
                )

                data["bottlenecks"] = [b.to_dict() for b in bottlenecks]

            # Save to file
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(f"✅ Profiling data exported to: {output_file}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to export JSON: {e}")
            return False


def generate_profiling_report(
    function_profiler: FunctionProfiler | None = None,
    component_profiler: ComponentProfiler | None = None,
    resource_monitor: ResourceMonitor | None = None,
    output_file: str | None = None,
    report_type: str = "comprehensive",
) -> str:
    """Convenience function to generate profiling report.

    Args:
        function_profiler: Function-level profiler instance
        component_profiler: Component-level profiler instance
        resource_monitor: Resource monitor instance
        output_file: Optional file path to save report
        report_type: Type of report to generate

    Returns:
        Formatted report string
    """
    generator = ProfilingReportGenerator(
        function_profiler=function_profiler,
        component_profiler=component_profiler,
        resource_monitor=resource_monitor,
    )

    if report_type == "comprehensive":
        report = generator.generate_comprehensive_report()
    elif report_type == "slowest":
        report = generator.generate_top_slowest_report()
    elif report_type == "bottleneck":
        report = generator.generate_bottleneck_report()
    elif report_type == "component":
        report = generator.generate_component_report()
    elif report_type == "resource":
        report = generator.generate_resource_report()
    else:
        report = generator.generate_comprehensive_report()

    if output_file:
        generator.save_report(output_file, report_type)

    return report
