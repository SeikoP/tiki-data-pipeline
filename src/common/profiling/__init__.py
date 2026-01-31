"""Performance profiling utilities for the Tiki data pipeline.

This module provides comprehensive profiling tools to identify performance bottlenecks:
- FunctionProfiler: Decorator-based function-level profiling
- ComponentProfiler: Pipeline component profiling (Crawler, Transformer, Loader)
- ResourceMonitor: System resource monitoring (CPU, memory, I/O)
- BottleneckAnalyzer: Automated bottleneck identification and ranking
- PipelineProfiler: Wrapper decorators for pipeline stages
- ProfiledPipelineRunner: Complete pipeline execution with profiling
- ProfilingReportGenerator: Comprehensive report generation
"""

from .bottleneck_analyzer import (
    BottleneckAnalyzer,
    Bottleneck,
    BottleneckCategory,
    BottleneckSeverity,
    get_global_analyzer,
)
from .component_profiler import ComponentProfiler, ComponentStats, get_global_component_profiler
from .function_profiler import FunctionProfiler, ProfileStats, profile, get_global_profiler
from .resource_monitor import ResourceMonitor, ResourceSnapshot, get_global_resource_monitor
from .pipeline_profiler import (
    profile_crawler,
    profile_transformer,
    profile_loader,
    PipelineProfilerContext,
)
from .pipeline_runner import (
    ProfiledPipelineRunner,
    run_with_profiling,
    get_global_runner,
)
from .report_generator import (
    ProfilingReportGenerator,
    generate_profiling_report,
)

__all__ = [
    "FunctionProfiler",
    "ProfileStats",
    "profile",
    "get_global_profiler",
    "ComponentProfiler",
    "ComponentStats",
    "get_global_component_profiler",
    "ResourceMonitor",
    "ResourceSnapshot",
    "get_global_resource_monitor",
    "BottleneckAnalyzer",
    "Bottleneck",
    "BottleneckCategory",
    "BottleneckSeverity",
    "get_global_analyzer",
    "profile_crawler",
    "profile_transformer",
    "profile_loader",
    "PipelineProfilerContext",
    "ProfiledPipelineRunner",
    "run_with_profiling",
    "get_global_runner",
    "ProfilingReportGenerator",
    "generate_profiling_report",
]
