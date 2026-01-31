"""Bottleneck analyzer for identifying and ranking performance issues.

This module analyzes profiling data to automatically identify bottlenecks
and categorize them by type (CPU, I/O, Memory, Database, Network).
"""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

from .component_profiler import ComponentStats
from .function_profiler import ProfileStats
from .resource_monitor import ResourceSnapshot


class BottleneckCategory(Enum):
    """Categories of performance bottlenecks."""
    CPU = "CPU"
    IO = "I/O"
    MEMORY = "Memory"
    DATABASE = "Database"
    NETWORK = "Network"
    UNKNOWN = "Unknown"


class BottleneckSeverity(Enum):
    """Severity levels for bottlenecks."""
    HIGH = "HIGH"      # >20% of total execution time
    MEDIUM = "MEDIUM"  # 5-20% of total execution time
    LOW = "LOW"        # <5% of total execution time


@dataclass
class Bottleneck:
    """Identified performance bottleneck.
    
    Attributes:
        name: Name of the bottleneck (function or component name)
        category: Type of bottleneck (CPU, I/O, Memory, etc.)
        severity: Impact level (HIGH, MEDIUM, LOW)
        time_percentage: Percentage of total execution time
        description: Human-readable description
        recommendation: Suggested optimization approach
    """
    
    name: str
    category: BottleneckCategory
    severity: BottleneckSeverity
    time_percentage: float
    description: str
    recommendation: str
    
    def to_dict(self) -> Dict:
        """Convert bottleneck to dictionary for serialization."""
        return {
            "name": self.name,
            "category": self.category.value,
            "severity": self.severity.value,
            "time_percentage": round(self.time_percentage, 2),
            "description": self.description,
            "recommendation": self.recommendation,
        }


class BottleneckAnalyzer:
    """Analyzes profiling data to identify and rank bottlenecks.
    
    Uses heuristics to categorize bottlenecks by type and rank by impact.
    
    Example:
        >>> analyzer = BottleneckAnalyzer()
        >>> bottlenecks = analyzer.analyze(
        ...     function_stats=profiler.get_stats(),
        ...     component_stats=component_profiler.get_stats(),
        ...     resource_snapshots=resource_monitor.get_snapshots()
        ... )
        >>> for b in bottlenecks:
        ...     print(f"{b.severity.value}: {b.name} ({b.time_percentage:.1f}%)")
    """
    
    def __init__(self):
        """Initialize the bottleneck analyzer."""
        pass
    
    def analyze(
        self,
        function_stats: Dict[str, ProfileStats] = None,
        component_stats: Dict[str, ComponentStats] = None,
        resource_snapshots: List[ResourceSnapshot] = None,
    ) -> List[Bottleneck]:
        """Analyze profiling data and identify bottlenecks.
        
        Args:
            function_stats: Function-level profiling statistics
            component_stats: Component-level profiling statistics
            resource_snapshots: System resource snapshots
            
        Returns:
            List of Bottleneck objects sorted by severity and time percentage
        """
        bottlenecks = []
        
        # Calculate total execution time
        total_time_ms = 0.0
        if component_stats:
            total_time_ms = sum(s.execution_time_ms for s in component_stats.values())
        elif function_stats:
            total_time_ms = sum(s.total_time_ms for s in function_stats.values())
        
        if total_time_ms == 0:
            return []
        
        # Analyze component-level bottlenecks
        if component_stats:
            for stats in component_stats.values():
                time_pct = (stats.execution_time_ms / total_time_ms) * 100
                
                # Categorize bottleneck
                category = self._categorize_component(stats)
                
                # Determine severity
                severity = self._determine_severity(time_pct)
                
                # Generate description and recommendation
                description = self._generate_component_description(stats, time_pct)
                recommendation = self._generate_component_recommendation(stats, category)
                
                bottleneck = Bottleneck(
                    name=stats.component_name,
                    category=category,
                    severity=severity,
                    time_percentage=time_pct,
                    description=description,
                    recommendation=recommendation,
                )
                bottlenecks.append(bottleneck)
        
        # Analyze function-level bottlenecks (top slowest functions)
        if function_stats:
            # Get top 10 slowest functions
            sorted_funcs = sorted(
                function_stats.values(),
                key=lambda s: s.total_time_ms,
                reverse=True
            )[:10]
            
            for stats in sorted_funcs:
                time_pct = (stats.total_time_ms / total_time_ms) * 100
                
                # Only include if significant (>1%)
                if time_pct < 1.0:
                    continue
                
                # Categorize bottleneck
                category = self._categorize_function(stats)
                
                # Determine severity
                severity = self._determine_severity(time_pct)
                
                # Generate description and recommendation
                description = self._generate_function_description(stats, time_pct)
                recommendation = self._generate_function_recommendation(stats, category)
                
                bottleneck = Bottleneck(
                    name=stats.function_name,
                    category=category,
                    severity=severity,
                    time_percentage=time_pct,
                    description=description,
                    recommendation=recommendation,
                )
                bottlenecks.append(bottleneck)
        
        # Sort by severity (HIGH > MEDIUM > LOW) then by time percentage
        severity_order = {
            BottleneckSeverity.HIGH: 0,
            BottleneckSeverity.MEDIUM: 1,
            BottleneckSeverity.LOW: 2,
        }
        bottlenecks.sort(key=lambda b: (severity_order[b.severity], -b.time_percentage))
        
        return bottlenecks
    
    def _categorize_component(self, stats: ComponentStats) -> BottleneckCategory:
        """Categorize a component bottleneck by type.
        
        Uses heuristics based on CPU vs I/O wait time.
        """
        if stats.execution_time_ms == 0:
            return BottleneckCategory.UNKNOWN
        
        cpu_ratio = stats.cpu_time_ms / stats.execution_time_ms
        io_ratio = stats.io_wait_ms / stats.execution_time_ms
        
        # High CPU usage (>70%)
        if cpu_ratio > 0.7:
            return BottleneckCategory.CPU
        
        # High I/O wait (>70%)
        if io_ratio > 0.7:
            # Distinguish between database and network I/O based on component name
            component_lower = stats.component_name.lower()
            if "load" in component_lower or "database" in component_lower:
                return BottleneckCategory.DATABASE
            elif "crawl" in component_lower or "fetch" in component_lower:
                return BottleneckCategory.NETWORK
            else:
                return BottleneckCategory.IO
        
        # Balanced CPU/I/O
        return BottleneckCategory.UNKNOWN
    
    def _categorize_function(self, stats: ProfileStats) -> BottleneckCategory:
        """Categorize a function bottleneck by type.
        
        Uses heuristics based on function name and CPU vs I/O wait time.
        """
        if stats.total_time_ms == 0:
            return BottleneckCategory.UNKNOWN
        
        func_lower = stats.function_name.lower()
        cpu_ratio = stats.cpu_time_ms / stats.total_time_ms
        io_ratio = stats.io_wait_ms / stats.total_time_ms
        
        # Check function name patterns
        if any(pattern in func_lower for pattern in ["request", "fetch", "get_page", "crawl"]):
            return BottleneckCategory.NETWORK
        
        if any(pattern in func_lower for pattern in ["database", "query", "insert", "upsert", "load"]):
            return BottleneckCategory.DATABASE
        
        if any(pattern in func_lower for pattern in ["transform", "process", "calculate", "compute"]):
            if cpu_ratio > 0.6:
                return BottleneckCategory.CPU
            else:
                return BottleneckCategory.IO
        
        # Fallback to CPU/I/O ratio
        if cpu_ratio > 0.7:
            return BottleneckCategory.CPU
        elif io_ratio > 0.7:
            return BottleneckCategory.IO
        
        return BottleneckCategory.UNKNOWN
    
    def _determine_severity(self, time_percentage: float) -> BottleneckSeverity:
        """Determine severity based on time percentage."""
        if time_percentage > 20:
            return BottleneckSeverity.HIGH
        elif time_percentage > 5:
            return BottleneckSeverity.MEDIUM
        else:
            return BottleneckSeverity.LOW
    
    def _generate_component_description(self, stats: ComponentStats, time_pct: float) -> str:
        """Generate human-readable description for component bottleneck."""
        return (
            f"{stats.component_name} takes {stats.execution_time_ms:.0f}ms "
            f"({time_pct:.1f}% of total time). "
            f"Processed {stats.items_processed} items at "
            f"{stats.throughput_items_per_sec:.1f} items/sec."
        )
    
    def _generate_function_description(self, stats: ProfileStats, time_pct: float) -> str:
        """Generate human-readable description for function bottleneck."""
        return (
            f"{stats.function_name} called {stats.call_count} times, "
            f"total {stats.total_time_ms:.0f}ms ({time_pct:.1f}% of total time). "
            f"Mean: {stats.mean_time_ms:.2f}ms, P95: {stats.p95_time_ms:.2f}ms."
        )
    
    def _generate_component_recommendation(
        self,
        stats: ComponentStats,
        category: BottleneckCategory
    ) -> str:
        """Generate optimization recommendation for component."""
        recommendations = {
            BottleneckCategory.CPU: (
                "Consider: 1) Vectorization with NumPy/Pandas, "
                "2) Multiprocessing for CPU-bound tasks, "
                "3) Reduce redundant loops"
            ),
            BottleneckCategory.IO: (
                "Consider: 1) Async I/O for concurrent operations, "
                "2) Batch processing to reduce I/O calls, "
                "3) Caching frequently accessed data"
            ),
            BottleneckCategory.DATABASE: (
                "Consider: 1) Batch upsert operations, "
                "2) Connection pooling, "
                "3) Use COPY command for bulk inserts, "
                "4) Add indexes on frequently queried columns"
            ),
            BottleneckCategory.NETWORK: (
                "Consider: 1) Async HTTP requests with aiohttp, "
                "2) Increase concurrent request limit, "
                "3) Session reuse and connection pooling, "
                "4) Optimize cache hit rate"
            ),
            BottleneckCategory.MEMORY: (
                "Consider: 1) Streaming/generator-based processing, "
                "2) Reduce data copying, "
                "3) Implement LRU cache with size limits"
            ),
            BottleneckCategory.UNKNOWN: (
                "Profile this component in more detail to identify specific bottleneck"
            ),
        }
        return recommendations.get(category, "No specific recommendation available")
    
    def _generate_function_recommendation(
        self,
        stats: ProfileStats,
        category: BottleneckCategory
    ) -> str:
        """Generate optimization recommendation for function."""
        # Similar to component recommendations but more specific
        return self._generate_component_recommendation(stats, category)
    
    def generate_report(self, bottlenecks: List[Bottleneck]) -> str:
        """Generate a human-readable bottleneck analysis report.
        
        Args:
            bottlenecks: List of identified bottlenecks
            
        Returns:
            Formatted string report
        """
        if not bottlenecks:
            return "No significant bottlenecks identified."
        
        lines = ["=" * 100]
        lines.append("BOTTLENECK ANALYSIS REPORT")
        lines.append("=" * 100)
        lines.append("")
        
        # Group by severity
        high_bottlenecks = [b for b in bottlenecks if b.severity == BottleneckSeverity.HIGH]
        medium_bottlenecks = [b for b in bottlenecks if b.severity == BottleneckSeverity.MEDIUM]
        low_bottlenecks = [b for b in bottlenecks if b.severity == BottleneckSeverity.LOW]
        
        if high_bottlenecks:
            lines.append("🔴 HIGH PRIORITY BOTTLENECKS (>20% of total time):")
            lines.append("-" * 100)
            for b in high_bottlenecks:
                lines.append(f"\n{b.name} [{b.category.value}] - {b.time_percentage:.1f}%")
                lines.append(f"  Description: {b.description}")
                lines.append(f"  Recommendation: {b.recommendation}")
            lines.append("")
        
        if medium_bottlenecks:
            lines.append("🟡 MEDIUM PRIORITY BOTTLENECKS (5-20% of total time):")
            lines.append("-" * 100)
            for b in medium_bottlenecks:
                lines.append(f"\n{b.name} [{b.category.value}] - {b.time_percentage:.1f}%")
                lines.append(f"  Description: {b.description}")
                lines.append(f"  Recommendation: {b.recommendation}")
            lines.append("")
        
        if low_bottlenecks:
            lines.append("🟢 LOW PRIORITY BOTTLENECKS (<5% of total time):")
            lines.append("-" * 100)
            for b in low_bottlenecks:
                lines.append(f"  • {b.name} [{b.category.value}] - {b.time_percentage:.1f}%")
            lines.append("")
        
        lines.append("=" * 100)
        lines.append("")
        lines.append("OPTIMIZATION PRIORITY:")
        lines.append("1. Focus on HIGH priority bottlenecks first (biggest impact)")
        lines.append("2. Address MEDIUM priority bottlenecks after HIGH are resolved")
        lines.append("3. LOW priority bottlenecks may not be worth optimizing")
        lines.append("")
        
        return "\n".join(lines)


# Global bottleneck analyzer instance
_global_analyzer = BottleneckAnalyzer()


def get_global_analyzer() -> BottleneckAnalyzer:
    """Get the global bottleneck analyzer instance."""
    return _global_analyzer
