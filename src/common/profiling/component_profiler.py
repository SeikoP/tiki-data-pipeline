"""Component-level profiler for tracking pipeline stage performance.

This module provides profiling for major pipeline components (Crawler, Transformer, Loader)
to identify which stage is the bottleneck.
"""

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


@dataclass
class ComponentStats:
    """Statistics for a pipeline component.
    
    Attributes:
        component_name: Name of the component (e.g., "Crawler", "Transformer", "Loader")
        execution_time_ms: Total wall-clock execution time
        cpu_time_ms: CPU time (user + system)
        io_wait_ms: Estimated I/O wait time
        memory_peak_mb: Peak memory usage during execution
        memory_avg_mb: Average memory usage during execution
        items_processed: Number of items processed
        throughput_items_per_sec: Processing throughput
    """
    
    component_name: str
    execution_time_ms: float = 0.0
    cpu_time_ms: float = 0.0
    io_wait_ms: float = 0.0
    memory_peak_mb: float = 0.0
    memory_avg_mb: float = 0.0
    items_processed: int = 0
    throughput_items_per_sec: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert stats to dictionary for serialization."""
        return {
            "component_name": self.component_name,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "cpu_time_ms": round(self.cpu_time_ms, 2),
            "io_wait_ms": round(self.io_wait_ms, 2),
            "memory_peak_mb": round(self.memory_peak_mb, 2),
            "memory_avg_mb": round(self.memory_avg_mb, 2),
            "items_processed": self.items_processed,
            "throughput_items_per_sec": round(self.throughput_items_per_sec, 2),
        }


class ComponentProfiler:
    """Thread-safe profiler for pipeline components.
    
    Tracks execution time, CPU usage, memory usage, and throughput for
    major pipeline stages (Crawler, Transformer, Loader).
    
    Example:
        >>> profiler = ComponentProfiler()
        >>> 
        >>> profiler.start_component("Crawler")
        >>> # ... crawl products ...
        >>> stats = profiler.end_component("Crawler", items_processed=100)
        >>> print(f"Throughput: {stats.throughput_items_per_sec} items/sec")
    """
    
    def __init__(self):
        """Initialize the component profiler."""
        self._active_components: Dict[str, Dict] = {}
        self._completed_stats: Dict[str, ComponentStats] = {}
        self._lock = threading.Lock()
        
        # Get process handle for memory monitoring
        self._process = None
        if PSUTIL_AVAILABLE:
            try:
                import os
                self._process = psutil.Process(os.getpid())
                # Test if memory_info() works (some systems may hang)
                self._process.memory_info()
            except Exception:
                # If psutil fails, disable it
                self._process = None
    
    def start_component(self, component_name: str) -> None:
        """Mark the start of a component execution.
        
        Args:
            component_name: Name of the component (e.g., "Crawler")
        """
        with self._lock:
            if component_name in self._active_components:
                raise ValueError(f"Component '{component_name}' is already active")
            
            # Record start metrics
            start_data = {
                "start_time": time.perf_counter(),
                "start_cpu": time.process_time(),
                "start_timestamp": datetime.now(),
                "memory_samples": [],
            }
            
            # Record initial memory
            if self._process:
                try:
                    mem_info = self._process.memory_info()
                    start_data["start_memory_mb"] = mem_info.rss / 1024 / 1024
                    start_data["memory_samples"].append(start_data["start_memory_mb"])
                except Exception:
                    start_data["start_memory_mb"] = 0.0
            else:
                start_data["start_memory_mb"] = 0.0
            
            self._active_components[component_name] = start_data
    
    def sample_memory(self, component_name: str) -> None:
        """Sample current memory usage for an active component.
        
        This should be called periodically during component execution
        to track memory usage over time.
        
        Args:
            component_name: Name of the active component
        """
        with self._lock:
            if component_name not in self._active_components:
                return
            
            if self._process:
                try:
                    mem_info = self._process.memory_info()
                    current_memory_mb = mem_info.rss / 1024 / 1024
                    self._active_components[component_name]["memory_samples"].append(current_memory_mb)
                except Exception:
                    pass
    
    def end_component(
        self,
        component_name: str,
        items_processed: int = 0
    ) -> ComponentStats:
        """Mark the end of a component execution and return statistics.
        
        Args:
            component_name: Name of the component
            items_processed: Number of items processed by this component
            
        Returns:
            ComponentStats with collected metrics
            
        Raises:
            ValueError: If component was not started
        """
        # Sample final memory BEFORE acquiring lock to avoid deadlock
        if self._process:
            try:
                mem_info = self._process.memory_info()
                final_memory_mb = mem_info.rss / 1024 / 1024
            except Exception:
                final_memory_mb = None
        else:
            final_memory_mb = None
        
        with self._lock:
            if component_name not in self._active_components:
                raise ValueError(f"Component '{component_name}' was not started")
            
            start_data = self._active_components[component_name]
            
            # Calculate elapsed times
            end_time = time.perf_counter()
            end_cpu = time.process_time()
            
            execution_time_ms = (end_time - start_data["start_time"]) * 1000
            cpu_time_ms = (end_cpu - start_data["start_cpu"]) * 1000
            io_wait_ms = execution_time_ms - cpu_time_ms
            
            # Add final memory sample if available
            if final_memory_mb is not None:
                start_data["memory_samples"].append(final_memory_mb)
            
            # Calculate memory statistics
            memory_samples = start_data["memory_samples"]
            if memory_samples:
                memory_peak_mb = max(memory_samples)
                memory_avg_mb = sum(memory_samples) / len(memory_samples)
            else:
                memory_peak_mb = 0.0
                memory_avg_mb = 0.0
            
            # Calculate throughput
            if execution_time_ms > 0:
                throughput = (items_processed / execution_time_ms) * 1000  # items per second
            else:
                throughput = 0.0
            
            # Create stats object
            stats = ComponentStats(
                component_name=component_name,
                execution_time_ms=execution_time_ms,
                cpu_time_ms=cpu_time_ms,
                io_wait_ms=io_wait_ms,
                memory_peak_mb=memory_peak_mb,
                memory_avg_mb=memory_avg_mb,
                items_processed=items_processed,
                throughput_items_per_sec=throughput,
            )
            
            # Store completed stats and remove from active
            self._completed_stats[component_name] = stats
            del self._active_components[component_name]
            
            return stats
    
    def get_stats(self, component_name: Optional[str] = None) -> Dict[str, ComponentStats] | ComponentStats:
        """Get component statistics.
        
        Args:
            component_name: Optional component name to get stats for specific component
            
        Returns:
            Dictionary of all stats if component_name is None, otherwise stats for specific component
        """
        with self._lock:
            if component_name:
                return self._completed_stats.get(component_name)
            return dict(self._completed_stats)
    
    def reset(self):
        """Reset all component statistics."""
        with self._lock:
            self._active_components.clear()
            self._completed_stats.clear()
    
    def generate_report(self) -> str:
        """Generate a human-readable component profiling report.
        
        Returns:
            Formatted string report of component statistics
        """
        with self._lock:
            if not self._completed_stats:
                return "No component profiling data collected."
            
            lines = ["=" * 100]
            lines.append("COMPONENT PROFILING REPORT")
            lines.append("=" * 100)
            lines.append("")
            
            # Component statistics
            lines.append("COMPONENT PERFORMANCE:")
            lines.append("-" * 100)
            lines.append(
                f"{'Component':<20} {'Time (ms)':>12} {'CPU (ms)':>12} {'I/O (ms)':>12} "
                f"{'Items':>10} {'Throughput':>15}"
            )
            lines.append("-" * 100)
            
            for stats in self._completed_stats.values():
                lines.append(
                    f"{stats.component_name:<20} {stats.execution_time_ms:>12.2f} "
                    f"{stats.cpu_time_ms:>12.2f} {stats.io_wait_ms:>12.2f} "
                    f"{stats.items_processed:>10} {stats.throughput_items_per_sec:>12.2f} /s"
                )
            
            lines.append("")
            
            # Memory statistics
            lines.append("MEMORY USAGE:")
            lines.append("-" * 100)
            lines.append(f"{'Component':<20} {'Peak (MB)':>15} {'Average (MB)':>15}")
            lines.append("-" * 100)
            
            for stats in self._completed_stats.values():
                lines.append(
                    f"{stats.component_name:<20} {stats.memory_peak_mb:>15.2f} "
                    f"{stats.memory_avg_mb:>15.2f}"
                )
            
            lines.append("")
            lines.append("=" * 100)
            
            return "\n".join(lines)


# Global component profiler instance
_global_component_profiler = ComponentProfiler()


def get_global_component_profiler() -> ComponentProfiler:
    """Get the global component profiler instance."""
    return _global_component_profiler
