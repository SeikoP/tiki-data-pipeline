"""Function-level profiler for measuring execution time and call counts.

This module provides a decorator-based profiler that collects detailed statistics
about function execution including timing percentiles, call counts, and more.
"""

import functools
import statistics
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List


@dataclass
class ProfileStats:
    """Statistics for a profiled function.
    
    Attributes:
        function_name: Name of the profiled function
        call_count: Number of times the function was called
        total_time_ms: Total execution time across all calls
        min_time_ms: Minimum execution time
        max_time_ms: Maximum execution time
        mean_time_ms: Mean execution time
        p50_time_ms: 50th percentile (median) execution time
        p95_time_ms: 95th percentile execution time
        p99_time_ms: 99th percentile execution time
        cpu_time_ms: CPU time (user + system)
        io_wait_ms: Estimated I/O wait time (total - CPU)
    """
    
    function_name: str
    call_count: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float('inf')
    max_time_ms: float = 0.0
    mean_time_ms: float = 0.0
    p50_time_ms: float = 0.0
    p95_time_ms: float = 0.0
    p99_time_ms: float = 0.0
    cpu_time_ms: float = 0.0
    io_wait_ms: float = 0.0
    
    # Internal: store recent execution times for percentile calculation
    # Limited to 1000 samples to prevent unbounded memory growth
    _execution_times: List[float] = field(default_factory=list, repr=False)
    _max_samples: int = field(default=1000, repr=False)
    
    def update(self, execution_time_ms: float, cpu_time_ms: float = 0.0):
        """Update statistics with a new execution time.
        
        Args:
            execution_time_ms: Wall-clock execution time in milliseconds
            cpu_time_ms: CPU time in milliseconds
        """
        self.call_count += 1
        self.total_time_ms += execution_time_ms
        self.cpu_time_ms += cpu_time_ms
        self.min_time_ms = min(self.min_time_ms, execution_time_ms)
        self.max_time_ms = max(self.max_time_ms, execution_time_ms)
        
        # Add to execution times with sliding window to prevent unbounded growth
        self._execution_times.append(execution_time_ms)
        if len(self._execution_times) > self._max_samples:
            # Keep most recent samples (sliding window)
            self._execution_times.pop(0)
        
        # Recalculate statistics
        self.mean_time_ms = self.total_time_ms / self.call_count
        
        # Calculate percentiles
        if len(self._execution_times) == 1:
            # Only one sample
            self.p50_time_ms = execution_time_ms
            self.p95_time_ms = execution_time_ms
            self.p99_time_ms = execution_time_ms
        elif len(self._execution_times) >= 2:
            sorted_times = sorted(self._execution_times)
            self.p50_time_ms = statistics.median(sorted_times)
            
            # For p95 and p99, need enough samples
            if len(sorted_times) >= 20:
                self.p95_time_ms = statistics.quantiles(sorted_times, n=20)[18]  # 95th percentile
            else:
                # Approximate with max for small samples
                self.p95_time_ms = sorted_times[int(len(sorted_times) * 0.95)]
            
            if len(sorted_times) >= 100:
                self.p99_time_ms = statistics.quantiles(sorted_times, n=100)[98]  # 99th percentile
            else:
                # Approximate with max for small samples
                self.p99_time_ms = sorted_times[int(len(sorted_times) * 0.99)]
        
        # Estimate I/O wait time
        self.io_wait_ms = self.total_time_ms - self.cpu_time_ms
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for serialization."""
        return {
            "function_name": self.function_name,
            "call_count": self.call_count,
            "total_time_ms": round(self.total_time_ms, 2),
            "min_time_ms": round(self.min_time_ms, 2),
            "max_time_ms": round(self.max_time_ms, 2),
            "mean_time_ms": round(self.mean_time_ms, 2),
            "p50_time_ms": round(self.p50_time_ms, 2),
            "p95_time_ms": round(self.p95_time_ms, 2),
            "p99_time_ms": round(self.p99_time_ms, 2),
            "cpu_time_ms": round(self.cpu_time_ms, 2),
            "io_wait_ms": round(self.io_wait_ms, 2),
        }


class FunctionProfiler:
    """Thread-safe function profiler that collects execution statistics.
    
    This profiler uses decorators to wrap functions and collect timing data.
    All statistics are stored in a thread-safe manner.
    
    Example:
        >>> profiler = FunctionProfiler()
        >>> 
        >>> @profiler.profile
        >>> def my_function():
        >>>     time.sleep(0.1)
        >>> 
        >>> my_function()
        >>> stats = profiler.get_stats()
        >>> print(stats["my_function"].mean_time_ms)
    """
    
    def __init__(self):
        """Initialize the profiler with empty statistics."""
        self._stats: Dict[str, ProfileStats] = {}
        self._lock = threading.Lock()
    
    def profile(self, func: Callable) -> Callable:
        """Decorator that profiles function execution.
        
        Args:
            func: Function to profile
            
        Returns:
            Wrapped function that collects profiling data
        """
        func_name = f"{func.__module__}.{func.__qualname__}"
        
        # Initialize stats for this function
        with self._lock:
            if func_name not in self._stats:
                self._stats[func_name] = ProfileStats(function_name=func_name)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Measure wall-clock time
            start_time = time.perf_counter()
            
            # Measure CPU time (user + system)
            start_cpu = time.process_time()
            
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                # Calculate elapsed times
                end_time = time.perf_counter()
                end_cpu = time.process_time()
                
                execution_time_ms = (end_time - start_time) * 1000
                cpu_time_ms = (end_cpu - start_cpu) * 1000
                
                # Update statistics in thread-safe manner
                with self._lock:
                    self._stats[func_name].update(execution_time_ms, cpu_time_ms)
        
        return wrapper
    
    def get_stats(self, func_name: str | None = None) -> Dict[str, ProfileStats] | ProfileStats:
        """Get profiling statistics.
        
        Args:
            func_name: Optional function name to get stats for specific function
            
        Returns:
            Dictionary of all stats if func_name is None, otherwise stats for specific function
        """
        with self._lock:
            if func_name:
                return self._stats.get(func_name)
            return dict(self._stats)
    
    def get_top_slowest(self, n: int = 10) -> List[ProfileStats]:
        """Get the N slowest functions by total execution time.
        
        Args:
            n: Number of functions to return
            
        Returns:
            List of ProfileStats sorted by total_time_ms descending
        """
        with self._lock:
            sorted_stats = sorted(
                self._stats.values(),
                key=lambda s: s.total_time_ms,
                reverse=True
            )
            return sorted_stats[:n]
    
    def reset(self):
        """Reset all profiling statistics."""
        with self._lock:
            self._stats.clear()
    
    def generate_report(self) -> str:
        """Generate a human-readable profiling report.
        
        Returns:
            Formatted string report of profiling statistics
        """
        with self._lock:
            if not self._stats:
                return "No profiling data collected."
            
            lines = ["=" * 100]
            lines.append("FUNCTION PROFILING REPORT")
            lines.append("=" * 100)
            lines.append("")
            
            # Top 10 slowest functions
            lines.append("TOP 10 SLOWEST FUNCTIONS (by total time):")
            lines.append("-" * 100)
            lines.append(
                f"{'Function':<50} {'Calls':>8} {'Total (ms)':>12} {'Mean (ms)':>12} {'P95 (ms)':>12}"
            )
            lines.append("-" * 100)
            
            for stats in self.get_top_slowest(10):
                lines.append(
                    f"{stats.function_name:<50} {stats.call_count:>8} "
                    f"{stats.total_time_ms:>12.2f} {stats.mean_time_ms:>12.2f} "
                    f"{stats.p95_time_ms:>12.2f}"
                )
            
            lines.append("")
            lines.append("=" * 100)
            
            return "\n".join(lines)


# Global profiler instance for convenience
_global_profiler = FunctionProfiler()


def profile(func: Callable) -> Callable:
    """Convenience decorator using the global profiler instance.
    
    Example:
        >>> from common.profiling import profile
        >>> 
        >>> @profile
        >>> def my_function():
        >>>     time.sleep(0.1)
    """
    return _global_profiler.profile(func)


def get_global_profiler() -> FunctionProfiler:
    """Get the global profiler instance."""
    return _global_profiler
