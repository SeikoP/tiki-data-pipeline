"""Performance metrics collection for pipeline runs.

This module provides comprehensive metrics collection for pipeline executions,
tracking throughput, latency, error rates, resource utilization, and cache hit rates.
"""

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
import statistics


@dataclass
class PipelineRunMetrics:
    """Metrics for a single pipeline run.
    
    Attributes:
        run_id: Unique identifier for the pipeline run
        start_time: When the pipeline run started
        end_time: When the pipeline run ended
        duration_seconds: Total execution time
        products_processed: Number of products processed
        throughput_products_per_second: Products processed per second
        latency_per_product_ms: Average time per product in milliseconds
        error_count: Number of errors encountered
        error_rate: Percentage of operations that failed
        cpu_utilization_percent: Average CPU utilization
        memory_peak_mb: Peak memory usage in MB
        memory_avg_mb: Average memory usage in MB
        disk_read_mb: Total disk read in MB
        disk_write_mb: Total disk write in MB
        network_sent_mb: Total network sent in MB
        network_recv_mb: Total network received in MB
        cache_l1_hits: L1 (memory) cache hits
        cache_l1_misses: L1 cache misses
        cache_l2_hits: L2 (Redis) cache hits
        cache_l2_misses: L2 cache misses
        cache_l3_hits: L3 (file) cache hits
        cache_l3_misses: L3 cache misses
        cache_l1_hit_rate: L1 cache hit rate (0.0-1.0)
        cache_l2_hit_rate: L2 cache hit rate (0.0-1.0)
        cache_l3_hit_rate: L3 cache hit rate (0.0-1.0)
        data_completeness_percent: Percentage of complete data records
    """
    
    run_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0
    products_processed: int = 0
    throughput_products_per_second: float = 0.0
    latency_per_product_ms: float = 0.0
    error_count: int = 0
    error_rate: float = 0.0
    cpu_utilization_percent: float = 0.0
    memory_peak_mb: float = 0.0
    memory_avg_mb: float = 0.0
    disk_read_mb: float = 0.0
    disk_write_mb: float = 0.0
    network_sent_mb: float = 0.0
    network_recv_mb: float = 0.0
    cache_l1_hits: int = 0
    cache_l1_misses: int = 0
    cache_l2_hits: int = 0
    cache_l2_misses: int = 0
    cache_l3_hits: int = 0
    cache_l3_misses: int = 0
    cache_l1_hit_rate: float = 0.0
    cache_l2_hit_rate: float = 0.0
    cache_l3_hit_rate: float = 0.0
    data_completeness_percent: float = 0.0
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary for serialization."""
        return {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 2),
            "products_processed": self.products_processed,
            "throughput_products_per_second": round(self.throughput_products_per_second, 2),
            "latency_per_product_ms": round(self.latency_per_product_ms, 2),
            "error_count": self.error_count,
            "error_rate": round(self.error_rate, 4),
            "cpu_utilization_percent": round(self.cpu_utilization_percent, 2),
            "memory_peak_mb": round(self.memory_peak_mb, 2),
            "memory_avg_mb": round(self.memory_avg_mb, 2),
            "disk_read_mb": round(self.disk_read_mb, 2),
            "disk_write_mb": round(self.disk_write_mb, 2),
            "network_sent_mb": round(self.network_sent_mb, 2),
            "network_recv_mb": round(self.network_recv_mb, 2),
            "cache_l1_hits": self.cache_l1_hits,
            "cache_l1_misses": self.cache_l1_misses,
            "cache_l2_hits": self.cache_l2_hits,
            "cache_l2_misses": self.cache_l2_misses,
            "cache_l3_hits": self.cache_l3_hits,
            "cache_l3_misses": self.cache_l3_misses,
            "cache_l1_hit_rate": round(self.cache_l1_hit_rate, 4),
            "cache_l2_hit_rate": round(self.cache_l2_hit_rate, 4),
            "cache_l3_hit_rate": round(self.cache_l3_hit_rate, 4),
            "data_completeness_percent": round(self.data_completeness_percent, 2),
        }


class MetricsCollector:
    """Collects and tracks performance metrics for pipeline runs.
    
    Integrates with profiling infrastructure to collect comprehensive metrics
    including throughput, latency, error rates, resource utilization, and cache hit rates.
    
    Example:
        >>> collector = MetricsCollector()
        >>> collector.start_run("run_001")
        >>> # ... execute pipeline ...
        >>> collector.record_products_processed(1000)
        >>> collector.record_error()
        >>> collector.record_cache_hit("L1")
        >>> metrics = collector.end_run()
        >>> print(f"Throughput: {metrics.throughput_products_per_second} products/sec")
    """
    
    def __init__(self):
        """Initialize the metrics collector."""
        self._current_run: Optional[PipelineRunMetrics] = None
        self._run_history: List[PipelineRunMetrics] = []
        self._operation_count = 0
        
    def start_run(self, run_id: str) -> None:
        """Start collecting metrics for a new pipeline run.
        
        Args:
            run_id: Unique identifier for this run
        """
        if self._current_run is not None:
            # End previous run if not already ended
            self.end_run()
        
        self._current_run = PipelineRunMetrics(
            run_id=run_id,
            start_time=datetime.now()
        )
        self._operation_count = 0
    
    def end_run(self) -> PipelineRunMetrics:
        """End the current pipeline run and calculate final metrics.
        
        Returns:
            PipelineRunMetrics with all collected metrics
        """
        if self._current_run is None:
            raise ValueError("No active run to end. Call start_run() first.")
        
        self._current_run.end_time = datetime.now()
        self._current_run.duration_seconds = (
            self._current_run.end_time - self._current_run.start_time
        ).total_seconds()
        
        # Calculate throughput
        if self._current_run.duration_seconds > 0:
            self._current_run.throughput_products_per_second = (
                self._current_run.products_processed / self._current_run.duration_seconds
            )
        
        # Calculate latency per product
        if self._current_run.products_processed > 0:
            self._current_run.latency_per_product_ms = (
                self._current_run.duration_seconds * 1000 / self._current_run.products_processed
            )
        
        # Calculate error rate
        if self._operation_count > 0:
            self._current_run.error_rate = (
                self._current_run.error_count / self._operation_count
            )
        
        # Calculate cache hit rates
        self._calculate_cache_hit_rates()
        
        # Store in history
        self._run_history.append(self._current_run)
        
        # Return and clear current run
        metrics = self._current_run
        self._current_run = None
        return metrics
    
    def record_products_processed(self, count: int) -> None:
        """Record the number of products processed.
        
        Args:
            count: Number of products processed
        """
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        self._current_run.products_processed += count
        self._operation_count += count
    
    def record_error(self) -> None:
        """Record an error occurrence."""
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        self._current_run.error_count += 1
        self._operation_count += 1
    
    def record_cache_hit(self, level: str) -> None:
        """Record a cache hit.
        
        Args:
            level: Cache level ("L1", "L2", or "L3")
        """
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        level = level.upper()
        if level == "L1":
            self._current_run.cache_l1_hits += 1
        elif level == "L2":
            self._current_run.cache_l2_hits += 1
        elif level == "L3":
            self._current_run.cache_l3_hits += 1
        else:
            raise ValueError(f"Invalid cache level: {level}. Must be L1, L2, or L3.")
    
    def record_cache_miss(self, level: str) -> None:
        """Record a cache miss.
        
        Args:
            level: Cache level ("L1", "L2", or "L3")
        """
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        level = level.upper()
        if level == "L1":
            self._current_run.cache_l1_misses += 1
        elif level == "L2":
            self._current_run.cache_l2_misses += 1
        elif level == "L3":
            self._current_run.cache_l3_misses += 1
        else:
            raise ValueError(f"Invalid cache level: {level}. Must be L1, L2, or L3.")
    
    def update_resource_metrics(self, resource_summary: Dict) -> None:
        """Update resource utilization metrics from ResourceMonitor.
        
        Args:
            resource_summary: Summary dict from ResourceMonitor.get_summary()
        """
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        if "cpu_percent" in resource_summary:
            self._current_run.cpu_utilization_percent = resource_summary["cpu_percent"].get("mean", 0.0)
        
        if "memory_rss_mb" in resource_summary:
            self._current_run.memory_peak_mb = resource_summary["memory_rss_mb"].get("max", 0.0)
            self._current_run.memory_avg_mb = resource_summary["memory_rss_mb"].get("mean", 0.0)
        
        self._current_run.disk_read_mb = resource_summary.get("disk_read_mb", 0.0)
        self._current_run.disk_write_mb = resource_summary.get("disk_write_mb", 0.0)
        self._current_run.network_sent_mb = resource_summary.get("network_sent_mb", 0.0)
        self._current_run.network_recv_mb = resource_summary.get("network_recv_mb", 0.0)
    
    def set_data_completeness(self, completeness_percent: float) -> None:
        """Set the data completeness percentage.
        
        Args:
            completeness_percent: Completeness percentage (0.0-100.0)
        """
        if self._current_run is None:
            raise ValueError("No active run. Call start_run() first.")
        
        self._current_run.data_completeness_percent = completeness_percent
    
    def _calculate_cache_hit_rates(self) -> None:
        """Calculate cache hit rates for all levels."""
        if self._current_run is None:
            return
        
        # L1 cache hit rate
        l1_total = self._current_run.cache_l1_hits + self._current_run.cache_l1_misses
        if l1_total > 0:
            self._current_run.cache_l1_hit_rate = self._current_run.cache_l1_hits / l1_total
        
        # L2 cache hit rate
        l2_total = self._current_run.cache_l2_hits + self._current_run.cache_l2_misses
        if l2_total > 0:
            self._current_run.cache_l2_hit_rate = self._current_run.cache_l2_hits / l2_total
        
        # L3 cache hit rate
        l3_total = self._current_run.cache_l3_hits + self._current_run.cache_l3_misses
        if l3_total > 0:
            self._current_run.cache_l3_hit_rate = self._current_run.cache_l3_hits / l3_total
    
    def get_current_metrics(self) -> Optional[PipelineRunMetrics]:
        """Get metrics for the current run (if active).
        
        Returns:
            Current PipelineRunMetrics or None if no active run
        """
        return self._current_run
    
    def get_run_history(self) -> List[PipelineRunMetrics]:
        """Get all historical run metrics.
        
        Returns:
            List of PipelineRunMetrics for all completed runs
        """
        return list(self._run_history)
    
    def get_latest_run(self) -> Optional[PipelineRunMetrics]:
        """Get metrics for the most recent completed run.
        
        Returns:
            PipelineRunMetrics for the latest run or None if no runs completed
        """
        if not self._run_history:
            return None
        return self._run_history[-1]
    
    def clear_history(self) -> None:
        """Clear all historical run metrics."""
        self._run_history.clear()
    
    def generate_summary_report(self) -> str:
        """Generate a human-readable summary report of the current or latest run.
        
        Returns:
            Formatted string report
        """
        metrics = self._current_run or self.get_latest_run()
        
        if metrics is None:
            return "No metrics available. Start a run with start_run()."
        
        lines = ["=" * 80]
        lines.append("PIPELINE RUN METRICS SUMMARY")
        lines.append("=" * 80)
        lines.append("")
        
        lines.append(f"Run ID: {metrics.run_id}")
        lines.append(f"Start Time: {metrics.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        if metrics.end_time:
            lines.append(f"End Time: {metrics.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Duration: {metrics.duration_seconds:.2f} seconds")
        lines.append("")
        
        lines.append("THROUGHPUT & LATENCY:")
        lines.append(f"  Products Processed: {metrics.products_processed:,}")
        lines.append(f"  Throughput: {metrics.throughput_products_per_second:.2f} products/sec")
        lines.append(f"  Latency: {metrics.latency_per_product_ms:.2f} ms/product")
        lines.append("")
        
        lines.append("ERROR METRICS:")
        lines.append(f"  Error Count: {metrics.error_count}")
        lines.append(f"  Error Rate: {metrics.error_rate * 100:.2f}%")
        lines.append("")
        
        lines.append("RESOURCE UTILIZATION:")
        lines.append(f"  CPU Utilization: {metrics.cpu_utilization_percent:.2f}%")
        lines.append(f"  Memory Peak: {metrics.memory_peak_mb:.2f} MB")
        lines.append(f"  Memory Average: {metrics.memory_avg_mb:.2f} MB")
        lines.append(f"  Disk Read: {metrics.disk_read_mb:.2f} MB")
        lines.append(f"  Disk Write: {metrics.disk_write_mb:.2f} MB")
        lines.append(f"  Network Sent: {metrics.network_sent_mb:.2f} MB")
        lines.append(f"  Network Received: {metrics.network_recv_mb:.2f} MB")
        lines.append("")
        
        lines.append("CACHE HIT RATES:")
        l1_total = metrics.cache_l1_hits + metrics.cache_l1_misses
        l2_total = metrics.cache_l2_hits + metrics.cache_l2_misses
        l3_total = metrics.cache_l3_hits + metrics.cache_l3_misses
        
        lines.append(f"  L1 (Memory): {metrics.cache_l1_hit_rate * 100:.2f}% "
                    f"({metrics.cache_l1_hits}/{l1_total} hits)")
        lines.append(f"  L2 (Redis):  {metrics.cache_l2_hit_rate * 100:.2f}% "
                    f"({metrics.cache_l2_hits}/{l2_total} hits)")
        lines.append(f"  L3 (File):   {metrics.cache_l3_hit_rate * 100:.2f}% "
                    f"({metrics.cache_l3_hits}/{l3_total} hits)")
        lines.append("")
        
        lines.append("DATA QUALITY:")
        lines.append(f"  Completeness: {metrics.data_completeness_percent:.2f}%")
        lines.append("")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)


# Global metrics collector instance
_global_metrics_collector: Optional[MetricsCollector] = None


def get_global_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance.
    
    Creates the instance if it doesn't exist.
    """
    global _global_metrics_collector
    if _global_metrics_collector is None:
        _global_metrics_collector = MetricsCollector()
    return _global_metrics_collector
