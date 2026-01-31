"""System resource monitoring for performance profiling.

This module provides background monitoring of system resources including
CPU, memory, disk I/O, network I/O, and connection pool statistics.
"""

import threading
import time
from dataclasses import dataclass
from datetime import datetime

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


@dataclass
class ResourceSnapshot:
    """System resource usage at a point in time.

    Attributes:
        timestamp: When the snapshot was taken
        cpu_percent: Overall CPU utilization percentage
        cpu_per_core: CPU utilization per core
        memory_rss_mb: Resident Set Size (physical memory)
        memory_vms_mb: Virtual Memory Size
        disk_read_mb: Cumulative disk read in MB
        disk_write_mb: Cumulative disk write in MB
        network_sent_mb: Cumulative network sent in MB
        network_recv_mb: Cumulative network received in MB
        connection_pool_active: Number of active database connections
        connection_pool_idle: Number of idle database connections
    """

    timestamp: datetime
    cpu_percent: float = 0.0
    cpu_per_core: list[float] = None
    memory_rss_mb: float = 0.0
    memory_vms_mb: float = 0.0
    disk_read_mb: float = 0.0
    disk_write_mb: float = 0.0
    network_sent_mb: float = 0.0
    network_recv_mb: float = 0.0
    connection_pool_active: int = 0
    connection_pool_idle: int = 0

    def __post_init__(self):
        if self.cpu_per_core is None:
            self.cpu_per_core = []

    def to_dict(self) -> dict:
        """Convert snapshot to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "cpu_percent": round(self.cpu_percent, 2),
            "cpu_per_core": [round(c, 2) for c in self.cpu_per_core],
            "memory_rss_mb": round(self.memory_rss_mb, 2),
            "memory_vms_mb": round(self.memory_vms_mb, 2),
            "disk_read_mb": round(self.disk_read_mb, 2),
            "disk_write_mb": round(self.disk_write_mb, 2),
            "network_sent_mb": round(self.network_sent_mb, 2),
            "network_recv_mb": round(self.network_recv_mb, 2),
            "connection_pool_active": self.connection_pool_active,
            "connection_pool_idle": self.connection_pool_idle,
        }


class ResourceMonitor:
    """Background system resource monitor.

    Monitors CPU, memory, disk I/O, network I/O, and connection pools
    at regular intervals in a background thread.

    Example:
        >>> monitor = ResourceMonitor()
        >>> monitor.start_monitoring(interval_seconds=1.0)
        >>> # ... run pipeline ...
        >>> monitor.stop_monitoring()
        >>> snapshots = monitor.get_snapshots()
        >>> print(f"Peak memory: {max(s.memory_rss_mb for s in snapshots)} MB")
    """

    def __init__(self):
        """Initialize the resource monitor."""
        if not PSUTIL_AVAILABLE:
            raise ImportError(
                "psutil is required for resource monitoring. Install with: pip install psutil"
            )

        import os

        self._process = psutil.Process(os.getpid())
        self._snapshots: list[ResourceSnapshot] = []
        self._monitoring = False
        self._monitor_thread: threading.Thread | None = None
        self._lock = threading.Lock()

        # Store initial I/O counters for delta calculation
        self._initial_disk_io = None
        self._initial_net_io = None

    def sample_resources(self) -> ResourceSnapshot:
        """Capture current resource usage.

        Returns:
            ResourceSnapshot with current system metrics
        """
        snapshot = ResourceSnapshot(timestamp=datetime.now())

        try:
            # CPU utilization
            snapshot.cpu_percent = self._process.cpu_percent()
            snapshot.cpu_per_core = psutil.cpu_percent(percpu=True)

            # Memory usage
            mem_info = self._process.memory_info()
            snapshot.memory_rss_mb = mem_info.rss / 1024 / 1024
            snapshot.memory_vms_mb = mem_info.vms / 1024 / 1024

            # Disk I/O
            try:
                disk_io = psutil.disk_io_counters()
                if disk_io:
                    if self._initial_disk_io is None:
                        self._initial_disk_io = disk_io

                    snapshot.disk_read_mb = (
                        (disk_io.read_bytes - self._initial_disk_io.read_bytes) / 1024 / 1024
                    )
                    snapshot.disk_write_mb = (
                        (disk_io.write_bytes - self._initial_disk_io.write_bytes) / 1024 / 1024
                    )
            except Exception:
                # Disk I/O counters may not be available on all platforms
                pass

            # Network I/O
            try:
                net_io = psutil.net_io_counters()
                if net_io:
                    if self._initial_net_io is None:
                        self._initial_net_io = net_io

                    snapshot.network_sent_mb = (
                        (net_io.bytes_sent - self._initial_net_io.bytes_sent) / 1024 / 1024
                    )
                    snapshot.network_recv_mb = (
                        (net_io.bytes_recv - self._initial_net_io.bytes_recv) / 1024 / 1024
                    )
            except Exception:
                # Network I/O counters may not be available on all platforms
                pass

            # Connection pool stats (if available)
            # This would need to be integrated with the actual connection pool
            # For now, we'll leave it at 0
            snapshot.connection_pool_active = 0
            snapshot.connection_pool_idle = 0

        except Exception:
            # If sampling fails, return snapshot with zeros
            pass

        return snapshot

    def start_monitoring(self, interval_seconds: float = 1.0) -> None:
        """Start background resource monitoring.

        Args:
            interval_seconds: Sampling interval in seconds
        """
        if self._monitoring:
            return

        self._monitoring = True
        self._snapshots.clear()

        # Reset I/O counters
        self._initial_disk_io = None
        self._initial_net_io = None

        def monitor_loop():
            while self._monitoring:
                snapshot = self.sample_resources()
                with self._lock:
                    self._snapshots.append(snapshot)
                time.sleep(interval_seconds)

        self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._monitor_thread.start()

    def stop_monitoring(self) -> None:
        """Stop background resource monitoring."""
        if not self._monitoring:
            return

        self._monitoring = False
        if self._monitor_thread and self._monitor_thread.is_alive():
            # Wait for thread to finish, but don't block forever
            self._monitor_thread.join(timeout=2.0)
            if self._monitor_thread.is_alive():
                # Thread didn't stop in time, but that's okay
                # It's a daemon thread so it will be killed when program exits
                pass
            self._monitor_thread = None

    def get_snapshots(self) -> list[ResourceSnapshot]:
        """Get all collected resource snapshots.

        Returns:
            List of ResourceSnapshot objects
        """
        # Use trylock to avoid deadlock if monitoring thread is holding the lock
        acquired = self._lock.acquire(blocking=True, timeout=1.0)
        if not acquired:
            # Could not acquire lock, return empty list
            return []

        try:
            return list(self._snapshots)
        finally:
            self._lock.release()

    def get_summary(self) -> dict:
        """Get summary statistics of resource usage.

        Returns:
            Dictionary with min, max, mean, and peak values
        """
        # Use trylock to avoid deadlock if monitoring thread is holding the lock
        acquired = self._lock.acquire(blocking=True, timeout=1.0)
        if not acquired:
            # Could not acquire lock, return empty dict
            return {}

        try:
            if not self._snapshots:
                return {}

            cpu_values = [s.cpu_percent for s in self._snapshots]
            memory_rss_values = [s.memory_rss_mb for s in self._snapshots]
            memory_vms_values = [s.memory_vms_mb for s in self._snapshots]

            return {
                "cpu_percent": {
                    "min": round(min(cpu_values), 2),
                    "max": round(max(cpu_values), 2),
                    "mean": round(sum(cpu_values) / len(cpu_values), 2),
                },
                "memory_rss_mb": {
                    "min": round(min(memory_rss_values), 2),
                    "max": round(max(memory_rss_values), 2),
                    "mean": round(sum(memory_rss_values) / len(memory_rss_values), 2),
                },
                "memory_vms_mb": {
                    "min": round(min(memory_vms_values), 2),
                    "max": round(max(memory_vms_values), 2),
                    "mean": round(sum(memory_vms_values) / len(memory_vms_values), 2),
                },
                "disk_read_mb": round(self._snapshots[-1].disk_read_mb, 2)
                if self._snapshots
                else 0.0,
                "disk_write_mb": round(self._snapshots[-1].disk_write_mb, 2)
                if self._snapshots
                else 0.0,
                "network_sent_mb": round(self._snapshots[-1].network_sent_mb, 2)
                if self._snapshots
                else 0.0,
                "network_recv_mb": round(self._snapshots[-1].network_recv_mb, 2)
                if self._snapshots
                else 0.0,
                "sample_count": len(self._snapshots),
            }
        finally:
            self._lock.release()

    def reset(self):
        """Reset all collected snapshots."""
        acquired = self._lock.acquire(blocking=True, timeout=1.0)
        if acquired:
            try:
                self._snapshots.clear()
            finally:
                self._lock.release()
        self._initial_disk_io = None
        self._initial_net_io = None

    def generate_report(self) -> str:
        """Generate a human-readable resource monitoring report.

        Returns:
            Formatted string report of resource usage
        """
        summary = self.get_summary()

        if not summary:
            return "No resource monitoring data collected."

        lines = ["=" * 80]
        lines.append("RESOURCE MONITORING REPORT")
        lines.append("=" * 80)
        lines.append("")

        lines.append(f"Samples collected: {summary['sample_count']}")
        lines.append("")

        lines.append("CPU USAGE:")
        lines.append(f"  Min:  {summary['cpu_percent']['min']:>8.2f}%")
        lines.append(f"  Max:  {summary['cpu_percent']['max']:>8.2f}%")
        lines.append(f"  Mean: {summary['cpu_percent']['mean']:>8.2f}%")
        lines.append("")

        lines.append("MEMORY USAGE (RSS):")
        lines.append(f"  Min:  {summary['memory_rss_mb']['min']:>10.2f} MB")
        lines.append(f"  Max:  {summary['memory_rss_mb']['max']:>10.2f} MB")
        lines.append(f"  Mean: {summary['memory_rss_mb']['mean']:>10.2f} MB")
        lines.append("")

        lines.append("DISK I/O:")
        lines.append(f"  Read:  {summary['disk_read_mb']:>10.2f} MB")
        lines.append(f"  Write: {summary['disk_write_mb']:>10.2f} MB")
        lines.append("")

        lines.append("NETWORK I/O:")
        lines.append(f"  Sent:     {summary['network_sent_mb']:>10.2f} MB")
        lines.append(f"  Received: {summary['network_recv_mb']:>10.2f} MB")
        lines.append("")

        lines.append("=" * 80)

        return "\n".join(lines)


# Global resource monitor instance
_global_resource_monitor: ResourceMonitor | None = None


def get_global_resource_monitor() -> ResourceMonitor:
    """Get the global resource monitor instance.

    Creates the instance if it doesn't exist.
    """
    global _global_resource_monitor
    if _global_resource_monitor is None:
        if PSUTIL_AVAILABLE:
            _global_resource_monitor = ResourceMonitor()
        else:
            raise ImportError(
                "psutil is required for resource monitoring. Install with: pip install psutil"
            )
    return _global_resource_monitor
