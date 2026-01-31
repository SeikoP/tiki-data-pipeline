"""Environmental condition recorder for benchmark reproducibility.

This module records environmental conditions (CPU load, memory, network) at
benchmark start to enable analysis of external factors that may affect results.
"""

import platform
import psutil
import socket
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import subprocess
import time


@dataclass
class EnvironmentalConditions:
    """Environmental conditions at benchmark start.
    
    Attributes:
        recorded_at: When conditions were recorded
        system_info: System information (OS, Python version, etc.)
        cpu_info: CPU information and current load
        memory_info: Memory information and availability
        disk_info: Disk information and I/O stats
        network_info: Network information and conditions
        process_info: Information about the current process
    """
    
    recorded_at: datetime
    system_info: Dict
    cpu_info: Dict
    memory_info: Dict
    disk_info: Dict
    network_info: Dict
    process_info: Dict
    
    def to_dict(self) -> Dict:
        """Convert conditions to dictionary for serialization."""
        return {
            "recorded_at": self.recorded_at.isoformat(),
            "system_info": self.system_info,
            "cpu_info": self.cpu_info,
            "memory_info": self.memory_info,
            "disk_info": self.disk_info,
            "network_info": self.network_info,
            "process_info": self.process_info,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "EnvironmentalConditions":
        """Create EnvironmentalConditions from dictionary."""
        return cls(
            recorded_at=datetime.fromisoformat(data["recorded_at"]),
            system_info=data["system_info"],
            cpu_info=data["cpu_info"],
            memory_info=data["memory_info"],
            disk_info=data["disk_info"],
            network_info=data["network_info"],
            process_info=data["process_info"],
        )
    
    def format_summary(self) -> str:
        """Generate a human-readable summary of environmental conditions.
        
        Returns:
            Formatted string summary
        """
        lines = ["=" * 80]
        lines.append("ENVIRONMENTAL CONDITIONS")
        lines.append("=" * 80)
        lines.append("")
        
        lines.append(f"Recorded At: {self.recorded_at.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        
        # System info
        lines.append("SYSTEM INFORMATION:")
        lines.append(f"  OS: {self.system_info.get('os', 'Unknown')}")
        lines.append(f"  OS Version: {self.system_info.get('os_version', 'Unknown')}")
        lines.append(f"  Python Version: {self.system_info.get('python_version', 'Unknown')}")
        lines.append(f"  Hostname: {self.system_info.get('hostname', 'Unknown')}")
        lines.append("")
        
        # CPU info
        lines.append("CPU INFORMATION:")
        lines.append(f"  Physical Cores: {self.cpu_info.get('physical_cores', 'Unknown')}")
        lines.append(f"  Logical Cores: {self.cpu_info.get('logical_cores', 'Unknown')}")
        lines.append(f"  CPU Frequency: {self.cpu_info.get('cpu_freq_mhz', 'Unknown')} MHz")
        lines.append(f"  Current Load: {self.cpu_info.get('cpu_percent', 0):.1f}%")
        lines.append(f"  Load Average (1m): {self.cpu_info.get('load_avg_1m', 'N/A')}")
        lines.append("")
        
        # Memory info
        lines.append("MEMORY INFORMATION:")
        lines.append(f"  Total: {self.memory_info.get('total_mb', 0):.0f} MB")
        lines.append(f"  Available: {self.memory_info.get('available_mb', 0):.0f} MB")
        lines.append(f"  Used: {self.memory_info.get('used_mb', 0):.0f} MB")
        lines.append(f"  Usage: {self.memory_info.get('percent', 0):.1f}%")
        lines.append("")
        
        # Disk info
        lines.append("DISK INFORMATION:")
        lines.append(f"  Total: {self.disk_info.get('total_gb', 0):.1f} GB")
        lines.append(f"  Free: {self.disk_info.get('free_gb', 0):.1f} GB")
        lines.append(f"  Usage: {self.disk_info.get('percent', 0):.1f}%")
        lines.append("")
        
        # Network info
        lines.append("NETWORK INFORMATION:")
        lines.append(f"  Hostname: {self.network_info.get('hostname', 'Unknown')}")
        lines.append(f"  IP Address: {self.network_info.get('ip_address', 'Unknown')}")
        lines.append("")
        
        # Process info
        lines.append("PROCESS INFORMATION:")
        lines.append(f"  PID: {self.process_info.get('pid', 'Unknown')}")
        lines.append(f"  Memory RSS: {self.process_info.get('memory_rss_mb', 0):.1f} MB")
        lines.append(f"  CPU Percent: {self.process_info.get('cpu_percent', 0):.1f}%")
        lines.append(f"  Threads: {self.process_info.get('num_threads', 0)}")
        lines.append("")
        
        lines.append("=" * 80)
        
        return "\n".join(lines)


class EnvironmentRecorder:
    """Records environmental conditions for benchmark reproducibility.
    
    Captures system state at benchmark start including CPU load, memory
    availability, disk space, and network conditions. This enables analysis
    of external factors that may affect benchmark results.
    
    Example:
        >>> recorder = EnvironmentRecorder()
        >>> conditions = recorder.record_conditions()
        >>> print(conditions.format_summary())
        >>> # Include in benchmark report
        >>> baseline_system.capture_baseline(
        ...     baseline_id="baseline_001",
        ...     description="Baseline run",
        ...     metrics=metrics,
        ...     environment=conditions.to_dict()
        ... )
    """
    
    def __init__(self):
        """Initialize the environment recorder."""
        pass
    
    def record_conditions(self) -> EnvironmentalConditions:
        """Record current environmental conditions.
        
        Returns:
            EnvironmentalConditions with all recorded data
        """
        return EnvironmentalConditions(
            recorded_at=datetime.now(),
            system_info=self._record_system_info(),
            cpu_info=self._record_cpu_info(),
            memory_info=self._record_memory_info(),
            disk_info=self._record_disk_info(),
            network_info=self._record_network_info(),
            process_info=self._record_process_info(),
        )
    
    def _record_system_info(self) -> Dict:
        """Record system information.
        
        Returns:
            Dictionary with system info
        """
        return {
            "os": platform.system(),
            "os_version": platform.version(),
            "os_release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "python_version": platform.python_version(),
            "hostname": socket.gethostname(),
        }
    
    def _record_cpu_info(self) -> Dict:
        """Record CPU information and current load.
        
        Returns:
            Dictionary with CPU info
        """
        cpu_info = {
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
        }
        
        # CPU frequency
        try:
            cpu_freq = psutil.cpu_freq()
            if cpu_freq:
                cpu_info["cpu_freq_mhz"] = cpu_freq.current
                cpu_info["cpu_freq_min_mhz"] = cpu_freq.min
                cpu_info["cpu_freq_max_mhz"] = cpu_freq.max
        except Exception:
            cpu_info["cpu_freq_mhz"] = None
        
        # Current CPU load (sample over 1 second)
        cpu_info["cpu_percent"] = psutil.cpu_percent(interval=1.0)
        cpu_info["cpu_percent_per_core"] = psutil.cpu_percent(interval=0.1, percpu=True)
        
        # Load average (Unix-like systems only)
        try:
            load_avg = psutil.getloadavg()
            cpu_info["load_avg_1m"] = load_avg[0]
            cpu_info["load_avg_5m"] = load_avg[1]
            cpu_info["load_avg_15m"] = load_avg[2]
        except (AttributeError, OSError):
            # Not available on Windows
            cpu_info["load_avg_1m"] = None
            cpu_info["load_avg_5m"] = None
            cpu_info["load_avg_15m"] = None
        
        return cpu_info
    
    def _record_memory_info(self) -> Dict:
        """Record memory information and availability.
        
        Returns:
            Dictionary with memory info
        """
        mem = psutil.virtual_memory()
        
        return {
            "total_mb": mem.total / (1024 * 1024),
            "available_mb": mem.available / (1024 * 1024),
            "used_mb": mem.used / (1024 * 1024),
            "free_mb": mem.free / (1024 * 1024),
            "percent": mem.percent,
            "cached_mb": getattr(mem, 'cached', 0) / (1024 * 1024),
            "buffers_mb": getattr(mem, 'buffers', 0) / (1024 * 1024),
        }
    
    def _record_disk_info(self) -> Dict:
        """Record disk information and I/O stats.
        
        Returns:
            Dictionary with disk info
        """
        # Get disk usage for current directory
        disk = psutil.disk_usage('.')
        
        disk_info = {
            "total_gb": disk.total / (1024 * 1024 * 1024),
            "used_gb": disk.used / (1024 * 1024 * 1024),
            "free_gb": disk.free / (1024 * 1024 * 1024),
            "percent": disk.percent,
        }
        
        # Get disk I/O counters
        try:
            io_counters = psutil.disk_io_counters()
            if io_counters:
                disk_info["read_count"] = io_counters.read_count
                disk_info["write_count"] = io_counters.write_count
                disk_info["read_bytes"] = io_counters.read_bytes
                disk_info["write_bytes"] = io_counters.write_bytes
        except Exception:
            pass
        
        return disk_info
    
    def _record_network_info(self) -> Dict:
        """Record network information and conditions.
        
        Returns:
            Dictionary with network info
        """
        network_info = {
            "hostname": socket.gethostname(),
        }
        
        # Get IP address
        try:
            # Try to get external IP by connecting to a public DNS
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            network_info["ip_address"] = s.getsockname()[0]
            s.close()
        except Exception:
            network_info["ip_address"] = "127.0.0.1"
        
        # Get network I/O counters
        try:
            io_counters = psutil.net_io_counters()
            if io_counters:
                network_info["bytes_sent"] = io_counters.bytes_sent
                network_info["bytes_recv"] = io_counters.bytes_recv
                network_info["packets_sent"] = io_counters.packets_sent
                network_info["packets_recv"] = io_counters.packets_recv
        except Exception:
            pass
        
        # Test network latency to common endpoint
        network_info["network_latency_ms"] = self._measure_network_latency()
        
        return network_info
    
    def _measure_network_latency(self, host: str = "8.8.8.8", timeout: float = 2.0) -> Optional[float]:
        """Measure network latency to a host.
        
        Args:
            host: Host to ping
            timeout: Timeout in seconds
        
        Returns:
            Latency in milliseconds or None if failed
        """
        try:
            start = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, 80))
            s.close()
            latency_ms = (time.time() - start) * 1000
            return latency_ms
        except Exception:
            return None
    
    def _record_process_info(self) -> Dict:
        """Record information about the current process.
        
        Returns:
            Dictionary with process info
        """
        process = psutil.Process()
        
        process_info = {
            "pid": process.pid,
            "name": process.name(),
            "status": process.status(),
            "create_time": process.create_time(),
            "num_threads": process.num_threads(),
        }
        
        # Memory info
        try:
            mem_info = process.memory_info()
            process_info["memory_rss_mb"] = mem_info.rss / (1024 * 1024)
            process_info["memory_vms_mb"] = mem_info.vms / (1024 * 1024)
        except Exception:
            pass
        
        # CPU percent (sample over short interval)
        try:
            process_info["cpu_percent"] = process.cpu_percent(interval=0.1)
        except Exception:
            process_info["cpu_percent"] = 0.0
        
        # Open files and connections
        try:
            process_info["num_fds"] = process.num_fds() if hasattr(process, 'num_fds') else None
            process_info["num_connections"] = len(process.connections())
        except Exception:
            pass
        
        return process_info
    
    def compare_conditions(
        self,
        conditions1: EnvironmentalConditions,
        conditions2: EnvironmentalConditions
    ) -> Dict:
        """Compare two sets of environmental conditions.
        
        Args:
            conditions1: First set of conditions
            conditions2: Second set of conditions
        
        Returns:
            Dictionary with comparison results and differences
        """
        comparison = {
            "time_difference_seconds": (
                conditions2.recorded_at - conditions1.recorded_at
            ).total_seconds(),
            "differences": [],
        }
        
        # Compare CPU load
        cpu1 = conditions1.cpu_info.get("cpu_percent", 0)
        cpu2 = conditions2.cpu_info.get("cpu_percent", 0)
        cpu_diff = abs(cpu2 - cpu1)
        if cpu_diff > 10:  # More than 10% difference
            comparison["differences"].append(
                f"CPU load differs by {cpu_diff:.1f}% ({cpu1:.1f}% vs {cpu2:.1f}%)"
            )
        
        # Compare memory availability
        mem1 = conditions1.memory_info.get("available_mb", 0)
        mem2 = conditions2.memory_info.get("available_mb", 0)
        mem_diff_percent = abs(mem2 - mem1) / mem1 * 100 if mem1 > 0 else 0
        if mem_diff_percent > 10:  # More than 10% difference
            comparison["differences"].append(
                f"Available memory differs by {mem_diff_percent:.1f}% "
                f"({mem1:.0f} MB vs {mem2:.0f} MB)"
            )
        
        # Compare disk space
        disk1 = conditions1.disk_info.get("free_gb", 0)
        disk2 = conditions2.disk_info.get("free_gb", 0)
        disk_diff_percent = abs(disk2 - disk1) / disk1 * 100 if disk1 > 0 else 0
        if disk_diff_percent > 10:  # More than 10% difference
            comparison["differences"].append(
                f"Free disk space differs by {disk_diff_percent:.1f}% "
                f"({disk1:.1f} GB vs {disk2:.1f} GB)"
            )
        
        # Check if conditions are similar
        comparison["conditions_similar"] = len(comparison["differences"]) == 0
        
        return comparison


# Global environment recorder instance
_global_environment_recorder: Optional[EnvironmentRecorder] = None


def get_global_environment_recorder() -> EnvironmentRecorder:
    """Get the global environment recorder instance.
    
    Creates the instance if it doesn't exist.
    """
    global _global_environment_recorder
    if _global_environment_recorder is None:
        _global_environment_recorder = EnvironmentRecorder()
    return _global_environment_recorder
