"""
Configuration Analyzer for detecting system capabilities and recommending optimal settings.

This module provides functionality to:
- Detect CPU cores (physical and logical)
- Measure available memory
- Estimate disk I/O speed
- Estimate network bandwidth
- Generate optimal configuration recommendations
"""

import logging
import multiprocessing
import os
import platform
import time
from dataclasses import dataclass, field
from pathlib import Path

import psutil

logger = logging.getLogger(__name__)


@dataclass
class SystemCapabilities:
    """System capabilities detected from the machine."""

    # CPU information
    cpu_cores_physical: int
    cpu_cores_logical: int
    cpu_freq_mhz: float

    # Memory information
    total_memory_mb: float
    available_memory_mb: float

    # Disk I/O information
    disk_read_speed_mbps: float
    disk_write_speed_mbps: float

    # Network information
    network_bandwidth_mbps: float

    # Platform information
    platform: str
    python_version: str


@dataclass
class OptimizedConfig:
    """Recommended configuration values with rationale."""

    # Crawl configuration
    product_batch_size: int
    product_pool_size: int
    http_connector_limit: int
    http_timeout_total: int
    http_timeout_connect: int

    # Transform configuration
    thread_pool_size: int
    process_pool_size: int
    use_vectorization: bool

    # Load configuration
    db_connection_pool_min: int
    db_connection_pool_max: int
    db_batch_size: int
    use_copy_command: bool

    # Cache configuration
    redis_cache_ttl: int
    memory_cache_size_mb: int
    cache_eviction_threshold: float

    # Rationale for each recommendation
    rationale: dict[str, str] = field(default_factory=dict)


class ConfigurationAnalyzer:
    """Analyzes system capabilities and recommends optimal configuration."""

    def __init__(self):
        """Initialize the configuration analyzer."""
        self.logger = logging.getLogger(__name__)

    def analyze_system(self) -> SystemCapabilities:
        """Detect system capabilities.

        Returns:
            SystemCapabilities with detected values
        """
        self.logger.info("Analyzing system capabilities...")

        try:
            # CPU detection
            cpu_physical = psutil.cpu_count(logical=False) or multiprocessing.cpu_count()
            cpu_logical = psutil.cpu_count(logical=True) or multiprocessing.cpu_count()
            cpu_freq = psutil.cpu_freq()
            cpu_freq_mhz = cpu_freq.current if cpu_freq else 2000.0  # Default fallback

            # Memory detection
            memory = psutil.virtual_memory()
            total_memory_mb = memory.total / (1024 * 1024)
            available_memory_mb = memory.available / (1024 * 1024)

            # Disk I/O speed estimation
            disk_read_speed, disk_write_speed = self._estimate_disk_io_speed()

            # Network bandwidth estimation
            network_bandwidth = self._estimate_network_bandwidth()

            # Platform information
            platform_info = platform.platform()
            python_version = platform.python_version()

            capabilities = SystemCapabilities(
                cpu_cores_physical=cpu_physical,
                cpu_cores_logical=cpu_logical,
                cpu_freq_mhz=cpu_freq_mhz,
                total_memory_mb=total_memory_mb,
                available_memory_mb=available_memory_mb,
                disk_read_speed_mbps=disk_read_speed,
                disk_write_speed_mbps=disk_write_speed,
                network_bandwidth_mbps=network_bandwidth,
                platform=platform_info,
                python_version=python_version,
            )

            self.logger.info(
                f"System capabilities detected: {cpu_physical} physical cores, "
                f"{cpu_logical} logical cores, {total_memory_mb:.0f} MB total memory"
            )

            return capabilities

        except Exception as e:
            self.logger.error(f"Error detecting system capabilities: {e}")
            # Return conservative defaults on error
            return self._get_default_capabilities()

    def _estimate_disk_io_speed(self) -> tuple[float, float]:
        """Estimate disk I/O speed by performing a quick benchmark.

        Returns:
            Tuple of (read_speed_mbps, write_speed_mbps)
        """
        try:
            # Use a temporary file for testing
            test_file = Path("temp_io_test.bin")
            test_size_mb = 10  # 10 MB test
            test_data = os.urandom(test_size_mb * 1024 * 1024)

            # Measure write speed
            start_time = time.time()
            test_file.write_bytes(test_data)
            write_time = time.time() - start_time
            write_speed_mbps = test_size_mb / write_time if write_time > 0 else 100.0

            # Measure read speed
            start_time = time.time()
            _ = test_file.read_bytes()
            read_time = time.time() - start_time
            read_speed_mbps = test_size_mb / read_time if read_time > 0 else 100.0

            # Cleanup
            test_file.unlink()

            self.logger.debug(
                f"Disk I/O speed: read={read_speed_mbps:.1f} MB/s, "
                f"write={write_speed_mbps:.1f} MB/s"
            )

            return read_speed_mbps, write_speed_mbps

        except Exception as e:
            self.logger.warning(f"Could not estimate disk I/O speed: {e}")
            # Return conservative defaults (100 MB/s for both)
            return 100.0, 100.0

    def _estimate_network_bandwidth(self) -> float:
        """Estimate network bandwidth.

        Since we can't reliably test network bandwidth without external servers,
        we use heuristics based on network interface capabilities.

        Returns:
            Estimated bandwidth in Mbps
        """
        try:
            # Get network interface statistics
            net_if_stats = psutil.net_if_stats()

            # Find the fastest active interface (excluding loopback)
            max_speed = 0
            for interface, stats in net_if_stats.items():
                if stats.isup and interface != "lo" and stats.speed > 0:
                    max_speed = max(max_speed, stats.speed)

            if max_speed > 0:
                # Convert from Mbps to MB/s and assume 70% efficiency
                bandwidth_mbps = max_speed * 0.7 / 8
                self.logger.debug(f"Network bandwidth estimate: {bandwidth_mbps:.1f} MB/s")
                return bandwidth_mbps
            else:
                # Default to 10 MB/s (80 Mbps) for typical home connection
                return 10.0

        except Exception as e:
            self.logger.warning(f"Could not estimate network bandwidth: {e}")
            return 10.0

    def _get_default_capabilities(self) -> SystemCapabilities:
        """Return conservative default capabilities when detection fails.

        Returns:
            SystemCapabilities with safe default values
        """
        return SystemCapabilities(
            cpu_cores_physical=2,
            cpu_cores_logical=4,
            cpu_freq_mhz=2000.0,
            total_memory_mb=4096.0,
            available_memory_mb=2048.0,
            disk_read_speed_mbps=100.0,
            disk_write_speed_mbps=100.0,
            network_bandwidth_mbps=10.0,
            platform=platform.platform(),
            python_version=platform.python_version(),
        )

    def recommend_config(
        self, system: SystemCapabilities | None = None, profile_data: dict | None = None
    ) -> OptimizedConfig:
        """Generate optimized configuration based on system capabilities.

        Args:
            system: System capabilities (auto-detected if not provided)
            profile_data: Optional profiling data to inform recommendations

        Returns:
            OptimizedConfig with recommended values and rationale
        """
        if system is None:
            system = self.analyze_system()

        rationale = {}

        # Calculate optimal batch sizes based on memory
        # Use 10% of available memory for batch processing
        memory_for_batch_mb = system.available_memory_mb * 0.1
        # Assume ~1KB per product on average
        product_batch_size = int(memory_for_batch_mb * 1024)
        product_batch_size = max(100, min(10000, product_batch_size))  # Clamp to reasonable range
        rationale["product_batch_size"] = (
            f"Based on {system.available_memory_mb:.0f} MB available memory, "
            f"allocating 10% for batch processing allows {product_batch_size} products per batch"
        )

        # Calculate optimal pool sizes based on CPU cores
        # For I/O-bound tasks, use 2x logical cores
        product_pool_size = system.cpu_cores_logical * 2
        product_pool_size = max(4, min(50, product_pool_size))
        rationale["product_pool_size"] = (
            f"With {system.cpu_cores_logical} logical cores, using 2x for I/O-bound tasks "
            f"gives {product_pool_size} workers"
        )

        # Calculate optimal connector limits based on network
        # Higher bandwidth allows more concurrent connections
        if system.network_bandwidth_mbps >= 50:
            http_connector_limit = 300
        elif system.network_bandwidth_mbps >= 20:
            http_connector_limit = 200
        else:
            http_connector_limit = 100
        rationale["http_connector_limit"] = (
            f"Network bandwidth of {system.network_bandwidth_mbps:.1f} MB/s "
            f"supports {http_connector_limit} concurrent connections"
        )

        # Timeout configuration
        http_timeout_total = 15
        http_timeout_connect = 10
        rationale["http_timeout"] = (
            "Standard timeouts: 15s total, 10s connect for reliable operation"
        )

        # Thread pool size for I/O-bound transform operations
        thread_pool_size = system.cpu_cores_logical * 2
        thread_pool_size = max(4, min(32, thread_pool_size))
        rationale["thread_pool_size"] = (
            f"For I/O-bound transformations, {thread_pool_size} threads "
            f"(2x {system.cpu_cores_logical} logical cores)"
        )

        # Process pool size for CPU-bound operations
        process_pool_size = system.cpu_cores_physical
        process_pool_size = max(2, min(16, process_pool_size))
        rationale["process_pool_size"] = (
            f"For CPU-bound operations, {process_pool_size} processes "
            f"(matching {system.cpu_cores_physical} physical cores)"
        )

        # Vectorization recommendation
        use_vectorization = system.total_memory_mb >= 2048
        rationale["use_vectorization"] = (
            f"Vectorization {'enabled' if use_vectorization else 'disabled'} "
            f"based on {system.total_memory_mb:.0f} MB total memory"
        )

        # Database connection pool sizing
        # Min: 2, Max: based on CPU cores and expected query concurrency
        db_connection_pool_min = 2
        db_connection_pool_max = system.cpu_cores_logical + 4
        db_connection_pool_max = max(4, min(20, db_connection_pool_max))
        rationale["db_connection_pool"] = (
            f"Connection pool: {db_connection_pool_min}-{db_connection_pool_max} "
            f"based on {system.cpu_cores_logical} logical cores"
        )

        # Database batch size
        # Larger batches for faster disk I/O
        if system.disk_write_speed_mbps >= 200:
            db_batch_size = 5000
        elif system.disk_write_speed_mbps >= 100:
            db_batch_size = 2000
        else:
            db_batch_size = 1000
        rationale["db_batch_size"] = (
            f"Batch size {db_batch_size} optimized for disk write speed "
            f"of {system.disk_write_speed_mbps:.1f} MB/s"
        )

        # Use COPY command for bulk inserts (always recommended for local PostgreSQL)
        use_copy_command = True
        rationale["use_copy_command"] = "COPY command enabled for optimal bulk insert performance"

        # Cache configuration
        redis_cache_ttl = 3600  # 1 hour default
        rationale["redis_cache_ttl"] = "1 hour TTL balances freshness and cache hit rate"

        # Memory cache size: use 5% of available memory
        memory_cache_size_mb = int(system.available_memory_mb * 0.05)
        memory_cache_size_mb = max(50, min(500, memory_cache_size_mb))
        rationale["memory_cache_size_mb"] = (
            f"Memory cache size {memory_cache_size_mb} MB "
            f"(5% of {system.available_memory_mb:.0f} MB available memory)"
        )

        cache_eviction_threshold = 0.9
        rationale["cache_eviction_threshold"] = "Eviction at 90% capacity prevents memory pressure"

        config = OptimizedConfig(
            product_batch_size=product_batch_size,
            product_pool_size=product_pool_size,
            http_connector_limit=http_connector_limit,
            http_timeout_total=http_timeout_total,
            http_timeout_connect=http_timeout_connect,
            thread_pool_size=thread_pool_size,
            process_pool_size=process_pool_size,
            use_vectorization=use_vectorization,
            db_connection_pool_min=db_connection_pool_min,
            db_connection_pool_max=db_connection_pool_max,
            db_batch_size=db_batch_size,
            use_copy_command=use_copy_command,
            redis_cache_ttl=redis_cache_ttl,
            memory_cache_size_mb=memory_cache_size_mb,
            cache_eviction_threshold=cache_eviction_threshold,
            rationale=rationale,
        )

        self.logger.info("Generated optimized configuration recommendations")
        return config

    def validate_config(
        self, config: OptimizedConfig, system: SystemCapabilities | None = None
    ) -> tuple[bool, list[str]]:
        """Validate that configuration values are within acceptable ranges.

        Args:
            config: Configuration to validate
            system: System capabilities (auto-detected if not provided)

        Returns:
            Tuple of (is_valid, list of warnings/errors)
        """
        if system is None:
            system = self.analyze_system()

        warnings = []
        is_valid = True

        # Validate batch sizes
        if config.product_batch_size < 10 or config.product_batch_size > 50000:
            warnings.append(
                f"product_batch_size {config.product_batch_size} is outside "
                f"recommended range [10, 50000]"
            )
            is_valid = False

        # Check if batch size fits in available memory
        estimated_memory_mb = config.product_batch_size * 0.001  # ~1KB per product
        if estimated_memory_mb > system.available_memory_mb * 0.5:
            warnings.append(
                f"product_batch_size {config.product_batch_size} may use "
                f"{estimated_memory_mb:.0f} MB, exceeding 50% of available memory "
                f"({system.available_memory_mb:.0f} MB)"
            )

        # Validate pool sizes
        if config.product_pool_size < 1 or config.product_pool_size > 100:
            warnings.append(
                f"product_pool_size {config.product_pool_size} is outside "
                f"recommended range [1, 100]"
            )
            is_valid = False

        if config.thread_pool_size < 1 or config.thread_pool_size > 64:
            warnings.append(
                f"thread_pool_size {config.thread_pool_size} is outside recommended range [1, 64]"
            )
            is_valid = False

        if config.process_pool_size < 1 or config.process_pool_size > 32:
            warnings.append(
                f"process_pool_size {config.process_pool_size} is outside recommended range [1, 32]"
            )
            is_valid = False

        # Warn if process pool exceeds physical cores
        if config.process_pool_size > system.cpu_cores_physical:
            warnings.append(
                f"process_pool_size {config.process_pool_size} exceeds "
                f"physical cores ({system.cpu_cores_physical}), may cause contention"
            )

        # Validate connection limits
        if config.http_connector_limit < 10 or config.http_connector_limit > 1000:
            warnings.append(
                f"http_connector_limit {config.http_connector_limit} is outside "
                f"recommended range [10, 1000]"
            )
            is_valid = False

        # Validate timeouts
        if config.http_timeout_total < 1 or config.http_timeout_total > 300:
            warnings.append(
                f"http_timeout_total {config.http_timeout_total} is outside "
                f"recommended range [1, 300] seconds"
            )
            is_valid = False

        if config.http_timeout_connect < 1 or config.http_timeout_connect > 60:
            warnings.append(
                f"http_timeout_connect {config.http_timeout_connect} is outside "
                f"recommended range [1, 60] seconds"
            )
            is_valid = False

        if config.http_timeout_connect > config.http_timeout_total:
            warnings.append(
                f"http_timeout_connect ({config.http_timeout_connect}) should not exceed "
                f"http_timeout_total ({config.http_timeout_total})"
            )
            is_valid = False

        # Validate database configuration
        if config.db_connection_pool_min < 1 or config.db_connection_pool_min > 50:
            warnings.append(
                f"db_connection_pool_min {config.db_connection_pool_min} is outside "
                f"recommended range [1, 50]"
            )
            is_valid = False

        if config.db_connection_pool_max < config.db_connection_pool_min:
            warnings.append(
                f"db_connection_pool_max ({config.db_connection_pool_max}) should be >= "
                f"db_connection_pool_min ({config.db_connection_pool_min})"
            )
            is_valid = False

        if config.db_connection_pool_max > 100:
            warnings.append(
                f"db_connection_pool_max {config.db_connection_pool_max} exceeds "
                f"recommended maximum of 100"
            )

        if config.db_batch_size < 100 or config.db_batch_size > 10000:
            warnings.append(
                f"db_batch_size {config.db_batch_size} is outside recommended range [100, 10000]"
            )
            is_valid = False

        # Validate cache configuration
        if config.redis_cache_ttl < 60 or config.redis_cache_ttl > 86400:
            warnings.append(
                f"redis_cache_ttl {config.redis_cache_ttl} is outside "
                f"recommended range [60, 86400] seconds"
            )

        if config.memory_cache_size_mb < 10 or config.memory_cache_size_mb > 2000:
            warnings.append(
                f"memory_cache_size_mb {config.memory_cache_size_mb} is outside "
                f"recommended range [10, 2000] MB"
            )
            is_valid = False

        # Check if memory cache fits in available memory
        if config.memory_cache_size_mb > system.available_memory_mb * 0.3:
            warnings.append(
                f"memory_cache_size_mb {config.memory_cache_size_mb} exceeds 30% of "
                f"available memory ({system.available_memory_mb:.0f} MB)"
            )

        if config.cache_eviction_threshold < 0.5 or config.cache_eviction_threshold > 1.0:
            warnings.append(
                f"cache_eviction_threshold {config.cache_eviction_threshold} is outside "
                f"recommended range [0.5, 1.0]"
            )
            is_valid = False

        if is_valid and not warnings:
            self.logger.info("Configuration validation passed with no warnings")
        elif is_valid:
            self.logger.warning(f"Configuration validation passed with {len(warnings)} warnings")
        else:
            self.logger.error(f"Configuration validation failed with {len(warnings)} errors")

        return is_valid, warnings
