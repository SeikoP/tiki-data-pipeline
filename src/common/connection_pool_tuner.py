"""Connection pool tuner for dynamic pool sizing.

This module provides utilities to calculate optimal connection pool sizes
based on workload characteristics and system resources.
"""

import logging
import os
from typing import Any

import psutil

logger = logging.getLogger(__name__)


class ConnectionPoolTuner:
    """Dynamically tunes connection pool size based on workload.

    Uses workload characteristics and system resources to recommend
    optimal connection pool sizes.
    """

    def __init__(
        self, min_connections: int = 2, max_connections: int = 100, target_utilization: float = 0.8
    ):
        """Initialize connection pool tuner.

        Args:
            min_connections: Minimum allowed connections (default: 2)
            max_connections: Maximum allowed connections (default: 100)
            target_utilization: Target connection utilization 0.0-1.0 (default: 0.8)
        """
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.target_utilization = target_utilization

    def recommend_pool_size(
        self, avg_query_time_ms: float, queries_per_second: float, target_utilization: float = None
    ) -> int:
        """Calculate optimal pool size based on workload.

        Formula: pool_size = (queries_per_second * avg_query_time_ms / 1000) / target_utilization

        This formula calculates how many concurrent connections are needed to handle
        the expected query rate, given the average query execution time.

        Args:
            avg_query_time_ms: Average query execution time in milliseconds
            queries_per_second: Expected query rate (queries per second)
            target_utilization: Target connection utilization (0.0-1.0)
                              If None, uses instance default

        Returns:
            Recommended pool size (bounded by min/max)
        """
        if target_utilization is None:
            target_utilization = self.target_utilization

        # Validate inputs
        if avg_query_time_ms <= 0:
            logger.warning(f"Invalid avg_query_time_ms: {avg_query_time_ms}, using default")
            avg_query_time_ms = 100  # Default 100ms

        if queries_per_second <= 0:
            logger.warning(f"Invalid queries_per_second: {queries_per_second}, using default")
            queries_per_second = 10  # Default 10 qps

        if not 0 < target_utilization <= 1:
            logger.warning(f"Invalid target_utilization: {target_utilization}, using default")
            target_utilization = self.target_utilization

        # Calculate required connections
        # queries_per_second * avg_query_time_ms / 1000 = concurrent queries
        # concurrent queries / target_utilization = pool size needed
        required_connections = (queries_per_second * avg_query_time_ms / 1000) / target_utilization

        # Round up and apply bounds
        pool_size = max(self.min_connections, int(required_connections + 0.5))
        pool_size = min(pool_size, self.max_connections)

        logger.info(
            f"Recommended pool size: {pool_size} "
            f"(qps={queries_per_second:.1f}, "
            f"avg_time={avg_query_time_ms:.1f}ms, "
            f"utilization={target_utilization:.1%})"
        )

        return pool_size

    def recommend_based_on_system(
        self, queries_per_second: float = None, avg_query_time_ms: float = None
    ) -> dict[str, Any]:
        """Recommend pool size based on system resources.

        If workload parameters are not provided, estimates them based on
        system capabilities.

        Args:
            queries_per_second: Expected query rate (optional)
            avg_query_time_ms: Average query time (optional)

        Returns:
            Dictionary with recommendations and rationale
        """
        # Get system resources
        cpu_count = os.cpu_count() or 4
        memory_gb = psutil.virtual_memory().total / (1024**3)

        # Estimate workload if not provided
        if queries_per_second is None:
            # Estimate based on CPU cores (conservative: 5 queries per core per second)
            queries_per_second = cpu_count * 5
            logger.info(
                f"Estimated queries_per_second: {queries_per_second} (based on {cpu_count} CPU cores)"
            )

        if avg_query_time_ms is None:
            # Assume moderate query complexity (50ms average)
            avg_query_time_ms = 50
            logger.info(f"Estimated avg_query_time_ms: {avg_query_time_ms}ms")

        # Calculate recommended pool size
        recommended_size = self.recommend_pool_size(
            avg_query_time_ms=avg_query_time_ms, queries_per_second=queries_per_second
        )

        # Calculate max based on system resources
        # Rule of thumb: 1 connection per 100MB RAM, capped by CPU cores * 10
        max_by_memory = int(memory_gb * 10)  # 10 connections per GB
        max_by_cpu = cpu_count * 10
        system_max = min(max_by_memory, max_by_cpu, self.max_connections)

        # Adjust if recommended exceeds system capacity
        if recommended_size > system_max:
            logger.warning(
                f"Recommended size {recommended_size} exceeds system capacity {system_max}, "
                f"capping at {system_max}"
            )
            recommended_size = system_max

        return {
            "recommended_min": max(self.min_connections, recommended_size // 2),
            "recommended_max": recommended_size,
            "system_max": system_max,
            "queries_per_second": queries_per_second,
            "avg_query_time_ms": avg_query_time_ms,
            "target_utilization": self.target_utilization,
            "rationale": {
                "cpu_cores": cpu_count,
                "memory_gb": round(memory_gb, 2),
                "formula": f"({queries_per_second} qps * {avg_query_time_ms}ms / 1000) / {self.target_utilization}",
                "calculation": f"{recommended_size} connections needed for target utilization",
            },
        }

    def validate_pool_config(self, min_conn: int, max_conn: int) -> dict[str, Any]:
        """Validate connection pool configuration.

        Args:
            min_conn: Minimum connections
            max_conn: Maximum connections

        Returns:
            Dictionary with validation results and warnings
        """
        warnings = []
        errors = []

        # Check bounds
        if min_conn < 1:
            errors.append(f"min_conn must be >= 1, got {min_conn}")

        if max_conn < min_conn:
            errors.append(f"max_conn ({max_conn}) must be >= min_conn ({min_conn})")

        if min_conn < self.min_connections:
            warnings.append(
                f"min_conn ({min_conn}) is below recommended minimum ({self.min_connections})"
            )

        if max_conn > self.max_connections:
            warnings.append(
                f"max_conn ({max_conn}) exceeds recommended maximum ({self.max_connections})"
            )

        # Check system resources
        cpu_count = os.cpu_count() or 4
        memory_gb = psutil.virtual_memory().total / (1024**3)

        max_by_cpu = cpu_count * 10
        if max_conn > max_by_cpu:
            warnings.append(
                f"max_conn ({max_conn}) may be too high for {cpu_count} CPU cores "
                f"(recommended max: {max_by_cpu})"
            )

        max_by_memory = int(memory_gb * 10)
        if max_conn > max_by_memory:
            warnings.append(
                f"max_conn ({max_conn}) may be too high for {memory_gb:.1f}GB RAM "
                f"(recommended max: {max_by_memory})"
            )

        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": warnings,
            "system_info": {
                "cpu_cores": cpu_count,
                "memory_gb": round(memory_gb, 2),
                "recommended_max_by_cpu": max_by_cpu,
                "recommended_max_by_memory": max_by_memory,
            },
        }


__all__ = ["ConnectionPoolTuner"]
