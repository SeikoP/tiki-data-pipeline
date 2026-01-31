"""Integration module for optimized batch loader.

This module provides integration between the OptimizedBatchLoader and the
existing pipeline, with profiling support and connection pool tuning.
"""

import logging
from typing import Any

from .connection_pool_tuner import ConnectionPoolTuner
from .optimized_batch_loader import OptimizedBatchLoader
from .profiling import profile_loader

logger = logging.getLogger(__name__)


class IntegratedOptimizedLoader:
    """Integrated optimized loader with profiling and pool tuning.

    This class wraps OptimizedBatchLoader and adds:
    - Connection pool tuning based on workload
    - Profiling integration
    - Backward compatibility with existing loader interface
    """

    def __init__(
        self,
        connection_pool,
        batch_size: int = 1000,
        use_copy_for_inserts: bool = True,
        enable_profiling: bool = True,
        enable_pool_tuning: bool = True,
    ):
        """Initialize integrated optimized loader.

        Args:
            connection_pool: PostgreSQL connection pool
            batch_size: Number of records per batch
            use_copy_for_inserts: Use COPY command for insert-only operations
            enable_profiling: Enable database operation profiling
            enable_pool_tuning: Enable automatic pool size tuning
        """
        self.pool = connection_pool
        self.batch_size = batch_size
        self.enable_profiling = enable_profiling
        self.enable_pool_tuning = enable_pool_tuning

        # Initialize optimized loader
        self.loader = OptimizedBatchLoader(
            connection_pool=connection_pool,
            batch_size=batch_size,
            use_copy_for_inserts=use_copy_for_inserts,
        )

        # Initialize pool tuner
        if enable_pool_tuning:
            self.pool_tuner = ConnectionPoolTuner()
        else:
            self.pool_tuner = None

        # Statistics
        self.stats = {
            "total_loads": 0,
            "total_products": 0,
            "total_time_ms": 0,
            "avg_time_per_product_ms": 0,
            "pool_recommendations": [],
        }

    @profile_loader
    def load_products(self, products: list[dict[str, Any]], upsert: bool = True) -> dict[str, Any]:
        """Load products using optimized batch operations.

        This method is compatible with the existing loader interface but uses
        the optimized implementation under the hood.

        Args:
            products: List of product dictionaries to load
            upsert: If True, use INSERT ON CONFLICT UPDATE; if False, use INSERT only

        Returns:
            Dictionary with statistics
        """
        if not products:
            logger.warning("No products to load")
            return {"total_processed": 0, "inserted": 0, "updated": 0, "failed": 0}

        # Load using optimized loader
        result = self.loader.load_products(products, upsert=upsert)

        # Update statistics
        self.stats["total_loads"] += 1
        self.stats["total_products"] += result.get("total_processed", 0)

        return result

    def tune_pool_size(self, avg_query_time_ms: float, queries_per_second: float) -> dict[str, Any]:
        """Tune connection pool size based on workload.

        Args:
            avg_query_time_ms: Average query execution time in milliseconds
            queries_per_second: Expected query rate

        Returns:
            Dictionary with pool size recommendations
        """
        if not self.pool_tuner:
            logger.warning("Pool tuning is disabled")
            return {}

        recommendation = self.pool_tuner.recommend_based_on_system(
            queries_per_second=queries_per_second, avg_query_time_ms=avg_query_time_ms
        )

        self.stats["pool_recommendations"].append(recommendation)

        logger.info(
            f"Pool size recommendation: "
            f"min={recommendation['recommended_min']}, "
            f"max={recommendation['recommended_max']}"
        )

        return recommendation

    def get_stats(self) -> dict[str, Any]:
        """Get loading statistics.

        Returns:
            Dictionary with loading statistics
        """
        loader_stats = self.loader.get_stats()

        return {**self.stats, "loader_stats": loader_stats}

    def validate_pool_config(self, min_conn: int, max_conn: int) -> dict[str, Any]:
        """Validate connection pool configuration.

        Args:
            min_conn: Minimum connections
            max_conn: Maximum connections

        Returns:
            Dictionary with validation results
        """
        if not self.pool_tuner:
            logger.warning("Pool tuning is disabled")
            return {"valid": True, "errors": [], "warnings": []}

        return self.pool_tuner.validate_pool_config(min_conn, max_conn)


def create_optimized_loader(
    connection_pool,
    batch_size: int = 1000,
    use_copy_for_inserts: bool = True,
    enable_profiling: bool = True,
    enable_pool_tuning: bool = True,
) -> IntegratedOptimizedLoader:
    """Factory function to create an integrated optimized loader.

    Args:
        connection_pool: PostgreSQL connection pool
        batch_size: Number of records per batch
        use_copy_for_inserts: Use COPY command for insert-only operations
        enable_profiling: Enable database operation profiling
        enable_pool_tuning: Enable automatic pool size tuning

    Returns:
        IntegratedOptimizedLoader instance
    """
    return IntegratedOptimizedLoader(
        connection_pool=connection_pool,
        batch_size=batch_size,
        use_copy_for_inserts=use_copy_for_inserts,
        enable_profiling=enable_profiling,
        enable_pool_tuning=enable_pool_tuning,
    )


__all__ = ["IntegratedOptimizedLoader", "create_optimized_loader"]
