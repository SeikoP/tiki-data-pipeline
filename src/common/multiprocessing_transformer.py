"""
Multiprocessing transformer for CPU-bound tasks.

This module provides a MultiprocessingTransformer that uses multiprocessing
to bypass Python's GIL (Global Interpreter Lock) for true parallelism on
CPU-bound transformation tasks.
"""

import logging
import multiprocessing
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class MultiprocessingTransformer:
    """
    Transformer that uses multiprocessing for CPU-bound transformations.

    Bypasses Python's GIL to achieve true parallelism for CPU-intensive tasks.
    Uses chunking to minimize inter-process communication overhead.
    """

    def __init__(self, num_processes: int | None = None):
        """
        Initialize MultiprocessingTransformer.

        Args:
            num_processes: Number of worker processes. Defaults to CPU count.
        """
        self.num_processes = num_processes or multiprocessing.cpu_count()
        self.pool: multiprocessing.Pool | None = None
        logger.info(f"Initialized MultiprocessingTransformer with {self.num_processes} processes")

    def __enter__(self):
        """Context manager entry - initialize pool."""
        self._initialize_pool()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup pool."""
        self.close()

    def _initialize_pool(self) -> None:
        """Initialize the multiprocessing pool."""
        if self.pool is None:
            self.pool = multiprocessing.Pool(processes=self.num_processes)
            logger.debug(f"Created multiprocessing pool with {self.num_processes} workers")

    def transform_parallel(
        self,
        products: list[dict[str, Any]],
        transform_func: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Apply transformation function in parallel across processes.

        Args:
            products: List of product dictionaries to transform
            transform_func: Function to apply to each product

        Returns:
            List of transformed products

        Note:
            The transform_func must be picklable (defined at module level or as a lambda
            that doesn't capture local variables) for inter-process communication.
        """
        if not products:
            logger.warning("Empty product list provided")
            return []

        # Initialize pool if not already done
        if self.pool is None:
            self._initialize_pool()

        # Calculate optimal chunk size
        # Use 4x the number of processes to ensure good load balancing
        chunk_size = max(1, len(products) // (self.num_processes * 4))

        logger.debug(
            f"Processing {len(products)} products with chunk_size={chunk_size} "
            f"across {self.num_processes} processes"
        )

        try:
            # Map transform function across products with chunking
            # chunksize parameter minimizes inter-process communication overhead
            transformed = self.pool.map(transform_func, products, chunksize=chunk_size)

            # Filter out None results (failed transformations)
            transformed = [p for p in transformed if p is not None]

            logger.info(
                f"Successfully transformed {len(transformed)}/{len(products)} products "
                f"using multiprocessing"
            )

            return transformed

        except Exception as e:
            logger.error(f"Error in parallel transformation: {e}")
            raise

    def transform_parallel_batched(
        self,
        products: list[dict[str, Any]],
        transform_func: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
        batch_size: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Apply batch transformation function in parallel across processes.

        This is useful when the transform function can process multiple items
        more efficiently than processing them one at a time.

        Args:
            products: List of product dictionaries to transform
            transform_func: Function that takes a batch and returns transformed batch
            batch_size: Size of batches to process

        Returns:
            List of transformed products
        """
        if not products:
            logger.warning("Empty product list provided")
            return []

        # Initialize pool if not already done
        if self.pool is None:
            self._initialize_pool()

        # Split products into batches
        batches = [products[i : i + batch_size] for i in range(0, len(products), batch_size)]

        logger.debug(
            f"Processing {len(products)} products in {len(batches)} batches "
            f"of size {batch_size} across {self.num_processes} processes"
        )

        try:
            # Map transform function across batches
            # Each process handles one or more batches
            transformed_batches = self.pool.map(transform_func, batches)

            # Flatten results
            transformed = []
            for batch in transformed_batches:
                if batch:
                    transformed.extend(batch)

            logger.info(
                f"Successfully transformed {len(transformed)}/{len(products)} products "
                f"using batched multiprocessing"
            )

            return transformed

        except Exception as e:
            logger.error(f"Error in batched parallel transformation: {e}")
            raise

    def close(self) -> None:
        """
        Close the multiprocessing pool and cleanup resources.

        Should be called when done with transformations to free resources.
        """
        if self.pool is not None:
            logger.debug("Closing multiprocessing pool")
            self.pool.close()
            self.pool.join()
            self.pool = None

    def __del__(self):
        """Destructor - ensure pool is closed."""
        self.close()


# Module-level helper functions for common transformations
# These are defined at module level to be picklable for multiprocessing


def _transform_product_simple(product: dict[str, Any]) -> dict[str, Any] | None:
    """
    Simple product transformation function (example).

    This is a module-level function that can be pickled for multiprocessing.
    Replace with actual transformation logic as needed.
    """
    try:
        if not product:
            return None

        # Example transformation
        transformed = product.copy()

        # Normalize product_id
        if "product_id" in transformed:
            transformed["product_id"] = str(transformed["product_id"]).strip()

        # Normalize name
        if "name" in transformed and transformed["name"]:
            transformed["name"] = " ".join(str(transformed["name"]).split())

        return transformed

    except Exception as e:
        logger.error(f"Error transforming product: {e}")
        return None


def _transform_batch_simple(products: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Simple batch transformation function (example).

    This is a module-level function that can be pickled for multiprocessing.
    Replace with actual batch transformation logic as needed.
    """
    try:
        transformed = []
        for product in products:
            result = _transform_product_simple(product)
            if result:
                transformed.append(result)
        return transformed

    except Exception as e:
        logger.error(f"Error transforming batch: {e}")
        return []
