"""Optimized batch loader with PostgreSQL COPY and batch upsert.

This module provides an optimized loader that uses:
- PostgreSQL COPY command for bulk inserts (fastest method)
- Batch upsert with execute_values for updates
- Automatic selection between COPY and upsert based on operation type
"""

import csv
import io
import logging
from datetime import datetime
from typing import Any

try:
    from psycopg2.extras import execute_values

    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

logger = logging.getLogger(__name__)


class OptimizedBatchLoader:
    """Loads data using optimized batch operations.

    Uses PostgreSQL COPY for bulk inserts and batch upsert for updates.
    Automatically selects the optimal method based on operation type.
    """

    def __init__(self, connection_pool, batch_size: int = 1000, use_copy_for_inserts: bool = True):
        """Initialize loader with connection pool.

        Args:
            connection_pool: PostgreSQL connection pool
            batch_size: Number of records per batch (default: 1000)
            use_copy_for_inserts: Use COPY command for insert-only operations
        """
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 not installed. Install: pip install psycopg2-binary")

        self.pool = connection_pool
        self.batch_size = batch_size
        self.use_copy_for_inserts = use_copy_for_inserts

        # Statistics
        self.stats = {
            "total_processed": 0,
            "inserted": 0,
            "updated": 0,
            "failed": 0,
            "copy_operations": 0,
            "upsert_operations": 0,
        }

    def load_products(self, products: list[dict[str, Any]], upsert: bool = True) -> dict[str, Any]:
        """Load products using optimized batch operations.

        Args:
            products: List of product dictionaries to load
            upsert: If True, use INSERT ON CONFLICT UPDATE; if False, use INSERT only

        Returns:
            Dictionary with statistics (total_processed, inserted, updated, failed)
        """
        if not products:
            logger.warning("No products to load")
            return self.stats

        # Reset stats
        self.stats = {
            "total_processed": 0,
            "inserted": 0,
            "updated": 0,
            "failed": 0,
            "copy_operations": 0,
            "upsert_operations": 0,
        }

        # Process in batches
        for i in range(0, len(products), self.batch_size):
            batch = products[i : i + self.batch_size]

            try:
                with self.pool.get_cursor(commit=True) as cursor:
                    if self._is_insert_only(batch) and not upsert and self.use_copy_for_inserts:
                        # Use COPY for insert-only operations
                        rows_affected = self._copy_insert(cursor, batch)
                        self.stats["copy_operations"] += 1
                    else:
                        # Use batch upsert for mixed operations or when upsert is enabled
                        rows_affected = self._batch_upsert(cursor, batch, upsert)
                        self.stats["upsert_operations"] += 1

                    self.stats["total_processed"] += len(batch)
                    self.stats["inserted"] += rows_affected

            except Exception as e:
                logger.error(f"Failed to load batch: {e}")
                self.stats["failed"] += len(batch)

        logger.info(
            f"Loaded {self.stats['total_processed']} products "
            f"(COPY: {self.stats['copy_operations']}, "
            f"Upsert: {self.stats['upsert_operations']}, "
            f"Failed: {self.stats['failed']})"
        )

        return self.stats

    def _is_insert_only(self, products: list[dict[str, Any]]) -> bool:
        """Check if all products are new inserts (no existing records).

        For simplicity, we assume insert-only if products don't have an 'updated_at' field
        or if all products are marked as new. In production, this would query the database
        to check for existing records.

        Args:
            products: List of product dictionaries

        Returns:
            True if all products are new inserts, False otherwise
        """
        # Simple heuristic: if no products have 'updated_at', assume insert-only
        # In production, you'd query the database to check for existing records
        return all(not p.get("updated_at") for p in products)

    def _copy_insert(self, cursor, products: list[dict[str, Any]]) -> int:
        """Use COPY command for fastest bulk insert.

        COPY bypasses much of PostgreSQL's overhead and is the fastest
        method for bulk loading.

        Args:
            cursor: Database cursor
            products: List of product dictionaries

        Returns:
            Number of rows inserted
        """
        # Define column order for COPY
        columns = [
            "product_id",
            "category_url",
            "category_id",
            "name",
            "short_name",
            "url",
            "price",
            "original_price",
            "discount_percent",
            "rating_average",
            "review_count",
            "sales_count",
            "brand",
            "seller_name",
            "seller_id",
            "seller_is_official",
            "crawled_at",
            "updated_at",
            "crawl_quality",
            "needs_enrichment",
        ]

        # Create CSV in memory
        buffer = io.StringIO()
        writer = csv.writer(buffer)

        for product in products:
            row = [
                product.get("product_id"),
                product.get("category_url"),
                product.get("category_id"),
                product.get("name"),
                product.get("short_name"),
                product.get("url"),
                product.get("price"),
                product.get("original_price"),
                product.get("discount_percent"),
                product.get("rating_average"),
                product.get("review_count"),
                product.get("sales_count"),
                product.get("brand"),
                product.get("seller_name"),
                product.get("seller_id"),
                product.get("seller_is_official"),
                product.get("crawled_at") or datetime.now(),
                datetime.now(),
                product.get("crawl_quality", "basic"),
                product.get("needs_enrichment", False),
            ]
            writer.writerow(row)

        # Reset buffer position
        buffer.seek(0)

        # Use COPY command
        try:
            cursor.copy_from(buffer, "products", sep=",", columns=columns, null="")
            return len(products)
        except Exception as e:
            logger.error(f"COPY command failed: {e}")
            raise

    def _batch_upsert(self, cursor, products: list[dict[str, Any]], upsert: bool = True) -> int:
        """Use batch INSERT ... ON CONFLICT for upserts.

        Uses execute_values for efficient batch insert with optional
        conflict resolution.

        Args:
            cursor: Database cursor
            products: List of product dictionaries
            upsert: If True, use ON CONFLICT UPDATE; if False, use INSERT only

        Returns:
            Number of rows affected
        """
        if upsert:
            query = """
                INSERT INTO products (
                    product_id, category_url, category_id, name, short_name, url,
                    price, original_price, discount_percent, rating_average,
                    review_count, sales_count, brand, seller_name, seller_id,
                    seller_is_official, crawled_at, updated_at, crawl_quality,
                    needs_enrichment
                ) VALUES %s
                ON CONFLICT (product_id) DO UPDATE SET
                    category_url = COALESCE(EXCLUDED.category_url, products.category_url),
                    category_id = COALESCE(EXCLUDED.category_id, products.category_id),
                    name = COALESCE(NULLIF(EXCLUDED.name, ''), products.name),
                    short_name = COALESCE(NULLIF(EXCLUDED.short_name, ''), products.short_name),
                    url = COALESCE(NULLIF(EXCLUDED.url, ''), products.url),
                    price = COALESCE(EXCLUDED.price, products.price),
                    original_price = COALESCE(EXCLUDED.original_price, products.original_price),
                    discount_percent = COALESCE(EXCLUDED.discount_percent, products.discount_percent),
                    rating_average = COALESCE(EXCLUDED.rating_average, products.rating_average),
                    review_count = COALESCE(EXCLUDED.review_count, products.review_count),
                    sales_count = COALESCE(EXCLUDED.sales_count, products.sales_count),
                    brand = COALESCE(NULLIF(EXCLUDED.brand, ''), products.brand),
                    seller_name = COALESCE(NULLIF(EXCLUDED.seller_name, ''), products.seller_name),
                    seller_id = COALESCE(NULLIF(EXCLUDED.seller_id, ''), products.seller_id),
                    seller_is_official = COALESCE(EXCLUDED.seller_is_official, products.seller_is_official),
                    updated_at = EXCLUDED.updated_at,
                    crawl_quality = EXCLUDED.crawl_quality,
                    needs_enrichment = EXCLUDED.needs_enrichment
            """
        else:
            query = """
                INSERT INTO products (
                    product_id, category_url, category_id, name, short_name, url,
                    price, original_price, discount_percent, rating_average,
                    review_count, sales_count, brand, seller_name, seller_id,
                    seller_is_official, crawled_at, updated_at, crawl_quality,
                    needs_enrichment
                ) VALUES %s
            """

        # Prepare values
        values = [
            (
                p.get("product_id"),
                p.get("category_url"),
                p.get("category_id"),
                p.get("name"),
                p.get("short_name"),
                p.get("url"),
                p.get("price"),
                p.get("original_price"),
                p.get("discount_percent"),
                p.get("rating_average"),
                p.get("review_count"),
                p.get("sales_count"),
                p.get("brand"),
                p.get("seller_name"),
                p.get("seller_id"),
                p.get("seller_is_official"),
                p.get("crawled_at") or datetime.now(),
                datetime.now(),
                p.get("crawl_quality", "basic"),
                p.get("needs_enrichment", False),
            )
            for p in products
        ]

        # Execute batch insert/upsert
        execute_values(cursor, query, values, page_size=self.batch_size)

        return len(products)

    def get_stats(self) -> dict[str, Any]:
        """Get loading statistics.

        Returns:
            Dictionary with loading statistics
        """
        return self.stats.copy()


__all__ = ["OptimizedBatchLoader"]
