"""
Example demonstrating optimized batch loader integration.

This example shows how to use the OptimizedLoaderIntegration to load products
with optimized batch operations for improved database performance.
"""

import logging
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.load.optimized_loader_integration import get_optimized_loader_integration

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def create_sample_products(count: int = 100):
    """Create sample products for testing."""
    from datetime import datetime

    products = []

    for i in range(count):
        product = {
            "product_id": str(1000000 + i),
            "name": f"Sample Product {i}",
            "short_name": f"Product {i}",
            "url": f"https://tiki.vn/product-{i}.html",
            "category_url": "https://tiki.vn/category-1.html",
            "category_id": "1882",
            "price": 100000 + (i * 1000),
            "original_price": 150000 + (i * 1000),
            "discount_percent": 33,
            "rating_average": 4.5,
            "review_count": 100 + i,
            "sales_count": 500 + (i * 10),
            "brand": "Sample Brand",
            "seller_name": f"Seller {i}",
            "seller_id": str(5000 + i),
            "seller_is_official": i % 2 == 0,
            "crawled_at": datetime.now().isoformat(),
            "crawl_quality": "basic",
            "needs_enrichment": False,
        }
        products.append(product)

    return products


def example_optimized_loader():
    """Example: Load products using optimized batch loader."""

    logger.info("=" * 60)
    logger.info("Optimized Batch Loader Integration Example")
    logger.info("=" * 60)

    # Create sample products
    products = create_sample_products(100)
    logger.info(f"\nCreated {len(products)} sample products")

    # Example 1: Load with optimized mode enabled (requires database)
    logger.info("\n1. Loading with optimized mode enabled...")
    logger.info("   (Note: This requires a PostgreSQL database connection)")

    try:
        with get_optimized_loader_integration(
            use_optimized=True, enable_db=True, show_progress=True
        ) as loader:
            stats = loader.load_products(products, upsert=True)

            logger.info(f"✅ Loaded {stats['db_loaded']} products (optimized mode)")
            logger.info(f"   Stats: {stats}")
    except Exception as e:
        logger.warning(f"⚠️  Database loading failed (expected if DB not configured): {e}")

    # Example 2: Load to file only (no database)
    logger.info("\n2. Loading to file only (no database)...")

    output_file = "data/test_output/optimized_loader_example.json"

    with get_optimized_loader_integration(
        use_optimized=False, enable_db=False, show_progress=True
    ) as loader:
        stats = loader.load_products(products, upsert=True, save_to_file=output_file)

        logger.info(f"✅ Saved {stats['file_loaded']} products to file")
        logger.info(f"   File: {output_file}")

    logger.info("\n" + "=" * 60)
    logger.info("Example completed successfully!")
    logger.info("=" * 60)


def example_performance_comparison():
    """Example: Compare optimized vs traditional loader performance."""

    logger.info("=" * 60)
    logger.info("Performance Comparison: Optimized vs Traditional Loader")
    logger.info("=" * 60)
    logger.info("\n(Note: This requires a PostgreSQL database connection)")

    # Test with different dataset sizes
    sizes = [100, 500, 1000]

    for size in sizes:
        logger.info(f"\nTesting with {size} products...")
        products = create_sample_products(size)

        try:
            # Test optimized mode
            start_time = time.time()
            with get_optimized_loader_integration(
                use_optimized=True, enable_db=True, show_progress=False
            ) as loader:
                stats_opt = loader.load_products(products, upsert=True)
            optimized_time = time.time() - start_time

            logger.info(f"  Optimized: {stats_opt['db_loaded']} products in {optimized_time:.3f}s")

            # Test traditional mode
            start_time = time.time()
            with get_optimized_loader_integration(
                use_optimized=False, enable_db=True, show_progress=False
            ) as loader:
                stats_trad = loader.load_products(products, upsert=True)
            traditional_time = time.time() - start_time

            logger.info(
                f"  Traditional: {stats_trad['db_loaded']} products in {traditional_time:.3f}s"
            )

            # Compare
            if optimized_time > 0 and traditional_time > 0:
                speedup = traditional_time / optimized_time
                logger.info(f"  📊 Speedup: {speedup:.2f}x faster with optimized mode")

        except Exception as e:
            logger.warning(f"⚠️  Performance test failed (expected if DB not configured): {e}")
            break

    logger.info("\n" + "=" * 60)


def example_batch_size_tuning():
    """Example: Test different batch sizes."""

    logger.info("=" * 60)
    logger.info("Batch Size Tuning Example")
    logger.info("=" * 60)
    logger.info("\n(Note: This requires a PostgreSQL database connection)")

    products = create_sample_products(1000)
    batch_sizes = [100, 500, 1000, 2000]

    for batch_size in batch_sizes:
        logger.info(f"\nTesting batch_size={batch_size}...")

        try:
            start_time = time.time()
            with get_optimized_loader_integration(
                use_optimized=True, batch_size=batch_size, enable_db=True, show_progress=False
            ) as loader:
                stats = loader.load_products(products, upsert=True)

            elapsed = time.time() - start_time
            rate = stats["db_loaded"] / elapsed if elapsed > 0 else 0

            logger.info(f"  Loaded {stats['db_loaded']} products in {elapsed:.3f}s")
            logger.info(f"  Rate: {rate:.1f} products/second")

        except Exception as e:
            logger.warning(f"⚠️  Test failed (expected if DB not configured): {e}")
            break

    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    # Run basic example
    example_optimized_loader()

    # Uncomment to run performance comparison (requires database)
    # example_performance_comparison()

    # Uncomment to run batch size tuning (requires database)
    # example_batch_size_tuning()
