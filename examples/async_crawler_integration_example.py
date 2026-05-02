"""
Example demonstrating async crawler integration.

This example shows how to use the AsyncCrawlerIntegration to crawl products
with async I/O for improved performance.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.crawl.async_crawler_integration import get_async_crawler_integration

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def example_async_crawler():
    """Example: Crawl products using async crawler integration."""

    logger.info("=" * 60)
    logger.info("Async Crawler Integration Example")
    logger.info("=" * 60)

    # Example 1: Crawl category products with async mode enabled
    logger.info("\n1. Crawling category products (async mode enabled)...")

    with get_async_crawler_integration(use_async=True) as crawler:
        # Crawl a small category (e.g., category 1882 - Điện thoại)
        products = crawler.crawl_category_products(
            category_id="1882",
            max_pages=2,  # Just 2 pages for demo
            items_per_page=20,
        )

        logger.info(f"✅ Fetched {len(products)} products (async mode)")

        # Display first few products
        for i, product in enumerate(products[:3], 1):
            logger.info(f"   Product {i}: {product.get('name', 'N/A')[:50]}...")

    # Example 2: Crawl with async mode disabled (fallback to sync)
    logger.info("\n2. Crawling category products (async mode disabled)...")

    with get_async_crawler_integration(use_async=False) as crawler:
        products = crawler.crawl_category_products(
            category_id="1882", max_pages=1, items_per_page=10
        )

        logger.info(f"✅ Fetched {len(products)} products (sync fallback)")

    # Example 3: Crawl specific product IDs
    logger.info("\n3. Crawling specific product IDs (async mode)...")

    with get_async_crawler_integration(use_async=True) as crawler:
        product_ids = ["123456", "234567", "345678"]  # Example IDs
        products = crawler.crawl_products_by_ids(product_ids)

        logger.info(f"✅ Fetched {len(products)} products by ID")

    logger.info("\n" + "=" * 60)
    logger.info("Example completed successfully!")
    logger.info("=" * 60)


def example_performance_comparison():
    """Example: Compare async vs sync performance."""
    import time

    logger.info("=" * 60)
    logger.info("Performance Comparison: Async vs Sync")
    logger.info("=" * 60)

    category_id = "1882"
    max_pages = 3

    # Test async mode
    logger.info("\nTesting async mode...")
    start_time = time.time()

    with get_async_crawler_integration(use_async=True) as crawler:
        products_async = crawler.crawl_category_products(
            category_id=category_id, max_pages=max_pages
        )

    async_time = time.time() - start_time
    logger.info(f"✅ Async mode: {len(products_async)} products in {async_time:.2f}s")

    # Test sync mode
    logger.info("\nTesting sync mode...")
    start_time = time.time()

    with get_async_crawler_integration(use_async=False) as crawler:
        products_sync = crawler.crawl_category_products(
            category_id=category_id, max_pages=max_pages
        )

    sync_time = time.time() - start_time
    logger.info(f"✅ Sync mode: {len(products_sync)} products in {sync_time:.2f}s")

    # Compare
    if async_time > 0 and sync_time > 0:
        speedup = sync_time / async_time
        logger.info(f"\n📊 Performance improvement: {speedup:.2f}x faster with async mode")

    logger.info("\n" + "=" * 60)


if __name__ == "__main__":
    # Run basic example
    example_async_crawler()

    # Uncomment to run performance comparison
    # example_performance_comparison()
