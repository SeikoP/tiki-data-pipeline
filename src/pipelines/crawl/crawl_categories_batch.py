"""
Batch category crawler with driver pooling for Airflow DAG
Optimized for parallel category crawling with driver reuse
"""

import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Import existing crawlers
try:
    from .crawl_products import crawl_category_products
    from .extract_category_link_selenium import (
        crawl_with_driver,
        crawl_with_selenium,
        parse_categories,
    )
    from .utils import SeleniumDriverPool
except ImportError:
    # Fallback for direct execution
    import importlib.util
    import os

    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Load extract_category_link_selenium (guard spec None for mypy)
    spec = importlib.util.spec_from_file_location(
        "extract_category_link_selenium",
        os.path.join(current_dir, "extract_category_link_selenium.py"),
    )
    if spec and spec.loader:
        extract_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(extract_module)
    else:  # type: ignore[unreachable]
        raise ImportError("Cannot load extract_category_link_selenium module spec") from None
    crawl_with_selenium = extract_module.crawl_with_selenium
    crawl_with_driver = getattr(extract_module, "crawl_with_driver", None)
    parse_categories = extract_module.parse_categories

    # Load crawl_products
    spec = importlib.util.spec_from_file_location(
        "crawl_products",
        os.path.join(current_dir, "crawl_products.py"),
    )
    if spec and spec.loader:
        products_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(products_module)
    else:  # type: ignore[unreachable]
        raise ImportError("Cannot load crawl_products module spec") from None
    crawl_category_products = products_module.crawl_category_products

    # Load utils
    spec = importlib.util.spec_from_file_location(
        "crawl_utils",
        os.path.join(current_dir, "utils.py"),
    )
    if spec and spec.loader:
        utils_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(utils_module)
        SeleniumDriverPool = getattr(utils_module, "SeleniumDriverPool", None)
    else:  # type: ignore[unreachable]
        SeleniumDriverPool = None


def crawl_single_category_with_pool(
    category: dict[str, Any],
    driver_pool: Any = None,
    max_pages: int = 20,
    use_selenium: bool = False,
    cache_dir: str = "data/raw/cache",
    use_redis_cache: bool = True,
    use_rate_limiting: bool = True,
    verbose: bool = False,
) -> dict[str, Any]:
    """
    Crawl một category với driver pool support

    Args:
        category: Category info dict with url, name, id
        driver_pool: Optional SeleniumDriverPool for reuse
        max_pages: Max pages to crawl
        use_selenium: Use Selenium for dynamic content
        cache_dir: Cache directory
        use_redis_cache: Use Redis cache
        use_rate_limiting: Use rate limiting
        verbose: Verbose logging

    Returns:
        Dict with products and metadata
    """
    category_url = category.get("url", "")
    category_name = category.get("name", "Unknown")
    category_id = category.get("id", "")

    result = {
        "category_id": category_id,
        "category_name": category_name,
        "category_url": category_url,
        "products": [],
        "status": "failed",
        "error": None,
        "crawled_at": datetime.now().isoformat(),
        "pages_crawled": 0,
        "products_count": 0,
    }

    try:
        # Crawl products from category
        products = crawl_category_products(
            category_url,
            max_pages=max_pages if max_pages > 0 else None,
            use_selenium=use_selenium,
            cache_dir=cache_dir,
            use_redis_cache=use_redis_cache,
            use_rate_limiting=use_rate_limiting,
        )

        result["products"] = products
        result["status"] = "success"
        result["products_count"] = len(products)

        if verbose:
            logger.info(f"✅ {category_name}: {len(products)} products")

    except Exception as e:
        result["error"] = str(e)
        result["status"] = "failed"
        if verbose:
            logger.error(f"❌ {category_name}: {e}")

    return result


def crawl_category_batch(
    category_batch: list[dict[str, Any]],
    batch_index: int = -1,
    max_pages: int = 20,
    use_selenium: bool = False,
    pool_size: int = 3,
    **kwargs,
) -> list[dict[str, Any]]:
    """
    Crawl batch categories với driver pooling

    Args:
        category_batch: List of category dicts
        batch_index: Batch index for logging
        max_pages: Max pages per category
        use_selenium: Use Selenium
        pool_size: Driver pool size

    Returns:
        List of results for each category
    """
    if not category_batch:
        logger.warning(f"⚠️  BATCH {batch_index} RỖNG")
        return []

    logger.info(f"📦 BATCH {batch_index}: Crawl {len(category_batch)} categories")
    logger.info(f"   - Categories: {[c.get('name', 'unknown') for c in category_batch[:3]]}")
    if len(category_batch) > 3:
        logger.info(f"   - ... và {len(category_batch) - 3} categories nữa")

    results = []
    driver_pool = None

    try:
        # Initialize driver pool if Selenium is used
        if use_selenium and SeleniumDriverPool is not None:
            driver_pool = SeleniumDriverPool(pool_size=pool_size, headless=True, timeout=90)  # type: ignore[call-arg]
            logger.info(f"✅ Driver pool initialized with {pool_size} drivers")

        # Crawl each category in batch
        for category in category_batch:
            result = crawl_single_category_with_pool(
                category=category,
                driver_pool=driver_pool,
                max_pages=max_pages,
                use_selenium=use_selenium,
                use_redis_cache=True,
                use_rate_limiting=True,
                verbose=True,
            )
            results.append(result)

        # Stats
        success_count = sum(1 for r in results if r.get("status") == "success")
        failed_count = len(results) - success_count

        logger.info(f"✅ Batch {batch_index} hoàn thành:")
        logger.info(f"   - Success: {success_count}/{len(category_batch)}")
        logger.info(f"   - Failed: {failed_count}/{len(category_batch)}")

    except Exception as e:
        logger.error(f"❌ Lỗi khi crawl batch {batch_index}: {e}", exc_info=True)
        # Return failed results for all
        results.extend(
            [
                {
                    "category_id": category.get("id", "unknown"),
                    "category_name": category.get("name", "Unknown"),
                    "category_url": category.get("url", ""),
                    "products": [],
                    "status": "failed",
                    "error": f"Batch error: {str(e)}",
                    "crawled_at": datetime.now().isoformat(),
                    "pages_crawled": 0,
                    "products_count": 0,
                }
                for category in category_batch
            ]
        )

    finally:
        # Cleanup driver pool
        if driver_pool is not None:
            try:
                driver_pool.cleanup()
                logger.info("✅ Driver pool cleaned up")
            except Exception:
                pass

    return results


__all__ = [
    "crawl_single_category_with_pool",
    "crawl_category_batch",
]
