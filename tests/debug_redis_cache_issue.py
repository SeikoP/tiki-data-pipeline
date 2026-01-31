"""Debug why Redis cache is not being hit"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def debug_cache_key_generation():
    """Debug cache key generation"""
    print("\n" + "=" * 70)
    print("DEBUG: Cache Key Generation")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_redis_cache

    cache = get_redis_cache("redis://localhost:6379/1")
    if not cache:
        print("❌ Failed to create cache")
        return

    test_url = "https://tiki.vn/test-priority/c99999"

    # Generate key
    key = cache.get_cache_key(test_url, "products")
    full_key = cache._make_key(key)

    print(f"\nTest URL: {test_url}")
    print(f"Cache key: {key}")
    print(f"Full Redis key: {full_key}")

    # Check if key exists
    exists = cache.exists(key)
    print(f"Key exists in Redis: {exists}")

    # Try to get value
    value = cache.get(key)
    print(f"Value from cache: {value is not None}")

    if value:
        print(f"Cached data keys: {list(value.keys())}")
        if "products" in value:
            print(f"Number of products: {len(value['products'])}")

    # Check with get_cached_products
    products = cache.get_cached_products(test_url)
    print(f"\nget_cached_products result: {products is not None}")
    if products:
        print(f"Number of products: {len(products)}")

    # List all keys in Redis
    print("\n" + "=" * 70)
    print("All keys in Redis DB 1:")
    print("=" * 70)
    try:
        all_keys = cache.client.keys("tiki:crawl:*")
        print(f"Total keys: {len(all_keys)}")
        for k in all_keys[:10]:
            print(f"  - {k}")
    except Exception as e:
        print(f"Error listing keys: {e}")


def debug_crawl_function():
    """Debug crawl_category_products function"""
    print("\n" + "=" * 70)
    print("DEBUG: crawl_category_products Function")
    print("=" * 70)

    import logging

    # Enable debug logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("pipelines.crawl.crawl_products")
    logger.setLevel(logging.DEBUG)

    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.crawl.storage.redis_cache import get_redis_cache

    test_url = "https://tiki.vn/test-priority/c99999"

    # Pre-populate cache
    cache = get_redis_cache("redis://localhost:6379/1")
    test_products = [
        {"product_id": "1", "name": "Test 1"},
        {"product_id": "2", "name": "Test 2"},
    ]
    cache.cache_products(test_url, test_products, ttl=3600)
    print(f"✅ Pre-populated cache with {len(test_products)} products")

    # Verify cache
    cached = cache.get_cached_products(test_url)
    print(f"✅ Verified cache: {len(cached) if cached else 0} products")

    # Now call crawl function
    print("\n" + "-" * 70)
    print("Calling crawl_category_products...")
    print("-" * 70)

    products = crawl_category_products(
        test_url,
        use_redis_cache=True,
        use_rate_limiting=False,
        force_refresh=False,
        logger=logger,
    )

    print("-" * 70)
    print(f"Result: {len(products) if products else 0} products")
    print("-" * 70)


if __name__ == "__main__":
    debug_cache_key_generation()
    debug_crawl_function()
