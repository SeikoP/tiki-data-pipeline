"""Test full Redis cache flow trong Airflow environment"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_redis_from_airflow_context():
    """Test Redis cache như trong Airflow tasks"""
    print("\n" + "=" * 70)
    print("TEST: Redis Cache in Airflow Context")
    print("=" * 70)

    # Simulate Airflow environment
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    # 1. Check Redis connection (như trong Airflow)
    print("\n1. Checking Redis connection from Airflow context...")
    redis_url = get_working_redis_url(db=1, timeout=2)
    if not redis_url:
        print("❌ Redis not available")
        return False

    print(f"✅ Redis URL: {redis_url}")

    # 2. Test với real category
    test_url = "https://tiki.vn/bo-quan-ao-nam-kich-co-lon/c49530"
    print(f"\n2. Testing with real category: {test_url}")

    # Clear cache first
    cache = get_redis_cache(redis_url)
    if cache:
        key = cache.get_cache_key(test_url, "products")
        cache.delete(key)
        print("✅ Cleared existing cache")

    # First call - should crawl and cache
    print("\n3. First call (should crawl and cache)...")
    import time

    start = time.time()
    products1 = crawl_category_products(
        test_url,
        use_redis_cache=True,
        use_rate_limiting=True,
        force_refresh=False,
        logger=logger,
    )
    duration1 = time.time() - start

    if not products1:
        print("❌ No products returned")
        return False

    print(f"✅ Got {len(products1)} products in {duration1:.2f}s")

    # Verify cache was populated
    cached = cache.get_cached_products(test_url)
    if not cached:
        print("❌ Cache was not populated")
        return False

    print(f"✅ Cache populated with {len(cached)} products")

    # Second call - should hit cache
    print("\n4. Second call (should hit Redis cache)...")
    start = time.time()
    products2 = crawl_category_products(
        test_url,
        use_redis_cache=True,
        use_rate_limiting=True,
        force_refresh=False,
        logger=logger,
    )
    duration2 = time.time() - start

    if not products2:
        print("❌ No products returned")
        return False

    print(f"✅ Got {len(products2)} products in {duration2:.2f}s")

    # Verify it was from cache
    if len(products2) != len(products1):
        print(f"⚠️  Product count mismatch: {len(products2)} != {len(products1)}")

    if duration2 < duration1 * 0.3:  # Should be much faster
        print(f"✅ Cache hit confirmed ({duration1 / duration2:.1f}x faster)")
    else:
        print(f"⚠️  May not be from cache ({duration1 / duration2:.1f}x faster)")

    # Check cache TTL
    print("\n5. Checking cache TTL...")
    try:
        full_key = cache._make_key(key)
        ttl = cache.client.ttl(full_key)
        print(f"✅ Cache TTL: {ttl}s ({ttl / 3600:.1f}h)")
        if ttl < 3600:
            print("⚠️  TTL is less than 1 hour")
    except Exception as e:
        print(f"⚠️  Could not check TTL: {e}")

    return True


def test_redis_stats():
    """Show Redis cache statistics"""
    print("\n" + "=" * 70)
    print("Redis Cache Statistics")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    redis_url = get_working_redis_url(db=1)
    if not redis_url:
        print("❌ Redis not available")
        return False

    cache = get_redis_cache(redis_url)

    try:
        # Get all keys
        all_keys = cache.client.keys("tiki:crawl:*")
        print(f"\n📊 Total cache keys: {len(all_keys)}")

        # Count by type
        html_keys = [k for k in all_keys if ":html:" in k]
        product_keys = [k for k in all_keys if ":products:" in k]
        detail_keys = [k for k in all_keys if ":detail:" in k]

        print(f"   - HTML cache: {len(html_keys)}")
        print(f"   - Product list cache: {len(product_keys)}")
        print(f"   - Product detail cache: {len(detail_keys)}")

        # Show sample keys with TTL
        if product_keys:
            print("\n📝 Sample product cache (with TTL):")
            for key in product_keys[:5]:
                try:
                    ttl = cache.client.ttl(key)
                    data = cache.client.get(key)
                    if data:
                        import json

                        cached = json.loads(data)
                        products = cached.get("products", [])
                        print(
                            f"   - {key[-32:]}: {len(products)} products, TTL={ttl}s ({ttl / 3600:.1f}h)"
                        )
                except Exception as e:
                    print(f"   - {key[-32:]}: Error - {e}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("\nStarting Full Redis Flow Test...\n")

    success = True

    # Test 1: Full flow
    if not test_redis_from_airflow_context():
        print("\nFull flow test FAILED")
        success = False
    else:
        print("\nFull flow test PASSED")

    # Test 2: Stats
    if not test_redis_stats():
        print("\nStats test FAILED")
        success = False
    else:
        print("\nStats test PASSED")

    if success:
        print("\n" + "=" * 70)
        print("ALL TESTS PASSED - Redis cache is working correctly!")
        print("=" * 70)
        print("\nNext steps:")
        print("   1. Monitor cache hit rate in production")
        print("   2. Check for 429 errors and adjust rate limiting")
        print("   3. Optimize task startup time with lazy imports")
        sys.exit(0)
    else:
        print("\n" + "=" * 70)
        print("SOME TESTS FAILED")
        print("=" * 70)
        sys.exit(1)
