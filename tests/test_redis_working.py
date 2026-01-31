"""Test Redis cache hoạt động đúng với priority Tier 1"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def test_redis_priority():
    """Test Redis được ưu tiên hơn file cache"""
    print("\n" + "=" * 70)
    print("TEST: Redis Cache Priority (Tier 1)")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    # 1. Check Redis connection
    print("\n1. Checking Redis connection...")
    redis_url = get_working_redis_url(db=1, timeout=2)
    if not redis_url:
        print("❌ Redis not available")
        return False

    print(f"✅ Redis available at: {redis_url}")

    # 2. Get cache instance
    cache = get_redis_cache(redis_url)
    if not cache:
        print("❌ Failed to create cache instance")
        return False

    print("✅ Cache instance created")

    # 3. Test cache operations
    test_url = "https://tiki.vn/test-priority/c99999"
    test_products = [
        {"product_id": "1", "name": "Test Product 1"},
        {"product_id": "2", "name": "Test Product 2"},
    ]

    print(f"\n2. Testing cache operations for: {test_url}")

    # Clear existing cache
    cache_key = cache.get_cache_key(test_url, "products")
    cache.delete(cache_key)
    print("✅ Cleared existing cache")

    # Cache products
    success = cache.cache_products(test_url, test_products, ttl=3600)
    if not success:
        print("❌ Failed to cache products")
        return False

    print(f"✅ Cached {len(test_products)} products with TTL=3600s")

    # Retrieve from cache
    cached = cache.get_cached_products(test_url)
    if not cached:
        print("❌ Failed to retrieve cached products")
        return False

    if len(cached) != len(test_products):
        print(f"❌ Product count mismatch: {len(cached)} != {len(test_products)}")
        return False

    print(f"✅ Retrieved {len(cached)} products from cache")

    # 4. Test with crawl_category_products
    print("\n3. Testing with crawl_category_products...")
    from pipelines.crawl.crawl_products import crawl_category_products

    # First call - should use Redis cache
    start = time.time()
    products1 = crawl_category_products(
        test_url,
        use_redis_cache=True,
        use_rate_limiting=False,
        force_refresh=False,
    )
    duration1 = time.time() - start

    if not products1:
        print("❌ No products returned")
        return False

    print(f"✅ First call: {len(products1)} products in {duration1:.3f}s")

    # Second call - should hit Redis cache (very fast)
    start = time.time()
    products2 = crawl_category_products(
        test_url,
        use_redis_cache=True,
        use_rate_limiting=False,
        force_refresh=False,
    )
    duration2 = time.time() - start

    if len(products2) != len(products1):
        print(f"❌ Product count mismatch: {len(products2)} != {len(products1)}")
        return False

    print(f"✅ Second call: {len(products2)} products in {duration2:.3f}s")

    # Cache hit should be much faster
    if duration2 < duration1 * 0.5:  # At least 2x faster
        print(f"✅ Redis cache hit confirmed (2nd call {duration1 / duration2:.1f}x faster)")
    else:
        print(f"⚠️  Cache may not be working (2nd call only {duration1 / duration2:.1f}x faster)")

    # Cleanup
    cache.delete(cache_key)
    print("\n✅ Cleanup completed")

    return True


def test_redis_stats():
    """Test Redis cache statistics"""
    print("\n" + "=" * 70)
    print("TEST: Redis Cache Statistics")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    redis_url = get_working_redis_url(db=1)
    if not redis_url:
        print("❌ Redis not available")
        return False

    cache = get_redis_cache(redis_url)

    # Get all cache keys
    try:
        keys = cache.client.keys("tiki:crawl:*")
        print(f"\n📊 Total cache keys: {len(keys)}")

        # Count by type
        html_keys = [k for k in keys if ":html:" in k]
        product_keys = [k for k in keys if ":products:" in k]
        detail_keys = [k for k in keys if ":detail:" in k]

        print(f"   - HTML cache: {len(html_keys)} keys")
        print(f"   - Product list cache: {len(product_keys)} keys")
        print(f"   - Product detail cache: {len(detail_keys)} keys")

        # Sample some keys
        if product_keys:
            print("\n📝 Sample product cache keys:")
            for key in product_keys[:3]:
                try:
                    data = cache.client.get(key)
                    if data:
                        import json

                        cached = json.loads(data)
                        products = cached.get("products", [])
                        cached_at = cached.get("cached_at", 0)
                        age_hours = (time.time() - cached_at) / 3600
                        print(f"   - {key[-16:]}: {len(products)} products, {age_hours:.1f}h old")
                except Exception as e:
                    print(f"   - {key[-16:]}: Error reading - {e}")

        return True
    except Exception as e:
        print(f"❌ Error getting stats: {e}")
        return False


if __name__ == "__main__":
    print("\n🚀 Starting Redis Cache Tests...\n")

    success = True

    # Test 1: Priority
    if not test_redis_priority():
        print("\n❌ Redis priority test FAILED")
        success = False
    else:
        print("\n✅ Redis priority test PASSED")

    # Test 2: Stats
    if not test_redis_stats():
        print("\n❌ Redis stats test FAILED")
        success = False
    else:
        print("\n✅ Redis stats test PASSED")

    if success:
        print("\n" + "=" * 70)
        print("✅ ALL TESTS PASSED")
        print("=" * 70)
        sys.exit(0)
    else:
        print("\n" + "=" * 70)
        print("❌ SOME TESTS FAILED")
        print("=" * 70)
        sys.exit(1)
