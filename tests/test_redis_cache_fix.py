"""Test Redis cache fix trong crawl_products."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(level=logging.INFO)


def test_redis_auto_detection():
    """Test auto-detection của Redis URL."""
    print("\n" + "=" * 70)
    print("TEST 1: Redis Auto-Detection")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_working_redis_url

    url = get_working_redis_url(db=1)
    print(f"\nWorking Redis URL: {url}")

    if url:
        print("✅ Redis auto-detection works!")
        return True
    else:
        print("❌ No working Redis URL found")
        return False


def test_crawl_with_redis():
    """Test crawl với Redis cache."""
    print("\n" + "=" * 70)
    print("TEST 2: Crawl with Redis Cache")
    print("=" * 70)

    from pipelines.crawl.crawl_products import crawl_category_products

    test_url = "https://tiki.vn/bo-quan-ao-nam-kich-co-lon/c49530"

    print("\n1st crawl (should use API or file cache):")
    products1 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Got {len(products1)} products")

    print("\n2nd crawl (should hit Redis cache):")
    products2 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Got {len(products2)} products")

    if len(products1) == len(products2):
        print("✅ Cache working correctly!")
        return True
    else:
        print("⚠️  Product count mismatch")
        return False


def test_redis_cache_directly():
    """Test Redis cache trực tiếp."""
    print("\n" + "=" * 70)
    print("TEST 3: Direct Redis Cache Test")
    print("=" * 70)

    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    url = get_working_redis_url(db=1)
    if not url:
        print("❌ No Redis available")
        return False

    cache = get_redis_cache(url)
    if not cache:
        print("❌ Could not create cache")
        return False

    # Test cache operations
    test_url = "https://tiki.vn/test-category/c99999"
    test_products = [{"id": 1, "name": "Product 1"}, {"id": 2, "name": "Product 2"}]

    print(f"\nCaching {len(test_products)} products...")
    success = cache.cache_products(test_url, test_products, ttl=60)
    print(f"   Cache write: {'✅' if success else '❌'}")

    print("\nRetrieving from cache...")
    cached = cache.get_cached_products(test_url)
    print(f"   Cache read: {'✅' if cached else '❌'}")

    if cached:
        print(f"   Retrieved {len(cached)} products")

        # Cleanup
        cache.delete(cache.get_cache_key(test_url, "products"))
        print("   Cleaned up test data")
        return True

    return False


def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("REDIS CACHE FIX TEST SUITE")
    print("=" * 70)

    results = []

    # Test 1
    results.append(("Auto-Detection", test_redis_auto_detection()))

    # Test 2
    results.append(("Direct Cache", test_redis_cache_directly()))

    # Test 3
    results.append(("Crawl with Redis", test_crawl_with_redis()))

    # Summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)

    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")

    all_passed = all(success for _, success in results)

    if all_passed:
        print("\n🎉 All tests passed! Redis cache is working.")
    else:
        print("\n⚠️  Some tests failed.")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
