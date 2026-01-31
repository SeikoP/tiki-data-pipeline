"""Test Redis cache priority (Tier 1) vs File cache (Tier 2)."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def test_cache_priority():
    """Test cache priority: Redis (Tier 1) > File (Tier 2) > API (Tier 3)."""
    print("\n" + "=" * 70)
    print("TEST: Cache Priority (Redis > File > API)")
    print("=" * 70)

    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    test_url = "https://tiki.vn/test-cache-priority/c88888"

    # Clear both caches first
    print("\n1. Clearing caches...")

    # Clear Redis
    redis_url = get_working_redis_url(db=1)
    if redis_url:
        cache = get_redis_cache(redis_url)
        if cache:
            key = cache.get_cache_key(test_url, "products")
            cache.delete(key)
            print("   ✅ Cleared Redis cache")

    # Clear file cache
    import hashlib

    url_hash = hashlib.md5(test_url.encode()).hexdigest()
    cache_file = Path("data/raw/cache") / f"{url_hash}.json"
    if cache_file.exists():
        cache_file.unlink()
        print("   ✅ Cleared file cache")

    # Test 1: First crawl (should use API - Tier 3)
    print("\n2. First crawl (no cache, should use API)...")
    products1 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Result: {len(products1)} products")

    # Test 2: Second crawl (should hit Redis - Tier 1)
    print("\n3. Second crawl (should hit Redis cache - Tier 1)...")
    products2 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Result: {len(products2)} products")

    # Test 3: Disable Redis, should hit File cache (Tier 2)
    print("\n4. Third crawl (Redis disabled, should hit File cache - Tier 2)...")
    products3 = crawl_category_products(
        test_url,
        max_pages=1,
        use_redis_cache=False,  # Disable Redis
        force_refresh=False,
        cache_ttl=86400,
    )
    print(f"   Result: {len(products3)} products")

    # Verify Redis has the data
    print("\n5. Verifying Redis cache...")
    if redis_url and cache:
        cached = cache.get_cached_products(test_url)
        if cached:
            print(f"   ✅ Redis has {len(cached)} products cached")
        else:
            print("   ❌ Redis cache is empty")

    # Verify file cache has the data
    print("\n6. Verifying file cache...")
    if cache_file.exists():
        import json

        with open(cache_file, encoding="utf-8") as f:
            data = json.load(f)
            print(f"   ✅ File cache has {len(data.get('products', []))} products")
    else:
        print("   ❌ File cache doesn't exist")

    # Cleanup
    print("\n7. Cleanup...")
    if redis_url and cache:
        cache.delete(cache.get_cache_key(test_url, "products"))
    if cache_file.exists():
        cache_file.unlink()
    print("   ✅ Cleaned up")

    return True


def test_existing_category():
    """Test với category thực tế đã có cache."""
    print("\n" + "=" * 70)
    print("TEST: Existing Category with File Cache")
    print("=" * 70)

    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

    test_url = "https://tiki.vn/bo-quan-ao-nam-kich-co-lon/c49530"

    # Clear Redis cache only (keep file cache)
    print("\n1. Clearing Redis cache only...")
    redis_url = get_working_redis_url(db=1)
    if redis_url:
        cache = get_redis_cache(redis_url)
        if cache:
            key = cache.get_cache_key(test_url, "products")
            cache.delete(key)
            print("   ✅ Cleared Redis cache")

    # First crawl - should hit file cache and populate Redis
    print("\n2. First crawl (should hit File cache - Tier 2, then populate Redis)...")
    products1 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Result: {len(products1)} products")

    # Check if Redis was populated
    print("\n3. Checking if Redis was populated...")
    if redis_url and cache:
        cached = cache.get_cached_products(test_url)
        if cached:
            print(f"   ✅ Redis now has {len(cached)} products")
        else:
            print("   ❌ Redis was NOT populated (this is the bug!)")

    # Second crawl - should hit Redis
    print("\n4. Second crawl (should hit Redis cache - Tier 1)...")
    products2 = crawl_category_products(
        test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
    )
    print(f"   Result: {len(products2)} products")

    return True


def main():
    """Run tests."""
    print("\n" + "=" * 70)
    print("REDIS CACHE PRIORITY TEST")
    print("=" * 70)

    test_cache_priority()
    test_existing_category()

    print("\n" + "=" * 70)
    print("✅ Tests complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
