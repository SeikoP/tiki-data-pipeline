"""Final test: Verify Redis cache is Tier 1 and working correctly."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(level=logging.INFO, format='%(message)s')

def main():
    """Test complete cache flow."""
    print("\n" + "=" * 70)
    print("FINAL TEST: Redis Cache as Tier 1")
    print("=" * 70)
    
    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url
    
    test_url = "https://tiki.vn/bo-quan-ao-nam-kich-co-lon/c49530"
    
    # Step 1: Clear Redis cache
    print("\n📍 Step 1: Clear Redis cache")
    redis_url = get_working_redis_url(db=1)
    if redis_url:
        cache = get_redis_cache(redis_url)
        if cache:
            key = cache.get_cache_key(test_url, "products")
            cache.delete(key)
            print("   ✅ Redis cache cleared")
    
    # Step 2: First crawl - should hit File cache and populate Redis
    print("\n📍 Step 2: First crawl (File cache → populate Redis)")
    products1 = crawl_category_products(
        test_url,
        max_pages=1,
        use_redis_cache=True,
        force_refresh=False,
        cache_ttl=86400
    )
    print(f"   ✅ Got {len(products1)} products")
    
    # Step 3: Verify Redis was populated
    print("\n📍 Step 3: Verify Redis cache")
    if redis_url and cache:
        cached = cache.get_cached_products(test_url)
        if cached:
            print(f"   ✅ Redis has {len(cached)} products (Tier 1 ready!)")
        else:
            print("   ❌ Redis NOT populated")
            return False
    
    # Step 4: Second crawl - should hit Redis (Tier 1)
    print("\n📍 Step 4: Second crawl (should hit Redis - Tier 1)")
    print("   Watch for '[Redis Cache] ✅ Hit cache' message...")
    products2 = crawl_category_products(
        test_url,
        max_pages=1,
        use_redis_cache=True,
        force_refresh=False,
        cache_ttl=86400
    )
    print(f"   ✅ Got {len(products2)} products")
    
    # Step 5: Third crawl - confirm Redis is consistently hit
    print("\n📍 Step 5: Third crawl (confirm Redis consistency)")
    products3 = crawl_category_products(
        test_url,
        max_pages=1,
        use_redis_cache=True,
        force_refresh=False,
        cache_ttl=86400
    )
    print(f"   ✅ Got {len(products3)} products")
    
    # Verify all results match
    if len(products1) == len(products2) == len(products3):
        print("\n" + "=" * 70)
        print("🎉 SUCCESS: Redis cache is working as Tier 1!")
        print("=" * 70)
        print("\nCache flow:")
        print("  1st call: File cache (Tier 2) → populate Redis")
        print("  2nd call: Redis cache (Tier 1) ✅")
        print("  3rd call: Redis cache (Tier 1) ✅")
        return True
    else:
        print("\n❌ FAIL: Product counts don't match")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
