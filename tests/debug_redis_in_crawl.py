"""Debug Redis cache trong crawl_products.

Kiểm tra tại sao Redis cache không được sử dụng.
"""

import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Setup logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def test_redis_initialization():
    """Test khởi tạo Redis như trong crawl_products."""
    print("\n" + "=" * 70)
    print("🔍 DEBUG: Redis Initialization in crawl_products")
    print("=" * 70)

    # Simulate the code in crawl_category_products
    use_redis_cache = True
    redis_cache = None

    if use_redis_cache:
        try:
            print("\n1. Attempting to import get_redis_cache...")
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            print("   ✅ Import successful")

            print("\n2. Attempting to connect to redis://redis:6379/1...")
            redis_cache = get_redis_cache("redis://redis:6379/1")
            print(f"   ✅ Connection returned: {redis_cache}")

            if redis_cache:
                print("\n3. Testing Redis operations...")

                # Test ping
                redis_cache.client.ping()
                print("   ✅ PING successful")

                # Test cache operations
                test_url = "https://tiki.vn/test/c12345"
                test_products = [{"id": 1, "name": "Test"}]

                redis_cache.cache_products(test_url, test_products, ttl=60)
                print("   ✅ cache_products successful")

                cached = redis_cache.get_cached_products(test_url)
                print(f"   ✅ get_cached_products: {cached}")

                # Cleanup
                redis_cache.delete(redis_cache.get_cache_key(test_url, "products"))

            else:
                print("   ❌ redis_cache is None")

        except Exception as e:
            print(f"   ❌ Exception: {e}")
            import traceback

            traceback.print_exc()

    return redis_cache


def test_with_localhost():
    """Test với localhost thay vì redis."""
    print("\n" + "=" * 70)
    print("🔍 DEBUG: Try with localhost instead of redis")
    print("=" * 70)

    try:
        from pipelines.crawl.storage.redis_cache import get_redis_cache

        print("\nAttempting to connect to redis://localhost:6379/1...")
        redis_cache = get_redis_cache("redis://localhost:6379/1")

        if redis_cache:
            redis_cache.client.ping()
            print("✅ Connected to localhost:6379")
            return redis_cache
        else:
            print("❌ redis_cache is None")
            return None

    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_actual_crawl():
    """Test actual crawl function với Redis."""
    print("\n" + "=" * 70)
    print("🔍 DEBUG: Test actual crawl_category_products")
    print("=" * 70)

    try:
        from pipelines.crawl.crawl_products import crawl_category_products

        # Test với một category nhỏ
        test_url = "https://tiki.vn/bo-quan-ao-nam-kich-co-lon/c49530"

        print(f"\nCrawling: {test_url}")
        print("Parameters:")
        print("  - use_redis_cache=True")
        print("  - max_pages=1")
        print("  - force_refresh=False")

        products = crawl_category_products(
            test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
        )

        print(f"\n✅ Got {len(products)} products")

        # Try again to see if cache is hit
        print("\n🔄 Crawling again to test cache...")
        products2 = crawl_category_products(
            test_url, max_pages=1, use_redis_cache=True, force_refresh=False, cache_ttl=86400
        )

        print(f"✅ Got {len(products2)} products (should be from cache)")

    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback

        traceback.print_exc()


def check_redis_connection_from_code():
    """Check Redis connection như code thực tế."""
    print("\n" + "=" * 70)
    print("🔍 DEBUG: Check Redis Connection")
    print("=" * 70)

    # Test cả 2 URLs
    urls = [
        "redis://redis:6379/1",  # Docker internal
        "redis://localhost:6379/1",  # External
    ]

    for url in urls:
        print(f"\n📍 Testing: {url}")
        try:
            from urllib.parse import urlparse

            import redis

            parsed = urlparse(url)
            client = redis.Redis(
                host=parsed.hostname or "localhost",
                port=parsed.port or 6379,
                db=int(parsed.path.lstrip("/")) if parsed.path else 0,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )

            client.ping()
            print("   ✅ Connected successfully")

            # Test operations
            client.set("test_key", "test_value", ex=60)
            value = client.get("test_key")
            client.delete("test_key")
            print(f"   ✅ Operations work: {value}")

        except Exception as e:
            print(f"   ❌ Failed: {e}")


def main():
    """Run all debug tests."""
    print("\n" + "=" * 70)
    print("🐛 REDIS CACHE DEBUG SUITE")
    print("=" * 70)

    # Test 1: Check basic Redis connection
    check_redis_connection_from_code()

    # Test 2: Test Redis initialization as in crawl_products
    redis_cache = test_redis_initialization()

    # Test 3: Try with localhost
    if not redis_cache:
        redis_cache = test_with_localhost()

    # Test 4: Test actual crawl function
    test_actual_crawl()

    print("\n" + "=" * 70)
    print("✅ Debug complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
