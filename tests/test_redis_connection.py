"""Test Redis connection and basic operations.

Kiểm tra kết nối Redis và các thao tác cơ bản.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("❌ Redis library not installed. Install with: pip install redis")
    sys.exit(1)


def test_direct_connection():
    """Test kết nối trực tiếp tới Redis."""
    print("\n" + "=" * 70)
    print("🔌 TEST 1: Direct Redis Connection")
    print("=" * 70)
    
    try:
        # Kết nối tới Redis (localhost hoặc trong Docker)
        client = redis.Redis(
            host='localhost',  # Hoặc 'redis' nếu chạy trong Docker
            port=6379,
            db=0,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        
        # Test ping
        response = client.ping()
        print(f"✅ Redis PING: {response}")
        
        # Test set/get
        client.set('test_key', 'Hello Redis!')
        value = client.get('test_key')
        print(f"✅ SET/GET test: {value}")
        
        # Get info
        info = client.info('server')
        print(f"✅ Redis version: {info.get('redis_version')}")
        print(f"✅ Redis mode: {info.get('redis_mode')}")
        
        # Get memory info
        memory_info = client.info('memory')
        used_memory = memory_info.get('used_memory_human', 'N/A')
        print(f"✅ Memory used: {used_memory}")
        
        # Cleanup
        client.delete('test_key')
        
        return True
        
    except redis.ConnectionError as e:
        print(f"❌ Connection failed: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Check if Redis is running: docker ps | grep redis")
        print("   2. If in Docker, use host='redis' instead of 'localhost'")
        print("   3. If outside Docker, use host='localhost'")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_redis_cache_class():
    """Test RedisCache class từ project."""
    print("\n" + "=" * 70)
    print("🔌 TEST 2: RedisCache Class")
    print("=" * 70)
    
    try:
        from pipelines.crawl.storage.redis_cache import get_redis_cache
        
        # Thử kết nối với URL khác nhau
        redis_urls = [
            "redis://localhost:6379/1",  # Outside Docker
            "redis://redis:6379/1",      # Inside Docker
        ]
        
        cache = None
        for url in redis_urls:
            try:
                print(f"\n🔄 Trying: {url}")
                cache = get_redis_cache(url)
                if cache:
                    # Test ping
                    cache.client.ping()
                    print(f"✅ Connected to: {url}")
                    break
            except Exception as e:
                print(f"⚠️  Failed: {e}")
                continue
        
        if not cache:
            print("❌ Could not connect to Redis with any URL")
            return False
        
        # Test cache operations
        print("\n📦 Testing cache operations...")
        
        # Test set/get
        test_key = "test_cache_key"
        test_value = {"message": "Hello from cache!", "count": 42}
        
        success = cache.set(test_key, test_value, ttl=60)
        print(f"✅ SET: {success}")
        
        retrieved = cache.get(test_key)
        print(f"✅ GET: {retrieved}")
        
        # Test exists
        exists = cache.exists(test_key)
        print(f"✅ EXISTS: {exists}")
        
        # Test delete
        deleted = cache.delete(test_key)
        print(f"✅ DELETE: {deleted}")
        
        # Test URL caching
        print("\n🌐 Testing URL caching...")
        test_url = "https://tiki.vn/test-category/c12345?page=1&utm_source=test"
        test_html = "<html><body>Test content</body></html>"
        
        cache.cache_html(test_url, test_html, ttl=60)
        cached_html = cache.get_cached_html(test_url)
        print(f"✅ HTML cache: {len(cached_html) if cached_html else 0} bytes")
        
        # Test product caching
        test_products = [
            {"id": 1, "name": "Product 1"},
            {"id": 2, "name": "Product 2"}
        ]
        cache.cache_products(test_url, test_products, ttl=60)
        cached_products = cache.get_cached_products(test_url)
        print(f"✅ Products cache: {len(cached_products) if cached_products else 0} items")
        
        # Get cache stats
        info = cache.client.info()
        db_key = f"db{cache.client.connection_pool.connection_kwargs.get('db', 1)}"
        keys_count = info.get(db_key, {}).get('keys', 0)
        print(f"\n📊 Cache stats:")
        print(f"   - Keys in DB: {keys_count}")
        print(f"   - Keyspace hits: {info.get('keyspace_hits', 0)}")
        print(f"   - Keyspace misses: {info.get('keyspace_misses', 0)}")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_connection_from_docker():
    """Test kết nối từ trong Docker container."""
    print("\n" + "=" * 70)
    print("🐳 TEST 3: Connection from Docker")
    print("=" * 70)
    
    print("\n💡 To test from inside Docker container, run:")
    print("   docker exec -it <container_name> python /opt/airflow/tests/test_redis_connection.py")
    print("\n   Or use docker-compose:")
    print("   docker-compose exec airflow-scheduler python /opt/airflow/tests/test_redis_connection.py")


def main():
    """Run all tests."""
    print("\n" + "=" * 70)
    print("🧪 REDIS CONNECTION TEST SUITE")
    print("=" * 70)
    
    if not REDIS_AVAILABLE:
        return
    
    results = []
    
    # Test 1: Direct connection
    results.append(("Direct Connection", test_direct_connection()))
    
    # Test 2: RedisCache class
    results.append(("RedisCache Class", test_redis_cache_class()))
    
    # Test 3: Docker instructions
    test_connection_from_docker()
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(success for _, success in results)
    
    if all_passed:
        print("\n🎉 All tests passed!")
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
