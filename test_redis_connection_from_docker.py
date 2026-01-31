"""Test Redis connection from inside Docker (simulating Airflow task)."""
import sys
import os

# Simulate Airflow environment
sys.path.insert(0, "/opt/airflow/src")

from pipelines.crawl.storage.redis_cache import get_redis_cache, get_working_redis_url

print("=" * 80)
print("TESTING REDIS CONNECTION FROM DOCKER CONTEXT")
print("=" * 80)

# Test 1: Auto-detect
print("\n1. Auto-detecting Redis URL...")
redis_url = get_working_redis_url(db=1, timeout=2)
print(f"   Detected URL: {redis_url}")

# Test 2: Try both URLs
test_urls = [
    "redis://redis:6379/1",      # Docker internal
    "redis://localhost:6379/1",  # External
]

for url in test_urls:
    print(f"\n2. Testing connection to {url}...")
    try:
        redis_cache = get_redis_cache(url)
        if redis_cache:
            # Try to ping
            redis_cache.client.ping()
            print(f"   ✅ Connection successful")
            
            # Try to get a key
            test_key = "products:078f3e84ce7d932ac51aa8ac7ea5ce73"
            exists = redis_cache.exists(test_key)
            print(f"   Key exists: {exists}")
            
            if exists:
                cached = redis_cache.get(test_key)
                if cached:
                    products = cached.get("products", [])
                    print(f"   ✅ Retrieved {len(products)} products")
                else:
                    print(f"   ❌ Key exists but get() returned None")
        else:
            print(f"   ❌ Failed to create cache instance")
    except Exception as e:
        print(f"   ❌ Error: {e}")

print("\n" + "=" * 80)
