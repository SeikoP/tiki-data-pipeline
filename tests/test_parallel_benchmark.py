"""
Benchmark: Sequential vs Parallel crawling

Test scenarios:
1. Sequential crawling (baseline)
2. Parallel crawling with ThreadPool
3. Memory cache effectiveness
"""

import sys
import time
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 70)
print("🏁 PHASE 4: PARALLEL CRAWLING BENCHMARK")
print("=" * 70)
print()

# Mock crawler function (simulates web request)
def mock_crawl_product(product_id: str) -> dict:
    """Simulate crawling a product (I/O delay)"""
    time.sleep(0.1)  # 100ms per product (simulates network delay)
    return {
        "product_id": product_id,
        "name": f"Product {product_id}",
        "price": 100000,
        "crawled_at": time.time(),
    }


# Test 1: Sequential crawling (baseline)
print("📊 Test 1: Sequential crawling (baseline)")
print("-" * 70)

test_products = [f"prod_{i}" for i in range(50)]  # 50 products

start_time = time.time()
sequential_results = []

for product_id in test_products:
    result = mock_crawl_product(product_id)
    sequential_results.append(result)

sequential_time = time.time() - start_time
sequential_rate = len(test_products) / sequential_time

print(f"✅ Sequential crawling:")
print(f"   - Products: {len(sequential_results)}")
print(f"   - Time: {sequential_time:.2f}s")
print(f"   - Rate: {sequential_rate:.1f} products/s")
print()

# Test 2: Parallel crawling
print("🚀 Test 2: Parallel crawling (ThreadPoolExecutor)")
print("-" * 70)

try:
    from src.pipelines.crawl.parallel_crawler import ParallelCrawler
    
    crawler = ParallelCrawler(
        max_workers=5,
        rate_limit_per_worker=0.0,  # No rate limit for benchmark
        show_progress=True,
        continue_on_error=True,
    )
    
    start_time = time.time()
    parallel_result = crawler.crawl_parallel(
        test_products,
        mock_crawl_product,
        total_count=len(test_products),
    )
    parallel_time = time.time() - start_time
    
    print()
    print(f"✅ Parallel crawling:")
    print(f"   - Products: {parallel_result['stats']['completed']}")
    print(f"   - Failed: {parallel_result['stats']['failed']}")
    print(f"   - Time: {parallel_time:.2f}s")
    print(f"   - Rate: {parallel_result['stats']['rate']:.1f} products/s")
    print()
    
    # Calculate speedup
    speedup = sequential_time / parallel_time
    print(f"⚡ Speedup: {speedup:.2f}x faster than sequential")
    print(f"   - Sequential: {sequential_time:.2f}s")
    print(f"   - Parallel: {parallel_time:.2f}s")
    print(f"   - Time saved: {sequential_time - parallel_time:.2f}s ({(1 - parallel_time/sequential_time)*100:.1f}%)")
    
except Exception as e:
    print(f"❌ Parallel crawling test failed: {e}")
    import traceback
    traceback.print_exc()

print()

# Test 3: Memory cache effectiveness
print("💾 Test 3: Memory cache effectiveness")
print("-" * 70)

try:
    from src.common.cache_utils import cache_in_memory, get_cache_stats, clear_memory_cache
    
    # Clear cache before test
    clear_memory_cache()
    
    @cache_in_memory(ttl=60)
    def cached_function(x: int) -> int:
        """Simulate expensive computation"""
        time.sleep(0.01)  # 10ms
        return x * x
    
    # First pass (all cache misses)
    print("First pass (cache misses):")
    start_time = time.time()
    for i in range(100):
        result = cached_function(i % 10)  # 10 unique values
    first_pass_time = time.time() - start_time
    
    stats_after_first = get_cache_stats()
    print(f"   - Time: {first_pass_time:.2f}s")
    print(f"   - Hits: {stats_after_first['hits']}")
    print(f"   - Misses: {stats_after_first['misses']}")
    print(f"   - Cached items: {stats_after_first['memory_size']}")
    print()
    
    # Second pass (cache hits)
    print("Second pass (cache hits):")
    start_time = time.time()
    for i in range(100):
        result = cached_function(i % 10)  # Same 10 values
    second_pass_time = time.time() - start_time
    
    stats_after_second = get_cache_stats()
    print(f"   - Time: {second_pass_time:.2f}s")
    print(f"   - Hits: {stats_after_second['hits']}")
    print(f"   - Misses: {stats_after_second['misses']}")
    print(f"   - Hit rate: {stats_after_second['hit_rate']:.1f}%")
    print()
    
    # Calculate cache speedup
    cache_speedup = first_pass_time / second_pass_time if second_pass_time > 0 else 0
    print(f"⚡ Cache speedup: {cache_speedup:.2f}x faster")
    print(f"   - First pass: {first_pass_time:.2f}s")
    print(f"   - Second pass: {second_pass_time:.2f}s")
    
except Exception as e:
    print(f"❌ Cache test failed: {e}")
    import traceback
    traceback.print_exc()

print()

# Summary
print("=" * 70)
print("📊 BENCHMARK SUMMARY")
print("=" * 70)
print()

if 'speedup' in locals():
    print(f"🚀 Parallel Crawling:")
    print(f"   - Speedup: {speedup:.2f}x faster")
    print(f"   - Workers: 5 concurrent threads")
    print(f"   - Best for: I/O-bound operations (network requests)")
    print()

if 'cache_speedup' in locals():
    print(f"💾 Memory Cache:")
    print(f"   - Speedup: {cache_speedup:.2f}x faster")
    print(f"   - Hit rate: {stats_after_second['hit_rate']:.1f}%")
    print(f"   - Best for: Repeated queries with same parameters")
    print()

print("🎯 Combined Impact (estimated):")
if 'speedup' in locals() and 'cache_speedup' in locals():
    combined = speedup * 1.5  # Conservative estimate with caching
    print(f"   - Parallel + Cache: ~{combined:.1f}x overall speedup")
    print(f"   - For 1000 products:")
    print(f"     • Sequential: ~{1000 * 0.1:.0f}s ({1000 * 0.1 / 60:.1f} minutes)")
    print(f"     • Optimized: ~{1000 * 0.1 / combined:.0f}s ({1000 * 0.1 / combined / 60:.1f} minutes)")
    print(f"     • Time saved: ~{1000 * 0.1 - 1000 * 0.1 / combined:.0f}s")
print()

print("📋 Next steps:")
print("   1. Integrate parallel crawler into DAG")
print("   2. Add rate limiting for production")
print("   3. Test with real Tiki API")
print("   4. Monitor error rates and retries")
print()
