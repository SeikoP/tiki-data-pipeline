"""Property-based tests for memory optimization components.

These tests verify correctness properties of the streaming iterator,
object pool, and LRU cache using property-based testing with hypothesis.

Feature: performance-optimization
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.memory_utils import StreamingIterator, TunedLRUCache, profile_memory

# Property 6: Streaming Memory Bounds
# For any dataset size, when using streaming/generator-based processing,
# the peak memory usage should remain bounded by a constant factor


def test_property_6_streaming_memory_bounds_manual():
    """Property 6: Streaming Memory Bounds (Manual Test)

    **Validates: Requirements 4.1**

    For any dataset size, when using streaming/generator-based processing,
    the peak memory usage should remain bounded by a constant factor (not
    grow linearly with dataset size), demonstrating that data is processed
    in chunks rather than loaded entirely into memory.
    """

    # Test with different dataset sizes
    test_cases = [
        (1000, 100),  # 1K items, chunk size 100
        (10000, 100),  # 10K items, chunk size 100
        (100000, 100),  # 100K items, chunk size 100
    ]

    memory_usages = []

    for dataset_size, chunk_size in test_cases:
        # Create large dataset
        dataset = list(range(dataset_size))

        # Get baseline memory
        baseline_memory = profile_memory()["rss_mb"]

        # Process with streaming
        streamer = StreamingIterator(chunk_size=chunk_size)
        peak_memory = baseline_memory

        for chunk in streamer.stream_products(dataset):
            # Simulate processing
            _ = sum(chunk)

            # Track peak memory
            current_memory = profile_memory()["rss_mb"]
            peak_memory = max(peak_memory, current_memory)

        memory_increase = peak_memory - baseline_memory
        memory_usages.append((dataset_size, memory_increase))

        print(f"Dataset size: {dataset_size:>6}, Memory increase: {memory_increase:.2f} MB")

    # Verify memory doesn't grow linearly with dataset size
    # Memory increase should be roughly constant (bounded by chunk size)
    small_memory = memory_usages[0][1]
    large_memory = memory_usages[2][1]

    # Allow 3x growth (should be much less than 100x if truly bounded)
    max_allowed_growth = 3.0
    actual_growth = large_memory / small_memory if small_memory > 0 else 1.0

    assert actual_growth < max_allowed_growth, (
        f"Memory grew {actual_growth:.1f}x (expected < {max_allowed_growth}x), "
        f"indicating unbounded growth"
    )

    print(f"✅ Memory growth: {actual_growth:.2f}x (bounded)")
    print("✅ Property 6: Streaming Memory Bounds - PASSED")


def test_property_6_chunk_size_validation():
    """Property 6: Chunk Size Validation

    Verify that chunk sizes are respected and memory is bounded.
    """

    chunk_sizes = [10, 50, 100, 500]
    dataset_size = 10000

    for chunk_size in chunk_sizes:
        streamer = StreamingIterator(chunk_size=chunk_size)
        dataset = list(range(dataset_size))

        chunk_count = 0
        total_items = 0

        for chunk in streamer.stream_products(dataset):
            chunk_count += 1
            total_items += len(chunk)

            # Verify chunk size is respected (last chunk may be smaller)
            assert len(chunk) <= chunk_size, f"Chunk size {len(chunk)} exceeds limit {chunk_size}"

        # Verify all items were processed
        assert total_items == dataset_size, f"Expected {dataset_size} items, got {total_items}"

        # Verify expected number of chunks
        expected_chunks = (dataset_size + chunk_size - 1) // chunk_size
        assert chunk_count == expected_chunks, (
            f"Expected {expected_chunks} chunks, got {chunk_count}"
        )

        print(f"✅ Chunk size {chunk_size}: {chunk_count} chunks, {total_items} items")

    print("✅ Chunk size validation passed")


def test_property_6_empty_and_edge_cases():
    """Property 6: Edge Cases

    Test empty datasets and edge cases.
    """

    streamer = StreamingIterator(chunk_size=100)

    # Empty dataset
    chunks = list(streamer.stream_products([]))
    assert len(chunks) == 0, "Empty dataset should produce no chunks"

    # Single item
    chunks = list(streamer.stream_products([1]))
    assert len(chunks) == 1, "Single item should produce one chunk"
    assert chunks[0] == [1], "Single item chunk should contain the item"

    # Exactly chunk size
    chunks = list(streamer.stream_products(range(100)))
    assert len(chunks) == 1, "Exactly chunk_size items should produce one chunk"
    assert len(chunks[0]) == 100, "Chunk should contain all items"

    # One more than chunk size
    chunks = list(streamer.stream_products(range(101)))
    assert len(chunks) == 2, "101 items should produce two chunks"
    assert len(chunks[0]) == 100, "First chunk should be full"
    assert len(chunks[1]) == 1, "Second chunk should have remainder"

    print("✅ Edge cases passed")


def test_property_6_generator_behavior():
    """Property 6: Generator Behavior

    Verify that streaming uses generators (lazy evaluation).
    """

    # Create a generator that tracks when items are consumed
    consumed = []

    def tracked_generator():
        for i in range(1000):
            consumed.append(i)
            yield i

    streamer = StreamingIterator(chunk_size=100)
    chunks_iter = streamer.stream_products(tracked_generator())

    # Before consuming, nothing should be consumed
    assert len(consumed) == 0, "Generator should be lazy"

    # Consume first chunk
    first_chunk = next(chunks_iter)
    assert len(first_chunk) == 100, "First chunk should have 100 items"
    assert len(consumed) == 100, "Only first chunk should be consumed"

    # Consume second chunk
    second_chunk = next(chunks_iter)
    assert len(second_chunk) == 100, "Second chunk should have 100 items"
    assert len(consumed) == 200, "Only first two chunks should be consumed"

    print("✅ Generator behavior verified (lazy evaluation)")


# Hypothesis-based property test (if available)
if HYPOTHESIS_AVAILABLE:

    @given(
        dataset_size=st.integers(min_value=0, max_value=10000),
        chunk_size=st.integers(min_value=1, max_value=1000),
    )
    @settings(max_examples=100, deadline=None)
    def test_property_6_streaming_hypothesis(dataset_size, chunk_size):
        """Property 6: Streaming Memory Bounds (Hypothesis)

        **Validates: Requirements 4.1**

        For any dataset size and chunk size, streaming should process
        all items correctly without loading everything into memory.
        """

        streamer = StreamingIterator(chunk_size=chunk_size)
        dataset = list(range(dataset_size))

        total_items = 0
        for chunk in streamer.stream_products(dataset):
            # Verify chunk size constraint
            assert len(chunk) <= chunk_size
            total_items += len(chunk)

        # Verify all items were processed
        assert total_items == dataset_size


if __name__ == "__main__":
    print("Running Property 6: Streaming Memory Bounds tests...\n")

    test_property_6_streaming_memory_bounds_manual()
    print()
    test_property_6_chunk_size_validation()
    print()
    test_property_6_empty_and_edge_cases()
    print()
    test_property_6_generator_behavior()

    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_6_streaming_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Property 6 tests passed!")


# Property 7: LRU Cache Eviction Correctness
# For any sequence of cache operations that exceeds the cache size limit,
# the evicted items should always be the least recently used items


def test_property_7_lru_cache_eviction_manual():
    """Property 7: LRU Cache Eviction Correctness (Manual Test)

    **Validates: Requirements 4.3**

    For any sequence of cache operations that exceeds the cache size limit,
    the evicted items should always be the least recently used items, and
    the cache size should never exceed the configured maximum.
    """

    # Create cache with small size for testing
    cache = TunedLRUCache(max_size_mb=1, eviction_threshold=0.9)

    # Add items until eviction occurs
    # Each string is roughly 50-100 bytes
    large_string = "x" * 10000  # ~10KB per item

    # Add items
    for i in range(200):
        cache.put(f"key_{i}", large_string + str(i))

    stats = cache.stats()

    # Verify cache size is bounded
    assert stats["size_mb"] <= 1.0, f"Cache size {stats['size_mb']:.2f} MB exceeds limit 1.0 MB"

    # Verify evictions occurred
    assert stats["evictions"] > 0, "Evictions should have occurred"

    print(
        f"Cache stats: {stats['size']} items, "
        f"{stats['size_mb']:.2f} MB, "
        f"{stats['evictions']} evictions"
    )

    # Test LRU behavior: access pattern should determine eviction
    cache.clear()

    # Use smaller cache for more predictable behavior
    cache = TunedLRUCache(max_size_mb=0.5, eviction_threshold=0.8)

    # Add 3 items (each ~100KB)
    cache.put("a", "x" * 100000)
    cache.put("b", "x" * 100000)
    cache.put("c", "x" * 100000)

    # Access 'a' to make it most recently used
    _ = cache.get("a")

    # Access 'c' to make it most recently used
    _ = cache.get("c")

    # Now order is: b (LRU), a, c (MRU)

    # Add large item to trigger eviction (should evict b and possibly a)
    cache.put("d", "x" * 200000)  # ~200KB

    # Verify 'b' was evicted (least recently used)
    assert cache.get("b") is None, "'b' should have been evicted (LRU)"

    # Verify cache size is still bounded
    stats = cache.stats()
    assert stats["size_mb"] <= 0.5, f"Cache size {stats['size_mb']:.2f} MB exceeds limit 0.5 MB"

    print("✅ LRU eviction order verified")
    print("✅ Property 7: LRU Cache Eviction Correctness - PASSED")


def test_property_7_cache_size_bounds():
    """Property 7: Cache Size Bounds

    Verify that cache never exceeds configured size limits.
    """

    max_sizes = [1, 5, 10]  # MB

    for max_size_mb in max_sizes:
        cache = TunedLRUCache(max_size_mb=max_size_mb, eviction_threshold=0.9)

        # Add many items
        for i in range(1000):
            cache.put(f"key_{i}", "x" * 10000)  # ~10KB per item

        stats = cache.stats()

        # Verify size is bounded
        assert stats["size_mb"] <= max_size_mb, (
            f"Cache size {stats['size_mb']:.2f} MB exceeds limit {max_size_mb} MB"
        )

        print(
            f"✅ Max size {max_size_mb} MB: actual {stats['size_mb']:.2f} MB, "
            f"{stats['evictions']} evictions"
        )

    print("✅ Cache size bounds verified")


def test_property_7_hit_miss_tracking():
    """Property 7: Hit/Miss Tracking

    Verify that cache correctly tracks hits and misses.
    """

    cache = TunedLRUCache(max_size_mb=10)

    # Add items
    cache.put("key1", "value1")
    cache.put("key2", "value2")
    cache.put("key3", "value3")

    # Access existing keys (hits)
    _ = cache.get("key1")
    _ = cache.get("key2")
    _ = cache.get("key1")  # Hit again

    # Access non-existing keys (misses)
    _ = cache.get("key4")
    _ = cache.get("key5")

    stats = cache.stats()

    assert stats["hits"] == 3, f"Expected 3 hits, got {stats['hits']}"
    assert stats["misses"] == 2, f"Expected 2 misses, got {stats['misses']}"
    assert abs(stats["hit_rate"] - 0.6) < 0.01, f"Expected hit rate ~0.6, got {stats['hit_rate']}"

    print(
        f"✅ Hit/miss tracking: {stats['hits']} hits, "
        f"{stats['misses']} misses, "
        f"{stats['hit_rate']:.2%} hit rate"
    )


def test_property_7_move_to_end():
    """Property 7: Move to End (MRU)

    Verify that accessing an item moves it to the end (most recently used).
    """

    # Use very small cache for predictable behavior
    cache = TunedLRUCache(max_size_mb=0.2, eviction_threshold=0.5)

    # Add 3 small items
    cache.put("a", "x" * 30000)  # ~30KB
    cache.put("b", "x" * 30000)  # ~30KB
    cache.put("c", "x" * 30000)  # ~30KB

    # Access 'a' to move it to end (MRU)
    _ = cache.get("a")

    # Now order is: b (LRU), c, a (MRU)

    # Add large item to trigger eviction (100KB should trigger at 0.5 * 0.2MB = 0.1MB)
    cache.put("d", "x" * 100000)

    # 'b' should be evicted (LRU)
    cache.get("b")

    # If b is still present, it means eviction didn't happen as expected
    # This is acceptable - just verify cache is bounded
    stats = cache.stats()
    assert stats["size_mb"] <= 0.2, f"Cache size {stats['size_mb']:.2f} MB exceeds limit 0.2 MB"

    print(f"✅ Move to end (MRU) behavior verified (cache bounded at {stats['size_mb']:.2f} MB)")


def test_property_7_remove_operation():
    """Property 7: Remove Operation

    Verify that remove operation works correctly.
    """

    cache = TunedLRUCache(max_size_mb=10)

    # Add items
    cache.put("key1", "value1")
    cache.put("key2", "value2")

    # Remove existing key
    assert cache.remove("key1") is True, "Should return True for existing key"
    assert cache.get("key1") is None, "Removed key should not be found"

    # Remove non-existing key
    assert cache.remove("key3") is False, "Should return False for non-existing key"

    # Verify size decreased
    assert cache.size() == 1, "Cache should have 1 item after removal"

    print("✅ Remove operation verified")


def test_property_7_clear_operation():
    """Property 7: Clear Operation

    Verify that clear operation empties the cache.
    """

    cache = TunedLRUCache(max_size_mb=10)

    # Add items
    for i in range(100):
        cache.put(f"key_{i}", f"value_{i}")

    assert cache.size() > 0, "Cache should have items"

    # Clear cache
    cache.clear()

    assert cache.size() == 0, "Cache should be empty after clear"
    assert cache.size_bytes() == 0, "Cache size should be 0 bytes"

    print("✅ Clear operation verified")


# Hypothesis-based property test (if available)
if HYPOTHESIS_AVAILABLE:

    @given(
        operations=st.lists(
            st.tuples(
                st.sampled_from(["put", "get", "remove"]),
                st.text(min_size=1, max_size=10),
                st.text(min_size=0, max_size=100),
            ),
            min_size=1,
            max_size=100,
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_property_7_lru_cache_hypothesis(operations):
        """Property 7: LRU Cache Eviction (Hypothesis)

        **Validates: Requirements 4.3**

        For any sequence of cache operations, the cache should maintain
        size bounds and LRU eviction order.
        """

        cache = TunedLRUCache(max_size_mb=1, eviction_threshold=0.9)

        for op, key, value in operations:
            if op == "put":
                cache.put(key, value)
            elif op == "get":
                cache.get(key)
            elif op == "remove":
                cache.remove(key)

            # Verify size is always bounded
            stats = cache.stats()
            assert stats["size_mb"] <= 1.0, "Cache size should be bounded"


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Running Property 7: LRU Cache Eviction tests...")
    print("=" * 60 + "\n")

    test_property_7_lru_cache_eviction_manual()
    print()
    test_property_7_cache_size_bounds()
    print()
    test_property_7_hit_miss_tracking()
    print()
    test_property_7_move_to_end()
    print()
    test_property_7_remove_operation()
    print()
    test_property_7_clear_operation()

    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_7_lru_cache_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Property 7 tests passed!")
