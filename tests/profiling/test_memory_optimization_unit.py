"""Unit tests for memory optimization components.

These tests verify specific functionality of the streaming iterator,
object pool, and LRU cache components.

Feature: performance-optimization
"""

import sys
import threading
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from common.memory_utils import ObjectPool, StreamingIterator, TunedLRUCache

# ============================================================================
# ObjectPool Unit Tests
# ============================================================================


def test_object_pool_acquire_release_cycle():
    """Test acquire/release cycle.

    **Validates: Requirements 4.6**

    Verify that objects can be acquired and released correctly.
    """

    # Create simple factory
    counter = {"value": 0}

    def create_object():
        counter["value"] += 1
        return {"id": counter["value"]}

    pool = ObjectPool(factory=create_object, max_size=10)

    # Acquire object
    obj1 = pool.acquire()
    assert obj1 is not None, "Should acquire object"
    assert obj1["id"] == 1, "First object should have id 1"

    # Release object
    pool.release(obj1)
    assert pool.size() == 1, "Pool should have 1 object after release"

    # Acquire again - should get same object
    obj2 = pool.acquire()
    assert obj2 is obj1, "Should reuse released object"
    assert pool.size() == 0, "Pool should be empty after reacquire"

    print("✅ Acquire/release cycle test passed")


def test_object_pool_size_limits():
    """Test pool size limits.

    **Validates: Requirements 4.6**

    Verify that pool respects max_size limit.
    """

    def create_object():
        return {"data": "test"}

    pool = ObjectPool(factory=create_object, max_size=5)

    # Acquire and release many objects
    objects = []
    for _i in range(10):
        obj = pool.acquire()
        objects.append(obj)

    # Release all objects
    for obj in objects:
        pool.release(obj)

    # Pool should only keep max_size objects
    assert pool.size() <= 5, f"Pool size {pool.size()} exceeds max_size 5"

    stats = pool.stats()
    assert stats["pool_size"] <= 5, "Pool size should be bounded"
    assert stats["created_count"] == 10, "Should have created 10 objects"
    assert stats["acquired_count"] == 10, "Should have acquired 10 times"
    assert stats["released_count"] == 10, "Should have released 10 times"

    print(f"✅ Size limits test passed: {stats}")


def test_object_pool_thread_safety():
    """Test thread safety.

    **Validates: Requirements 4.6**

    Verify that pool is thread-safe for concurrent access.
    """

    def create_object():
        return {"data": []}

    pool = ObjectPool(factory=create_object, max_size=20)

    # Pre-populate pool
    for _ in range(10):
        pool.release(create_object())

    errors = []
    acquired_objects = []
    lock = threading.Lock()

    def worker():
        try:
            # Acquire object
            obj = pool.acquire()

            # Store object
            with lock:
                acquired_objects.append(obj)

            # Simulate work
            time.sleep(0.001)

            # Release object
            pool.release(obj)
        except Exception as e:
            errors.append(str(e))

    # Create threads
    threads = []
    for _ in range(50):
        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join()

    # Verify no errors
    assert len(errors) == 0, f"Thread safety errors: {errors}"

    # Verify all objects were acquired
    assert len(acquired_objects) == 50, f"Expected 50 acquired objects, got {len(acquired_objects)}"

    stats = pool.stats()
    print(f"✅ Thread safety test passed: {stats}")


def test_object_pool_stats():
    """Test pool statistics tracking.

    Verify that pool correctly tracks statistics.
    """

    def create_object():
        return {"id": id(object())}

    pool = ObjectPool(factory=create_object, max_size=10)

    # Acquire some objects
    obj1 = pool.acquire()
    obj2 = pool.acquire()
    pool.acquire()

    stats = pool.stats()
    assert stats["created_count"] == 3, "Should have created 3 objects"
    assert stats["acquired_count"] == 3, "Should have acquired 3 times"
    assert stats["released_count"] == 0, "Should have released 0 times"
    assert stats["pool_size"] == 0, "Pool should be empty"

    # Release objects
    pool.release(obj1)
    pool.release(obj2)

    stats = pool.stats()
    assert stats["released_count"] == 2, "Should have released 2 times"
    assert stats["pool_size"] == 2, "Pool should have 2 objects"

    print(f"✅ Stats tracking test passed: {stats}")


def test_object_pool_clear():
    """Test pool clear operation.

    Verify that clear removes all objects from pool.
    """

    def create_object():
        return {"data": "test"}

    pool = ObjectPool(factory=create_object, max_size=10)

    # Add objects to pool
    objects = []
    for _ in range(5):
        obj = pool.acquire()
        objects.append(obj)

    # Release all objects
    for obj in objects:
        pool.release(obj)

    assert pool.size() == 5, "Pool should have 5 objects"

    # Clear pool
    pool.clear()

    assert pool.size() == 0, "Pool should be empty after clear"

    print("✅ Clear operation test passed")


def test_object_pool_invalid_max_size():
    """Test that invalid max_size raises error.

    Verify that pool validates max_size parameter.
    """

    def create_object():
        return {}

    try:
        ObjectPool(factory=create_object, max_size=0)
        raise AssertionError("Should raise ValueError for max_size=0")
    except ValueError as e:
        assert "positive" in str(e).lower()
        print("✅ Invalid max_size validation passed")

    try:
        ObjectPool(factory=create_object, max_size=-5)
        raise AssertionError("Should raise ValueError for negative max_size")
    except ValueError as e:
        assert "positive" in str(e).lower()
        print("✅ Negative max_size validation passed")


# ============================================================================
# StreamingIterator Unit Tests
# ============================================================================


def test_streaming_iterator_basic():
    """Test basic streaming functionality.

    Verify that streaming iterator works correctly.
    """

    streamer = StreamingIterator(chunk_size=10)
    data = list(range(25))

    chunks = list(streamer.stream_products(data))

    assert len(chunks) == 3, "Should have 3 chunks"
    assert len(chunks[0]) == 10, "First chunk should have 10 items"
    assert len(chunks[1]) == 10, "Second chunk should have 10 items"
    assert len(chunks[2]) == 5, "Third chunk should have 5 items"

    # Verify all items are present
    all_items = []
    for chunk in chunks:
        all_items.extend(chunk)
    assert all_items == data, "All items should be present"

    print("✅ Basic streaming test passed")


def test_streaming_iterator_invalid_chunk_size():
    """Test that invalid chunk_size raises error.

    Verify that streaming iterator validates chunk_size parameter.
    """

    try:
        StreamingIterator(chunk_size=0)
        raise AssertionError("Should raise ValueError for chunk_size=0")
    except ValueError as e:
        assert "positive" in str(e).lower()
        print("✅ Invalid chunk_size validation passed")

    try:
        StreamingIterator(chunk_size=-10)
        raise AssertionError("Should raise ValueError for negative chunk_size")
    except ValueError as e:
        assert "positive" in str(e).lower()
        print("✅ Negative chunk_size validation passed")


# ============================================================================
# TunedLRUCache Unit Tests
# ============================================================================


def test_lru_cache_basic():
    """Test basic cache operations.

    Verify that cache get/put operations work correctly.
    """

    cache = TunedLRUCache(max_size_mb=10)

    # Put and get
    cache.put("key1", "value1")
    assert cache.get("key1") == "value1", "Should retrieve stored value"

    # Get non-existing key
    assert cache.get("key2") is None, "Should return None for missing key"

    # Update existing key
    cache.put("key1", "new_value")
    assert cache.get("key1") == "new_value", "Should retrieve updated value"

    print("✅ Basic cache operations test passed")


def test_lru_cache_invalid_parameters():
    """Test that invalid parameters raise errors.

    Verify that cache validates parameters.
    """

    # Invalid max_size_mb
    try:
        TunedLRUCache(max_size_mb=0)
        raise AssertionError("Should raise ValueError for max_size_mb=0")
    except ValueError as e:
        assert "positive" in str(e).lower()
        print("✅ Invalid max_size_mb validation passed")

    # Invalid eviction_threshold
    try:
        TunedLRUCache(max_size_mb=10, eviction_threshold=0)
        raise AssertionError("Should raise ValueError for eviction_threshold=0")
    except ValueError as e:
        assert "between 0 and 1" in str(e).lower()
        print("✅ Invalid eviction_threshold (0) validation passed")

    try:
        TunedLRUCache(max_size_mb=10, eviction_threshold=1.5)
        raise AssertionError("Should raise ValueError for eviction_threshold>1")
    except ValueError as e:
        assert "between 0 and 1" in str(e).lower()
        print("✅ Invalid eviction_threshold (>1) validation passed")


if __name__ == "__main__":
    print("Running Memory Optimization Unit Tests...\n")

    print("=" * 60)
    print("ObjectPool Tests")
    print("=" * 60)
    test_object_pool_acquire_release_cycle()
    test_object_pool_size_limits()
    test_object_pool_thread_safety()
    test_object_pool_stats()
    test_object_pool_clear()
    test_object_pool_invalid_max_size()

    print("\n" + "=" * 60)
    print("StreamingIterator Tests")
    print("=" * 60)
    test_streaming_iterator_basic()
    test_streaming_iterator_invalid_chunk_size()

    print("\n" + "=" * 60)
    print("TunedLRUCache Tests")
    print("=" * 60)
    test_lru_cache_basic()
    test_lru_cache_invalid_parameters()

    print("\n" + "=" * 60)
    print("✅ All unit tests passed!")
    print("=" * 60)
