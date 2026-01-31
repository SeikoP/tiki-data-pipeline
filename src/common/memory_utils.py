"""Memory optimization utilities.

Features:
- Memory profiling
- Garbage collection management
- Large object detection
- Memory-efficient data structures
- Streaming iterators for chunked processing
- Object pooling for reuse
- LRU cache with memory limits
"""

import gc
import sys
import threading
from collections import OrderedDict, deque
from typing import Any, Callable, Iterable, Iterator, List, Optional, TypeVar


def get_object_size(obj: Any) -> int:
    """
    Get approximate size of object in bytes.
    """
    return sys.getsizeof(obj)


def profile_memory() -> dict[str, Any]:
    """Get current memory usage statistics.

    Returns:
        dict with memory stats
    """
    import os

    import psutil

    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()

    return {
        "rss_mb": mem_info.rss / 1024 / 1024,  # Resident Set Size
        "vms_mb": mem_info.vms / 1024 / 1024,  # Virtual Memory Size
        "percent": process.memory_percent(),
    }


def force_gc():
    """
    Force garbage collection.
    """
    collected = gc.collect()
    return collected


def get_large_objects(limit: int = 10) -> list[tuple]:
    """Get largest objects in memory.

    Args:
        limit: Number of objects to return

    Returns:
        List of (size, type, repr) tuples
    """
    objects = []

    for obj in gc.get_objects():
        try:
            size = sys.getsizeof(obj)
            objects.append((size, type(obj).__name__, str(obj)[:100]))
        except Exception:
            continue

    objects.sort(reverse=True, key=lambda x: x[0])
    return objects[:limit]


def optimize_dataframe_memory(df):
    """Optimize pandas DataFrame memory usage.

    Args:
        df: pandas DataFrame

    Returns:
        Optimized DataFrame
    """
    try:
        import pandas  # noqa: F401
    except ImportError:
        return df

    for col in df.columns:
        col_type = df[col].dtype

        if str(col_type) != "object":
            c_min = df[col].min()
            c_max = df[col].max()

            if str(col_type)[:3] == "int":
                if c_min > -128 and c_max < 127:
                    df[col] = df[col].astype("int8")
                elif c_min > -32768 and c_max < 32767:
                    df[col] = df[col].astype("int16")
                elif c_min > -2147483648 and c_max < 2147483647:
                    df[col] = df[col].astype("int32")

            elif str(col_type)[:5] == "float":
                if c_min > -3.4e38 and c_max < 3.4e38:
                    df[col] = df[col].astype("float32")

    return df


__all__ = [
    "get_object_size",
    "profile_memory",
    "force_gc",
    "get_large_objects",
    "optimize_dataframe_memory",
    "StreamingIterator",
    "ObjectPool",
    "TunedLRUCache",
]


T = TypeVar('T')


class StreamingIterator:
    """Processes data in chunks without loading everything into memory.
    
    This iterator yields chunks of configurable size, ensuring constant
    memory usage regardless of dataset size. Ideal for processing large
    datasets that don't fit in memory.
    
    Example:
        >>> products = [product1, product2, ..., product1000000]
        >>> streamer = StreamingIterator(chunk_size=1000)
        >>> for chunk in streamer.stream_products(products):
        ...     process_chunk(chunk)  # Process 1000 products at a time
    """
    
    def __init__(self, chunk_size: int = 1000):
        """Initialize streaming iterator.
        
        Args:
            chunk_size: Number of items per chunk (default: 1000)
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.chunk_size = chunk_size
    
    def stream_products(self, source: Iterable[T]) -> Iterator[List[T]]:
        """Yield chunks of products for processing.
        
        Advantages:
        - Constant memory usage regardless of dataset size
        - Enables pipeline parallelism (crawl while transforming)
        - Reduces GC pressure
        
        Args:
            source: Iterable source of products
            
        Yields:
            Lists of products, each containing up to chunk_size items
        """
        chunk = []
        for item in source:
            chunk.append(item)
            if len(chunk) >= self.chunk_size:
                yield chunk
                chunk = []
        
        # Yield remaining items
        if chunk:
            yield chunk



class ObjectPool:
    """Reuses objects to reduce allocation overhead.
    
    This pool maintains a collection of reusable objects, reducing the
    overhead of creating and destroying objects repeatedly. Thread-safe
    for concurrent access.
    
    Example:
        >>> def create_driver():
        ...     return SeleniumDriver()
        >>> pool = ObjectPool(factory=create_driver, max_size=10)
        >>> driver = pool.acquire()
        >>> # Use driver
        >>> pool.release(driver)
    """
    
    def __init__(self, factory: Callable[[], Any], max_size: int = 100):
        """Initialize object pool.
        
        Args:
            factory: Function that creates new objects
            max_size: Maximum number of pooled objects (default: 100)
        """
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self.factory = factory
        self.max_size = max_size
        self.pool: deque = deque()
        self.lock = threading.Lock()
        self._created_count = 0
        self._acquired_count = 0
        self._released_count = 0
    
    def acquire(self) -> Any:
        """Get object from pool or create new one.
        
        Returns:
            Object from pool or newly created object
        """
        with self.lock:
            self._acquired_count += 1
            if self.pool:
                return self.pool.popleft()
        
        # Create new object outside lock to avoid blocking
        self._created_count += 1
        return self.factory()
    
    def release(self, obj: Any) -> None:
        """Return object to pool for reuse.
        
        Args:
            obj: Object to return to pool
        """
        with self.lock:
            self._released_count += 1
            if len(self.pool) < self.max_size:
                self.pool.append(obj)
    
    def size(self) -> int:
        """Get current pool size.
        
        Returns:
            Number of objects currently in pool
        """
        with self.lock:
            return len(self.pool)
    
    def stats(self) -> dict:
        """Get pool statistics.
        
        Returns:
            Dictionary with pool statistics
        """
        with self.lock:
            return {
                "pool_size": len(self.pool),
                "max_size": self.max_size,
                "created_count": self._created_count,
                "acquired_count": self._acquired_count,
                "released_count": self._released_count,
            }
    
    def clear(self) -> None:
        """Clear all objects from pool."""
        with self.lock:
            self.pool.clear()



class TunedLRUCache:
    """LRU cache with configurable size and eviction policy.
    
    This cache implements Least Recently Used (LRU) eviction with memory
    size tracking. When the cache exceeds the configured memory threshold,
    the least recently used items are evicted.
    
    Example:
        >>> cache = TunedLRUCache(max_size_mb=100, eviction_threshold=0.9)
        >>> cache.put("key1", large_object)
        >>> value = cache.get("key1")  # Moves key1 to end (most recently used)
    """
    
    def __init__(
        self,
        max_size_mb: int = 100,
        eviction_threshold: float = 0.9
    ):
        """Initialize cache with memory limits.
        
        Args:
            max_size_mb: Maximum cache size in megabytes (default: 100)
            eviction_threshold: Trigger eviction at this % of max_size (default: 0.9)
        """
        if max_size_mb <= 0:
            raise ValueError("max_size_mb must be positive")
        if not 0 < eviction_threshold <= 1:
            raise ValueError("eviction_threshold must be between 0 and 1")
        
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.eviction_threshold = eviction_threshold
        self.cache: OrderedDict = OrderedDict()
        self.current_size = 0
        self.lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get value and move to end (most recently used).
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        with self.lock:
            if key in self.cache:
                self._hits += 1
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                return self.cache[key]
            self._misses += 1
            return None
    
    def put(self, key: str, value: Any) -> None:
        """Add value and evict if necessary.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        value_size = sys.getsizeof(value)
        
        with self.lock:
            # If key exists, remove old value size
            if key in self.cache:
                old_value = self.cache[key]
                self.current_size -= sys.getsizeof(old_value)
                del self.cache[key]
            
            # Evict if over threshold
            threshold_bytes = self.max_size_bytes * self.eviction_threshold
            while self.current_size + value_size > threshold_bytes and self.cache:
                # Remove least recently used (first item)
                evicted_key, evicted_value = self.cache.popitem(last=False)
                self.current_size -= sys.getsizeof(evicted_value)
                self._evictions += 1
            
            # Add new value
            self.cache[key] = value
            self.current_size += value_size
    
    def remove(self, key: str) -> bool:
        """Remove key from cache.
        
        Args:
            key: Cache key to remove
            
        Returns:
            True if key was removed, False if not found
        """
        with self.lock:
            if key in self.cache:
                value = self.cache[key]
                self.current_size -= sys.getsizeof(value)
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """Clear all items from cache."""
        with self.lock:
            self.cache.clear()
            self.current_size = 0
    
    def size(self) -> int:
        """Get number of items in cache.
        
        Returns:
            Number of cached items
        """
        with self.lock:
            return len(self.cache)
    
    def size_bytes(self) -> int:
        """Get current cache size in bytes.
        
        Returns:
            Current cache size in bytes
        """
        with self.lock:
            return self.current_size
    
    def stats(self) -> dict:
        """Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self.lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0.0
            
            return {
                "size": len(self.cache),
                "size_bytes": self.current_size,
                "size_mb": self.current_size / (1024 * 1024),
                "max_size_mb": self.max_size_bytes / (1024 * 1024),
                "utilization": self.current_size / self.max_size_bytes if self.max_size_bytes > 0 else 0.0,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "evictions": self._evictions,
            }
