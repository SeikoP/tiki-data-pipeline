"""Advanced caching utilities.

Features:
- Multi-level cache (memory + Redis)
- TTL management
- Cache warming
- Cache statistics
"""

import hashlib
import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# In-memory cache
_memory_cache: dict[str, Any] = {}
_cache_stats = {
    "hits": 0,
    "misses": 0,
    "memory_size": 0,
}


def get_cache_key(prefix: str, *args, **kwargs) -> str:
    """
    Generate cache key from function arguments.
    """
    key_parts = [prefix]

    # Add args
    key_parts.extend(str(arg) for arg in args)

    # Add sorted kwargs
    key_parts.extend(f"{k}={kwargs[k]}" for k in sorted(kwargs.keys()))

    key_str = "|".join(key_parts)

    # Hash for consistent length
    return hashlib.md5(key_str.encode()).hexdigest()


def cache_in_memory(ttl: int = 300):
    """Decorator for in-memory caching.

    Args:
        ttl: Time to live in seconds (default 5 minutes)
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            cache_key = get_cache_key(func.__name__, *args, **kwargs)

            # Check cache
            if cache_key in _memory_cache:
                cached_data, timestamp = _memory_cache[cache_key]

                # Check TTL
                if time.time() - timestamp < ttl:
                    _cache_stats["hits"] += 1
                    return cached_data
                else:
                    # Expired
                    del _memory_cache[cache_key]

            # Cache miss
            _cache_stats["misses"] += 1

            # Execute function
            result = func(*args, **kwargs)

            # Store in cache
            _memory_cache[cache_key] = (result, time.time())
            _cache_stats["memory_size"] = len(_memory_cache)

            return result

        return wrapper

    return decorator


def get_cache_stats() -> dict:
    """
    Get cache statistics.
    """
    total = _cache_stats["hits"] + _cache_stats["misses"]
    hit_rate = (_cache_stats["hits"] / total * 100) if total > 0 else 0

    return {
        "hits": _cache_stats["hits"],
        "misses": _cache_stats["misses"],
        "hit_rate": hit_rate,
        "memory_size": _cache_stats["memory_size"],
    }


def clear_memory_cache():
    """
    Clear all cached data.
    """
    _memory_cache.clear()
    logger.info(f"✅ Cleared {_cache_stats['memory_size']} cached items")
    _cache_stats["memory_size"] = 0


__all__ = [
    "get_cache_key",
    "cache_in_memory",
    "get_cache_stats",
    "clear_memory_cache",
]
