"""
Storage utilities cho crawl pipeline
- PostgreSQL storage
- Redis cache
- Multi-level cache
- Compression utilities
"""

from .postgres_storage import PostgresStorage
from .redis_cache import (
    RedisCache,
    RedisLock,
    RedisRateLimiter,
    get_redis_cache,
    get_redis_lock,
    get_redis_rate_limiter,
)

# Optional imports (có thể fail nếu dependencies không có)
try:
    from .multi_level_cache import (  # noqa: F401
        FileCache,
        LRUCache,
        MultiLevelCache,
        get_multi_level_cache,
    )

    _MULTI_LEVEL_CACHE_AVAILABLE = True
except ImportError:
    _MULTI_LEVEL_CACHE_AVAILABLE = False

try:
    from .compression import (  # noqa: F401
        compress_json,
        decompress_json,
        get_compression_ratio,
        read_compressed_json,
        write_compressed_json,
    )

    _COMPRESSION_AVAILABLE = True
except ImportError:
    _COMPRESSION_AVAILABLE = False

__all__ = [
    "PostgresStorage",
    "RedisCache",
    "RedisRateLimiter",
    "RedisLock",
    "get_redis_cache",
    "get_redis_rate_limiter",
    "get_redis_lock",
]

if _MULTI_LEVEL_CACHE_AVAILABLE:
    __all__.extend(
        [
            "MultiLevelCache",
            "LRUCache",
            "FileCache",
            "get_multi_level_cache",
        ]
    )

if _COMPRESSION_AVAILABLE:
    __all__.extend(
        [
            "compress_json",
            "decompress_json",
            "write_compressed_json",
            "read_compressed_json",
            "get_compression_ratio",
        ]
    )
