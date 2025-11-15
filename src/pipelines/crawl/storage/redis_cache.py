"""
Redis Cache và Rate Limiting cho Tiki Crawl Pipeline

Các tính năng:
1. Caching HTML/JSON responses để tránh crawl lại
2. Distributed rate limiting
3. Distributed locking để tránh crawl trùng lặp
4. Session storage
"""

import hashlib
import json
import time
from typing import Any

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


class RedisCache:
    """Redis cache wrapper cho crawl pipeline"""

    def __init__(self, redis_url: str = "redis://redis:6379/1", default_ttl: int = 86400):
        """
        Args:
            redis_url: Redis connection URL (database 1 cho cache, database 0 đã dùng cho Celery)
            default_ttl: Time to live mặc định (giây), mặc định 24 giờ
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")

        self.client = redis.from_url(redis_url, decode_responses=True)
        self.default_ttl = default_ttl
        self.prefix = "tiki:crawl:"

    def _make_key(self, key: str) -> str:
        """Tạo key với prefix"""
        return f"{self.prefix}{key}"

    def get(self, key: str) -> Any | None:
        """Lấy giá trị từ cache"""
        try:
            value = self.client.get(self._make_key(key))
            if value:
                return json.loads(value)
            return None
        except Exception:
            # Nếu lỗi, return None để fallback về file cache
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Lưu giá trị vào cache"""
        try:
            ttl = ttl or self.default_ttl
            serialized = json.dumps(value, ensure_ascii=False)
            return self.client.setex(self._make_key(key), ttl, serialized)
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        """Xóa key khỏi cache"""
        try:
            return bool(self.client.delete(self._make_key(key)))
        except Exception:
            return False

    def exists(self, key: str) -> bool:
        """Kiểm tra key có tồn tại không"""
        try:
            return bool(self.client.exists(self._make_key(key)))
        except Exception:
            return False

    def get_cache_key(self, url: str, cache_type: str = "html") -> str:
        """Tạo cache key từ URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return f"{cache_type}:{url_hash}"

    def cache_html(self, url: str, html: str, ttl: int | None = None) -> bool:
        """Cache HTML content"""
        key = self.get_cache_key(url, "html")
        return self.set(key, {"url": url, "html": html, "cached_at": time.time()}, ttl)

    def get_cached_html(self, url: str) -> str | None:
        """Lấy cached HTML"""
        key = self.get_cache_key(url, "html")
        cached = self.get(key)
        if cached:
            return cached.get("html")
        return None

    def cache_products(self, category_url: str, products: list, ttl: int | None = None) -> bool:
        """Cache products từ category"""
        key = self.get_cache_key(category_url, "products")
        return self.set(
            key, {"category_url": category_url, "products": products, "cached_at": time.time()}, ttl
        )

    def get_cached_products(self, category_url: str) -> list | None:
        """Lấy cached products"""
        key = self.get_cache_key(category_url, "products")
        cached = self.get(key)
        if cached:
            return cached.get("products")
        return None

    def cache_product_detail(self, product_id: str, detail: dict, ttl: int | None = None) -> bool:
        """Cache product detail"""
        key = f"detail:{product_id}"
        return self.set(
            key, {"product_id": product_id, "detail": detail, "cached_at": time.time()}, ttl
        )

    def get_cached_product_detail(self, product_id: str) -> dict | None:
        """Lấy cached product detail"""
        key = f"detail:{product_id}"
        cached = self.get(key)
        if cached:
            return cached.get("detail")
        return None


class RedisRateLimiter:
    """Distributed rate limiter sử dụng Redis"""

    def __init__(
        self, redis_url: str = "redis://redis:6379/2", max_requests: int = 10, window: int = 60
    ):
        """
        Args:
            redis_url: Redis connection URL (database 2 cho rate limiting)
            max_requests: Số request tối đa trong window
            window: Thời gian window (giây)
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")

        self.client = redis.from_url(redis_url, decode_responses=True)
        self.max_requests = max_requests
        self.window = window
        self.prefix = "tiki:ratelimit:"

    def _make_key(self, identifier: str) -> str:
        """Tạo key với prefix"""
        return f"{self.prefix}{identifier}"

    def is_allowed(self, identifier: str = "default") -> bool:
        """
        Kiểm tra xem request có được phép không

        Args:
            identifier: Identifier cho rate limit (có thể là IP, domain, etc.)

        Returns:
            True nếu được phép, False nếu đã vượt quá limit
        """
        try:
            key = self._make_key(identifier)
            current = self.client.incr(key)

            if current == 1:
                # Set expiration cho lần đầu
                self.client.expire(key, self.window)

            return current <= self.max_requests
        except Exception:
            # Nếu Redis lỗi, cho phép request (fail open)
            return True

    def wait_if_needed(self, identifier: str = "default") -> None:
        """Đợi nếu cần thiết để không vượt quá rate limit"""
        if not self.is_allowed(identifier):
            # Tính thời gian cần đợi
            key = self._make_key(identifier)
            ttl = self.client.ttl(key)
            if ttl > 0:
                time.sleep(min(ttl, self.window))

    def reset(self, identifier: str = "default") -> None:
        """Reset rate limit cho identifier"""
        try:
            self.client.delete(self._make_key(identifier))
        except Exception:
            pass


class RedisLock:
    """Distributed lock sử dụng Redis để tránh crawl trùng lặp"""

    def __init__(self, redis_url: str = "redis://redis:6379/2", default_timeout: int = 300):
        """
        Args:
            redis_url: Redis connection URL
            default_timeout: Timeout mặc định cho lock (giây)
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")

        self.client = redis.from_url(redis_url, decode_responses=True)
        self.default_timeout = default_timeout
        self.prefix = "tiki:lock:"

    def _make_key(self, key: str) -> str:
        """Tạo key với prefix"""
        return f"{self.prefix}{key}"

    def acquire(self, key: str, timeout: int | None = None, blocking: bool = True) -> bool:
        """
        Acquire lock

        Args:
            key: Lock key
            timeout: Timeout cho lock (giây)
            blocking: Có đợi lock được release không

        Returns:
            True nếu acquire được, False nếu không
        """
        try:
            timeout = timeout or self.default_timeout
            lock_key = self._make_key(key)

            # Thử set với NX (chỉ set nếu không tồn tại) và EX (expiration)
            if blocking:
                # Đợi tối đa timeout giây
                end_time = time.time() + timeout
                while time.time() < end_time:
                    if self.client.set(lock_key, "locked", nx=True, ex=timeout):
                        return True
                    time.sleep(0.1)
                return False
            else:
                # Không đợi, thử một lần
                return bool(self.client.set(lock_key, "locked", nx=True, ex=timeout))
        except Exception:
            return False

    def release(self, key: str) -> bool:
        """Release lock"""
        try:
            return bool(self.client.delete(self._make_key(key)))
        except Exception:
            return False

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # noqa: ARG002
        """Context manager exit"""
        pass


# Singleton instances (optional, có thể tạo mới mỗi lần)
_redis_cache_instance = None
_redis_rate_limiter_instance = None
_redis_lock_instance = None


def get_redis_cache(redis_url: str = "redis://redis:6379/1") -> RedisCache | None:
    """Lấy Redis cache instance (singleton)"""
    global _redis_cache_instance
    if not REDIS_AVAILABLE:
        return None
    if _redis_cache_instance is None:
        try:
            _redis_cache_instance = RedisCache(redis_url)
        except Exception:
            return None
    return _redis_cache_instance


def get_redis_rate_limiter(redis_url: str = "redis://redis:6379/2") -> RedisRateLimiter | None:
    """Lấy Redis rate limiter instance (singleton)"""
    global _redis_rate_limiter_instance
    if not REDIS_AVAILABLE:
        return None
    if _redis_rate_limiter_instance is None:
        try:
            _redis_rate_limiter_instance = RedisRateLimiter(redis_url)
        except Exception:
            return None
    return _redis_rate_limiter_instance


def get_redis_lock(redis_url: str = "redis://redis:6379/2") -> RedisLock | None:
    """Lấy Redis lock instance (singleton)"""
    global _redis_lock_instance
    if not REDIS_AVAILABLE:
        return None
    if _redis_lock_instance is None:
        try:
            _redis_lock_instance = RedisLock(redis_url)
        except Exception:
            return None
    return _redis_lock_instance
