"""Redis Cache và Rate Limiting cho Tiki Crawl Pipeline.

Các tính năng:
1. Caching HTML/JSON responses để tránh crawl lại
2. Distributed rate limiting
3. Distributed locking để tránh crawl trùng lặp
4. Session storage
5. Connection pooling for better performance
"""

import hashlib
import json
import time
from collections.abc import Iterable
from typing import Any
from urllib.parse import (
    parse_qsl,
    urlencode,
    urlparse,
    urlsplit,
    urlunsplit,
)

try:
    import redis
    from redis import ConnectionPool

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None
    ConnectionPool = None

# Global connection pool (shared across instances)
_connection_pools = {}


def get_redis_pool(redis_url: str, max_connections: int = 20) -> ConnectionPool:
    """Get or create a connection pool for the given Redis URL.

    Args:
        redis_url: Redis connection URL
        max_connections: Maximum number of connections in the pool

    Returns:
        ConnectionPool instance
    """
    if redis_url not in _connection_pools:
        parsed = urlparse(redis_url)
        _connection_pools[redis_url] = ConnectionPool(
            host=parsed.hostname or "localhost",
            port=parsed.port or 6379,
            db=int(parsed.path.lstrip("/")) if parsed.path else 0,
            max_connections=max_connections,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )
    return _connection_pools[redis_url]


class RedisCache:
    """
    Redis cache wrapper với connection pooling cho crawl pipeline.
    """

    def __init__(self, redis_url: str = "redis://redis:6379/1", default_ttl: int = 86400):
        """
        Args:
            redis_url: Redis connection URL (database 1 cho cache, database 0 đã dùng cho Celery)
            default_ttl: Time to live mặc định (giây), mặc định 24 giờ
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")

        # Use connection pool for better performance
        pool = get_redis_pool(redis_url)
        self.client = redis.Redis(connection_pool=pool)
        self.default_ttl = default_ttl
        self.prefix = "tiki:crawl:"

    def _canonicalize_url(self, url: str, drop_params: Iterable[str] | None = None) -> str:
        """Chuẩn hóa URL để tối đa hóa cache hit rate.

        - Lowercase scheme/host
        - Force https cho tiki.vn
        - Remove URL fragment (#...)
        - Sort query params for deterministic order
        - Drop tracking params (utm_*, ref, referrer, src, spm) and any in drop_params
        """
        try:
            drop = set(drop_params or [])
            default_drops = {
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "ref",
                "referrer",
                "src",
                "spm",
            }
            drop |= default_drops

            parts = urlsplit(url)
            scheme = (parts.scheme or "https").lower()
            netloc = (parts.netloc or "").lower()

            # Force https for tiki.vn
            if netloc.endswith("tiki.vn"):
                scheme = "https"

            # Normalize path (remove trailing slashes except root)
            path = parts.path or "/"
            if len(path) > 1 and path.endswith("/"):
                path = path.rstrip("/")

            # Normalize and filter query params
            query_items = []
            for k, v in parse_qsl(parts.query, keep_blank_values=False):
                if k in drop or k.startswith("utm_"):
                    continue
                # Skip empty values
                if v is None or v == "":
                    continue
                query_items.append((k, v))

            # Sort for deterministic ordering
            query_items.sort(key=lambda kv: (kv[0], kv[1]))
            query = urlencode(query_items, doseq=True)

            return urlunsplit((scheme, netloc, path, query, ""))
        except Exception:
            # Nếu có lỗi, trả về URL gốc để không chặn luồng
            return url

    def _make_key(self, key: str) -> str:
        """
        Tạo key với prefix.
        """
        return f"{self.prefix}{key}"

    def get(self, key: str) -> Any | None:
        """
        Lấy giá trị từ cache.
        """
        try:
            value = self.client.get(self._make_key(key))
            if value:
                return json.loads(value)
            return None
        except Exception:
            # Nếu lỗi, return None để fallback về file cache
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """
        Lưu giá trị vào cache.
        """
        try:
            ttl = ttl or self.default_ttl
            serialized = json.dumps(value, ensure_ascii=False)
            return self.client.setex(self._make_key(key), ttl, serialized)
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        """
        Xóa key khỏi cache.
        """
        try:
            return bool(self.client.delete(self._make_key(key)))
        except Exception:
            return False

    def exists(self, key: str) -> bool:
        """
        Kiểm tra key có tồn tại không.
        """
        try:
            return bool(self.client.exists(self._make_key(key)))
        except Exception:
            return False

    def _new_key_for_url(
        self, url: str, cache_type: str = "html", drop_params: Iterable[str] | None = None
    ) -> str:
        canonical = self._canonicalize_url(url, drop_params=drop_params)
        url_hash = hashlib.md5(canonical.encode()).hexdigest()
        return f"{cache_type}:{url_hash}"

    def _legacy_key_for_url(self, url: str, cache_type: str = "html") -> str:
        # Legacy behavior: hash nguyên URL không chuẩn hóa
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return f"{cache_type}:{url_hash}"

    def get_cache_key(self, url: str, cache_type: str = "html") -> str:
        """
        Giữ API cũ: trả về key mới (đã canonicalize).
        """
        return self._new_key_for_url(url, cache_type)

    def cache_html(self, url: str, html: str, ttl: int | None = None) -> bool:
        """
        Cache HTML content.
        """
        key = self._new_key_for_url(url, "html")
        return self.set(key, {"url": url, "html": html, "cached_at": time.time()}, ttl)

    def get_cached_html(self, url: str) -> str | None:
        """
        Lấy cached HTML.
        """
        # Thử key mới (canonical) trước, sau đó fallback legacy key để không mất cache cũ
        for key in (
            self._new_key_for_url(url, "html"),
            self._legacy_key_for_url(url, "html"),
        ):
            cached = self.get(key)
            if cached:
                return cached.get("html")
        return None

    def cache_products(self, category_url: str, products: list, ttl: int | None = None) -> bool:
        """
        Cache products từ category.
        """
        # Với category, bỏ tham số trang để gom cache theo danh mục
        key = self._new_key_for_url(category_url, "products", drop_params={"page"})
        return self.set(
            key, {"category_url": category_url, "products": products, "cached_at": time.time()}, ttl
        )

    def get_cached_products(self, category_url: str) -> list | None:
        """
        Lấy cached products.
        """
        # Thử key mới (bỏ page) trước, sau đó legacy key
        for key in (
            self._new_key_for_url(category_url, "products", drop_params={"page"}),
            self._legacy_key_for_url(category_url, "products"),
        ):
            cached = self.get(key)
            if cached:
                return cached.get("products")
        return None

    def cache_product_detail(self, product_id: str, detail: dict, ttl: int | None = None) -> bool:
        """
        Cache product detail.
        """
        key = f"detail:{product_id}"
        return self.set(
            key, {"product_id": product_id, "detail": detail, "cached_at": time.time()}, ttl
        )

    def get_cached_product_detail(self, product_id: str) -> dict | None:
        """
        Lấy cached product detail.
        """
        key = f"detail:{product_id}"
        cached = self.get(key)
        if cached:
            return cached.get("detail")
        return None

    def validate_product_detail(self, detail: dict | None, min_fields: list | None = None) -> bool:
        """Kiểm tra product detail có hợp lệ không (flexible validation).

        Args:
            detail: Product detail dict để validate
            min_fields: List các fields cần ít nhất một cái (default: ["price", "sales_count", "name", "brand"])

        Returns:
            True nếu valid (có ít nhất một field từ min_fields VÀ có brand), False nếu None hoặc không đủ
        """
        if detail is None:
            return False

        # CRITICAL: Bắt buộc phải có brand
        # Brand thiếu thường dẫn đến nhiều trường khác cũng thiếu
        brand = detail.get("brand")
        if not brand or (isinstance(brand, str) and not brand.strip()):
            return False

        # Default validation fields
        if min_fields is None:
            min_fields = ["price", "sales_count", "name"]

        # Check nếu có ít nhất 1 trong các field yêu cầu
        for field in min_fields:
            if field == "price":
                # Check nested: detail.price.current_price
                if detail.get("price", {}).get("current_price"):
                    return True
            elif field == "sales_count":
                if detail.get("sales_count") is not None:
                    return True
            elif field == "name":
                if detail.get("name"):
                    return True
            else:
                # Generic field check
                if detail.get(field):
                    return True

        return False

    def get_product_detail_with_validation(
        self, product_id: str, min_fields: list | None = None
    ) -> tuple[dict | None, bool]:
        """Lấy product detail từ cache với validation.

        Returns:
            Tuple[detail_dict or None, is_valid_bool]
        """
        detail = self.get_cached_product_detail(product_id)
        is_valid = self.validate_product_detail(detail, min_fields)
        return detail, is_valid


class RedisRateLimiter:
    """
    Distributed rate limiter sử dụng Redis.
    """

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
        """
        Tạo key với prefix.
        """
        return f"{self.prefix}{identifier}"

    def is_allowed(self, identifier: str = "default") -> bool:
        """Kiểm tra xem request có được phép không.

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
        """
        Đợi nếu cần thiết để không vượt quá rate limit.
        """
        if not self.is_allowed(identifier):
            # Tính thời gian cần đợi
            key = self._make_key(identifier)
            ttl = self.client.ttl(key)
            if ttl > 0:
                time.sleep(min(ttl, self.window))

    def reset(self, identifier: str = "default") -> None:
        """
        Reset rate limit cho identifier.
        """
        try:
            self.client.delete(self._make_key(identifier))
        except Exception:
            pass


class RedisLock:
    """
    Distributed lock sử dụng Redis để tránh crawl trùng lặp.
    """

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
        """
        Tạo key với prefix.
        """
        return f"{self.prefix}{key}"

    def acquire(self, key: str, timeout: int | None = None, blocking: bool = True) -> bool:
        """Acquire lock.

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
        """
        Release lock.
        """
        try:
            return bool(self.client.delete(self._make_key(key)))
        except Exception:
            return False

    def __enter__(self):
        """
        Context manager entry.
        """
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """
        Context manager exit.
        """


# Singleton instances (optional, có thể tạo mới mỗi lần)
_redis_cache_instance = None
_redis_rate_limiter_instance = None
_redis_lock_instance = None


def get_redis_cache(redis_url: str = "redis://redis:6379/1") -> RedisCache | None:
    """
    Lấy Redis cache instance (singleton)
    """
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
    """
    Lấy Redis rate limiter instance (singleton)
    """
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
    """
    Lấy Redis lock instance (singleton)
    """
    global _redis_lock_instance
    if not REDIS_AVAILABLE:
        return None
    if _redis_lock_instance is None:
        try:
            _redis_lock_instance = RedisLock(redis_url)
        except Exception:
            return None
    return _redis_lock_instance
