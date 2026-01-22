"""
Multi-level Cache System cho Tiki Crawl Pipeline

Cấu trúc 3 tầng:
- L1: In-memory LRU cache (nhanh nhất, giới hạn size)
- L2: Redis cache (distributed, persistent trong memory)
- L3: File cache (persistent trên disk, fallback)

Strategy: Check L1 -> L2 -> L3, write to all levels
"""

import gzip
import hashlib
import json
import time
from collections import OrderedDict
from pathlib import Path
from typing import Any

try:
    import redis

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None


class LRUCache:
    """In-memory LRU cache (L1)"""

    def __init__(self, maxsize: int = 1000):
        """
        Args:
            maxsize: Số lượng items tối đa trong cache
        """
        self.maxsize = maxsize
        self.cache: OrderedDict = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Any | None:
        """Lấy giá trị từ cache"""
        if key in self.cache:
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            self.hits += 1
            return self.cache[key]
        self.misses += 1
        return None

    def set(self, key: str, value: Any) -> None:
        """Lưu giá trị vào cache"""
        if key in self.cache:
            # Update existing
            self.cache.move_to_end(key)
        else:
            # Add new, remove oldest if full
            if len(self.cache) >= self.maxsize:
                self.cache.popitem(last=False)  # Remove oldest
        self.cache[key] = value

    def delete(self, key: str) -> bool:
        """Xóa key khỏi cache"""
        if key in self.cache:
            del self.cache[key]
            return True
        return False

    def clear(self) -> None:
        """Xóa toàn bộ cache"""
        self.cache.clear()
        self.hits = 0
        self.misses = 0

    def stats(self) -> dict:
        """Lấy thống kê cache"""
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0
        return {
            "size": len(self.cache),
            "maxsize": self.maxsize,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": f"{hit_rate:.2f}%",
        }


class FileCache:
    """File-based cache với compression (L3)"""

    def __init__(self, cache_dir: str | Path, compress: bool = True):
        """
        Args:
            cache_dir: Thư mục lưu cache files
            compress: Có nén file không (gzip)
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.compress = compress
        self.extension = ".json.gz" if compress else ".json"

    def _get_filepath(self, key: str) -> Path:
        """Lấy đường dẫn file từ key"""
        # Hash key để tránh tên file quá dài
        key_hash = hashlib.md5(key.encode()).hexdigest()
        return self.cache_dir / f"{key_hash}{self.extension}"

    def get(self, key: str) -> Any | None:
        """Lấy giá trị từ file cache"""
        filepath = self._get_filepath(key)
        if not filepath.exists():
            return None

        try:
            if self.compress:
                with gzip.open(filepath, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)

            # Check expiration
            if "expires_at" in data:
                if time.time() > data["expires_at"]:
                    filepath.unlink()  # Delete expired file
                    return None

            return data.get("value")
        except Exception:
            # Nếu lỗi, xóa file corrupt
            try:
                filepath.unlink()
            except Exception:
                pass
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """Lưu giá trị vào file cache"""
        filepath = self._get_filepath(key)
        try:
            data = {
                "value": value,
                "cached_at": time.time(),
            }
            if ttl:
                data["expires_at"] = time.time() + ttl

            if self.compress:
                with gzip.open(filepath, "wt", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
            else:
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
            return True
        except Exception:
            return False

    def delete(self, key: str) -> bool:
        """Xóa file cache"""
        filepath = self._get_filepath(key)
        try:
            if filepath.exists():
                filepath.unlink()
                return True
            return False
        except Exception:
            return False

    def exists(self, key: str) -> bool:
        """Kiểm tra file có tồn tại không"""
        return self._get_filepath(key).exists()


class MultiLevelCache:
    """
    Multi-level cache system với 3 tầng:
    - L1: In-memory LRU (nhanh nhất)
    - L2: Redis (distributed)
    - L3: File cache (persistent)
    """

    def __init__(
        self,
        l1_maxsize: int = 1000,
        redis_url: str = "redis://redis:6379/1",
        cache_dir: str | Path | None = None,
        default_ttl: int = 86400,
        enable_l1: bool = True,
        enable_l2: bool = True,
        enable_l3: bool = True,
        compress_l3: bool = True,
    ):
        """
        Args:
            l1_maxsize: Số items tối đa trong L1 cache
            redis_url: Redis connection URL cho L2
            cache_dir: Thư mục cho L3 file cache
            default_ttl: TTL mặc định (giây)
            enable_l1: Bật/tắt L1 cache
            enable_l2: Bật/tắt L2 cache
            enable_l3: Bật/tắt L3 cache
            compress_l3: Nén file trong L3
        """
        # L1: In-memory LRU
        self.l1 = LRUCache(maxsize=l1_maxsize) if enable_l1 else None

        # L2: Redis
        self.l2 = None
        if enable_l2 and REDIS_AVAILABLE:
            try:
                from .redis_cache import RedisCache

                self.l2 = RedisCache(redis_url=redis_url, default_ttl=default_ttl)
            except Exception:
                pass

        # L3: File cache
        self.l3 = None
        if enable_l3:
            if cache_dir is None:
                try:
                    from ..utils import DEFAULT_CACHE_DIR

                    cache_dir = DEFAULT_CACHE_DIR
                except ImportError:
                    # Fallback to a safe relative path if utils cannot be imported
                    from pathlib import Path

                    cache_dir = Path("data/raw/cache")
            self.l3 = FileCache(cache_dir=cache_dir, compress=compress_l3)

        self.default_ttl = default_ttl
        self.stats = {
            "l1_hits": 0,
            "l2_hits": 0,
            "l3_hits": 0,
            "misses": 0,
            "writes": 0,
        }

    def get(self, key: str) -> Any | None:
        """
        Lấy giá trị từ cache (check L1 -> L2 -> L3)

        Args:
            key: Cache key

        Returns:
            Giá trị từ cache hoặc None
        """
        # L1: In-memory
        if self.l1:
            value = self.l1.get(key)
            if value is not None:
                self.stats["l1_hits"] += 1
                # Promote to L1 if found in lower levels
                return value

        # L2: Redis
        if self.l2:
            try:
                value = self.l2.get(key)
                if value is not None:
                    self.stats["l2_hits"] += 1
                    # Promote to L1
                    if self.l1:
                        self.l1.set(key, value)
                    return value
            except Exception:
                pass

        # L3: File cache
        if self.l3:
            value = self.l3.get(key)
            if value is not None:
                self.stats["l3_hits"] += 1
                # Promote to L1 and L2
                if self.l1:
                    self.l1.set(key, value)
                if self.l2:
                    try:
                        self.l2.set(key, value, ttl=self.default_ttl)
                    except Exception:
                        pass
                return value

        self.stats["misses"] += 1
        return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        """
        Lưu giá trị vào tất cả các tầng cache

        Args:
            key: Cache key
            value: Giá trị cần cache
            ttl: Time to live (giây), None = dùng default_ttl

        Returns:
            True nếu lưu thành công ít nhất 1 tầng
        """
        success = False
        ttl = ttl or self.default_ttl

        # L1: In-memory
        if self.l1:
            try:
                self.l1.set(key, value)
                success = True
            except Exception:
                pass

        # L2: Redis
        if self.l2:
            try:
                if self.l2.set(key, value, ttl=ttl):
                    success = True
            except Exception:
                pass

        # L3: File cache
        if self.l3:
            try:
                if self.l3.set(key, value, ttl=ttl):
                    success = True
            except Exception:
                pass

        if success:
            self.stats["writes"] += 1

        return success

    def delete(self, key: str) -> bool:
        """Xóa key khỏi tất cả các tầng cache"""
        results = []

        if self.l1:
            results.append(self.l1.delete(key))

        if self.l2:
            try:
                results.append(self.l2.delete(key))
            except Exception:
                pass

        if self.l3:
            try:
                results.append(self.l3.delete(key))
            except Exception:
                pass

        return any(results)

    def exists(self, key: str) -> bool:
        """Kiểm tra key có tồn tại trong bất kỳ tầng nào"""
        if self.l1 and self.l1.get(key) is not None:
            return True
        if self.l2:
            try:
                if self.l2.exists(key):
                    return True
            except Exception:
                pass
        if self.l3:
            try:
                if self.l3.exists(key):
                    return True
            except Exception:
                pass
        return False

    def clear(self) -> None:
        """Xóa toàn bộ cache ở tất cả các tầng"""
        if self.l1:
            self.l1.clear()
        if self.l2:
            try:
                # Redis không có clear all, cần xóa từng key hoặc flushdb
                pass
            except Exception:
                pass
        # L3: Không clear file cache (có thể rất nhiều files)

    def get_cache_key(self, url: str, cache_type: str = "html") -> str:
        """Tạo cache key từ URL"""
        url_hash = hashlib.md5(url.encode()).hexdigest()
        return f"{cache_type}:{url_hash}"

    def cache_html(self, url: str, html: str, ttl: int | None = None) -> bool:
        """Cache HTML content"""
        key = self.get_cache_key(url, "html")
        return self.set(key, {"url": url, "html": html, "cached_at": time.time()}, ttl=ttl)

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
            key,
            {"category_url": category_url, "products": products, "cached_at": time.time()},
            ttl=ttl,
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
            key, {"product_id": product_id, "detail": detail, "cached_at": time.time()}, ttl=ttl
        )

    def get_cached_product_detail(self, product_id: str) -> dict | None:
        """Lấy cached product detail"""
        key = f"detail:{product_id}"
        cached = self.get(key)
        if cached:
            return cached.get("detail")
        return None

    def get_stats(self) -> dict:
        """Lấy thống kê cache"""
        stats = self.stats.copy()

        if self.l1:
            stats["l1"] = self.l1.stats()

        total_requests = sum(
            [stats["l1_hits"], stats["l2_hits"], stats["l3_hits"], stats["misses"]]
        )

        if total_requests > 0:
            stats["overall_hit_rate"] = (
                f"{(total_requests - stats['misses']) / total_requests * 100:.2f}%"
            )
        else:
            stats["overall_hit_rate"] = "0%"

        return stats


# Singleton instance
_multi_level_cache_instance: MultiLevelCache | None = None


def get_multi_level_cache(
    l1_maxsize: int = 1000,
    redis_url: str = "redis://redis:6379/1",
    cache_dir: str | Path | None = None,
    default_ttl: int = 86400,
    enable_l1: bool = True,
    enable_l2: bool = True,
    enable_l3: bool = True,
    compress_l3: bool = True,
) -> MultiLevelCache:
    """
    Lấy MultiLevelCache instance (singleton)

    Args:
        l1_maxsize: Số items tối đa trong L1 cache
        redis_url: Redis connection URL cho L2
        cache_dir: Thư mục cho L3 file cache
        default_ttl: TTL mặc định (giây)
        enable_l1: Bật/tắt L1 cache
        enable_l2: Bật/tắt L2 cache
        enable_l3: Bật/tắt L3 cache
        compress_l3: Nén file trong L3

    Returns:
        MultiLevelCache instance
    """
    global _multi_level_cache_instance

    if _multi_level_cache_instance is None:
        _multi_level_cache_instance = MultiLevelCache(
            l1_maxsize=l1_maxsize,
            redis_url=redis_url,
            cache_dir=cache_dir,
            default_ttl=default_ttl,
            enable_l1=enable_l1,
            enable_l2=enable_l2,
            enable_l3=enable_l3,
            compress_l3=compress_l3,
        )

    return _multi_level_cache_instance
