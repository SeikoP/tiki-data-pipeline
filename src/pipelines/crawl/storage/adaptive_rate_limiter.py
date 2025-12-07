"""
Adaptive Rate Limiter - Tự động điều chỉnh delay dựa trên success/error rate

Thay vì dùng fixed delay, adaptive rate limiter sẽ:
- Giảm delay khi success rate cao và không có errors
- Tăng delay khi detect errors (429, timeouts)
- Tự động tối ưu để đạt tốc độ cao nhất mà không bị block
"""

import time
from collections import deque
from typing import Any

try:
    import redis
    from redis import ConnectionPool

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None
    ConnectionPool = None


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter với tự động điều chỉnh delay
    """

    def __init__(
        self,
        redis_url: str = "redis://redis:6379/2",
        initial_delay: float = 0.7,
        min_delay: float = 0.3,
        max_delay: float = 2.0,
        success_window: int = 100,
        error_threshold: float = 0.02,  # 2% error rate
    ):
        """
        Args:
            redis_url: Redis connection URL
            initial_delay: Delay ban đầu (giây)
            min_delay: Delay tối thiểu (giây) - aggressive mode
            max_delay: Delay tối đa (giây) - conservative mode
            success_window: Số requests để track success/error
            error_threshold: Error rate threshold để tăng delay (0.02 = 2%)
        """
        if not REDIS_AVAILABLE:
            raise ImportError("Redis chưa được cài đặt. Cài đặt: pip install redis")

        self.client = redis.from_url(redis_url, decode_responses=True)
        self.current_delay = initial_delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.success_window = success_window
        self.error_threshold = error_threshold

        # Redis keys để track stats
        self.stats_prefix = "tiki:ratelimit:stats:"
        self.delay_key = "tiki:ratelimit:adaptive:delay"

        # Load current delay từ Redis (nếu có)
        try:
            saved_delay = self.client.get(self.delay_key)
            if saved_delay:
                self.current_delay = float(saved_delay)
        except Exception:
            pass

    def _get_stats_key(self, identifier: str) -> str:
        """Tạo key cho stats"""
        return f"{self.stats_prefix}{identifier}"

    def _update_delay(self, identifier: str, success: bool, error_type: str | None = None):
        """
        Cập nhật delay dựa trên success/error

        Args:
            identifier: Identifier (domain, IP, etc.)
            success: True nếu request thành công
            error_type: Loại error ('429', 'timeout', 'connection', etc.)
        """
        try:
            stats_key = self._get_stats_key(identifier)

            # Track stats trong Redis (circular buffer)
            now = int(time.time())
            success_value = 1 if success else 0
            error_type_value = error_type or ""

            # Add to stats list (keep last N requests)
            pipe = self.client.pipeline()
            pipe.lpush(f"{stats_key}:history", f"{now}:{success_value}:{error_type_value}")
            pipe.ltrim(f"{stats_key}:history", 0, self.success_window - 1)
            pipe.expire(f"{stats_key}:history", 3600)  # 1 hour TTL

            # Update counters
            if success:
                pipe.incr(f"{stats_key}:success")
            else:
                pipe.incr(f"{stats_key}:errors")
                if error_type == "429":
                    pipe.incr(f"{stats_key}:errors_429")  # Track 429 specifically

            pipe.expire(f"{stats_key}:success", 3600)
            pipe.expire(f"{stats_key}:errors", 3600)
            pipe.expire(f"{stats_key}:errors_429", 3600)
            pipe.execute()

            # Calculate error rate từ recent history
            history = self.client.lrange(f"{stats_key}:history", 0, self.success_window - 1)
            if len(history) >= 50:  # Cần ít nhất 50 requests để tính toán
                errors = sum(1 for h in history if h.split(":")[1] == "0")
                total = len(history)
                error_rate = errors / total

                # Adaptive logic
                if error_type == "429":
                    # Có 429 error -> tăng delay mạnh
                    self.current_delay = min(self.max_delay, self.current_delay * 1.5)
                elif error_rate > self.error_threshold:
                    # Error rate cao -> tăng delay
                    self.current_delay = min(self.max_delay, self.current_delay * 1.2)
                elif error_rate < self.error_threshold / 2 and len(history) >= 100:
                    # Error rate thấp và đủ samples -> giảm delay
                    self.current_delay = max(self.min_delay, self.current_delay * 0.9)

                # Save delay to Redis
                self.client.set(self.delay_key, str(self.current_delay), ex=3600)

        except Exception:
            # Nếu Redis lỗi, giữ nguyên delay hiện tại
            pass

    def wait(self, identifier: str = "default"):
        """
        Đợi với delay hiện tại

        Args:
            identifier: Identifier (domain, IP, etc.)
        """
        time.sleep(self.current_delay)

    def record_success(self, identifier: str = "default"):
        """Ghi nhận request thành công"""
        self._update_delay(identifier, success=True)

    def record_error(self, identifier: str = "default", error_type: str | None = None):
        """
        Ghi nhận request lỗi

        Args:
            identifier: Identifier (domain, IP, etc.)
            error_type: Loại error ('429', 'timeout', 'connection', etc.)
        """
        self._update_delay(identifier, success=False, error_type=error_type)

    def get_current_delay(self) -> float:
        """Lấy delay hiện tại"""
        return self.current_delay

    def reset_stats(self, identifier: str = "default"):
        """Reset stats cho identifier"""
        try:
            stats_key = self._get_stats_key(identifier)
            self.client.delete(f"{stats_key}:history")
            self.client.delete(f"{stats_key}:success")
            self.client.delete(f"{stats_key}:errors")
            self.client.delete(f"{stats_key}:errors_429")
        except Exception:
            pass


# Singleton instance
_adaptive_rate_limiter_instance = None


def get_adaptive_rate_limiter(
    redis_url: str = "redis://redis:6379/2",
    initial_delay: float = 0.7,
    min_delay: float = 0.3,
    max_delay: float = 2.0,
) -> AdaptiveRateLimiter | None:
    """
    Lấy adaptive rate limiter instance (singleton)

    Args:
        redis_url: Redis connection URL
        initial_delay: Delay ban đầu
        min_delay: Delay tối thiểu
        max_delay: Delay tối đa
    """
    global _adaptive_rate_limiter_instance
    if not REDIS_AVAILABLE:
        return None
    if _adaptive_rate_limiter_instance is None:
        try:
            _adaptive_rate_limiter_instance = AdaptiveRateLimiter(
                redis_url=redis_url,
                initial_delay=initial_delay,
                min_delay=min_delay,
                max_delay=max_delay,
            )
        except Exception:
            return None
    return _adaptive_rate_limiter_instance

