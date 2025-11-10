"""
Rate Limiter và Retry Logic cho Tiki Crawler
Tối ưu cho crawl dữ liệu khổng lồ với rate limiting thông minh
"""
import time
import random
from functools import wraps
from typing import Callable, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter với sliding window và exponential backoff
    """
    
    def __init__(
        self,
        max_requests_per_minute: int = 30,
        max_requests_per_hour: int = 1000,
        burst_size: int = 5,
        backoff_factor: float = 2.0,
        max_backoff: int = 300  # 5 phút
    ):
        self.max_per_minute = max_requests_per_minute
        self.max_per_hour = max_requests_per_hour
        self.burst_size = burst_size
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        
        # Tracking requests
        self.minute_requests = []
        self.hour_requests = []
        self.last_request_time = None
        self.consecutive_errors = 0
        
    def wait_if_needed(self):
        """Chờ nếu cần để tuân thủ rate limit"""
        now = datetime.now()
        
        # Cleanup old requests
        minute_ago = now - timedelta(minutes=1)
        hour_ago = now - timedelta(hours=1)
        
        self.minute_requests = [t for t in self.minute_requests if t > minute_ago]
        self.hour_requests = [t for t in self.hour_requests if t > hour_ago]
        
        # Check rate limits
        if len(self.minute_requests) >= self.max_per_minute:
            sleep_time = 60 - (now - self.minute_requests[0]).total_seconds()
            if sleep_time > 0:
                logger.info(f"Rate limit: Chờ {sleep_time:.2f}s (đã đạt {self.max_per_minute} requests/phút)")
                time.sleep(sleep_time)
                # Cleanup lại sau khi chờ
                self.minute_requests = [t for t in self.minute_requests if t > datetime.now() - timedelta(minutes=1)]
        
        if len(self.hour_requests) >= self.max_per_hour:
            sleep_time = 3600 - (now - self.hour_requests[0]).total_seconds()
            if sleep_time > 0:
                logger.warning(f"Rate limit: Chờ {sleep_time:.2f}s (đã đạt {self.max_per_hour} requests/giờ)")
                time.sleep(sleep_time)
                self.hour_requests = [t for t in self.hour_requests if t > datetime.now() - timedelta(hours=1)]
        
        # Thêm jitter để tránh thundering herd
        if self.last_request_time:
            elapsed = (now - self.last_request_time).total_seconds()
            min_interval = 60.0 / self.max_per_minute
            if elapsed < min_interval:
                jitter = random.uniform(0.1, 0.5)
                time.sleep(min_interval - elapsed + jitter)
        
        # Record request
        self.minute_requests.append(now)
        self.hour_requests.append(now)
        self.last_request_time = now
    
    def record_error(self):
        """Ghi nhận lỗi và tăng backoff"""
        self.consecutive_errors += 1
        backoff = min(
            self.backoff_factor ** self.consecutive_errors,
            self.max_backoff
        )
        logger.warning(f"Lỗi phát hiện, backoff {backoff}s (lỗi liên tiếp: {self.consecutive_errors})")
        time.sleep(backoff)
    
    def record_success(self):
        """Ghi nhận thành công và reset error counter"""
        if self.consecutive_errors > 0:
            logger.info(f"Reset error counter sau {self.consecutive_errors} lỗi")
        self.consecutive_errors = 0


def rate_limited(max_per_minute: int = 30, max_per_hour: int = 1000):
    """
    Decorator để áp dụng rate limiting cho function
    """
    limiter = RateLimiter(max_per_minute, max_per_hour)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter.wait_if_needed()
            try:
                result = func(*args, **kwargs)
                limiter.record_success()
                return result
            except Exception as e:
                limiter.record_error()
                raise
        return wrapper
    return decorator


def retry_with_backoff(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Decorator để retry với exponential backoff
    
    Args:
        max_retries: Số lần retry tối đa
        initial_delay: Thời gian chờ ban đầu (giây)
        backoff_factor: Hệ số tăng delay
        max_delay: Delay tối đa (giây)
        exceptions: Tuple các exception cần retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"Lỗi ở attempt {attempt + 1}/{max_retries + 1}: {str(e)}. "
                            f"Chờ {delay:.2f}s trước khi retry..."
                        )
                        time.sleep(delay)
                        delay = min(delay * backoff_factor, max_delay)
                    else:
                        logger.error(f"Đã hết retry sau {max_retries + 1} attempts")
                        raise
            
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

