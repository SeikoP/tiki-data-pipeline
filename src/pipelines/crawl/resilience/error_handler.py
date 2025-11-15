"""
Error Handler - Tích hợp tất cả error handling components
"""

from typing import Any, Callable, Optional, TypeVar, Dict
from functools import wraps
import uuid

from .exceptions import CrawlError, classify_error
from .circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from .dead_letter_queue import get_dlq, DeadLetterQueue
from .graceful_degradation import get_service_health, GracefulDegradation

T = TypeVar('T')


class ErrorHandler:
    """
    Error Handler tích hợp:
    - Custom exceptions
    - Circuit breaker
    - Dead letter queue
    - Graceful degradation
    """
    
    def __init__(
        self,
        task_type: str,
        circuit_breaker: Optional[CircuitBreaker] = None,
        dlq: Optional[DeadLetterQueue] = None,
        degradation: Optional[GracefulDegradation] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Args:
            task_type: Loại task (để DLQ)
            circuit_breaker: Circuit breaker instance (optional)
            dlq: Dead letter queue instance (optional)
            degradation: Graceful degradation instance (optional)
            max_retries: Số lần retry tối đa
            retry_delay: Delay giữa các retry (giây)
        """
        self.task_type = task_type
        self.circuit_breaker = circuit_breaker
        self.dlq = dlq or get_dlq()
        self.degradation = degradation
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def handle(
        self,
        func: Callable[..., T],
        *args,
        task_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Optional[T]:
        """
        Xử lý function call với error handling đầy đủ
        
        Args:
            func: Function cần gọi
            *args, **kwargs: Arguments cho function
            task_id: ID của task (tự động generate nếu None)
            context: Context thêm
            
        Returns:
            Kết quả từ function hoặc None nếu failed
        """
        if task_id is None:
            task_id = str(uuid.uuid4())
        
        context = context or {}
        retry_count = 0
        
        # Kiểm tra graceful degradation
        if self.degradation and self.degradation.should_skip():
            # Service đang failed, skip
            return None
        
        while retry_count < self.max_retries:
            try:
                # Gọi function với circuit breaker (nếu có)
                if self.circuit_breaker:
                    result = self.circuit_breaker.call(func, *args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Success
                if self.degradation:
                    self.degradation.record_success()
                
                return result
                
            except CircuitBreakerOpenError as e:
                # Circuit breaker đang mở
                crawl_error = classify_error(e, context=context)
                self._add_to_dlq(task_id, crawl_error, context, retry_count)
                return None
                
            except Exception as e:
                # Phân loại error
                crawl_error = classify_error(e, context=context)
                
                # Ghi nhận failure
                if self.degradation:
                    self.degradation.record_failure()
                
                retry_count += 1
                
                # Nếu đã hết retries, thêm vào DLQ
                if retry_count >= self.max_retries:
                    self._add_to_dlq(task_id, crawl_error, context, retry_count)
                    return None
                
                # Retry với delay
                import time
                time.sleep(self.retry_delay * retry_count)  # Exponential backoff
        
        return None
    
    def _add_to_dlq(
        self,
        task_id: str,
        error: CrawlError,
        context: Dict[str, Any],
        retry_count: int,
    ):
        """Thêm failed task vào DLQ"""
        try:
            self.dlq.add(
                task_id=task_id,
                task_type=self.task_type,
                error=error,
                context=context,
                retry_count=retry_count,
            )
        except Exception:
            # Nếu DLQ cũng fail, log nhưng không raise
            pass


def with_error_handling(
    task_type: str,
    circuit_breaker: Optional[CircuitBreaker] = None,
    dlq: Optional[DeadLetterQueue] = None,
    degradation: Optional[GracefulDegradation] = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
):
    """
    Decorator để áp dụng error handling cho function
    
    Args:
        task_type: Loại task
        circuit_breaker: Circuit breaker instance
        dlq: Dead letter queue instance
        degradation: Graceful degradation instance
        max_retries: Số lần retry
        retry_delay: Delay giữa retries
    
    Example:
        @with_error_handling(
            task_type="crawl_category",
            max_retries=3,
        )
        def crawl_category(url):
            return products
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        handler = ErrorHandler(
            task_type=task_type,
            circuit_breaker=circuit_breaker,
            dlq=dlq,
            degradation=degradation,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Optional[T]:
            return handler.handle(func, *args, **kwargs)
        
        return wrapper
    
    return decorator

