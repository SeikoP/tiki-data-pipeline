"""
Circuit Breaker Pattern Implementation Ngừng gọi service khi nó lỗi liên tục, tránh cascade failure.
"""

from collections.abc import Callable
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """
    Trạng thái của circuit breaker.
    """

    CLOSED = "closed"  # Bình thường, cho phép requests
    OPEN = "open"  # Đã mở, từ chối requests
    HALF_OPEN = "half_open"  # Đang thử nghiệm, cho phép một số requests


class CircuitBreaker:
    """Circuit Breaker để bảo vệ service khỏi cascade failure.

    Khi số lỗi vượt quá threshold trong một khoảng thời gian, circuit sẽ mở và từ chối tất cả
    requests cho đến khi recovery.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: type[Exception] = Exception,
        name: str = "default",
    ):
        """
        Args:
            failure_threshold: Số lỗi tối đa trước khi mở circuit
            recovery_timeout: Thời gian (giây) trước khi thử lại (half-open)
            expected_exception: Loại exception được coi là failure
            name: Tên circuit breaker (để logging)
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name

        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: datetime | None = None
        self.last_success_time: datetime | None = None
        self.opened_at: datetime | None = None

        self._lock = Lock()

    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Gọi function với circuit breaker protection.

        Args:
            func: Function cần gọi
            *args, **kwargs: Arguments cho function

        Returns:
            Kết quả từ function

        Raises:
            CircuitBreakerOpenError: Nếu circuit đang mở
            Exception: Exception từ function (nếu không phải expected_exception)
        """
        with self._lock:
            # Kiểm tra state
            if self.state == CircuitState.OPEN:
                # Kiểm tra xem đã đến lúc thử lại chưa
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker '{self.name}' is OPEN. "
                        f"Last failure: {self.last_failure_time}. "
                        f"Will retry after {self.recovery_timeout}s"
                    )

            # Gọi function
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except self.expected_exception:
                self._on_failure()
                raise
            except Exception:
                # Exception không phải expected_exception -> không tính là failure
                # Nhưng vẫn raise
                raise

    def _should_attempt_reset(self) -> bool:
        """
        Kiểm tra xem có nên thử reset circuit không.
        """
        if self.opened_at is None:
            return False
        elapsed = (datetime.now() - self.opened_at).total_seconds()
        return elapsed >= self.recovery_timeout

    def _on_success(self):
        """
        Xử lý khi call thành công.
        """
        self.last_success_time = datetime.now()

        if self.state == CircuitState.HALF_OPEN:
            # Trong half-open, cần một số success để đóng lại
            self.success_count += 1
            if self.success_count >= 2:  # Cần 2 success liên tiếp
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.opened_at = None
        elif self.state == CircuitState.CLOSED:
            # Reset failure count khi có success
            self.failure_count = 0

    def _on_failure(self):
        """
        Xử lý khi call thất bại.
        """
        self.last_failure_time = datetime.now()
        self.failure_count += 1

        if self.state == CircuitState.HALF_OPEN:
            # Trong half-open mà fail -> mở lại
            self.state = CircuitState.OPEN
            self.opened_at = datetime.now()
            self.success_count = 0
        elif self.state == CircuitState.CLOSED:
            # Kiểm tra xem có vượt threshold không
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                self.opened_at = datetime.now()

    def reset(self):
        """
        Reset circuit về trạng thái closed.
        """
        with self._lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            self.opened_at = None

    def get_state(self) -> dict:
        """
        Lấy thông tin state của circuit breaker.
        """
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": (
                self.last_failure_time.isoformat() if self.last_failure_time else None
            ),
            "last_success_time": (
                self.last_success_time.isoformat() if self.last_success_time else None
            ),
            "opened_at": self.opened_at.isoformat() if self.opened_at else None,
        }


class CircuitBreakerOpenError(Exception):
    """
    Exception khi circuit breaker đang mở.
    """


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: int = 60,
    expected_exception: type = Exception,
    name: str | None = None,
):
    """Decorator để áp dụng circuit breaker cho function.

    Args:
        failure_threshold: Số lỗi tối đa
        recovery_timeout: Thời gian recovery (giây)
        expected_exception: Loại exception được coi là failure
        name: Tên circuit breaker (mặc định: tên function)

    Example:
        @circuit_breaker(failure_threshold=5, recovery_timeout=60)
        def crawl_url(url):
            return requests.get(url)
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cb_name = name or f"{func.__module__}.{func.__name__}"
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=cb_name,
        )

        def wrapper(*args, **kwargs) -> T:
            return breaker.call(func, *args, **kwargs)

        # Attach breaker để có thể access
        wrapper.circuit_breaker = breaker
        return wrapper

    return decorator
