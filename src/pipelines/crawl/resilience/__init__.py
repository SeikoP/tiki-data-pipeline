"""
Resilience Module - Error Handling & Resilience Components
"""

from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    circuit_breaker,
)
from .dead_letter_queue import (
    DeadLetterQueue,
    get_dlq,
)
from .error_handler import (
    ErrorHandler,
    with_error_handling,
)
from .exceptions import (
    ConfigurationError,
    CrawlError,
    NetworkError,
    ParseError,
    RateLimitError,
    SeleniumError,
    StorageError,
    TimeoutError,
    ValidationError,
    classify_error,
)
from .graceful_degradation import (
    DegradationLevel,
    GracefulDegradation,
    ServiceHealth,
    get_service_health,
    graceful_degradation,
)

__all__ = [
    # Exceptions
    "CrawlError",
    "NetworkError",
    "ParseError",
    "StorageError",
    "ValidationError",
    "RateLimitError",
    "TimeoutError",
    "SeleniumError",
    "ConfigurationError",
    "classify_error",
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerOpenError",
    "CircuitState",
    "circuit_breaker",
    # Dead Letter Queue
    "DeadLetterQueue",
    "get_dlq",
    # Graceful Degradation
    "GracefulDegradation",
    "DegradationLevel",
    "ServiceHealth",
    "get_service_health",
    "graceful_degradation",
    # Error Handler
    "ErrorHandler",
    "with_error_handling",
]
