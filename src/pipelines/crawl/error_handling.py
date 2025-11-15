"""
Error Handling Module - Export tất cả error handling components
(Re-export từ resilience module để backward compatibility)
"""

from .resilience import (
    CrawlError,
    NetworkError,
    ParseError,
    StorageError,
    ValidationError,
    RateLimitError,
    TimeoutError,
    SeleniumError,
    ConfigurationError,
    classify_error,
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    circuit_breaker,
    DeadLetterQueue,
    get_dlq,
    GracefulDegradation,
    DegradationLevel,
    ServiceHealth,
    get_service_health,
    graceful_degradation,
    ErrorHandler,
    with_error_handling,
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

