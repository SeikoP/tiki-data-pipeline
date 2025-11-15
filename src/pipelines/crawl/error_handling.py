"""
Error Handling Module - Export tất cả error handling components
(Re-export từ resilience module để backward compatibility)
"""

from .resilience import (
    CircuitBreaker,
    CircuitBreakerOpenError,
    CircuitState,
    ConfigurationError,
    CrawlError,
    DeadLetterQueue,
    DegradationLevel,
    ErrorHandler,
    GracefulDegradation,
    NetworkError,
    ParseError,
    RateLimitError,
    SeleniumError,
    ServiceHealth,
    StorageError,
    TimeoutError,
    ValidationError,
    circuit_breaker,
    classify_error,
    get_dlq,
    get_service_health,
    graceful_degradation,
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
