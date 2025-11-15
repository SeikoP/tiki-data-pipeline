"""
Crawl pipeline package
"""

# Export error handling modules từ resilience
try:
    from .resilience import (
        # Exceptions
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
        # Circuit Breaker
        CircuitBreaker,
        CircuitBreakerOpenError,
        CircuitState,
        circuit_breaker,
        # Dead Letter Queue
        DeadLetterQueue,
        get_dlq,
        # Graceful Degradation
        GracefulDegradation,
        DegradationLevel,
        ServiceHealth,
        get_service_health,
        graceful_degradation,
        # Error Handler
        ErrorHandler,
        with_error_handling,
    )
    _ERROR_HANDLING_AVAILABLE = True
except ImportError:
    _ERROR_HANDLING_AVAILABLE = False

__all__ = []

if _ERROR_HANDLING_AVAILABLE:
    __all__.extend([
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
        "CircuitBreaker",
        "CircuitBreakerOpenError",
        "CircuitState",
        "circuit_breaker",
        "DeadLetterQueue",
        "get_dlq",
        "GracefulDegradation",
        "DegradationLevel",
        "ServiceHealth",
        "get_service_health",
        "graceful_degradation",
        "ErrorHandler",
        "with_error_handling",
    ])
