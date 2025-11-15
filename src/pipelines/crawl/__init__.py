"""
Crawl pipeline package
"""

# Export error handling modules từ resilience
try:
    from .resilience import (  # noqa: F401
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

    _ERROR_HANDLING_AVAILABLE = True
except ImportError:
    _ERROR_HANDLING_AVAILABLE = False

__all__ = []

if _ERROR_HANDLING_AVAILABLE:
    __all__.extend(
        [
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
        ]
    )
