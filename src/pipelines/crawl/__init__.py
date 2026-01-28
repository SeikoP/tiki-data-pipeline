"""
Crawl pipeline package.
"""

# Export utilities modules from utils.py file
# Note: There's both utils.py file and utils/ package, so we need to import directly from file
try:
    import importlib.util
    from pathlib import Path

    utils_file_path = Path(__file__).parent / "utils.py"
    if utils_file_path.exists():
        spec = importlib.util.spec_from_file_location("_crawl_utils_file", str(utils_file_path))
        if spec and spec.loader:
            _utils_file = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(_utils_file)
            SeleniumDriverPool = getattr(_utils_file, "SeleniumDriverPool", None)  # noqa: F401
    _UTILS_AVAILABLE = True
except ImportError:
    _UTILS_AVAILABLE = False
    SeleniumDriverPool = None

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

if _UTILS_AVAILABLE:
    __all__.extend(
        [
            "SeleniumDriverPool",
        ]
    )

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
