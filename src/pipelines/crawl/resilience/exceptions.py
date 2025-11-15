"""
Custom Exceptions cho Tiki Crawl Pipeline
Phân loại rõ ràng các loại lỗi để dễ xử lý và debug
"""

from typing import Optional, Dict, Any


class CrawlError(Exception):
    """Base exception cho tất cả các lỗi crawl"""
    
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        """
        Args:
            message: Thông báo lỗi
            context: Context thêm (URL, product_id, etc.)
            original_error: Exception gốc (nếu có)
        """
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.original_error = original_error
    
    def __str__(self) -> str:
        base_msg = self.message
        if self.context:
            context_str = ", ".join(f"{k}={v}" for k, v in self.context.items())
            return f"{base_msg} [{context_str}]"
        return base_msg
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception thành dict để log/serialize"""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "context": self.context,
            "original_error": str(self.original_error) if self.original_error else None,
        }


class NetworkError(CrawlError):
    """Lỗi liên quan đến network (timeout, connection, DNS, etc.)"""
    
    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        status_code: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if url:
            context["url"] = url
        if status_code:
            context["status_code"] = status_code
        super().__init__(message, context=context, original_error=original_error)


class ParseError(CrawlError):
    """Lỗi khi parse HTML/JSON (missing data, invalid format, etc.)"""
    
    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        element: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if url:
            context["url"] = url
        if element:
            context["element"] = element
        super().__init__(message, context=context, original_error=original_error)


class StorageError(CrawlError):
    """Lỗi khi lưu/đọc dữ liệu (database, file, cache, etc.)"""
    
    def __init__(
        self,
        message: str,
        storage_type: Optional[str] = None,
        operation: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if storage_type:
            context["storage_type"] = storage_type
        if operation:
            context["operation"] = operation
        super().__init__(message, context=context, original_error=original_error)


class ValidationError(CrawlError):
    """Lỗi validation dữ liệu (missing required fields, invalid format, etc.)"""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if field:
            context["field"] = field
        if value is not None:
            context["value"] = str(value)
        super().__init__(message, context=context, original_error=original_error)


class RateLimitError(CrawlError):
    """Lỗi khi vượt quá rate limit"""
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if retry_after:
            context["retry_after"] = retry_after
        super().__init__(message, context=context, original_error=original_error)


class TimeoutError(CrawlError):
    """Lỗi timeout (request timeout, operation timeout, etc.)"""
    
    def __init__(
        self,
        message: str,
        timeout: Optional[float] = None,
        operation: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if timeout:
            context["timeout"] = timeout
        if operation:
            context["operation"] = operation
        super().__init__(message, context=context, original_error=original_error)


class SeleniumError(CrawlError):
    """Lỗi liên quan đến Selenium (driver, browser, etc.)"""
    
    def __init__(
        self,
        message: str,
        driver_error: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if driver_error:
            context["driver_error"] = driver_error
        super().__init__(message, context=context, original_error=original_error)


class ConfigurationError(CrawlError):
    """Lỗi cấu hình (missing config, invalid config, etc.)"""
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_error: Optional[Exception] = None,
    ):
        context = context or {}
        if config_key:
            context["config_key"] = config_key
        super().__init__(message, context=context, original_error=original_error)


def classify_error(error: Exception, context: Optional[Dict[str, Any]] = None) -> CrawlError:
    """
    Phân loại generic exception thành custom exception
    
    Args:
        error: Exception gốc
        context: Context thêm
        
    Returns:
        CrawlError instance
    """
    error_type = type(error).__name__
    error_msg = str(error)
    error_msg_lower = error_msg.lower()
    
    context = context or {}
    
    # Network errors
    if isinstance(error, (ConnectionError, TimeoutError)) or "connection" in error_msg_lower or "timeout" in error_msg_lower:
        if "timeout" in error_msg_lower or isinstance(error, TimeoutError):
            return TimeoutError(
                f"Timeout: {error_msg}",
                context=context,
                original_error=error,
            )
        return NetworkError(
            f"Network error: {error_msg}",
            context=context,
            original_error=error,
        )
    
    # Rate limit
    if "rate limit" in error_msg_lower or "429" in error_msg:
        retry_after = None
        if "retry-after" in error_msg_lower:
            # Try to extract retry-after value
            import re
            match = re.search(r"retry[-_]after[:\s]+(\d+)", error_msg_lower)
            if match:
                retry_after = int(match.group(1))
        return RateLimitError(
            f"Rate limit exceeded: {error_msg}",
            retry_after=retry_after,
            context=context,
            original_error=error,
        )
    
    # Storage errors
    if "database" in error_msg_lower or "postgres" in error_msg_lower or "redis" in error_msg_lower:
        return StorageError(
            f"Storage error: {error_msg}",
            context=context,
            original_error=error,
        )
    
    # Parse errors
    if "parse" in error_msg_lower or "json" in error_msg_lower or "html" in error_msg_lower:
        return ParseError(
            f"Parse error: {error_msg}",
            context=context,
            original_error=error,
        )
    
    # Selenium errors
    if "selenium" in error_msg_lower or "webdriver" in error_msg_lower or "chrome" in error_msg_lower:
        return SeleniumError(
            f"Selenium error: {error_msg}",
            context=context,
            original_error=error,
        )
    
    # Default: wrap as generic CrawlError
    return CrawlError(
        f"{error_type}: {error_msg}",
        context=context,
        original_error=error,
    )

