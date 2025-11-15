"""
Example: Cách sử dụng Error Handling trong crawl pipeline

File này minh họa cách tích hợp error handling vào các functions crawl
"""

from . import (
    NetworkError,
    ParseError,
    CircuitBreaker,
    get_dlq,
    get_service_health,
    with_error_handling,
    circuit_breaker,
)


# Example 1: Sử dụng Circuit Breaker
@circuit_breaker(failure_threshold=5, recovery_timeout=60, name="tiki_api")
def fetch_page_with_circuit_breaker(url: str):
    """Fetch page với circuit breaker protection"""
    import requests
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


# Example 2: Sử dụng Error Handler decorator
@with_error_handling(
    task_type="crawl_category",
    max_retries=3,
    retry_delay=1.0,
)
def crawl_category_with_error_handling(category_url: str):
    """Crawl category với error handling đầy đủ"""
    # Function này sẽ tự động:
    # - Retry khi lỗi
    # - Thêm vào DLQ nếu fail sau retries
    # - Ghi nhận success/failure cho graceful degradation
    
    import requests
    response = requests.get(category_url, timeout=30)
    response.raise_for_status()
    return response.text


# Example 3: Sử dụng manual Error Handler
def crawl_product_manual(product_url: str):
    """Crawl product với manual error handling"""
    from . import ErrorHandler, CircuitBreaker
    
    # Tạo circuit breaker cho Tiki API
    cb = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=60,
        name="tiki_api",
    )
    
    # Tạo error handler
    handler = ErrorHandler(
        task_type="crawl_product",
        circuit_breaker=cb,
        max_retries=3,
    )
    
    def _fetch_product():
        import requests
        response = requests.get(product_url, timeout=30)
        response.raise_for_status()
        return response.text
    
    # Gọi với error handling
    result = handler.handle(
        _fetch_product,
        task_id=f"product_{product_url}",
        context={"url": product_url},
    )
    
    return result


# Example 4: Sử dụng Graceful Degradation
def crawl_with_graceful_degradation():
    """Crawl với graceful degradation"""
    from . import get_service_health
    
    service_health = get_service_health()
    
    # Đăng ký service
    tiki_service = service_health.register_service(
        name="tiki_api",
        failure_threshold=3,
        recovery_threshold=5,
    )
    
    # Callback khi service degrade
    def on_degrade(old_level, new_level):
        print(f"Service degraded: {old_level} -> {new_level}")
    
    tiki_service.register_callback("reduced", on_degrade)
    tiki_service.register_callback("minimal", on_degrade)
    tiki_service.register_callback("failed", on_degrade)
    
    # Sử dụng trong crawl
    try:
        import requests
        response = requests.get("https://tiki.vn", timeout=30)
        tiki_service.record_success()
        return response.text
    except Exception as e:
        tiki_service.record_failure()
        raise


# Example 5: Xử lý DLQ entries
def retry_failed_tasks():
    """Retry các tasks đã fail từ DLQ"""
    dlq = get_dlq()
    
    # Lấy tất cả failed tasks
    failed_tasks = dlq.list_all(limit=100)
    
    for task in failed_tasks:
        task_id = task["task_id"]
        task_type = task["task_type"]
        context = task.get("context", {})
        
        print(f"Retrying task {task_id} ({task_type})")
        
        # Retry logic
        try:
            if task_type == "crawl_category":
                # Retry crawl category
                url = context.get("url")
                if url:
                    # crawl_category_with_error_handling(url)
                    pass
            
            # Nếu thành công, xóa khỏi DLQ
            dlq.remove(task_id)
            print(f"✅ Task {task_id} retried successfully")
            
        except Exception as e:
            print(f"❌ Task {task_id} still failed: {e}")


# Example 6: Tích hợp vào existing function
def crawl_category_products_enhanced(
    category_url: str,
    max_pages: int = None,
    use_error_handling: bool = True,
):
    """
    Enhanced version của crawl_category_products với error handling
    
    Args:
        category_url: URL của category
        max_pages: Số trang tối đa
        use_error_handling: Có dùng error handling không
    """
    if not use_error_handling:
        # Fallback về code cũ
        from ..crawl_products import crawl_category_products
        return crawl_category_products(category_url, max_pages=max_pages)
    
    # Sử dụng error handling
    from . import ErrorHandler, CircuitBreaker
    
    cb = CircuitBreaker(
        failure_threshold=5,
        recovery_timeout=60,
        name="tiki_crawl",
    )
    
    handler = ErrorHandler(
        task_type="crawl_category",
        circuit_breaker=cb,
        max_retries=2,
    )
    
    def _crawl():
        from ..crawl_products import crawl_category_products
        return crawl_category_products(category_url, max_pages=max_pages)
    
    result = handler.handle(
        _crawl,
        task_id=f"category_{category_url}",
        context={"url": category_url, "max_pages": max_pages},
    )
    
    return result or []  # Return empty list nếu fail

