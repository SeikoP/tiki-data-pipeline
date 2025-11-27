"""
Configuration cho crawl pipeline
"""

import os
from typing import Any


def get_config() -> dict[str, Any]:
    """Get configuration từ environment variables"""
    return {"firecrawl_api_url": os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")}


# Category crawling configuration - OPTIMIZED
CATEGORY_BATCH_SIZE = 12  # Categories per batch
CATEGORY_TIMEOUT = 120  # Seconds - tối ưu: 120 thay vì 180 (2 phút)
CATEGORY_CONCURRENT_REQUESTS = 5  # Tối ưu: 5 thay vì 3 concurrent requests
CATEGORY_POOL_SIZE = 8  # Tối ưu: selenium drivers cho category crawl

# Product crawling configuration - OPTIMIZED
PRODUCT_BATCH_SIZE = 12  # Products per batch (từ 15)
PRODUCT_TIMEOUT = 120  # Seconds for product detail fetch (tăng từ 60 -> 120 để trang load đầy đủ)
PRODUCT_POOL_SIZE = 15  # Selenium drivers , tốn ram hơn nhưng nhanh hơn

# HTTP client configuration - OPTIMIZED
HTTP_CONNECTOR_LIMIT = 100  # Tổng concurrent HTTP connections
HTTP_CONNECTOR_LIMIT_PER_HOST = 10  # Per-host limit
HTTP_TIMEOUT_TOTAL = 20  # Seconds (từ 30)
HTTP_TIMEOUT_CONNECT = 10  # Seconds
HTTP_DNS_CACHE_TTL = 300  # Seconds (5 phút)

# Redis Cache TTL configuration - CRITICAL FOR CACHE HIT RATE
REDIS_CACHE_TTL_PRODUCT_DETAIL = 604800  # 7 days (604800 seconds) - long TTL để maximize hits
REDIS_CACHE_TTL_PRODUCT_LIST = (
    43200  # 12 hours (43200 seconds) - product lists change less frequently
)
REDIS_CACHE_TTL_HTML = 604800  # 7 days (604800 seconds) - HTML pages stable for 1 week

# Cache validation configuration
CACHE_MIN_FIELDS_FOR_VALIDITY = [
    "price",
    "sales_count",
    "name",
]  # Product needs at least one of these
CACHE_ACCEPT_PARTIAL_DATA = True  # Chấp nhận partial cache (không cần tất cả fields)
