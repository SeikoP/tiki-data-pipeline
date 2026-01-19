"""
Configuration cho crawl pipeline
"""

import os
from typing import Any


def get_config() -> dict[str, Any]:
    """Get configuration từ environment variables"""
    return {"firecrawl_api_url": os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")}


# Category level configuration
MAX_CATEGORY_LEVELS = 5  # Tiki categories can have up to 5 levels with parent category included

# Category crawling configuration - OPTIMIZED
CATEGORY_BATCH_SIZE = 12  # Categories per batch
CATEGORY_TIMEOUT = 120  # Seconds - tối ưu: 120 thay vì 180 (2 phút)
CATEGORY_CONCURRENT_REQUESTS = 5  # Tối ưu: 5 thay vì 3 concurrent requests
CATEGORY_POOL_SIZE = 8  # Tối ưu: selenium drivers cho category crawl

# Product crawling configuration - OPTIMIZED
PRODUCT_BATCH_SIZE = 12  # Products per batch (từ 15)
PRODUCT_TIMEOUT = 120  # Seconds for product detail fetch (tăng từ 60 -> 120 để trang load đầy đủ)
PRODUCT_POOL_SIZE = (
    12  # Selenium drivers - match với crawl_pool (Airflow 12 slots), tiết kiệm ~150-300MB RAM
)

# HTTP client configuration - OPTIMIZED
HTTP_CONNECTOR_LIMIT = 150  # Tổng concurrent HTTP connections (tăng từ 100)
HTTP_CONNECTOR_LIMIT_PER_HOST = 15  # Per-host limit (tăng từ 10)
HTTP_TIMEOUT_TOTAL = 20  # Seconds (từ 30)
HTTP_TIMEOUT_CONNECT = 10  # Seconds
HTTP_DNS_CACHE_TTL = 1800  # Seconds (30 phút - tăng từ 5 phút để giảm DNS lookups)

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

# ========== RETRY CONFIGURATION - HYBRID APPROACH ==========
# Conditional retry for missing critical fields (seller_name, brand)
PRODUCT_RETRY_MAX_ATTEMPTS = 2  # Retry up to 2 times for missing critical fields
PRODUCT_RETRY_DELAY_BASE = 3  # Base delay in seconds between retries
PRODUCT_RETRY_WAIT_MULTIPLIER = 2  # Multiply wait times on retry (e.g., 4s -> 8s implicit_wait)

# ========== DATA QUALITY THRESHOLDS ==========
# Fields classification for validation
DATA_QUALITY_CRITICAL_FIELDS = ["name", "price", "product_id"]  # Must have to save
DATA_QUALITY_IMPORTANT_FIELDS = [
    "seller_name",
    "brand",
    "category_id",
]  # Should have, trigger retry if missing
DATA_QUALITY_OPTIONAL_FIELDS = [
    "rating_average",
    "sales_count",
    "stock_quantity",
]  # Nice to have

# Minimum completeness score to accept product (0.0 - 1.0)
DATA_QUALITY_MIN_SCORE = 0.7  # 70% completeness threshold

# Wait time adjustments for retry (Tier 1 + Tier 2 approach)
# Tier 1: Normal crawl (moderate increase from current)
CRAWL_IMPLICIT_WAIT_NORMAL = 4  # seconds (increased from 3s)
CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL = 4  # seconds (increased from 3s)
CRAWL_POST_SCROLL_SLEEP_NORMAL = 1.2  # seconds (increased from 0.8s)

# Tier 2: Retry crawl (2x wait times)
CRAWL_IMPLICIT_WAIT_RETRY = 8  # seconds (2x normal)
CRAWL_DYNAMIC_CONTENT_WAIT_RETRY = 8  # seconds (2x normal)
CRAWL_POST_SCROLL_SLEEP_RETRY = 2.4  # seconds (2x normal)
