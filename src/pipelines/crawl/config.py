"""
Configuration cho crawl pipeline.
"""

import os
from typing import Any

from common.settings import get_pipeline_settings, get_setting

_SETTINGS = get_pipeline_settings(use_airflow=False)


def get_config() -> dict[str, Any]:
    """
    Get configuration từ environment variables.
    """
    return {"firecrawl_api_url": get_setting("FIRECRAWL_API_URL", use_airflow=False)}


# Category level configuration
MAX_CATEGORY_LEVELS = _SETTINGS.crawl.max_category_levels

# Category crawling configuration - OPTIMIZED
CATEGORY_BATCH_SIZE = int(get_setting("CATEGORY_BATCH_SIZE", use_airflow=False))
CATEGORY_TIMEOUT = int(get_setting("CATEGORY_TIMEOUT", use_airflow=False))
CATEGORY_CONCURRENT_REQUESTS = int(get_setting("CATEGORY_CONCURRENT_REQUESTS", use_airflow=False))
CATEGORY_POOL_SIZE = int(get_setting("CATEGORY_POOL_SIZE", use_airflow=False))

# Product crawling configuration - OPTIMIZED
PRODUCT_BATCH_SIZE = int(get_setting("PRODUCT_BATCH_SIZE", use_airflow=False))
PRODUCT_TIMEOUT = int(get_setting("PRODUCT_TIMEOUT", use_airflow=False))
PRODUCT_POOL_SIZE = int(get_setting("PRODUCT_POOL_SIZE", use_airflow=False))

# HTTP client configuration - OPTIMIZED
HTTP_CONNECTOR_LIMIT = _SETTINGS.crawl.http_connector_limit
HTTP_CONNECTOR_LIMIT_PER_HOST = _SETTINGS.crawl.http_connector_limit_per_host
HTTP_TIMEOUT_TOTAL = _SETTINGS.crawl.http_timeout_total
HTTP_TIMEOUT_CONNECT = _SETTINGS.crawl.http_timeout_connect
HTTP_DNS_CACHE_TTL = _SETTINGS.crawl.http_dns_cache_ttl

# Redis Cache TTL configuration - CRITICAL FOR CACHE HIT RATE
REDIS_CACHE_TTL_PRODUCT_DETAIL = int(
    get_setting("REDIS_CACHE_TTL_PRODUCT_DETAIL", use_airflow=False)
)
REDIS_CACHE_TTL_PRODUCT_LIST = int(get_setting("REDIS_CACHE_TTL_PRODUCT_LIST", use_airflow=False))
REDIS_CACHE_TTL_HTML = int(get_setting("REDIS_CACHE_TTL_HTML", use_airflow=False))

# Cache validation configuration
CACHE_MIN_FIELDS_FOR_VALIDITY = [
    "price",
    "sales_count",
    "name",
]  # Product needs at least one of these
CACHE_ACCEPT_PARTIAL_DATA = bool(get_setting("CACHE_ACCEPT_PARTIAL_DATA", use_airflow=False))

# ========== RETRY CONFIGURATION - HYBRID APPROACH ==========
# Conditional retry for missing critical fields (seller_name, brand)
PRODUCT_RETRY_MAX_ATTEMPTS = int(get_setting("PRODUCT_RETRY_MAX_ATTEMPTS", use_airflow=False))
PRODUCT_RETRY_DELAY_BASE = int(get_setting("PRODUCT_RETRY_DELAY_BASE", use_airflow=False))
PRODUCT_RETRY_WAIT_MULTIPLIER = int(get_setting("PRODUCT_RETRY_WAIT_MULTIPLIER", use_airflow=False))

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
DATA_QUALITY_MIN_SCORE = float(get_setting("DATA_QUALITY_MIN_SCORE", use_airflow=False))

# Wait time adjustments for retry (Tier 1 + Tier 2 approach)
# Tier 1: Normal crawl (moderate increase from current)
CRAWL_IMPLICIT_WAIT_NORMAL = int(get_setting("CRAWL_IMPLICIT_WAIT_NORMAL", use_airflow=False))
CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL = int(
    get_setting("CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL", use_airflow=False)
)
CRAWL_POST_SCROLL_SLEEP_NORMAL = float(
    get_setting("CRAWL_POST_SCROLL_SLEEP_NORMAL", use_airflow=False)
)

# Tier 2: Retry crawl (2x wait times)
CRAWL_IMPLICIT_WAIT_RETRY = int(get_setting("CRAWL_IMPLICIT_WAIT_RETRY", use_airflow=False))
CRAWL_DYNAMIC_CONTENT_WAIT_RETRY = int(
    get_setting("CRAWL_DYNAMIC_CONTENT_WAIT_RETRY", use_airflow=False)
)
CRAWL_POST_SCROLL_SLEEP_RETRY = float(
    get_setting("CRAWL_POST_SCROLL_SLEEP_RETRY", use_airflow=False)
)

# ========== SELLER NAME VALIDATION ==========
# Default invalid patterns for seller_name (lowercase)
# These values will cause seller_name to be set to NULL and trigger retry
INVALID_SELLER_PATTERNS_DEFAULT = [
    "đã mua",  # "Đã mua hàng", "xxx đã mua"
    "đã bán",  # "Đã bán xxx"
    "sold",  # "xxx sold"
    "bought",  # "xxx bought"
    "xem thêm",  # "Xem thêm"
    "more info",  # "More info"
    "chi tiết",  # "Chi tiết"
    "loading",  # Loading placeholder
    "đang tải",  # Đang tải\
    "Đã mua hàng",
]

# Additional patterns from environment variable (comma-separated)
# Example in .env: INVALID_SELLER_PATTERNS_EXTRA=pattern1,pattern2,pattern3
_extra_patterns_str = os.getenv("INVALID_SELLER_PATTERNS_EXTRA", "")
INVALID_SELLER_PATTERNS_EXTRA = [
    p.strip().lower() for p in _extra_patterns_str.split(",") if p.strip()
]

# Combined list (default + extra from env)
INVALID_SELLER_PATTERNS = INVALID_SELLER_PATTERNS_DEFAULT + INVALID_SELLER_PATTERNS_EXTRA

# Minimum/Maximum length for valid seller name
SELLER_NAME_MIN_LENGTH = int(os.getenv("SELLER_NAME_MIN_LENGTH", "2"))
SELLER_NAME_MAX_LENGTH = int(os.getenv("SELLER_NAME_MAX_LENGTH", "100"))
