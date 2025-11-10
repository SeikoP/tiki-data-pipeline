"""
Cấu hình cho Tiki Crawler Pipeline
"""
import os
from typing import Dict, Any

# API Configuration
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://api:3002")
TIKI_BASE_URL = "https://tiki.vn"

# Rate Limiting Configuration
RATE_LIMIT_CONFIG = {
    "max_requests_per_minute": int(os.getenv("TIKI_MAX_REQ_PER_MIN", "30")),
    "max_requests_per_hour": int(os.getenv("TIKI_MAX_REQ_PER_HOUR", "1000")),
    "burst_size": int(os.getenv("TIKI_BURST_SIZE", "5")),
    "backoff_factor": float(os.getenv("TIKI_BACKOFF_FACTOR", "2.0")),
    "max_backoff": int(os.getenv("TIKI_MAX_BACKOFF", "300")),
}

# Retry Configuration
RETRY_CONFIG = {
    "max_retries": int(os.getenv("TIKI_MAX_RETRIES", "3")),
    "initial_delay": float(os.getenv("TIKI_INITIAL_DELAY", "1.0")),
    "backoff_factor": float(os.getenv("TIKI_BACKOFF_FACTOR", "2.0")),
    "max_delay": float(os.getenv("TIKI_MAX_DELAY", "60.0")),
}

# Crawl Configuration
CRAWL_CONFIG = {
    "batch_size": int(os.getenv("TIKI_BATCH_SIZE", "50")),
    "max_workers": int(os.getenv("TIKI_MAX_WORKERS", "10")),
    "timeout": int(os.getenv("TIKI_TIMEOUT", "60")),
    "max_age": int(os.getenv("TIKI_MAX_AGE", "172800000")),  # 2 ngày
}

# Data Paths
DATA_PATHS = {
    "raw": "data/raw",
    "processed": "data/processed",
    "categories": "data/raw/tiki_categories.json",
    "sub_categories": "data/raw/tiki_sub_categories.json",
    "all_categories": "data/raw/tiki_all_categories.json",
    "merged_categories": "data/raw/tiki_categories_merged.json",
}

# Airflow Configuration
AIRFLOW_CONFIG = {
    "pool_name": os.getenv("TIKI_POOL_NAME", "tiki_crawler_pool"),
    "pool_slots": int(os.getenv("TIKI_POOL_SLOTS", "20")),
    "task_timeout": int(os.getenv("TIKI_TASK_TIMEOUT", "3600")),  # 1 giờ
    "max_active_tasks": int(os.getenv("TIKI_MAX_ACTIVE_TASKS", "50")),
}

def get_config() -> Dict[str, Any]:
    """Lấy toàn bộ config"""
    return {
        "rate_limit": RATE_LIMIT_CONFIG,
        "retry": RETRY_CONFIG,
        "crawl": CRAWL_CONFIG,
        "data_paths": DATA_PATHS,
        "airflow": AIRFLOW_CONFIG,
    }

