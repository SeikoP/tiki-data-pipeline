"""
Cấu hình cho Tiki Crawler Pipeline
"""
import os
from typing import Dict, Any

# Load .env file
try:
    from dotenv import load_dotenv
    # Load .env từ project root
    import sys
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    env_path = os.path.join(project_root, '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        # Try current directory
        load_dotenv()
except ImportError:
    # python-dotenv not installed, skip
    pass

# API Configuration
# Auto-detect: use localhost for host machine, api for Docker container
def _get_firecrawl_url():
    """Auto-detect Firecrawl URL based on environment"""
    env_url = os.getenv("FIRECRAWL_API_URL")
    if env_url:
        return env_url
    
    # If running in Docker, use api:3002
    # Otherwise (Windows/host machine), use localhost:3002
    if os.path.exists("/.dockerenv"):
        return "http://api:3002"
    else:
        return "http://localhost:3002"

FIRECRAWL_API_URL = _get_firecrawl_url()
TIKI_BASE_URL = "https://tiki.vn"
TIKI_API_BASE_URL = "https://api.tiki.vn/integration/v2"
TIKI_API_TOKEN = os.getenv("TIKI_API_TOKEN", "")  # Seller token key từ Tiki

# Groq API Configuration
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_API_KEYS = os.getenv("GROQ_API_KEYS", "")  # Comma-separated multiple keys
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")  # Default Groq model
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")

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

# Groq Configuration
GROQ_CONFIG = {
    "api_key": GROQ_API_KEY,
    "api_keys": GROQ_API_KEYS.split(",") if GROQ_API_KEYS else [],
    "model": GROQ_MODEL,
    "base_url": GROQ_BASE_URL,
    "enabled": bool(GROQ_API_KEY or GROQ_API_KEYS),
}

def get_config() -> Dict[str, Any]:
    """Lấy toàn bộ config"""
    return {
        "rate_limit": RATE_LIMIT_CONFIG,
        "retry": RETRY_CONFIG,
        "crawl": CRAWL_CONFIG,
        "data_paths": DATA_PATHS,
        "airflow": AIRFLOW_CONFIG,
        "groq": GROQ_CONFIG,
    }

