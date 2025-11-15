"""
Configuration cho crawl pipeline
"""

import os
from typing import Any


def get_config() -> dict[str, Any]:
    """Get configuration từ environment variables"""
    return {"firecrawl_api_url": os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")}
