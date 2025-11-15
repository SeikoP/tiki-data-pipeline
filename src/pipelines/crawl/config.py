"""
Configuration cho crawl pipeline
"""

import os
from typing import Dict, Any


def get_config() -> Dict[str, Any]:
    """Get configuration từ environment variables"""
    return {"firecrawl_api_url": os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")}


# Groq configuration
GROQ_CONFIG = {
    "enabled": os.getenv("GROQ_ENABLED", "false").lower() == "true",
    "base_url": os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1"),
    "model": os.getenv("GROQ_MODEL", "llama-3.1-70b-versatile"),
}

# Firecrawl API URL
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
