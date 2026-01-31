"""
Configuration module cho common modules Load từ file .env trong src/common/ hoặc từ environment
variables.
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Tìm file .env ở root (lên 2 cấp từ src/common/)
current_dir = Path(__file__).parent
env_file = current_dir.parent.parent / ".env"

# Load .env nếu có
if env_file.exists():
    try:
        from dotenv import load_dotenv

        # Load với override=True để đảm bảo các giá trị từ .env được sử dụng
        load_dotenv(env_file, override=True)
        logger.info(f"✅ Đã load .env từ: {env_file.absolute()}")
    except ImportError:
        # Nếu không có python-dotenv, bỏ qua
        logger.warning("⚠️  python-dotenv chưa được cài đặt, không thể load .env")
    except Exception as e:
        logger.warning(f"⚠️  Lỗi khi load .env: {e}")
else:
    logger.debug(f"📝 File .env không tồn tại tại: {env_file.absolute()}")

# AI configuration (Groq or OpenRouter)
# Models examples:
# Groq: llama-3.3-70b-versatile, llama-3.1-8b-instant
# OpenRouter: arcee-ai/trinity-large-preview:free, google/learnlm-1.5-pro-experimental:free
AI_CONFIG = {
    "enabled": os.getenv("AI_ENABLED", os.getenv("GROQ_ENABLED", "false")).lower() == "true",
    "api_key": os.getenv("AI_API_KEY", os.getenv("GROQ_API_KEY", "")),
    "base_url": os.getenv(
        "AI_API_BASE", os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
    ),
    "model": os.getenv("AI_MODEL", os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")),
}

# Discord configuration
DISCORD_CONFIG = {
    "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", ""),
    "enabled": os.getenv("DISCORD_ENABLED", "false").lower() == "true",
}

# Short Name AI Configuration
SHORT_NAME_CONFIG = {
    "use_ai": os.getenv("SHORT_NAME_USE_AI", "true").lower() == "true",
}

# Performance Optimization Feature Flags
OPTIMIZATION_CONFIG = {
    # Async I/O optimization (Task 14.1)
    "use_async_crawler": os.getenv("USE_ASYNC_CRAWLER", "false").lower() == "true",
    "async_connector_limit": int(os.getenv("ASYNC_CONNECTOR_LIMIT", "300")),
    "async_batch_size": int(os.getenv("ASYNC_BATCH_SIZE", "50")),
    "async_timeout_total": int(os.getenv("ASYNC_TIMEOUT_TOTAL", "15")),
    "async_timeout_connect": int(os.getenv("ASYNC_TIMEOUT_CONNECT", "10")),
    
    # Vectorized transformation optimization (Task 14.2)
    "use_vectorized_transform": os.getenv("USE_VECTORIZED_TRANSFORM", "false").lower() == "true",
    
    # Optimized batch loading (Task 14.3)
    "use_optimized_loader": os.getenv("USE_OPTIMIZED_LOADER", "false").lower() == "true",
    "optimized_batch_size": int(os.getenv("OPTIMIZED_BATCH_SIZE", "1000")),
    "use_copy_command": os.getenv("USE_COPY_COMMAND", "true").lower() == "true",
}
