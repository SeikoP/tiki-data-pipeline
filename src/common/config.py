"""
Configuration module cho common modules
Load từ file .env trong src/common/ hoặc từ environment variables
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

# Groq configuration
# Các model Groq có sẵn: openai/gpt-oss-120b, llama-3.3-70b-versatile, llama-3.1-8b-instant, mixtral-8x7b-32768, gemma2-9b-it
GROQ_CONFIG = {
    "enabled": os.getenv("GROQ_ENABLED", "false").lower() == "true",
    "api_key": os.getenv("GROQ_API_KEY", ""),
    "base_url": os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1"),
    "model": os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),  # Model mặc định Groq
}

# Discord configuration
DISCORD_CONFIG = {
    "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", ""),
    "enabled": os.getenv("DISCORD_ENABLED", "false").lower() == "true",
}
