"""
Configuration module cho common modules Load từ file .env trong src/common/ hoặc từ environment
variables.
"""

import logging
import os
from pathlib import Path

from common.settings import get_pipeline_settings

logger = logging.getLogger(__name__)

# Tìm file .env ở root (lên 2 cấp từ src/common/)
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent
env_file_candidates = [
    Path(os.getenv("ENV_FILE_PATH", "")) if os.getenv("ENV_FILE_PATH") else None,
    project_root / ".env",
]
env_file = next(
    (path for path in env_file_candidates if path and path.exists()), project_root / ".env"
)

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

_SETTINGS = get_pipeline_settings(use_airflow=False)

# AI configuration (Groq or any OpenAI-compatible provider)
AI_CONFIG = _SETTINGS.ai.as_config()

# Discord configuration
DISCORD_CONFIG = _SETTINGS.notifications.as_discord_config()

# Short Name AI Configuration
SHORT_NAME_CONFIG = {"use_ai": _SETTINGS.short_name_use_ai}

# Performance Optimization Feature Flags
OPTIMIZATION_CONFIG = _SETTINGS.optimization
