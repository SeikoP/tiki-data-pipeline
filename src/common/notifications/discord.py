"""
Module để gửi thông báo qua Discord webhook
"""

import json
import logging
import os
from typing import Any, Dict, Optional

import requests

# Import config từ common.config (ưu tiên) hoặc pipelines.crawl.config (fallback)
try:
    from ..config import DISCORD_CONFIG
except ImportError:
    try:
        from ...pipelines.crawl.config import DISCORD_CONFIG
    except ImportError:
        # Fallback: đọc trực tiếp từ environment
        DISCORD_CONFIG = {
            "webhook_url": os.getenv("DISCORD_WEBHOOK_URL", ""),
            "enabled": os.getenv("DISCORD_ENABLED", "false").lower() == "true",
        }

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Class để gửi thông báo qua Discord webhook"""

    def __init__(self):
        self.webhook_url = DISCORD_CONFIG.get("webhook_url", "")
        self.enabled = DISCORD_CONFIG.get("enabled", False)

        if not self.webhook_url:
            logger.warning("⚠️  DISCORD_WEBHOOK_URL không được cấu hình trong environment variables")
        if not self.enabled:
            logger.warning("⚠️  DISCORD_ENABLED chưa được bật")

    def send_message(
        self,
        content: str,
        title: Optional[str] = None,
        color: int = 0x00FF00,  # Màu xanh lá mặc định
        fields: Optional[list] = None,
    ) -> bool:
        """
        Gửi thông báo qua Discord webhook

        Args:
            content: Nội dung thông báo
            title: Tiêu đề embed (optional)
            color: Màu của embed (hex color code)
            fields: Danh sách các field để hiển thị (optional)

        Returns:
            True nếu gửi thành công, False nếu có lỗi
        """
        if not self.enabled or not self.webhook_url:
            logger.warning("⚠️  Discord không được bật hoặc thiếu webhook URL, bỏ qua gửi thông báo")
            return False

        try:
            # Tạo embed
            embed = {
                "title": title or "📊 Tổng hợp dữ liệu Tiki",
                "description": content,
                "color": color,
                "timestamp": None,  # Sẽ được set bởi Discord
            }

            if fields:
                embed["fields"] = fields

            payload = {"embeds": [embed]}

            # Gửi request
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            response.raise_for_status()

            if response.status_code == 204:
                logger.info("✅ Đã gửi thông báo thành công qua Discord")
                return True
            else:
                logger.warning(f"⚠️  Response không mong đợi từ Discord: {response.status_code}")
                return False

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Lỗi khi gửi thông báo qua Discord: {e}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_detail = e.response.json()
                    logger.error(f"   Chi tiết lỗi: {error_detail}")
                except:
                    logger.error(f"   Response text: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi không xác định khi gửi thông báo Discord: {e}")
            return False

    def send_summary(
        self,
        ai_summary: str,
        stats: Dict[str, Any],
        color: int = 0x3498DB,  # Màu xanh dương
    ) -> bool:
        """
        Gửi bản tổng hợp từ AI kèm thống kê

        Args:
            ai_summary: Bản tổng hợp từ AI
            stats: Dictionary chứa thống kê
            color: Màu của embed

        Returns:
            True nếu gửi thành công, False nếu có lỗi
        """
        if not ai_summary:
            logger.warning("⚠️  Không có nội dung tổng hợp để gửi")
            return False

        # Tạo fields từ stats
        fields = []
        if stats:
            # Thêm các thống kê quan trọng
            if "total_products" in stats:
                fields.append(
                    {
                        "name": "📦 Tổng sản phẩm",
                        "value": str(stats.get("total_products", 0)),
                        "inline": True,
                    }
                )

            if "with_detail" in stats:
                fields.append(
                    {
                        "name": "✅ Có chi tiết",
                        "value": str(stats.get("with_detail", 0)),
                        "inline": True,
                    }
                )

            if "failed" in stats:
                fields.append(
                    {
                        "name": "❌ Thất bại",
                        "value": str(stats.get("failed", 0)),
                        "inline": True,
                    }
                )

            if "timeout" in stats:
                fields.append(
                    {
                        "name": "⏱️ Timeout",
                        "value": str(stats.get("timeout", 0)),
                        "inline": True,
                    }
                )

        return self.send_message(
            content=ai_summary,
            title="🤖 Tổng hợp dữ liệu Tiki (AI)",
            color=color,
            fields=fields if fields else None,
        )

