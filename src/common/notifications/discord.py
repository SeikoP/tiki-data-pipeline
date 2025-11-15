"""
Module để gửi thông báo qua Discord webhook
"""

import logging
import os
from typing import Any

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
        title: str | None = None,
        color: int = 0x00FF00,  # Màu xanh lá mặc định
        fields: list | None = None,
        footer: str | None = None,
    ) -> bool:
        """
        Gửi thông báo qua Discord webhook

        Args:
            content: Nội dung thông báo
            title: Tiêu đề embed (optional)
            color: Màu của embed (hex color code)
            fields: Danh sách các field để hiển thị (optional)
            footer: Footer text (optional)

        Returns:
            True nếu gửi thành công, False nếu có lỗi
        """
        if not self.enabled or not self.webhook_url:
            logger.warning("⚠️  Discord không được bật hoặc thiếu webhook URL, bỏ qua gửi thông báo")
            return False

        try:
            # Tạo embed với format đẹp hơn
            embed = {
                "title": title or "📊 Tổng hợp dữ liệu Tiki",
                "description": content,
                "color": color,
                "timestamp": None,  # Sẽ được set bởi Discord
            }

            if fields:
                embed["fields"] = fields
            
            if footer:
                embed["footer"] = {"text": footer}

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
                except Exception:
                    logger.error(f"   Response text: {e.response.text}")
            return False
        except Exception as e:
            logger.error(f"❌ Lỗi không xác định khi gửi thông báo Discord: {e}")
            return False

    def send_summary(
        self,
        ai_summary: str,
        stats: dict[str, Any],
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

        # Tính toán tỷ lệ thành công để chọn màu phù hợp
        crawled_count = stats.get("crawled_count", 0)
        with_detail = stats.get("with_detail", 0)
        failed = stats.get("failed", 0)
        timeout = stats.get("timeout", 0)
        
        # Chọn màu dựa trên tỷ lệ thành công
        if crawled_count > 0:
            success_rate = (with_detail / crawled_count) * 100
            if success_rate >= 80:
                color = 0x00FF00  # Xanh lá - thành công tốt
            elif success_rate >= 50:
                color = 0xFFA500  # Cam - cảnh báo
            else:
                color = 0xFF0000  # Đỏ - cần chú ý

        # Tạo fields từ stats - tối ưu layout
        fields = []
        if stats:
            total_products = stats.get("total_products", 0)
            products_saved = stats.get("products_saved", 0)
            
            # Row 1: Tổng quan
            if total_products > 0:
                fields.append(
                    {
                        "name": "📦 Tổng sản phẩm",
                        "value": f"**{total_products:,}**",
                        "inline": True,
                    }
                )
            
            if crawled_count > 0:
                fields.append(
                    {
                        "name": "🔄 Đã crawl detail",
                        "value": f"**{crawled_count:,}**",
                        "inline": True,
                    }
                )
            
            if products_saved > 0:
                fields.append(
                    {
                        "name": "💾 Đã lưu",
                        "value": f"**{products_saved:,}**",
                        "inline": True,
                    }
                )
            
            # Row 2: Kết quả crawl detail
            if crawled_count > 0:
                success_rate = (with_detail / crawled_count) * 100
                fields.append(
                    {
                        "name": "✅ Thành công",
                        "value": f"**{with_detail:,}** ({success_rate:.1f}%)",
                        "inline": True,
                    }
                )
            
            if timeout > 0:
                timeout_rate = (timeout / crawled_count * 100) if crawled_count > 0 else 0
                fields.append(
                    {
                        "name": "⏱️ Timeout",
                        "value": f"**{timeout:,}** ({timeout_rate:.1f}%)",
                        "inline": True,
                    }
                )
            
            if failed > 0:
                failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0
                fields.append(
                    {
                        "name": "❌ Thất bại",
                        "value": f"**{failed:,}** ({failed_rate:.1f}%)",
                        "inline": True,
                    }
                )

        # Giới hạn độ dài AI summary để tránh vượt quá Discord limit (2000 chars cho description)
        max_summary_length = 1800  # Để lại chỗ cho format
        if len(ai_summary) > max_summary_length:
            ai_summary = ai_summary[:max_summary_length] + "...\n\n*(Đã cắt ngắn do giới hạn độ dài)*"

        return self.send_message(
            content=ai_summary,
            title="🤖 Tổng hợp dữ liệu Tiki (AI)",
            color=color,
            fields=fields if fields else None,
        )
