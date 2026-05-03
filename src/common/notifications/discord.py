"""
Module để gửi thông báo qua Discord webhook.
"""

import logging
import time
from typing import Any

import requests

# Import config từ common.config (ưu tiên) hoặc pipelines.crawl.config (fallback)
try:
    from ..config import DISCORD_CONFIG
except ImportError:
    try:
        from ...pipelines.crawl.config import DISCORD_CONFIG
    except ImportError:
        from common.settings import get_pipeline_settings

        DISCORD_CONFIG = get_pipeline_settings(use_airflow=False).notifications.as_discord_config()

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """
    Class để gửi thông báo qua Discord webhook.
    """

    def __init__(self):
        # Support both old and new config
        self.webhook_url = DISCORD_CONFIG.get("webhook_url", "")
        self.webhook_url_notification = (
            DISCORD_CONFIG.get("webhook_url_notification", "") or self.webhook_url
        )
        self.webhook_url_alert = DISCORD_CONFIG.get("webhook_url_alert", "")
        self.fallback_webhook_url = DISCORD_CONFIG.get("fallback_webhook_url", "")
        self.enabled = DISCORD_CONFIG.get("enabled", False)

        if not self.webhook_url_notification and not self.webhook_url_alert:
            logger.warning("⚠️  DISCORD_WEBHOOK_URL không được cấu hình trong environment variables")
        if not self.enabled:
            logger.warning("⚠️  DISCORD_ENABLED chưa được bật")
        if self.webhook_url_notification:
            logger.info("✅ Notification webhook configured")
        if self.webhook_url_alert:
            logger.info("✅ Alert webhook configured")
        if self.fallback_webhook_url:
            logger.info("✅ Fallback webhook configured")

    def send_message(
        self,
        content: str,
        title: str | None = None,
        color: int = 0x00FF00,
        fields: list | None = None,
        footer: str | None = None,
        max_retries: int = 3,
        channel: str = "notification",  # "notification" or "alert"
    ) -> bool:
        """Gửi thông báo qua Discord webhook (with retry and fallback).

        Args:
            content: Nội dung thông báo
            title: Tiêu đề embed (optional)
            color: Màu của embed (hex color code)
            fields: Danh sách các field để hiển thị (optional)
            footer: Footer text (optional)
            max_retries: Số lần retry tối đa (default: 3)
            channel: "notification" (tổng hợp) hoặc "alert" (chi tiết/cảnh báo)

        Returns:
            True nếu gửi thành công, False nếu có lỗi
        """
        if not self.enabled:
            logger.warning("⚠️  Discord không được bật, bỏ qua gửi thông báo")
            return False

        # Select webhook based on channel
        if channel == "alert" and self.webhook_url_alert:
            webhook_url = self.webhook_url_alert
        elif channel == "notification" and self.webhook_url_notification:
            webhook_url = self.webhook_url_notification
        else:
            # Fallback to default webhook
            webhook_url = self.webhook_url or self.webhook_url_notification

        if not webhook_url:
            logger.warning(f"⚠️  No webhook URL for channel '{channel}', bỏ qua gửi thông báo")
            return False

        # Try primary webhook first
        success = self._send_with_retry(
            webhook_url, content, title, color, fields, footer, max_retries
        )

        # If primary fails and fallback exists, try fallback
        if not success and self.fallback_webhook_url:
            logger.warning("⚠️  Primary webhook failed, trying fallback...")
            success = self._send_with_retry(
                self.fallback_webhook_url,
                content,
                title,
                color,
                fields,
                footer,
                max_retries=2,
            )

        return success

    def _send_with_retry(
        self,
        webhook_url: str,
        content: str,
        title: str | None,
        color: int,
        fields: list | None,
        footer: str | None,
        max_retries: int,
    ) -> bool:
        """Send message with exponential backoff retry."""
        # Split long content into multiple embeds if needed
        embeds = self._split_content_to_embeds(content, title, color, fields, footer)

        for attempt in range(max_retries):
            try:
                payload = {"embeds": embeds}

                # Gửi request
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=30,
                )

                response.raise_for_status()

                if response.status_code == 204:
                    logger.info(
                        f"✅ Đã gửi thông báo thành công qua Discord (attempt {attempt + 1})"
                    )
                    return True
                else:
                    logger.warning(f"⚠️  Response không mong đợi từ Discord: {response.status_code}")

            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Lỗi khi gửi thông báo qua Discord (attempt {attempt + 1}): {e}")
                if hasattr(e, "response") and e.response is not None:
                    try:
                        error_detail = e.response.json()
                        logger.error(f"   Chi tiết lỗi: {error_detail}")
                    except Exception:
                        logger.error(f"   Response text: {e.response.text}")

                # Retry with exponential backoff
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # 1s, 2s, 4s
                    logger.info(f"   Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error("   Max retries reached, giving up")

            except Exception as e:
                logger.error(f"❌ Lỗi không xác định khi gửi thông báo Discord: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)

        return False

    def _split_content_to_embeds(
        self,
        content: str,
        title: str | None,
        color: int,
        fields: list | None,
        footer: str | None,
    ) -> list[dict[str, Any]]:
        """Split long content into multiple embeds if needed."""
        max_description_length = 1800  # Leave room for formatting
        embeds = []

        # If content is short enough, return single embed
        if len(content) <= max_description_length:
            embed = {
                "title": title or "📊 Tổng hợp dữ liệu Tiki",
                "description": content,
                "color": color,
                "timestamp": None,
            }
            if fields:
                embed["fields"] = fields
            if footer:
                embed["footer"] = {"text": footer}
            embeds.append(embed)
            return embeds

        # Split long content into multiple embeds
        logger.info(f"⚠️  Content too long ({len(content)} chars), splitting into multiple embeds")

        # First embed with title and fields
        first_chunk = content[:max_description_length]
        # Try to split at newline
        last_newline = first_chunk.rfind("\n")
        if last_newline > max_description_length * 0.8:  # If newline is in last 20%
            first_chunk = content[:last_newline]
            remaining = content[last_newline:]
        else:
            remaining = content[max_description_length:]

        embed1 = {
            "title": title or "📊 Tổng hợp dữ liệu Tiki",
            "description": first_chunk + "\n\n*(Tiếp tục...)*",
            "color": color,
        }
        if fields:
            embed1["fields"] = fields
        embeds.append(embed1)

        # Additional embeds for remaining content
        while remaining:
            chunk = remaining[:max_description_length]
            last_newline = chunk.rfind("\n")
            if last_newline > max_description_length * 0.8:
                chunk = remaining[:last_newline]
                remaining = remaining[last_newline:]
            else:
                remaining = remaining[max_description_length:]

            embed = {
                "description": chunk + ("\n\n*(Tiếp tục...)*" if remaining else ""),
                "color": color,
            }
            if not remaining and footer:
                embed["footer"] = {"text": footer}
            embeds.append(embed)

        logger.info(f"   Split into {len(embeds)} embeds")
        return embeds

    def send_summary(
        self,
        ai_summary: str,
        stats: dict[str, Any],
        color: int = 0x3498DB,  # Màu xanh dương
    ) -> bool:
        """Gửi bản tổng hợp từ AI kèm thống kê.

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

        # Giới hạn độ dài AI summary để tránh vượt quá Discord limit (removed - now handled by split)
        return self.send_message(
            content=ai_summary,
            title="🤖 Tổng hợp dữ liệu Tiki (AI)",
            color=color,
            fields=fields if fields else None,
        )
