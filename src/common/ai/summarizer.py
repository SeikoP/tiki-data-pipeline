"""
Module để tổng hợp dữ liệu sử dụng Groq AI
"""

import json
import logging
import os
from typing import Any

import requests

# Import config từ common.config (ưu tiên) hoặc pipelines.crawl.config (fallback)
try:
    from ..config import GROQ_CONFIG
except ImportError:
    try:
        from ...pipelines.crawl.config import GROQ_CONFIG
    except ImportError:
        # Fallback: đọc trực tiếp từ environment
        GROQ_CONFIG = {
            "enabled": os.getenv("GROQ_ENABLED", "false").lower() == "true",
            "api_key": os.getenv("GROQ_API_KEY", ""),
            "base_url": os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1"),
            "model": os.getenv("GROQ_MODEL", "openai/gpt-oss-120b"),
        }

logger = logging.getLogger(__name__)


class AISummarizer:
    """Class để tổng hợp dữ liệu sử dụng Groq AI"""

    def __init__(self):
        self.api_key = GROQ_CONFIG.get("api_key", "")
        self.base_url = GROQ_CONFIG.get("base_url", "https://api.groq.com/openai/v1")
        self.model = GROQ_CONFIG.get("model", "openai/gpt-oss-120b")
        self.enabled = GROQ_CONFIG.get("enabled", False)

        if not self.api_key:
            logger.warning("⚠️  GROQ_API_KEY không được cấu hình trong environment variables")
        if not self.enabled:
            logger.warning("⚠️  GROQ_ENABLED chưa được bật")

    def summarize_data(self, data_summary: dict[str, Any], max_tokens: int = 2000) -> str:
        """
        Tổng hợp dữ liệu sử dụng Groq AI

        Args:
            data_summary: Dictionary chứa thông tin tổng hợp về dữ liệu
            max_tokens: Số tokens tối đa cho response

        Returns:
            String chứa bản tổng hợp từ AI
        """
        if not self.enabled or not self.api_key:
            logger.warning("⚠️  Groq AI không được bật hoặc thiếu API key, bỏ qua tổng hợp")
            return ""

        try:
            # Tạo prompt cho AI
            prompt = self._create_prompt(data_summary)

            # Gọi Groq API
            response = self._call_groq_api(prompt, max_tokens)

            if response:
                logger.info("✅ Tổng hợp dữ liệu thành công với Groq AI")
                return response
            else:
                logger.warning("⚠️  Không nhận được response từ Groq AI")
                return ""

        except Exception as e:
            logger.error(f"❌ Lỗi khi tổng hợp dữ liệu với Groq AI: {e}")
            return ""

    def _create_prompt(self, data_summary: dict[str, Any]) -> str:
        """Tạo prompt cho AI từ dữ liệu tổng hợp"""
        # Lấy thống kê và làm rõ số liệu quan trọng
        stats = data_summary.get("statistics", {})
        total_products = stats.get("total_products", 0)
        crawled_count = stats.get("crawled_count", 0)  # Số products được crawl detail
        with_detail = stats.get("with_detail", 0)
        failed = stats.get("failed", 0)
        timeout = stats.get("timeout", 0)
        
        # Tính toán các tỷ lệ
        success_rate = (with_detail / crawled_count * 100) if crawled_count > 0 else 0.0
        timeout_rate = (timeout / crawled_count * 100) if crawled_count > 0 else 0.0
        failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0.0
        total_error_rate = ((timeout + failed) / crawled_count * 100) if crawled_count > 0 else 0.0
        
        # Tạo context rõ ràng cho AI
        context_note = ""
        if crawled_count > 0:
            context_note = f"""
**LƯU Ý QUAN TRỌNG:**
- Tổng số sản phẩm từ danh sách: {total_products:,}
- Số lượng sản phẩm ĐÃ ĐƯỢC CRAWL DETAIL: {crawled_count:,} (đây là số liệu quan trọng để phân tích)
- Tỷ lệ thành công crawl detail: {success_rate:.1f}% ({with_detail}/{crawled_count})
- Khi phân tích, hãy SO SÁNH với số lượng ĐÃ CRAWL ({crawled_count:,}) chứ KHÔNG phải tổng số ({total_products:,})
"""
        
        prompt = f"""Bạn là một chuyên gia phân tích dữ liệu. Hãy phân tích và tổng hợp thông tin sau về dữ liệu sản phẩm Tiki:

{context_note}

{json.dumps(data_summary, ensure_ascii=False, indent=2)}

Hãy tạo một bản tổng hợp ngắn gọn, dễ hiểu bằng tiếng Việt với format nhất quán:

**1. Tổng quan về dữ liệu:**
- Số lượng sản phẩm đã được crawl detail: {crawled_count:,} (KHÔNG phải tổng số {total_products:,})
- Tỷ lệ thành công: {success_rate:.1f}% ({with_detail}/{crawled_count} products)
- Thời gian crawl: [từ metadata]

**2. Phân tích thống kê (chỉ cho {with_detail} sản phẩm có detail):**
- Giá trung bình: [min - max, trung bình]
- Rating trung bình: [dựa trên số sản phẩm có rating]
- Discount trung bình: [min - max, trung bình]
- Top 5 danh mục: [danh sách với số lượng, format: "Danh mục X: Y sản phẩm"]
- Top 5 seller: [danh sách với số lượng, format: "Seller X: Y sản phẩm"]

**3. Các vấn đề / lỗi:**
- Timeout: {timeout} products ({timeout_rate:.1f}%)
- Failed: {failed} products ({failed_rate:.1f}%)
- Tổng lỗi: {timeout + failed} products ({total_error_rate:.1f}% của {crawled_count:,} đã crawl)

**4. Nhận xét & Đề xuất:**
- Đánh giá hiệu quả crawl (dựa trên tỷ lệ thành công {success_rate:.1f}%)
- Đề xuất cải thiện nếu tỷ lệ thành công thấp (< 50%)

**QUAN TRỌNG:**
- KHÔNG sử dụng bảng markdown (| | |) vì khó đọc trong Discord
- Sử dụng format danh sách với bullet points (- hoặc •)
- Khi nói về "số lượng sản phẩm" hoặc tính tỷ lệ, LUÔN sử dụng số lượng ĐÃ CRAWL DETAIL ({crawled_count:,}) chứ KHÔNG phải tổng số ({total_products:,})
- Viết ngắn gọn, tự nhiên, dễ đọc trong Discord embed
- Format số với dấu phẩy (ví dụ: 1,234 thay vì 1234)"""

        return prompt

    def _call_groq_api(self, prompt: str, max_tokens: int = 2000) -> str:
        """Gọi Groq API để tổng hợp"""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }

            # Map model cũ sang model mới nếu cần
            model = self.model
            deprecated_models = {
                "llama-3.1-70b-versatile": "openai/gpt-oss-120b",  # Chuyển sang model mặc định mới
                "llama-3.3-70b-versatile": "openai/gpt-oss-120b",  # Chuyển sang model mặc định mới
                "gpt-oss-120b": "openai/gpt-oss-120b",  # Chuyển format cũ sang format mới
                "llama-3.1-8b-instant": "llama-3.1-8b-instant",  # Vẫn còn hỗ trợ
            }
            if model in deprecated_models:
                logger.info(
                    f"ℹ️  Model {model} đã deprecated, tự động chuyển sang {deprecated_models[model]}"
                )
                model = deprecated_models[model]

            payload = {
                "model": model,
                "messages": [
                    {
                        "role": "system",
                        "content": "Bạn là một chuyên gia phân tích dữ liệu, chuyên tổng hợp và trình bày thông tin một cách dễ hiểu.",
                    },
                    {"role": "user", "content": prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": 0.7,
            }

            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=60,
            )

            response.raise_for_status()
            result = response.json()

            if "choices" in result and len(result["choices"]) > 0:
                return result["choices"][0]["message"]["content"]
            else:
                logger.error(f"❌ Response không hợp lệ từ Groq API: {result}")
                return ""

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Lỗi khi gọi Groq API: {e}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_detail = e.response.json()
                    error_msg = error_detail.get("error", {}).get("message", "")
                    logger.error(f"   Chi tiết lỗi: {error_detail}")

                    # Tự động xử lý model deprecated hoặc không tồn tại
                    if (
                        "decommissioned" in error_msg.lower()
                        or "deprecated" in error_msg.lower()
                        or "does not exist" in error_msg.lower()
                        or "not found" in error_msg.lower()
                    ):
                        logger.warning("⚠️  Model không khả dụng, thử với model thay thế...")
                        # Thử với các model thay thế theo thứ tự ưu tiên
                        fallback_models = [
                            "openai/gpt-oss-120b",
                            "llama-3.3-70b-versatile",
                            "llama-3.1-8b-instant",
                            "mixtral-8x7b-32768",
                        ]
                        current_model_index = -1
                        if self.model in fallback_models:
                            current_model_index = fallback_models.index(self.model)

                        # Thử model tiếp theo trong danh sách
                        if current_model_index < len(fallback_models) - 1:
                            next_model = fallback_models[current_model_index + 1]
                            self.model = next_model
                            logger.info(f"   Đang thử lại với model: {self.model}")
                            return self._call_groq_api(prompt, max_tokens)
                        else:
                            logger.error("❌ Đã thử tất cả model thay thế nhưng không thành công")
                except Exception:
                    logger.error(f"   Response text: {e.response.text}")
            return ""
        except Exception as e:
            logger.error(f"❌ Lỗi không xác định khi gọi Groq API: {e}")
            return ""
