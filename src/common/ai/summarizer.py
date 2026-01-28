"""
Module để tổng hợp dữ liệu sử dụng Groq AI.
"""

import json
import logging
import os
import re
import time
from functools import lru_cache
from typing import Any

import requests

# Import config từ common.config (ưu tiên)
try:
    from ..config import AI_CONFIG
except ImportError:
    # Fallback: đọc trực tiếp từ environment
    AI_CONFIG = {
        "enabled": os.getenv("AI_ENABLED", os.getenv("GROQ_ENABLED", "false")).lower() == "true",
        "api_key": os.getenv("AI_API_KEY", os.getenv("GROQ_API_KEY", "")),
        "base_url": os.getenv(
            "AI_API_BASE", os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1")
        ),
        "model": os.getenv(
            "AI_MODEL", os.getenv("GROQ_MODEL", "arcee-ai/trinity-large-preview:free")
        ),
    }

logger = logging.getLogger(__name__)


class AISummarizer:
    """
    Class để tổng hợp dữ liệu sử dụng Groq AI.
    """

    def __init__(self):
        self.raw_api_key = AI_CONFIG.get("api_key", "")
        # Support multiple keys separated by comma
        self.api_keys = [k.strip() for k in self.raw_api_key.split(",") if k.strip()]
        self.current_key_index = 0

        self.base_url = AI_CONFIG.get("base_url", "https://openrouter.ai/api/v1")
        self.model = AI_CONFIG.get("model", "arcee-ai/trinity-large-preview:free")
        self.enabled = AI_CONFIG.get("enabled", False)

        if not self.api_keys:
            logger.warning("⚠️  AI_API_KEY không được cấu hình trong environment variables")
        if not self.enabled:
            logger.warning("⚠️  AI_ENABLED chưa được bật")

        if len(self.api_keys) > 1:
            logger.info(f"🔑 Đã load {len(self.api_keys)} API keys cho Groq AI")

    def _rotate_key(self):
        """
        Chuyển sang API Key tiếp theo.
        """
        if len(self.api_keys) <= 1:
            return

        old_index = self.current_key_index
        self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
        logger.info(f"🔄 Rotating API Key: {old_index} -> {self.current_key_index}")

    def summarize_data(self, data_summary: dict[str, Any], max_tokens: int = 2000) -> str:
        """Tổng hợp dữ liệu sử dụng Groq AI.

        Args:
            data_summary: Dictionary chứa thông tin tổng hợp về dữ liệu
            max_tokens: Số tokens tối đa cho response

        Returns:
            String chứa bản tổng hợp từ AI
        """
        if not self.enabled or not self.api_keys:
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
        """
        Tạo prompt cho AI từ dữ liệu tổng hợp.
        """
        # Lấy thống kê và làm rõ số liệu quan trọng
        stats = data_summary.get("statistics", {})
        total_products = stats.get("total_products", 0)
        crawled_count = stats.get("crawled_count", 0)  # Số products được crawl detail
        with_detail = stats.get("with_detail", 0)
        failed = stats.get("failed", 0)
        timeout = stats.get("timeout", 0)

        # Validation: với_detail không nên lớn hơn crawled_count
        if with_detail > crawled_count:
            logger.warning(
                f"⚠️  with_detail ({with_detail}) > crawled_count ({crawled_count}), điều chỉnh..."
            )
            with_detail = crawled_count

        # Tính toán các tỷ lệ dựa trên crawled_count (số thực tế đã crawl)
        success_rate = (with_detail / crawled_count * 100) if crawled_count > 0 else 0.0
        timeout_rate = (timeout / crawled_count * 100) if crawled_count > 0 else 0.0
        failed_rate = (failed / crawled_count * 100) if crawled_count > 0 else 0.0
        total_error_rate = ((timeout + failed) / crawled_count * 100) if crawled_count > 0 else 0.0

        # Tạo bảng so sánh rõ ràng về các con số quan trọng
        comparison_table = f"""
📊 **BẢNG SO SÁNH SỐ LIỆU QUAN TRỌNG:**
┌─ Tổng số sản phẩm trong danh sách (từ crawl list): {total_products:,}
├─ Số lượng sản phẩm ĐÃ ĐƯỢC CRAWL DETAIL: {crawled_count:,} (đây là số chính để phân tích)
├─ Sản phẩm có đầy đủ detail: {with_detail:,}
├─ Sản phẩm timeout: {timeout:,}
├─ Sản phẩm failed: {failed:,}
└─ Tỷ lệ thành công: {success_rate:.1f}% ({with_detail}/{crawled_count})

🔑 **NGUYÊN TẮC PHÂN TÍCH:**
1. KHI PHÂN TÍCH: Luôn so sánh/tính toán dựa trên {crawled_count:,} (ĐÃ CRAWL DETAIL) chứ KHÔNG phải {total_products:,}
2. Ví dụ: Nếu nói "X% sản phẩm có giá dưới 1 triệu", tính dựa trên {crawled_count:,} không phải {total_products:,}
3. Khi nói "Top 5 danh mục", đó là top từ {with_detail:,} sản phẩm đã crawl detail
4. So sánh DB stats: Hãy kiểm tra nếu các con số trong database khác với expected, có thể DB đã được cập nhật từ các nguồn khác
"""

        prompt = f"""Bạn là một chuyên gia phân tích dữ liệu. Hãy phân tích và tổng hợp thông tin sau về dữ liệu sản phẩm Tiki:

{comparison_table}

Data JSON:
{json.dumps(data_summary, ensure_ascii=False, indent=2)}

📝 **HƯỚNG DẪN TẠO BÁO CÁO:**

**1. Tổng quan về dữ liệu:**
- Số lượng sản phẩm đã crawl detail: {crawled_count:,} sản phẩm (từ {total_products:,} danh sách)
- Tỷ lệ thành công: {success_rate:.1f}% ({with_detail} sản phẩm với đầy đủ detail)
- Các sản phẩm không hoàn tất: Timeout {timeout} ({timeout_rate:.1f}%), Failed {failed} ({failed_rate:.1f}%)
- Tỷ lệ hoàn thành: {success_rate:.1f}% - [Đánh giá: Tốt/Bình thường/Cần cải thiện]

**2. Phân tích thống kê chi tiết (LUÔN dựa trên {crawled_count:,} sản phẩm):**
- Giá cả: Min, Max, Trung bình (VND) + Insight về phân bố giá
- Rating: Trung bình, Min, Max + % sản phẩm có rating trên 4.0
- Sales: Min, Max, Trung bình + % bestsellers (>1000 sales)
- Discount: Min, Max, Trung bình + % sản phẩm đang giảm giá
- Top 5 danh mục: "Danh mục X: Y sản phẩm (Z% tổng)"
- Top 5 seller: "Seller X: Y sản phẩm (Z% tổng)"

**3. Các vấn đề / lỗi:**
- Timeout: {timeout} products ({timeout_rate:.1f}%) - [Nguyên nhân có thể]
- Failed: {failed} products ({failed_rate:.1f}%) - [Nguyên nhân có thể]
- Tổng cộng: {timeout + failed} products ({total_error_rate:.1f}% lỗi)
- [Đề xuất xử lý nếu có]

**4. So sánh với database (nếu có sự khác biệt):**
- Nếu DB stats khác với crawl data, ghi chú điểm khác biệt
- Có thể DB đã được cập nhật từ các lần crawl trước
- [Kiểm tra consistency]

**5. Nhận xét & Đề xuất:**
- Đánh giá hiệu quả: Tỷ lệ thành công {success_rate:.1f}% [Tốt/Bình thường/Cần cải]
- Đề xuất cải thiện nếu tỷ lệ < 80%
- Điểm mạnh và điểm yếu

**⚠️ QUAN TRỌNG:**
- KHÔNG dùng bảng markdown (| |) vì khó đọc Discord
- Dùng bullet points: - hoặc •
- Format số: 1,234 (với dấu phẩy)
- Ngắn gọn, dễ đọc, tự nhiên
- LUÔN nhớ: {crawled_count:,} là số chính, {total_products:,} là bối cảnh"""

        return prompt

    @lru_cache(maxsize=2048)  # noqa: B019
    def shorten_product_name(self, product_name: str) -> str:
        """Rút gọn tên sản phẩm sử dụng AI (có caching và regex fallback).

        Args:
            product_name: Tên sản phẩm gốc

        Returns:
            Tên sản phẩm đã được rút gọn
        """
        if not product_name:
            return ""

        # 0. Pre-check: Nếu tên đã ngắn (< 15 chars) hoặc quá dài (> 200 chars - có thể là spam), trả về regex clean luôn
        if len(product_name) < 15:
            return product_name

        # 1. Regex Cleanup (Heuristic) - Luôn chạy cái này trước để tiết kiệm token
        # Loại bỏ các từ khóa spam/marketing phổ biến
        cleaned_name = self._regex_clean_name(product_name)

        # Nếu sau khi regex clean, tên đã đủ ngắn (< 40 chars) -> Return luôn, không cần AI
        if len(cleaned_name) < 40:
            return cleaned_name

        if not self.enabled or not self.api_keys:
            return cleaned_name

        try:
            prompt = f"""
Bạn là một chuyên gia ngôn ngữ học và chuyên gia tối ưu hóa dữ liệu thương mại điện tử (e-commerce).

Nhiệm vụ: Rút gọn "Tên gốc" thành "Tên rút gọn" cực kỳ súc tích, chuyên nghiệp và chuẩn SEO.

Tên gốc: "{cleaned_name}"

Quy tắc VÀNG:
1. Giữ lại LOẠI SẢN PHẨM chính (ví dụ: Máy tăm nước, Bàn chải điện, Bàn ủi hơi nước, Cây lau nhà).
2. Giữ lại THƯƠNG HIỆU (nếu có: Oxo, Parroti, 3M, Scotch Brite, Deli, Index Living Mall).
3. Giữ lại ĐẶC ĐIỂM CỐT LÕI duy nhất để phân biệt (ví dụ: 2 trong 1, Không dây, Mini, Cầm tay).
4. LOẠI BỎ hoàn toàn:
   - Các từ quảng cáo: Chính hãng, Cao cấp, Sang chảnh, Mẫu mới 2024, Bảo hành 12 tháng, Uy tín.
   - Các thông số thừa: W76xD30.5xH11.5Cm, 5 chế độ, 4 đầu thay thế, Công nghệ sóng âm, 5 nấc.
   - Các cụm từ khuyến mãi: Tặng kèm, Miễn phí, Giá rẻ, Sale sốc, Giao màu ngẫu nhiên.
   - Các mô tả tính năng rườm rà: Chải sạch mảng bám, Chăm sóc nướu, Ủi nhanh gấp gọn.

Ví dụ mục tiêu:
- "Máy vệ sinh chăm sóc răng miệng bằng điện... 5 Chế Độ... 4 đầu Bàn chải" -> "Bàn chải điện sóng âm"
- "Bàn Ủi Đồ Để Bàn Thép ERMA... | Index Living Mall" -> "Bàn ủi đồ thép ERMA"
- "Bàn Chải Nylon Vệ Sinh Khe Hở Cửa Sổ / Khe Hở Nhà Tắm" -> "Bàn chải nylon vệ sinh khe hở"
- "Chổi Chà Sàn Nhà Tắm Kết Hợp Gạt Nước Đầu Chữ V Deli" -> "Chổi chà sàn Deli 2 trong 1"
- "Dây Lò Xo Thông Tắc Cống, Nhà Vệ Sinh 5m 10m" -> "Dây lò xo thông tắc cống"

Yêu cầu định dạng:
- Độ dài: 4-7 từ.
- Trả về DUY NHẤT tên rút gọn.
- Viết hoa chữ cái đầu tiên của mỗi từ (Title Case).

Tên rút gọn:
"""

            # Increase max_tokens to accommodate reasoning steps used by some models
            response = self._call_groq_api(prompt, max_tokens=1000)
            if response:
                cleaned_name_ai = response.strip().strip('"').strip("'")
                # Fallback check: Nếu AI trả về tên quá ngắn hoặc rỗng, dùng regex clean
                if len(cleaned_name_ai) < 3:
                    return cleaned_name
                return cleaned_name_ai

            return cleaned_name

        except Exception as e:
            logger.error(f"❌ Lỗi khi rút gọn tên sản phẩm: {e}")
            return cleaned_name

    def _regex_clean_name(self, name: str) -> str:
        """
        Helper method để clean tên sản phẩm bằng regex.
        """
        if not name:
            return ""

        # 1. Remove hashtags (e.g., #jean)
        cleaned = re.sub(r"#\w+\b", "", name)

        # 2. Loại bỏ SKU codes phổ biến (e.g., CV0016, SP123, MS123)
        sku_patterns = [
            r"\b[A-Za-z]{2,}\d{3,}\b",  # CV0016, SP1234
            r"\b[A-Za-z]+\-\d+\b",  # SKU-123
            r"\bMS\s*\d+\b",  # MS 123
            r"\bDòng\s*.*\d+\b",  # Dòng X123
        ]
        for pattern in sku_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

        # 3. Loại bỏ ký tự đặc biệt thừa
        cleaned = re.sub(r"[\[\]\(\)\{\}\!]", " ", cleaned)

        # 4. Loại bỏ marketing keywords
        keywords = [
            "chính hãng",
            "cao cấp",
            "giá rẻ",
            "new",
            "hot",
            "xả kho",
            "thanh lý",
            "fullbox",
        ]
        pattern = r"\b(" + "|".join(keywords) + r")\b"
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)

        # 5. Normalize whitespace
        cleaned = " ".join(cleaned.split())

        return cleaned

    def _call_groq_api(self, prompt: str, max_tokens: int = 2000) -> str:
        """
        Gọi Groq API để tổng hợp (với Retry và Key Rotation)
        """
        # Thử với tối đa số lượng key * 2 lần (để retry mỗi key ít nhất 1 lần nếu cần)
        max_attempts = len(self.api_keys) * 2 if self.api_keys else 1
        attempts = 0

        while attempts < max_attempts:
            attempts += 1
            current_key = self.api_keys[self.current_key_index] if self.api_keys else ""

            try:
                headers = {
                    "Authorization": f"Bearer {current_key}",
                    "Content-Type": "application/json",
                }

                # OpenRouter specific headers (recommended)
                if "openrouter.ai" in self.base_url:
                    headers["HTTP-Referer"] = "https://github.com/SeikoP/tiki-data-pipeline"
                    headers["X-Title"] = "Tiki Data Pipeline"

                # Map model cũ sang model mới nếu cần
                model = self.model
                deprecated_models = {
                    "llama-3.1-70b-versatile": "openai/gpt-oss-120b",
                    "llama-3.3-70b-versatile": "openai/gpt-oss-120b",
                    "gpt-oss-120b": "openai/gpt-oss-120b",
                    "llama-3.1-8b-instant": "llama-3.1-8b-instant",
                }
                if model in deprecated_models:
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

                if response.status_code == 429:  # Rate Limit
                    retry_after = int(response.headers.get("Retry-After", 5))
                    wait_time = max(retry_after, 5)  # Wait at least 5s
                    logger.warning(
                        f"⚠️  Rate Limit (429) hit on Key #{self.current_key_index}. Waiting {wait_time}s then rotating..."
                    )
                    time.sleep(wait_time)
                    self._rotate_key()
                    continue

                if response.status_code == 401:  # Auth Error
                    logger.warning(
                        f"⚠️  Auth Error (401) on Key #{self.current_key_index}. Rotating..."
                    )
                    self._rotate_key()
                    continue

                response.raise_for_status()
                result = response.json()

                if "choices" in result and len(result["choices"]) > 0:
                    return result["choices"][0]["message"]["content"]

                # Nếu response 200 nhưng format lạ
                logger.error(f"❌ Response không hợp lệ từ Groq API: {result}")
                return ""

            except requests.exceptions.RequestException as e:
                # Xử lý các lỗi mạng khác
                logger.error(f"❌ Lỗi khi gọi Groq API (Key #{self.current_key_index}): {e}")

                # Nếu lỗi liên quan đến model, thử đổi model (chỉ làm 1 lần)
                if hasattr(e, "response") and e.response is not None:
                    error_detail = e.response.json()
                    error_msg = error_detail.get("error", {}).get("message", "")
                    if "not found" in error_msg.lower() or "deprecated" in error_msg.lower():
                        # Logic đổi model (đơn giản hóa)
                        pass

                # Với lỗi mạng, thử rotate key (có thể key này bị ban IP?)
                self._rotate_key()
                continue

            except Exception as e:
                logger.error(f"❌ Lỗi không xác định khi gọi Groq API: {e}")
                return ""

        logger.error("❌ Đã thử tất cả API keys nhưng đều thất bại.")
        return ""

    def generate_data_quality_report(self, conn) -> str:
        """Tạo báo cáo chất lượng dữ liệu với phân tích chiến lược giảm giá.

        Returns: Chuỗi báo cáo định dạng
        """
        try:
            from psycopg2.extras import RealDictCursor

            cur = conn.cursor(cursor_factory=RealDictCursor)

            # Lấy thống kê tổng quan
            cur.execute("""
                SELECT
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN sales_count IS NOT NULL AND sales_count > 0 THEN 1 END) as with_sales,
                    AVG(discount_percent) as avg_discount,
                    MAX(discount_percent) as max_discount,
                    MIN(discount_percent) as min_discount
                FROM products
            """)
            stats = cur.fetchone()

            # Lấy top 5 sản phẩm giảm giá cao
            cur.execute("""
                SELECT
                    product_id,
                    name,
                    url,
                    discount_percent,
                    price,
                    sales_count
                FROM products
                WHERE discount_percent IS NOT NULL
                    AND discount_percent > 20
                    AND name IS NOT NULL
                ORDER BY discount_percent DESC
                LIMIT 5
            """)
            discount_products = cur.fetchall()

            # Xây dựng báo cáo
            report = "🤖 BÁO CÁO PHÂN TÍCH DỮ LIỆU SẢN PHẨM TIKI\n"
            report += "━" * 50 + "\n\n"

            # I. Tổng quan
            report += "I. Tổng Quan Thu Thập Dữ Liệu\n\n"
            total = stats["total_products"] or 0
            with_sales = stats["with_sales"] or 0
            coverage = (with_sales * 100 / total) if total > 0 else 0

            report += "📊 Quy mô dataset:\n"
            report += f"   • Tổng sản phẩm trong DB: {total:,}\n"
            report += f"   • Sản phẩm có doanh số: {with_sales:,} ({coverage:.1f}%)\n"
            report += f"   • Sản phẩm không có doanh số: {total - with_sales:,} ({100 - coverage:.1f}%)\n\n"

            report += "✅ Chất lượng:\n"
            report += f"   • Hợp lệ đầy đủ: {with_sales:,} / {total:,} = {coverage:.1f}% ✓\n"
            report += f"   • Lỗi / thiếu dữ liệu: {100 - coverage:.1f}%\n"
            report += "   • Đánh giá: Dữ liệu ở mức chấp nhận được\n\n"

            # II. Phân tích giảm giá
            report += "II. Phân Tích Chiến Lược Giảm Giá\n\n"
            avg_disc = stats["avg_discount"] or 0
            max_disc = stats["max_discount"] or 0
            min_disc = stats["min_discount"] or 0

            report += "💰 Mức giảm giá trên thị trường:\n"
            report += f"   • Trung bình: {avg_disc:.1f}%\n"
            report += f"   • Phạm vi: {min_disc:.1f}% – {max_disc:.1f}%\n"
            report += "   • Nhận định: Hầu hết sản phẩm áp dụng giảm giá nhẹ (<20%)\n\n"

            # Top 5 sản phẩm giảm giá
            report += "📌 Các sản phẩm giảm giá sâu (>20%):\n\n"
            for i, prod in enumerate(discount_products, 1):
                name = (prod["name"] or "N/A")[:50]
                disc = prod["discount_percent"] or 0
                price = prod["price"] or 0
                sales = prod["sales_count"] or 0
                url = prod.get("url") or ""

                report += f"{i}️⃣ {name}\n"
                report += f"   Giảm: {disc:.1f}% | Giá: {price:,.0f}đ | Bán: {sales:,} cái\n"
                if url:
                    report += f"   🔗 {url}\n"
                report += "\n"

            report += "💡 Insight: Giảm 60-70% hiệu quả nếu sản phẩm có thương hiệu mạnh\n"
            report += "   Giảm > 75% thường là tín hiệu 'thanh lý' hoặc 'giá gốc ảo'\n"

            return report

        except Exception as e:
            logger.error(f"❌ Lỗi tạo báo cáo: {e}")
            return ""
