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

    def shorten_product_name(self, product_name: str) -> str:
        """
        Rút gọn tên sản phẩm sử dụng AI

        Args:
            product_name: Tên sản phẩm gốc

        Returns:
            Tên sản phẩm đã được rút gọn
        """
        if not self.enabled or not self.api_key:
            return product_name

        if not product_name or len(product_name) < 50:
            return product_name

        try:
            prompt = f"""
Bạn là trợ lý AI chuyên chuẩn hóa và rút gọn tên sản phẩm thương mại điện tử.

Tên gốc: "{product_name}"

Nhiệm vụ:
- Tạo một tên sản phẩm ngắn gọn, rõ nghĩa, phù hợp để hiển thị trên sàn TMĐT.

Quy tắc bắt buộc:
1. Giữ lại theo thứ tự ưu tiên:
   - Loại sản phẩm chính (ví dụ: Bikini, Áo bikini, Quần bơi, Đồ bơi, Bộ bà ba, Đồ lam…)
   - Đối tượng hoặc giới tính (nữ, nam, bé gái) nếu có
   - Đặc điểm quan trọng nhất (1 mảnh / 2 mảnh / liền thân / tay dài / lưng cao…)
   - Chất liệu hoặc họa tiết nổi bật (thun lạnh, len, lụa, hoa nhí…)
   - Thương hiệu hoặc dòng sản phẩm nếu có

2. Loại bỏ hoàn toàn:
   - Từ marketing, cảm xúc: sexy, quyến rũ, cao cấp, siêu đẹp, gợi cảm…
   - Mô tả dư thừa, hashtag, ký tự trang trí
   - Mã sản phẩm, quà tặng, thông tin bán hàng

3. Độ dài tối đa: 10–15 từ.

4. Không tự suy diễn thông tin không có trong tên gốc.

5. Trả về CHỈ tên đã rút gọn, không kèm giải thích, không xuống dòng.

6. Giữ nguyên ngôn ngữ gốc (Việt/Anh), viết hoa chữ cái đầu mỗi cụm chính.

Tên rút gọn:
"""

            # Increase max_tokens to accommodate reasoning steps used by some models
            response = self._call_groq_api(prompt, max_tokens=1000)
            if response:
                cleaned_name = response.strip().strip('"').strip("'")
                return cleaned_name
            return product_name

        except Exception as e:
            logger.error(f"❌ Lỗi khi rút gọn tên sản phẩm: {e}")
            return product_name

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

            if response.status_code == 429:
                logger.warning("⚠️  Groq AI Rate Limit (429) hit. Please check your plan limits.")
                return ""

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

    def generate_data_quality_report(self, conn) -> str:
        """
        Tạo báo cáo chất lượng dữ liệu với phân tích chiến lược giảm giá

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
            report += (
                f"   • Sản phẩm không có doanh số: {total - with_sales:,} ({100-coverage:.1f}%)\n\n"
            )

            report += "✅ Chất lượng:\n"
            report += f"   • Hợp lệ đầy đủ: {with_sales:,} / {total:,} = {coverage:.1f}% ✓\n"
            report += f"   • Lỗi / thiếu dữ liệu: {100-coverage:.1f}%\n"
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
