"""
Extract product details từ Tiki product pages
Sử dụng AI extraction với Firecrawl + Groq
"""
import os
import sys
import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup

# Fix encoding on Windows
if sys.platform == "win32":
    import io
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

from .config import GROQ_CONFIG, get_config, FIRECRAWL_API_URL, TIKI_API_BASE_URL, TIKI_API_TOKEN
from .extract_products import extract_product_id

config = get_config()

# Import Groq key manager
try:
    from .groq_config import get_groq_api_key
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False
    get_groq_api_key = None

# Check if Groq is enabled
GROQ_ENABLED = GROQ_CONFIG.get("enabled", False) and GROQ_AVAILABLE

# Import requests (cần cho cả Groq API và Tiki API)
import requests

# Groq API setup
if GROQ_ENABLED:
    GROQ_API_BASE = GROQ_CONFIG.get("base_url", "https://api.groq.com/openai/v1")
    GROQ_MODEL = GROQ_CONFIG.get("model", "llama-3.1-70b-versatile")


# ===== PRODUCT DETAILS SCHEMA =====
PRODUCT_DETAILS_SCHEMA = {
    "type": "object",
    "properties": {
        "product_id": {
            "type": "string",
            "description": "Product ID từ URL (ví dụ: 123345348)"
        },
        "name": {
            "type": "string",
            "description": "Tên sản phẩm đầy đủ"
        },
        "price": {
            "type": "object",
            "properties": {
                "current_price": {
                    "type": "number",
                    "description": "Giá hiện tại sau giảm giá (VND)"
                },
                "original_price": {
                    "type": "number",
                    "description": "Giá gốc trước giảm giá (VND)"
                },
                "discount_percent": {
                    "type": "number",
                    "description": "Phần trăm giảm giá (ví dụ: 23 cho 23%)"
                },
                "currency": {
                    "type": "string",
                    "description": "Đơn vị tiền tệ (mặc định: VND)"
                }
            },
            "required": ["current_price"]
        },
        "description": {
            "type": "string",
            "description": "Mô tả sản phẩm chi tiết"
        },
        "specifications": {
            "type": "object",
            "description": "Thông số kỹ thuật dạng key-value",
            "additionalProperties": {
                "type": "string"
            }
        },
        "detailed_info": {
            "type": "string",
            "description": "Thông tin chi tiết sản phẩm (phần 'Thông tin chi tiết' trên trang)"
        },
        "customer_reviews": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "reviewer_name": {
                        "type": "string",
                        "description": "Tên người đánh giá"
                    },
                    "rating": {
                        "type": "number",
                        "description": "Điểm đánh giá (1-5 sao)"
                    },
                    "review_text": {
                        "type": "string",
                        "description": "Nội dung đánh giá"
                    },
                    "review_date": {
                        "type": "string",
                        "description": "Ngày đánh giá"
                    },
                    "verified_purchase": {
                        "type": "boolean",
                        "description": "Đã mua hàng xác thực"
                    }
                }
            },
            "description": "Danh sách đánh giá từ khách hàng (lấy từ các đánh giá mới nhất)"
        },
        "rating": {
            "type": "object",
            "properties": {
                "average": {
                    "type": "number",
                    "description": "Điểm đánh giá trung bình (0-5)"
                },
                "total_reviews": {
                    "type": "integer",
                    "description": "Tổng số đánh giá"
                },
                "rating_distribution": {
                    "type": "object",
                    "description": "Phân bố đánh giá (5 sao, 4 sao, ...)",
                    "additionalProperties": {
                        "type": "integer"
                    }
                }
            }
        },
        "seller": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Tên người bán/shop"
                },
                "is_official": {
                    "type": "boolean",
                    "description": "Có phải hàng chính hãng không"
                },
                "seller_id": {
                    "type": "string",
                    "description": "ID của seller"
                }
            }
        },
        "shipping": {
            "type": "object",
            "properties": {
                "free_shipping": {
                    "type": "boolean",
                    "description": "Có miễn phí ship không"
                },
                "fast_delivery": {
                    "type": "boolean",
                    "description": "Có giao nhanh (2H, TikiNOW) không"
                },
                "delivery_time": {
                    "type": "string",
                    "description": "Thời gian giao hàng (ví dụ: 'Giao thứ 6, 14/11')"
                }
            }
        },
        "stock": {
            "type": "object",
            "properties": {
                "available": {
                    "type": "boolean",
                    "description": "Còn hàng không"
                },
                "quantity": {
                    "type": "integer",
                    "description": "Số lượng còn lại (nếu có)"
                },
                "stock_status": {
                    "type": "string",
                    "description": "Trạng thái tồn kho (ví dụ: 'Còn hàng', 'Hết hàng')"
                }
            }
        },
        "category_path": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "Đường dẫn category (ví dụ: ['Điện thoại', 'Smartphone', 'iPhone'])"
        },
        "brand": {
            "type": "string",
            "description": "Thương hiệu sản phẩm (ví dụ: Apple, Samsung)"
        },
        "warranty": {
            "type": "string",
            "description": "Thông tin bảo hành (ví dụ: '12 tháng', 'Chính hãng Apple 1 năm', v.v.)"
        },
        "promotions": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "Danh sách khuyến mãi (ví dụ: ['Giảm 10%', 'Tặng kèm ốp lưng'])"
        },
        "ai_summary": {
            "type": "object",
            "properties": {
                "product_summary": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "Danh sách các nhận xét tích cực/tiêu cực về sản phẩm từ AI tổng hợp (ví dụ: từ phần 'Trợ lý AI tổng hợp từ các đánh giá mới nhất')"
                },
                "service_summary": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description": "Danh sách các nhận xét tích cực/tiêu cực về dịch vụ từ AI tổng hợp"
                },
                "positive_count": {
                    "type": "object",
                    "description": "Số lượng đánh giá tích cực (dạng: {'product': 88, 'service': 85})",
                    "additionalProperties": {
                        "type": "integer"
                    }
                },
                "negative_count": {
                    "type": "object",
                    "description": "Số lượng đánh giá tiêu cực (dạng: {'product': 1, 'service': 3})",
                    "additionalProperties": {
                        "type": "integer"
                    }
                }
            },
            "description": "Tóm tắt đánh giá từ Trợ lý AI (nếu trang có phần này)"
        }
    },
    "required": ["product_id", "name", "price"]
}


def create_extraction_prompt(product_url: str, product_id: str, product_name: str = None) -> str:
    """
    Tạo prompt cho AI extraction
    
    Args:
        product_url: URL của product page
        product_id: Product ID
        product_name: Tên sản phẩm (optional)
    
    Returns:
        Extraction prompt
    """
    prompt = f"""
BẠN PHẢI EXTRACT ĐẦY ĐỦ TẤT CẢ THÔNG TIN có sẵn trên trang Tiki.vn.

Product Information:
- Product ID: {product_id}
- Product Name: {product_name or 'N/A'}
- URL: {product_url}

Hãy đọc KỸ LƯỠNG toàn bộ nội dung trang và extract TẤT CẢ thông tin sau:

1. **Thông tin cơ bản (BẮT BUỘC):**
   - Tên sản phẩm đầy đủ (lấy từ tiêu đề chính)
   - Product ID: {product_id}
   - Brand/Thương hiệu (tìm trong tên sản phẩm hoặc thông số)

2. **Giá cả (QUAN TRỌNG):**
   - Giá hiện tại (sau giảm giá) - TÌM KỸ trong trang
   - Giá gốc (giá cũ, giá niêm yết) - thường có dấu gạch ngang
   - Phần trăm giảm giá (tính từ giá gốc và giá hiện tại)
   - Currency: "VND"

3. **Mô tả sản phẩm (QUAN TRỌNG - PHẢI CÓ):**
   - Tìm phần "Mô tả sản phẩm", "Thông tin sản phẩm", "Giới thiệu", "Mô tả", "Chi tiết sản phẩm"
   - Tìm các đoạn văn mô tả về sản phẩm (không phải thông số kỹ thuật, không phải "Đặc điểm nổi bật")
   - Extract toàn bộ mô tả, không bỏ sót, không cắt ngắn
   - Nếu có nhiều đoạn, gộp lại thành một chuỗi
   - Nếu trang có mô tả, PHẢI extract, không được để rỗng ""
   - LƯU Ý: "Đặc điểm nổi bật" KHÔNG phải là mô tả, đó là thông tin tóm tắt

4. **Thông số kỹ thuật (RẤT QUAN TRỌNG - PHẢI CÓ):**
   - Tìm bảng "Thông số kỹ thuật", "Specifications", "Đặc điểm", "Thông số", "Thông tin kỹ thuật"
   - Tìm các dòng có format "Tên thông số: Giá trị" hoặc bảng 2 cột
   - Extract TẤT CẢ các thông số dạng key-value
   - Ví dụ cho điện thoại: "RAM": "8GB", "ROM": "128GB", "Màn hình": "6.1 inch", "CPU": "A14 Bionic", "Camera sau": "12MP", "Camera trước": "12MP", "Pin": "2815 mAh", "Hệ điều hành": "iOS 14", "Kích thước": "146.7 x 71.5 x 7.4 mm", "Trọng lượng": "162g"
   - PHẢI có ít nhất 5-15 thông số nếu trang có bảng thông số
   - KHÔNG được để specifications rỗng {{}} nếu trang có thông số kỹ thuật

5. **Thông tin chi tiết (QUAN TRỌNG):**
   - Tìm phần "Thông tin chi tiết", "Chi tiết sản phẩm", "Mô tả chi tiết", "Thông tin sản phẩm"
   - Trong HTML, tìm các selector: div[class*="detail"], div[id*="detail"], section[class*="detail"]
   - Tìm heading (h1-h6) chứa "Thông tin chi tiết" và lấy nội dung sau đó
   - Extract toàn bộ nội dung phần này (có thể là HTML, text, hoặc structured content)
   - Bao gồm các thông tin bổ sung về sản phẩm KHÔNG nằm trong mô tả ngắn
   - KHÁC với description: detailed_info là phần mở rộng, chi tiết hơn, có thể có bảng, danh sách, HTML
   - Nếu có nhiều phần, gộp lại thành một chuỗi
   - Nếu không có phần riêng "Thông tin chi tiết", để detailed_info = ""

6. **Khách hàng đánh giá (QUAN TRỌNG):**
   - Tìm phần "Khách hàng đánh giá", "Đánh giá", "Reviews", "Nhận xét", "Bình luận"
   - Extract TẤT CẢ các đánh giá mới nhất từ khách hàng (ít nhất 5-10 đánh giá)
   - Mỗi đánh giá cần có:
     * Tên người đánh giá (tên thật, không phải "Khách hàng 1", "User 1", v.v.)
     * Điểm đánh giá (số sao: 1-5)
     * Nội dung đánh giá (toàn bộ text)
     * Ngày đánh giá (nếu có)
     * Trạng thái đã mua hàng xác thực (nếu có)
   - Sắp xếp theo thứ tự mới nhất trước
   - QUAN TRỌNG: CHỈ extract reviews có thật trong nội dung trang, KHÔNG tự tạo reviews giả
   - Nếu không có reviews trong nội dung, để customer_reviews = []

7. **Đánh giá tổng quan:**
   - Điểm đánh giá trung bình (số từ 0-5, có thể có số thập phân)
   - Tổng số đánh giá (số lượng reviews)
   - Phân bố đánh giá (5 sao: X, 4 sao: Y, ...) nếu có

8. **Người bán (QUAN TRỌNG):**
   - Tên shop/người bán (tìm "Bởi", "Người bán", "Shop")
   - Có phải hàng chính hãng không (tìm "Chính hãng", "Official")
   - Seller ID nếu có

9. **Vận chuyển:**
   - Có miễn phí ship không (tìm "Miễn phí vận chuyển", "Freeship")
   - Có giao nhanh không (tìm "TikiNOW", "Giao nhanh", "2H")
   - Thời gian giao hàng (ví dụ: "Giao thứ 6, 14/11")

10. **Tồn kho:**
   - Còn hàng không (tìm "Còn hàng", "Hết hàng", "Sắp có hàng")
   - Số lượng còn lại nếu có
   - Trạng thái tồn kho (text mô tả)

11. **Category path:**
    - Tìm breadcrumb, đường dẫn category (ví dụ: Điện thoại > Smartphone > iPhone)
    - Extract thành array: ["Điện thoại", "Smartphone", "iPhone"]

12. **Bảo hành (QUAN TRỌNG):**
    - Tìm phần "Bảo hành", "Warranty", "Thông tin bảo hành"
    - Extract thông tin bảo hành (ví dụ: "12 tháng", "Bảo hành chính hãng", "Apple Care 1 năm")
    - Có thể là trong phần thông số kỹ thuật hoặc phần riêng
    - Nếu trang có bảo hành, PHẢI extract, không được để null ""

13. **Khuyến mãi:**
    - Tìm tất cả khuyến mãi, ưu đãi
    - Ví dụ: "Giảm 10%", "Tặng kèm ốp lưng", "Trả góp 0%"
    - Extract thành array

14. **AI Tóm Tắt Đánh Giá (NẾU CÓ - RẤT QUAN TRỌNG):**
    - Tìm phần "Trợ lý AI tổng hợp từ các đánh giá mới nhất" hoặc "AI Summary"
    - TRONG HTML: Tìm div với id="ai-summary" hoặc div[id="ai-summary"] hoặc div[class*="ai-summary"]
    - Selector cụ thể: div#ai-summary, div[id="ai-summary"], div[class*="ai-summary"]
    - Nếu tìm thấy phần này, extract:
      * product_summary: Danh sách các nhận xét tích cực/tiêu cực về sản phẩm (từ các list items, paragraphs trong div#ai-summary)
      * service_summary: Danh sách các nhận xét tích cực/tiêu cực về dịch vụ (từ các list items, paragraphs trong div#ai-summary)
      * positive_count: Số lượng positive về sản phẩm và dịch vụ (ví dụ: {{"product": 88, "service": 85}})
      * negative_count: Số lượng negative về sản phẩm và dịch vụ (ví dụ: {{"product": 1, "service": 3}})
    - Phân loại các items thành product_summary hoặc service_summary dựa trên từ khóa (sản phẩm/product vs dịch vụ/service)
    - Nếu không có phần này trong HTML hoặc markdown, để ai_summary = {{"product_summary": [], "service_summary": [], "positive_count": {{}}, "negative_count": {{}}}}

QUAN TRỌNG:
- PHẢI đọc KỸ toàn bộ nội dung trang, không bỏ sót
- Extract TẤT CẢ thông tin có sẵn, không để null nếu thông tin có trên trang
- Giá cả phải là số nguyên (VND), không có dấu phẩy, dấu chấm
- Warranty: extract nếu có, không để rỗng nếu trang có thông tin bảo hành
- Specifications PHẢI có ít nhất 5-10 thông số nếu trang có
- Description PHẢI có nội dung nếu trang có mô tả
- Seller info PHẢI có nếu trang hiển thị
- Detailed info: Tìm kỹ phần "Thông tin chi tiết" trong HTML, có thể nằm trong div/section có class/id chứa "detail" hoặc "info"
- AI Summary: TÌM KỸ div#ai-summary hoặc div[id="ai-summary"] trong HTML. CHỈ extract nếu thực sự có phần này trên trang, KHÔNG tự tạo. Nếu không tìm thấy, để ai_summary với các mảng rỗng
"""
    return prompt


def create_system_prompt() -> str:
    """Tạo system prompt cho AI extraction"""
    return """Bạn là chuyên gia extract dữ liệu từ trang web Tiki.vn với độ chính xác và đầy đủ cao.

NHIỆM VỤ CỦA BẠN:
1. Đọc KỸ LƯỠNG toàn bộ nội dung trang web Tiki product page
2. Extract ĐẦY ĐỦ TẤT CẢ thông tin có sẵn trên trang
3. Không được bỏ sót bất kỳ thông tin nào
4. Trả về JSON object thuần túy, KHÔNG có text giải thích trước/sau JSON

QUY TẮC QUAN TRỌNG:
- Giá cả: số nguyên (VND), không có dấu phẩy, dấu chấm (ví dụ: 16990000)
- Thông số kỹ thuật: extract TẤT CẢ thành key-value pairs, KHÔNG để rỗng {{}} nếu có thông số
- Mô tả: extract toàn bộ nội dung mô tả, không cắt ngắn
- Detailed info: extract toàn bộ phần "Thông tin chi tiết", không bỏ sót
- Customer reviews: extract TẤT CẢ các đánh giá mới nhất CHỈ nếu có trong nội dung trang, KHÔNG tự tạo reviews giả. Nếu không có reviews trong markdown, để customer_reviews = []
- Specifications: phải có ít nhất 5-10 thông số nếu trang có bảng thông số
- Seller: phải extract tên shop, trạng thái chính hãng nếu có trên trang
- Shipping: phải extract thông tin vận chuyển nếu có
- Category path: extract từ breadcrumb (ví dụ: ["Điện thoại", "Smartphone"])
- Promotions: extract tất cả khuyến mãi thành array

FORMAT OUTPUT:
- Chỉ trả về JSON object thuần túy
- KHÔNG có text giải thích như "Dưới đây là...", "JSON object:", v.v.
- KHÔNG có markdown code blocks nếu không cần thiết
- Bắt đầu trực tiếp bằng {{ và kết thúc bằng }}

Nếu thông tin thực sự không có trên trang, mới để null."""


def scrape_with_firecrawl_v2(product_url: str, timeout: int = 60) -> Optional[Dict[str, Any]]:
    """
    Scrape product page sử dụng Firecrawl v2/scrape để lấy cả markdown và HTML
    
    Args:
        product_url: URL của product page
        timeout: Timeout cho request (seconds)
    
    Returns:
        Dict với 'markdown' và 'html' hoặc None nếu lỗi
    """
    try:
        # Thử v0/scrape trước (đã test hoạt động)
        scrape_url = f"{FIRECRAWL_API_URL}/v2/scrape"
        
        payload = {
            "url": product_url,
            "formats": ["html", "markdown"],
            "onlyMainContent": False,  # Lấy toàn bộ content để có reviews
            "maxAge": 172800000,  # 2 days
            "waitFor": 8000,  # Tăng wait time để load all dynamic content (reviews, AI summary)
            "timeout": timeout * 1000  # Convert to milliseconds for Firecrawl
        }
        
        response = requests.post(scrape_url, json=payload, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle v0/scrape response format
        result = {}
        
        # Extract markdown và HTML từ data.data
        if data.get("data"):
            content_data = data.get("data", {})
            if content_data.get("markdown"):
                result["markdown"] = content_data.get("markdown")
            if content_data.get("html"):
                result["html"] = content_data.get("html")
        
        # Fallback: check root level
        if not result.get("markdown") and data.get("markdown"):
            result["markdown"] = data.get("markdown")
        if not result.get("html") and data.get("html"):
            result["html"] = data.get("html")
        
        if result.get("markdown") or result.get("html"):
            return result
        else:
            try:
                print(f"⚠️  Firecrawl scrape failed: {data.get('error', 'No content in response')}")
            except:
                pass
            return None
    except Exception as e:
        try:
            print(f"⚠️  Lỗi scrape Firecrawl: {str(e)[:100]}")
        except:
            pass
        return None


def extract_ai_summary_from_html(html_content: str) -> Optional[Dict[str, Any]]:
    """
    Extract AI summary từ HTML bằng cách tìm div#ai-summary
    
    Args:
        html_content: HTML content từ Firecrawl
    
    Returns:
        Dict chứa ai_summary structure hoặc None nếu không tìm thấy
    """
    if not html_content:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Tìm div với id="ai-summary"
        ai_summary_div = soup.find('div', id='ai-summary')
        if not ai_summary_div:
            # Thử tìm với class chứa "ai-summary" hoặc "aiSummary"
            ai_summary_div = soup.find('div', class_=re.compile(r'ai[-_]?summary', re.I))
        
        if not ai_summary_div:
            return None
        
        # Extract text content
        text_content = ai_summary_div.get_text(separator='\n', strip=True)
        
        # Parse structure từ text
        # Cấu trúc thường có:
        # - product_summary: các điểm tích cực/tiêu cực về sản phẩm
        # - service_summary: các điểm tích cực/tiêu cực về dịch vụ
        # - positive_count: số lượng positive (product: X, service: Y)
        # - negative_count: số lượng negative (product: X, service: Y)
        
        result = {
            "product_summary": [],
            "service_summary": [],
            "positive_count": {},
            "negative_count": {}
        }
        
        # Tìm các phần tử con có thể chứa summary
        # Thử tìm các list items, paragraphs, hoặc divs chứa thông tin
        summary_items = ai_summary_div.find_all(['li', 'p', 'div'], class_=re.compile(r'summary|review|point|item', re.I))
        
        product_items = []
        service_items = []
        
        for item in summary_items:
            item_text = item.get_text(strip=True)
            if not item_text:
                continue
            
            # Phân loại dựa trên từ khóa
            text_lower = item_text.lower()
            if any(keyword in text_lower for keyword in ['sản phẩm', 'product', 'máy', 'điện thoại', 'thiết bị']):
                product_items.append(item_text)
            elif any(keyword in text_lower for keyword in ['dịch vụ', 'service', 'giao hàng', 'vận chuyển', 'đóng gói']):
                service_items.append(item_text)
            else:
                # Mặc định thêm vào product_summary
                product_items.append(item_text)
        
        result["product_summary"] = product_items[:20]  # Giới hạn 20 items
        result["service_summary"] = service_items[:20]
        
        # Tìm số lượng positive/negative
        # Thường có format: "product: 88", "service: 85", hoặc "88 positive", "1 negative"
        count_patterns = [
            r'product[:\s]+(\d+)',
            r'service[:\s]+(\d+)',
            r'(\d+)\s*positive',
            r'(\d+)\s*negative',
            r'tích cực[:\s]+(\d+)',
            r'tiêu cực[:\s]+(\d+)',
        ]
        
        full_text = text_content.lower()
        for pattern in count_patterns:
            matches = re.findall(pattern, full_text, re.I)
            if matches:
                # Nếu tìm thấy số, thử phân loại
                for match in matches:
                    num = int(match) if match.isdigit() else 0
                    if 'product' in pattern or 'sản phẩm' in pattern:
                        result["positive_count"]["product"] = num
                    elif 'service' in pattern or 'dịch vụ' in pattern:
                        result["positive_count"]["service"] = num
                    elif 'positive' in pattern or 'tích cực' in pattern:
                        if "product" not in result["positive_count"]:
                            result["positive_count"]["product"] = num
                        elif "service" not in result["positive_count"]:
                            result["positive_count"]["service"] = num
                    elif 'negative' in pattern or 'tiêu cực' in pattern:
                        if "product" not in result["negative_count"]:
                            result["negative_count"]["product"] = num
                        elif "service" not in result["negative_count"]:
                            result["negative_count"]["service"] = num
        
        # Nếu không tìm thấy items cụ thể, lấy toàn bộ text và split thành các câu
        if not result["product_summary"] and not result["service_summary"]:
            sentences = [s.strip() for s in text_content.split('\n') if s.strip() and len(s.strip()) > 10]
            if sentences:
                # Phân loại câu thành product hoặc service
                for sentence in sentences[:30]:  # Giới hạn 30 câu
                    sentence_lower = sentence.lower()
                    if any(keyword in sentence_lower for keyword in ['dịch vụ', 'service', 'giao hàng', 'vận chuyển']):
                        result["service_summary"].append(sentence)
                    else:
                        result["product_summary"].append(sentence)
        
        # Chỉ trả về nếu có ít nhất một phần tử
        if result["product_summary"] or result["service_summary"] or result["positive_count"] or result["negative_count"]:
            return result
        
        return None
        
    except Exception as e:
        try:
            print(f"⚠️  Lỗi parse AI summary từ HTML: {str(e)[:100]}")
        except:
            pass
        return None


def extract_detailed_info_from_html(html_content: str) -> Optional[str]:
    """
    Extract phần "Thông tin chi tiết" từ HTML
    
    Args:
        html_content: HTML content từ Firecrawl
    
    Returns:
        String chứa thông tin chi tiết hoặc None nếu không tìm thấy
    """
    if not html_content:
        return None
    
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        
        # Tìm phần "Thông tin chi tiết" bằng nhiều cách:
        # 1. Tìm heading/tiêu đề chứa "Thông tin chi tiết"
        # 2. Tìm section/div có class/id liên quan
        # 3. Tìm theo text content
        
        detailed_info = None
        
        # Cách 1: Tìm heading (h1, h2, h3, h4) chứa "Thông tin chi tiết"
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for heading in headings:
            heading_text = heading.get_text(strip=True)
            if 'thông tin chi tiết' in heading_text.lower() or 'chi tiết sản phẩm' in heading_text.lower():
                # Lấy phần tử tiếp theo hoặc parent chứa nội dung
                parent = heading.find_next_sibling()
                if not parent:
                    parent = heading.parent
                
                if parent:
                    # Lấy tất cả text từ phần tử này
                    detailed_info = parent.get_text(separator='\n', strip=True)
                    # Loại bỏ heading text khỏi kết quả
                    if heading_text in detailed_info:
                        detailed_info = detailed_info.replace(heading_text, '', 1).strip()
                    break
        
        # Cách 2: Tìm div/section có class/id chứa "detail", "info", "specification"
        if not detailed_info:
            detail_selectors = [
                'div[class*="detail"]',
                'div[class*="info"]',
                'div[id*="detail"]',
                'div[id*="info"]',
                'section[class*="detail"]',
                'section[class*="info"]',
            ]
            
            for selector in detail_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        elem_text = elem.get_text(strip=True)
                        # Kiểm tra xem có chứa từ khóa "thông tin chi tiết" không
                        if 'thông tin chi tiết' in elem_text.lower() or 'chi tiết' in elem_text.lower():
                            if len(elem_text) > 100:  # Đảm bảo có đủ nội dung
                                detailed_info = elem_text
                                break
                    if detailed_info:
                        break
                except:
                    continue
        
        # Cách 3: Tìm theo text pattern
        if not detailed_info:
            # Tìm tất cả text và tìm phần sau "Thông tin chi tiết"
            all_text = soup.get_text()
            pattern = r'(?:thông tin chi tiết|chi tiết sản phẩm)[:\s]*(.+?)(?:\n\n|\n[A-Z]|$)'
            match = re.search(pattern, all_text, re.IGNORECASE | re.DOTALL)
            if match:
                detailed_info = match.group(1).strip()
                # Giới hạn độ dài
                if len(detailed_info) > 10000:
                    detailed_info = detailed_info[:10000]
        
        # Cách 4: Tìm trong các div có class đặc biệt của Tiki
        if not detailed_info:
            # Tiki thường dùng các class như "product-detail", "product-info", "specification"
            tiki_selectors = [
                'div.product-detail',
                'div.product-info',
                'div.product-specification',
                'div[data-testid*="detail"]',
                'div[data-testid*="info"]',
            ]
            
            for selector in tiki_selectors:
                try:
                    elements = soup.select(selector)
                    for elem in elements:
                        elem_text = elem.get_text(separator='\n', strip=True)
                        if len(elem_text) > 200:  # Đảm bảo có đủ nội dung
                            # Kiểm tra xem có phải là phần thông tin chi tiết không (không phải spec)
                            if 'thông tin chi tiết' in elem_text.lower() or \
                               ('mô tả' in elem_text.lower() and 'thông số' not in elem_text.lower()):
                                detailed_info = elem_text
                                break
                    if detailed_info:
                        break
                except:
                    continue
        
        # Clean up text
        if detailed_info:
            # Loại bỏ các dòng trống nhiều
            lines = [line.strip() for line in detailed_info.split('\n') if line.strip()]
            detailed_info = '\n'.join(lines)
            
            # Loại bỏ các ký tự đặc biệt không cần thiết
            detailed_info = re.sub(r'\s+', ' ', detailed_info)  # Nhiều space thành 1
            detailed_info = re.sub(r'\n{3,}', '\n\n', detailed_info)  # Nhiều newline thành 2
            
            if len(detailed_info) > 50:  # Chỉ trả về nếu có đủ nội dung
                return detailed_info
        
        return None
        
    except Exception as e:
        try:
            print(f"⚠️  Lỗi parse detailed info từ HTML: {str(e)[:100]}")
        except:
            pass
        return None


def extract_with_groq_ai(
    markdown_content: str = None,
    html_content: str = None,
    product_id: str = None,
    product_name: str = None
) -> Optional[Dict[str, Any]]:
    """
    Extract structured data từ markdown và/hoặc HTML bằng Groq AI
    
    Args:
        markdown_content: Markdown content từ Firecrawl (optional)
        html_content: HTML content từ Firecrawl (optional)
        product_id: Product ID
        product_name: Product name (optional)
    
    Returns:
        Dict chứa product details theo schema, hoặc None nếu lỗi
    """
    if not GROQ_ENABLED or not get_groq_api_key:
        return None
    
    try:
        # Lấy Groq API key (round-robin)
        groq_api_key = get_groq_api_key()
        if not groq_api_key:
            try:
                print("⚠️  Không có Groq API key khả dụng")
            except:
                pass
            return None
        
        # Tạo prompt
        prompt = create_extraction_prompt(f"(Product ID: {product_id})", product_id, product_name)
        system_prompt = create_system_prompt()
        
        # Gọi Groq API với structured output (JSON mode)
        extract_url = f"{GROQ_API_BASE}/chat/completions"
        
        # Combine markdown and HTML content
        content_parts = []
        
        if markdown_content:
            # Limit markdown content để tránh token limit
            markdown_limited = markdown_content[:15000] if len(markdown_content) > 15000 else markdown_content
            content_parts.append(f"=== MARKDOWN CONTENT ===\n{markdown_limited}")
            try:
                print(f"   📊 Using {len(markdown_limited)}/{len(markdown_content)} chars of markdown")
            except:
                pass
        
        if html_content:
            # Limit HTML content (lấy text từ HTML, không lấy toàn bộ HTML)
            # Có thể parse HTML để lấy text quan trọng
            html_limited = html_content[:10000] if len(html_content) > 10000 else html_content
            content_parts.append(f"=== HTML CONTENT (TEXT EXTRACTED) ===\n{html_limited}")
            try:
                print(f"   📊 Using {len(html_limited)}/{len(html_content)} chars of HTML")
            except:
                pass
        
        if not content_parts:
            try:
                print("⚠️  Không có content để extract")
            except:
                pass
            return None
        
        combined_content = "\n\n".join(content_parts)
        
        # Tạo user message với schema instruction (rút gọn)
        schema_summary = """{
  "product_id": "string",
  "name": "string",
  "price": {"current_price": number, "original_price": number, "discount_percent": number, "currency": "VND"},
  "description": "string",
  "specifications": {"key": "value"},
  "detailed_info": "string",
  "customer_reviews": [
    {
      "reviewer_name": "string",
      "rating": number,
      "review_text": "string",
      "review_date": "string",
      "verified_purchase": boolean
    }
  ],
  "rating": {"average": number, "total_reviews": number},
  "seller": {"name": "string", "is_official": boolean},
  "shipping": {"free_shipping": boolean, "fast_delivery": boolean, "delivery_time": "string"},
  "stock": {"available": boolean, "quantity": number, "stock_status": "string"},
  "category_path": ["string"],
  "brand": "string",
  "warranty": "string",
  "promotions": ["string"],
  "ai_summary": {
    "product_summary": ["string"],
    "service_summary": ["string"],
    "positive_count": {"product": number, "service": number},
    "negative_count": {"product": number, "service": number}
  }
}"""
        
        user_content = f"""{prompt}

{combined_content}

=== INSTRUCTIONS ===
Trả về JSON object đúng theo format sau (không có markdown code blocks, chỉ JSON thuần):
{schema_summary}

QUAN TRỌNG:
- CHỈ extract thông tin có THẬT trong nội dung trên, KHÔNG tự tạo/suy đoán
- Nếu không thấy reviews trong nội dung, để customer_reviews = []
- Nếu không thấy detailed_info riêng, để detailed_info = ""
- Giữ nguyên thông tin từ trang, không thay đổi"""
        
        payload = {
            "model": GROQ_MODEL,
            "messages": [
                {
                    "role": "system",
                    "content": f"{system_prompt}\n\nBạn PHẢI trả về JSON object hợp lệ, không có markdown code blocks, không có text thêm."
                },
                {
                    "role": "user",
                    "content": user_content
                }
            ],
            "temperature": 0.1
        }
        
        # Chỉ thêm response_format nếu model support (một số model không support)
        # Thử không dùng response_format trước, nếu cần sẽ parse từ text
        
        headers = {
            "Authorization": f"Bearer {groq_api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(extract_url, json=payload, headers=headers, timeout=60)
        
        # Handle model decommissioned error - fallback to newer model
        if response.status_code == 400:
            try:
                error_data = response.json()
                error_msg = error_data.get("error", {}).get("message", "")
                if "decommissioned" in error_msg.lower() or "model_decommissioned" in str(error_data):
                    # Try với model mới hơn
                    fallback_models = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768"]
                    for fallback_model in fallback_models:
                        if fallback_model != GROQ_MODEL:
                            try:
                                print(f"   🔄 Retrying với model: {fallback_model}")
                                payload["model"] = fallback_model
                                response = requests.post(extract_url, json=payload, headers=headers, timeout=60)
                                if response.status_code == 200:
                                    # Success với fallback model
                                    break
                                else:
                                    # Log error để debug
                                    try:
                                        error_data = response.json()
                                        print(f"   ⚠️  Fallback model {fallback_model} failed: {error_data.get('error', {}).get('message', 'Unknown')[:100]}")
                                    except:
                                        pass
                            except:
                                continue
            except:
                pass
        
        # Debug: check error response
        if response.status_code != 200:
            try:
                error_data = response.json()
                print(f"⚠️  Groq API error {response.status_code}: {error_data}")
            except:
                print(f"⚠️  Groq API error {response.status_code}: {response.text[:200]}")
            response.raise_for_status()
        
        data = response.json()
        
        # Debug: log response để hiểu structure
        try:
            print(f"   📊 Response keys: {list(data.keys())}")
            if "choices" in data:
                print(f"   📊 Choices count: {len(data['choices'])}")
        except:
            pass
        
        # Debug: log response structure
        if not data.get("choices"):
            try:
                print(f"⚠️  Response không có choices. Keys: {list(data.keys())}")
                print(f"   Full response: {json.dumps(data, indent=2)[:500]}")
            except:
                pass
            return None
        
        # Parse JSON từ response
        if data.get("choices") and len(data["choices"]) > 0:
            choice = data["choices"][0]
            
            # Debug: log choice structure
            try:
                print(f"   📊 Choice keys: {list(choice.keys())}")
                if "finish_reason" in choice:
                    print(f"   📊 Finish reason: {choice['finish_reason']}")
            except:
                pass
            
            if "message" not in choice:
                try:
                    print(f"⚠️  Choice không có message: {choice.keys()}")
                    print(f"   Full choice: {json.dumps(choice, indent=2)[:500]}")
                except:
                    pass
                return None
            
            content = choice["message"].get("content", "").strip()
            
            # Debug: log content
            if not content:
                try:
                    print(f"⚠️  Content trống. Message keys: {list(choice['message'].keys())}")
                    print(f"   Full message: {json.dumps(choice['message'], indent=2)[:500]}")
                except:
                    pass
                return None
            
            # Debug: log content length và preview
            try:
                print(f"   📊 Content length: {len(content)} chars")
                print(f"   📊 Content preview (first 200): {content[:200]}")
            except:
                pass
            
            # Remove markdown code blocks và text trước JSON
            content_stripped = content.strip()
            
            # Xử lý markdown code blocks
            if content_stripped.startswith("```"):
                # Extract JSON từ code block
                lines = content.split("\n")
                json_start = None
                json_end = None
                
                for i, line in enumerate(lines):
                    line_stripped = line.strip()
                    # Tìm dòng bắt đầu code block (```json hoặc ```)
                    if line_stripped.startswith("```json") or (line_stripped.startswith("```") and json_start is None):
                        json_start = i + 1
                    # Tìm dòng kết thúc code block
                    elif line_stripped == "```" and json_start is not None:
                        json_end = i
                        break
                
                if json_start is not None:
                    if json_end is not None:
                        content = "\n".join(lines[json_start:json_end]).strip()
                    else:
                        # Không có closing ```, lấy từ json_start đến cuối
                        content = "\n".join(lines[json_start:]).strip()
            
            # Xử lý text trước JSON (như "Dưới đây là JSON object...")
            # Tìm vị trí bắt đầu của JSON object {
            lines = content.split("\n")
            json_start_idx = None
            for i, line in enumerate(lines):
                line_stripped = line.strip()
                # Tìm dòng bắt đầu bằng { (JSON object)
                if line_stripped.startswith("{"):
                    json_start_idx = i
                    break
            
            if json_start_idx is not None and json_start_idx > 0:
                # Có text trước JSON, chỉ lấy phần JSON
                content = "\n".join(lines[json_start_idx:]).strip()
                try:
                    print(f"   📊 Removed {json_start_idx} lines of text before JSON")
                except:
                    pass
            
            # Tìm vị trí kết thúc của JSON object }
            # Đếm số { và } để tìm JSON object hoàn chỉnh
            brace_count = 0
            json_end_idx = None
            lines = content.split("\n")
            for i, line in enumerate(lines):
                brace_count += line.count("{") - line.count("}")
                if brace_count == 0 and i > 0:
                    json_end_idx = i + 1
                    break
            
            if json_end_idx is not None and json_end_idx < len(lines):
                # Có text sau JSON, chỉ lấy phần JSON
                content = "\n".join(lines[:json_end_idx]).strip()
                try:
                    print(f"   📊 Removed text after JSON")
                except:
                    pass
            
            # Debug
            try:
                print(f"   📊 After cleaning: {len(content)} chars")
            except:
                pass
            
            try:
                extracted = json.loads(content)
                return extracted
            except json.JSONDecodeError as e:
                try:
                    print(f"⚠️  JSON parse error: {str(e)[:100]}")
                    print(f"   Content preview: {content[:200]}")
                except:
                    pass
                return None
        else:
            try:
                print(f"⚠️  Groq response không có choices")
            except:
                pass
            return None
            
    except Exception as e:
        try:
            print(f"⚠️  Lỗi extract Groq AI: {str(e)[:100]}")
        except:
            pass
        return None


def extract_product_details_from_api(
    product_id: str,
    timeout: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Extract product details từ Tiki API
    
    Args:
        product_id: Product ID từ Tiki
        timeout: Timeout cho request (seconds)
    
    Returns:
        Dict chứa product details theo schema, hoặc None nếu lỗi
    
    Reference: https://open.tiki.vn/docs/docs/current/api-references/product-api/#get-a-product-v2-1
    """
    if not TIKI_API_TOKEN:
        try:
            print("⚠️  TIKI_API_TOKEN chưa được cấu hình!")
            print("   Hãy set TIKI_API_TOKEN trong .env file")
            print("   Ví dụ: TIKI_API_TOKEN=your_token_here")
        except:
            pass
        return None
    
    if not product_id:
        try:
            print("⚠️  Product ID không được cung cấp")
        except:
            pass
        return None
    
    try:
        # Gọi Tiki API để lấy product details
        api_url = f"{TIKI_API_BASE_URL}/products/{product_id}"
        
        headers = {
            "tiki-api": TIKI_API_TOKEN,
            "Content-Type": "application/json"
        }
        
        try:
            print(f"   📡 Gọi Tiki API: {api_url}")
        except:
            pass
        
        response = requests.get(api_url, headers=headers, timeout=timeout)
        
        if response.status_code == 404:
            try:
                print(f"   ⚠️  Product không tồn tại (404)")
            except:
                pass
            return None
        
        if response.status_code == 401:
            try:
                print(f"   ⚠️  Unauthorized - Token không hợp lệ (401)")
            except:
                pass
            return None
        
        if response.status_code == 429:
            try:
                print(f"   ⚠️  Rate limit exceeded (429)")
            except:
                pass
            return None
        
        response.raise_for_status()
        api_data = response.json()
        
        try:
            print(f"   ✓ Nhận được dữ liệu từ API")
        except:
            pass
        
        # Transform API response thành schema của chúng ta
        # API response structure có thể khác, cần map lại
        details = {
            "product_id": str(product_id),
            "name": api_data.get("name", ""),
            "price": {
                "current_price": api_data.get("price", 0),
                "original_price": api_data.get("original_price", api_data.get("price", 0)),
                "discount_percent": 0,
                "currency": "VND"
            },
            "description": api_data.get("description", ""),
            "specifications": {},
            "detailed_info": api_data.get("description", ""),  # Có thể lấy từ description
            "customer_reviews": [],
            "rating": {
                "average": api_data.get("rating_average", 0),
                "total_reviews": api_data.get("review_count", 0)
            },
            "seller": {
                "name": api_data.get("seller_name", ""),
                "is_official": api_data.get("is_official", False)
            },
            "shipping": {
                "free_shipping": api_data.get("free_shipping", False),
                "fast_delivery": api_data.get("fast_delivery", False),
                "delivery_time": ""
            },
            "stock": {
                "available": api_data.get("inventory_status", "available") == "available",
                "quantity": api_data.get("inventory_quantity", 0),
                "stock_status": api_data.get("inventory_status", "")
            },
            "category_path": [],
            "brand": api_data.get("brand", ""),
            "warranty": api_data.get("warranty", ""),
            "promotions": [],
            "ai_summary": {
                "product_summary": [],
                "service_summary": [],
                "positive_count": {},
                "negative_count": {}
            }
        }
        
        # Extract specifications từ attributes nếu có
        if "attributes" in api_data and isinstance(api_data["attributes"], list):
            for attr in api_data["attributes"]:
                if isinstance(attr, dict) and "name" in attr and "value" in attr:
                    details["specifications"][attr["name"]] = str(attr["value"])
        
        # Extract category path nếu có
        if "category" in api_data:
            category = api_data["category"]
            if isinstance(category, dict) and "path" in category:
                # Category path có thể là string hoặc array
                path = category["path"]
                if isinstance(path, str):
                    details["category_path"] = [p.strip() for p in path.split(">") if p.strip()]
                elif isinstance(path, list):
                    details["category_path"] = [str(p) for p in path]
        
        # Calculate discount percent
        if details["price"]["original_price"] and details["price"]["current_price"]:
            if details["price"]["original_price"] > details["price"]["current_price"]:
                discount = ((details["price"]["original_price"] - details["price"]["current_price"]) / details["price"]["original_price"]) * 100
                details["price"]["discount_percent"] = round(discount, 1)
        
        # Validate và enrich
        details = validate_and_enrich_product_details(details, product_id, f"https://tiki.vn/p{product_id}")
        
        return details
        
    except requests.exceptions.RequestException as e:
        try:
            print(f"⚠️  Lỗi khi gọi Tiki API: {str(e)[:100]}")
        except:
            pass
        return None
    except Exception as e:
        try:
            print(f"⚠️  Lỗi xử lý dữ liệu từ API: {str(e)[:100]}")
        except:
            pass
        return None


def extract_product_details_from_api_endpoints(
    product_url: str,
    product_id: str = None,
    timeout: int = 30
) -> Optional[Dict[str, Any]]:
    """
    Extract product details bằng cách tự động phát hiện và gọi các API endpoints của Tiki
    
    Các API endpoints được thử:
    - https://tiki.vn/api/v2/products/{product_id}
    - https://tiki.vn/api/reviews?product_id={product_id}
    - https://tiki.vn/api/nps/summary/{product_id} (AI summary)
    - https://tiki.vn/api/pdp/quickview (với product_id)
    
    Args:
        product_url: URL của product page
        product_id: Product ID (optional, sẽ extract từ URL nếu không có)
        timeout: Timeout cho mỗi request (seconds)
    
    Returns:
        Dict chứa product details theo schema, hoặc None nếu lỗi
    """
    if not product_id:
        product_id = extract_product_id(product_url)
        if not product_id:
            try:
                print(f"⚠️  Không thể extract product_id từ URL: {product_url}")
            except:
                pass
            return None
    
    # Headers giống browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": product_url,
        "Origin": "https://tiki.vn"
    }
    
    result = {
        "product_id": str(product_id),
        "name": "",
        "brand": "",
        "categories": [],
        "price": {
            "current": 0,
            "original": 0,
            "discount_percent": 0
        },
        "stock_status": "",
        "seller": {
            "name": "",
            "is_official_store": False
        },
        "shipping": {
            "methods": [],
            "delivery_time_estimate": ""
        },
        "promotions": [],
        "specifications_table": [],
        "ai_review_summary": {
            "product_positive": [],
            "product_negative": [],
            "service_positive": [],
            "service_negative": [],
            "counts": {
                "product_positive": 0,
                "product_negative": 0,
                "service_positive": 0,
                "service_negative": 0
            }
        }
    }
    
    # API endpoints để thử
    api_endpoints = [
        {
            "name": "product_details",
            "url": f"https://tiki.vn/api/v2/products/{product_id}",
            "method": "GET"
        },
        {
            "name": "quickview",
            "url": f"https://tiki.vn/api/pdp/quickview",
            "method": "POST",
            "data": {"product_id": product_id}
        },
        {
            "name": "reviews",
            "url": f"https://tiki.vn/api/reviews",
            "method": "GET",
            "params": {"product_id": product_id, "limit": 50}
        },
        {
            "name": "ai_summary",
            "url": f"https://tiki.vn/api/nps/summary/{product_id}",
            "method": "GET"
        }
    ]
    
    api_responses = {}
    
    # Gọi tất cả các API endpoints
    for endpoint in api_endpoints:
        try:
            if endpoint["method"] == "GET":
                if "params" in endpoint:
                    response = requests.get(
                        endpoint["url"],
                        headers=headers,
                        params=endpoint["params"],
                        timeout=timeout
                    )
                else:
                    response = requests.get(
                        endpoint["url"],
                        headers=headers,
                        timeout=timeout
                    )
            elif endpoint["method"] == "POST":
                response = requests.post(
                    endpoint["url"],
                    headers={**headers, "Content-Type": "application/json"},
                    json=endpoint.get("data", {}),
                    timeout=timeout
                )
            else:
                continue
            
            if response.status_code == 200:
                try:
                    api_responses[endpoint["name"]] = response.json()
                    try:
                        print(f"   ✓ {endpoint['name']}: OK")
                    except:
                        pass
                except json.JSONDecodeError:
                    try:
                        print(f"   ⚠️  {endpoint['name']}: Invalid JSON")
                    except:
                        pass
            else:
                try:
                    print(f"   ⚠️  {endpoint['name']}: {response.status_code}")
                except:
                    pass
        except requests.exceptions.RequestException as e:
            try:
                print(f"   ✗ {endpoint['name']}: {str(e)[:50]}")
            except:
                pass
        except Exception as e:
            try:
                print(f"   ✗ {endpoint['name']}: {str(e)[:50]}")
            except:
                pass
    
    # Parse product_details hoặc quickview
    product_data = api_responses.get("product_details") or api_responses.get("quickview") or {}
    
    if not product_data:
        try:
            print("⚠️  Không lấy được dữ liệu product từ API")
        except:
            pass
        return None
    
    # Extract basic info
    result["name"] = product_data.get("name") or product_data.get("title", "")
    result["brand"] = product_data.get("brand") or product_data.get("brand_name", "")
    
    # Extract price
    if "price" in product_data:
        result["price"]["current"] = int(product_data["price"])
    if "original_price" in product_data:
        result["price"]["original"] = int(product_data["original_price"])
    elif "list_price" in product_data:
        result["price"]["original"] = int(product_data["list_price"])
    
    if result["price"]["original"] and result["price"]["current"]:
        if result["price"]["original"] > result["price"]["current"]:
            discount = ((result["price"]["original"] - result["price"]["current"]) / result["price"]["original"]) * 100
            result["price"]["discount_percent"] = round(discount, 1)
    
    # Extract stock
    result["stock_status"] = product_data.get("inventory_status", product_data.get("stock_status", ""))
    
    # Extract seller
    if "seller" in product_data:
        seller_data = product_data["seller"]
        if isinstance(seller_data, dict):
            result["seller"]["name"] = seller_data.get("name", "")
            result["seller"]["is_official_store"] = seller_data.get("is_official", False)
        elif isinstance(seller_data, str):
            result["seller"]["name"] = seller_data
    
    # Extract categories
    if "categories" in product_data:
        categories = product_data["categories"]
        if isinstance(categories, list):
            result["categories"] = [str(c) for c in categories]
        elif isinstance(categories, dict) and "path" in categories:
            path = categories["path"]
            if isinstance(path, str):
                result["categories"] = [p.strip() for p in path.split(">") if p.strip()]
            elif isinstance(path, list):
                result["categories"] = [str(p) for p in path]
    
    # Extract specifications
    if "specifications" in product_data:
        specs = product_data["specifications"]
        if isinstance(specs, list):
            for spec in specs:
                if isinstance(spec, dict):
                    label = spec.get("name") or spec.get("label", "")
                    value = spec.get("value") or spec.get("text", "")
                    if label and value:
                        result["specifications_table"].append({
                            "label": str(label),
                            "value": str(value)
                        })
        elif isinstance(specs, dict):
            for key, value in specs.items():
                result["specifications_table"].append({
                    "label": str(key),
                    "value": str(value)
                })
    
    # Extract promotions
    if "promotions" in product_data:
        promotions = product_data["promotions"]
        if isinstance(promotions, list):
            result["promotions"] = [str(p) for p in promotions]
    
    # Extract shipping
    if "shipping" in product_data:
        shipping_data = product_data["shipping"]
        if isinstance(shipping_data, dict):
            if "methods" in shipping_data:
                result["shipping"]["methods"] = shipping_data["methods"]
            if "delivery_time" in shipping_data:
                result["shipping"]["delivery_time_estimate"] = shipping_data["delivery_time"]
    
    # Parse AI summary từ nps/summary endpoint
    ai_summary_data = api_responses.get("ai_summary", {})
    if ai_summary_data:
        # Parse structure từ AI summary API
        if "product" in ai_summary_data:
            product_summary = ai_summary_data["product"]
            if isinstance(product_summary, dict):
                if "positive" in product_summary:
                    positives = product_summary["positive"]
                    if isinstance(positives, list):
                        result["ai_review_summary"]["product_positive"] = [str(p) for p in positives]
                if "negative" in product_summary:
                    negatives = product_summary["negative"]
                    if isinstance(negatives, list):
                        result["ai_review_summary"]["product_negative"] = [str(n) for n in negatives]
                if "positive_count" in product_summary:
                    result["ai_review_summary"]["counts"]["product_positive"] = int(product_summary["positive_count"])
                if "negative_count" in product_summary:
                    result["ai_review_summary"]["counts"]["product_negative"] = int(product_summary["negative_count"])
        
        if "service" in ai_summary_data:
            service_summary = ai_summary_data["service"]
            if isinstance(service_summary, dict):
                if "positive" in service_summary:
                    positives = service_summary["positive"]
                    if isinstance(positives, list):
                        result["ai_review_summary"]["service_positive"] = [str(p) for p in positives]
                if "negative" in service_summary:
                    negatives = service_summary["negative"]
                    if isinstance(negatives, list):
                        result["ai_review_summary"]["service_negative"] = [str(n) for n in negatives]
                if "positive_count" in service_summary:
                    result["ai_review_summary"]["counts"]["service_positive"] = int(service_summary["positive_count"])
                if "negative_count" in service_summary:
                    result["ai_review_summary"]["counts"]["service_negative"] = int(service_summary["negative_count"])
    
    # Parse reviews nếu có
    reviews_data = api_responses.get("reviews", {})
    if reviews_data and "data" in reviews_data:
        # Reviews có thể được lưu vào result nếu cần
        pass
    
    return result


def extract_product_details_ai(
    product_url: str,
    product_id: str = None,
    product_name: str = None,
    timeout: int = 120
) -> Optional[Dict[str, Any]]:
    """
    Extract product details sử dụng AI (Firecrawl v2/scrape + Groq)
    
    Args:
        product_url: URL của product page
        product_id: Product ID (optional, sẽ extract từ URL nếu không có)
        product_name: Tên sản phẩm (optional)
        timeout: Timeout cho request (seconds)
    
    Returns:
        Dict chứa product details theo schema, hoặc None nếu lỗi
    """
    # Check if Groq is enabled
    if not GROQ_ENABLED:
        try:
            print("⚠️  Groq API chưa được cấu hình!")
            print("   Hãy set GROQ_API_KEY hoặc GROQ_API_KEYS trong .env file")
            print("   Ví dụ: GROQ_API_KEY=gsk_your_key_here")
        except:
            pass
        return None
    
    # Extract product_id từ URL nếu chưa có
    if not product_id:
        product_id = extract_product_id(product_url)
        if not product_id:
            try:
                print(f"⚠️  Không thể extract product_id từ URL: {product_url}")
            except:
                pass
            return None
    
    # Step 1: Scrape với Firecrawl v2 (cả markdown và HTML)
    try:
        print(f"   📥 Scraping với Firecrawl v2 (markdown + HTML)...")
    except:
        pass
    
    scrape_result = scrape_with_firecrawl_v2(product_url, timeout=timeout)
    if not scrape_result:
        try:
            print(f"   ✗ Scrape thất bại")
        except:
            pass
        return None
    
    markdown = scrape_result.get("markdown", "")
    html = scrape_result.get("html", "")
    
    try:
        markdown_len = len(markdown) if markdown else 0
        html_len = len(html) if html else 0
        print(f"   ✓ Scraped: {markdown_len} chars markdown, {html_len} chars HTML")
    except:
        pass
    
    # Step 1.5: Parse HTML trực tiếp để extract AI summary và detailed info
    html_parsed_data = {}
    
    if html:
        try:
            print(f"   🔍 Parsing HTML để extract AI summary và thông tin chi tiết...")
        except:
            pass
        
        # Extract AI summary từ HTML
        ai_summary = extract_ai_summary_from_html(html)
        if ai_summary:
            html_parsed_data["ai_summary"] = ai_summary
            try:
                product_count = len(ai_summary.get("product_summary", []))
                service_count = len(ai_summary.get("service_summary", []))
                print(f"   ✓ Extracted AI summary: {product_count} product items, {service_count} service items")
            except:
                pass
        
        # Extract detailed info từ HTML
        detailed_info = extract_detailed_info_from_html(html)
        if detailed_info:
            html_parsed_data["detailed_info"] = detailed_info
            try:
                print(f"   ✓ Extracted detailed info: {len(detailed_info)} chars")
            except:
                pass
    
    # Step 2: Extract với Groq AI (sử dụng cả markdown và HTML)
    try:
        print(f"   🧠 Extracting với Groq AI...")
    except:
        pass
    
    details = extract_with_groq_ai(
        markdown_content=markdown if markdown else None,
        html_content=html if html else None,
        product_id=product_id,
        product_name=product_name
    )
    
    if details:
        try:
            print(f"   ✓ Extract thành công")
        except:
            pass
        
        # Merge dữ liệu từ HTML parsing vào details (ưu tiên HTML parsing nếu có)
        if html_parsed_data:
            # Merge ai_summary
            if "ai_summary" in html_parsed_data:
                # Nếu AI đã extract được một phần, merge lại
                if "ai_summary" in details and details["ai_summary"]:
                    # Merge: ưu tiên HTML parsing, nhưng giữ lại dữ liệu từ AI nếu HTML không có
                    html_ai = html_parsed_data["ai_summary"]
                    ai_ai = details["ai_summary"]
                    
                    merged_ai_summary = {
                        "product_summary": html_ai.get("product_summary") or ai_ai.get("product_summary", []),
                        "service_summary": html_ai.get("service_summary") or ai_ai.get("service_summary", []),
                        "positive_count": {**ai_ai.get("positive_count", {}), **html_ai.get("positive_count", {})},
                        "negative_count": {**ai_ai.get("negative_count", {}), **html_ai.get("negative_count", {})}
                    }
                    details["ai_summary"] = merged_ai_summary
                else:
                    # Chỉ có HTML parsing
                    details["ai_summary"] = html_parsed_data["ai_summary"]
            
            # Merge detailed_info (ưu tiên HTML parsing)
            if "detailed_info" in html_parsed_data and html_parsed_data["detailed_info"]:
                # Chỉ ghi đè nếu HTML parsing có dữ liệu và AI không có hoặc có ít hơn
                current_detailed_info = details.get("detailed_info", "")
                if not current_detailed_info or len(current_detailed_info) < len(html_parsed_data["detailed_info"]):
                    details["detailed_info"] = html_parsed_data["detailed_info"]
        
        # Validate và enrich data
        details = validate_and_enrich_product_details(details, product_id, product_url)
    else:
        try:
            print(f"   ✗ Extract thất bại")
        except:
            pass
        # Nếu AI extraction thất bại nhưng có dữ liệu từ HTML parsing, vẫn trả về
        if html_parsed_data:
            try:
                print(f"   ⚠️  Sử dụng dữ liệu từ HTML parsing")
            except:
                pass
            # Tạo structure cơ bản
            details = {
                "product_id": product_id,
                "name": product_name or "",
                "price": {},
                "detailed_info": html_parsed_data.get("detailed_info", ""),
                "ai_summary": html_parsed_data.get("ai_summary", {
                    "product_summary": [],
                    "service_summary": [],
                    "positive_count": {},
                    "negative_count": {}
                })
            }
            details = validate_and_enrich_product_details(details, product_id, product_url)
    
    return details


def validate_and_enrich_product_details(
    details: Dict[str, Any],
    product_id: str,
    product_url: str
) -> Dict[str, Any]:
    """
    Validate và enrich product details
    
    Args:
        details: Raw extracted details
        product_id: Product ID
        product_url: Product URL
    
    Returns:
        Validated and enriched details
    """
    # Ensure product_id is set
    if not details.get("product_id"):
        details["product_id"] = product_id
    
    # Ensure price structure
    if "price" not in details:
        details["price"] = {}
    
    if "currency" not in details["price"]:
        details["price"]["currency"] = "VND"
    
    # Ensure specifications is object
    if "specifications" not in details:
        details["specifications"] = {}
    elif not isinstance(details["specifications"], dict):
        details["specifications"] = {}
    
    # Ensure detailed_info is string
    if "detailed_info" not in details:
        details["detailed_info"] = ""
    elif not isinstance(details["detailed_info"], str):
        details["detailed_info"] = str(details["detailed_info"])
    
    # Ensure customer_reviews is array
    if "customer_reviews" not in details:
        details["customer_reviews"] = []
    elif not isinstance(details["customer_reviews"], list):
        details["customer_reviews"] = []
    else:
        # Validate each review structure
        validated_reviews = []
        for review in details["customer_reviews"]:
            if isinstance(review, dict):
                validated_review = {
                    "reviewer_name": review.get("reviewer_name", ""),
                    "rating": review.get("rating", 0),
                    "review_text": review.get("review_text", ""),
                    "review_date": review.get("review_date", ""),
                    "verified_purchase": review.get("verified_purchase", False)
                }
                validated_reviews.append(validated_review)
        details["customer_reviews"] = validated_reviews
    
    # Ensure category_path is array
    if "category_path" not in details:
        details["category_path"] = []
    elif not isinstance(details["category_path"], list):
        details["category_path"] = []
    
    # Ensure promotions is array
    if "promotions" not in details:
        details["promotions"] = []
    elif not isinstance(details["promotions"], list):
        details["promotions"] = []
    
    # Ensure ai_summary is object
    if "ai_summary" not in details:
        details["ai_summary"] = {}
    elif not isinstance(details["ai_summary"], dict):
        details["ai_summary"] = {}
    else:
        # Validate ai_summary structure
        ai_summary = details["ai_summary"]
        if not isinstance(ai_summary.get("product_summary"), list):
            ai_summary["product_summary"] = ai_summary.get("product_summary") if ai_summary.get("product_summary") else []
        if not isinstance(ai_summary.get("service_summary"), list):
            ai_summary["service_summary"] = ai_summary.get("service_summary") if ai_summary.get("service_summary") else []
        if not isinstance(ai_summary.get("positive_count"), dict):
            ai_summary["positive_count"] = ai_summary.get("positive_count") if ai_summary.get("positive_count") else {}
        if not isinstance(ai_summary.get("negative_count"), dict):
            ai_summary["negative_count"] = ai_summary.get("negative_count") if ai_summary.get("negative_count") else {}
    
    # Add metadata
    details["_metadata"] = {
        "extracted_at": datetime.now().isoformat(),
        "source_url": product_url,
        "extraction_method": "ai_groq"
    }
    
    return details


def crawl_product_details(
    products: List[Dict[str, Any]],
    max_products: int = None,
    timeout: int = 120,
    delay_between_requests: float = 1.0
) -> List[Dict[str, Any]]:
    """
    Crawl product details từ list products
    
    Args:
        products: List products (có 'url', 'product_id', 'name')
        max_products: Giới hạn số products
        timeout: Timeout cho mỗi request (seconds)
        delay_between_requests: Delay giữa các requests (seconds)
    
    Returns:
        List product details
    """
    import time
    
    if max_products:
        products = products[:max_products]
    
    all_details = []
    total = len(products)
    
    for i, product in enumerate(products, 1):
        product_url = product.get('url', '')
        product_id = product.get('product_id', '')
        product_name = product.get('name', 'N/A')
        
        if not product_url:
            continue
        
        try:
            print(f"[{i}/{total}] 📦 Extracting details: {product_name} (ID: {product_id})")
        except (ValueError, OSError):
            try:
                print(f"[{i}/{total}] Extracting: {product_name} (ID: {product_id})", file=sys.stderr)
            except:
                pass
        
        details = extract_product_details_ai(
            product_url=product_url,
            product_id=product_id,
            product_name=product_name,
            timeout=timeout
        )
        
        if details:
            try:
                print(f"   ✓ Thành công")
            except (ValueError, OSError):
                try:
                    print(f"   Success", file=sys.stderr)
                except:
                    pass
            all_details.append(details)
        else:
            try:
                print(f"   ✗ Thất bại")
            except (ValueError, OSError):
                try:
                    print(f"   Failed", file=sys.stderr)
                except:
                    pass
        
        # Delay giữa các requests
        if i < total and delay_between_requests > 0:
            time.sleep(delay_between_requests)
    
    return all_details


def save_product_details_to_json(
    product_details: List[Dict[str, Any]],
    output_file: str
):
    """
    Lưu product details vào file JSON
    
    Args:
        product_details: List các product details
        output_file: Đường dẫn file output
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    result = {
        "crawl_time": datetime.now().isoformat(),
        "total_products": len(product_details),
        "products": product_details
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    try:
        print(f"💾 Đã lưu {len(product_details)} product details vào: {output_file}")
    except (ValueError, OSError):
        try:
            print(f"Saved {len(product_details)} product details to: {output_file}", file=sys.stderr)
        except:
            pass


def load_product_details_from_json(json_file: str) -> List[Dict[str, Any]]:
    """
    Load product details từ file JSON
    
    Args:
        json_file: Đường dẫn file JSON
    
    Returns:
        List các product details
    """
    if not os.path.exists(json_file):
        return []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict) and 'products' in data:
                return data['products']
            elif isinstance(data, list):
                return data
            return []
    except json.JSONDecodeError as e:
        try:
            print(f"⚠️  Lỗi khi parse JSON: {e}")
        except:
            pass
        return []
    except Exception as e:
        try:
            print(f"⚠️  Lỗi khi load file: {e}")
        except:
            pass
        return []

