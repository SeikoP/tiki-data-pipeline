"""
Extract product details từ Tiki product pages
Sử dụng AI extraction với Firecrawl + Groq
"""
import os
import sys
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

# Fix encoding on Windows
if sys.platform == "win32":
    import io
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

from .config import GROQ_CONFIG, get_config, FIRECRAWL_API_URL
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

# Groq API setup
if GROQ_ENABLED:
    import requests
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
        "images": {
            "type": "array",
            "items": {
                "type": "string",
                "description": "URL hình ảnh sản phẩm"
            },
            "description": "Danh sách URL hình ảnh sản phẩm"
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
            "description": "Thông tin bảo hành (ví dụ: '12 tháng')"
        },
        "promotions": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "description": "Danh sách khuyến mãi (ví dụ: ['Giảm 10%', 'Tặng kèm ốp lưng'])"
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
Extract thông tin chi tiết sản phẩm từ trang Tiki.vn.

Product Information:
- Product ID: {product_id}
- Product Name: {product_name or 'N/A'}
- URL: {product_url}

Hãy extract tất cả thông tin có sẵn trên trang theo schema được cung cấp:

1. **Thông tin cơ bản:**
   - Tên sản phẩm đầy đủ
   - Product ID (từ URL)
   - Brand/Thương hiệu

2. **Giá cả:**
   - Giá hiện tại (sau giảm giá)
   - Giá gốc (nếu có)
   - Phần trăm giảm giá
   - Đơn vị tiền tệ (VND)

3. **Mô tả:**
   - Mô tả sản phẩm chi tiết

4. **Thông số kỹ thuật:**
   - Extract tất cả thông số kỹ thuật dạng key-value
   - Ví dụ: RAM: 8GB, ROM: 128GB, Màn hình: 6.1 inch

5. **Hình ảnh:**
   - Danh sách URL hình ảnh sản phẩm

6. **Đánh giá:**
   - Điểm đánh giá trung bình (0-5)
   - Tổng số đánh giá
   - Phân bố đánh giá (nếu có)

7. **Người bán:**
   - Tên shop/người bán
   - Có phải hàng chính hãng không
   - Seller ID (nếu có)

8. **Vận chuyển:**
   - Có miễn phí ship không
   - Có giao nhanh không
   - Thời gian giao hàng

9. **Tồn kho:**
   - Còn hàng không
   - Số lượng (nếu có)
   - Trạng thái tồn kho

10. **Khác:**
    - Đường dẫn category
    - Thông tin bảo hành
    - Danh sách khuyến mãi

Lưu ý:
- Chỉ extract thông tin có sẵn trên trang, không tự suy đoán
- Nếu thông tin không có, để null
- Giá cả phải là số (không có dấu phẩy, dấu chấm)
- Đảm bảo format đúng theo schema
"""
    return prompt


def create_system_prompt() -> str:
    """Tạo system prompt cho AI extraction"""
    return """Bạn là chuyên gia extract dữ liệu từ trang web Tiki.vn. 

Nhiệm vụ của bạn:
1. Phân tích nội dung trang web Tiki product page
2. Extract thông tin sản phẩm theo schema được cung cấp
3. Đảm bảo dữ liệu chính xác và đầy đủ
4. Chỉ extract thông tin có sẵn trên trang, không tự suy đoán

Quan trọng:
- Giá cả phải là số nguyên (VND), không có dấu phẩy hoặc dấu chấm
- Thông số kỹ thuật extract dạng key-value
- Nếu thông tin không có, để null
- Đảm bảo format JSON đúng theo schema"""


def scrape_with_firecrawl_v2(product_url: str, timeout: int = 60) -> Optional[str]:
    """
    Scrape product page sử dụng Firecrawl v2/scrape để lấy markdown
    
    Args:
        product_url: URL của product page
        timeout: Timeout cho request (seconds)
    
    Returns:
        Markdown content hoặc None nếu lỗi
    """
    try:
        scrape_url = f"{FIRECRAWL_API_URL}/v2/scrape"
        
        payload = {
            "url": product_url,
            "formats": ["markdown"],
            "onlyMainContent": True,
            "waitFor": 2000
        }
        
        response = requests.post(scrape_url, json=payload, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle v2/scrape response format
        # Format 1: {"success": true, "markdown": "..."}
        # Format 2: {"data": {"markdown": "..."}}
        markdown = None
        if data.get("success") and data.get("markdown"):
            markdown = data.get("markdown")
        elif data.get("data") and data.get("data", {}).get("markdown"):
            markdown = data.get("data", {}).get("markdown")
        elif data.get("markdown"):
            markdown = data.get("markdown")
        
        if markdown:
            return markdown
        else:
            try:
                print(f"⚠️  Firecrawl scrape failed: {data.get('error', 'No markdown in response')}")
            except:
                pass
            return None
    except Exception as e:
        try:
            print(f"⚠️  Lỗi scrape Firecrawl: {str(e)[:100]}")
        except:
            pass
        return None


def extract_with_groq_ai(markdown_content: str, product_id: str, product_name: str = None) -> Optional[Dict[str, Any]]:
    """
    Extract structured data từ markdown bằng Groq AI
    
    Args:
        markdown_content: Markdown content từ Firecrawl
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
        
        # Limit markdown content để tránh token limit
        markdown_limited = markdown_content[:8000] if len(markdown_content) > 8000 else markdown_content
        
        # Tạo user message với schema instruction (rút gọn)
        schema_summary = """{
  "product_id": "string",
  "name": "string",
  "price": {"current_price": number, "original_price": number, "discount_percent": number, "currency": "VND"},
  "description": "string",
  "specifications": {"key": "value"},
  "images": ["url"],
  "rating": {"average": number, "total_reviews": number},
  "seller": {"name": "string", "is_official": boolean},
  "shipping": {"free_shipping": boolean, "fast_delivery": boolean, "delivery_time": "string"},
  "stock": {"available": boolean, "quantity": number, "stock_status": "string"},
  "category_path": ["string"],
  "brand": "string",
  "warranty": "string",
  "promotions": ["string"]
}"""
        
        user_content = f"""{prompt}

=== PAGE CONTENT ===
{markdown_limited}

=== INSTRUCTIONS ===
Trả về JSON object đúng theo format sau (không có markdown code blocks, chỉ JSON thuần):
{schema_summary}"""
        
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
            if "message" not in choice:
                try:
                    print(f"⚠️  Choice không có message: {choice.keys()}")
                except:
                    pass
                return None
            
            content = choice["message"].get("content", "").strip()
            
            # Debug: log content
            if not content:
                try:
                    print(f"⚠️  Content trống. Choice structure: {json.dumps(choice, indent=2)[:300]}")
                except:
                    pass
                return None
            
            # Remove markdown code blocks nếu có
            if content.startswith("```"):
                # Extract JSON từ code block
                lines = content.split("\n")
                json_start = None
                json_end = None
                for i, line in enumerate(lines):
                    if line.strip().startswith("```json") or line.strip().startswith("```"):
                        json_start = i + 1
                    elif line.strip() == "```" and json_start is not None:
                        json_end = i
                        break
                
                if json_start is not None:
                    if json_end is not None:
                        content = "\n".join(lines[json_start:json_end])
                    else:
                        content = "\n".join(lines[json_start:])
            
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
    
    # Step 1: Scrape với Firecrawl v2
    try:
        print(f"   📥 Scraping với Firecrawl v2...")
    except:
        pass
    
    markdown = scrape_with_firecrawl_v2(product_url, timeout=timeout)
    if not markdown:
        try:
            print(f"   ✗ Scrape thất bại")
        except:
            pass
        return None
    
    try:
        print(f"   ✓ Scraped {len(markdown)} chars")
    except:
        pass
    
    # Step 2: Extract với Groq AI
    try:
        print(f"   🧠 Extracting với Groq AI...")
    except:
        pass
    
    details = extract_with_groq_ai(markdown, product_id, product_name)
    
    if details:
        try:
            print(f"   ✓ Extract thành công")
        except:
            pass
        # Validate và enrich data
        details = validate_and_enrich_product_details(details, product_id, product_url)
    else:
        try:
            print(f"   ✗ Extract thất bại")
        except:
            pass
    
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
    
    # Ensure images is array
    if "images" not in details:
        details["images"] = []
    elif not isinstance(details["images"], list):
        details["images"] = []
    
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

