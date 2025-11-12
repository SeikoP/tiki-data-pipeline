"""
Demo script để test crawl product details với AI extraction
"""
import os
import sys
import json
from datetime import datetime

# Load .env file
try:
    from dotenv import load_dotenv
    # Load .env từ project root
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        # Try current directory
        load_dotenv()
except ImportError:
    # python-dotenv not installed, skip
    pass

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_product_details import (
    extract_product_details_ai,
    crawl_product_details,
    save_product_details_to_json,
    load_product_details_from_json
)
from pipelines.crawl.tiki.extract_products import load_products_from_json
from pipelines.crawl.tiki.config import GROQ_CONFIG

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def safe_print(*args, **kwargs):
    """Safe print function"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            print(*args, **kwargs, file=sys.stderr)
        except:
            pass


def print_section(title):
    """Print section header"""
    safe_print("\n" + "=" * 70)
    safe_print(f"  {title}")
    safe_print("=" * 70)


def check_groq_config():
    """Kiểm tra Groq config"""
    print_section("KIỂM TRA GROQ CONFIG")
    
    if GROQ_CONFIG.get("enabled"):
        safe_print("✓ Groq API đã được cấu hình")
        if GROQ_CONFIG.get("api_keys"):
            safe_print(f"  - Số lượng keys: {len(GROQ_CONFIG['api_keys'])}")
        elif GROQ_CONFIG.get("api_key"):
            safe_print("  - Single key mode")
        safe_print(f"  - Model: {GROQ_CONFIG.get('model', 'N/A')}")
        return True
    else:
        safe_print("⚠️  Groq API chưa được cấu hình")
        safe_print("   Hãy set GROQ_API_KEY hoặc GROQ_API_KEYS trong .env")
        return False


def demo_load_products():
    """Load products từ file"""
    print_section("BƯỚC 1: Load Products")
    
    products_file = "data/raw/demo/crawled_products_1795.json"
    
    if not os.path.exists(products_file):
        safe_print(f"⚠️  Không tìm thấy file: {products_file}")
        return []
    
    try:
        with open(products_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, dict) and 'products' in data:
            products = data['products']
        elif isinstance(data, list):
            products = data
        else:
            products = []
        
        safe_print(f"✓ Đã load {len(products)} products từ: {products_file}")
        
        # Hiển thị sample
        if products:
            safe_print("\n📋 Sample products:")
            for i, prod in enumerate(products[:3], 1):
                name = prod.get('name', 'N/A')
                product_id = prod.get('product_id', 'N/A')
                safe_print(f"  {i}. {name} (ID: {product_id})")
        
        return products
        
    except Exception as e:
        safe_print(f"⚠️  Lỗi khi load products: {e}")
        return []


def demo_extract_single_product(product_url: str, product_id: str, product_name: str = None):
    """Demo extract single product"""
    print_section("BƯỚC 2: Extract Single Product Details")
    
    safe_print(f"📦 Product: {product_name or 'N/A'}")
    safe_print(f"🔗 URL: {product_url}")
    safe_print(f"📌 ID: {product_id}")
    safe_print("\n⏳ Đang extract với AI (Firecrawl + Groq)...")
    
    details = extract_product_details_ai(
        product_url=product_url,
        product_id=product_id,
        product_name=product_name,
        timeout=120
    )
    
    if details:
        safe_print("\n✓ Extract thành công!")
        display_product_details(details)
        return details
    else:
        safe_print("\n✗ Extract thất bại")
        return None


def demo_crawl_multiple_products(products, max_products=2):
    """Demo crawl multiple products"""
    print_section("BƯỚC 3: Crawl Multiple Products")
    
    safe_print(f"💡 Đang crawl {max_products} products đầu tiên")
    safe_print(f"   - Timeout: 120s mỗi request")
    safe_print(f"   - Delay: 1s giữa các requests")
    safe_print("")
    
    all_details = crawl_product_details(
        products=products,
        max_products=max_products,
        timeout=120,
        delay_between_requests=1.0
    )
    
    safe_print(f"\n✓ Tổng cộng extract được {len(all_details)} products")
    
    return all_details


def display_product_details(details: dict):
    """Display product details"""
    safe_print("\n" + "-" * 70)
    safe_print("PRODUCT DETAILS")
    safe_print("-" * 70)
    
    # Basic info
    safe_print(f"\n📦 Tên: {details.get('name', 'N/A')}")
    safe_print(f"🆔 Product ID: {details.get('product_id', 'N/A')}")
    safe_print(f"🏷️  Brand: {details.get('brand', 'N/A')}")
    
    # Price
    price_info = details.get('price', {})
    if price_info:
        current = price_info.get('current_price')
        original = price_info.get('original_price')
        discount = price_info.get('discount_percent')
        
        if current:
            safe_print(f"\n💰 Giá:")
            safe_print(f"   - Hiện tại: {current:,.0f} VND" if current else "   - Hiện tại: N/A")
            if original:
                safe_print(f"   - Gốc: {original:,.0f} VND")
            if discount:
                safe_print(f"   - Giảm: {discount}%")
    
    # Rating
    rating_info = details.get('rating', {})
    if rating_info:
        avg = rating_info.get('average')
        total = rating_info.get('total_reviews')
        if avg or total:
            safe_print(f"\n⭐ Đánh giá:")
            if avg:
                safe_print(f"   - Điểm TB: {avg}/5")
            if total:
                safe_print(f"   - Số reviews: {total:,}")
    
    # Stock
    stock_info = details.get('stock', {})
    if stock_info:
        available = stock_info.get('available')
        status = stock_info.get('stock_status', 'N/A')
        safe_print(f"\n📦 Tồn kho:")
        safe_print(f"   - Trạng thái: {status}")
        safe_print(f"   - Còn hàng: {'Có' if available else 'Không'}")
    
    # Shipping
    shipping_info = details.get('shipping', {})
    if shipping_info:
        free = shipping_info.get('free_shipping')
        fast = shipping_info.get('fast_delivery')
        if free is not None or fast is not None:
            safe_print(f"\n🚚 Vận chuyển:")
            if free is not None:
                safe_print(f"   - Miễn phí ship: {'Có' if free else 'Không'}")
            if fast is not None:
                safe_print(f"   - Giao nhanh: {'Có' if fast else 'Không'}")
    
    # Specifications
    specs = details.get('specifications', {})
    if specs:
        safe_print(f"\n⚙️  Thông số kỹ thuật ({len(specs)} items):")
        for key, value in list(specs.items())[:5]:
            safe_print(f"   - {key}: {value}")
        if len(specs) > 5:
            safe_print(f"   ... và {len(specs) - 5} thông số khác")
    
    # Images
    images = details.get('images', [])
    if images:
        safe_print(f"\n🖼️  Hình ảnh: {len(images)} ảnh")
    
    # Category
    category_path = details.get('category_path', [])
    if category_path:
        safe_print(f"\n📁 Category: {' > '.join(category_path)}")


def demo_save_details(product_details):
    """Save product details"""
    print_section("BƯỚC 4: Save Product Details")
    
    if not product_details:
        safe_print("⚠️  Không có product details để lưu")
        return
    
    output_dir = "data/raw/demo"
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "product_details.json")
    save_product_details_to_json(product_details, output_file)
    
    safe_print(f"✓ Đã lưu vào: {output_file}")
    safe_print(f"  - File size: {os.path.getsize(output_file)} bytes")


def main():
    """Main demo function"""
    safe_print("\n" + "=" * 70)
    safe_print(" " * 15 + "TIKI PRODUCT DETAILS - AI EXTRACTION DEMO")
    safe_print("=" * 70)
    safe_print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    safe_print("\n💡 Demo extract product details sử dụng AI (Firecrawl + Groq)")
    safe_print("   - Sử dụng structured extraction với JSON schema")
    safe_print("   - Hỗ trợ multiple Groq API keys với round-robin")
    
    # Check Groq config
    if not check_groq_config():
        safe_print("\n⚠️  Vui lòng cấu hình Groq API trước khi chạy demo")
        return
    
    # ============================================
    # CẤU HÌNH - Điều chỉnh để chạy nhanh/chậm
    # ============================================
    MAX_PRODUCTS = 2  # Chỉ crawl 2 products để demo nhanh
    EXTRACT_SINGLE = False  # True = test single product, False = crawl multiple
    # ============================================
    
    try:
        # Load products
        products = demo_load_products()
        
        if not products:
            safe_print("\n⚠️  Không có products để crawl")
            return
        
        if EXTRACT_SINGLE:
            # Test single product
            product = products[0]
            details = demo_extract_single_product(
                product_url=product.get('url', ''),
                product_id=product.get('product_id', ''),
                product_name=product.get('name', '')
            )
            
            if details:
                demo_save_details([details])
        else:
            # Crawl multiple products
            all_details = demo_crawl_multiple_products(products, max_products=MAX_PRODUCTS)
            
            if all_details:
                # Display first product
                if len(all_details) > 0:
                    display_product_details(all_details[0])
                
                # Save
                demo_save_details(all_details)
        
        # Summary
        print_section("TÓM TẮT")
        safe_print("✅ Demo hoàn thành!")
        safe_print("\n💡 Tips:")
        safe_print("   - Tăng MAX_PRODUCTS để crawl nhiều hơn")
        safe_print("   - Kiểm tra file JSON output để xem chi tiết")
        safe_print("   - Sử dụng multiple Groq keys để tăng throughput")
        
    except KeyboardInterrupt:
        safe_print("\n\n⚠️  Đã dừng bởi người dùng")
    except Exception as e:
        safe_print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    
    safe_print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

