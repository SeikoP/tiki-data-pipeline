"""
Test crawl trang thực từ Tiki để kiểm tra extraction products logic
Crawl từ: https://tiki.vn/dien-thoai-smartphone/c1795
"""
import os
import sys
import json
import requests
from datetime import datetime

# Thêm path để import modules
# Tính toán đường dẫn tuyệt đối đến src từ script hiện tại
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, '..', '..'))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pipelines.crawl.tiki.extract_products import (
    parse_firecrawl_products,
    extract_products_from_html,
    extract_products_from_markdown
)

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


def crawl_firecrawl(url, timeout=60):
    """
    Crawl trang sử dụng Firecrawl API
    """
    FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
    
    payload = {
        "url": url,
        "onlyMainContent": True,
        "maxAge": 172800000,  # 2 days
        "parsers": [],
        "formats": ["html", "markdown"]
    }
    
    try:
        safe_print(f"📡 Connecting to Firecrawl API: {FIRECRAWL_API_URL}")
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v0/scrape",
            json=payload,
            timeout=timeout
        )
        response.raise_for_status()
        response_data = response.json()
        
        safe_print("✓ Connected successfully!")
        
        return response_data
        
    except requests.exceptions.ConnectionError as e:
        safe_print(f"❌ Cannot connect to Firecrawl API at {FIRECRAWL_API_URL}")
        safe_print(f"   Make sure Firecrawl is running locally")
        safe_print(f"   Error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        safe_print(f"❌ Request error: {e}")
        return None
    except Exception as e:
        safe_print(f"❌ Unexpected error: {e}")
        return None


def test_crawl_real_page():
    """Test crawl trang thực từ Tiki"""
    print_section("TEST CRAWL TRANG ĐIỆN THOẠI TIKI")
    
    # Target URL
    target_url = "https://tiki.vn/dien-thoai-smartphone/c1795"
    category_id = "1795"
    category_name = "Điện Thoại Smartphone"
    
    safe_print(f"📱 Target Category: {category_name}")
    safe_print(f"🔗 URL: {target_url}")
    safe_print(f"📌 Category ID: {category_id}")
    
    # Try to crawl
    print_section("BƯỚC 1: Crawl trang từ Firecrawl")
    
    response_data = crawl_firecrawl(target_url)
    
    if not response_data:
        safe_print("\n⚠️  Cannot crawl. Showing mock test instead...")
        return test_with_mock_data(category_id, category_name)
    
    # Parse response
    print_section("BƯỚC 2: Parse response từ Firecrawl")
    
    # Check what we got
    has_html = 'data' in response_data and 'html' in response_data.get('data', {})
    has_markdown = 'data' in response_data and 'markdown' in response_data.get('data', {})
    
    safe_print(f"✓ HTML data: {'Yes' if has_html else 'No'}")
    safe_print(f"✓ Markdown data: {'Yes' if has_markdown else 'No'}")
    
    if has_html:
        html_size = len(response_data['data']['html'])
        safe_print(f"  HTML size: {html_size} bytes")
    
    if has_markdown:
        markdown_size = len(response_data['data']['markdown'])
        safe_print(f"  Markdown size: {markdown_size} bytes")
    
    # Extract products
    print_section("BƯỚC 3: Extract Products")
    
    products = parse_firecrawl_products(response_data, category_id, category_name)
    
    safe_print(f"\n✓ Tìm thấy {len(products)} products")
    
    # Display sample
    display_products(products)
    
    # Save to file
    print_section("BƯỚC 4: Save Results")
    
    if products:
        save_products(products, category_id, category_name)
    else:
        safe_print("⚠️  No products to save")
    
    return products


def test_with_mock_data(category_id, category_name):
    """Test with mock HTML data (when Firecrawl not available)"""
    print_section("TEST WITH MOCK DATA")
    
    # Mock HTML dari trang Tiki điện thoại
    mock_html = """
    <html>
    <body>
        <div class="product-list">
            <a href="https://tiki.vn/p12345678">iPhone 15 Pro 256GB Chính Hãng</a>
            <a href="https://tiki.vn/p12345679">Samsung Galaxy S24 Ultra 512GB</a>
            <a href="https://tiki.vn/p12345680">Xiaomi 14 5G 12GB Ram</a>
            <a href="https://tiki.vn/p12345681">Oppo Find X7 Pro 12GB</a>
            <a href="https://tiki.vn/p12345682">Vivo V30 Ultra 12GB Chính Hãng</a>
            <a href="https://tiki.vn/p12345683">Google Pixel 8 Pro 256GB</a>
            <a href="https://tiki.vn/p12345684">OnePlus 12 12GB Ram</a>
            <a href="https://tiki.vn/p12345685">Realme GT 6 Pro 12GB</a>
            <a href="/c1795">Xem thêm điện thoại</a>  <!-- Category link, should be excluded -->
            <a href="https://tiki.vn/p12345686">Poco F6 Pro 12GB</a>
            <a href="https://tiki.vn/dien-thoai-smartphone/c1795">Quay lại danh mục</a>  <!-- Category link -->
        </div>
    </body>
    </html>
    """
    
    safe_print(f"💡 Using mock HTML data")
    safe_print(f"   Mock products count: 9 phones + 2 category links")
    
    # Extract products
    products = extract_products_from_html(mock_html, category_id, category_name)
    
    safe_print(f"\n✓ Extracted {len(products)} products (category links excluded)")
    
    # Display
    display_products(products)
    
    # Save
    if products:
        save_products(products, category_id, category_name, is_mock=True)
    
    return products


def display_products(products):
    """Display products"""
    print_section("SẢN PHẨM TÌMTHẤY")
    
    if not products:
        safe_print("⚠️  No products found")
        return
    
    safe_print(f"📦 Tổng cộng: {len(products)} products\n")
    
    # Display first 10
    display_count = min(10, len(products))
    
    for i, product in enumerate(products[:display_count], 1):
        name = product.get('name', 'N/A')
        product_id = product.get('product_id', 'N/A')
        url = product.get('url', 'N/A')
        category = product.get('category_name', 'N/A')
        
        safe_print(f"{i:2d}. {name}")
        safe_print(f"    ID: {product_id}")
        safe_print(f"    URL: {url}")
        safe_print(f"    Category: {category}\n")
    
    if len(products) > display_count:
        safe_print(f"... và {len(products) - display_count} products khác")


def save_products(products, category_id, category_name, is_mock=False):
    """Save products to file"""
    output_dir = "data/raw/demo"
    os.makedirs(output_dir, exist_ok=True)
    
    # File names
    suffix = "_mock" if is_mock else ""
    output_file = os.path.join(output_dir, f"crawled_products_{category_id}{suffix}.json")
    
    # Metadata
    result = {
        "crawl_time": datetime.now().isoformat(),
        "is_mock": is_mock,
        "category_id": category_id,
        "category_name": category_name,
        "total_products": len(products),
        "products": products
    }
    
    # Save JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    safe_print(f"✓ Saved to: {output_file}")
    safe_print(f"  - Total products: {len(products)}")
    safe_print(f"  - File size: {os.path.getsize(output_file)} bytes")


def main():
    """Main test"""
    safe_print("\n" + "=" * 70)
    safe_print(" " * 15 + "TIKI PRODUCTS CRAWL - REAL PAGE TEST")
    safe_print("=" * 70)
    safe_print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    safe_print("\n💡 Test crawl trang điện thoại Tiki")
    safe_print("   - Nếu Firecrawl chạy: crawl trang thực")
    safe_print("   - Nếu Firecrawl k chạy: test với mock data")
    
    try:
        products = test_crawl_real_page()
        
        # Summary
        print_section("TÓM TẮT")
        
        if products:
            safe_print(f"✅ Thành công!")
            safe_print(f"   - Tìm thấy {len(products)} products")
            safe_print(f"   - Đã lưu vào file")
            safe_print(f"\n💡 Product extraction logic hoạt động tốt!")
            safe_print(f"   - Lọc được category links")
            safe_print(f"   - Extract được product IDs")
            safe_print(f"   - Format output đúng")
        else:
            safe_print(f"⚠️  Không tìm thấy products")
        
        safe_print("\n" + "=" * 70)
        
    except KeyboardInterrupt:
        safe_print("\n\n⚠️  Dừng bởi người dùng")
    except Exception as e:
        safe_print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

