"""
Test crawl một product cụ thể để xem dữ liệu extract được
"""
import os
import sys
import json
from datetime import datetime

# Load .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    pass

# Thêm path để import modules
# Tính toán đường dẫn tuyệt đối đến src từ script hiện tại
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, '..', '..'))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pipelines.crawl.tiki.extract_product_details import (
    extract_product_details_ai,
    scrape_with_firecrawl_v2,
    extract_with_groq_ai
)
from pipelines.crawl.tiki.extract_products import extract_product_id

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


def main():
    # URL test
    test_url = "https://tiki.vn/apple-iphone-13-hang-chinh-hang-p184059211.html"
    product_id = extract_product_id(test_url)
    
    safe_print("=" * 70)
    safe_print("TEST CRAWL SINGLE PRODUCT")
    safe_print("=" * 70)
    safe_print(f"URL: {test_url}")
    safe_print(f"Product ID: {product_id}")
    safe_print()
    
    # Step 1: Scrape với Firecrawl v2 (cả markdown và HTML)
    safe_print("=" * 70)
    safe_print("BƯỚC 1: Scrape với Firecrawl v2 (markdown + HTML)")
    safe_print("=" * 70)
    
    scrape_result = scrape_with_firecrawl_v2(test_url, timeout=120)
    
    if scrape_result:
        markdown = scrape_result.get("markdown", "")
        html = scrape_result.get("html", "")
        
        safe_print(f"✓ Scraped thành công:")
        safe_print(f"   - Markdown: {len(markdown)} chars")
        safe_print(f"   - HTML: {len(html)} chars")
        safe_print()
        
        if markdown:
            safe_print("Preview markdown (first 1000 chars):")
            safe_print("-" * 70)
            safe_print(markdown[:1000])
            safe_print("-" * 70)
            safe_print()
        
        # Lưu markdown và HTML để xem
        output_dir = os.path.join(os.path.dirname(__file__), '../data/raw/demo')
        os.makedirs(output_dir, exist_ok=True)
        
        if markdown:
            markdown_file = os.path.join(output_dir, 'test_iphone13_markdown.md')
            with open(markdown_file, 'w', encoding='utf-8') as f:
                f.write(markdown)
            safe_print(f"💾 Đã lưu markdown vào: {markdown_file}")
        
        if html:
            html_file = os.path.join(output_dir, 'test_iphone13_html.html')
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(html)
            safe_print(f"💾 Đã lưu HTML vào: {html_file}")
        
        safe_print()
    else:
        safe_print("✗ Scrape thất bại")
        return
    
    # Step 2: Extract với AI
    safe_print("=" * 70)
    safe_print("BƯỚC 2: Extract với Groq AI")
    safe_print("=" * 70)
    
    details = extract_product_details_ai(
        product_url=test_url,
        product_id=product_id,
        product_name="Apple iPhone 13",
        timeout=120
    )
    
    if details:
        safe_print("✓ Extract thành công!")
        safe_print()
        safe_print("=" * 70)
        safe_print("DỮ LIỆU EXTRACT ĐƯỢC:")
        safe_print("=" * 70)
        
        # Hiển thị chi tiết
        safe_print(f"📦 Tên: {details.get('name', 'N/A')}")
        safe_print(f"🆔 Product ID: {details.get('product_id', 'N/A')}")
        safe_print(f"🏷️  Brand: {details.get('brand', 'N/A')}")
        safe_print()
        
        # Price
        price = details.get('price', {})
        safe_print("💰 Giá:")
        safe_print(f"   - Hiện tại: {price.get('current_price', 'N/A'):,} {price.get('currency', 'VND')}")
        if price.get('original_price'):
            safe_print(f"   - Gốc: {price.get('original_price'):,} {price.get('currency', 'VND')}")
        if price.get('discount_percent'):
            safe_print(f"   - Giảm: {price.get('discount_percent')}%")
        safe_print()
        
        # Description
        description = details.get('description', '')
        safe_print(f"📝 Mô tả: {len(description)} chars")
        if description:
            safe_print(f"   Preview: {description[:200]}...")
        safe_print()
        
        # Specifications
        specs = details.get('specifications', {})
        safe_print(f"⚙️  Thông số kỹ thuật: {len(specs)} items")
        if specs:
            for key, value in list(specs.items())[:10]:
                safe_print(f"   - {key}: {value}")
            if len(specs) > 10:
                safe_print(f"   ... và {len(specs) - 10} thông số khác")
        safe_print()
        
        # Detailed info
        detailed_info = details.get('detailed_info', '')
        safe_print(f"📋 Thông tin chi tiết: {len(detailed_info)} chars")
        if detailed_info:
            safe_print(f"   Preview: {detailed_info[:300]}...")
        safe_print()
        
        # Customer reviews
        reviews = details.get('customer_reviews', [])
        safe_print(f"⭐ Khách hàng đánh giá: {len(reviews)} reviews")
        if reviews:
            for i, review in enumerate(reviews[:5], 1):
                safe_print(f"   Review {i}:")
                safe_print(f"      - Người đánh giá: {review.get('reviewer_name', 'N/A')}")
                safe_print(f"      - Điểm: {review.get('rating', 'N/A')}/5")
                safe_print(f"      - Nội dung: {review.get('review_text', '')[:100]}...")
                if review.get('review_date'):
                    safe_print(f"      - Ngày: {review.get('review_date')}")
                safe_print()
            if len(reviews) > 5:
                safe_print(f"   ... và {len(reviews) - 5} reviews khác")
        safe_print()
        
        # Rating
        rating = details.get('rating', {})
        safe_print("⭐ Đánh giá tổng quan:")
        safe_print(f"   - Điểm TB: {rating.get('average', 'N/A')}/5")
        safe_print(f"   - Tổng reviews: {rating.get('total_reviews', 'N/A')}")
        safe_print()
        
        # Seller
        seller = details.get('seller', {})
        safe_print("🏪 Người bán:")
        safe_print(f"   - Tên: {seller.get('name', 'N/A')}")
        safe_print(f"   - Chính hãng: {seller.get('is_official', 'N/A')}")
        safe_print()
        
        # Shipping
        shipping = details.get('shipping', {})
        safe_print("🚚 Vận chuyển:")
        safe_print(f"   - Miễn phí ship: {shipping.get('free_shipping', 'N/A')}")
        safe_print(f"   - Giao nhanh: {shipping.get('fast_delivery', 'N/A')}")
        safe_print(f"   - Thời gian: {shipping.get('delivery_time', 'N/A')}")
        safe_print()
        
        # Stock
        stock = details.get('stock', {})
        safe_print("📦 Tồn kho:")
        safe_print(f"   - Còn hàng: {stock.get('available', 'N/A')}")
        safe_print(f"   - Trạng thái: {stock.get('stock_status', 'N/A')}")
        safe_print()
        
        # Category
        category_path = details.get('category_path', [])
        safe_print(f"📁 Category: {' > '.join(category_path) if category_path else 'N/A'}")
        safe_print()
        
        # Promotions
        promotions = details.get('promotions', [])
        safe_print(f"🎁 Khuyến mãi: {len(promotions)} items")
        if promotions:
            for promo in promotions[:5]:
                safe_print(f"   - {promo}")
        safe_print()
        
        # Lưu kết quả
        output_dir = os.path.join(os.path.dirname(__file__), '../data/raw/demo')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'test_iphone13_details.json')
        
        result = {
            "crawl_time": datetime.now().isoformat(),
            "product_url": test_url,
            "product_details": details
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        safe_print(f"💾 Đã lưu kết quả vào: {output_file}")
        safe_print()
        
        # Summary
        safe_print("=" * 70)
        safe_print("TÓM TẮT:")
        safe_print("=" * 70)
        safe_print(f"✓ Description: {'Có' if description else 'Không'}")
        safe_print(f"✓ Specifications: {len(specs)} items")
        safe_print(f"✓ Detailed info: {'Có' if detailed_info else 'Không'} ({len(detailed_info)} chars)")
        safe_print(f"✓ Customer reviews: {len(reviews)} reviews")
        safe_print(f"✓ Category path: {'Có' if category_path else 'Không'}")
        safe_print(f"✓ Promotions: {len(promotions)} items")
        
    else:
        safe_print("✗ Extract thất bại")


if __name__ == "__main__":
    main()

