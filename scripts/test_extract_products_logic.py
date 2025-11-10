"""
Test logic extraction products từ HTML/Markdown
Không cần Firecrawl API chạy
"""
import os
import sys
import json

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_products import (
    extract_products_from_html,
    extract_products_from_markdown,
    extract_product_id
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


def test_extract_product_id():
    """Test product ID extraction"""
    print_section("TEST 1: Extract Product ID")
    
    test_cases = [
        ("https://tiki.vn/p1234567", "1234567"),
        ("https://tiki.vn/san-pham/p9876543", "9876543"),
        ("/p5555555", "5555555"),
        ("https://tiki.vn/product?id=1111111", "1111111"),
        ("https://tiki.vn/c123", None),  # Category, not product
        ("/c456", None),  # Category, not product
    ]
    
    all_pass = True
    for url, expected_id in test_cases:
        extracted = extract_product_id(url)
        status = "✓" if extracted == expected_id else "✗"
        
        if extracted != expected_id:
            all_pass = False
        
        safe_print(f"{status} URL: {url}")
        safe_print(f"   Expected: {expected_id}, Got: {extracted}")
    
    safe_print(f"\n{'✅ PASS' if all_pass else '❌ FAIL'}")
    return all_pass


def test_extract_from_markdown():
    """Test product extraction từ Markdown"""
    print_section("TEST 2: Extract Products from Markdown")
    
    markdown_html = """
# Sách và Truyện

- [Lập trình Python cơ bản](/p12345678)
- [Tiểu thuyết Conan tập 100](/p87654321)
- [Java Programming Guide](/p11111111)
- [Danh mục Sách tiếng Anh](/c320)  # Đây là category, không phải product
    """
    
    products = extract_products_from_markdown(
        markdown_html,
        category_id="316",
        category_name="Sách tiếng Việt"
    )
    
    safe_print(f"Tìm thấy {len(products)} products:")
    
    for i, prod in enumerate(products, 1):
        safe_print(f"\n  {i}. {prod['name']}")
        safe_print(f"     Product ID: {prod['product_id']}")
        safe_print(f"     URL: {prod['url']}")
        safe_print(f"     Category: {prod['category_name']}")
    
    # Kiểm tra
    expected_count = 3  # Chỉ 3 products, không tính category link
    status = "✓" if len(products) == expected_count else "✗"
    
    safe_print(f"\n{status} Expected {expected_count} products, got {len(products)}")
    safe_print(f"{'✅ PASS' if len(products) == expected_count else '❌ FAIL'}")
    
    return len(products) == expected_count


def test_extract_from_html():
    """Test product extraction từ HTML"""
    print_section("TEST 3: Extract Products from HTML")
    
    html_content = """
    <html>
    <body>
        <div class="product-list">
            <a href="https://tiki.vn/p22222222">Sách Toán Lớp 1</a>
            <a href="/p33333333">Vở viết</a>
            <a href="https://tiki.vn/danh-muc/c852">Đạo đức - Kỹ năng</a>
            <a href="/p44444444">Bút chì Faber Castell</a>
        </div>
    </body>
    </html>
    """
    
    products = extract_products_from_html(
        html_content,
        category_id="852",
        category_name="Đạo đức - Kỹ năng sống"
    )
    
    safe_print(f"Tìm thấy {len(products)} products:")
    
    for i, prod in enumerate(products, 1):
        safe_print(f"\n  {i}. {prod['name']}")
        safe_print(f"     Product ID: {prod['product_id']}")
        safe_print(f"     Category: {prod['category_name']}")
    
    # Kiểm tra: chỉ nên có 3 products (loại bỏ category link)
    expected_count = 3
    status = "✓" if len(products) == expected_count else "✗"
    
    safe_print(f"\n{status} Expected {expected_count} products, got {len(products)}")
    safe_print(f"{'✅ PASS' if len(products) == expected_count else '❌ FAIL'}")
    
    return len(products) == expected_count


def main():
    """Run all tests"""
    safe_print("\n" + "=" * 70)
    safe_print(" " * 15 + "TIKI PRODUCTS - EXTRACTION LOGIC TEST")
    safe_print("=" * 70)
    
    safe_print("\n💡 Test logic extraction products mà không cần Firecrawl API")
    
    results = []
    
    # Test 1
    results.append(test_extract_product_id())
    
    # Test 2
    results.append(test_extract_from_markdown())
    
    # Test 3
    results.append(test_extract_from_html())
    
    # Summary
    print_section("TÓM TẮT")
    
    passed = sum(results)
    total = len(results)
    
    safe_print(f"✓ Passed: {passed}/{total}")
    
    if passed == total:
        safe_print("\n✅ Tất cả tests passed!")
    else:
        safe_print(f"\n⚠️  {total - passed} tests failed!")
    
    safe_print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

