import json
import re
import sys
from pathlib import Path

import pytest

# Thêm đường dẫn src vào sys.path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.crawl.crawl_categories_optimized import safe_print
from pipelines.crawl.crawl_products import (
    crawl_category_products,
    crawl_products_from_categories,
    get_page_with_requests,
    get_total_pages,
    parse_products_from_html,
)


def test_parse_products_from_html():
    """
    Test parse sản phẩm từ HTML.
    """
    print("=" * 70)
    print("🧪 TEST: Parse sản phẩm từ HTML")
    print("=" * 70)

    # HTML mẫu (giả lập)
    html_sample = """
    <html>
    <body>
        <div class="product-item">
            <a href="/p/12345">
                <img src="https://salt.tikicdn.com/cache/200x200/ts/product/12/34/56.jpg" alt="Sản phẩm test">
                <h3 class="product-title">Sản phẩm Test 1</h3>
                <div class="product-price__current-price">100.000 ₫</div>
            </a>
        </div>
        <div class="product-item">
            <a href="/p/67890">
                <img src="https://salt.tikicdn.com/cache/200x200/ts/product/67/89/90.jpg" alt="Sản phẩm test 2">
                <h3 class="product-title">Sản phẩm Test 2</h3>
                <div class="product-price__current-price">200.000 ₫</div>
            </a>
        </div>
    </body>
    </html>
    """

    category_url = "https://tiki.vn/test/c123"
    products = parse_products_from_html(html_sample, category_url)

    print(f"✓ Tìm thấy {len(products)} sản phẩm")
    for product in products:
        print(f"  - {product.get('name')} (ID: {product.get('product_id')})")

    assert len(products) >= 2, "Phải tìm thấy ít nhất 2 sản phẩm"
    assert products[0].get("product_id") == "12345", "Product ID không đúng"
    assert products[0].get("name") == "Sản phẩm Test 1", "Tên sản phẩm không đúng"

    print("✅ Test parse HTML thành công!\n")


def test_category_progress_output_ignores_closed_stdout(monkeypatch):
    class ClosedStdout:
        closed = True
        encoding = "cp1252"

        def write(self, _message):
            raise ValueError("I/O operation on closed file")

        def flush(self):
            raise ValueError("I/O operation on closed file")

    monkeypatch.setattr(sys, "stdout", ClosedStdout())

    safe_print("danh mục ✅", flush=True)


def test_crawl_single_category():
    """
    Test crawl một danh mục cụ thể.
    """
    print("=" * 70)
    print("🧪 TEST: Crawl một danh mục")
    print("=" * 70)

    # Chọn một danh mục bất kỳ từ file (thử với danh mục khác)
    categories_file = "data/raw/categories_recursive_optimized.json"

    if not Path(categories_file).exists():
        pytest.skip(f"Missing categories fixture: {categories_file}")

    with open(categories_file, encoding="utf-8") as f:
        categories = json.load(f)

    # Chọn danh mục có nhiều sản phẩm - thử với danh mục level 2-3 và có nhiều sản phẩm
    # Ưu tiên các danh mục phổ biến như điện tử, thời trang, etc.
    test_categories = []
    for cat in categories:
        level = cat.get("level", 0)
        name = cat.get("name", "").lower()
        # Chọn các danh mục phổ biến
        if 2 <= level <= 3:
            if any(
                keyword in name
                for keyword in [
                    "điện thoại",
                    "laptop",
                    "tai nghe",
                    "áo",
                    "quần",
                    "giày",
                    "túi",
                    "đồng hồ",
                ]
            ):
                test_categories.append(cat)

    # Nếu không tìm thấy danh mục phổ biến, lấy bất kỳ danh mục level 2-3
    if not test_categories:
        test_categories = [cat for cat in categories if 2 <= cat.get("level", 0) <= 3]

    # Nếu vẫn không có, lấy bất kỳ
    if not test_categories:
        test_categories = categories[:10] if len(categories) > 10 else categories

    # Chọn ngẫu nhiên hoặc lấy danh mục thứ 3 để test với danh mục khác

    test_category = (
        test_categories[2]
        if len(test_categories) > 2
        else test_categories[0]
        if test_categories
        else None
    )

    if not test_category:
        pytest.skip("No category available in fixture")

    category_url = test_category.get("url", "")
    category_name = test_category.get("name", "Unknown")

    print(f"📁 Danh mục: {category_name}")
    print(f"🔗 URL: {category_url}")
    print(f"📊 Level: {test_category.get('level', 0)}")
    print("📝 Lưu ý: Chỉ crawl thông tin cơ bản (ID, tên, URL, hình) - sẽ crawl detail sau")
    print("\n⏳ Đang crawl...")

    # Crawl với giới hạn 2 trang để test nhanh
    # Thử với Selenium nếu requests không tìm thấy sản phẩm
    products = crawl_category_products(
        category_url,
        max_pages=2,
        use_selenium=True,  # Dùng Selenium để render JavaScript
    )

    print(f"\n✅ Tìm thấy {len(products)} sản phẩm")

    if products:
        print("\n📦 Mẫu sản phẩm (5 sản phẩm đầu):")
        for i, product in enumerate(products[:5], 1):
            print(f"  {i}. {product.get('name', 'N/A')}")
            print(f"     ID: {product.get('product_id')}")
            print(f"     URL: {product.get('url')}")
            if product.get("image_url"):
                print(f"     Hình: {product.get('image_url')[:50]}...")
            print("     (Giá, đánh giá, số lượng bán sẽ crawl detail sau)")
            print()

    assert products, "Phải tìm thấy ít nhất 1 sản phẩm"
    print("✅ Test crawl danh mục thành công!\n")


def test_crawl_multiple_categories():
    """Test crawl nhiều danh mục - CRAWL HẾT SẢN PHẨM trong một vài danh mục deep level"""
    print("=" * 70)
    print("🧪 TEST: Crawl HẾT sản phẩm trong một vài danh mục DEEP LEVEL")
    print("=" * 70)

    categories_file = "data/raw/categories_recursive_optimized.json"
    output_file = "data/demo/products/products.json"

    if not Path(categories_file).exists():
        pytest.skip(f"Missing categories fixture: {categories_file}")

    with open(categories_file, encoding="utf-8") as f:
        all_categories = json.load(f)

    # Filter lấy các danh mục ở DEEP LEVEL (level 3-4) - có nhiều sản phẩm cụ thể
    deep_level_categories = []
    for cat in all_categories:
        level = cat.get("level", 0)
        # Lấy level 3-4 (deep level - danh mục con sâu)
        if 3 <= level <= 4:
            deep_level_categories.append(cat)

    # Chọn một vài danh mục từ deep level
    # Có thể điều chỉnh số lượng danh mục ở đây
    NUM_CATEGORIES_TO_CRAWL = 4  # Số danh mục để crawl (có thể thay đổi)

    selected_categories = (
        deep_level_categories[:NUM_CATEGORIES_TO_CRAWL]
        if len(deep_level_categories) > NUM_CATEGORIES_TO_CRAWL
        else deep_level_categories
    )

    if not selected_categories:
        print("❌ Không tìm thấy danh mục ở deep level (level 3-4)")
        print("   Thử với level 2-3...")
        # Fallback: lấy level 2-3 nếu không có level 3-4
        for cat in all_categories:
            if 2 <= cat.get("level", 0) <= 3:
                selected_categories.append(cat)
                if len(selected_categories) >= 10:
                    break

    if not selected_categories:
        pytest.skip("No suitable categories available in fixture")

    print(f"📖 Đã chọn {len(selected_categories)} danh mục ở deep level (level 3-4)")
    print(f"📁 File output: {output_file}")
    print("📝 Lưu ý: Crawl HẾT sản phẩm (TẤT CẢ trang) trong các danh mục này")
    print("          Chỉ lấy thông tin cơ bản (ID, tên, URL, hình)")
    print("          Giá, đánh giá, số lượng bán sẽ crawl detail sau")
    print(f"\n📋 Danh sách {len(selected_categories)} danh mục sẽ crawl:")
    for i, cat in enumerate(selected_categories, 1):
        print(f"   {i}. {cat.get('name')} (Level {cat.get('level')})")
        print(f"      {cat.get('url')}")
    print("=" * 70)

    # Filter function để chỉ crawl các danh mục đã chọn
    selected_urls = {cat.get("url") for cat in selected_categories}

    def filter_selected_categories(cat):
        return cat.get("url") in selected_urls

    products = crawl_products_from_categories(
        categories_file=categories_file,
        output_file=output_file,
        max_categories=None,  # Crawl tất cả các danh mục đã chọn
        max_pages_per_category=None,  # Crawl TẤT CẢ trang (không giới hạn)
        max_workers=5,  # 5 thread song song
        use_selenium=False,  # Dùng requests (nhanh hơn), tự động fallback Selenium nếu cần
        categories_filter=filter_selected_categories,  # Chỉ crawl các danh mục đã chọn
    )

    print("\n✅ Crawl hoàn thành!")
    print(f"📦 Tổng sản phẩm: {len(products)}")
    print(f"📁 File output: {output_file}")

    # Thống kê theo danh mục
    if products:
        category_stats = {}
        category_names = {}

        # Lấy tên danh mục từ selected_categories
        for cat in selected_categories:
            category_names[cat.get("url")] = cat.get("name", "Unknown")

        for product in products:
            cat_url = product.get("category_url", "Unknown")
            category_stats[cat_url] = category_stats.get(cat_url, 0) + 1
            # Lưu tên danh mục nếu chưa có
            if cat_url not in category_names:
                category_names[cat_url] = cat_url.split("/")[-2] if "/" in cat_url else "Unknown"

        print("\n📊 Thống kê theo danh mục:")
        print(f"   Số danh mục: {len(category_stats)}")
        print(f"   Tổng sản phẩm: {len(products)}")

        # Sắp xếp và hiển thị
        sorted_stats = sorted(category_stats.items(), key=lambda x: x[1], reverse=True)
        for cat_url, count in sorted_stats:
            cat_name = category_names.get(cat_url, cat_url)
            print(
                f"   - {count:4d} sản phẩm | {cat_name} (Level {next((c.get('level') for c in selected_categories if c.get('url') == cat_url), '?')})"
            )
            print(f"     {cat_url}")

    assert products, "Expected at least one product from selected categories"


def test_get_page():
    """
    Test lấy trang web.
    """
    print("=" * 70)
    print("🧪 TEST: Lấy trang web")
    print("=" * 70)

    test_url = "https://tiki.vn/dien-thoai-smartphone/c1795"

    print(f"⏳ Đang lấy HTML từ: {test_url}")
    html = get_page_with_requests(test_url)

    assert html, "Không lấy được HTML"

    print(f"✅ Đã lấy thành công ({len(html)} ký tự)")

    # Kiểm tra __NEXT_DATA__
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    next_data = soup.find("script", id="__NEXT_DATA__")
    if next_data:
        print(f"✓ Tìm thấy __NEXT_DATA__ ({len(next_data.string)} ký tự)")
    else:
        print("⚠️  Không tìm thấy __NEXT_DATA__ - có thể cần Selenium")

    # Test parse
    products = parse_products_from_html(html, test_url)
    print(f"📦 Tìm thấy {len(products)} sản phẩm")

    if len(products) == 0:
        # Debug: tìm các link /p/
        all_links = soup.find_all("a", href=re.compile(r"/p/\d+"))
        print(f"   Debug: Tìm thấy {len(all_links)} link /p/ trong HTML")

    # Test get total pages
    total_pages = get_total_pages(html)
    print(f"📄 Tổng số trang: {total_pages}")


def main():
    """Chạy tất cả tests - CRAWL HẾT SẢN PHẨM VÀ LƯU VÀO DATA/DEMO"""
    print("=" * 70)
    print("🧪 CHẠY TEST CRAWL SẢN PHẨM - CRAWL HẾT VÀ LƯU VÀO DATA/DEMO")
    print("=" * 70)
    print()

    # Có thể bỏ qua các test nhỏ và chỉ chạy crawl hết
    # Uncomment các test dưới nếu muốn chạy đầy đủ

    # # Test 1: Parse HTML
    # try:
    #     test_parse_products_from_html()
    # except Exception as e:
    #     print(f"❌ Test parse HTML thất bại: {e}\n")

    # # Test 2: Get page
    # try:
    #     test_get_page()
    #     print()
    # except Exception as e:
    #     print(f"❌ Test get page thất bại: {e}\n")

    # # Test 3: Crawl single category (test nhanh)
    # try:
    #     test_crawl_single_category()
    # except Exception as e:
    #     print(f"❌ Test crawl single category thất bại: {e}\n")

    # Test 4: Crawl multiple categories - CRAWL HẾT SẢN PHẨM
    print("\n" + "=" * 70)
    print("🚀 BẮT ĐẦU CRAWL HẾT SẢN PHẨM")
    print("=" * 70)
    print("📁 Lưu vào: data/demo/products/products.json")
    print("⏳ Quá trình này có thể mất nhiều thời gian...")
    print("=" * 70)

    try:
        test_crawl_multiple_categories()
        print("\n" + "=" * 70)
        print("✅ HOÀN THÀNH CRAWL HẾT SẢN PHẨM!")
        print("=" * 70)
        print("📁 File kết quả: data/demo/products/products.json")
        print("💡 Có thể dùng file này để crawl detail (giá, đánh giá, số lượng bán) sau")
    except Exception as e:
        print(f"❌ Test crawl multiple categories thất bại: {e}\n")
        import traceback

        traceback.print_exc()
        print("\n" + "=" * 70)
        print("❌ CRAWL THẤT BẠI!")
        print("=" * 70)


if __name__ == "__main__":
    main()
