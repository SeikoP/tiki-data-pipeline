"""
Script test để kiểm tra khả năng lấy số lượng đã bán (sales_count) Test từ crawl_products.py và
crawl_products_detail.py.
"""

import json
import os
import sys

# Thêm src vào path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "pipelines", "crawl"))

from crawl_products import crawl_category_products
from crawl_products_detail import crawl_product_detail_with_selenium, extract_product_detail


def test_crawl_products_sales_count():
    """Test 1: Kiểm tra crawl products từ categories có lấy được sales_count không"""
    print("=" * 70)
    print("TEST 1: Crawl Products từ Categories - Kiểm tra sales_count")
    print("=" * 70)

    # Test với một category URL
    test_category_url = "https://tiki.vn/dien-thoai-smartphone/c1795"

    print(f"\n📁 Category URL: {test_category_url}")
    print("🔄 Đang crawl products từ category...")

    try:
        # Crawl products từ category (chỉ trang đầu)
        products = crawl_category_products(
            test_category_url,
            max_pages=1,  # Chỉ crawl trang đầu
            use_selenium=False,  # Dùng requests trước
            cache_dir=None,  # Không dùng cache
        )

        print(f"\n✅ Đã crawl được {len(products)} products")

        # Kiểm tra sales_count
        products_with_sales = [p for p in products if p.get("sales_count") is not None]
        products_without_sales = [p for p in products if p.get("sales_count") is None]

        print("\n📊 Thống kê:")
        print(f"   - Tổng số products: {len(products)}")
        print(
            f"   - Có sales_count: {len(products_with_sales)} ({len(products_with_sales) / len(products) * 100:.1f}%)"
        )
        print(
            f"   - Không có sales_count: {len(products_without_sales)} ({len(products_without_sales) / len(products) * 100:.1f}%)"
        )

        # Hiển thị một số ví dụ
        if products_with_sales:
            print("\n✅ Ví dụ products CÓ sales_count:")
            for i, product in enumerate(products_with_sales[:5], 1):
                sales_count = product.get("sales_count")
                sales_str = (
                    f"{sales_count:,}"
                    if isinstance(sales_count, (int, float))
                    else str(sales_count)
                )
                print(f"   {i}. {product.get('name', 'N/A')[:50]}")
                print(f"      ID: {product.get('product_id')}")
                print(f"      Sales count: {sales_str}")
                print(f"      URL: {product.get('url', 'N/A')[:60]}...")
                print()

        if products_without_sales:
            print("\n⚠️  Ví dụ products KHÔNG có sales_count:")
            for i, product in enumerate(products_without_sales[:3], 1):
                print(f"   {i}. {product.get('name', 'N/A')[:50]}")
                print(f"      ID: {product.get('product_id')}")
                print(f"      URL: {product.get('url', 'N/A')[:60]}...")
                print()

        # Lưu kết quả vào file
        output_file = "data/test_output/test_products_sales_count.json"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "category_url": test_category_url,
                    "total_products": len(products),
                    "products_with_sales_count": len(products_with_sales),
                    "products_without_sales_count": len(products_without_sales),
                    "products": products,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        print(f"\n💾 Đã lưu kết quả vào: {output_file}")

        return products

    except Exception as e:
        print(f"\n❌ Lỗi khi crawl products: {e}")
        import traceback

        traceback.print_exc()
        return []


def test_crawl_product_detail_sales_count():
    """Test 2: Kiểm tra crawl product detail có lấy được sales_count không"""
    print("\n" + "=" * 70)
    print("TEST 2: Crawl Product Detail - Kiểm tra sales_count")
    print("=" * 70)

    # Test với một số product URLs
    test_urls = []

    # Ưu tiên: Dùng products từ test 1 (nếu có)
    try:
        test_file = "data/test_output/test_products_sales_count.json"
        if os.path.exists(test_file):
            with open(test_file, encoding="utf-8") as f:
                data = json.load(f)
                products = data.get("products", [])
                if products:
                    # Lấy 3-5 products có URL (ưu tiên products có sales_count từ test 1)
                    products_with_sales = [
                        p for p in products if p.get("url") and p.get("sales_count") is not None
                    ]
                    products_without_sales = [
                        p for p in products if p.get("url") and p.get("sales_count") is None
                    ]

                    # Lấy 2-3 products có sales_count và 1-2 products không có sales_count
                    test_urls = [p.get("url") for p in products_with_sales[:3] if p.get("url")]
                    test_urls.extend(
                        [p.get("url") for p in products_without_sales[:2] if p.get("url")]
                    )

                    print(f"   📋 Sử dụng {len(test_urls)} products từ Test 1")
    except Exception as e:
        print(f"   ⚠️  Không đọc được file test 1: {e}")

    # Fallback: Dùng URLs mặc định nếu không có products từ test 1
    if not test_urls:
        test_urls = [
            "https://tiki.vn/dien-thoai-iphone-15-pro-max-256gb-chinh-hang-vn-a-p293100123.html",
            "https://tiki.vn/samsung-galaxy-s24-ultra-5g-256gb-chinh-hang-vn-p293100124.html",
        ]
        print("   📋 Sử dụng URLs mặc định")

    results = []

    for i, url in enumerate(test_urls, 1):
        if not url:
            continue

        print(f"\n📦 Test Product {i}/{len(test_urls)}")
        print(f"🔗 URL: {url}")

        try:
            # Crawl với Selenium
            print("   🔄 Đang crawl với Selenium...")
            html_content = crawl_product_detail_with_selenium(url, save_html=False, verbose=False)

            if not html_content or len(html_content) < 100:
                print(
                    f"   ❌ HTML content quá ngắn: {len(html_content) if html_content else 0} ký tự"
                )
                continue

            print(f"   ✅ Đã lấy HTML: {len(html_content)} ký tự")

            # Extract detail
            print("   🔄 Đang extract detail...")
            detail = extract_product_detail(html_content, url, verbose=False)

            # Kiểm tra sales_count
            sales_count = detail.get("sales_count")
            product_id = detail.get("product_id")
            name = detail.get("name", "N/A")

            result = {
                "url": url,
                "product_id": product_id,
                "name": name,
                "sales_count": sales_count,
                "has_sales_count": sales_count is not None,
                "price": detail.get("price", {}),
                "rating": detail.get("rating", {}),
            }

            results.append(result)

            print("   📊 Kết quả:")
            print(f"      - Product ID: {product_id}")
            print(f"      - Tên: {name[:60]}...")
            sales_str = (
                f"{sales_count:,}"
                if isinstance(sales_count, (int, float))
                else (str(sales_count) if sales_count is not None else "N/A")
            )
            print(f"      - Sales count: {sales_str}")
            print(f"      - Có sales_count: {'✅ CÓ' if sales_count is not None else '❌ KHÔNG'}")
            if detail.get("price", {}).get("current_price"):
                print(f"      - Giá: {detail.get('price', {}).get('current_price'):,} VND")
            if detail.get("rating", {}).get("average"):
                print(
                    f"      - Đánh giá: {detail.get('rating', {}).get('average')}/5 ({detail.get('rating', {}).get('total_reviews')} reviews)"
                )

        except Exception as e:
            print(f"   ❌ Lỗi: {e}")
            import traceback

            traceback.print_exc()
            results.append({"url": url, "error": str(e), "has_sales_count": False})

    # Thống kê tổng
    print("\n📊 Thống kê tổng:")
    total = len(results)
    with_sales = len([r for r in results if r.get("has_sales_count")])
    without_sales = total - with_sales

    print(f"   - Tổng số products test: {total}")
    print(
        f"   - Có sales_count: {with_sales} ({with_sales / total * 100:.1f}%)"
        if total > 0
        else "   - Có sales_count: 0"
    )
    print(
        f"   - Không có sales_count: {without_sales} ({without_sales / total * 100:.1f}%)"
        if total > 0
        else "   - Không có sales_count: 0"
    )

    # Lưu kết quả
    output_file = "data/test_output/test_product_detail_sales_count.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "total_tested": total,
                "with_sales_count": with_sales,
                "without_sales_count": without_sales,
                "results": results,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"\n💾 Đã lưu kết quả vào: {output_file}")

    return results


def main():
    """
    Chạy tất cả tests.
    """
    print("=" * 70)
    print("🧪 TEST SALES COUNT - Kiểm tra khả năng lấy số lượng đã bán")
    print("=" * 70)

    # Test 1: Crawl products từ categories
    products = test_crawl_products_sales_count()

    # Test 2: Crawl product detail
    detail_results = test_crawl_product_detail_sales_count()

    # Tổng kết
    print("\n" + "=" * 70)
    print("📋 TỔNG KẾT")
    print("=" * 70)

    if products:
        products_with_sales = len([p for p in products if p.get("sales_count") is not None])
        print("✅ Test 1 (Crawl Products):")
        print(f"   - Tổng: {len(products)} products")
        print(
            f"   - Có sales_count: {products_with_sales} ({products_with_sales / len(products) * 100:.1f}%)"
        )

    if detail_results:
        detail_with_sales = len([r for r in detail_results if r.get("has_sales_count")])
        print("\n✅ Test 2 (Crawl Product Detail):")
        print(f"   - Tổng: {len(detail_results)} products")
        print(
            f"   - Có sales_count: {detail_with_sales} ({detail_with_sales / len(detail_results) * 100:.1f}%)"
        )

    print("\n" + "=" * 70)
    print("✅ Hoàn thành test!")
    print("=" * 70)


if __name__ == "__main__":
    main()
