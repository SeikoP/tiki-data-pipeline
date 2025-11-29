"""
Demo Step 1: Crawl Products từ Tiki.vn

Bước này crawl danh sách sản phẩm từ các danh mục và lưu vào file JSON.
"""

import json
import sys
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Import crawl module
try:
    from pipelines.crawl.config import get_config
    from pipelines.crawl.crawl_products import crawl_category_products
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Đảm bảo bạn đã cài đặt dependencies: pip install -r requirements.txt")
    sys.exit(1)


def main():
    print("=" * 80)
    print("📥 DEMO STEP 1: CRAWL PRODUCTS")
    print("=" * 80)
    print()
    print("Bước này sẽ:")
    print("  1. Crawl danh sách sản phẩm từ một danh mục Tiki.vn")
    print("  2. Lưu kết quả vào file JSON")
    print()

    # Cấu hình
    config = get_config()

    # Demo: crawl một danh mục nhỏ (điện thoại)
    category_url = "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"
    category_name = "Điện thoại & Máy tính bảng"

    print(f"📂 Danh mục: {category_name}")
    print(f"🔗 URL: {category_url}")
    print()
    print("⏳ Đang crawl... (có thể mất vài phút)")
    print()

    try:
        # Crawl products từ danh mục (giới hạn 2 trang để demo nhanh)
        products = []
        max_pages = 2  # Giới hạn số trang để demo nhanh

        for page in range(1, max_pages + 1):
            print(f"   📄 Đang crawl trang {page}/{max_pages}...")
            page_products = crawl_category_products(
                category_url=category_url,
                page=page,
                max_products=20,  # Giới hạn 20 sản phẩm mỗi trang
            )
            if page_products:
                products.extend(page_products)
                print(f"   ✅ Đã crawl {len(page_products)} sản phẩm từ trang {page}")
            else:
                print(f"   ⚠️  Không có sản phẩm ở trang {page}")
                break

        if not products:
            print("❌ Không crawl được sản phẩm nào!")
            return

        print()
        print(f"✅ Đã crawl thành công {len(products)} sản phẩm!")
        print()

        # Lưu vào file
        output_dir = project_root / "data" / "raw" / "products"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "demo_products.json"

        output_data = {
            "crawled_at": str(Path(__file__).stat().st_mtime),
            "category": category_name,
            "category_url": category_url,
            "total_products": len(products),
            "products": products,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"💾 Đã lưu vào: {output_file}")
        print()
        print("📊 Thống kê:")
        print(f"   - Tổng số sản phẩm: {len(products)}")
        if products:
            print(f"   - Sản phẩm đầu tiên: {products[0].get('name', 'N/A')}")
            print(f"   - Product ID đầu tiên: {products[0].get('product_id', 'N/A')}")
        print()
        print("✅ Bước 1 hoàn thành! Chạy demo_step2_transform.py để tiếp tục.")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Lỗi khi crawl: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
