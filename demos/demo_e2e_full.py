"""
Demo End-to-End: Chạy toàn bộ pipeline từ đầu đến cuối

Pipeline: Crawl -> Transform -> Load
"""

import os
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

# Import các modules
try:
    from pipelines.crawl.config import get_config
    from pipelines.crawl.crawl_products import crawl_category_products
    from pipelines.load.loader import DataLoader
    from pipelines.transform.transformer import DataTransformer
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Đảm bảo bạn đã cài đặt dependencies: pip install -r requirements.txt")
    sys.exit(1)


def main():
    print("=" * 80)
    print("🚀 DEMO END-TO-END PIPELINE")
    print("=" * 80)
    print()
    print("Pipeline sẽ chạy 3 bước:")
    print("  1. 📥 Crawl products từ Tiki.vn")
    print("  2. 🔄 Transform dữ liệu")
    print("  3. 💾 Load vào database")
    print()
    print("=" * 80)
    print()

    # ==========================================
    # BƯỚC 1: CRAWL
    # ==========================================
    print("📥 BƯỚC 1: CRAWL PRODUCTS")
    print("-" * 80)

    category_url = "https://tiki.vn/dien-thoai-may-tinh-bang/c1789"
    category_name = "Điện thoại & Máy tính bảng"

    print(f"📂 Danh mục: {category_name}")
    print(f"🔗 URL: {category_url}")
    print("⏳ Đang crawl...")

    products = []
    max_pages = 2  # Giới hạn để demo nhanh

    for page in range(1, max_pages + 1):
        print(f"   📄 Trang {page}/{max_pages}...", end=" ")
        page_products = crawl_category_products(
            category_url=category_url, page=page, max_products=20
        )
        if page_products:
            products.extend(page_products)
            print(f"✅ {len(page_products)} sản phẩm")
        else:
            print("⚠️  Không có sản phẩm")
            break

    if not products:
        print("❌ Không crawl được sản phẩm nào!")
        return

    print(f"✅ Đã crawl {len(products)} sản phẩm")
    print()

    # ==========================================
    # BƯỚC 2: TRANSFORM
    # ==========================================
    print("🔄 BƯỚC 2: TRANSFORM PRODUCTS")
    print("-" * 80)
    print("⏳ Đang transform...")

    transformer = DataTransformer(
        strict_validation=False, remove_invalid=True, normalize_fields=True
    )

    transformed_products, transform_stats = transformer.transform_products(products, validate=True)

    print(f"✅ Valid: {transform_stats['valid_products']}")
    print(f"❌ Invalid: {transform_stats['invalid_products']}")
    print(f"🔄 Duplicates removed: {transform_stats['duplicates_removed']}")
    print()

    if not transformed_products:
        print("❌ Không có products hợp lệ sau transform!")
        return

    # ==========================================
    # BƯỚC 3: LOAD
    # ==========================================
    print("💾 BƯỚC 3: LOAD TO DATABASE")
    print("-" * 80)

    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "crawl_data")
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow")

    print(f"🔌 Database: {db_host}:{db_port}/{db_name}")
    print("⏳ Đang load...")

    loader = DataLoader(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
        batch_size=100,
        enable_db=True,
    )

    try:
        output_dir = project_root / "data" / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "demo_e2e_products_final.json"

        load_stats = loader.load_products(
            transformed_products,
            save_to_file=str(output_file),
            upsert=True,
            validate_before_load=True,
        )

        print(f"✅ DB loaded: {load_stats['db_loaded']}")
        print(f"✅ File loaded: {load_stats['file_loaded']}")
        print(f"❌ Failed: {load_stats['failed_count']}")
        print()

    except Exception as e:
        print(f"⚠️  Lỗi khi load vào database: {e}")
        print("💡 Dữ liệu vẫn được lưu vào file JSON.")
    finally:
        loader.close()

    # ==========================================
    # TÓM TẮT
    # ==========================================
    print("=" * 80)
    print("🎉 PIPELINE HOÀN THÀNH!")
    print("=" * 80)
    print()
    print("📊 Thống kê:")
    print(f"   - Products crawled: {len(products)}")
    print(f"   - Products transformed: {len(transformed_products)}")
    print(f"   - Products loaded: {load_stats.get('file_loaded', 0)}")
    print()
    print("📁 Files đã tạo:")
    print(f"   - {output_file}")
    print()
    print("✅ Pipeline đã chạy thành công từ đầu đến cuối!")
    print("=" * 80)


if __name__ == "__main__":
    main()
