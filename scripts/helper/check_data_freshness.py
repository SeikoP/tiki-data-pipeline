"""
Script để kiểm tra dữ liệu có được refresh hay không

Kiểm tra:
1. Xem products có được crawl lại (không dùng cache)
2. So sánh dữ liệu trước và sau khi chạy DAG
3. Kiểm tra updated_at timestamp
"""

import json
import os
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# Đường dẫn files
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
PRODUCTS_WITH_DETAIL_FILE = PROJECT_ROOT / "data" / "raw" / "products" / "products_with_detail.json"

# Database connection
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "crawl_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),  # trufflehog:ignore
}


def get_products_from_db(product_ids: list[str]) -> dict[str, dict]:
    """Lấy products từ database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            placeholders = ",".join(["%s"] * len(product_ids))
            cur.execute(
                f"""
                SELECT
                    product_id, name, price, sales_count,
                    updated_at
                FROM products
                WHERE product_id IN ({placeholders})
                """,
                product_ids,
            )
            results = cur.fetchall()
            return {row["product_id"]: dict(row) for row in results}
    except Exception as e:
        print(f"❌ Lỗi khi kết nối database: {e}")
        return {}
    finally:
        if conn:
            conn.close()


def check_data_freshness():
    """Kiểm tra dữ liệu có được refresh hay không"""
    print("=" * 70)
    print("🔍 KIỂM TRA DATA FRESHNESS")
    print("=" * 70)

    # Đọc products_with_detail.json
    if not PRODUCTS_WITH_DETAIL_FILE.exists():
        print(f"❌ Không tìm thấy file: {PRODUCTS_WITH_DETAIL_FILE}")
        return

    print(f"\n📖 Đọc file: {PRODUCTS_WITH_DETAIL_FILE}")
    with open(PRODUCTS_WITH_DETAIL_FILE, encoding="utf-8") as f:
        data = json.load(f)

    products = data.get("products", [])
    stats = data.get("stats", {})
    crawled_at = data.get("crawled_at")

    print("\n📊 Thống kê từ file:")
    print(f"   - Tổng products: {len(products)}")
    print(f"   - Cached: {stats.get('cached', 0)}")
    print(f"   - Success: {stats.get('with_detail', 0)}")
    print(f"   - Crawled at: {crawled_at}")

    # Kiểm tra cache usage
    cached_count = stats.get("cached", 0)
    if cached_count > 0:
        print(f"\n⚠️  CẢNH BÁO: Có {cached_count} products dùng cache (không được crawl lại)")
        print("   → Để force refresh, set Airflow Variable: TIKI_FORCE_REFRESH_CACHE = true")
    else:
        print("\n✅ Tất cả products đều được crawl lại (không dùng cache)")

    # Lấy product_ids
    product_ids = [p.get("product_id") for p in products if p.get("product_id")]
    if not product_ids:
        print("\n❌ Không có product_id nào trong file")
        return

    print(f"\n🔍 Đang kiểm tra {len(product_ids)} products trong database...")
    db_products = get_products_from_db(product_ids)

    if not db_products:
        print("❌ Không tìm thấy products trong database")
        return

    print(f"\n✅ Tìm thấy {len(db_products)} products trong database")

    # So sánh dữ liệu
    print("\n" + "=" * 70)
    print("📊 SO SÁNH DỮ LIỆU")
    print("=" * 70)

    for product in products[:10]:  # Chỉ hiển thị 10 đầu tiên
        product_id = product.get("product_id")
        if not product_id:
            continue

        db_product = db_products.get(product_id)
        if not db_product:
            print(f"\n❌ Product {product_id}: Không có trong DB")
            continue

        # So sánh các field quan trọng
        file_price = product.get("price", {}).get("current_price")
        db_price = db_product.get("price")

        file_sales = product.get("sales_count")
        db_sales = db_product.get("sales_count")

        updated_at = db_product.get("updated_at")

        print(f"\n📦 Product {product_id}:")
        print(f"   - Name: {product.get('name', '')[:50]}...")
        print(f"   - Price: File={file_price}, DB={db_price}, Match={file_price == db_price}")
        print(f"   - Sales: File={file_sales}, DB={db_sales}, Match={file_sales == db_sales}")
        print(f"   - Updated: {updated_at}")

        # Kiểm tra nếu updated_at gần đây (trong 1 giờ)
        if updated_at:
            try:
                if isinstance(updated_at, str):
                    updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                else:
                    updated_dt = updated_at

                time_diff = (datetime.now(updated_dt.tzinfo) - updated_dt).total_seconds()
                if time_diff < 3600:  # 1 giờ
                    print(f"   ✅ Updated gần đây ({time_diff/60:.1f} phút trước)")
                else:
                    print(f"   ⚠️  Updated lâu rồi ({time_diff/3600:.1f} giờ trước)")
            except Exception as e:
                print(f"   ⚠️  Không thể parse updated_at: {e}")

    print("\n" + "=" * 70)
    print("💡 KHUYẾN NGHỊ")
    print("=" * 70)
    print("1. Nếu muốn force refresh (bỏ qua cache):")
    print("   - Set Airflow Variable: TIKI_FORCE_REFRESH_CACHE = true")
    print("   - Chạy lại DAG")
    print("2. Nếu muốn có products mới (INSERT thay vì UPDATE):")
    print("   - Cần crawl products với product_id chưa có trong DB")
    print("3. UPDATE vẫn là dữ liệu mới nếu được crawl lại")
    print("   - Kiểm tra updated_at để xác nhận")


if __name__ == "__main__":
    check_data_freshness()
