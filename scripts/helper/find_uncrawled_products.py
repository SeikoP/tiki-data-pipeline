"""
Script để tìm products chưa có trong database

Kiểm tra:
1. Tổng số products trong file products.json
2. Số products đã có trong DB (có price và sales_count)
3. Số products chưa có trong DB
4. Đề xuất strategy để crawl products mới
"""

import json
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# Đường dẫn files
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
PRODUCTS_FILE = PROJECT_ROOT / "data" / "raw" / "products" / "products.json"

# Database connection
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "crawl_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def get_existing_product_ids(product_ids: list[str]) -> set[str]:
    """Lấy danh sách product_ids đã có trong DB (có price và sales_count)"""
    if not product_ids:
        return set()
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        existing_ids = set()
        
        with conn.cursor() as cur:
            # Chia nhỏ query nếu có quá nhiều product_ids
            for i in range(0, len(product_ids), 1000):
                batch_ids = product_ids[i : i + 1000]
                placeholders = ",".join(["%s"] * len(batch_ids))
                cur.execute(
                    f"""
                    SELECT product_id 
                    FROM products 
                    WHERE product_id IN ({placeholders})
                      AND price IS NOT NULL 
                      AND sales_count IS NOT NULL
                    """,
                    batch_ids,
                )
                existing_ids.update(row[0] for row in cur.fetchall())
        
        return existing_ids
    except Exception as e:
        print(f"❌ Lỗi khi kết nối database: {e}")
        return set()
    finally:
        if conn:
            conn.close()


def get_all_product_ids_in_db() -> set[str]:
    """Lấy tất cả product_ids trong DB (có price và sales_count)"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT product_id 
                FROM products 
                WHERE price IS NOT NULL 
                  AND sales_count IS NOT NULL
                """
            )
            return set(row[0] for row in cur.fetchall())
    except Exception as e:
        print(f"❌ Lỗi khi kết nối database: {e}")
        return set()
    finally:
        if conn:
            conn.close()


def analyze_products():
    """Phân tích products và đề xuất strategy"""
    print("=" * 70)
    print("🔍 PHÂN TÍCH PRODUCTS CHƯA CRAWL")
    print("=" * 70)

    # Đọc products.json
    if not PRODUCTS_FILE.exists():
        print(f"❌ Không tìm thấy file: {PRODUCTS_FILE}")
        return

    print(f"\n📖 Đọc file: {PRODUCTS_FILE}")
    with open(PRODUCTS_FILE, encoding="utf-8") as f:
        data = json.load(f)

    products = data.get("products", [])
    print(f"📊 Tổng số products trong file: {len(products)}")

    # Lấy product_ids
    product_ids = [p.get("product_id") for p in products if p.get("product_id")]
    print(f"📊 Số products có product_id: {len(product_ids)}")

    # Kiểm tra trong DB
    print(f"\n🔍 Đang kiểm tra trong database...")
    existing_ids = get_existing_product_ids(product_ids)
    print(f"✅ Tìm thấy {len(existing_ids)} products đã có trong DB (có price và sales_count)")

    # Products chưa có trong DB
    uncrawled_ids = set(product_ids) - existing_ids
    print(f"🆕 Số products chưa có trong DB: {len(uncrawled_ids)}")

    # Thống kê tổng trong DB
    print(f"\n🔍 Đang kiểm tra tổng số products trong DB...")
    all_db_ids = get_all_product_ids_in_db()
    print(f"📊 Tổng số products trong DB (có price và sales_count): {len(all_db_ids)}")

    # Phân tích
    print("\n" + "=" * 70)
    print("📊 PHÂN TÍCH")
    print("=" * 70)
    
    if len(uncrawled_ids) == 0:
        print("⚠️  TẤT CẢ PRODUCTS TRONG FILE ĐÃ CÓ TRONG DB!")
        print("\n💡 GIẢI PHÁP:")
        print("1. Crawl categories/products mới:")
        print("   - Chọn categories khác chưa được crawl")
        print("   - Hoặc tăng số lượng products từ mỗi category")
        print("2. Force refresh products cũ:")
        print("   - Set Airflow Variable: TIKI_FORCE_REFRESH_CACHE = true")
        print("   - Để crawl lại và update dữ liệu mới nhất")
        print("3. Kiểm tra categories:")
        print("   - Xem có categories nào chưa được crawl không")
        print("   - Hoặc có products mới được thêm vào categories không")
    else:
        print(f"✅ Có {len(uncrawled_ids)} products chưa có trong DB")
        print(f"   - Tỷ lệ: {len(uncrawled_ids)/len(product_ids)*100:.1f}% chưa crawl")
        print(f"   - Tỷ lệ: {len(existing_ids)/len(product_ids)*100:.1f}% đã crawl")
        
        print("\n💡 KHUYẾN NGHỊ:")
        print("1. Hệ thống sẽ tự động ưu tiên crawl products chưa có trong DB")
        print("2. Products đã có trong DB sẽ được skip (trừ khi force refresh)")
        print("3. Tiếp tục chạy DAG để crawl products mới")
        
        # Hiển thị một số product_ids chưa crawl
        if len(uncrawled_ids) > 0:
            print(f"\n📋 Một số products chưa crawl (hiển thị 10 đầu tiên):")
            for i, pid in enumerate(list(uncrawled_ids)[:10]):
                product = next((p for p in products if p.get("product_id") == pid), None)
                name = product.get("name", "N/A")[:60] if product else "N/A"
                print(f"   {i+1}. {pid}: {name}...")

    print("\n" + "=" * 70)
    print("📈 TỔNG KẾT")
    print("=" * 70)
    print(f"📦 Products trong file: {len(product_ids)}")
    print(f"✅ Đã có trong DB: {len(existing_ids)} ({len(existing_ids)/len(product_ids)*100:.1f}%)")
    print(f"🆕 Chưa có trong DB: {len(uncrawled_ids)} ({len(uncrawled_ids)/len(product_ids)*100:.1f}%)")
    print(f"📊 Tổng trong DB: {len(all_db_ids)} products")


if __name__ == "__main__":
    analyze_products()

