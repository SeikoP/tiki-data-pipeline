#!/usr/bin/env python3
"""
Ví dụ về cách lưu dữ liệu crawl vào PostgreSQL

Cách sử dụng:
1. Đảm bảo PostgreSQL đã chạy: docker compose up -d postgres
2. Chạy script này: python scripts/example_save_to_postgres.py
"""

import sys
from pathlib import Path

# Thêm src vào path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.crawl.storage.postgres_storage import PostgresStorage
from pipelines.crawl.utils import safe_read_json

# Ví dụ dữ liệu
example_categories = [
    {
        "category_id": "1789",
        "name": "Điện thoại",
        "url": "https://tiki.vn/dien-thoai-may-tinh-bang/c1789",
        "image_url": "https://...",
        "parent_url": None,
        "level": 1,
        "product_count": 1000,
    }
]

example_products = [
    {
        "product_id": "123456",
        "name": "iPhone 15 Pro Max",
        "url": "https://tiki.vn/iphone-15-pro-max/p123456",
        "image_url": "https://...",
        "category_url": "https://tiki.vn/dien-thoai-may-tinh-bang/c1789",
        "sales_count": 5000,
        "price": 29990000,
        "original_price": 32990000,
        "discount_percent": 9,
        "rating_average": 4.5,
        "review_count": 1200,
    }
]


def main():
    print("🔌 Kết nối đến PostgreSQL...")
    
    # Khởi tạo PostgresStorage
    # Tự động lấy credentials từ environment variables
    storage = PostgresStorage(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        database="crawl_data",
        user=os.getenv("POSTGRES_USER", "airflow_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )

    try:
        # 1. Lưu categories
        print("\n📁 Đang lưu categories...")
        categories_file = Path("data/raw/categories_recursive_optimized.json")
        if categories_file.exists():
            categories_data = safe_read_json(categories_file, [])
            if isinstance(categories_data, list):
                saved = storage.save_categories(categories_data, upsert=True)
                print(f"✅ Đã lưu {saved} categories vào database")
            else:
                print("⚠️  File categories không đúng format")
        else:
            print("⚠️  File categories không tồn tại, dùng dữ liệu mẫu")
            saved = storage.save_categories(example_categories, upsert=True)
            print(f"✅ Đã lưu {saved} categories mẫu vào database")

        # 2. Lưu products
        print("\n📦 Đang lưu products...")
        products_file = Path("data/raw/products/products.json")
        if products_file.exists():
            products_data = safe_read_json(products_file, {})
            if isinstance(products_data, dict) and "products" in products_data:
                products = products_data["products"]
                saved = storage.save_products(products, upsert=True, batch_size=100)
                print(f"✅ Đã lưu {saved} products vào database")
            else:
                print("⚠️  File products không đúng format")
        else:
            print("⚠️  File products không tồn tại, dùng dữ liệu mẫu")
            saved = storage.save_products(example_products, upsert=True)
            print(f"✅ Đã lưu {saved} products mẫu vào database")

        # 3. Xem thống kê
        print("\n📊 Thống kê database:")
        stats = storage.get_category_stats()
        for key, value in stats.items():
            print(f"  - {key}: {value}")

        # 4. Log crawl history
        print("\n📝 Ghi log crawl history...")
        log_id = storage.log_crawl_history(
            crawl_type="products",
            status="success",
            items_count=saved,
            started_at=None,
        )
        print(f"✅ Đã ghi log với ID: {log_id}")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        storage.close()
        print("\n✅ Hoàn tất!")


if __name__ == "__main__":
    import os
    main()

