#!/usr/bin/env python3
"""
Script để apply database schema changes:
- Thêm category_id column vào products
- Thêm category_path column vào products
- Thêm category_path column vào categories
- Tạo indexes
"""

import os
import sys
from pathlib import Path

import psycopg2

# Import config từ DAG nếu có
try:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from pipelines.crawl.config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_USER,
    )
except ImportError:
    # Fallback to environment variables
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "crawl_data")


def apply_schema_changes():
    """Apply database schema changes"""
    print("=" * 70)
    print("🔧 APPLY SCHEMA CHANGES")
    print("=" * 70)

    try:
        # Kết nối đến database
        print(f"\n📡 Kết nối đến PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
        )
        cur = conn.cursor()
        print("✅ Kết nối thành công!")

        # Change 1: Add category_id to products
        print("\n📝 Bước 1: Thêm category_id column...")
        try:
            cur.execute(
                """
                ALTER TABLE products ADD COLUMN category_id VARCHAR(255);
            """
            )
            print("✅ Thêm category_id thành công")
        except psycopg2.Error as e:
            if "already exists" in str(e):
                print("ℹ️ Column category_id đã tồn tại")
            else:
                raise
        conn.commit()

        # Change 2: Add category_path to products
        print("📝 Bước 2: Thêm category_path column...")
        try:
            cur.execute(
                """
                ALTER TABLE products ADD COLUMN category_path JSONB;
            """
            )
            print("✅ Thêm category_path thành công")
        except psycopg2.Error as e:
            if "already exists" in str(e):
                print("ℹ️ Column category_path đã tồn tại")
            else:
                raise
        conn.commit()

        # Change 3: Add category_path to categories
        print("📝 Bước 3: Thêm category_path column vào categories...")
        try:
            cur.execute(
                """
                ALTER TABLE categories ADD COLUMN category_path JSONB;
            """
            )
            print("✅ Thêm category_path vào categories thành công")
        except psycopg2.Error as e:
            if "already exists" in str(e):
                print("ℹ️ Column category_path trong categories đã tồn tại")
            else:
                raise
        conn.commit()

        # Change 4: Create indexes
        print("📝 Bước 4: Tạo indexes...")
        indexes_to_create = [
            (
                "idx_products_category_id",
                """
                CREATE INDEX IF NOT EXISTS idx_products_category_id 
                ON products(category_id);
            """,
            ),
            (
                "idx_products_category_path",
                """
                CREATE INDEX IF NOT EXISTS idx_products_category_path 
                ON products USING GIN (category_path);
            """,
            ),
            (
                "idx_categories_category_path",
                """
                CREATE INDEX IF NOT EXISTS idx_categories_category_path 
                ON categories USING GIN (category_path);
            """,
            ),
        ]

        for index_name, index_sql in indexes_to_create:
            try:
                cur.execute(index_sql)
                print(f"  ✅ Tạo index {index_name} thành công")
            except psycopg2.Error as e:
                if "already exists" in str(e):
                    print(f"  ℹ️ Index {index_name} đã tồn tại")
                else:
                    print(f"  ⚠️ Lỗi tạo index {index_name}: {e}")
        conn.commit()

        # Statistics
        print("\n📊 Thống kê sau schema change:")

        # Count columns
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'products' AND column_name IN ('category_id', 'category_path')
        """
        )
        cols_count = cur.fetchone()[0]
        print(f"  ✅ Products có {cols_count}/2 columns mới (category_id, category_path)")

        # Count indexes
        cur.execute(
            """
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE tablename = 'products' AND indexname LIKE 'idx_products_category%'
        """
        )
        idx_count = cur.fetchone()[0]
        print(f"  ✅ Products có {idx_count} indexes mới")

        # Close connection
        cur.close()
        conn.close()

        print("\n" + "=" * 70)
        print("✅ Schema changes áp dụng thành công!")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    apply_schema_changes()
