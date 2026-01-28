#!/usr/bin/env python3
"""Script để fix NULL values trong category_path column.

Khi categories có dữ liệu nhưng category_path bị NULL,
script này sẽ:
1. Replace NULL category_path với '[]' (empty array)
2. Add DEFAULT CONSTRAINT để ngăn chặn future NULLs
3. Verify kết quả
"""

import os
import sys

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def fix_category_path_null():
    """
    Fix NULL category_path values.
    """

    # Get DB connection info
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "crawl_data")
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow")

    try:
        # Connect to database
        print(f"📌 Kết nối tới database: {db_user}@{db_host}:{db_port}/{db_name}")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        print("✅ Đã kết nối database\n")

        # Step 1: Check current state
        print("📊 Bước 1: Kiểm tra state hiện tại...")
        cur.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE category_path IS NULL) as null_count,
                COUNT(*) FILTER (WHERE category_path IS NOT NULL) as non_null_count
            FROM products;
        """
        )
        total, null_count, non_null_count = cur.fetchone()
        print(f"   - Total products: {total}")
        print(f"   - category_path IS NULL: {null_count}")
        print(f"   - category_path IS NOT NULL: {non_null_count}")

        # Step 2: Fix NULL values
        if null_count > 0:
            print(f"\n📝 Bước 2: Fix {null_count} NULL values...")
            cur.execute(
                """
                UPDATE products
                SET category_path = '[]'::jsonb
                WHERE category_path IS NULL;
            """
            )
            updated_count = cur.rowcount
            print(f"   ✅ Đã update {updated_count} rows")
        else:
            print("\n✅ Không có NULL values cần fix")

        # Step 3: Verify results
        print("\n📊 Bước 3: Verify kết quả...")
        cur.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE category_path IS NULL) as null_count,
                COUNT(*) FILTER (WHERE category_path = '[]'::jsonb) as empty_array_count,
                COUNT(*) FILTER (WHERE category_path IS NOT NULL AND category_path != '[]'::jsonb) as populated_count
            FROM products;
        """
        )
        total, null_count, empty_array_count, populated_count = cur.fetchone()
        print(f"   - Total products: {total}")
        print(f"   - category_path IS NULL: {null_count} ✅")
        print(f"   - category_path = [] (empty): {empty_array_count}")
        print(f"   - category_path populated: {populated_count}")

        # Step 4: Add default constraint (optional - PostgreSQL doesn't require it for JSONB)
        print("\n📝 Bước 4: Kiểm tra table schema...")
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'products' AND column_name = 'category_path';
        """
        )
        result = cur.fetchone()
        if result:
            col_name, data_type, is_nullable = result
            print(f"   - Column: {col_name}")
            print(f"   - Type: {data_type}")
            print(f"   - Nullable: {is_nullable}")

        # Step 5: Create CHECK constraint to prevent future issues
        print("\n📝 Bước 5: Add CHECK constraint...")
        try:
            cur.execute(
                """
                ALTER TABLE products
                ADD CONSTRAINT check_category_path_not_null
                CHECK (category_path IS NOT NULL);
            """
            )
            print("   ✅ Đã add CHECK constraint")
        except psycopg2.Error as e:
            if "already exists" in str(e):
                print("   ✅ CHECK constraint đã tồn tại")
            else:
                print(f"   ⚠️  Lỗi add constraint: {e}")

        cur.close()
        conn.close()

        print("\n" + "=" * 70)
        print("✅ FIX HOÀN TẤT!")
        print("=" * 70)
        print("\n📈 Kết quả:")
        print(f"   - Total NULL values fixed: {null_count}")
        print(f"   - Empty arrays: {empty_array_count}")
        print(f"   - Populated arrays: {populated_count}")
        print(f"   - Total: {total}")

        return 0

    except psycopg2.OperationalError as e:
        print(f"\n❌ Lỗi kết nối database: {e}")
        print("\n💡 Hướng dẫn:")
        print("   - Kiểm tra PostgreSQL đang chạy")
        print("   - Kiểm tra .env file với credentials")
        print("   - Thử: python scripts/helper/fix_category_path_null.py")
        return 1

    except Exception as e:
        print(f"\n❌ Lỗi không xác định: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(fix_category_path_null())
