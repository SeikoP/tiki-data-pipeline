#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kiểm tra category có products không
"""

import os
import sys
from pathlib import Path

# Fix encoding cho Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("❌ Cần cài đặt psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def get_db_connection():
    """Kết nối đến database"""
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    if db_host == "postgres":
        db_host = "localhost"
    
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "tiki")
    db_user = os.getenv("POSTGRES_USER", "bungmoto")
    db_password = os.getenv("POSTGRES_PASSWORD", "0946932602a")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None


def main():
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Kiểm tra category
            cur.execute("""
                SELECT category_id, url, name, parent_url
                FROM categories
                WHERE url = 'https://tiki.vn/vat-pham-phong-thuy/c5848'
            """)
            cat = cur.fetchone()
            
            if not cat:
                print("❌ Không tìm thấy category")
                return
            
            print(f"📌 Category: {cat['name']}")
            print(f"   Category ID: {cat['category_id']}")
            print(f"   URL: {cat['url']}")
            print()
            
            # Kiểm tra products
            cur.execute("""
                SELECT COUNT(*) as count
                FROM products
                WHERE category_id = %s OR category_url = %s
            """, (cat['category_id'], cat['url']))
            result = cur.fetchone()
            product_count = result['count'] if result else 0
            
            print(f"📊 Products trong DB:")
            print(f"   Số lượng: {product_count}")
            
            if product_count > 0:
                cur.execute("""
                    SELECT product_id, name, category_id, category_url
                    FROM products
                    WHERE category_id = %s OR category_url = %s
                    LIMIT 5
                """, (cat['category_id'], cat['url']))
                products = cur.fetchall()
                print(f"   Sample products:")
                for p in products:
                    print(f"     - {p['name']} ({p['product_id']})")
            
            # Kiểm tra used_category_ids
            print(f"\n🔍 Used Category IDs:")
            cur.execute("""
                SELECT DISTINCT category_id
                FROM products
                WHERE category_id IS NOT NULL
            """)
            used_ids = [row['category_id'] for row in cur.fetchall()]
            print(f"   Tổng số: {len(used_ids)}")
            print(f"   Sample: {used_ids[:10]}")
            
            if cat['category_id'] in used_ids:
                print(f"   ✅ Category {cat['category_id']} CÓ trong used_category_ids")
            else:
                print(f"   ❌ Category {cat['category_id']} KHÔNG có trong used_category_ids")
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()
