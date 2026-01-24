#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để kiểm tra parent category và đề xuất giải pháp
"""

import os
import sys
import json
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


def check_category_in_json(json_file, category_url):
    """Kiểm tra category có trong file JSON không"""
    if not os.path.exists(json_file):
        return None
    
    try:
        with open(json_file, encoding="utf-8") as f:
            categories = json.load(f)
        
        for cat in categories:
            if cat.get("url") == category_url:
                return cat
        return None
    except Exception as e:
        print(f"⚠️  Lỗi đọc file JSON: {e}")
        return None


def main():
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Lấy category có vấn đề
            cur.execute("""
                SELECT url, name, parent_url, category_path, level
                FROM categories
                WHERE url = 'https://tiki.vn/vat-pham-phong-thuy/c5848'
            """)
            cat = cur.fetchone()
            
            if not cat:
                print("❌ Không tìm thấy category")
                return
            
            print("=" * 80)
            print("📌 CATEGORY CÓ VẤN ĐỀ:")
            print("=" * 80)
            print(f"  Name: {cat['name']}")
            print(f"  URL: {cat['url']}")
            print(f"  Parent URL: {cat['parent_url']}")
            print(f"  Category Path: {cat['category_path']}")
            print(f"  Level: {cat['level']}")
            print()
            
            # Kiểm tra parent có trong DB không
            parent_url = cat['parent_url']
            if parent_url:
                cur.execute("SELECT * FROM categories WHERE url = %s", (parent_url,))
                parent_in_db = cur.fetchone()
                
                if parent_in_db:
                    print("✅ Parent category CÓ trong DB:")
                    print(f"   Name: {parent_in_db['name']}")
                    print(f"   URL: {parent_in_db['url']}")
                    print(f"   Parent URL: {parent_in_db['parent_url']}")
                    print(f"   Category Path: {parent_in_db['category_path']}")
                else:
                    print(f"❌ Parent category KHÔNG có trong DB: {parent_url}")
                    
                    # Kiểm tra trong file JSON
                    json_files = [
                        project_root / "data" / "raw" / "categories_recursive_optimized.json",
                        project_root / "data" / "raw" / "categories_recursive.json",
                        project_root / "data" / "raw" / "categories.json",
                    ]
                    
                    parent_found = False
                    for json_file in json_files:
                        parent_cat = check_category_in_json(json_file, parent_url)
                        if parent_cat:
                            print(f"✅ Tìm thấy parent trong file: {json_file.name}")
                            print(f"   Name: {parent_cat.get('name')}")
                            print(f"   URL: {parent_cat.get('url')}")
                            print(f"   Parent URL: {parent_cat.get('parent_url')}")
                            print(f"   Level: {parent_cat.get('level')}")
                            parent_found = True
                            break
                    
                    if not parent_found:
                        print("❌ Parent category KHÔNG có trong file JSON")
                        print("   Cần kiểm tra lại file JSON hoặc parent URL có đúng không")
            
            # Kiểm tra tất cả categories trong DB
            print("\n" + "=" * 80)
            print("📊 TẤT CẢ CATEGORIES TRONG DB:")
            print("=" * 80)
            cur.execute("SELECT url, name, parent_url, level FROM categories ORDER BY level, name")
            all_cats = cur.fetchall()
            for c in all_cats:
                print(f"  [{c['level']}] {c['name']}")
                print(f"      URL: {c['url']}")
                if c['parent_url']:
                    print(f"      Parent: {c['parent_url']}")
                print()
            
    finally:
        conn.close()


if __name__ == "__main__":
    main()
