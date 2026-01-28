#!/usr/bin/env python
"""
Tự động fix category_path trong database - đảm bảo parent category (Level 0) luôn ở index 0

Script này sẽ:
1. Tìm tất cả products có category_path mà không có parent category ở index 0
2. Tự động fix bằng cách prepend parent category vào đầu
3. Cập nhật database với category_path đã được fix

Có thể chạy tự động hoặc manual để fix dữ liệu đã có.
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.pipelines.crawl.crawl_products_detail import (
    load_category_hierarchy,
)
from src.pipelines.crawl.validate_category_path import (
    PARENT_CATEGORY_NAME,
    validate_and_fix_category_path,
)

load_dotenv()


def get_db_connection():
    """
    Connect to crawl_data database.
    """
    import psycopg2

    # Try multiple connection options
    connections = [
        # Option 1: From .env (Docker)
        {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "database": os.getenv("POSTGRES_DB", "crawl_data"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
        },
        # Option 2: Docker service name
        {
            "host": "postgres",
            "database": "crawl_data",
            "user": "postgres",
            "password": "postgres",
            "port": 5432,
        },
        # Option 3: Local
        {
            "host": "localhost",
            "database": "crawl_data",
            "user": "postgres",
            "password": "postgres",
            "port": 5432,
        },
    ]

    for i, conn_info in enumerate(connections):
        try:
            print(f"  Trying connection {i + 1}: {conn_info['host']}:{conn_info['port']}")
            conn = psycopg2.connect(**conn_info)
            print("  ✓ Connected successfully")
            return conn
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            continue

    raise Exception("Could not connect to any database")


def analyze_products_needing_fix(cur, hierarchy_map, limit=None):
    """Phân tích products cần fix.

    Returns:
        list: Danh sách products cần fix với old_path và new_path
    """
    # Build name -> url lookup
    {info.get("name"): url for url, info in hierarchy_map.items()}

    # Query: Lấy tất cả products có category_path
    query = """
        SELECT id, product_id, category_path, category_url, name
        FROM products
        WHERE category_path IS NOT NULL
        AND jsonb_array_length(category_path) > 0
        ORDER BY id
    """
    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    products = cur.fetchall()

    need_fix = []
    already_ok = []

    print(f"\n📊 Analyzing {len(products)} products...")

    for product_id, product_id_str, cat_path_json, category_url, name in products:
        # Parse category_path
        if isinstance(cat_path_json, str):
            cat_path = json.loads(cat_path_json)
        else:
            cat_path = cat_path_json

        if not cat_path or len(cat_path) == 0:
            continue

        # Validate và fix
        fixed_path = validate_and_fix_category_path(
            category_path=cat_path,
            category_url=category_url,
            hierarchy_map=hierarchy_map,
            auto_fix=True,
        )

        # Kiểm tra xem có cần fix không
        if fixed_path != cat_path:
            need_fix.append(
                {
                    "id": product_id,
                    "product_id": product_id_str,
                    "name": name,
                    "old_path": cat_path,
                    "new_path": fixed_path,
                    "category_url": category_url,
                }
            )
        else:
            already_ok.append(product_id)

    return need_fix, already_ok


def main():
    print("=" * 80)
    print("AUTO FIX CATEGORY_PATH - Đảm bảo parent category ở index 0")
    print("=" * 80)

    # Load hierarchy
    print("\n📖 Loading category hierarchy...")
    try:
        hierarchy_map = load_category_hierarchy()
        if not hierarchy_map:
            print("❌ ERROR: Could not load hierarchy map")
            return
        print(f"✓ Loaded {len(hierarchy_map)} categories")
    except Exception as e:
        print(f"❌ ERROR: Could not load hierarchy map: {e}")
        return

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("✓ Connected to crawl_data database")
    except Exception as e:
        print(f"❌ ERROR: Could not connect to database: {e}")
        return

    # Analyze products
    print("\n🔍 Analyzing products...")
    try:
        # Có thể giới hạn số lượng để test trước
        need_fix, already_ok = analyze_products_needing_fix(
            cur,
            hierarchy_map,
            limit=None,  # None = không giới hạn
        )
    except Exception as e:
        print(f"❌ ERROR analyzing products: {e}")
        import traceback

        traceback.print_exc()
        cur.close()
        conn.close()
        return

    print("\n📈 Analysis Results:")
    print(f"  • Products OK (không cần fix): {len(already_ok)}")
    print(f"  • Products cần fix: {len(need_fix)}")

    if len(need_fix) == 0:
        print("\n✅ Không có products nào cần fix!")
        cur.close()
        conn.close()
        return

    # Show sample
    print("\n📋 Sample products cần fix (first 5):")
    for item in need_fix[:5]:
        print(f"\n  ID {item['id']}: {item['product_id']}")
        print(f"    Name: {item['name'][:60] if item['name'] else 'N/A'}...")
        print(f"    Old: {item['old_path']}")
        print(f"    New: {item['new_path']}")

    # Ask for confirmation
    print(f"\n⚠️  Sẽ update {len(need_fix)} products")
    response = input("Tiếp tục? (yes/no): ").strip().lower()

    if response != "yes":
        print("❌ Đã hủy")
        cur.close()
        conn.close()
        return

    # Update database
    print(f"\n🔧 Updating {len(need_fix)} products...")
    updated = 0
    errors = 0

    for item in need_fix:
        try:
            new_path_json = json.dumps(item["new_path"], ensure_ascii=False)
            cur.execute(
                """
                UPDATE products
                SET category_path = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (new_path_json, item["id"]),
            )
            updated += 1

            if updated % 100 == 0:
                print(f"  • Đã update {updated}/{len(need_fix)} products...")
        except Exception as e:
            print(f"  ❌ Error updating ID {item['id']}: {e}")
            errors += 1

    # Commit changes
    print("\n💾 Committing changes...")
    try:
        conn.commit()
        print(f"✅ Đã update {updated} products thành công")
        if errors > 0:
            print(f"⚠️  {errors} errors")
    except Exception as e:
        print(f"❌ ERROR committing changes: {e}")
        conn.rollback()

    # Verify
    print("\n✅ Verification...")
    cur.execute(
        """
        SELECT jsonb_array_length(category_path) as levels, COUNT(*) as count
        FROM products
        WHERE category_path IS NOT NULL
        GROUP BY jsonb_array_length(category_path)
        ORDER BY levels
    """
    )

    distribution = cur.fetchall()
    print("Distribution of category levels sau khi fix:")
    for levels, count in distribution:
        print(f"  {levels} levels: {count:,} products")

    # Check parent category coverage
    print("\n📊 Kiểm tra parent category coverage...")
    cur.execute(
        """
        SELECT COUNT(*) as total,
               SUM(CASE WHEN category_path->>0 = %s THEN 1 ELSE 0 END) as has_parent
        FROM products
        WHERE category_path IS NOT NULL
        AND jsonb_array_length(category_path) > 0
    """,
        (PARENT_CATEGORY_NAME,),
    )

    total, has_parent = cur.fetchone()
    if total > 0:
        coverage = (has_parent / total) * 100
        print(f"  • Total products: {total:,}")
        print(f"  • Có parent category ở index 0: {has_parent:,} ({coverage:.1f}%)")
        if coverage < 100:
            print(f"  ⚠️  Vẫn còn {total - has_parent:,} products chưa có parent category")

    # Show sample of fixed data
    print("\n📋 Sample products sau khi fix:")
    cur.execute(
        """
        SELECT product_id, name, category_path
        FROM products
        WHERE category_path IS NOT NULL
        AND jsonb_array_length(category_path) >= 4
        AND category_path->>0 = %s
        LIMIT 3
    """,
        (PARENT_CATEGORY_NAME,),
    )

    for product_id, name, cat_path in cur.fetchall():
        path = json.loads(cat_path) if isinstance(cat_path, str) else cat_path
        print(f"\n  {product_id}: {name[:50] if name else 'N/A'}...")
        print(f"  Path ({len(path)} levels): {' → '.join(path[:3])}...")

    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("✅ HOÀN TẤT")
    print("=" * 80)


if __name__ == "__main__":
    main()
