#!/usr/bin/env python
"""
Fix category_path data in database - add parent categories to 3-4 level paths
"""

import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.pipelines.crawl.config import MAX_CATEGORY_LEVELS
from src.pipelines.crawl.crawl_products_detail import (
    get_parent_category_name,
    load_category_hierarchy,
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


def add_parent_to_path(category_path, hierarchy_map, name_to_url):
    """
    Add parent category to beginning of path if missing and found in hierarchy.
    """
    if not category_path or len(category_path) == 0:
        return category_path

    # Skip if already has parent (>4 levels)
    if len(category_path) > 4:
        return category_path

    first_item = category_path[0]

    # Check if first item is a top-level category (already has parent)
    if len(category_path) >= 4:
        # Likely has parent already, skip
        return category_path

    # Try to find parent in hierarchy
    if first_item in name_to_url:
        cat_url = name_to_url[first_item]
        parent_name = get_parent_category_name(cat_url, hierarchy_map)

        if parent_name and parent_name != first_item:
            # Check if parent is already at the beginning
            if category_path[0] != parent_name:
                # Add parent to beginning
                new_path = [parent_name] + category_path
                # Ensure not exceeding MAX_CATEGORY_LEVELS
                if len(new_path) > MAX_CATEGORY_LEVELS:
                    new_path = new_path[:MAX_CATEGORY_LEVELS]
                return new_path

    return category_path


def main():
    print("=" * 80)
    print("FIX CATEGORY_PATH DATA IN DATABASE")
    print("=" * 80)

    # Load hierarchy
    print("\n📖 Loading category hierarchy...")
    hierarchy_map = load_category_hierarchy()
    if not hierarchy_map:
        print("❌ ERROR: Could not load hierarchy map")
        return

    # Build name -> url lookup
    name_to_url = {info.get("name"): url for url, info in hierarchy_map.items()}
    print(f"✓ Loaded {len(hierarchy_map)} categories, {len(name_to_url)} unique names")

    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("✓ Connected to crawl_data database")
    except Exception as e:
        print(f"❌ ERROR: Could not connect to database: {e}")
        return

    # Get products with 3-4 level paths
    print("\n📊 Analyzing products with 3-4 level paths...")
    cur.execute(
        """
        SELECT id, product_id, category_path, name
        FROM products
        WHERE category_path IS NOT NULL
        AND jsonb_array_length(category_path) >= 3
        AND jsonb_array_length(category_path) <= 4
        ORDER BY jsonb_array_length(category_path), id
        LIMIT 100000
    """
    )

    products = cur.fetchall()
    print(f"✓ Found {len(products)} products with 3-4 level paths")

    # Analyze which ones need fixing
    need_fix = []
    already_have_parent = []

    for product_id, product_id_str, cat_path_json, name in products:
        cat_path = json.loads(cat_path_json) if isinstance(cat_path_json, str) else cat_path_json

        # Check if first item is already a parent category (top-level)
        first_item = cat_path[0]

        # Try to find if first item has parent in hierarchy
        if first_item in name_to_url:
            cat_url = name_to_url[first_item]
            parent_name = get_parent_category_name(cat_url, hierarchy_map)

            if parent_name and parent_name != first_item:
                # This should have a parent
                need_fix.append(
                    {
                        "id": product_id,
                        "product_id": product_id_str,
                        "old_path": cat_path,
                        "parent": parent_name,
                        "new_path": [parent_name] + cat_path[:4],  # Keep max 5 levels
                    }
                )
            else:
                already_have_parent.append(product_id)
        else:
            # First item not found in hierarchy
            print(f"  ⚠️  '{first_item}' not in hierarchy (ID: {product_id})")

    print("\n📈 Analysis:")
    print(f"  • Need fixing: {len(need_fix)}")
    print(f"  • Already have parent: {len(already_have_parent)}")

    if len(need_fix) == 0:
        print("\n✅ No products need fixing!")
        cur.close()
        conn.close()
        return

    # Show sample
    print("\n📋 Sample products to fix (first 5):")
    for item in need_fix[:5]:
        print(f"\n  ID {item['id']}: {item['product_id']}")
        print(f"    Old: {item['old_path']}")
        print(f"    New: {item['new_path']}")

    # Ask for confirmation
    print(f"\n⚠️  This will update {len(need_fix)} products")
    response = input("Continue? (yes/no): ").strip().lower()

    if response != "yes":
        print("❌ Cancelled")
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
                    updated_at = NOW()
                WHERE id = %s
                """,
                (new_path_json, item["id"]),
            )
            updated += 1

            if updated % 100 == 0:
                print(f"  • Updated {updated} products...")
        except Exception as e:
            print(f"  ❌ Error updating ID {item['id']}: {e}")
            errors += 1

    # Commit changes
    print("\n💾 Committing changes...")
    conn.commit()
    print(f"✅ Updated {updated} products")
    if errors > 0:
        print(f"⚠️  {errors} errors")

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
    print("Distribution of category levels after fix:")
    for levels, count in distribution:
        print(f"  {levels} levels: {count} products")

    # Show sample of fixed data
    print("\n📋 Sample fixed products:")
    cur.execute(
        """
        SELECT product_id, name, category_path
        FROM products
        WHERE category_path IS NOT NULL
        AND jsonb_array_length(category_path) >= 4
        LIMIT 3
    """
    )

    for product_id, name, cat_path in cur.fetchall():
        path = json.loads(cat_path) if isinstance(cat_path, str) else cat_path
        print(f"\n  {product_id}: {name[:50]}")
        print(f"  Path ({len(path)} levels): {' → '.join(path)}")

    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("✅ COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
