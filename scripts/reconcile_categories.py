"""
Reconcile Categories Script

This script reconciles categories table by:
1. Updating auto-created category names (name = category_id) with real names from JSON
2. Removing orphan categories (no products)
3. Updating product_count accurately
4. Merging duplicate categories if any remain

Usage:
    python scripts/reconcile_categories.py
"""

import json
import os
import sys
from pathlib import Path

# Fix Unicode Output for Windows Console
import io
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Add src to path
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Load .env file manually
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"
if env_path.exists():
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            if "=" in line and not line.strip().startswith("#"):
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ[key] = value

# Check/Set default POSTGRES_HOST for local execution
if "POSTGRES_HOST" not in os.environ or os.environ["POSTGRES_HOST"] == "postgres":
    print("ℹ️  Overriding POSTGRES_HOST to 'localhost' for local execution")
    os.environ["POSTGRES_HOST"] = "localhost"

from pipelines.crawl.storage.postgres_storage import PostgresStorage


def reconcile_categories(categories_json_path: str | None = None):
    """
    Reconcile categories table with source data
    
    Args:
        categories_json_path: Path to categories JSON file (optional)
    """
    print("🔄 Starting category reconciliation...")
    
    storage = PostgresStorage()
    
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Step 1: Update auto-created category names
            print("\n📝 Step 1: Updating auto-created category names...")
            
            if categories_json_path and os.path.exists(categories_json_path):
                with open(categories_json_path, 'r', encoding='utf-8') as f:
                    categories_data = json.load(f)
                
                # Build lookup: category_id -> name
                id_to_name = {
                    cat.get('category_id'): cat.get('name')
                    for cat in categories_data
                    if cat.get('category_id') and cat.get('name')
                }
                
                # Update categories where name = category_id (auto-created)
                updated_count = 0
                for cat_id, name in id_to_name.items():
                    cur.execute("""
                        UPDATE categories
                        SET name = %s
                        WHERE category_id = %s AND name = category_id
                    """, (name, cat_id))
                    updated_count += cur.rowcount
                
                print(f"   ✅ Updated {updated_count} category names")
            else:
                print("   ⚠️  No categories JSON provided, skipping name updates")
            
            # Step 2: Remove orphan categories (no products)
            print("\n🧹 Step 2: Removing orphan categories...")
            
            cur.execute("""
                DELETE FROM categories
                WHERE is_leaf = TRUE
                  AND id NOT IN (
                      SELECT DISTINCT c.id
                      FROM categories c
                      INNER JOIN products p ON c.category_id = p.category_id
                  )
            """)
            orphan_count = cur.rowcount
            print(f"   ✅ Removed {orphan_count} orphan categories")
            
            # Step 3: Update product_count accurately
            print("\n📊 Step 3: Updating product counts...")
            
            cur.execute("""
                UPDATE categories c
                SET product_count = (
                    SELECT COUNT(*)
                    FROM products p
                    WHERE p.category_id = c.category_id
                )
                WHERE is_leaf = TRUE
            """)
            print(f"   ✅ Updated {cur.rowcount} category product counts")
            
            # Step 4: Find and report potential duplicates
            print("\n🔍 Step 4: Checking for duplicates...")
            
            cur.execute("""
                SELECT 
                    name,
                    COUNT(*) as count,
                    STRING_AGG(category_id::text, ', ') as ids
                FROM categories
                GROUP BY name
                HAVING COUNT(*) > 1
            """)
            
            duplicates = cur.fetchall()
            if duplicates:
                print(f"   ⚠️  Found {len(duplicates)} potential duplicate category names:")
                for name, count, ids in duplicates[:10]:  # Show first 10
                    print(f"      - '{name}': {count} entries ({ids})")
            else:
                print("   ✅ No duplicate category names found")
            
            # Step 5: Verify data integrity
            print("\n✅ Step 5: Verifying data integrity...")
            
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN is_leaf = TRUE THEN 1 END) as leaf_count,
                    COUNT(CASE WHEN is_leaf = FALSE THEN 1 END) as parent_count,
                    COUNT(CASE WHEN name = category_id THEN 1 END) as auto_named
                FROM categories
            """)
            
            stats = cur.fetchone()
            print(f"   Total categories: {stats[0]}")
            print(f"   Leaf categories: {stats[1]}")
            print(f"   Parent categories: {stats[2]}")
            print(f"   Auto-named (need update): {stats[3]}")
    
    print("\n✅ Category reconciliation completed!")


if __name__ == "__main__":
    # Default path to categories JSON
    default_path = Path(__file__).parent.parent / "data" / "raw" / "categories_recursive_optimized.json"
    
    # Allow command line arg override
    json_path = sys.argv[1] if len(sys.argv) > 1 else str(default_path)
    
    # Set POSTGRES_HOST for local execution
    if "POSTGRES_HOST" not in os.environ:
        print("ℹ️  Setting POSTGRES_HOST=localhost for local execution")
        os.environ["POSTGRES_HOST"] = "localhost"
    
    reconcile_categories(json_path if os.path.exists(json_path) else None)
