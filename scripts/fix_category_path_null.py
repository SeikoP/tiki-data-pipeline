#!/usr/bin/env python3
"""
Fix NULL category_path in products table

When categories table has data but category_path is NULL,
this script will:
1. Replace NULL category_path with '[]' (empty JSONB array)
2. Add DEFAULT constraint to prevent future NULLs
3. Verify results
"""

import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def fix_category_path_null():
    """Fix NULL category_path values in products table"""
    
    # Get DB connection info from environment or defaults
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", 5432))
    db_name = os.getenv("POSTGRES_DB", "crawl_data")
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_password = os.getenv("POSTGRES_PASSWORD", "airflow")
    
    print("=" * 70)
    print("🔧 FIX: category_path NULL values in products table")
    print("=" * 70)
    print(f"📌 Connecting to: {db_user}@{db_host}:{db_port}/{db_name}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        print("✅ Connected to database\n")
        
        # Step 1: Check current state BEFORE fix
        print("📊 Step 1: Check state BEFORE fix...")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE category_path IS NULL) as null_count,
                COUNT(*) FILTER (WHERE category_path IS NOT NULL) as non_null_count
            FROM products;
        """)
        total, null_count, non_null_count = cur.fetchone()
        print(f"   Total products: {total}")
        print(f"   ❌ NULL category_path: {null_count}")
        print(f"   ✅ Non-NULL: {non_null_count}\n")
        
        if null_count == 0:
            print("✨ No NULL values found! Database is already clean.\n")
            cur.close()
            conn.close()
            return True
        
        # Step 2: Fix NULL values
        print("🔨 Step 2: Fixing NULL category_path values...")
        cur.execute("""
            UPDATE products 
            SET category_path = '[]'::jsonb 
            WHERE category_path IS NULL;
        """)
        updated_count = cur.rowcount
        print(f"   ✅ Updated {updated_count} rows\n")
        
        # Step 3: Add DEFAULT constraint
        print("🔨 Step 3: Adding DEFAULT constraint for future inserts...")
        cur.execute("""
            ALTER TABLE products 
            ALTER COLUMN category_path SET DEFAULT '[]'::jsonb;
        """)
        print("   ✅ DEFAULT constraint added\n")
        
        # Step 4: Verify fix AFTER
        print("📊 Step 4: Check state AFTER fix...")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE category_path IS NULL) as null_count,
                COUNT(*) FILTER (WHERE category_path = '[]'::jsonb) as empty_array_count,
                COUNT(*) FILTER (WHERE category_path IS NOT NULL AND 
                    category_path != '[]'::jsonb) as has_breadcrumb_count
            FROM products;
        """)
        result = cur.fetchone()
        total, null_count, empty_array_count, has_breadcrumb = result
        
        print(f"   Total products: {total}")
        print(f"   ✅ NULL count: {null_count} (should be 0)")
        print(f"   📦 Empty array ([]): {empty_array_count}")
        print(f"   📍 With breadcrumb data: {has_breadcrumb}\n")
        
        # Step 5: Show sample of fixed data
        print("📋 Step 5: Sample of fixed data...")
        cur.execute("""
            SELECT product_id, name, category_path 
            FROM products 
            LIMIT 5;
        """)
        samples = cur.fetchall()
        for product_id, name, cat_path in samples:
            path_str = "[]" if cat_path == [] else str(cat_path)[:40]
            print(f"   Product {product_id}: {name[:30]:30} → category_path: {path_str}")
        
        print("\n" + "=" * 70)
        if null_count == 0:
            print("✨ SUCCESS! All NULL values fixed!")
            print("=" * 70)
            print("\nChanges applied:")
            print(f"  ✅ Fixed {updated_count} NULL values")
            print(f"  ✅ Set DEFAULT constraint: category_path = '[]'::jsonb")
            print(f"  ✅ Now total_products={total}, null_count={null_count}")
        else:
            print("⚠️  WARNING! Some NULL values still remain.")
            print("=" * 70)
        
        cur.close()
        conn.close()
        
        return null_count == 0
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("=" * 70)
        return False


if __name__ == "__main__":
    success = fix_category_path_null()
    exit(0 if success else 1)
