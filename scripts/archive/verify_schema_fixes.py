"""
Script để verify các fixes cho categories và crawl_history
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.crawl.storage.postgres_storage import PostgresStorage

def verify_categories():
    """Verify categories table fixes"""
    print("=" * 60)
    print("🔍 VERIFYING CATEGORIES FIXES")
    print("=" * 60)
    
    storage = PostgresStorage()
    
    with storage.get_connection() as conn:
        cur = conn.cursor()
        
        # Check categories count
        cur.execute("SELECT COUNT(*) FROM categories")
        categories_count = cur.fetchone()[0]
        print(f"✓ Total categories: {categories_count}")
        
        # Check leaf vs non-leaf
        cur.execute("SELECT COUNT(*) FROM categories WHERE is_leaf = true")
        leaf_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM categories WHERE is_leaf = false")
        non_leaf_count = cur.fetchone()[0]
        print(f"✓ Leaf categories: {leaf_count}")
        print(f"✓ Non-leaf (parent) categories: {non_leaf_count}")
        
        # Check categories with products
        cur.execute("SELECT COUNT(*) FROM categories WHERE product_count > 0")
        cats_with_products = cur.fetchone()[0]
        print(f"✓ Categories with products: {cats_with_products}")
        
        # Check category_id normalization
        cur.execute("""
            SELECT COUNT(*) FROM categories 
            WHERE category_id IS NOT NULL 
            AND NOT category_id LIKE 'c%'
        """)
        non_normalized = cur.fetchone()[0]
        if non_normalized > 0:
            print(f"⚠️  WARNING: {non_normalized} categories have non-normalized category_id")
        else:
            print(f"✓ All category_ids are normalized (c{{id}} format)")
        
        # Check used category IDs
        used_ids = storage.get_used_category_ids()
        print(f"✓ Used category IDs from products: {len(used_ids)}")
        
        # Check if all used category IDs are in categories table
        if used_ids:
            placeholders = ','.join(['%s'] * len(list(used_ids)[:100]))  # Limit to 100 for safety
            cur.execute(f"""
                SELECT COUNT(DISTINCT category_id) 
                FROM categories 
                WHERE category_id IN ({placeholders})
            """, list(used_ids)[:100])
            matched_count = cur.fetchone()[0]
            print(f"✓ Matched categories: {matched_count}/{min(len(used_ids), 100)}")
            
            if matched_count < min(len(used_ids), 100) * 0.9:
                print("⚠️  WARNING: Many used category IDs not found in categories table")
                print("   → This may indicate a filtering/matching issue")
            else:
                print("✅ Category matching looks good!")

def verify_crawl_history():
    """Verify crawl_history table fixes"""
    print("\n" + "=" * 60)
    print("🔍 VERIFYING CRAWL_HISTORY FIXES")
    print("=" * 60)
    
    storage = PostgresStorage()
    
    with storage.get_connection() as conn:
        cur = conn.cursor()
        
        # Check if table exists and has correct columns
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'crawl_history'
            ORDER BY ordinal_position
        """)
        columns = {row[0]: row[1] for row in cur.fetchall()}
        
        expected_columns = [
            'id', 'product_id', 'price', 'original_price', 'discount_percent',
            'discount_amount', 'price_change', 'price_change_percent',
            'previous_price', 'previous_original_price', 'previous_discount_percent',
            'sales_count', 'sales_change', 'is_flash_sale', 'crawl_type',
            'crawled_at', 'updated_at'
        ]
        
        print("✓ Checking schema columns:")
        missing_columns = []
        for col in expected_columns:
            if col in columns:
                print(f"  ✓ {col}: {columns[col]}")
            else:
                print(f"  ❌ {col}: MISSING")
                missing_columns.append(col)
        
        if missing_columns:
            print(f"\n⚠️  WARNING: Missing columns: {missing_columns}")
            print("   → Run _ensure_history_schema() to add them")
        else:
            print("\n✅ All expected columns exist!")
        
        # Check crawl_history count
        cur.execute("SELECT COUNT(*) FROM crawl_history")
        history_count = cur.fetchone()[0]
        print(f"\n✓ Total crawl_history records: {history_count}")
        
        if history_count == 0:
            print("⚠️  No crawl history records found")
            print("   → This may be normal if no products have been crawled yet")
            
            # Check if products exist
            cur.execute("SELECT COUNT(*) FROM products WHERE price IS NOT NULL")
            products_with_price = cur.fetchone()[0]
            print(f"   → Products with price: {products_with_price}")
            
            if products_with_price > 0:
                print("   ⚠️  Products exist but no history - history logging may not be working")
        else:
            # Show sample data
            cur.execute("""
                SELECT product_id, price, discount_amount, sales_count, 
                       is_flash_sale, crawl_type, crawled_at
                FROM crawl_history
                ORDER BY crawled_at DESC
                LIMIT 5
            """)
            print("\n  Sample records (latest 5):")
            for row in cur.fetchall():
                print(f"    {row}")
            
            # Check data quality
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_price,
                    SUM(CASE WHEN discount_amount IS NULL THEN 1 ELSE 0 END) as null_discount,
                    SUM(CASE WHEN sales_count IS NULL THEN 1 ELSE 0 END) as null_sales,
                    SUM(CASE WHEN crawl_type IS NULL THEN 1 ELSE 0 END) as null_type
                FROM crawl_history
            """)
            total, null_price, null_discount, null_sales, null_type = cur.fetchone()
            
            print(f"\n  Data quality:")
            print(f"    Total: {total}")
            if null_price > 0:
                print(f"    ⚠️  NULL price: {null_price} ({null_price/total*100:.1f}%)")
            if null_type > 0:
                print(f"    ⚠️  NULL crawl_type: {null_type} ({null_type/total*100:.1f}%)")
            
            if null_price == 0 and null_type == 0:
                print("    ✅ Required fields are populated")

if __name__ == "__main__":
    try:
        verify_categories()
        verify_crawl_history()
        print("\n" + "=" * 60)
        print("✅ Verification completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
