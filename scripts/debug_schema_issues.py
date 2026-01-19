"""
Debug script để kiểm tra vấn đề categories và crawl_history
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipelines.crawl.storage.postgres_storage import PostgresStorage

def debug_categories():
    """Debug categories loading issue"""
    print("=" * 60)
    print("🔍 DEBUGGING CATEGORIES")
    print("=" * 60)
    
    storage = PostgresStorage()
    
    with storage.get_connection() as conn:
        cur = conn.cursor()
        
        # Check products count
        cur.execute("SELECT COUNT(*) FROM products")
        products_count = cur.fetchone()[0]
        print(f"✓ Products count: {products_count}")
        
        # Check categories count
        cur.execute("SELECT COUNT(*) FROM categories")
        categories_count = cur.fetchone()[0]
        print(f"✓ Categories count: {categories_count}")
        
        # Check unique category_ids in products
        cur.execute("SELECT COUNT(DISTINCT category_id) FROM products WHERE category_id IS NOT NULL")
        unique_cats = cur.fetchone()[0]
        print(f"✓ Unique category_ids in products: {unique_cats}")
        
        # Test get_used_category_ids()
        used_ids = storage.get_used_category_ids()
        print(f"✓ get_used_category_ids() returned: {len(used_ids)} IDs")
        if used_ids:
            print(f"  Sample IDs: {list(used_ids)[:5]}")
        
        # Check if categories have product_count updated
        cur.execute("SELECT COUNT(*) FROM categories WHERE product_count > 0")
        cats_with_products = cur.fetchone()[0]
        print(f"✓ Categories with product_count > 0: {cats_with_products}")
        
        print("\n📊 Analysis:")
        if products_count == 0:
            print("  ⚠️  NO PRODUCTS! Categories will load all (fallback behavior)")
        elif unique_cats == 0:
            print("  ⚠️  Products have NULL category_id!")
        elif categories_count > unique_cats * 3:
            print(f"  ⚠️  Too many categories ({categories_count}) vs products ({unique_cats})")
            print("     → Smart filtering may not be working")
        else:
            print("  ✅ Categories count looks reasonable")

def debug_crawl_history():
    """Debug crawl_history empty issue"""
    print("\n" + "=" * 60)
    print("🔍 DEBUGGING CRAWL_HISTORY")
    print("=" * 60)
    
    storage = PostgresStorage()
    
    with storage.get_connection() as conn:
        cur = conn.cursor()
        
        # Check crawl_history count
        cur.execute("SELECT COUNT(*) FROM crawl_history")
        history_count = cur.fetchone()[0]
        print(f"✓ Crawl history records: {history_count}")
        
        if history_count == 0:
            print("  ⚠️  NO CRAWL HISTORY!")
            print("  Possible reasons:")
            print("    1. _log_batch_crawl_history() not being called")
            print("    2. Products don't have price field")
            print("    3. Error during insert (check logs)")
            
            # Check if products have price
            cur.execute("SELECT COUNT(*) FROM products WHERE price IS NOT NULL")
            products_with_price = cur.fetchone()[0]
            print(f"\n  Products with price: {products_with_price}")
            
            if products_with_price == 0:
                print("  ❌ PROBLEM: No products have price!")
        else:
            # Show sample data
            cur.execute("""
                SELECT product_id, price, discount_amount, sales_count, 
                       is_flash_sale, crawl_type, crawled_at
                FROM crawl_history
                ORDER BY crawled_at DESC
                LIMIT 3
            """)
            print("\n  Sample records:")
            for row in cur.fetchall():
                print(f"    {row}")
            
            # Check for NULL fields
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN discount_amount IS NULL THEN 1 ELSE 0 END) as null_discount,
                    SUM(CASE WHEN sales_count IS NULL THEN 1 ELSE 0 END) as null_sales,
                    SUM(CASE WHEN is_flash_sale IS NULL THEN 1 ELSE 0 END) as null_flash_sale
                FROM crawl_history
            """)
            total, null_disc, null_sales, null_flash = cur.fetchone()
            print(f"\n  NULL fields analysis:")
            print(f"    Total records: {total}")
            print(f"    NULL discount_amount: {null_disc} ({null_disc/total*100:.1f}%)")
            print(f"    NULL sales_count: {null_sales} ({null_sales/total*100:.1f}%)")
            print(f"    NULL is_flash_sale: {null_flash} ({null_flash/total*100:.1f}%)")

if __name__ == "__main__":
    try:
        debug_categories()
        debug_crawl_history()
        print("\n" + "=" * 60)
        print("✅ Debug completed!")
        print("=" * 60)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
