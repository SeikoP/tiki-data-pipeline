"""
Script để kiểm tra sự khác biệt giữa products và crawl_history
"""
import os
import psycopg2

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_NAME = os.getenv("POSTGRES_DB", "tiki")
DB_USER = os.getenv("POSTGRES_USER", "bungmoto")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "0946932602a")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def check_data_consistency():
    conn = psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )
    
    with conn.cursor() as cur:
        # 1. Count records in each table
        cur.execute("SELECT COUNT(*) FROM products")
        products_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM crawl_history")
        history_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT product_id) FROM crawl_history")
        history_unique_products = cur.fetchone()[0]
        
        print("=" * 60)
        print("DATA CONSISTENCY CHECK")
        print("=" * 60)
        print(f"Products table:           {products_count} records")
        print(f"Crawl_history table:      {history_count} records")
        print(f"Unique products in history: {history_unique_products}")
        print()
        
        # 2. Products WITHOUT history
        cur.execute("""
            SELECT COUNT(*) FROM products p
            WHERE NOT EXISTS (
                SELECT 1 FROM crawl_history h WHERE h.product_id = p.product_id
            )
        """)
        missing_history = cur.fetchone()[0]
        print(f"Products WITHOUT history:   {missing_history}")
        
        # 3. History WITHOUT products (orphans)
        cur.execute("""
            SELECT COUNT(*) FROM crawl_history h
            WHERE NOT EXISTS (
                SELECT 1 FROM products p WHERE p.product_id = h.product_id
            )
        """)
        orphan_history = cur.fetchone()[0]
        print(f"History WITHOUT products (orphans): {orphan_history}")
        
        print()
        print("-" * 60)
        
        if missing_history > 0:
            print(f"\n[!] {missing_history} products do not have price history.")
            print("    These were likely crawled before history tracking was added.")
            print("    Solution: Re-crawl these products or ignore (not critical).")
        
        if orphan_history > 0:
            print(f"\n[!] {orphan_history} history records have no matching product.")
            print("    These are orphan records (product was deleted but history remains).")
            print("    Solution: Delete orphan records with:")
            print("    DELETE FROM crawl_history WHERE product_id NOT IN (SELECT product_id FROM products);")
        
        if missing_history == 0 and orphan_history == 0:
            print("\n[OK] Data is consistent! Every product has history and vice versa.")
        
        print()
    
    conn.close()

if __name__ == "__main__":
    check_data_consistency()
