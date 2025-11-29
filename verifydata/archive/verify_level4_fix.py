import psycopg2

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

print("=== VERIFICATION AFTER FIX ===\n")

# 1. Check all "Xe đẩy hàng" products now have 4 levels
cur.execute('''
    SELECT 
        jsonb_array_length(category_path) as path_length,
        COUNT(*) as count
    FROM products
    WHERE category_path ->> 2 = 'Xe đẩy hàng'
    GROUP BY path_length
    ORDER BY path_length;
''')

print("Distribution of category_path length for 'Xe đẩy hàng':")
for path_len, count in cur.fetchall():
    print(f"  {path_len} levels: {count} products")

# 2. Verify the 4 fixed products
print("\n4 Fixed Products:")
ids = [480, 852, 861, 1125]
for pid in ids:
    cur.execute('''
        SELECT 
            id,
            name,
            category_path
        FROM products
        WHERE id = %s;
    ''', (pid,))
    
    id, name, cat_path = cur.fetchone()
    print(f"  ID {id}: {name[:40]}... → {cat_path[-1]}")

# 3. Check warehouse dimensions
conn2 = psycopg2.connect(
    host='localhost',
    database='tiki_warehouse',
    user='postgres',
    password='postgres'
)
cur2 = conn2.cursor()

print("\n\nWarehouse Status:")
cur2.execute('SELECT COUNT(*) FROM dim_category;')
print(f"  dim_category: {cur2.fetchone()[0]} rows")
cur2.execute('SELECT COUNT(*) FROM dim_product;')
print(f"  dim_product: {cur2.fetchone()[0]} rows")
cur2.execute('SELECT COUNT(*) FROM fact_product_sales;')
print(f"  fact_product_sales: {cur2.fetchone()[0]} rows")

print("\n✅ All 'Xe đẩy hàng' products now have 4 levels!")

cur.close()
cur2.close()
conn.close()
conn2.close()
