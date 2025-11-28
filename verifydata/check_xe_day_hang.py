import psycopg2
import json

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

# Tìm sản phẩm có category_path liên quan đến 'Xe đẩy hàng'
cur.execute("""
    SELECT 
        id,
        name,
        category_path,
        jsonb_array_length(category_path) as path_length
    FROM products
    WHERE category_path::text LIKE '%Xe day hang%'
    ORDER BY id;
""")

results = cur.fetchall()
print(f'Found {len(results)} products with Xe day hang in category_path:\n')

for id, name, cat_path, path_len in results[:30]:
    print(f'ID: {id}')
    print(f'Name: {name[:60]}...' if len(name) > 60 else f'Name: {name}')
    print(f'Category Path ({path_len} levels): {cat_path}')
    print()

if len(results) > 30:
    print(f'\n... and {len(results) - 30} more products')

cur.close()
conn.close()
