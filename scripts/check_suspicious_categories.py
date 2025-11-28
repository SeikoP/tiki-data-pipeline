import psycopg2
from dotenv import load_dotenv
import os
import json

load_dotenv()
conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user=os.getenv('POSTGRES_USER', 'airflow'),
    password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    port=5432
)
cur = conn.cursor()

# Get all products and check L4 length
cur.execute('''
SELECT product_id, name, category_path, jsonb_array_length(category_path)
FROM products
ORDER BY product_id
''')

results = cur.fetchall()
suspicious = []

for pid, name, cat_path, path_len in results:
    if isinstance(cat_path, str):
        cat_path = json.loads(cat_path)
    
    # Check if L4 is suspiciously long (likely product name)
    if path_len == 4:
        l4 = cat_path[3]
        name_upper = name.upper()
        l4_upper = l4.upper()
        
        # If L4 length > 80 or L4 matches product name, it's likely wrong
        if len(l4) > 80 or (len(l4) > 50 and name_upper[:50] == l4_upper[:50]):
            suspicious.append({
                'id': pid,
                'name': name,
                'l4': l4,
                'l4_len': len(l4),
                'path': cat_path
            })

print(f'Total suspicious products: {len(suspicious)}\n')
for i, item in enumerate(suspicious[:15], 1):
    print(f'{i}. Product ID: {item["id"]}')
    print(f'   Product Name: {item["name"][:70]}...')
    print(f'   L4 (suspect): {item["l4"][:70]}... (len={item["l4_len"]})')
    print(f'   Full path: {item["path"]}')
    print()

if len(suspicious) > 15:
    print(f'... and {len(suspicious) - 15} more')

conn.close()
