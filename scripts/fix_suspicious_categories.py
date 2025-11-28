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
to_fix = []

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
            to_fix.append({
                'id': pid,
                'old_path': cat_path,
                'new_path': cat_path[:3]  # Keep only first 3 levels
            })

print(f'Found {len(to_fix)} products to fix\n')

# Update each product
updated = 0
for item in to_fix:
    cur.execute('''
    UPDATE products
    SET category_path = %s
    WHERE product_id = %s
    ''', (json.dumps(item['new_path']), item['id']))
    updated += 1
    
    if updated <= 5:
        print(f'✓ ID {item["id"]}: truncated from {len(item["old_path"])} → {len(item["new_path"])} levels')

if len(to_fix) > 5:
    print(f'... and {len(to_fix) - 5} more products')

conn.commit()

# Verify
cur.execute('''
SELECT COUNT(*), jsonb_array_length(category_path)
FROM products
GROUP BY jsonb_array_length(category_path)
''')

print(f'\n✓ Updated {updated} products')
print('\nFinal distribution:')
for count, length in sorted(cur.fetchall()):
    print(f'  {length} levels: {count} products')

conn.close()
