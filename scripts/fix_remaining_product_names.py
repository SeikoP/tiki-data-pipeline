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

# Find all products where L4 matches product name (truncate to 3 levels)
cur.execute('''
SELECT product_id, name, category_path
FROM products
WHERE jsonb_array_length(category_path) >= 4
  AND UPPER(SUBSTRING(name, 1, 50)) = UPPER(SUBSTRING(category_path ->> 3, 1, 50))
''')

results = cur.fetchall()
print(f'Found {len(results)} products with L4 = product name\n')

to_fix = []
for pid, name, cat_path in results:
    if isinstance(cat_path, str):
        cat_path = json.loads(cat_path)
    
    to_fix.append({
        'id': pid,
        'old_path': cat_path,
        'new_path': cat_path[:3]  # Keep only first 3 levels
    })
    
    if len(to_fix) <= 5:
        print(f'✓ ID {pid}: Will truncate {len(cat_path)} → 3 levels')

if len(to_fix) > 5:
    print(f'... and {len(to_fix) - 5} more')

# Update each product
updated = 0
for item in to_fix:
    cur.execute('''
    UPDATE products
    SET category_path = %s
    WHERE product_id = %s
    ''', (json.dumps(item['new_path']), item['id']))
    updated += 1

conn.commit()

# Verify
cur.execute('''
SELECT jsonb_array_length(category_path), COUNT(*)
FROM products
GROUP BY jsonb_array_length(category_path)
ORDER BY jsonb_array_length(category_path)
''')

print(f'\n✓ Updated {updated} products')
print('\nFinal distribution:')
for length, count in cur.fetchall():
    print(f'  {length} levels: {count} products')

# Verify no more matches
cur.execute('''
SELECT COUNT(*) FROM products
WHERE jsonb_array_length(category_path) >= 4
  AND UPPER(SUBSTRING(name, 1, 50)) = UPPER(SUBSTRING(category_path ->> 3, 1, 50))
''')

remaining = cur.fetchone()[0]
if remaining == 0:
    print(f'\n✅ VERIFICATION: 0 products with L4 = product name')
else:
    print(f'\n⚠️  WARNING: {remaining} products still have L4 = product name')

conn.close()
