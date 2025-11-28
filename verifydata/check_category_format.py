import psycopg2
import json

conn = psycopg2.connect('dbname=crawl_data user=postgres password=postgres host=localhost')
cur = conn.cursor()

print('=== KIỂM TRA FORMAT CATEGORY NAMES ===')
print()

# Check products - first level
cur.execute('''
SELECT DISTINCT category_path->0 as first_level
FROM products
WHERE category_path IS NOT NULL
LIMIT 20
''')

print('📦 Products - First level category names:')
rows = cur.fetchall()
for row in rows:
    if row[0]:
        print(f'  "{row[0]}"')

print()

# Check products - all levels with Nhà cửa
cur.execute('''
SELECT DISTINCT 
  category_path->>0 as level_0,
  category_path->>1 as level_1,
  category_path->>2 as level_2
FROM products
WHERE category_path->>0 ILIKE '%nhà%'
LIMIT 10
''')

print('Products with "Nhà cửa":')
for row in cur.fetchall():
    print(f'  L0: {row[0]}, L1: {row[1]}, L2: {row[2]}')

print()

# Check hierarchy map
try:
    with open('data/raw/category_hierarchy_map.json', 'r') as f:
        hierarchy = json.load(f)
    
    print('📂 Hierarchy Map - Categories with "Nhà cửa":')
    count = 0
    for url, cat_info in hierarchy.items():
        name = cat_info.get('name', '')
        if 'nhà' in name.lower():
            print(f'  "{name}"')
            count += 1
            if count >= 10:
                break
except Exception as e:
    print(f'⚠️  Could not load hierarchy map: {e}')
finally:
    cur.close()
    conn.close()
