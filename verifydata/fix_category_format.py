import psycopg2
import json

conn = psycopg2.connect('dbname=crawl_data user=postgres password=postgres host=localhost')
cur = conn.cursor()

print('=== FIX CATEGORY NAME FORMAT ===')
print()

# Define format mapping - convert lowercase to Title Case
format_map = {
    'Nhà cửa - đời sống': 'Nhà Cửa - Đời Sống',
    'nhà cửa - đời sống': 'Nhà Cửa - Đời Sống',
    'Điện thoại - Tablet': 'Điện Thoại - Tablet',
    'Laptop - Máy tính - Linh kiện': 'Laptop - Máy Tính - Linh Kiện',
}

print('🔍 Finding all unique first-level categories...')
cur.execute('''
SELECT DISTINCT category_path->>0 as level_0
FROM products
WHERE category_path IS NOT NULL
ORDER BY level_0
''')

all_categories = [row[0] for row in cur.fetchall() if row[0]]
print(f'Found {len(all_categories)} unique first-level categories:')
for cat in all_categories:
    print(f'  - "{cat}"')

print()
print('🔧 Applying format fixes...')

# Count before
cur.execute('SELECT COUNT(*) FROM products WHERE category_path->>0 = %s', ('Nhà cửa - đời sống',))
count_before = cur.fetchone()[0]
print(f'Before: {count_before} products with lowercase format')

# Fix products
for old_name, new_name in format_map.items():
    cur.execute('''
    UPDATE products 
    SET category_path = jsonb_set(
        category_path, 
        '{0}', 
        to_jsonb(%s::text)
    )
    WHERE category_path->>0 = %s
    ''', (new_name, old_name))
    
    rows_updated = cur.rowcount
    if rows_updated > 0:
        print(f'✅ Updated {rows_updated} products: "{old_name}" → "{new_name}"')

conn.commit()

# Verify
cur.execute('''
SELECT DISTINCT category_path->>0 as level_0
FROM products
WHERE category_path IS NOT NULL
ORDER BY level_0
''')

print()
print('✅ After fix - All first-level categories:')
for row in cur.fetchall():
    if row[0]:
        print(f'  - "{row[0]}"')

# Count after
cur.execute('SELECT COUNT(*) FROM products WHERE category_path->>0 = %s', ('Nhà Cửa - Đời Sống',))
count_after = cur.fetchone()[0]
print()
print(f'After: {count_after} products with correct format')
print(f'Fixed: {count_after - count_before} additional products')

cur.close()
conn.close()

print()
print('✅ Format unification complete!')
