#!/usr/bin/env python3
"""Fix category_path in database by prepending parent category"""
import psycopg2
import json

conn = psycopg2.connect('dbname=crawl_data user=postgres password=postgres host=localhost')
cur = conn.cursor()

PARENT_CATEGORY = "Nhà Cửa - Đời Sống"

# Get all products with category_path
cur.execute('SELECT product_id, category_path FROM products WHERE category_path IS NOT NULL')
products = cur.fetchall()

updated_count = 0
for product_id, cat_path in products:
    if isinstance(cat_path, list) and len(cat_path) > 0:
        # Check if first element is already the parent
        if cat_path[0] != PARENT_CATEGORY:
            # Prepend parent category
            new_path = [PARENT_CATEGORY] + cat_path
            
            cur.execute(
                'UPDATE products SET category_path = %s WHERE product_id = %s',
                (json.dumps(new_path), product_id)
            )
            updated_count += 1

conn.commit()
print(f"✅ Updated {updated_count} products with parent category prepended")

# Verify
cur.execute('SELECT DISTINCT category_path->>0 FROM products WHERE category_path IS NOT NULL ORDER BY 1')
print('\nLevel 1 categories in database:')
for row in cur.fetchall():
    print(f'  • {row[0]}')

# Show sample
print('\nSample updated categories:')
cur.execute('SELECT category_path FROM products WHERE category_path IS NOT NULL LIMIT 5')
for i, (path,) in enumerate(cur.fetchall(), 1):
    print(f'{i}. {path}')

conn.close()
