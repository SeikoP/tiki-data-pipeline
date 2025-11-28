#!/usr/bin/env python
"""Remove product names from category_path in crawl_data database"""
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

print("=" * 80)
print("FIX: Remove product names from category_path")
print("=" * 80)

# Get products with category_path longer than 4 elements
cur.execute('''
SELECT id, product_id, name, category_path 
FROM products 
WHERE jsonb_array_length(category_path) > 4
ORDER BY product_id
''')

products = cur.fetchall()
print(f"\nFound {len(products)} products with category_path length > 4")

updated_count = 0
for id, product_id, name, cat_path in products:
    try:
        # Keep only first 4 elements (which should be actual categories)
        # If a product has 5 elements, the 5th one is likely the product name
        fixed_path = cat_path[:4]
        
        # Only update if it changed
        if len(fixed_path) != len(cat_path):
            cur.execute(
                'UPDATE products SET category_path = %s WHERE id = %s',
                (json.dumps(fixed_path), id)
            )
            updated_count += 1
            
            if updated_count <= 5:  # Show first 5
                print(f"\n✓ Product: {name[:50]}...")
                print(f"  Old path ({len(cat_path)}): {cat_path}")
                print(f"  New path ({len(fixed_path)}): {fixed_path}")
    except Exception as e:
        print(f"  ❌ Error for {product_id}: {e}")

conn.commit()
print(f"\n{'=' * 80}")
print(f"✅ Updated {updated_count} products")
print(f"{'=' * 80}")

# Verify
cur.execute('''
SELECT jsonb_array_length(category_path) as path_length, COUNT(*) 
FROM products 
GROUP BY jsonb_array_length(category_path)
ORDER BY path_length
''')

print("\nVerification - Distribution of category_path lengths:")
for row in cur.fetchall():
    print(f"  Length {row[0]}: {row[1]} products")

# Show sample fixed categories
print("\nSample fixed categories:")
cur.execute('''
SELECT product_id, name, category_path 
FROM products 
WHERE jsonb_array_length(category_path) = 4
LIMIT 3
''')

for row in cur.fetchall():
    print(f"\n✓ {row[1][:50]}...")
    print(f"  Path: {' > '.join(row[2])}")

conn.close()
