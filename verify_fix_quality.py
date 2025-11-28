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

# Check current state after fixing suspicious categories
print("=" * 80)
print("CATEGORY PATH DATA QUALITY CHECK AFTER FIX")
print("=" * 80)

# Get distribution of category_path lengths
cur.execute('''
SELECT jsonb_array_length(category_path) as level_count, COUNT(*) as product_count
FROM products
GROUP BY jsonb_array_length(category_path)
ORDER BY level_count
''')

print("\nDistribution of category path levels:")
results = cur.fetchall()
for level_count, product_count in results:
    print(f"  {level_count} levels: {product_count:>4} products")

# Check for suspicious items (very long category text)
print("\n" + "=" * 80)
print("CHECKING FOR REMAINING SUSPICIOUS ITEMS (text length > 80 chars)")
print("=" * 80)

cur.execute('''
SELECT product_id, name, category_path
FROM products
WHERE category_path::text ILIKE '%' || name || '%'
  AND jsonb_array_length(category_path) >= 3
LIMIT 10
''')

results = cur.fetchall()
if results:
    print(f"\nFound {len(results)} products with potential issues:")
    for pid, name, cat_path in results:
        if isinstance(cat_path, str):
            cat_path = json.loads(cat_path)
        
        # Check each level length
        suspicious_levels = [i for i, level in enumerate(cat_path, 1) if len(level) > 80]
        if suspicious_levels:
            print(f"\nID: {pid}")
            print(f"  Name: {name[:70]}")
            print(f"  Suspicious levels: {suspicious_levels}")
            for i, level in enumerate(cat_path, 1):
                if i in suspicious_levels:
                    print(f"    L{i}: {level[:80]}... (len={len(level)})")
else:
    print("\n✓ No products with suspicious long category texts found!")

# Final summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

cur.execute('SELECT COUNT(*) FROM products')
total_products = cur.fetchone()[0]

# Count products with valid paths (3-4 levels)
cur.execute('''
SELECT COUNT(*) FROM products
WHERE jsonb_array_length(category_path) IN (3, 4)
''')

valid_products = cur.fetchone()[0]

print(f"✓ Total products: {total_products}")
print(f"✓ Products with valid category paths (3-4 levels): {valid_products}")
print(f"✓ Data quality: {(valid_products/total_products*100):.1f}%")

if valid_products == total_products:
    print("\n✅ ALL PRODUCTS HAVE VALID CATEGORY PATHS - FIX SUCCESSFUL!")
else:
    print(f"\n⚠️  {total_products - valid_products} products may need attention")

conn.close()
