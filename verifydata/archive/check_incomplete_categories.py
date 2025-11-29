import psycopg2
import json

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

print("=== Searching for categories with only 3 levels (missing level 4) ===\n")

# Get all level 3 categories and check if they have products with only 3 levels
cur.execute('''
    SELECT 
        category_path ->> 2 as level_3,
        jsonb_array_length(category_path) as path_length,
        COUNT(*) as product_count
    FROM products
    GROUP BY category_path ->> 2, path_length
    HAVING COUNT(*) > 0
    ORDER BY level_3, path_length;
''')

results = cur.fetchall()

# Group by level 3 to see the distribution
level3_groups = {}
for level3, path_len, count in results:
    if level3 not in level3_groups:
        level3_groups[level3] = {}
    level3_groups[level3][path_len] = count

print(f"Found {len(level3_groups)} level 3 categories\n")

categories_with_3levels = []

for level3 in sorted(level3_groups.keys()):
    dist = level3_groups[level3]
    
    # Check if this category has products with only 3 levels
    if 3 in dist and (4 not in dist or dist[3] > 0):
        has_3only = dist.get(3, 0)
        has_4 = dist.get(4, 0)
        
        if has_3only > 0:
            categories_with_3levels.append((level3, has_3only, has_4))
            status = "❌ INCOMPLETE" if has_4 == 0 else "⚠️  MIXED"
            print(f"{status} {level3}")
            print(f"     3 levels: {has_3only} products | 4 levels: {has_4} products")
            print()

print(f"\n=== Summary ===")
print(f"Categories with products having ONLY 3 levels (incomplete): {len([x for x in categories_with_3levels if x[2] == 0])}")
print(f"Categories with products having MIXED 3 & 4 levels: {len([x for x in categories_with_3levels if x[2] > 0])}")

if categories_with_3levels:
    print(f"\nCategories with incomplete data:")
    for level3, count3, count4 in categories_with_3levels:
        if count4 == 0:
            # Get product IDs
            cur.execute('''
                SELECT id FROM products
                WHERE category_path ->> 2 = %s
                AND jsonb_array_length(category_path) = 3
                ORDER BY id;
            ''', (level3,))
            ids = [str(row[0]) for row in cur.fetchall()]
            print(f"  • {level3}: {count3} products (IDs: {', '.join(ids[:5])}{'...' if len(ids) > 5 else ''})")

cur.close()
conn.close()
