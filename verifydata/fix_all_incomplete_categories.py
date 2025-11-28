import psycopg2
import json

conn = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user='postgres',
    password='postgres'
)
cur = conn.cursor()

print("=== Fixing all categories with incomplete 3-level data ===\n")

# 6 INCOMPLETE categories + 4 MIXED categories
fixes = {
    "Bàn thờ": "Bàn thờ",  # Level 3 is same as level 2, no subcategories
    "Bộ đồ thờ": "Bộ đồ thờ",
    "Hương, nhang": "Hương, nhang",
    "Lư trầm, bát hương, bát nhang": "Lư trầm, bát hương, bát nhang",
    "Mâm đồng/Mâm ngũ quả": "Mâm đồng/Mâm ngũ quả",
    "Phụ kiện thờ cúng": "Phụ kiện thờ cúng",
    "Bình, ly uống cà phê và phụ kiện": "Bình, ly uống cà phê và phụ kiện",
    "Ly, cốc & phụ kiện ly": "Ly, cốc & phụ kiện ly",
    "Áo mưa, ô dù và phụ kiện đi mưa": "Áo mưa, ô dù và phụ kiện đi mưa",
    "Đàn guitar, ukulele": "Đàn guitar, ukulele",
}

total_fixed = 0

for level3, level4 in fixes.items():
    # Get products with only 3 levels in this category
    cur.execute('''
        SELECT id, category_path FROM products
        WHERE category_path ->> 2 = %s
        AND jsonb_array_length(category_path) = 3;
    ''', (level3,))
    
    products = cur.fetchall()
    
    if products:
        print(f"Fixing {level3}: {len(products)} products")
        
        for pid, cat_path in products:
            # Add level 4 (which is the same as level 3 when there are no subcategories)
            new_path = list(cat_path) + [level4]
            
            cur.execute('''
                UPDATE products
                SET category_path = %s
                WHERE id = %s;
            ''', (json.dumps(new_path), pid))
            
            total_fixed += 1
        
        print(f"  ✓ Updated {len(products)} products")
        print()

conn.commit()

# Verify
print("\n=== Verification ===")
cur.execute('''
    SELECT 
        jsonb_array_length(category_path) as path_length,
        COUNT(*) as count
    FROM products
    GROUP BY path_length
    ORDER BY path_length;
''')

print("Final distribution:")
for path_len, count in cur.fetchall():
    print(f"  {path_len} levels: {count} products")

# Check if any 3-level products remain
cur.execute('''
    SELECT COUNT(*) FROM products
    WHERE jsonb_array_length(category_path) = 3;
''')

count_3level = cur.fetchone()[0]

if count_3level == 0:
    print(f"\n✅ All {total_fixed} products fixed! No more 3-level categories!")
else:
    print(f"\n⚠️  Still {count_3level} products with 3 levels")

cur.close()
conn.close()
