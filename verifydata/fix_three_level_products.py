#!/usr/bin/env python
"""
Fix all 3-level products in database by adding missing Level 0 (parent category)

Uses category_hierarchy_map.json to auto-detect parent categories
"""

import json
import psycopg2
from difflib import SequenceMatcher

# Load hierarchy map
print("📖 Loading hierarchy map...")
with open('data/raw/category_hierarchy_map.json', encoding='utf-8') as f:
    hierarchy = json.load(f)
print(f"✅ Loaded {len(hierarchy)} categories")

# Connect to database
conn = psycopg2.connect('dbname=crawl_data user=postgres password=postgres host=localhost')
cur = conn.cursor()

# Get all 3-level products
cur.execute('''
SELECT id, category_path
FROM products
WHERE jsonb_array_length(category_path) = 3
ORDER BY id
''')

three_level_products = cur.fetchall()
print(f"\n🔍 Found {len(three_level_products)} products with 3 levels")

# Helper: fuzzy match category names (to handle typos like "vượn" vs "vườn")
def fuzzy_match(s1, s2, threshold=0.85):
    """Check if two strings match with fuzzy matching"""
    return SequenceMatcher(None, s1, s2).ratio() >= threshold

def find_parent_category(first_level_name):
    """Find parent category for a given L1 name using hierarchy"""
    
    # Try exact match first
    for url, info in hierarchy.items():
        if info['level'] == 1 and info['name'] == first_level_name:
            parent_chain = info.get('parent_chain', [])
            if parent_chain:
                root_url = parent_chain[0]
                root_info = hierarchy.get(root_url)
                if root_info:
                    return root_info['name']
    
    # Try fuzzy match if exact failed
    for url, info in hierarchy.items():
        if info['level'] == 1 and fuzzy_match(info['name'], first_level_name):
            parent_chain = info.get('parent_chain', [])
            if parent_chain:
                root_url = parent_chain[0]
                root_info = hierarchy.get(root_url)
                if root_info:
                    return root_info['name']
    
    return None

# Fix products
fixed = 0
failed = 0
failed_examples = []

print("\n🔧 Fixing 3-level products...")

for product_id, category_path in three_level_products:
    first_level = category_path[0] if category_path else None
    
    if not first_level:
        failed += 1
        continue
    
    # Find parent
    parent = find_parent_category(first_level)
    
    if parent:
        # Add parent to beginning
        new_path = [parent] + category_path
        
        # Update database
        try:
            cur.execute(
                'UPDATE products SET category_path = %s WHERE id = %s',
                (json.dumps(new_path), product_id)
            )
            fixed += 1
            
            # Show first few
            if fixed <= 5:
                print(f"  ✅ ID {product_id}: Added '{parent}'")
        except Exception as e:
            failed += 1
            failed_examples.append((product_id, first_level, str(e)))
    else:
        failed += 1
        if len(failed_examples) < 5:
            failed_examples.append((product_id, first_level, "No parent found"))

# Commit
conn.commit()

print(f"\n📊 Results:")
print(f"  ✅ Fixed: {fixed} products")
print(f"  ❌ Failed: {failed} products")

if failed_examples:
    print(f"\n❌ Failed Examples:")
    for product_id, first_level, error in failed_examples[:5]:
        print(f"  ID {product_id} (L1: {first_level}): {error}")

# Verify
cur.execute('''
SELECT jsonb_array_length(category_path) as path_length, COUNT(*) as count
FROM products
GROUP BY jsonb_array_length(category_path)
ORDER BY path_length
''')

print(f"\n✅ Final State:")
for row in cur.fetchall():
    print(f"  {row[0]} levels: {row[1]} products")

cur.close()
conn.close()

print("\n🎉 Complete!")
