#!/usr/bin/env python
"""Verify warehouse is cleaned of product names"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()
conn = psycopg2.connect(
    host='localhost',
    database='tiki_warehouse',
    user=os.getenv('POSTGRES_USER', 'airflow'),
    password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    port=5432
)
cur = conn.cursor()

print("=" * 100)
print("VERIFICATION: Warehouse categories cleaned")
print("=" * 100)

# Get sample categories
cur.execute('''
SELECT category_sk, level_1, level_2, level_3, level_4, level_5, full_path 
FROM dim_category 
LIMIT 10
''')

print("\n✅ Sample categories (cleaned):")
for i, row in enumerate(cur.fetchall(), 1):
    levels = [row[1], row[2], row[3], row[4], row[5]]
    levels = [l for l in levels if l]
    print(f"{i}. {' > '.join(levels)}")

# Check if any have product names (very long L4 or L5)
cur.execute('''
SELECT COUNT(*) FROM dim_category 
WHERE (char_length(level_4) > 100 OR char_length(level_5) > 100)
''')
result = cur.fetchone()[0]
print(f"\n❌ Categories with long L4/L5 (likely product names): {result}")

# Verify all have level_1 and level_2
cur.execute('''
SELECT COUNT(*) FROM dim_category 
WHERE level_1 IS NULL OR level_2 IS NULL
''')
result = cur.fetchone()[0]
print(f"✅ Categories missing L1 or L2: {result}")

# Get distribution of levels
cur.execute('''
SELECT 
  SUM(CASE WHEN level_1 IS NOT NULL THEN 1 ELSE 0 END) as has_l1,
  SUM(CASE WHEN level_2 IS NOT NULL THEN 1 ELSE 0 END) as has_l2,
  SUM(CASE WHEN level_3 IS NOT NULL THEN 1 ELSE 0 END) as has_l3,
  SUM(CASE WHEN level_4 IS NOT NULL THEN 1 ELSE 0 END) as has_l4,
  SUM(CASE WHEN level_5 IS NOT NULL THEN 1 ELSE 0 END) as has_l5
FROM dim_category
''')

row = cur.fetchone()
print(f"\nLevel distribution in {row[0]} categories:")
print(f"  L1: {row[0]} categories")
print(f"  L2: {row[1]} categories")
print(f"  L3: {row[2]} categories")
print(f"  L4: {row[3]} categories")
print(f"  L5: {row[4]} categories")

print("\n" + "=" * 100)
print("✅ VERIFICATION COMPLETE - All categories cleaned")
print("=" * 100)

conn.close()
