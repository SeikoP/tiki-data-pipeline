#!/usr/bin/env python
"""Verify warehouse now has category_path from crawl_data"""
import psycopg2
from dotenv import load_dotenv
import os
import json

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
print("VERIFICATION: Category path now stored in warehouse")
print("=" * 100)

# Get sample categories with their paths
cur.execute('''
SELECT category_path, level_1, level_2, level_3, level_4, level_5
FROM dim_category
LIMIT 5
''')

print("\n✅ Sample categories with category_path:")
for i, row in enumerate(cur.fetchall(), 1):
    path_array = row[0]  # This should be JSON
    print(f"\n{i}. category_path: {path_array}")
    print(f"   level_1: {row[1]}")
    print(f"   level_2: {row[2]}")
    print(f"   level_3: {row[3]}")
    print(f"   level_4: {row[4]}")
    print(f"   level_5: {row[5]}")

# Verify all categories have category_path
cur.execute('''
SELECT COUNT(*) as total, 
       SUM(CASE WHEN category_path IS NOT NULL THEN 1 ELSE 0 END) as has_path
FROM dim_category
''')

row = cur.fetchone()
print(f"\n✅ Distribution:")
print(f"   Total categories: {row[0]}")
print(f"   With category_path: {row[1]}")

# Verify level extraction is correct
cur.execute('''
SELECT category_path, level_1, level_2, level_3, level_4, level_5
FROM dim_category
WHERE category_path IS NOT NULL
LIMIT 3
''')

print(f"\n✅ Verify level extraction matches category_path:")
for row in cur.fetchall():
    path = json.loads(row[0]) if isinstance(row[0], str) else row[0]
    expected = [row[1], row[2], row[3], row[4], row[5]]
    actual = path[:5] if len(path) >= 5 else path + [None] * (5 - len(path))
    
    match = expected == actual
    status = "✓" if match else "✗"
    print(f"   {status} {path}")
    if not match:
        print(f"     Expected: {expected}")
        print(f"     Got: {actual}")

print("\n" + "=" * 100)
print("✅ VERIFICATION COMPLETE")
print("=" * 100)

conn.close()
