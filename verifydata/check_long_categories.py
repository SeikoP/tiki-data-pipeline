#!/usr/bin/env python
"""Find categories with long L4 (likely product names)"""
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
print("Categories with L4 > 100 chars (likely product names)")
print("=" * 100)

cur.execute('''
SELECT category_sk, level_1, level_2, level_3, level_4, char_length(level_4) as l4_len
FROM dim_category 
WHERE char_length(level_4) > 100
ORDER BY l4_len DESC
''')

for i, row in enumerate(cur.fetchall(), 1):
    print(f"\n{i}. SK={row[0]}, L4 length={row[4]}")
    print(f"   L1: {row[1]}")
    print(f"   L2: {row[2]}")
    print(f"   L3: {row[3]}")
    print(f"   L4: {row[4][:100]}...")

# Check in source database
print("\n" + "=" * 100)
print("Check if these long L4s came from category_path in source")
print("=" * 100)

conn_source = psycopg2.connect(
    host='localhost',
    database='crawl_data',
    user=os.getenv('POSTGRES_USER', 'airflow'),
    password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
    port=5432
)
cur_source = conn_source.cursor()

# Get some products and their category_path
cur_source.execute('''
SELECT product_id, name, category_path
FROM products
WHERE jsonb_array_length(category_path) = 4
LIMIT 1
''')

print("\nSample from source (all should have 4 levels):")
for row in cur_source.fetchall():
    print(f"Path: {row[2]}")
    print(f"Levels: {' | '.join(row[2])}")

conn.close()
conn_source.close()
