#!/usr/bin/env python
"""Check warehouse category data"""
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

# Get categories
cur.execute('''
SELECT level_1, level_2, level_3, level_4, level_5, char_length(level_4) as l4_len 
FROM dim_category
ORDER BY l4_len DESC
LIMIT 10
''')

print("Top 10 longest L4s:")
for row in cur.fetchall():
    l4 = row[3][:80] if row[3] else "NULL"
    print(f"L4 length={row[5]}: {l4}")

# Check distribution
cur.execute('''
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN level_4 IS NULL THEN 1 ELSE 0 END) as null_l4,
  SUM(CASE WHEN level_5 IS NULL THEN 1 ELSE 0 END) as null_l5,
  SUM(CASE WHEN level_5 IS NOT NULL THEN 1 ELSE 0 END) as has_l5
FROM dim_category
''')

row = cur.fetchone()
print(f"\nDistribution:")
print(f"  Total: {row[0]}")
print(f"  NULL L4: {row[1]}")
print(f"  NULL L5: {row[2]}")
print(f"  Has L5: {row[3]}")

# Get a few sample categories
print("\nSample categories:")
cur.execute('''
SELECT level_1, level_2, level_3, level_4, level_5
FROM dim_category
LIMIT 5
''')

for i, row in enumerate(cur.fetchall(), 1):
    path = [r for r in row if r]
    print(f"{i}. {' > '.join(path)}")

conn.close()
