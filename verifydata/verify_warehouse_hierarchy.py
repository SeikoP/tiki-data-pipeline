#!/usr/bin/env python
"""Verify warehouse category hierarchy after rebuild."""
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

# Check category levels
cur.execute('''
SELECT level_1, COUNT(*) as count 
FROM dim_category 
GROUP BY level_1
ORDER BY count DESC
LIMIT 10
''')
print('Top 10 Level 1 Categories:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} products')

# Check hierarchy for specific categories
cur.execute('''
SELECT level_1, level_2, level_3, level_4, level_5 
FROM dim_category 
WHERE level_1 = 'Nhà Cửa - Đời Sống'
LIMIT 5
''')
print('\nSample hierarchies for Nhà Cửa - Đời Sống:')
for i, row in enumerate(cur.fetchall(), 1):
    hierarchy = [x for x in row if x]
    print(f'  {i}. {" > ".join(hierarchy)}')

# Verify fact table connections
cur.execute('''
SELECT COUNT(*) FROM fact_product_sales fps
WHERE NOT EXISTS (SELECT 1 FROM dim_product dp WHERE fps.product_sk = dp.product_sk)
''')
orphaned = cur.fetchone()[0]
print(f'\n✓ Orphaned facts: {orphaned}')

cur.execute('SELECT COUNT(*) FROM fact_product_sales')
total_facts = cur.fetchone()[0]
print(f'✓ Total facts: {total_facts}')

conn.close()
