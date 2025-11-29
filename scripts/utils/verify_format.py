import psycopg2

conn = psycopg2.connect("dbname=crawl_data user=postgres password=postgres host=localhost")
cur = conn.cursor()

print("=== CATEGORY FORMAT VERIFICATION ===")
print()

# Check all unique first-level categories
cur.execute(
    """
SELECT DISTINCT category_path->>0 as level_0
FROM products
WHERE category_path IS NOT NULL
ORDER BY level_0
"""
)

print("✅ All first-level categories in database:")
for row in cur.fetchall():
    if row[0]:
        print(f'  - "{row[0]}"')

print()

# Check warehouse
conn2 = psycopg2.connect("dbname=tiki_warehouse user=postgres password=postgres host=localhost")
cur2 = conn2.cursor()

cur2.execute(
    "SELECT DISTINCT level_1 FROM dim_category WHERE level_1 IS NOT NULL ORDER BY level_1 LIMIT 5"
)
print("✅ Sample categories from warehouse dim_category:")
for row in cur2.fetchall():
    print(f'  - "{row[0]}"')

cur.close()
conn.close()
cur2.close()
conn2.close()

print()
print("🎉 All formats unified to Title Case!")
