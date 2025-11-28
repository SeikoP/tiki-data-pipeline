import psycopg2

conn = psycopg2.connect("dbname=crawl_data user=postgres password=postgres host=localhost")
cur = conn.cursor()

cur.execute(
    """
    SELECT column_name, data_type 
    FROM information_schema.columns 
    WHERE table_name='products' 
    ORDER BY ordinal_position
"""
)

print("SCHEMA OF crawl_data.products:")
print("-" * 50)
for col_name, col_type in cur.fetchall():
    print(f"  {col_name:20} → {col_type}")

conn.close()
