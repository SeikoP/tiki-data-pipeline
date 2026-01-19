import psycopg2

conn = psycopg2.connect("dbname=crawl_data user=postgres password=postgres host=localhost")
cur = conn.cursor()

# Check records with NULL brand or seller
cur.execute(
    """
    SELECT 
        COUNT(*) FILTER (WHERE brand IS NULL) as null_brand,
        COUNT(*) FILTER (WHERE seller_name IS NULL) as null_seller,
        COUNT(*) FILTER (WHERE brand IS NULL OR seller_name IS NULL) as null_either,
        COUNT(*) as total
    FROM products
"""
)

null_brand, null_seller, null_either, total = cur.fetchone()

print(f"Total products: {total}")
print(f"  - NULL brand: {null_brand}")
print(f"  - NULL seller: {null_seller}")
print(f"  - NULL brand OR seller: {null_either}")
print(f"  - Valid (both brand & seller): {total - null_either}")

conn.close()
