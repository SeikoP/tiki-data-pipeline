import psycopg2

conn = psycopg2.connect("dbname=crawl_data user=postgres password=postgres host=localhost")
cur = conn.cursor()

# Check product 783 and 839
for prod_id in [783, 839, 1053]:
    cur.execute("SELECT product_id, category_path FROM products WHERE id = %s", (prod_id,))
    result = cur.fetchone()
    if result:
        print(f"Product {prod_id}: category_path = {result[1]}")
    else:
        print(f"Product {prod_id}: NOT FOUND")

conn.close()
