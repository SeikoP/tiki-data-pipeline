import psycopg2

conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/crawl_data")
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM products")
total = cur.fetchone()[0]

cur.execute("SELECT COUNT(DISTINCT category_url) FROM products")
cats = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM products WHERE sales_count IS NOT NULL AND sales_count > 0")
detail_crawled = cur.fetchone()[0]

print(f"Total products in DB: {total}")
print(f"Categories: {cats}")
print(f"With sales data: {detail_crawled}")
print(f"Coverage: {detail_crawled}/{total} = {detail_crawled*100/total:.1f}%")

conn.close()
