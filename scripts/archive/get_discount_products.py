import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/crawl_data")
cur = conn.cursor(cursor_factory=RealDictCursor)

# Lấy top 5 sản phẩm giảm giá cao nhất
cur.execute(
    """
    SELECT 
        name,
        discount_percent,
        price,
        rating_average,
        sales_count
    FROM products
    WHERE discount_percent IS NOT NULL
        AND discount_percent > 20
        AND name IS NOT NULL
    ORDER BY discount_percent DESC
    LIMIT 5
"""
)

print("=== TOP 5 SẢN PHẨM GIẢM GIÁ CAO NHẤT ===\n")
for i, row in enumerate(cur.fetchall(), 1):
    name = row["name"][:60] if row["name"] else "N/A"
    discount = row["discount_percent"] or 0
    price = row["price"] or 0
    sales = row["sales_count"] or 0

    print(f"{i}. {name}")
    print(f"   Giảm giá: {discount:.1f}% | Giá: {price:,.0f}đ | Bán: {sales:,} cái\n")

conn.close()
