import psycopg2

conn = psycopg2.connect("dbname=crawl_data user=postgres password=postgres host=localhost")
conn.autocommit = True
cur = conn.cursor()

print("=" * 70)
print("XÓA PRODUCTS CÓ BRAND HOẶC SELLER NULL/EMPTY")
print("=" * 70)

# Check before deletion
cur.execute(
    """
    SELECT 
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE brand IS NULL OR brand = '') as null_brand,
        COUNT(*) FILTER (WHERE seller_name IS NULL OR seller_name = '') as null_seller,
        COUNT(*) FILTER (WHERE brand IS NULL OR brand = '' OR seller_name IS NULL OR seller_name = '') as to_delete
    FROM products
"""
)

total, null_brand, null_seller, to_delete = cur.fetchone()

print("\nTrước khi xóa:")
print(f"  - Tổng sản phẩm: {total}")
print(f"  - NULL brand: {null_brand}")
print(f"  - NULL seller: {null_seller}")
print(f"  - Sẽ xóa (brand OR seller NULL): {to_delete}")

if to_delete > 0:
    # Delete products with NULL brand or seller
    cur.execute(
        "DELETE FROM products WHERE brand IS NULL OR brand = '' OR seller_name IS NULL OR seller_name = '';"
    )
    deleted = cur.rowcount

    print(f"\n✅ Đã xóa {deleted} products")

    # Check after deletion
    cur.execute("SELECT COUNT(*) FROM products")
    remaining = cur.fetchone()[0]

    print("\nSau khi xóa:")
    print(f"  - Còn lại: {remaining} products")
    print(f"  - Đã xóa: {deleted} products")
else:
    print("\n✓ Không có products nào cần xóa")

print("=" * 70)

cur.close()
conn.close()
