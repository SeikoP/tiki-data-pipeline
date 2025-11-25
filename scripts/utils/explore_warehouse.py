import psycopg2
import json

conn = psycopg2.connect('dbname=tiki_warehouse user=postgres password=postgres host=localhost')
cur = conn.cursor()

print("\n" + "=" * 80)
print("KHÁM PHÁ TIKI_WAREHOUSE DATABASE")
print("=" * 80)

# 1. Check tables
print("\n1. DANH SÁCH BẢNG:")
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' ORDER BY table_name
""")
tables = cur.fetchall()
for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
    count = cur.fetchone()[0]
    print(f"   - {table[0]:30} : {count:,} rows")

# 2. Detailed schema
print("\n2. SCHEMA CHI TIẾT:")

# Fact table
print("\n   FACT_PRODUCT_SALES:")
cur.execute("""
    SELECT column_name, data_type FROM information_schema.columns 
    WHERE table_name = 'fact_product_sales' ORDER BY ordinal_position
""")
for col, dtype in cur.fetchall():
    print(f"      - {col:30} : {dtype}")

# 3. Sample data from fact
print("\n3. DỮ LIỆU MẪU FACT_PRODUCT_SALES:")
cur.execute("""
    SELECT 
        f.fact_id, p.product_name, b.brand_name, s.seller_name,
        f.price, f.discount_percent, f.quantity_sold, f.average_rating
    FROM fact_product_sales f
    JOIN dim_product p ON f.product_sk = p.product_sk
    JOIN dim_brand b ON f.brand_sk = b.brand_sk
    JOIN dim_seller s ON f.seller_sk = s.seller_sk
    LIMIT 5
""")
print("\n   Tình trạng dữ liệu mẫu:")
for row in cur.fetchall():
    fact_id, product, brand, seller, price, discount, qty, rating = row
    print(f"   - Fact {fact_id}: {product[:40]:40} | {brand:20} | ${price:,} | Rating: {rating}/5")

# 4. Dimension statistics
print("\n4. THỐNG KÊ DIMENSIONS:")
cur.execute("SELECT COUNT(DISTINCT category_id) FROM dim_category")
categories = cur.fetchone()[0]
print(f"   - Danh mục: {categories}")

cur.execute("SELECT COUNT(DISTINCT brand_name) FROM dim_brand")
brands = cur.fetchone()[0]
print(f"   - Thương hiệu: {brands}")

cur.execute("SELECT COUNT(DISTINCT seller_id) FROM dim_seller")
sellers = cur.fetchone()[0]
print(f"   - Người bán: {sellers}")

cur.execute("SELECT COUNT(DISTINCT segment_name) FROM dim_price_segment")
segments = cur.fetchone()[0]
print(f"   - Phân khúc giá: {segments}")

# 5. Data quality metrics
print("\n5. CHỈ TIÊU CHẤT LƯỢNG DỮ LIỆU:")
cur.execute("""
    SELECT 
        COUNT(*) as total_facts,
        COUNT(CASE WHEN price IS NOT NULL THEN 1 END) as has_price,
        COUNT(CASE WHEN average_rating > 0 THEN 1 END) as has_rating,
        ROUND(AVG(CASE WHEN price IS NOT NULL THEN price ELSE 0 END), 2) as avg_price,
        ROUND(AVG(CASE WHEN average_rating > 0 THEN average_rating ELSE 0 END), 2) as avg_rating
    FROM fact_product_sales
""")
total, has_price, has_rating, avg_price, avg_rating = cur.fetchone()
print(f"   - Tổng fact records: {total:,}")
print(f"   - Có giá: {has_price:,} ({has_price*100/total:.1f}%)")
print(f"   - Có rating: {has_rating:,} ({has_rating*100/total:.1f}%)")
print(f"   - Giá trung bình: ${avg_price:,}")
print(f"   - Rating trung bình: {avg_rating:.2f}/5")

# 6. Price segments distribution
print("\n6. PHÂN PHỐI PHÂN KHÚC GIÁ:")
cur.execute("""
    SELECT 
        ps.segment_name, 
        COUNT(*) as product_count,
        ROUND(AVG(f.price), 2) as avg_price,
        SUM(f.estimated_revenue) as total_revenue
    FROM fact_product_sales f
    JOIN dim_price_segment ps ON f.price_segment_sk = ps.price_segment_sk
    GROUP BY ps.segment_name
    ORDER BY product_count DESC
""")
for segment, count, avg_p, revenue in cur.fetchall():
    print(f"   - {segment:30} : {count:4,} sản phẩm | Giá TB: ${avg_p:,} | Doanh thu: ${revenue:,.0f}")

# 7. Top categories
print("\n7. TOP 10 DANH MỤC:")
cur.execute("""
    SELECT 
        dc.full_path,
        COUNT(*) as product_count,
        ROUND(AVG(f.average_rating), 2) as avg_rating
    FROM fact_product_sales f
    JOIN dim_category dc ON f.category_sk = dc.category_sk
    GROUP BY dc.full_path
    ORDER BY product_count DESC
    LIMIT 10
""")
for path, count, rating in cur.fetchall():
    print(f"   - {path[:60]:60} : {count:4,} products | Rating: {rating}")

# 8. Top brands
print("\n8. TOP 10 THƯƠNG HIỆU:")
cur.execute("""
    SELECT 
        db.brand_name,
        COUNT(*) as product_count,
        ROUND(AVG(f.average_rating), 2) as avg_rating,
        ROUND(AVG(f.price), 2) as avg_price
    FROM fact_product_sales f
    JOIN dim_brand db ON f.brand_sk = db.brand_sk
    GROUP BY db.brand_name
    ORDER BY product_count DESC
    LIMIT 10
""")
for brand, count, rating, price in cur.fetchall():
    print(f"   - {brand:30} : {count:4,} products | Rating: {rating} | Giá TB: ${price:,}")

print("\n" + "=" * 80 + "\n")

cur.close()
conn.close()
