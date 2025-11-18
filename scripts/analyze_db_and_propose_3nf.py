"""
Script để kết nối PostgreSQL, phân tích schema hiện tại và đề xuất 3NF
"""
import os
import sys
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("❌ psycopg2 chưa được cài đặt. Install: pip install psycopg2-binary")
    sys.exit(1)

# Database connection config
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'crawl_data',
    'user': 'postgres',
    'password': 'postgres'
}

def connect_db():
    """Kết nối database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print(f"✅ Đã kết nối database: {DB_CONFIG['database']}")
        return conn
    except Exception as e:
        print(f"❌ Lỗi kết nối database: {e}")
        return None

def analyze_table_schema(conn, table_name):
    """Phân tích schema của table"""
    print(f"\n{'='*60}")
    print(f"📊 Phân tích bảng: {table_name}")
    print('='*60)
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get columns info
    cursor.execute(f"""
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    print(f"\n📋 Columns ({len(columns)} cột):")
    for col in columns:
        nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
        max_len = f"({col['character_maximum_length']})" if col['character_maximum_length'] else ""
        print(f"  - {col['column_name']}: {col['data_type']}{max_len} {nullable}")
    
    # Get row count
    cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
    count = cursor.fetchone()['count']
    print(f"\n📈 Tổng số records: {count:,}")
    
    # Sample data
    if count > 0:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()
        print(f"\n🔍 Sample record (1 row):")
        for key, value in sample.items():
            if isinstance(value, dict) or isinstance(value, list):
                print(f"  {key}: {json.dumps(value, ensure_ascii=False)[:100]}...")
            else:
                print(f"  {key}: {value}")
    
    cursor.close()
    return columns, count

def check_jsonb_structure(conn, table_name, jsonb_columns):
    """Kiểm tra cấu trúc JSONB columns"""
    print(f"\n🔎 Phân tích JSONB columns trong {table_name}:")
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    for col in jsonb_columns:
        # Get distinct keys in JSONB
        cursor.execute(f"""
            SELECT DISTINCT jsonb_object_keys({col}) as key
            FROM {table_name}
            WHERE {col} IS NOT NULL
            LIMIT 20
        """)
        
        keys = [row['key'] for row in cursor.fetchall()]
        print(f"\n  📦 {col} - Keys tìm thấy: {keys}")
        
        # Sample values
        if keys:
            sample_key = keys[0]
            cursor.execute(f"""
                SELECT {col}->'{sample_key}' as sample_value
                FROM {table_name}
                WHERE {col}->'{sample_key}' IS NOT NULL
                LIMIT 1
            """)
            sample = cursor.fetchone()
            if sample:
                print(f"     Sample {sample_key}: {sample['sample_value']}")
    
    cursor.close()

def analyze_relationships(conn):
    """Phân tích mối quan hệ giữa products và categories"""
    print(f"\n{'='*60}")
    print("🔗 Phân tích mối quan hệ Products <-> Categories")
    print('='*60)
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Check products có category_url
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(category_url) as has_category_url,
            COUNT(DISTINCT category_url) as unique_categories
        FROM products
    """)
    result = cursor.fetchone()
    print(f"\n📊 Products statistics:")
    print(f"  - Total products: {result['total']:,}")
    print(f"  - Có category_url: {result['has_category_url']:,} ({result['has_category_url']/result['total']*100:.1f}%)")
    print(f"  - Unique categories: {result['unique_categories']:,}")
    
    # Check categories table
    cursor.execute("SELECT COUNT(*) as count FROM categories")
    cat_count = cursor.fetchone()['count']
    print(f"\n📊 Categories table:")
    print(f"  - Total categories: {cat_count:,}")
    
    # Sample join
    cursor.execute("""
        SELECT 
            p.product_id,
            p.name as product_name,
            p.category_url,
            c.category_id,
            c.name as category_name,
            c.parent_id,
            c.level
        FROM products p
        LEFT JOIN categories c ON p.category_url = c.url
        LIMIT 5
    """)
    
    print(f"\n🔗 Sample join Products <-> Categories:")
    for row in cursor.fetchall():
        print(f"  Product: {row['product_name'][:50]}")
        print(f"    Category URL: {row['category_url']}")
        print(f"    Category: {row['category_name']} (ID: {row['category_id']}, Level: {row['level']})")
        print()
    
    cursor.close()

def propose_3nf_schema():
    """Đề xuất schema 3NF"""
    print(f"\n{'='*60}")
    print("💡 ĐỀ XUẤT SCHEMA 3NF")
    print('='*60)
    
    schema = """
-- ============================================
-- THIRD NORMAL FORM (3NF) SCHEMA
-- ============================================

-- 1️⃣ CATEGORIES (dimension table)
CREATE TABLE dim_categories (
    category_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    url TEXT,
    parent_id VARCHAR(50),
    level INTEGER,
    category_path TEXT[],  -- Array breadcrumb
    is_leaf BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (parent_id) REFERENCES dim_categories(category_id)
);

CREATE INDEX idx_categories_parent ON dim_categories(parent_id);
CREATE INDEX idx_categories_level ON dim_categories(level);
CREATE INDEX idx_categories_url ON dim_categories(url);

-- 2️⃣ SELLERS (dimension table)
CREATE TABLE dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(500),
    is_official BOOLEAN DEFAULT FALSE,
    seller_type VARCHAR(50),  -- 'official', 'marketplace', 'individual'
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_sellers_type ON dim_sellers(seller_type);

-- 3️⃣ BRANDS (dimension table)
CREATE TABLE dim_brands (
    brand_id SERIAL PRIMARY KEY,
    name VARCHAR(200) UNIQUE NOT NULL,
    normalized_name VARCHAR(200),  -- Lowercase, no accents
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_brands_normalized ON dim_brands(normalized_name);

-- 4️⃣ PRODUCTS (fact table - core data)
CREATE TABLE fact_products (
    product_id VARCHAR(50) PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    image_url TEXT,
    description TEXT,
    
    -- Foreign keys
    category_id VARCHAR(50),
    seller_id VARCHAR(50),
    brand_id INTEGER,
    
    -- Numeric metrics
    price NUMERIC(15, 2),
    original_price NUMERIC(15, 2),
    discount_percent SMALLINT,
    rating_average NUMERIC(3, 2),
    review_count INTEGER,
    sales_count INTEGER,
    
    -- Stock
    stock_available BOOLEAN,
    stock_quantity INTEGER,
    stock_status VARCHAR(50),
    
    -- Timestamps
    crawled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Data quality
    is_clean BOOLEAN DEFAULT FALSE,
    cleaned_at TIMESTAMP,
    quality_score NUMERIC(5, 2),
    
    FOREIGN KEY (category_id) REFERENCES dim_categories(category_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id),
    FOREIGN KEY (brand_id) REFERENCES dim_brands(brand_id)
);

CREATE INDEX idx_products_category ON fact_products(category_id);
CREATE INDEX idx_products_seller ON fact_products(seller_id);
CREATE INDEX idx_products_brand ON fact_products(brand_id);
CREATE INDEX idx_products_price ON fact_products(price);
CREATE INDEX idx_products_sales ON fact_products(sales_count);
CREATE INDEX idx_products_is_clean ON fact_products(is_clean);

-- 5️⃣ PRODUCT SPECIFICATIONS (1:N - normalized)
CREATE TABLE fact_product_specifications (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    spec_key VARCHAR(200) NOT NULL,
    spec_value TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_id) REFERENCES fact_products(product_id) ON DELETE CASCADE
);

CREATE INDEX idx_specs_product ON fact_product_specifications(product_id);
CREATE INDEX idx_specs_key ON fact_product_specifications(spec_key);

-- 6️⃣ PRODUCT IMAGES (1:N - normalized)
CREATE TABLE fact_product_images (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    image_url TEXT NOT NULL,
    image_order SMALLINT DEFAULT 0,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_id) REFERENCES fact_products(product_id) ON DELETE CASCADE
);

CREATE INDEX idx_images_product ON fact_product_images(product_id);

-- 7️⃣ PRODUCT COMPUTED METRICS (1:1 - separated for cleaning)
CREATE TABLE fact_product_metrics (
    product_id VARCHAR(50) PRIMARY KEY,
    estimated_revenue NUMERIC(15, 2),
    price_savings NUMERIC(15, 2),
    discount_amount NUMERIC(15, 2),
    price_category VARCHAR(50),  -- 'budget', 'mid-range', 'premium', 'luxury'
    popularity_score NUMERIC(5, 2),
    value_score NUMERIC(10, 2),
    sales_velocity INTEGER,
    engagement_rate NUMERIC(5, 2),  -- review_count / sales_count * 100
    computed_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_id) REFERENCES fact_products(product_id) ON DELETE CASCADE
);

CREATE INDEX idx_metrics_price_category ON fact_product_metrics(price_category);
CREATE INDEX idx_metrics_popularity ON fact_product_metrics(popularity_score);

-- 8️⃣ SHIPPING INFO (1:1 - normalized)
CREATE TABLE fact_product_shipping (
    product_id VARCHAR(50) PRIMARY KEY,
    fee NUMERIC(10, 2),
    warehouse VARCHAR(200),
    delivery_time VARCHAR(100),
    free_shipping BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (product_id) REFERENCES fact_products(product_id) ON DELETE CASCADE
);

-- 9️⃣ DATA QUALITY AUDIT (separate table)
CREATE TABLE audit_rejected_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    rejection_reason VARCHAR(200),
    rejection_details JSONB,
    rejected_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_rejected_date ON audit_rejected_products(rejected_at);

-- 🔟 CLEANING AUDIT LOG
CREATE TABLE audit_cleaning_log (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    transformation_type VARCHAR(100),
    old_value TEXT,
    new_value TEXT,
    cleaned_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_cleaning_product ON audit_cleaning_log(product_id);
CREATE INDEX idx_cleaning_date ON audit_cleaning_log(cleaned_at);

-- ============================================
-- MATERIALIZED VIEWS (for analytics)
-- ============================================

-- View: Products with all dimensions denormalized
CREATE MATERIALIZED VIEW view_products_enriched AS
SELECT 
    p.*,
    c.name as category_name,
    c.level as category_level,
    c.category_path,
    s.name as seller_name,
    s.is_official as seller_is_official,
    s.seller_type,
    b.name as brand_name,
    m.estimated_revenue,
    m.price_savings,
    m.price_category,
    m.popularity_score,
    m.value_score,
    m.sales_velocity
FROM fact_products p
LEFT JOIN dim_categories c ON p.category_id = c.category_id
LEFT JOIN dim_sellers s ON p.seller_id = s.seller_id
LEFT JOIN dim_brands b ON p.brand_id = b.brand_id
LEFT JOIN fact_product_metrics m ON p.product_id = m.product_id
WHERE p.is_clean = TRUE;

CREATE UNIQUE INDEX idx_mv_products_id ON view_products_enriched(product_id);

-- Refresh command (run after data changes):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY view_products_enriched;
"""
    
    print(schema)
    
    print("\n📋 Key Design Decisions:")
    print("""
1. **Dimension Tables** (dim_*):
   - dim_categories: Hierarchical category tree
   - dim_sellers: Seller master data
   - dim_brands: Brand master data (normalized, no duplicates)

2. **Fact Tables** (fact_*):
   - fact_products: Core product data (main table)
   - fact_product_specifications: Normalized specs (1:N)
   - fact_product_images: Normalized images (1:N)
   - fact_product_metrics: Computed fields (1:1, separate for easy updates)
   - fact_product_shipping: Shipping info (1:1)

3. **Audit Tables** (audit_*):
   - audit_rejected_products: Track rejected records
   - audit_cleaning_log: Track all transformations

4. **Benefits of 3NF**:
   ✅ No data redundancy (brands, sellers, categories normalized)
   ✅ Easy to update (change brand name once, affects all products)
   ✅ Data integrity (foreign keys enforce consistency)
   ✅ Flexible queries (join as needed)
   ✅ JSONB eliminated (fully relational)

5. **Performance**:
   - Materialized view for analytics (denormalized for speed)
   - Proper indexes on FK and query columns
   - Separate metrics table (clean/recompute without touching main table)
""")

def generate_migration_script():
    """Tạo script migration từ current schema sang 3NF"""
    print(f"\n{'='*60}")
    print("🔄 MIGRATION SCRIPT")
    print('='*60)
    
    migration = """
-- ============================================
-- MIGRATION: Current Schema -> 3NF
-- ============================================

-- Step 1: Create new 3NF tables (run schema above first)

-- Step 2: Migrate Categories
INSERT INTO dim_categories (category_id, name, url, parent_id, level, category_path)
SELECT 
    category_id,
    name,
    url,
    parent_id,
    level,
    string_to_array(url, '/') as category_path
FROM categories
ON CONFLICT (category_id) DO NOTHING;

-- Step 3: Extract and migrate Sellers
INSERT INTO dim_sellers (seller_id, name, is_official)
SELECT DISTINCT
    COALESCE(seller_id::TEXT, 'unknown') as seller_id,
    seller_name,
    COALESCE(seller_is_official, FALSE)
FROM products
WHERE seller_id IS NOT NULL
ON CONFLICT (seller_id) DO NOTHING;

-- Step 4: Extract and migrate Brands
INSERT INTO dim_brands (name, normalized_name)
SELECT DISTINCT
    brand,
    LOWER(REGEXP_REPLACE(brand, '[^a-zA-Z0-9]', '', 'g')) as normalized_name
FROM products
WHERE brand IS NOT NULL AND brand != ''
ON CONFLICT (name) DO NOTHING;

-- Step 5: Migrate Products (main data)
INSERT INTO fact_products (
    product_id, name, url, image_url, description,
    category_id, seller_id, brand_id,
    price, original_price, discount_percent,
    rating_average, review_count, sales_count,
    stock_available, stock_quantity, stock_status,
    crawled_at
)
SELECT 
    p.product_id,
    p.name,
    p.url,
    p.image_url,
    p.description,
    -- Join to get IDs
    c.category_id,
    s.seller_id,
    b.brand_id,
    -- Numeric fields
    p.price,
    p.original_price,
    p.discount_percent,
    p.rating_average,
    p.review_count,
    p.sales_count,
    -- Stock
    p.stock_available,
    p.stock_quantity,
    p.stock_status,
    -- Timestamp
    p.crawled_at
FROM products p
LEFT JOIN dim_categories c ON p.category_url = c.url
LEFT JOIN dim_sellers s ON p.seller_id::TEXT = s.seller_id
LEFT JOIN dim_brands b ON p.brand = b.name
ON CONFLICT (product_id) DO UPDATE SET
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    updated_at = NOW();

-- Step 6: Migrate Specifications (flatten JSONB)
INSERT INTO fact_product_specifications (product_id, spec_key, spec_value)
SELECT 
    product_id,
    key as spec_key,
    value::TEXT as spec_value
FROM products,
LATERAL jsonb_each(specifications)
WHERE specifications IS NOT NULL;

-- Step 7: Migrate Images (flatten JSONB)
INSERT INTO fact_product_images (product_id, image_url, image_order, is_primary)
SELECT 
    product_id,
    elem->>'url' as image_url,
    idx as image_order,
    (idx = 0) as is_primary
FROM products,
LATERAL jsonb_array_elements(images) WITH ORDINALITY AS arr(elem, idx)
WHERE images IS NOT NULL;

-- Step 8: Compute and insert metrics
INSERT INTO fact_product_metrics (
    product_id,
    estimated_revenue,
    price_savings,
    discount_amount,
    price_category,
    popularity_score,
    value_score,
    sales_velocity
)
SELECT 
    product_id,
    -- Computed fields
    CASE WHEN sales_count IS NOT NULL AND price IS NOT NULL 
         THEN sales_count * price ELSE NULL END as estimated_revenue,
    CASE WHEN original_price > price 
         THEN original_price - price ELSE NULL END as price_savings,
    CASE WHEN original_price > price 
         THEN original_price - price ELSE NULL END as discount_amount,
    -- Price category
    CASE 
        WHEN price < 500000 THEN 'budget'
        WHEN price < 2000000 THEN 'mid-range'
        WHEN price < 10000000 THEN 'premium'
        ELSE 'luxury'
    END as price_category,
    -- Popularity score (simplified)
    CASE 
        WHEN sales_count IS NOT NULL AND rating_average IS NOT NULL 
        THEN (sales_count::NUMERIC / 100000 * 50 + rating_average / 5 * 50)
        ELSE NULL
    END as popularity_score,
    -- Value score
    CASE 
        WHEN rating_average IS NOT NULL AND price > 0
        THEN rating_average / (price / 1000000.0)
        ELSE NULL
    END as value_score,
    sales_count as sales_velocity
FROM products
WHERE product_id IS NOT NULL;

-- Step 9: Migrate Shipping (flatten JSONB)
INSERT INTO fact_product_shipping (product_id, fee, warehouse, free_shipping)
SELECT 
    product_id,
    (shipping->>'fee')::NUMERIC as fee,
    shipping->>'warehouse' as warehouse,
    COALESCE((shipping->>'free_shipping')::BOOLEAN, FALSE) as free_shipping
FROM products
WHERE shipping IS NOT NULL;

-- Step 10: Create materialized view
REFRESH MATERIALIZED VIEW view_products_enriched;

-- Step 11: Verify migration
SELECT 
    'products' as table_name,
    COUNT(*) as old_count,
    (SELECT COUNT(*) FROM fact_products) as new_count,
    (SELECT COUNT(*) FROM fact_products) - COUNT(*) as diff
FROM products
UNION ALL
SELECT 
    'categories' as table_name,
    COUNT(*) as old_count,
    (SELECT COUNT(*) FROM dim_categories) as new_count,
    (SELECT COUNT(*) FROM dim_categories) - COUNT(*) as diff
FROM categories;

-- ============================================
-- ROLLBACK (if needed)
-- ============================================
-- DROP TABLE IF EXISTS fact_product_shipping CASCADE;
-- DROP TABLE IF EXISTS fact_product_metrics CASCADE;
-- DROP TABLE IF EXISTS fact_product_images CASCADE;
-- DROP TABLE IF EXISTS fact_product_specifications CASCADE;
-- DROP TABLE IF EXISTS audit_cleaning_log CASCADE;
-- DROP TABLE IF EXISTS audit_rejected_products CASCADE;
-- DROP TABLE IF EXISTS fact_products CASCADE;
-- DROP TABLE IF EXISTS dim_brands CASCADE;
-- DROP TABLE IF EXISTS dim_sellers CASCADE;
-- DROP TABLE IF EXISTS dim_categories CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS view_products_enriched;
"""
    
    print(migration)

def main():
    """Main function"""
    print("="*60)
    print("🔍 DATABASE ANALYSIS & 3NF PROPOSAL")
    print("="*60)
    
    conn = connect_db()
    if not conn:
        return
    
    try:
        # Analyze current schema
        print("\n" + "="*60)
        print("📊 CURRENT SCHEMA ANALYSIS")
        print("="*60)
        
        # Analyze products table
        prod_cols, prod_count = analyze_table_schema(conn, 'products')
        
        # Check JSONB columns
        jsonb_cols = [col['column_name'] for col in prod_cols 
                     if col['data_type'] in ('jsonb', 'json')]
        if jsonb_cols:
            check_jsonb_structure(conn, 'products', jsonb_cols)
        
        # Analyze categories table
        cat_cols, cat_count = analyze_table_schema(conn, 'categories')
        
        # Analyze relationships
        analyze_relationships(conn)
        
        # Propose 3NF
        propose_3nf_schema()
        
        # Generate migration
        generate_migration_script()
        
        print("\n✅ Phân tích hoàn tất!")
        print(f"📄 Lưu output này để tham khảo khi implement 3NF")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\n🔌 Đã đóng kết nối database")

if __name__ == "__main__":
    main()
