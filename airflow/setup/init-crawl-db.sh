#!/bin/bash
set -e

# Use POSTGRES_DB from environment (defaults to 'tiki' from .env)
CRAWL_DB="${POSTGRES_DB:-tiki}"

echo "Setting up crawl tables in database: $CRAWL_DB"

# Connect to default database to create the crawl database if different from postgres
if [ "$CRAWL_DB" != "postgres" ]; then
    DB_EXISTS=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" -tAc "SELECT 1 FROM pg_database WHERE datname='$CRAWL_DB'" 2>/dev/null || echo "")
    
    if [ -z "$DB_EXISTS" ]; then
        echo "Creating database $CRAWL_DB..."
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
            CREATE DATABASE $CRAWL_DB;
            GRANT ALL PRIVILEGES ON DATABASE $CRAWL_DB TO $POSTGRES_USER;
EOSQL
    else
        echo "Database $CRAWL_DB already exists."
    fi
fi

# Create tables in the crawl database
echo "Creating tables in $CRAWL_DB..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$CRAWL_DB" <<-EOSQL
    -- =====================================================
    -- BẢNG CATEGORIES
    -- =====================================================
    CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        category_id VARCHAR(255) UNIQUE,
        name VARCHAR(500) NOT NULL,
        url TEXT NOT NULL UNIQUE,
        image_url TEXT,
        parent_url TEXT,
        level INTEGER,
        -- NEW: Full path hierarchy as JSONB array
        category_path JSONB,
        -- NEW: Root category name for easy grouping
        root_category_name VARCHAR(255),
        -- NEW: Is this the deepest category (no children)
        is_leaf BOOLEAN DEFAULT FALSE,
        product_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_categories_url ON categories(url);
    CREATE INDEX IF NOT EXISTS idx_categories_parent_url ON categories(parent_url);
    CREATE INDEX IF NOT EXISTS idx_categories_level ON categories(level);
    CREATE INDEX IF NOT EXISTS idx_categories_path ON categories USING GIN (category_path);
    CREATE INDEX IF NOT EXISTS idx_categories_is_leaf ON categories(is_leaf);
    
    -- =====================================================
    -- BẢNG PRODUCTS
    -- =====================================================
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(1000) NOT NULL,
        url TEXT NOT NULL,
        image_url TEXT,
        category_url TEXT,
        category_id VARCHAR(255),
        category_path JSONB,
        sales_count INTEGER,
        price DECIMAL(12, 2),
        original_price DECIMAL(12, 2),
        discount_percent INTEGER,
        rating_average DECIMAL(3, 2),
        review_count INTEGER,
        description TEXT,
        specifications JSONB,
        images JSONB,
        seller_name VARCHAR(500),
        seller_id VARCHAR(255),
        seller_is_official BOOLEAN DEFAULT FALSE,
        brand VARCHAR(255),
        stock_available BOOLEAN,
        stock_quantity INTEGER,
        stock_status VARCHAR(50),
        shipping JSONB,
        estimated_revenue DECIMAL(15, 2),
        price_savings DECIMAL(12, 2),
        price_category VARCHAR(50),
        popularity_score DECIMAL(10, 2),
        value_score DECIMAL(10, 2),
        discount_amount DECIMAL(12, 2),
        sales_velocity INTEGER,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_products_product_id ON products(product_id);
    CREATE INDEX IF NOT EXISTS idx_products_category_url ON products(category_url);
    CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
    CREATE INDEX IF NOT EXISTS idx_products_category_path ON products USING GIN (category_path);
    CREATE INDEX IF NOT EXISTS idx_products_sales_count ON products(sales_count);
    CREATE INDEX IF NOT EXISTS idx_products_crawled_at ON products(crawled_at);
    
    -- =====================================================
    -- BẢNG CRAWL_HISTORY (Price Tracking Only)
    -- =====================================================
    CREATE TABLE IF NOT EXISTS crawl_history (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(255) NOT NULL,
        price DECIMAL(12, 2),
        -- NEW: Price change compared to previous crawl
        price_change DECIMAL(12, 2),
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id);
    CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at);
    CREATE INDEX IF NOT EXISTS idx_history_product_price ON crawl_history(product_id, crawled_at, price);
    
    -- =====================================================
    -- TRIGGERS
    -- =====================================================
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS \$\$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    \$\$ language 'plpgsql';
    
    DROP TRIGGER IF EXISTS update_products_updated_at ON products;
    CREATE TRIGGER update_products_updated_at
        BEFORE UPDATE ON products
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    
    DROP TRIGGER IF EXISTS update_categories_updated_at ON categories;
    CREATE TRIGGER update_categories_updated_at
        BEFORE UPDATE ON categories
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL

echo "✅ Database $CRAWL_DB setup completed!"
