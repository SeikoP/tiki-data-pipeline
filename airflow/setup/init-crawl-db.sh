#!/bin/bash
set -e

echo "Creating crawl_data database for crawled products and categories..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Tạo database cho dữ liệu crawl
    CREATE DATABASE crawl_data;
    GRANT ALL PRIVILEGES ON DATABASE crawl_data TO $POSTGRES_USER;
    
    -- Kết nối vào database crawl_data để tạo schema
    \c crawl_data
    
    -- Bảng categories
    CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        category_id VARCHAR(255) UNIQUE,
        name VARCHAR(500) NOT NULL,
        url TEXT NOT NULL UNIQUE,
        image_url TEXT,
        parent_url TEXT,
        level INTEGER,
        product_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_categories_url ON categories(url);
    CREATE INDEX idx_categories_parent_url ON categories(parent_url);
    CREATE INDEX idx_categories_level ON categories(level);
    
    -- Bảng products
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_id VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(1000) NOT NULL,
        url TEXT NOT NULL,
        image_url TEXT,
        category_url TEXT,
        sales_count INTEGER,
        price DECIMAL(12, 2),
        original_price DECIMAL(12, 2),
        discount_percent INTEGER,
        rating_average DECIMAL(3, 2),
        review_count INTEGER,
        description TEXT,
        specifications JSONB,
        images JSONB,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_products_product_id ON products(product_id);
    CREATE INDEX idx_products_category_url ON products(category_url);
    CREATE INDEX idx_products_sales_count ON products(sales_count);
    CREATE INDEX idx_products_crawled_at ON products(crawled_at);
    
    -- Bảng crawl_history để track lịch sử crawl
    CREATE TABLE IF NOT EXISTS crawl_history (
        id SERIAL PRIMARY KEY,
        crawl_type VARCHAR(50) NOT NULL, -- 'categories', 'products', 'product_detail'
        category_url TEXT,
        product_id VARCHAR(255),
        status VARCHAR(20) NOT NULL, -- 'success', 'failed', 'partial'
        items_count INTEGER DEFAULT 0,
        error_message TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP
    );
    
    CREATE INDEX idx_crawl_history_type ON crawl_history(crawl_type);
    CREATE INDEX idx_crawl_history_started_at ON crawl_history(started_at);
    
    -- Function để tự động update updated_at
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS \$\$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    \$\$ language 'plpgsql';
    
    -- Trigger để tự động update updated_at cho products
    DROP TRIGGER IF EXISTS update_products_updated_at ON products;
    CREATE TRIGGER update_products_updated_at
        BEFORE UPDATE ON products
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    
    -- Trigger để tự động update updated_at cho categories
    DROP TRIGGER IF EXISTS update_categories_updated_at ON categories;
    CREATE TRIGGER update_categories_updated_at
        BEFORE UPDATE ON categories
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL

echo "Crawl data database and schema created successfully!"

