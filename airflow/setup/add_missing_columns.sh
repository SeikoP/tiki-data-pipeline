#!/bin/bash
set -e

echo "Adding missing columns to products table..."

# Sử dụng database mặc định (postgres) hoặc POSTGRES_DB nếu được set
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${POSTGRES_DB:-postgres}" <<-EOSQL
    -- Kết nối vào database crawl_data
    \c crawl_data
    
    -- Thêm seller fields nếu chưa có
    ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_name VARCHAR(500);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_id VARCHAR(255);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS seller_is_official BOOLEAN DEFAULT FALSE;
    
    -- Thêm brand và stock fields nếu chưa có
    ALTER TABLE products ADD COLUMN IF NOT EXISTS brand VARCHAR(255);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_available BOOLEAN;
    ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_quantity INTEGER;
    ALTER TABLE products ADD COLUMN IF NOT EXISTS stock_status VARCHAR(50);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS shipping JSONB;
    
    -- Thêm computed fields nếu chưa có
    ALTER TABLE products ADD COLUMN IF NOT EXISTS estimated_revenue DECIMAL(15, 2);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS price_savings DECIMAL(12, 2);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS price_category VARCHAR(50);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS popularity_score DECIMAL(10, 2);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS value_score DECIMAL(10, 2);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS discount_amount DECIMAL(12, 2);
    ALTER TABLE products ADD COLUMN IF NOT EXISTS sales_velocity INTEGER;
    
    -- Tạo index cho các cột quan trọng
    CREATE INDEX IF NOT EXISTS idx_products_seller_id ON products(seller_id);
    CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
    CREATE INDEX IF NOT EXISTS idx_products_price_category ON products(price_category);
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL

echo "✅ Missing columns added successfully!"

