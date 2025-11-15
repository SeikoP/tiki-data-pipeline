#!/bin/bash
set -e

echo "Adding category_id and category_path columns to products table..."

# Kiểm tra xem psql có sẵn không
if ! command -v psql &> /dev/null; then
    echo "⚠️  psql command not found. Using Python script instead..."
    echo "💡 Chạy: python airflow/setup/add_category_fields.py"
    exit 1
fi

# Sử dụng database mặc định (postgres) hoặc POSTGRES_DB nếu được set
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${POSTGRES_DB:-postgres}" <<-EOSQL
    -- Kết nối vào database crawl_data
    \c crawl_data
    
    -- Thêm category_id để link với categories table
    ALTER TABLE products ADD COLUMN IF NOT EXISTS category_id VARCHAR(255);
    
    -- Thêm category_path để làm breadcrumb (JSONB để lưu array)
    ALTER TABLE products ADD COLUMN IF NOT EXISTS category_path JSONB;
    
    -- Tạo index cho category_id để tối ưu join với categories
    CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
    
    -- Tạo index cho category_path (GIN index cho JSONB)
    CREATE INDEX IF NOT EXISTS idx_products_category_path ON products USING GIN (category_path);
    
    -- Update category_id từ category_url nếu có thể extract
    -- Pattern: /slug/c{category_id} -> c{category_id}
    UPDATE products 
    SET category_id = 'c' || substring(category_url from '/c([0-9]+)')
    WHERE category_id IS NULL 
      AND category_url IS NOT NULL 
      AND category_url ~ '/c[0-9]+';
    
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL

echo "✅ Category fields added successfully!"

