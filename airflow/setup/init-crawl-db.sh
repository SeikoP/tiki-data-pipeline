#!/bin/bash
set -e

CRAWL_DB="${POSTGRES_DB:-tiki}"

echo "Setting up layered crawl schema in database: $CRAWL_DB"

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

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$CRAWL_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS crawl_runs (
        id BIGSERIAL PRIMARY KEY,
        source VARCHAR(50) NOT NULL DEFAULT 'tiki',
        airflow_dag_id VARCHAR(255),
        airflow_run_id VARCHAR(255),
        started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP,
        status VARCHAR(30) NOT NULL DEFAULT 'running',
        total_raw_items INTEGER DEFAULT 0,
        total_transformed_items INTEGER DEFAULT 0,
        total_loaded_items INTEGER DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (airflow_dag_id, airflow_run_id)
    );

    CREATE TABLE IF NOT EXISTS crawl_tasks (
        id BIGSERIAL PRIMARY KEY,
        crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
        task_key VARCHAR(500) NOT NULL,
        task_type VARCHAR(80) NOT NULL,
        url TEXT,
        category_id VARCHAR(255),
        status VARCHAR(30) NOT NULL DEFAULT 'pending',
        retry_count INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (crawl_run_id, task_key)
    );

    CREATE TABLE IF NOT EXISTS raw_products (
        id BIGSERIAL PRIMARY KEY,
        crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
        tiki_product_id VARCHAR(255) NOT NULL,
        source_stage VARCHAR(50) NOT NULL,
        category_id VARCHAR(255),
        raw_json JSONB NOT NULL,
        crawled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (crawl_run_id, tiki_product_id, source_stage)
    );

    CREATE TABLE IF NOT EXISTS sellers (
        id BIGSERIAL PRIMARY KEY,
        tiki_seller_id VARCHAR(255) UNIQUE,
        name VARCHAR(500),
        is_official BOOLEAN DEFAULT FALSE,
        raw_json JSONB,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS categories (
        id BIGSERIAL PRIMARY KEY,
        tiki_category_id VARCHAR(255) UNIQUE,
        category_id VARCHAR(255) UNIQUE,
        name VARCHAR(500) NOT NULL,
        url TEXT UNIQUE,
        image_url TEXT,
        parent_url TEXT,
        level INTEGER,
        category_path JSONB,
        level_1 VARCHAR(255),
        level_2 VARCHAR(255),
        level_3 VARCHAR(255),
        level_4 VARCHAR(255),
        level_5 VARCHAR(255),
        root_category_name VARCHAR(255),
        is_leaf BOOLEAN DEFAULT FALSE,
        product_count INTEGER DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS products (
        id BIGSERIAL PRIMARY KEY,
        tiki_product_id VARCHAR(255) UNIQUE,
        product_id VARCHAR(255) UNIQUE NOT NULL,
        seller_pk BIGINT REFERENCES sellers(id) ON DELETE SET NULL,
        category_pk BIGINT REFERENCES categories(id) ON DELETE SET NULL,
        name VARCHAR(1000) NOT NULL,
        short_name VARCHAR(255),
        brand VARCHAR(255),
        url TEXT NOT NULL,
        image_url TEXT,
        description TEXT,
        category_id VARCHAR(255) REFERENCES categories(category_id) ON DELETE SET NULL,
        category_url TEXT,
        seller_id VARCHAR(255),
        seller_name VARCHAR(500),
        seller_is_official BOOLEAN DEFAULT FALSE,
        price DECIMAL(12, 2),
        original_price DECIMAL(12, 2),
        discount_percent DECIMAL(5, 2),
        rating_average DECIMAL(4, 2),
        review_count INTEGER DEFAULT 0,
        sales_count INTEGER,
        stock_available BOOLEAN DEFAULT TRUE,
        shipping JSONB,
        crawl_quality VARCHAR(50) DEFAULT 'basic',
        needs_enrichment BOOLEAN DEFAULT TRUE,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS product_prices (
        id BIGSERIAL PRIMARY KEY,
        product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
        price DECIMAL(12, 2),
        original_price DECIMAL(12, 2),
        discount_rate DECIMAL(5, 2),
        recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (product_id, crawl_run_id)
    );

    CREATE TABLE IF NOT EXISTS product_metrics (
        id BIGSERIAL PRIMARY KEY,
        product_id BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
        crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
        rating_average DECIMAL(4, 2),
        review_count INTEGER,
        quantity_sold INTEGER,
        recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (product_id, crawl_run_id)
    );

    CREATE TABLE IF NOT EXISTS crawl_history (
        id BIGSERIAL PRIMARY KEY,
        product_id VARCHAR(255) NOT NULL REFERENCES products(product_id) ON DELETE CASCADE,
        crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL,
        price DECIMAL(12, 2),
        original_price DECIMAL(12, 2),
        discount_percent DECIMAL(5, 2),
        discount_amount DECIMAL(12, 2),
        price_change DECIMAL(12, 2),
        price_change_percent DECIMAL(5, 2),
        previous_price DECIMAL(12, 2),
        previous_original_price DECIMAL(12, 2),
        previous_discount_percent DECIMAL(5, 2),
        sales_count INTEGER,
        sales_change INTEGER,
        rating_average DECIMAL(4, 2),
        review_count INTEGER,
        is_flash_sale BOOLEAN DEFAULT FALSE,
        crawl_type VARCHAR(20) DEFAULT 'compat',
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_categories_tiki_category_id ON categories(tiki_category_id) WHERE tiki_category_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_categories_parent_url ON categories(parent_url);
    CREATE INDEX IF NOT EXISTS idx_categories_level ON categories(level);
    CREATE INDEX IF NOT EXISTS idx_categories_path ON categories USING GIN (category_path);
    CREATE INDEX IF NOT EXISTS idx_categories_is_leaf ON categories(is_leaf);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_products_tiki_product_id ON products(tiki_product_id) WHERE tiki_product_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_products_seller_pk ON products(seller_pk);
    CREATE INDEX IF NOT EXISTS idx_products_category_pk ON products(category_pk);
    CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
    CREATE INDEX IF NOT EXISTS idx_products_crawled_at ON products(crawled_at);
    CREATE INDEX IF NOT EXISTS idx_raw_products_crawl_run_id ON raw_products(crawl_run_id);
    CREATE INDEX IF NOT EXISTS idx_raw_products_tiki_product_id ON raw_products(tiki_product_id);
    CREATE INDEX IF NOT EXISTS idx_raw_products_json ON raw_products USING GIN (raw_json);
    CREATE INDEX IF NOT EXISTS idx_prices_product_recorded ON product_prices(product_id, recorded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_prices_crawl_run_id ON product_prices(crawl_run_id);
    CREATE INDEX IF NOT EXISTS idx_metrics_product_recorded ON product_metrics(product_id, recorded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_metrics_crawl_run_id ON product_metrics(crawl_run_id);
    CREATE INDEX IF NOT EXISTS idx_crawl_tasks_run_status ON crawl_tasks(crawl_run_id, status);
    CREATE INDEX IF NOT EXISTS idx_history_product_latest ON crawl_history(product_id, crawled_at DESC);
    CREATE INDEX IF NOT EXISTS idx_history_crawl_run_id ON crawl_history(crawl_run_id);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_history_product_run ON crawl_history(product_id, crawl_run_id) WHERE crawl_run_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_history_price_change ON crawl_history(price_change) WHERE price_change IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_history_flash_sale ON crawl_history(is_flash_sale) WHERE is_flash_sale = true;

    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS \$\$
    BEGIN
        NEW.updated_at = CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    \$\$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS update_crawl_runs_updated_at ON crawl_runs;
    CREATE TRIGGER update_crawl_runs_updated_at BEFORE UPDATE ON crawl_runs
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    DROP TRIGGER IF EXISTS update_crawl_tasks_updated_at ON crawl_tasks;
    CREATE TRIGGER update_crawl_tasks_updated_at BEFORE UPDATE ON crawl_tasks
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    DROP TRIGGER IF EXISTS update_sellers_updated_at ON sellers;
    CREATE TRIGGER update_sellers_updated_at BEFORE UPDATE ON sellers
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    DROP TRIGGER IF EXISTS update_categories_updated_at ON categories;
    CREATE TRIGGER update_categories_updated_at BEFORE UPDATE ON categories
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    DROP TRIGGER IF EXISTS update_products_updated_at ON products;
    CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $POSTGRES_USER;
EOSQL

echo "✅ Layered database $CRAWL_DB setup completed!"
