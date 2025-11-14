#!/bin/bash
set -e

# Script này sẽ tự động chạy khi Postgres container khởi động lần đầu
# Tạo cả 2 databases: airflow và nuq

echo "Creating multiple databases..."

# Tạo user và database cho Airflow
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Tạo user airflow
    CREATE USER airflow WITH PASSWORD 'airflow';
    
    -- Tạo database airflow
    CREATE DATABASE airflow OWNER airflow;
    
    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

# Tạo database nuq (dùng user postgres)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Tạo database nuq
    CREATE DATABASE nuq OWNER postgres;
    
    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE nuq TO postgres;
EOSQL

echo "Multiple databases created successfully!"

