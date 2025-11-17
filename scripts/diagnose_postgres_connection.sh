#!/bin/bash
# Script chẩn đoán vấn đề kết nối PostgreSQL

echo "=========================================="
echo "Chẩn đoán vấn đề kết nối PostgreSQL"
echo "=========================================="

# 1. Kiểm tra PostgreSQL đang chạy
echo -e "\n1. Kiểm tra PostgreSQL container..."
docker-compose ps postgres

# 2. Kiểm tra PostgreSQL health
echo -e "\n2. Kiểm tra PostgreSQL health..."
docker-compose exec -T postgres pg_isready -U ${POSTGRES_USER:-airflow_user} || echo "⚠️  PostgreSQL không healthy"

# 3. Kiểm tra network
echo -e "\n3. Kiểm tra network connectivity..."
docker-compose exec -T airflow-init ping -c 2 postgres 2>&1 | head -5 || echo "⚠️  Không thể ping postgres"

# 4. Kiểm tra connection string trong container
echo -e "\n4. Kiểm tra environment variables trong Airflow container..."
docker-compose exec -T airflow-init env | grep -E "SQL_ALCHEMY|POSTGRES" || echo "⚠️  Không tìm thấy environment variables"

# 5. Test connection trực tiếp với psycopg2
echo -e "\n5. Test connection trực tiếp với psycopg2..."
docker-compose exec -T airflow-init python3 << 'EOF'
import os
import sys

try:
    import psycopg2
except ImportError:
    print("❌ psycopg2 chưa được cài đặt")
    sys.exit(1)

host = os.getenv("POSTGRES_HOST", "postgres")
port = int(os.getenv("POSTGRES_PORT", "5432"))
user = os.getenv("POSTGRES_USER", "airflow_user")
password = os.getenv("POSTGRES_PASSWORD", "")
database = os.getenv("POSTGRES_DB", "airflow")

print(f"Host: {host}, Port: {port}, User: {user}, Database: {database}")

# Test TCP/IP connection
try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=10,
    )
    print("✅ TCP/IP connection thành công!")
    conn.close()
except Exception as e:
    print(f"❌ TCP/IP connection failed: {e}")
    sys.exit(1)
EOF

echo -e "\n=========================================="
echo "Hoàn tất chẩn đoán"
echo "=========================================="






