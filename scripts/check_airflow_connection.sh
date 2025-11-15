#!/bin/bash
# Script kiểm tra kết nối Airflow đến PostgreSQL

echo "=========================================="
echo "Kiểm tra kết nối Airflow -> PostgreSQL"
echo "=========================================="

# Kiểm tra PostgreSQL đang chạy
echo "1. Kiểm tra PostgreSQL container..."
docker-compose ps postgres

# Kiểm tra PostgreSQL health
echo -e "\n2. Kiểm tra PostgreSQL health..."
docker-compose exec -T postgres pg_isready -U ${POSTGRES_USER:-airflow_user}

# Kiểm tra network connectivity
echo -e "\n3. Kiểm tra network từ Airflow container..."
docker-compose exec -T airflow-init ping -c 2 postgres || echo "⚠️  Không thể ping postgres"

# Kiểm tra connection string
echo -e "\n4. Kiểm tra connection string trong Airflow..."
docker-compose exec -T airflow-init env | grep SQL_ALCHEMY_CONN

# Test connection trực tiếp
echo -e "\n5. Test kết nối PostgreSQL từ Airflow container..."
docker-compose exec -T airflow-init python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='airflow',
        user=os.getenv('POSTGRES_USER', 'airflow_user'),
        password=os.getenv('POSTGRES_PASSWORD', '')
    )
    print('✅ Kết nối thành công!')
    conn.close()
except Exception as e:
    print(f'❌ Lỗi kết nối: {e}')
"

echo -e "\n=========================================="
echo "Hoàn tất kiểm tra"
echo "=========================================="

