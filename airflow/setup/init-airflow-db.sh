#!/bin/bash
set -e

echo "Creating Airflow database..."

# Sử dụng database mặc định (postgres) hoặc POSTGRES_DB nếu được set
DEFAULT_DB="${POSTGRES_DB:-postgres}"

# Kiểm tra xem database airflow đã tồn tại chưa
DB_EXISTS=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DEFAULT_DB" -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" 2>/dev/null || echo "")

if [ -z "$DB_EXISTS" ]; then
    echo "Creating airflow database..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DEFAULT_DB" <<-EOSQL
        CREATE DATABASE airflow;
        GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL
    echo "Airflow database created successfully!"
else
    echo "Airflow database already exists, skipping creation."
    # Vẫn grant privileges để đảm bảo user có quyền
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$DEFAULT_DB" <<-EOSQL
        GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL
fi

