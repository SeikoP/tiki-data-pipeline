#!/bin/bash
set -e

echo "Creating Airflow database..."

# Check and generate FERNET_KEY if not set
# FERNET_KEY is used for encrypting Variables and Connections in Airflow
if [ -z "$AIRFLOW__CORE__FERNET_KEY" ]; then
    echo "⚠️  Warning: AIRFLOW__CORE__FERNET_KEY is not set in .env"
    echo "   Airflow will generate a new key, but it won't persist across container restarts"
    echo "   To set a persistent key, add to .env:"
    echo "   AIRFLOW__CORE__FERNET_KEY=\$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
fi

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

