#!/bin/bash
# Script chờ PostgreSQL sẵn sàng

set -e

host="${POSTGRES_HOST:-postgres}"
port="${POSTGRES_PORT:-5432}"
user="${POSTGRES_USER:-airflow_user}"
password="${POSTGRES_PASSWORD:-}"
database="${POSTGRES_DB:-airflow}"
max_retries=30
retry_interval=2

echo "Waiting for PostgreSQL to be ready..."
echo "Host: $host, Port: $port, User: $user, Database: $database"

for i in $(seq 1 $max_retries); do
    if PGPASSWORD="$password" psql -h "$host" -p "$port" -U "$user" -d "$database" -c "SELECT 1" > /dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        exit 0
    fi
    echo "⏳ Attempt $i/$max_retries: PostgreSQL is not ready yet. Waiting ${retry_interval}s..."
    sleep $retry_interval
done

echo "❌ PostgreSQL is not ready after $max_retries attempts"
exit 1

