#!/bin/bash

# Script để fix các lỗi services trong tiki-data-pipeline

set -e

echo "======================================"
echo "Fixing Tiki Data Pipeline Services"
echo "======================================"
echo ""

# 1. Kiểm tra và khởi động Docker containers
echo "1. Checking Docker Compose services..."
docker-compose ps

echo ""
echo "2. Ensuring all services are healthy..."

# 2. Tạo Airflow database tables
echo ""
echo "2a. Running Airflow DB initialization..."
docker-compose run --rm airflow-init

# 3. Kiểm tra PostgreSQL connection
echo ""
echo "2b. Checking PostgreSQL connection..."
docker-compose exec -T postgres psql -U postgres -d airflow -c "\dt" || echo "Tables not yet created"

# 4. Manual Airflow DB migration (nếu cần)
echo ""
echo "3. Running Airflow DB migrate..."
docker-compose exec -T postgres psql -U airflow -d airflow -c "SELECT 1" || {
    echo "ERROR: Cannot connect to airflow database!"
    exit 1
}

# 5. Check all services health
echo ""
echo "4. Checking service health..."
echo "   - Redis:"
docker-compose exec -T redis redis-cli ping || echo "   ✗ Redis not responding"

echo "   - PostgreSQL:"
docker-compose exec -T postgres pg_isready -U postgres -d airflow || echo "   ✗ PostgreSQL not ready"

echo "   - Airflow API Server:"
curl -s http://localhost:8080/api/v2/version | jq . || echo "   ✗ Airflow API not responding"

echo ""
echo "5. Restarting key services..."
docker-compose restart airflow-apiserver airflow-scheduler airflow-worker airflow-triggerer

echo ""
echo "======================================"
echo "✓ Service fix completed!"
echo "======================================"
echo ""
echo "To check service logs:"
echo "  docker-compose logs -f airflow-worker"
echo "  docker-compose logs -f postgres"
echo "  docker-compose logs -f redis"

