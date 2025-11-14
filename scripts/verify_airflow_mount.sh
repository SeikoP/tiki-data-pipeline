#!/bin/bash
# Script để verify volume mount trong Airflow container

echo "🔍 Checking Airflow volume mounts..."
echo ""

# Kiểm tra xem container có đang chạy không
if ! docker-compose ps | grep -q airflow; then
    echo "⚠️  Airflow containers are not running"
    echo "   Start them first: docker-compose --profile airflow up -d"
    exit 1
fi

echo "📁 Checking /opt/airflow/src mount:"
docker-compose exec airflow-scheduler ls -la /opt/airflow/src 2>/dev/null || echo "❌ /opt/airflow/src does not exist"

echo ""
echo "📁 Checking /opt/airflow/src/pipelines/crawl:"
docker-compose exec airflow-scheduler ls -la /opt/airflow/src/pipelines/crawl 2>/dev/null || echo "❌ /opt/airflow/src/pipelines/crawl does not exist"

echo ""
echo "📄 Checking crawl_products.py:"
docker-compose exec airflow-scheduler test -f /opt/airflow/src/pipelines/crawl/crawl_products.py && echo "✅ crawl_products.py exists" || echo "❌ crawl_products.py does not exist"

echo ""
echo "📁 Checking /opt/airflow/data mount:"
docker-compose exec airflow-scheduler ls -la /opt/airflow/data 2>/dev/null || echo "❌ /opt/airflow/data does not exist"

echo ""
echo "🧪 Testing import:"
docker-compose exec airflow-scheduler python -c "import sys; sys.path.insert(0, '/opt/airflow/src/pipelines/crawl'); import importlib.util; spec = importlib.util.spec_from_file_location('crawl_products', '/opt/airflow/src/pipelines/crawl/crawl_products.py'); module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module); print('✅ Import test: PASSED')" 2>&1 | grep -E "(✅|❌|Error|Traceback)" || echo "⚠️  Import test failed"

echo ""
echo "📊 Checking DAG status:"
docker-compose logs airflow-dag-processor --tail 5 2>&1 | grep -i "tiki_crawl_products" | tail -1 || echo "   (No recent DAG processing logs)"

echo ""
echo "💡 If mounts are missing, restart containers:"
echo "   docker-compose down"
echo "   docker-compose --profile airflow up -d"

