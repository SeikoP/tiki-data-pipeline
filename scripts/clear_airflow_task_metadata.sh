#!/bin/bash
# Script để clear task metadata trong Airflow khi task đã bị xóa khỏi DAG

DAG_ID="${1:-tiki_crawl_products_v2}"
TASK_ID="${2:-transform_and_load.update_category_product_counts}"

echo "🔄 Clearing task metadata for: ${DAG_ID}.${TASK_ID}"

# Sử dụng docker-compose exec để chạy airflow CLI trong container
docker-compose exec -T airflow-scheduler airflow tasks clear "${DAG_ID}" --task-ids "${TASK_ID}" --yes || \
docker-compose exec -T airflow-webserver airflow tasks clear "${DAG_ID}" --task-ids "${TASK_ID}" --yes

echo "✅ Done! Task metadata cleared."
echo ""
echo "💡 Nếu vẫn còn lỗi, có thể cần:"
echo "   1. Reload DAG trong Airflow UI"
echo "   2. Hoặc restart scheduler: docker-compose restart airflow-scheduler"
