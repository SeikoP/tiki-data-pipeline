# PowerShell script để clear task metadata trong Airflow khi task đã bị xóa khỏi DAG

param(
    [string]$DagId = "tiki_crawl_products_v2",
    [string]$TaskId = "transform_and_load.update_category_product_counts"
)

Write-Host "🔄 Clearing task metadata for: ${DagId}.${TaskId}" -ForegroundColor Cyan

# Sử dụng docker-compose exec để chạy airflow CLI trong container
try {
    docker-compose exec -T airflow-scheduler airflow tasks clear "${DagId}" --task-ids "${TaskId}" --yes
    Write-Host "✅ Done! Task metadata cleared." -ForegroundColor Green
} catch {
    Write-Host "⚠️  Error using airflow-scheduler, trying webserver..." -ForegroundColor Yellow
    try {
        docker-compose exec -T airflow-webserver airflow tasks clear "${DagId}" --task-ids "${TaskId}" --yes
        Write-Host "✅ Done! Task metadata cleared." -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to clear task metadata. Make sure containers are running:" -ForegroundColor Red
        Write-Host "   docker-compose ps" -ForegroundColor Yellow
        Write-Host "   docker-compose up -d" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "💡 Nếu vẫn còn lỗi, có thể cần:" -ForegroundColor Cyan
Write-Host "   1. Reload DAG trong Airflow UI (http://localhost:8080)" -ForegroundColor White
Write-Host "   2. Hoặc restart scheduler: docker-compose restart airflow-scheduler" -ForegroundColor White
