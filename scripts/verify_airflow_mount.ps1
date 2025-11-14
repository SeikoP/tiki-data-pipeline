# Script PowerShell để verify volume mount trong Airflow container

Write-Host "🔍 Checking Airflow volume mounts..." -ForegroundColor Cyan
Write-Host ""

# Kiểm tra xem container có đang chạy không
$containers = docker-compose ps 2>$null | Select-String "airflow"
if (-not $containers) {
    Write-Host "⚠️  Airflow containers are not running" -ForegroundColor Yellow
    Write-Host "   Start them first: docker-compose --profile airflow up -d" -ForegroundColor White
    exit 1
}

Write-Host "📁 Checking /opt/airflow/src mount:" -ForegroundColor Cyan
docker-compose exec airflow-scheduler ls -la /opt/airflow/src 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ /opt/airflow/src does not exist" -ForegroundColor Red
}

Write-Host ""
Write-Host "📁 Checking /opt/airflow/src/pipelines/crawl:" -ForegroundColor Cyan
docker-compose exec airflow-scheduler ls -la /opt/airflow/src/pipelines/crawl 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ /opt/airflow/src/pipelines/crawl does not exist" -ForegroundColor Red
}

Write-Host ""
Write-Host "📄 Checking crawl_products.py:" -ForegroundColor Cyan
docker-compose exec airflow-scheduler test -f /opt/airflow/src/pipelines/crawl/crawl_products.py 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ crawl_products.py exists" -ForegroundColor Green
} else {
    Write-Host "❌ crawl_products.py does not exist" -ForegroundColor Red
}

Write-Host ""
Write-Host "📁 Checking /opt/airflow/data mount:" -ForegroundColor Cyan
docker-compose exec airflow-scheduler ls -la /opt/airflow/data 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ /opt/airflow/data does not exist" -ForegroundColor Red
}

Write-Host ""
Write-Host "💡 If mounts are missing, restart containers:" -ForegroundColor Yellow
Write-Host "   docker-compose down" -ForegroundColor White
Write-Host "   docker-compose --profile airflow up -d" -ForegroundColor White

