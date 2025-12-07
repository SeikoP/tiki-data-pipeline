# Script để quản lý Docker services - Tạm dừng/Khởi động khi cần
# Usage:
#   .\scripts\manage-services.ps1 stop    # Tạm dừng tất cả services
#   .\scripts\manage-services.ps1 start   # Khởi động lại services
#   .\scripts\manage-services.ps1 status  # Xem trạng thái
#   .\scripts\manage-services.ps1 light   # Chỉ chạy services cần thiết (postgres, redis, apiserver)

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("stop", "start", "restart", "status", "light")]
    [string]$Action
)

$ErrorActionPreference = "Stop"

function Stop-Services {
    Write-Host "🛑 Đang tạm dừng tất cả services..." -ForegroundColor Yellow
    docker-compose down
    Write-Host "✅ Đã tạm dừng tất cả services" -ForegroundColor Green
}

function Start-Services {
    Write-Host "🚀 Đang khởi động tất cả services..." -ForegroundColor Cyan
    docker-compose up -d
    Write-Host "✅ Đã khởi động tất cả services" -ForegroundColor Green
    Write-Host "📊 Xem logs: docker-compose logs -f" -ForegroundColor Gray
}

function Restart-Services {
    Write-Host "🔄 Đang khởi động lại services..." -ForegroundColor Cyan
    docker-compose restart
    Write-Host "✅ Đã khởi động lại services" -ForegroundColor Green
}

function Show-Status {
    Write-Host "📊 Trạng thái services:" -ForegroundColor Cyan
    docker-compose ps
    Write-Host "`n💾 Sử dụng tài nguyên:" -ForegroundColor Cyan
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"
}

function Start-LightMode {
    Write-Host "💡 Khởi động chế độ nhẹ (chỉ postgres, redis, apiserver)..." -ForegroundColor Cyan
    
    # Tạm dừng các services nặng
    docker-compose stop airflow-worker airflow-scheduler airflow-dag-processor airflow-triggerer
    
    # Khởi động các services cần thiết
    docker-compose up -d postgres redis airflow-apiserver
    
    Write-Host "✅ Đã khởi động chế độ nhẹ" -ForegroundColor Green
    Write-Host "   - Chạy: postgres, redis, airflow-apiserver" -ForegroundColor Gray
    Write-Host "   - Tạm dừng: worker, scheduler, dag-processor, triggerer" -ForegroundColor Gray
    Write-Host "`n💡 Để khởi động lại tất cả: .\scripts\manage-services.ps1 start" -ForegroundColor Yellow
}

switch ($Action) {
    "stop" { Stop-Services }
    "start" { Start-Services }
    "restart" { Restart-Services }
    "status" { Show-Status }
    "light" { Start-LightMode }
}

