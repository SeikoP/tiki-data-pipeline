# Script để chuẩn bị môi trường trước khi chạy harness
# Kiểm tra và dừng container API nếu đang chạy

Write-Host "Đang kiểm tra container API..." -ForegroundColor Yellow

$containerName = "tiki-data-pipeline-api-1"

# Kiểm tra container có đang chạy không
$container = docker ps -a --filter "name=$containerName" --format "{{.Names}} {{.Status}}" 2>$null

if ($container -and $container -match "Up") {
    Write-Host "Tìm thấy container $containerName đang chạy" -ForegroundColor Red
    Write-Host "Đang dừng container..." -ForegroundColor Yellow
    docker stop $containerName | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Đã dừng container $containerName" -ForegroundColor Green
    } else {
        Write-Host "Không thể dừng container $containerName" -ForegroundColor Red
        exit 1
    }
} else {
    Write-Host "Không có container API đang chạy" -ForegroundColor Green
}

# Kiểm tra port 3002
Write-Host "`nĐang kiểm tra port 3002..." -ForegroundColor Yellow
Start-Sleep -Seconds 2  # Đợi một chút để port được giải phóng

$connection = Get-NetTCPConnection -LocalPort 3002 -ErrorAction SilentlyContinue -State Listen

if ($connection) {
    $processId = $connection.OwningProcess
    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
    
    if ($process) {
        Write-Host "Port 3002 vẫn còn bị chiếm bởi process: $($process.ProcessName) (PID: $processId)" -ForegroundColor Red
        Write-Host "Vui lòng dừng process này hoặc đợi một chút rồi thử lại" -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "Port 3002 đã sẵn sàng!" -ForegroundColor Green
}

Write-Host "`nMôi trường đã sẵn sàng để chạy harness" -ForegroundColor Green

