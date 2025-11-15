# Script để rebuild Airflow Docker image
# Xử lý conflict khi image đang được sử dụng

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Rebuild Airflow Docker Image" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Load environment variables từ .env file
$envFile = ".env"
$airflowImageName = "tiki-airflow:3.1.3"

# Đọc từ .env file
if (Test-Path $envFile) {
    $envContent = Get-Content $envFile
    foreach ($line in $envContent) {
        if ($line -match "^AIRFLOW_IMAGE_NAME=(.+)$") {
            $airflowImageName = $matches[1].Trim()
        }
    }
}

# Nếu không tìm thấy trong .env, thử tìm từ docker images
if ($airflowImageName -eq "tiki-airflow:3.1.3") {
    $existingImages = docker images --format "{{.Repository}}:{{.Tag}}" | Select-String "tiki.*airflow.*3.1.3"
    if ($existingImages) {
        $airflowImageName = $existingImages[0].ToString().Trim()
        Write-Host "⚠️  Phát hiện image từ Docker: $airflowImageName" -ForegroundColor Yellow
    }
}

Write-Host "Image name: $airflowImageName" -ForegroundColor White
Write-Host ""

# 1. Dừng tất cả containers sử dụng image này
Write-Host "1. Dừng containers đang sử dụng image..." -ForegroundColor Cyan
$containers = docker ps -a --filter "ancestor=$airflowImageName" --format "{{.ID}}"
if ($containers) {
    Write-Host "   Đang dừng containers..." -ForegroundColor Yellow
    docker-compose down 2>&1 | Out-Null
    Write-Host "   ✅ Đã dừng containers" -ForegroundColor Green
} else {
    Write-Host "   ℹ️  Không có containers nào đang chạy" -ForegroundColor Gray
}
Write-Host ""

# 2. Xóa image cũ nếu tồn tại
Write-Host "2. Xóa image cũ..." -ForegroundColor Cyan
$imageExists = docker images $airflowImageName --format "{{.Repository}}:{{.Tag}}" 2>&1
if ($imageExists -and $LASTEXITCODE -eq 0) {
    Write-Host "   Đang xóa image $airflowImageName..." -ForegroundColor Yellow
    docker rmi -f $airflowImageName 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✅ Đã xóa image cũ" -ForegroundColor Green
    } else {
        Write-Host "   ⚠️  Không thể xóa image (có thể đang được sử dụng)" -ForegroundColor Yellow
    }
} else {
    Write-Host "   ℹ️  Image chưa tồn tại" -ForegroundColor Gray
}
Write-Host ""

# 3. Xóa các dangling images liên quan
Write-Host "3. Dọn dẹp dangling images..." -ForegroundColor Cyan
docker image prune -f 2>&1 | Out-Null
Write-Host "   ✅ Đã dọn dẹp" -ForegroundColor Green
Write-Host ""

# 4. Build lại image
Write-Host "4. Build lại image..." -ForegroundColor Cyan
Write-Host "   Đang build (có thể mất vài phút)..." -ForegroundColor Yellow
docker-compose build --no-cache airflow-init 2>&1 | Tee-Object -Variable buildOutput

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✅ Build thành công!" -ForegroundColor Green
    Write-Host ""
    Write-Host "5. Tag image cho các services khác..." -ForegroundColor Cyan
    
    # Tag image cho các services khác (nếu cần)
    # Docker Compose sẽ tự động sử dụng image đã build
    Write-Host "   ✅ Image đã sẵn sàng" -ForegroundColor Green
} else {
    Write-Host ""
    Write-Host "❌ Build thất bại!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Chi tiết lỗi:" -ForegroundColor Yellow
    $buildOutput | Select-Object -Last 20
    exit 1
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Hoàn tất" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Khởi động lại containers:" -ForegroundColor Yellow
Write-Host "   docker-compose --profile airflow up -d" -ForegroundColor White

