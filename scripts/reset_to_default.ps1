# Script để reset cấu hình về mặc định (development)
# Giá trị mặc định:
# - POSTGRES_USER=postgres
# - POSTGRES_PASSWORD=postgres
# - REDIS_PASSWORD= (empty)
# - _AIRFLOW_WWW_USER_USERNAME=admin
# - _AIRFLOW_WWW_USER_PASSWORD=admin

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Reset cấu hình về mặc định" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

$envFile = ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "❌ Không tìm thấy file .env" -ForegroundColor Red
    exit 1
}

Write-Host "Đang cập nhật file .env về giá trị mặc định..." -ForegroundColor Cyan

# Đọc file .env hiện tại
$envContent = Get-Content $envFile

# Cập nhật các giá trị
$newContent = @()
foreach ($line in $envContent) {
    if ($line -match "^POSTGRES_USER=") {
        $newContent += "POSTGRES_USER=postgres"
    }
    elseif ($line -match "^POSTGRES_PASSWORD=") {
        $newContent += "POSTGRES_PASSWORD=postgres"
    }
    elseif ($line -match "^REDIS_PASSWORD=") {
        $newContent += "REDIS_PASSWORD="
    }
    elseif ($line -match "^_AIRFLOW_WWW_USER_USERNAME=") {
        $newContent += "_AIRFLOW_WWW_USER_USERNAME=admin"
    }
    elseif ($line -match "^_AIRFLOW_WWW_USER_PASSWORD=") {
        $newContent += "_AIRFLOW_WWW_USER_PASSWORD=admin"
    }
    else {
        $newContent += $line
    }
}

# Ghi lại file
$newContent | Set-Content $envFile -Encoding UTF8

Write-Host "✅ File .env đã được cập nhật" -ForegroundColor Green
Write-Host ""

# Kiểm tra PostgreSQL container
$postgresStatus = docker-compose ps postgres 2>&1
if ($LASTEXITCODE -eq 0 -and ($postgresStatus -match "Up" -or $postgresStatus -match "healthy")) {
    Write-Host "Đang reset password trong database..." -ForegroundColor Cyan
    
    # Reset password cho user postgres
    $resetCmd = "ALTER USER postgres WITH PASSWORD 'postgres';"
    docker-compose exec -T postgres psql -U postgres -c $resetCmd 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Password cho user postgres da duoc reset" -ForegroundColor Green
    } else {
        Write-Host "Khong the reset password cho user postgres (co the user chua ton tai)" -ForegroundColor Yellow
    }
    
    # Cap quyen cho user postgres
    $grant1 = "GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;"
    $grant2 = "GRANT ALL PRIVILEGES ON DATABASE crawl_data TO postgres;"
    docker-compose exec -T postgres psql -U postgres -c $grant1 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -c $grant2 2>&1 | Out-Null
    
    Write-Host "Quyen da duoc cap cho user postgres" -ForegroundColor Green
} else {
    Write-Host "PostgreSQL container khong chay, bo qua buoc reset password" -ForegroundColor Yellow
    Write-Host "   Chay: docker-compose up -d postgres" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Hoàn tất" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Lưu ý:" -ForegroundColor Yellow
Write-Host "   - File .env đã được cập nhật về giá trị mặc định" -ForegroundColor White
Write-Host "   - Để áp dụng thay đổi, restart các services:" -ForegroundColor White
Write-Host "     docker-compose down" -ForegroundColor Gray
Write-Host "     docker-compose up -d" -ForegroundColor Gray
Write-Host ""

