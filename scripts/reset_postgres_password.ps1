# Script PowerShell để reset password cho PostgreSQL user
# Sử dụng khi password trong .env không khớp với password trong database

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Reset password cho PostgreSQL user" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

# Load environment variables từ .env file
$envFile = ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "❌ Không tìm thấy file .env" -ForegroundColor Red
    exit 1
}

$envContent = Get-Content $envFile
$postgresUser = ""
$postgresPassword = ""

foreach ($line in $envContent) {
    if ($line -match "^POSTGRES_USER=(.+)$") {
        $postgresUser = $matches[1].Trim()
    }
    if ($line -match "^POSTGRES_PASSWORD=(.+)$") {
        $postgresPassword = $matches[1].Trim()
    }
}

if ([string]::IsNullOrEmpty($postgresUser)) {
    $postgresUser = "bungmoto"
    Write-Host "⚠️  POSTGRES_USER không được set, sử dụng mặc định: $postgresUser" -ForegroundColor Yellow
}

if ([string]::IsNullOrEmpty($postgresPassword)) {
    Write-Host "❌ POSTGRES_PASSWORD không được set trong .env file" -ForegroundColor Red
    exit 1
}

Write-Host "User: $postgresUser" -ForegroundColor White
Write-Host ""

# Kiểm tra PostgreSQL container đang chạy
$postgresStatus = docker-compose ps postgres 2>&1
if ($LASTEXITCODE -ne 0 -or $postgresStatus -notmatch "Up") {
    Write-Host "❌ PostgreSQL container không chạy" -ForegroundColor Red
    Write-Host "   Khởi động: docker-compose up -d postgres" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ PostgreSQL container đang chạy" -ForegroundColor Green
Write-Host ""

# Kiểm tra xem user postgres có tồn tại không, nếu không thì tạo
Write-Host "Kiểm tra user postgres..." -ForegroundColor Cyan
$postgresExists = docker-compose exec -T postgres psql -U postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='postgres';" 2>&1
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($postgresExists)) {
    Write-Host "User postgres không tồn tại, đang tạo user postgres với quyền superuser..." -ForegroundColor Yellow
    # Thử kết nối với user hiện tại để tạo user postgres
    docker-compose exec -T postgres psql -U $postgresUser -d postgres -c "CREATE USER postgres WITH SUPERUSER PASSWORD 'postgres';" 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  Không thể tạo user postgres, thử reset password trực tiếp với user $postgresUser..." -ForegroundColor Yellow
        Write-Host "   (Cần biết mật khẩu cũ hoặc sử dụng script reset_postgres_password_simple.ps1)" -ForegroundColor Yellow
        $usePostgres = $false
    } else {
        Write-Host "✅ User postgres đã được tạo" -ForegroundColor Green
        $usePostgres = $true
    }
} else {
    Write-Host "✅ User postgres đã tồn tại" -ForegroundColor Green
    $usePostgres = $true
}

Write-Host ""

# Reset password cho user
Write-Host "Đang reset password cho user '$postgresUser'..." -ForegroundColor Cyan

# Escape single quotes trong password nếu có
$escapedPassword = $postgresPassword -replace "'", "''"

# Reset password
if ($usePostgres) {
    docker-compose exec -T postgres psql -U postgres -c "ALTER USER $postgresUser WITH PASSWORD '$escapedPassword';" 2>&1 | Out-Null
} else {
    # Thử với user hiện tại (có thể sẽ fail nếu không biết mật khẩu cũ)
    docker-compose exec -T postgres psql -U $postgresUser -d postgres -c "ALTER USER $postgresUser WITH PASSWORD '$escapedPassword';" 2>&1 | Out-Null
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khi reset password" -ForegroundColor Red
    Write-Host "   Thử sử dụng script reset_postgres_password_simple.ps1 với mật khẩu mới:" -ForegroundColor Yellow
    Write-Host "   .\scripts\reset_postgres_password_simple.ps1 -NewPassword 'your_password'" -ForegroundColor White
    exit 1
}

# Grant privileges trên databases
if ($usePostgres) {
    docker-compose exec -T postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE crawl_data TO $postgresUser;" 2>&1 | Out-Null
    
    # Grant privileges trên schema public cho airflow
    docker-compose exec -T postgres psql -U postgres -d airflow -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d airflow -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d airflow -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d airflow -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $postgresUser;" 2>&1 | Out-Null
    
    # Grant privileges trên schema public cho crawl_data
    docker-compose exec -T postgres psql -U postgres -d crawl_data -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d crawl_data -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d crawl_data -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $postgresUser;" 2>&1 | Out-Null
    docker-compose exec -T postgres psql -U postgres -d crawl_data -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $postgresUser;" 2>&1 | Out-Null
}

Write-Host ""
Write-Host "✅ Password đã được reset thành công!" -ForegroundColor Green
Write-Host ""
Write-Host "Test kết nối..." -ForegroundColor Cyan

$env:PGPASSWORD = $postgresPassword
$testResult = docker-compose exec -T postgres psql -U $postgresUser -d airflow -c "SELECT 1;" 2>&1
Remove-Item Env:\PGPASSWORD

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Kết nối test thành công!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Kết nối test thất bại, nhưng password đã được reset" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Hoàn tất" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

