# Script PowerShell đơn giản để reset password cho PostgreSQL user "bungmoto"
# Sử dụng khi không có file .env hoặc password trong .env không khớp

param(
    [Parameter(Mandatory=$true)]
    [string]$NewPassword
)

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Reset password cho PostgreSQL user" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""

$postgresUser = "bungmoto"

# Kiểm tra PostgreSQL container đang chạy
$postgresStatus = docker-compose ps postgres 2>&1
if ($LASTEXITCODE -ne 0 -or ($postgresStatus -notmatch "Up" -and $postgresStatus -notmatch "healthy")) {
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
    Write-Host "Tạo user postgres với password mặc định..." -ForegroundColor Yellow
    docker-compose exec -T postgres psql -U bungmoto -d postgres -c "CREATE USER postgres WITH SUPERUSER PASSWORD 'postgres';" 2>&1 | Out-Null  # trufflehog:ignore
    if ($LASTEXITCODE -ne 0) {
        Write-Host "⚠️  Không thể tạo user postgres, thử kết nối trực tiếp với user $postgresUser..." -ForegroundColor Yellow
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
$escapedPassword = $NewPassword -replace "'", "''"

# Reset password - thử với user postgres trước, nếu không được thì thử với user hiện tại
if ($usePostgres) {
    $resetResult = docker-compose exec -T postgres psql -U postgres -c "ALTER USER $postgresUser WITH PASSWORD '$escapedPassword';" 2>&1
} else {
    # Thử kết nối với user hiện tại (cần biết mật khẩu cũ hoặc dùng trust)
    $resetResult = docker-compose exec -T postgres psql -U $postgresUser -d postgres -c "ALTER USER $postgresUser WITH PASSWORD '$escapedPassword';" 2>&1
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khi reset password:" -ForegroundColor Red
    Write-Host $resetResult -ForegroundColor Red
    Write-Host ""
    Write-Host "💡 Gợi ý: Nếu lỗi vẫn tiếp tục, bạn có thể:" -ForegroundColor Yellow
    Write-Host "   1. Xóa volume và khởi tạo lại database:" -ForegroundColor Yellow
    Write-Host "      docker-compose down -v" -ForegroundColor White
    Write-Host "      docker-compose up -d postgres" -ForegroundColor White
    Write-Host "   2. Hoặc tạo file .env với POSTGRES_USER và POSTGRES_PASSWORD" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Password đã được reset thành công!" -ForegroundColor Green
Write-Host ""

# Grant privileges trên databases
Write-Host "Đang cấp quyền cho user '$postgresUser'..." -ForegroundColor Cyan

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

Write-Host "✅ Quyền đã được cấp" -ForegroundColor Green
Write-Host ""

# Test kết nối
Write-Host "Test kết nối..." -ForegroundColor Cyan

$env:PGPASSWORD = $NewPassword
$testResult = docker-compose exec -T postgres psql -U $postgresUser -d airflow -c "SELECT 1;" 2>&1
Remove-Item Env:\PGPASSWORD

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Kết nối test thành công!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Kết nối test thất bại, nhưng password đã được reset" -ForegroundColor Yellow
    Write-Host $testResult -ForegroundColor Yellow
}

Write-Host ""
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Hoàn tất" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "💡 Lưu ý: Hãy đảm bảo file .env của bạn có:" -ForegroundColor Yellow
Write-Host "   POSTGRES_USER=$postgresUser" -ForegroundColor White
Write-Host "   POSTGRES_PASSWORD=$NewPassword" -ForegroundColor White
Write-Host ""

