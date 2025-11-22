<#
.SYNOPSIS
    Convert PostgreSQL dump file (.dump) to SQL format and restore to database
    Workaround cho lỗi "unsupported version (1.16) in file header"

.DESCRIPTION
    Script này giúp:
    1. Convert file dump (.dump) sang format SQL
    2. Restore SQL file vào PostgreSQL database
    3. Kiểm tra kết quả restore

.PARAMETER BackupFile
    Đường dẫn tới file backup (.dump hoặc .sql)
    Ví dụ: "backups/postgres/backup_20251123.dump"

.PARAMETER Database
    Tên database để restore dữ liệu
    Ví dụ: "crawl_data"

.EXAMPLE
    # Restore từ file dump
    .\restore-postgres-advanced.ps1 -BackupFile "backups/postgres/backup_20251123.dump" -Database "crawl_data"

.EXAMPLE
    # Restore từ file SQL
    .\restore-postgres-advanced.ps1 -BackupFile "backups/postgres/backup_20251123.sql" -Database "crawl_data"

.NOTES
    Yêu cầu:
    - Docker Container: tiki-data-pipeline-postgres-1 phải đang chạy
    - File .env phải chứa POSTGRES_USER và POSTGRES_PASSWORD
    - PostgreSQL tools (pg_restore, psql) sẽ được chạy trong container
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile,
    
    [Parameter(Mandatory=$true)]
    [string]$Database
)

Write-Host "🔄 PostgreSQL Dump to SQL Converter & Restore" -ForegroundColor Cyan
Write-Host ""

# Kiểm tra file backup
if (-not (Test-Path $BackupFile)) {
    Write-Host "❌ File backup không tồn tại: $BackupFile" -ForegroundColor Red
    exit 1
}

# Lấy thông tin từ .env
$envFile = ".env"
$postgresUser = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_USER=").ToString().Split("=")[1]
$postgresPassword = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_PASSWORD=").ToString().Split("=")[1]
$containerName = "tiki-data-pipeline-postgres-1"

if (-not $postgresUser) {
    $postgresUser = "airflow_user"
}

Write-Host "📁 File backup: $BackupFile" -ForegroundColor Cyan
Write-Host "📊 Database: $Database" -ForegroundColor Cyan
Write-Host "📊 User: $postgresUser" -ForegroundColor Cyan
Write-Host ""

# Copy file vào container
Write-Host "📦 Copy file vào container..." -ForegroundColor Yellow
docker cp $BackupFile "${containerName}:/tmp/backup_to_convert.dump"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khi copy file" -ForegroundColor Red
    exit 1
}

# Convert dump to SQL using pg_restore -f
Write-Host "🔄 Convert dump file sang SQL format..." -ForegroundColor Yellow
docker exec $containerName pg_restore -f /tmp/backup_converted.sql /tmp/backup_to_convert.dump 2>&1 | Out-String -Stream | ForEach-Object {
    if ($_ -match "error|ERROR") {
        Write-Host "   ❌ $_" -ForegroundColor Red
    } elseif ($_ -match "warning|WARNING") {
        Write-Host "   ⚠️  $_" -ForegroundColor Yellow
    }
}

# Nếu convert thành công, restore bằng psql
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Convert thành công!" -ForegroundColor Green
    Write-Host ""
    Write-Host "🔄 Đang restore SQL file..." -ForegroundColor Yellow
    
    docker exec -e PGPASSWORD=$postgresPassword $containerName `
        psql -U $postgresUser -d $Database -f /tmp/backup_converted.sql 2>&1 | Out-String -Stream | ForEach-Object {
        if ($_ -match "error|ERROR" -and $_ -notmatch "already exists") {
            Write-Host "   ❌ $_" -ForegroundColor Red
        } elseif ($_ -match "warning|WARNING") {
            Write-Host "   ⚠️  $_" -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "❌ Convert failed, thử restore trực tiếp..." -ForegroundColor Yellow
    docker exec -e PGPASSWORD=$postgresPassword $containerName `
        pg_restore -U $postgresUser -d $Database --no-owner --no-acl /tmp/backup_to_convert.dump
}

# Cleanup
Write-Host ""
Write-Host "🧹 Cleanup..." -ForegroundColor Yellow
docker exec $containerName rm -f /tmp/backup_to_convert.dump /tmp/backup_converted.sql 2>$null | Out-Null

# Kiểm tra kết quả
Write-Host ""
Write-Host "📊 Kiểm tra dữ liệu sau restore..." -ForegroundColor Cyan
$productCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM products;" 2>$null
$categoryCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM categories;" 2>$null

if ($productCount) {
    $productCount = ($productCount | Out-String).Trim()
    Write-Host "   ✅ Products: $productCount" -ForegroundColor Green
}
if ($categoryCount) {
    $categoryCount = ($categoryCount | Out-String).Trim()
    Write-Host "   ✅ Categories: $categoryCount" -ForegroundColor Green
}

Write-Host ""
Write-Host "✅ Hoàn tất!" -ForegroundColor Green
