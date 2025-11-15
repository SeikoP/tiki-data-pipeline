# Script restore PostgreSQL database từ backup file
# Cách sử dụng: .\scripts\restore-postgres.ps1 -BackupFile "backups/postgres/crawl_data_20241115_120000.dump" -Database "crawl_data"

param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile,
    
    [Parameter(Mandatory=$true)]
    [string]$Database
)

Write-Host "🔄 PostgreSQL Restore Script" -ForegroundColor Cyan
Write-Host ""

# Kiểm tra file backup
if (-not (Test-Path $BackupFile)) {
    Write-Host "❌ File backup không tồn tại: $BackupFile" -ForegroundColor Red
    exit 1
}

Write-Host "📁 File backup: $BackupFile" -ForegroundColor Cyan

# Kiểm tra container
$containerName = "tiki-data-pipeline-postgres-1"
$container = docker ps --filter "name=$containerName" --format "{{.Names}}"

if (-not $container) {
    Write-Host "❌ Container PostgreSQL không đang chạy!" -ForegroundColor Red
    Write-Host "💡 Chạy: docker compose up -d postgres" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Container PostgreSQL đang chạy: $containerName" -ForegroundColor Green

# Lấy thông tin từ .env
$envFile = ".env"
if (-not (Test-Path $envFile)) {
    Write-Host "❌ File .env không tồn tại!" -ForegroundColor Red
    exit 1
}

$postgresUser = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_USER=").ToString().Split("=")[1]
$postgresPassword = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_PASSWORD=").ToString().Split("=")[1]

if (-not $postgresUser) {
    $postgresUser = "airflow_user"
}

Write-Host "📊 Database: $Database" -ForegroundColor Cyan
Write-Host "📊 User: $postgresUser" -ForegroundColor Cyan
Write-Host ""

# Xác nhận restore
$confirm = Read-Host "⚠️  Cảnh báo: Restore sẽ ghi đè database hiện tại. Bạn có chắc chắn? (yes/no)"
if ($confirm -ne "yes") {
    Write-Host "❌ Đã hủy restore" -ForegroundColor Yellow
    exit 0
}

Write-Host ""
Write-Host "🔄 Đang restore database..." -ForegroundColor Yellow

# Copy file vào container
$containerBackupPath = "/tmp/restore_backup.dump"
docker cp $BackupFile "${containerName}:${containerBackupPath}"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khi copy file vào container" -ForegroundColor Red
    exit 1
}

# Restore database
Write-Host "📦 Đang restore từ backup file..." -ForegroundColor Yellow
docker exec -e PGPASSWORD=$postgresPassword $containerName \
    pg_restore -U $postgresUser -d $Database -c -v "$containerBackupPath"

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Đã restore thành công!" -ForegroundColor Green
} else {
    Write-Host "❌ Lỗi khi restore database" -ForegroundColor Red
    exit 1
}

# Xóa file tạm trong container
docker exec $containerName rm -f "$containerBackupPath"

Write-Host ""
Write-Host "✅ Hoàn tất restore!" -ForegroundColor Green

