# Script backup PostgreSQL database
# Chạy script này để backup database ra thư mục backups/postgres

param(
    [string]$Database = "all",  # "all", "airflow", "tiki"
    [string]$Format = "sql"     # "sql" (recommended), "custom", "tar"
)

Write-Host "🗄️  PostgreSQL Backup Script" -ForegroundColor Cyan
Write-Host "💡 Format: $Format (sql = plain text, dễ restore & tương thích)" -ForegroundColor Yellow
Write-Host ""

# Kiểm tra container có đang chạy không
$containerName = "tiki-data-pipeline-postgres-1"
$container = docker ps --filter "name=$containerName" --format "{{.Names}}"

if (-not $container) {
    Write-Host "❌ Container PostgreSQL không đang chạy!" -ForegroundColor Red
    Write-Host "💡 Chạy: docker compose up -d postgres" -ForegroundColor Yellow
    exit 1
}

Write-Host "✅ Container PostgreSQL đang chạy: $containerName" -ForegroundColor Green

# Lấy thông tin từ env file hợp nhất
$envFile = if ($env:ENV_FILE_PATH -and (Test-Path $env:ENV_FILE_PATH)) {
    $env:ENV_FILE_PATH
} elseif (Test-Path ".env") {
    ".env"
} else {
    ".env"
}
if (-not (Test-Path $envFile)) {
    Write-Host "❌ Không tìm thấy env file: $envFile" -ForegroundColor Red
    exit 1
}

$postgresUser = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_USER=").ToString().Split("=")[1]
$postgresPassword = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_PASSWORD=").ToString().Split("=")[1]
$postgresDb = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_DB=").ToString().Split("=")[1]

if (-not $postgresUser) {
    $postgresUser = "airflow_user"
}
if (-not $postgresDb) {
    $postgresDb = "tiki"
}

Write-Host "📊 User: $postgresUser" -ForegroundColor Cyan
Write-Host ""

# Tạo tên file backup
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupDir = "backups/postgres"

# Đảm bảo thư mục tồn tại
New-Item -ItemType Directory -Force -Path $backupDir | Out-Null

# Backup function
function Backup-Database {
    param(
        [string]$DbName,
        [string]$BackupFormat
    )
    
    $backupFile = "$backupDir/${DbName}_${timestamp}"
    
    Write-Host "📦 Đang backup database: $DbName..." -ForegroundColor Yellow
    
    if ($BackupFormat -eq "custom") {
        $backupFile += ".dump"
        $containerBackupFile = "/tmp/$(Split-Path $backupFile -Leaf)"
        $env:PGPASSWORD = $postgresPassword
        # Dùng -f để pg_dump ghi trực tiếp file trong container (tránh binary corruption khi redirect)
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_dump -U $postgresUser -Fc --no-owner --no-acl -f "$containerBackupFile" $DbName
        if ($LASTEXITCODE -eq 0) {
            # Copy file từ container ra host
            docker cp "$($containerName):$containerBackupFile" $backupFile
            Write-Host "✅ Đã backup: $backupFile" -ForegroundColor Green
        } else {
            Write-Host "❌ Lỗi khi backup $DbName" -ForegroundColor Red
        }
    } elseif ($BackupFormat -eq "sql") {
        $backupFile += ".sql"
        $containerBackupFile = "/tmp/$(Split-Path $backupFile -Leaf)"
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_dump -U $postgresUser --format=plain --no-owner --no-acl -f "$containerBackupFile" $DbName
        if ($LASTEXITCODE -eq 0) {
            # Copy file từ container ra host
            docker cp "$($containerName):$containerBackupFile" $backupFile
            Write-Host "✅ Đã backup: $backupFile (SQL plain text)" -ForegroundColor Green
        } else {
            Write-Host "❌ Lỗi khi backup $DbName" -ForegroundColor Red
        }
    } elseif ($BackupFormat -eq "tar") {
        $backupFile += ".tar"
        $containerBackupFile = "/tmp/$(Split-Path $backupFile -Leaf)"
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_dump -U $postgresUser -Ft --no-owner --no-acl -f "$containerBackupFile" $DbName
        if ($LASTEXITCODE -eq 0) {
            # Copy file từ container ra host
            docker cp "$($containerName):$containerBackupFile" $backupFile
            Write-Host "✅ Đã backup: $backupFile" -ForegroundColor Green
        } else {
            Write-Host "❌ Lỗi khi backup $DbName" -ForegroundColor Red
        }
    }
}

# Thực hiện backup
if ($Database -eq "all") {
    Write-Host "🔄 Backup database $postgresDb..." -ForegroundColor Cyan
    Backup-Database -DbName $postgresDb -BackupFormat $Format
} else {
    Backup-Database -DbName $Database -BackupFormat $Format
}

Write-Host ""
Write-Host "✅ Hoàn tất backup!" -ForegroundColor Green
Write-Host "📁 Thư mục backup: $backupDir" -ForegroundColor Cyan

# Hiển thị danh sách backup files
Write-Host ""
Write-Host "📋 Danh sách backup files:" -ForegroundColor Cyan
Get-ChildItem -Path $backupDir -Filter "*_$timestamp*" | ForEach-Object {
    $size = [math]::Round($_.Length / 1MB, 2)
    Write-Host "  - $($_.Name) ($size MB)" -ForegroundColor White
}
