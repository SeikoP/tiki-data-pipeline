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

# Xác định file format
$fileExtension = [System.IO.Path]::GetExtension($BackupFile)
Write-Host "📄 File format: $fileExtension" -ForegroundColor Cyan

# Copy file vào container với extension phù hợp
if ($fileExtension -eq ".sql") {
    $containerBackupPath = "/tmp/restore_backup.sql"
} else {
    $containerBackupPath = "/tmp/restore_backup.dump"
}

docker cp $BackupFile "${containerName}:${containerBackupPath}"

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Lỗi khi copy file vào container" -ForegroundColor Red
    exit 1
}

# Restore database theo format
Write-Host "📦 Đang restore từ backup file..." -ForegroundColor Yellow

if ($fileExtension -eq ".sql") {
    # SQL format - dùng psql
    Write-Host "💡 Sử dụng psql để restore SQL file..." -ForegroundColor Cyan
    docker exec -e PGPASSWORD=$postgresPassword $containerName `
        psql -U $postgresUser -d $Database -f "$containerBackupPath"
} else {
    # Custom/Dump format - dùng pg_restore
    Write-Host "💡 Sử dụng pg_restore để restore dump file..." -ForegroundColor Cyan
    
    # Thử restore với nhiều options khác nhau
    Write-Host "🔧 Thử method 1: pg_restore với --clean --if-exists" -ForegroundColor Cyan
    docker exec -e PGPASSWORD=$postgresPassword $containerName `
        pg_restore -U $postgresUser -d $Database --clean --if-exists --no-owner --no-acl --verbose "$containerBackupPath" 2>&1 | Out-String -Stream | ForEach-Object {
        if ($_ -match "error|ERROR") {
            Write-Host "   ❌ $_" -ForegroundColor Red
        } elseif ($_ -match "warning|WARNING") {
            Write-Host "   ⚠️  $_" -ForegroundColor Yellow
        } else {
            Write-Host "   $_" -ForegroundColor Gray
        }
    }
    
    $restoreResult = $LASTEXITCODE
    
    # Nếu failed, thử không dùng --clean
    if ($restoreResult -ne 0) {
        Write-Host ""
        Write-Host "🔧 Thử method 2: pg_restore không dùng --clean" -ForegroundColor Cyan
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_restore -U $postgresUser -d $Database --no-owner --no-acl --verbose "$containerBackupPath" 2>&1 | Out-String -Stream | ForEach-Object {
            if ($_ -match "error|ERROR") {
                Write-Host "   ❌ $_" -ForegroundColor Red
            } elseif ($_ -match "warning|WARNING") {
                Write-Host "   ⚠️  $_" -ForegroundColor Yellow
            } else {
                Write-Host "   $_" -ForegroundColor Gray
            }
        }
        
        $restoreResult = $LASTEXITCODE
    }
    
    # Nếu vẫn failed, thử với -Fc format explicit
    if ($restoreResult -ne 0) {
        Write-Host ""
        Write-Host "🔧 Thử method 3: pg_restore với -Fc format" -ForegroundColor Cyan
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_restore -U $postgresUser -d $Database -Fc --no-owner --no-acl --verbose "$containerBackupPath" 2>&1 | Out-String -Stream | ForEach-Object {
            if ($_ -match "error|ERROR") {
                Write-Host "   ❌ $_" -ForegroundColor Red
            } elseif ($_ -match "warning|WARNING") {
                Write-Host "   ⚠️  $_" -ForegroundColor Yellow
            } else {
                Write-Host "   $_" -ForegroundColor Gray
            }
        }
        
        $restoreResult = $LASTEXITCODE
    }
}

# Kiểm tra kết quả
Write-Host ""
if ($restoreResult -eq 0) {
    Write-Host "✅ Đã restore thành công!" -ForegroundColor Green
} else {
    Write-Host "⚠️  Restore có lỗi - kiểm tra logs ở trên" -ForegroundColor Yellow
}

# Xóa file tạm trong container
docker exec $containerName rm -f "$containerBackupPath" 2>$null | Out-Null

# Kiểm tra số lượng dữ liệu sau restore
Write-Host ""
Write-Host "📊 Kiểm tra dữ liệu sau restore..." -ForegroundColor Cyan
$productCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM products;" 2>$null
$categoryCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM categories;" 2>$null

if ($productCount) {
    $productCount = [int]$productCount.Trim()
    Write-Host "   Products: $productCount" -ForegroundColor Green
}
if ($categoryCount) {
    $categoryCount = [int]$categoryCount.Trim()
    Write-Host "   Categories: $categoryCount" -ForegroundColor Green
}

Write-Host ""
Write-Host "✅ Hoàn tất restore!" -ForegroundColor Green

