# Script để restore từng backup tạm thời và đếm số products
# Giúp tìm backup có nhiều dữ liệu nhất

param(
    [Parameter(Mandatory=$false)]
    [switch]$RestoreLargest
)

Write-Host "🔍 Kiểm tra số lượng products trong các backup files..." -ForegroundColor Cyan
Write-Host "⚠️  Quá trình này sẽ mất vài phút vì phải restore từng file" -ForegroundColor Yellow
Write-Host ""

$backupDir = "backups/postgres"
$backups = Get-ChildItem -Path $backupDir -Filter "*.dump" | Sort-Object LastWriteTime -Descending

# Lấy thông tin từ .env
$envFile = ".env"
$postgresUser = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_USER=").ToString().Split("=")[1]
$containerName = "tiki-data-pipeline-postgres-1"

$results = @()

foreach ($backup in $backups) {
    Write-Host "📦 Đang kiểm tra: $($backup.Name)..." -ForegroundColor Cyan
    
    # Restore vào database tạm
    $tempDb = "temp_check_db"
    
    # Drop database tạm nếu đã tồn tại
    docker exec $containerName psql -U $postgresUser -c "DROP DATABASE IF EXISTS $tempDb;" 2>$null | Out-Null
    docker exec $containerName psql -U $postgresUser -c "CREATE DATABASE $tempDb;" 2>$null | Out-Null
    
    # Copy file vào container
    docker cp "$backupDir/$($backup.Name)" "${containerName}:/tmp/temp_backup.dump" 2>$null | Out-Null
    
    # Restore
    docker exec $containerName pg_restore -U $postgresUser -d $tempDb --clean --if-exists /tmp/temp_backup.dump 2>$null | Out-Null
    
    # Đếm số products
    $productCount = docker exec $containerName psql -U $postgresUser -d $tempDb -t -c "SELECT COUNT(*) FROM products;" 2>$null
    $productCount = if ($productCount) { [int]$productCount.Trim() } else { 0 }
    
    # Đếm số categories
    $categoryCount = docker exec $containerName psql -U $postgresUser -d $tempDb -t -c "SELECT COUNT(*) FROM categories;" 2>$null
    $categoryCount = if ($categoryCount) { [int]$categoryCount.Trim() } else { 0 }
    
    $results += [PSCustomObject]@{
        FileName = $backup.Name
        Products = $productCount
        Categories = $categoryCount
        SizeMB = [math]::Round($backup.Length / 1MB, 2)
        DateTime = $backup.LastWriteTime
    }
    
    Write-Host "   ✅ Products: $productCount | Categories: $categoryCount" -ForegroundColor Green
    
    # Cleanup
    docker exec $containerName psql -U $postgresUser -c "DROP DATABASE IF EXISTS $tempDb;" 2>$null | Out-Null
}

# Hiển thị kết quả
Write-Host ""
Write-Host "📊 KẾT QUẢ KIỂM TRA:" -ForegroundColor Yellow
Write-Host ""
Write-Host ("{0,-35} {1,10} {1,12} {2,10}" -f "File", "Products", "Categories", "Size") -ForegroundColor Cyan
Write-Host ("-" * 80)

$results | Sort-Object Products -Descending | ForEach-Object {
    $color = if ($_.Products -ge 2000) { "Green" } elseif ($_.Products -ge 1000) { "Yellow" } else { "White" }
    Write-Host ("{0,-35} {1,10} {2,12} {3,10} MB" -f $_.FileName, $_.Products, $_.Categories, $_.SizeMB) -ForegroundColor $color
}

# Tìm backup tốt nhất
$best = $results | Sort-Object Products -Descending | Select-Object -First 1
Write-Host ""
Write-Host "🏆 BACKUP TỐT NHẤT:" -ForegroundColor Green
Write-Host "   File: $($best.FileName)" -ForegroundColor Green
Write-Host "   Products: $($best.Products)" -ForegroundColor Green
Write-Host "   Categories: $($best.Categories)" -ForegroundColor Green
Write-Host "   Thời gian: $($best.DateTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Green
Write-Host ""

if ($RestoreLargest) {
    Write-Host "🔄 Đang restore backup tốt nhất vào crawl_data..." -ForegroundColor Cyan
    & ".\scripts\restore-postgres.ps1" -BackupFile "backups/postgres/$($best.FileName)" -Database "crawl_data"
} else {
    Write-Host "📝 Để restore backup này, chạy:" -ForegroundColor Cyan
    Write-Host "   .\scripts\analyze-backups.ps1 -RestoreLargest" -ForegroundColor White
    Write-Host "hoặc:" -ForegroundColor Cyan
    Write-Host "   .\scripts\restore-postgres.ps1 -BackupFile `"backups/postgres/$($best.FileName)`" -Database `"crawl_data`"" -ForegroundColor White
}
