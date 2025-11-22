# Script kiểm tra số lượng products trong các backup files
# Giúp tìm backup có nhiều dữ liệu nhất

Write-Host "🔍 Kiểm tra kích thước các backup files..." -ForegroundColor Cyan
Write-Host ""

$backupDir = "backups/postgres"
$backups = Get-ChildItem -Path $backupDir -Filter "*.dump" | Sort-Object LastWriteTime -Descending

Write-Host "📊 Danh sách backups (theo thứ tự mới nhất):" -ForegroundColor Yellow
Write-Host ""
Write-Host ("{0,-35} {1,15} {2,20}" -f "File", "Kích thước", "Thời gian") -ForegroundColor Cyan
Write-Host ("-" * 70)

foreach ($backup in $backups) {
    $sizeKB = [math]::Round($backup.Length / 1KB, 2)
    $sizeMB = [math]::Round($backup.Length / 1MB, 2)
    $sizeStr = if ($sizeMB -ge 1) { "$sizeMB MB" } else { "$sizeKB KB" }
    
    Write-Host ("{0,-35} {1,15} {2,20}" -f $backup.Name, $sizeStr, $backup.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss"))
}

Write-Host ""
Write-Host "💡 File có kích thước lớn nhất thường chứa nhiều dữ liệu nhất" -ForegroundColor Yellow

# Tìm file lớn nhất
$largestBackup = $backups | Sort-Object Length -Descending | Select-Object -First 1
Write-Host ""
Write-Host "🏆 File lớn nhất: $($largestBackup.Name)" -ForegroundColor Green
Write-Host "   Kích thước: $([math]::Round($largestBackup.Length / 1MB, 2)) MB" -ForegroundColor Green
Write-Host "   Thời gian: $($largestBackup.LastWriteTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Green
Write-Host ""
Write-Host "📝 Để restore file này, chạy:" -ForegroundColor Cyan
Write-Host "   .\scripts\restore-postgres.ps1 -BackupFile `"backups/postgres/$($largestBackup.Name)`" -Database `"crawl_data`"" -ForegroundColor White
