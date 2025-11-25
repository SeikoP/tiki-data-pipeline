# Script PowerShell để merge tất cả product detail files thành 1 file
# Sau đó có thể dùng demo_step3_load.py để load

Write-Host "🔄 Merge tất cả product detail files..." -ForegroundColor Cyan

$cacheDir = "data/raw/products/detail/cache"
$outputFile = "data/raw/all_products_merged.json"

# Lấy tất cả file JSON
$jsonFiles = Get-ChildItem -Path $cacheDir -Filter "*.json"
Write-Host "📁 Tìm thấy $($jsonFiles.Count) files" -ForegroundColor Yellow

# Đọc tất cả products
$allProducts = @()
$errorCount = 0

foreach ($file in $jsonFiles) {
    try {
        $content = Get-Content $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        $allProducts += $content
    }
    catch {
        $errorCount++
        if ($errorCount -le 5) {
            Write-Host "   ⚠️  Error in $($file.Name): $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }
}

Write-Host "✅ Đọc xong! $($allProducts.Count) products hợp lệ" -ForegroundColor Green

if ($errorCount -gt 0) {
    Write-Host "⚠️  $errorCount files có lỗi" -ForegroundColor Yellow
}

# Ghi ra file
Write-Host "💾 Đang ghi vào $outputFile..." -ForegroundColor Cyan
$allProducts | ConvertTo-Json -Depth 10 | Set-Content $outputFile -Encoding UTF8

Write-Host "✅ Hoàn tất! File output: $outputFile" -ForegroundColor Green
Write-Host "📊 Tổng: $($allProducts.Count) products" -ForegroundColor Green
Write-Host ""
Write-Host "📝 Để load vào database, chạy:" -ForegroundColor Cyan
Write-Host "   python demos/demo_step3_load.py" -ForegroundColor White
