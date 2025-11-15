# Script dọn dẹp Docker để giải quyết lỗi "no space left on device"
# Chạy script này trước khi build Docker images

Write-Host "🧹 Dọn dẹp Docker cache và unused resources..." -ForegroundColor Cyan

# Kiểm tra không gian đĩa trước khi dọn dẹp
Write-Host "`n📊 Không gian đĩa trước khi dọn dẹp:" -ForegroundColor Yellow
docker system df

# Dừng tất cả containers đang chạy (nếu có)
Write-Host "`n🛑 Dừng containers đang chạy..." -ForegroundColor Yellow
docker-compose down 2>$null

# Xóa tất cả containers đã dừng
Write-Host "`n🗑️  Xóa stopped containers..." -ForegroundColor Yellow
docker container prune -f

# Xóa tất cả images không được sử dụng (không có tags hoặc không được tham chiếu)
Write-Host "`n🗑️  Xóa dangling images..." -ForegroundColor Yellow
docker image prune -f

# Xóa tất cả unused images (cả những images có tags nhưng không được sử dụng)
Write-Host "`n🗑️  Xóa unused images..." -ForegroundColor Yellow
docker image prune -a -f

# Xóa tất cả unused volumes
Write-Host "`n🗑️  Xóa unused volumes..." -ForegroundColor Yellow
docker volume prune -f

# Xóa tất cả unused networks
Write-Host "`n🗑️  Xóa unused networks..." -ForegroundColor Yellow
docker network prune -f

# Xóa build cache (quan trọng nhất để giải quyết lỗi "no space left")
Write-Host "`n🗑️  Xóa build cache..." -ForegroundColor Yellow
docker builder prune -a -f --volumes

# Dọn dẹp toàn bộ hệ thống (tùy chọn, có thể xóa cả images đang được sử dụng)
Write-Host "`n🗑️  Dọn dẹp toàn bộ hệ thống (giữ lại images đang được sử dụng)..." -ForegroundColor Yellow
docker system prune -f

# Hiển thị không gian đĩa sau khi dọn dẹp
Write-Host "`n📊 Không gian đĩa sau khi dọn dẹp:" -ForegroundColor Green
docker system df

Write-Host "`n✅ Dọn dẹp hoàn tất!" -ForegroundColor Green
Write-Host "💡 Bây giờ bạn có thể build Docker images lại:" -ForegroundColor Cyan
Write-Host "   docker-compose build" -ForegroundColor White

