#!/bin/bash
# Script dọn dẹp Docker để giải quyết lỗi "no space left on device"
# Chạy script này trước khi build Docker images

echo "🧹 Dọn dẹp Docker cache và unused resources..."

# Kiểm tra không gian đĩa trước khi dọn dẹp
echo ""
echo "📊 Không gian đĩa trước khi dọn dẹp:"
docker system df

# Dừng tất cả containers đang chạy (nếu có)
echo ""
echo "🛑 Dừng containers đang chạy..."
docker-compose down 2>/dev/null || true

# Xóa tất cả containers đã dừng
echo ""
echo "🗑️  Xóa stopped containers..."
docker container prune -f

# Xóa tất cả images không được sử dụng (không có tags hoặc không được tham chiếu)
echo ""
echo "🗑️  Xóa dangling images..."
docker image prune -f

# Xóa tất cả unused images (cả những images có tags nhưng không được sử dụng)
echo ""
echo "🗑️  Xóa unused images..."
docker image prune -a -f

# Xóa tất cả unused volumes
echo ""
echo "🗑️  Xóa unused volumes..."
docker volume prune -f

# Xóa tất cả unused networks
echo ""
echo "🗑️  Xóa unused networks..."
docker network prune -f

# Xóa build cache (quan trọng nhất để giải quyết lỗi "no space left")
echo ""
echo "🗑️  Xóa build cache..."
docker builder prune -a -f --volumes

# Dọn dẹp toàn bộ hệ thống (tùy chọn, có thể xóa cả images đang được sử dụng)
echo ""
echo "🗑️  Dọn dẹp toàn bộ hệ thống (giữ lại images đang được sử dụng)..."
docker system prune -f

# Hiển thị không gian đĩa sau khi dọn dẹp
echo ""
echo "📊 Không gian đĩa sau khi dọn dẹp:"
docker system df

echo ""
echo "✅ Dọn dẹp hoàn tất!"
echo "💡 Bây giờ bạn có thể build Docker images lại:"
echo "   docker-compose build"

