#!/bin/bash
# Script để cài đặt dependencies cần thiết cho ChromeDriver trong WSL2/Linux
# Fix lỗi: "error while loading shared libraries: libnss3.so"
#
# Usage:
#   bash scripts/install_chromedriver_dependencies.sh
#   (Sẽ yêu cầu sudo password)

echo "🔧 Đang cài đặt dependencies cho ChromeDriver..."
echo "⚠️  Script này cần quyền sudo để cài đặt packages hệ thống"
echo ""

# Kiểm tra OS
if [ -f /etc/debian_version ]; then
    echo "📦 Phát hiện Debian/Ubuntu, đang cài đặt packages..."
    
    # Update package list
    sudo apt-get update -qq
    
    echo "📦 Đang cài đặt các package quan trọng (NSS, NSPR)..."
    # Cài đặt các package QUAN TRỌNG NHẤT trước (NSS - cần thiết cho ChromeDriver)
    if sudo apt-get install -y libnss3 libnspr4 2>&1; then
        echo "✅ Đã cài đặt libnss3 và libnspr4"
    else
        echo "❌ Lỗi khi cài đặt libnss3/libnspr4"
        echo "💡 Thử chạy thủ công: sudo apt-get install -y libnss3 libnspr4"
    fi
    
    echo "📦 Đang cài đặt các dependencies khác..."
    # Cài đặt các dependencies khác, xử lý lỗi từng package
    # Sử dụng || true để tiếp tục ngay cả khi một package fail
    sudo apt-get install -y \
        libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libdbus-1-3 \
        libxkbcommon0 libxcomposite1 libxdamage1 \
        libxfixes3 libxrandr2 libgbm1 \
        libpango-1.0-0 libcairo2 libatspi2.0-0 \
        libxshmfence1 2>&1 | grep -v "already installed" || true
    
    # Thử cài libasound2 (có thể có tên khác trong Ubuntu mới)
    echo "📦 Đang cài đặt libasound2..."
    sudo apt-get install -y libasound2t64 2>&1 | grep -v "already installed" || \
    sudo apt-get install -y libasound2 2>&1 | grep -v "already installed" || \
    echo "⚠️  Không thể cài libasound2 (không bắt buộc cho headless mode)"
    
    echo "✅ Đã cài đặt dependencies"
    
elif [ -f /etc/redhat-release ]; then
    echo "📦 Phát hiện RedHat/CentOS, đang cài đặt packages..."
    sudo yum install -y nss nspr atk cups-libs libdrm libXkbcommon libXcomposite libXdamage libXfixes libXrandr libgbm alsa-lib pango cairo at-spi2-atk libxshmfence
    
    echo "✅ Đã cài đặt dependencies"
else
    echo "⚠️  Không xác định được OS. Vui lòng cài đặt thủ công:"
    echo "   - libnss3"
    echo "   - libnspr4"
    echo "   - libatk1.0-0"
    echo "   - libgbm1"
    echo "   - và các dependencies khác"
fi

echo ""
echo "🧪 Đang kiểm tra ChromeDriver..."
CHROMEDRIVER_PATH="$HOME/.wdm/drivers/chromedriver/linux64/114.0.5735.90/chromedriver"
if [ -f "$CHROMEDRIVER_PATH" ]; then
    if "$CHROMEDRIVER_PATH" --version >/dev/null 2>&1; then
        echo "✅ ChromeDriver hoạt động bình thường!"
        "$CHROMEDRIVER_PATH" --version
    else
        echo "❌ ChromeDriver vẫn có lỗi. Kiểm tra lại dependencies:"
        ldd "$CHROMEDRIVER_PATH" | grep "not found"
    fi
else
    echo "⚠️  ChromeDriver chưa được tải. Sẽ được tải tự động khi chạy script crawl."
fi

echo ""
echo "✅ Hoàn thành!"
