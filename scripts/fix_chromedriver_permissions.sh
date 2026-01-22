#!/bin/bash
# Script để fix quyền thực thi cho ChromeDriver (fix lỗi status code 127)
# Đặc biệt cần thiết trong WSL2/Linux

echo "🔧 Đang fix quyền thực thi cho ChromeDriver..."

# Tìm ChromeDriver trong thư mục webdriver-manager cache
CHROMEDRIVER_DIR="$HOME/.wdm/drivers/chromedriver"

if [ -d "$CHROMEDRIVER_DIR" ]; then
    echo "📁 Tìm thấy thư mục ChromeDriver: $CHROMEDRIVER_DIR"
    
    # Tìm tất cả file chromedriver và set quyền thực thi
    find "$CHROMEDRIVER_DIR" -name "chromedriver" -type f -exec chmod +x {} \;
    
    echo "✅ Đã set quyền thực thi cho tất cả ChromeDriver"
    
    # Liệt kê các file đã fix
    echo ""
    echo "📋 Các file ChromeDriver đã được fix:"
    find "$CHROMEDRIVER_DIR" -name "chromedriver" -type f -ls
else
    echo "⚠️  Không tìm thấy thư mục ChromeDriver: $CHROMEDRIVER_DIR"
    echo "   ChromeDriver sẽ được tự động fix khi được download lần tiếp theo"
fi

echo ""
echo "✅ Hoàn thành!"
