#!/bin/bash
# Script để cài đặt dependencies cho Tiki Data Pipeline (Linux/Mac)

set -e

echo "=========================================="
echo "Tiki Data Pipeline - Install Dependencies"
echo "=========================================="
echo ""

# Kiểm tra Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 không được tìm thấy"
    echo "   Vui lòng cài đặt Python3"
    exit 1
fi

echo "✓ Python3 đã được tìm thấy"
echo ""

# Lấy đường dẫn script và project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Kiểm tra requirements.txt
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "❌ Không tìm thấy file requirements.txt"
    echo "   Đường dẫn: $REQUIREMENTS_FILE"
    exit 1
fi

echo "✓ Tìm thấy requirements.txt"
echo ""

# Cài đặt dependencies
echo "Đang cài đặt dependencies từ requirements.txt..."
echo ""

python3 -m pip install --upgrade pip
python3 -m pip install -r "$REQUIREMENTS_FILE"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Cài đặt dependencies thành công!"
    echo ""
    echo "Để kiểm tra dependencies:"
    echo "  python3 -m pip list"
    echo ""
    echo "Để kiểm tra lxml đã được cài đặt:"
    echo "  python3 -c 'import lxml; print(\"lxml: OK\")'"
else
    echo "❌ Cài đặt dependencies có lỗi (exit code: $EXIT_CODE)"
    echo ""
    echo "Một số lỗi thường gặp:"
    echo "  1. Không có quyền cài đặt - sử dụng sudo (Linux) hoặc --user"
    echo "  2. Lỗi build lxml - cài đặt libxml2-dev và libxslt-dev (Linux)"
    echo "  3. Lỗi network - kiểm tra kết nối internet"
fi

exit $EXIT_CODE

