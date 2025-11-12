#!/bin/bash
# Script để chạy test cho Tiki Crawler (Linux/Mac)
# Usage: ./run_test.sh [test_name]
# Examples:
#   ./run_test.sh test_product_details_demo
#   ./run_test.sh test_products_demo
#   ./run_test.sh test_crawl_demo

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$SCRIPT_DIR/../tests"
TEST_NAME="$1"

echo "=========================================="
echo "Tiki Crawler - Test Runner"
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

# Nếu không có test name, liệt kê các test có sẵn
if [ -z "$TEST_NAME" ]; then
    echo "Các test có sẵn:"
    echo "  - test_product_details_demo"
    echo "  - test_products_demo"
    echo "  - test_crawl_demo"
    echo "  - test_api_endpoints"
    echo "  - test_groq_config"
    echo "  - test_tiki_crawl_e2e"
    echo ""
    echo "Usage: ./run_test.sh [test_name]"
    echo "Example: ./run_test.sh test_product_details_demo"
    exit 0
fi

# Kiểm tra file test có tồn tại không
TEST_SCRIPT="$TEST_DIR/${TEST_NAME}.py"
if [ ! -f "$TEST_SCRIPT" ]; then
    echo "❌ Không tìm thấy file test: $TEST_SCRIPT"
    echo ""
    echo "Các test có sẵn:"
    ls -1 "$TEST_DIR"/test_*.py 2>/dev/null | xargs -n1 basename | sed 's/\.py$//' | sed 's/^/  - /'
    exit 1
fi

echo "Đang chạy test: $TEST_NAME"
echo "Script: $TEST_SCRIPT"
echo ""

python3 "$TEST_SCRIPT"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Test hoàn thành thành công!"
else
    echo "❌ Test có lỗi (exit code: $EXIT_CODE)"
fi

exit $EXIT_CODE

