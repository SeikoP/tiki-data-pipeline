@echo off
REM Script để chạy test cho Tiki Crawler (Windows)
REM Usage: run_test.bat [test_name]
REM Examples:
REM   run_test.bat test_product_details_demo
REM   run_test.bat test_products_demo
REM   run_test.bat test_crawl_demo

setlocal enabledelayedexpansion

echo ==========================================
echo Tiki Crawler - Test Runner
echo ==========================================
echo.

REM Kiểm tra Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python không được tìm thấy
    echo    Vui lòng cài đặt Python hoặc thêm vào PATH
    exit /b 1
)

echo ✓ Python đã được tìm thấy
echo.

REM Lấy đường dẫn script
set SCRIPT_DIR=%~dp0
set TEST_DIR=%SCRIPT_DIR%..\tests
set TEST_NAME=%1

REM Nếu không có test name, liệt kê các test có sẵn
if "%TEST_NAME%"=="" (
    echo Các test có sẵn:
    echo   - test_product_details_demo
    echo   - test_products_demo
    echo   - test_crawl_demo
    echo   - test_api_endpoints
    echo   - test_groq_config
    echo   - test_tiki_crawl_e2e
    echo.
    echo Usage: run_test.bat [test_name]
    echo Example: run_test.bat test_product_details_demo
    exit /b 0
)

REM Kiểm tra file test có tồn tại không
set TEST_SCRIPT=%TEST_DIR%\%TEST_NAME%.py
if not exist "%TEST_SCRIPT%" (
    echo ❌ Không tìm thấy file test: %TEST_SCRIPT%
    echo.
    echo Các test có sẵn:
    dir /b "%TEST_DIR%\test_*.py" 2>nul
    exit /b 1
)

echo Đang chạy test: %TEST_NAME%
echo Script: %TEST_SCRIPT%
echo.

python "%TEST_SCRIPT%"

set EXIT_CODE=%ERRORLEVEL%

echo.
if %EXIT_CODE% equ 0 (
    echo ✅ Test hoàn thành thành công!
) else (
    echo ❌ Test có lỗi (exit code: %EXIT_CODE%)
)

exit /b %EXIT_CODE%
