@echo off
REM Script để chạy test end-to-end cho Tiki Crawler (Windows)

echo ==========================================
echo Tiki Crawler - End-to-End Test
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
set TEST_SCRIPT=%SCRIPT_DIR%test_tiki_crawl_e2e.py

if not exist "%TEST_SCRIPT%" (
    echo ❌ Không tìm thấy file test: %TEST_SCRIPT%
    exit /b 1
)

echo Đang chạy test...
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

