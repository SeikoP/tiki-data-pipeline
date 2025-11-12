@echo off
REM Script để cài đặt dependencies cho Tiki Data Pipeline (Windows)

echo ==========================================
echo Tiki Data Pipeline - Install Dependencies
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

REM Lấy đường dẫn script và project root
set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..\..

REM Kiểm tra requirements.txt
set REQUIREMENTS_FILE=%PROJECT_ROOT%\requirements.txt
if not exist "%REQUIREMENTS_FILE%" (
    echo ❌ Không tìm thấy file requirements.txt
    echo    Đường dẫn: %REQUIREMENTS_FILE%
    exit /b 1
)

echo ✓ Tìm thấy requirements.txt
echo.

REM Cài đặt dependencies
echo Đang cài đặt dependencies từ requirements.txt...
echo.

python -m pip install --upgrade pip
python -m pip install -r "%REQUIREMENTS_FILE%"

set EXIT_CODE=%ERRORLEVEL%

echo.
if %EXIT_CODE% equ 0 (
    echo ✅ Cài đặt dependencies thành công!
    echo.
    echo Để kiểm tra dependencies:
    echo   python -m pip list
    echo.
    echo Để kiểm tra lxml đã được cài đặt:
    echo   python -c "import lxml; print('lxml: OK')"
) else (
    echo ❌ Cài đặt dependencies có lỗi (exit code: %EXIT_CODE%)
    echo.
    echo Một số lỗi thường gặp:
    echo   1. Không có quyền cài đặt - chạy với quyền Administrator
    echo   2. Lỗi build lxml - cài đặt Microsoft Visual C++ Build Tools
    echo   3. Lỗi network - kiểm tra kết nối internet
)

exit /b %EXIT_CODE%

