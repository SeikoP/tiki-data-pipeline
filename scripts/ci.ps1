# CI/CD Script cho Windows PowerShell
# Sử dụng: .\scripts\ci.ps1 <command>

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

function Show-Help {
    Write-Host "`n=== CI/CD Commands ===" -ForegroundColor Cyan
    Write-Host "install          - Cài đặt dependencies" -ForegroundColor Yellow
    Write-Host "lint             - Chạy linting với ruff" -ForegroundColor Yellow
    Write-Host "format           - Format code với black và isort" -ForegroundColor Yellow
    Write-Host "format-check     - Kiểm tra format code (không sửa)" -ForegroundColor Yellow
    Write-Host "type-check       - Kiểm tra type với mypy" -ForegroundColor Yellow
    Write-Host "test             - Chạy tests với pytest" -ForegroundColor Yellow
    Write-Host "test-fast        - Chạy tests nhanh (không coverage)" -ForegroundColor Yellow
    Write-Host "validate-dags    - Validate Airflow DAGs" -ForegroundColor Yellow
    Write-Host "security-check   - Kiểm tra bảo mật với bandit và safety" -ForegroundColor Yellow
    Write-Host "docker-build     - Build Docker images" -ForegroundColor Yellow
    Write-Host "docker-up        - Khởi động Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-down      - Dừng Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-logs      - Xem logs của Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-test      - Test Docker Compose setup" -ForegroundColor Yellow
    Write-Host "clean            - Dọn dẹp cache và temporary files" -ForegroundColor Yellow
    Write-Host "ci-local         - Chạy tất cả các bước CI cục bộ" -ForegroundColor Green
    Write-Host "ci-fast          - Chạy CI nhanh (không test)" -ForegroundColor Green
    Write-Host ""
}

function Install-Dependencies {
    Write-Host "`n📦 Cài đặt dependencies..." -ForegroundColor Cyan
    python -m pip install --upgrade pip
    pip install -r requirements.txt
    pip install ruff black isort mypy pylint bandit safety pytest pytest-cov pytest-mock
    Write-Host "✅ Hoàn thành!" -ForegroundColor Green
}

function Invoke-Lint {
    Write-Host "`n🔍 Chạy linting với ruff..." -ForegroundColor Cyan
    ruff check src/ tests/ airflow/dags/
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Linting passed!" -ForegroundColor Green
    } else {
        Write-Host "❌ Linting failed!" -ForegroundColor Red
        exit 1
    }
}

function Invoke-Format {
    Write-Host "`n✨ Format code với black và isort..." -ForegroundColor Cyan
    black src/ tests/ airflow/dags/
    isort src/ tests/ airflow/dags/
    Write-Host "✅ Format completed!" -ForegroundColor Green
}

function Invoke-FormatCheck {
    Write-Host "`n🔍 Kiểm tra format code..." -ForegroundColor Cyan
    black --check --diff src/ tests/ airflow/dags/
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Format check failed! Run 'format' to fix." -ForegroundColor Red
        exit 1
    }
    isort --check-only --diff src/ tests/ airflow/dags/
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Import sorting check failed! Run 'format' to fix." -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Format check passed!" -ForegroundColor Green
}

function Invoke-TypeCheck {
    Write-Host "`n🔍 Kiểm tra type với mypy..." -ForegroundColor Cyan
    mypy src/ --ignore-missing-imports
    Write-Host "✅ Type check completed!" -ForegroundColor Green
}

function Invoke-Test {
    Write-Host "`n🧪 Chạy tests với pytest..." -ForegroundColor Cyan
    pytest tests/ -v --cov=src --cov-report=term --cov-report=html
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Tests passed!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Some tests failed (exit code: $LASTEXITCODE)" -ForegroundColor Yellow
    }
}

function Invoke-TestFast {
    Write-Host "`n🧪 Chạy tests nhanh..." -ForegroundColor Cyan
    pytest tests/ -v
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Tests passed!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Some tests failed" -ForegroundColor Yellow
    }
}

function Invoke-ValidateDags {
    Write-Host "`n🔍 Validating Airflow DAGs..." -ForegroundColor Cyan
    $dagValidationScript = @"
import sys
import os
os.environ['AIRFLOW_HOME'] = 'airflow'
from airflow.models import DagBag

dag_bag = DagBag(dag_folder='airflow/dags', include_examples=False)
if dag_bag.import_errors:
    print('❌ DAG Import Errors:')
    for filename, error in dag_bag.import_errors.items():
        print(f'  {filename}: {error}')
    sys.exit(1)
else:
    print('✅ All DAGs validated successfully!')
    print(f'Found {len(dag_bag.dags)} DAG(s)')
    for dag_id in dag_bag.dag_ids:
        print(f'  - {dag_id}')
"@
    python -c $dagValidationScript
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ DAG validation passed!" -ForegroundColor Green
    } else {
        Write-Host "❌ DAG validation failed!" -ForegroundColor Red
        exit 1
    }
}

function Invoke-SecurityCheck {
    Write-Host "`n🔒 Kiểm tra bảo mật..." -ForegroundColor Cyan
    Write-Host "Running Bandit..." -ForegroundColor Yellow
    bandit -r src/
    Write-Host "`nRunning Safety..." -ForegroundColor Yellow
    safety check
    Write-Host "✅ Security check completed!" -ForegroundColor Green
}

function Invoke-DockerBuild {
    Write-Host "`n🐳 Build Docker images..." -ForegroundColor Cyan
    docker-compose build
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Docker build completed!" -ForegroundColor Green
    } else {
        Write-Host "❌ Docker build failed!" -ForegroundColor Red
        exit 1
    }
}

function Invoke-DockerUp {
    Write-Host "`n🐳 Khởi động Docker Compose services..." -ForegroundColor Cyan
    docker-compose up -d
    Write-Host "✅ Services started!" -ForegroundColor Green
}

function Invoke-DockerDown {
    Write-Host "`n🐳 Dừng Docker Compose services..." -ForegroundColor Cyan
    docker-compose down
    Write-Host "✅ Services stopped!" -ForegroundColor Green
}

function Invoke-DockerLogs {
    Write-Host "`n🐳 Xem logs của Docker Compose services..." -ForegroundColor Cyan
    docker-compose logs -f
}

function Invoke-DockerTest {
    Write-Host "`n🐳 Test Docker Compose setup..." -ForegroundColor Cyan
    Write-Host "Validating docker-compose.yaml..." -ForegroundColor Yellow
    docker-compose config
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ docker-compose.yaml validation failed!" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "`nBuilding images..." -ForegroundColor Yellow
    docker-compose build
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Docker build failed!" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "`nStarting services..." -ForegroundColor Yellow
    docker-compose up -d
    Start-Sleep -Seconds 30
    
    Write-Host "`nTesting Airflow API..." -ForegroundColor Yellow
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8080/api/v2/version" -UseBasicParsing -TimeoutSec 10
        Write-Host "✅ Airflow API is responding!" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  Airflow API not ready yet" -ForegroundColor Yellow
    }
    
    Write-Host "`nCleaning up..." -ForegroundColor Yellow
    docker-compose down -v
    Write-Host "✅ Docker test completed!" -ForegroundColor Green
}

function Invoke-Clean {
    Write-Host "`n🧹 Dọn dẹp cache và temporary files..." -ForegroundColor Cyan
    Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".pytest_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".mypy_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".ruff_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "htmlcov" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path ".coverage" -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "coverage.xml" -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "dist" -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -Path "build" -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter "*.egg-info" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "✅ Clean completed!" -ForegroundColor Green
}

function Invoke-CILocal {
    Write-Host "`n🚀 Running local CI checks..." -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    try {
        Invoke-FormatCheck
        Invoke-Lint
        Invoke-TypeCheck
        Invoke-ValidateDags
        Invoke-SecurityCheck
        Invoke-Test
        
        Write-Host "`n========================================" -ForegroundColor Green
        Write-Host "✅ All CI checks passed!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } catch {
        Write-Host "`n========================================" -ForegroundColor Red
        Write-Host "❌ CI checks failed!" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit 1
    }
}

function Invoke-CIFast {
    Write-Host "`n⚡ Running fast CI checks..." -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    try {
        Invoke-FormatCheck
        Invoke-Lint
        Invoke-ValidateDags
        
        Write-Host "`n========================================" -ForegroundColor Green
        Write-Host "✅ Fast CI checks passed!" -ForegroundColor Green
        Write-Host "========================================" -ForegroundColor Green
    } catch {
        Write-Host "`n========================================" -ForegroundColor Red
        Write-Host "❌ Fast CI checks failed!" -ForegroundColor Red
        Write-Host "========================================" -ForegroundColor Red
        exit 1
    }
}

# Main command router
switch ($Command.ToLower()) {
    "help" { Show-Help }
    "install" { Install-Dependencies }
    "lint" { Invoke-Lint }
    "format" { Invoke-Format }
    "format-check" { Invoke-FormatCheck }
    "type-check" { Invoke-TypeCheck }
    "test" { Invoke-Test }
    "test-fast" { Invoke-TestFast }
    "validate-dags" { Invoke-ValidateDags }
    "security-check" { Invoke-SecurityCheck }
    "docker-build" { Invoke-DockerBuild }
    "docker-up" { Invoke-DockerUp }
    "docker-down" { Invoke-DockerDown }
    "docker-logs" { Invoke-DockerLogs }
    "docker-test" { Invoke-DockerTest }
    "clean" { Invoke-Clean }
    "ci-local" { Invoke-CILocal }
    "ci-fast" { Invoke-CIFast }
    default {
        Write-Host "❌ Unknown command: $Command" -ForegroundColor Red
        Show-Help
        exit 1
    }
}

