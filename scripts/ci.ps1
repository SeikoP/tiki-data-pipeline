# CI/CD Script cho Windows PowerShell
# Sử dụng: .\scripts\ci.ps1 <command>

param(
    [Parameter(Position=0)]
    [string]$Command = "help"
)

# Auto-detect local .venv or python3 on Linux
if ($IsLinux) {
    if (Test-Path "./.venv/bin/python") {
        Write-Host "✅ Using local virtual environment (.venv)" -ForegroundColor Green
        New-Alias -Name python -Value "./.venv/bin/python" -Force -Scope Script
    } elseif ($null -eq (Get-Command "python" -ErrorAction SilentlyContinue)) {
        if (Get-Command "python3" -ErrorAction SilentlyContinue) {
            Write-Host "⚠️  'python' not found, aliasing to 'python3'" -ForegroundColor Yellow
            New-Alias -Name python -Value "python3" -Force -Scope Script
        }
    }
}

# Force Python to use UTF-8 for I/O to avoid 'charmap' codec errors on Windows
$env:PYTHONIOENCODING = "utf-8"

function Show-Help {
    Write-Host "`n=== CI/CD Commands ===" -ForegroundColor Cyan
    Write-Host "setup            - Khởi tạo môi trường (venv & dependencies)" -ForegroundColor Green
    Write-Host "install          - Cài đặt dependencies" -ForegroundColor Yellow
    Write-Host "lint             - Chạy linting với ruff" -ForegroundColor Yellow
    Write-Host "lint-fix         - Tự động sửa lỗi linting với ruff" -ForegroundColor Green
    Write-Host "perf-check       - Kiểm tra performance với Ruff PERF" -ForegroundColor Yellow
    Write-Host "dead-code        - Tìm code thừa với Vulture" -ForegroundColor Yellow
    Write-Host "complexity       - Phân tích độ phức tạp với Radon" -ForegroundColor Yellow
    Write-Host "code-quality     - Chạy tất cả code quality checks" -ForegroundColor Yellow
    Write-Host "format           - Format code với black và isort" -ForegroundColor Yellow
    Write-Host "format-check     - Kiểm tra format code (không sửa)" -ForegroundColor Yellow
    Write-Host "type-check       - Kiểm tra type với mypy" -ForegroundColor Yellow
    Write-Host "test             - Chạy tests với pytest" -ForegroundColor Yellow
    Write-Host "test-fast        - Chạy tests nhanh (không coverage)" -ForegroundColor Yellow
    Write-Host "validate-dags    - Validate Airflow DAGs" -ForegroundColor Yellow
    Write-Host "install-hooks    - Cài đặt Git pre-commit hooks" -ForegroundColor Green
    Write-Host "security-check   - Kiểm tra bảo mật với bandit và safety" -ForegroundColor Yellow
    Write-Host "docker-build     - Build Docker images" -ForegroundColor Yellow
    Write-Host "docker-up        - Khởi động Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-down      - Dừng Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-logs      - Xem logs của Docker Compose services" -ForegroundColor Yellow
    Write-Host "docker-test      - Test Docker Compose setup" -ForegroundColor Yellow
    Write-Host "clean            - Dọn dẹp cache và temporary files" -ForegroundColor Yellow
    Write-Host "ci-local         - Chạy tất cả các bước CI cục bộ (Parallel)" -ForegroundColor Green
    Write-Host "ci-fast          - Chạy CI nhanh (Parallel)" -ForegroundColor Green
    Write-Host ""
}

function Invoke-Setup {
    Write-Host "`n🛠️  Khởi tạo môi trường..." -ForegroundColor Cyan
    
    if ($IsLinux) {
        if (-not (Test-Path "./.venv")) {
            Write-Host "Creating virtual environment..." -ForegroundColor Yellow
            python3 -m venv .venv
        }
        New-Alias -Name python -Value "./.venv/bin/python" -Force -Scope Script
        New-Alias -Name pip -Value "./.venv/bin/pip" -Force -Scope Script
    } else {
        if (-not (Test-Path "./.venv")) {
            Write-Host "Creating virtual environment..." -ForegroundColor Yellow
            python -m venv .venv
        }
        New-Alias -Name python -Value "./.venv/Scripts/python.exe" -Force -Scope Script
        New-Alias -Name pip -Value "./.venv/Scripts/pip.exe" -Force -Scope Script
    }
    
    Install-Dependencies
}

function Install-Dependencies {
    Write-Host "`n📦 Cài đặt dependencies..." -ForegroundColor Cyan
    python -m pip install --upgrade pip
    python -m pip install -r requirements.txt
    python -m pip install ruff black isort mypy pylint bandit safety pytest pytest-cov pytest-mock vulture radon
    Write-Host "✅ Hoàn thành!" -ForegroundColor Green
}

function Invoke-Lint {
    Write-Host "`n🔍 Chạy linting với ruff..." -ForegroundColor Cyan
    python -m ruff check src/ tests/ airflow/dags/
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Linting passed!" -ForegroundColor Green
    } else {
        Write-Host "❌ Linting failed!" -ForegroundColor Red
        exit 1
    }
}

function Invoke-LintFix {
    Write-Host "`n🛠️  Tự động sửa lỗi linting với ruff..." -ForegroundColor Cyan
    python -m ruff check --fix --unsafe-fixes src/ tests/ airflow/dags/
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Linting fixed/passed!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Some linting errors could not be fixed automatically." -ForegroundColor Yellow
    }
}

function Invoke-PerfCheck {
    Write-Host "`n⚡ Kiểm tra performance với Ruff PERF..." -ForegroundColor Cyan
    python -m ruff check --select PERF src/ tests/ airflow/dags/
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Performance check passed!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Performance issues found!" -ForegroundColor Yellow
    }
}

function Invoke-DeadCode {
    Write-Host "`n🧹 Tìm code thừa với Vulture..." -ForegroundColor Cyan
    python -m vulture src/ airflow/dags/ --min-confidence 80
    Write-Host "✅ Dead code check completed!" -ForegroundColor Green
}

function Invoke-Complexity {
    Write-Host "`n📊 Phân tích độ phức tạp với Radon..." -ForegroundColor Cyan
    Write-Host "`nCyclomatic Complexity:" -ForegroundColor Yellow
    python -m radon cc src/ airflow/dags/ --min B
    Write-Host "`nMaintainability Index:" -ForegroundColor Yellow
    python -m radon mi src/ airflow/dags/ --min B
    Write-Host "✅ Complexity analysis completed!" -ForegroundColor Green
}

function Invoke-Format {
    Write-Host "`n✨ Format code với black và isort..." -ForegroundColor Cyan
    python -m black src/ tests/ airflow/dags/
    python -m isort src/ tests/ airflow/dags/
    Write-Host "✅ Format completed!" -ForegroundColor Green
}

function Invoke-FormatCheck {
    Write-Host "`n🔍 Kiểm tra format code..." -ForegroundColor Cyan
    python -m black --check --diff src/ tests/ airflow/dags/
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Format check failed! Run 'format' to fix." -ForegroundColor Red
        exit 1
    }
    python -m isort --check-only --diff src/ tests/ airflow/dags/
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Import sorting check failed! Run 'format' to fix." -ForegroundColor Red
        exit 1
    }
    Write-Host "✅ Format check passed!" -ForegroundColor Green
}

function Invoke-TypeCheck {
    Write-Host "`n🔍 Kiểm tra type với mypy..." -ForegroundColor Cyan
    python -m mypy src/ --ignore-missing-imports
    Write-Host "✅ Type check completed!" -ForegroundColor Green
}

function Invoke-Test {
    Write-Host "`n🧪 Chạy tests với pytest..." -ForegroundColor Cyan
    python -m pytest tests/ -v --cov=src --cov-report=term --cov-report=html
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Tests passed!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Some tests failed (exit code: $LASTEXITCODE)" -ForegroundColor Yellow
    }
    
    Write-Host "`n🔍 Kiểm tra Transform và Load modules..." -ForegroundColor Cyan
    python -c "import sys; sys.path.insert(0, 'src'); from pipelines.transform.transformer import DataTransformer; from pipelines.load.loader import DataLoader; print('✅ Transform and Load modules imported successfully')"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Transform and Load modules OK!" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Transform and Load modules check failed" -ForegroundColor Yellow
    }
}

function Invoke-TestFast {
    Write-Host "`n🧪 Chạy tests nhanh..." -ForegroundColor Cyan
    python -m pytest tests/ -v
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
import pkgutil

# Set AIRFLOW_HOME to avoid SQLite errors
os.environ['AIRFLOW_HOME'] = os.path.abspath('airflow')

if not pkgutil.find_loader('airflow'):
    print('SKIP: Airflow not installed locally')
    sys.exit(0)

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
    sys.exit(0)
"@

    # 1. Thử validate cục bộ
    python -c $dagValidationScript
    $localResult = $LASTEXITCODE
    
    if ($localResult -eq 0) {
        $output = python -c "$dagValidationScript" | Out-String
        if ($output -match "SKIP: Airflow not installed") {
            # 2. Thử validate qua Docker nếu cục bộ không có
            Write-Host "🐳 Local Airflow not found, trying via Docker..." -ForegroundColor Yellow
            docker-compose run --rm airflow-scheduler python3 -c "$dagValidationScript"
            if ($LASTEXITCODE -eq 0) {
                Write-Host "✅ DAG validation via Docker passed!" -ForegroundColor Green
            } else {
                Write-Host "⚠️  DAG validation via Docker failed." -ForegroundColor Yellow
            }
        } else {
            Write-Host "✅ Local DAG validation passed!" -ForegroundColor Green
        }
    } else {
        Write-Host "❌ DAG validation failed!" -ForegroundColor Red
        exit 1
    }
}

function Invoke-InstallHooks {
    Write-Host "`n⚓ Cài đặt Git pre-commit hooks..." -ForegroundColor Cyan
    
    $hookContent = @"
#!/bin/bash
# Pre-commit hook generated by ci.ps1
echo "🚀 Running pre-commit checks..."
pwsh ./scripts/ci.ps1 ci-quick
if [ $? -ne 0 ]; then
    echo "❌ Pre-commit checks failed. Commit aborted."
    exit 1
fi
"@

    if (-not (Test-Path ".git")) {
        Write-Host "❌ Not a git repository!" -ForegroundColor Red
        return
    }

    $hookPath = ".git/hooks/pre-commit"
    $hookContent | Out-File -FilePath $hookPath -Encoding utf8
    
    if ($IsLinux) {
        chmod +x $hookPath
    }
    
    Write-Host "✅ Git hooks installed successfully!" -ForegroundColor Green
}

function Invoke-SecurityCheck {
    Write-Host "`n🔒 Kiểm tra bảo mật..." -ForegroundColor Cyan
    Write-Host "Running Bandit..." -ForegroundColor Yellow
    python -m bandit -r src/ --exit-zero
    
    Write-Host "`nRunning Safety..." -ForegroundColor Yellow
    try {
        python -m safety check
    } catch {
        Write-Host "⚠️  Safety issue or error." -ForegroundColor Yellow
    }
    
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
    docker-compose config
    docker-compose build
    docker-compose up -d
    Start-Sleep -Seconds 30
    try {
        $null = Invoke-WebRequest -Uri "http://localhost:8080/api/v2/version" -UseBasicParsing -TimeoutSec 10
        Write-Host "✅ Airflow API responding!" -ForegroundColor Green
    } catch {
        Write-Host "⚠️  API not ready" -ForegroundColor Yellow
    }
    docker-compose down -v
    Write-Host "✅ Docker test completed!" -ForegroundColor Green
}

function Invoke-Clean {
    Write-Host "`n🧹 Dọn dẹp cache và temporary files..." -ForegroundColor Cyan
    Get-ChildItem -Path . -Recurse -Directory -Filter "__pycache__" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".pytest_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".mypy_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Recurse -Directory -Filter ".ruff_cache" | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "✅ Clean completed!" -ForegroundColor Green
}

function Invoke-CILocal {
    Write-Host "`n🚀 Running local CI checks (Parallel)..." -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    $jobs = @()
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 format-check }
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 lint }
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 perf-check }
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 type-check }
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 security-check }
    
    Write-Host "Waiting for tasks to complete..." -ForegroundColor Yellow
    $results = Wait-Job $jobs
    
    $failedCount = 0
    foreach ($job in $results) {
        $jobName = $job.Command
        Receive-Job $job
        if ($job.State -ne "Completed") {
            Write-Host "❌ Job $jobName failed to complete!" -ForegroundColor Red
            $failedCount++
        }
    }
    
    if ($failedCount -gt 0) {
        Write-Host "❌ $failedCount checks failed!" -ForegroundColor Red
        exit 1
    }
    
    # Run tests and DAG validation sequentially as they are more heavy/resource dependent
    Invoke-ValidateDags
    Invoke-Test
    
    Write-Host "✅ All CI checks passed!" -ForegroundColor Green
}

function Invoke-CIFast {
    Write-Host "`n⚡ Running fast CI checks (Parallel)..." -ForegroundColor Cyan
    
    $jobs = @()
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 format-check }
    $jobs += Start-Job -ScriptBlock { pwsh ./scripts/ci.ps1 lint }
    
    Wait-Job $jobs | Receive-Job
    Invoke-ValidateDags
    
    Write-Host "✅ Fast CI passed!" -ForegroundColor Green
}

function Invoke-CIQuick {
    Write-Host "`n⚡ Running quick CI checks..." -ForegroundColor Cyan
    Invoke-FormatCheck
    Invoke-Lint
    Invoke-PerfCheck
    Write-Host "✅ Quick CI passed!" -ForegroundColor Green
}

# Main command router
switch ($Command.ToLower()) {
    "help" { Show-Help }
    "setup" { Invoke-Setup }
    "install" { Install-Dependencies }
    "lint" { Invoke-Lint }
    "lint-fix" { Invoke-LintFix }
    "perf-check" { Invoke-PerfCheck }
    "dead-code" { Invoke-DeadCode }
    "complexity" { Invoke-Complexity }
    "code-quality" { 
        Invoke-PerfCheck
        Invoke-DeadCode
        Invoke-Complexity
    }
    "format" { Invoke-Format }
    "format-check" { Invoke-FormatCheck }
    "type-check" { Invoke-TypeCheck }
    "test" { Invoke-Test }
    "test-fast" { Invoke-TestFast }
    "validate-dags" { Invoke-ValidateDags }
    "install-hooks" { Invoke-InstallHooks }
    "security-check" { Invoke-SecurityCheck }
    "docker-build" { Invoke-DockerBuild }
    "docker-up" { Invoke-DockerUp }
    "docker-down" { Invoke-DockerDown }
    "docker-logs" { Invoke-DockerLogs }
    "docker-test" { Invoke-DockerTest }
    "clean" { Invoke-Clean }
    "ci-local" { Invoke-CILocal }
    "ci-fast" { Invoke-CIFast }
    "ci-quick" { Invoke-CIQuick }
    default {
        Write-Host "❌ Unknown command: $Command" -ForegroundColor Red
        Show-Help
        exit 1
    }
}

