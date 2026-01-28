#!/usr/bin/env pwsh
# CI/CD Pipeline Check Script
# Checks: Code Style (Ruff), Type Safety (Mypy), Tests, Security
# Usage: ./scripts/ci.ps1

$ErrorActionPreference = "Stop"

# --- Colors ---
$Green = "[32m"
$Red = "[31m"
$Yellow = "[33m"
$Cyan = "[36m"
$Reset = "[0m"

function Log-Info ($msg) { Write-Host "${Cyan}[INFO] $msg${Reset}" }
function Log-Pass ($msg) { Write-Host "${Green}[PASS] $msg${Reset}" }
function Log-Fail ($msg) { Write-Host "${Red}[FAIL] $msg${Reset}" }
function Log-Warn ($msg) { Write-Host "${Yellow}[WARN] $msg${Reset}" }

# --- Check Environment ---
Log-Info "Checking Development Environment..."
if (!(Get-Command "docker" -ErrorAction SilentlyContinue)) {
    Log-Fail "Docker is not installed or not in PATH."
    exit 1
}

# Ensure .env exists
if (!(Test-Path ".env")) {
    Log-Warn ".env file not found. Copying from .env.example..."
    Copy-Item ".env.example" ".env"
}

# --- 1. Linting & Formatting (Ruff) ---
Log-Info "1. Running Ruff (Linting & Formatting)..."
try {
    # Check only first, then format check
    docker compose exec -T airflow-webserver ruff check src/ airflow/dags/ scripts/
    if ($LASTEXITCODE -eq 0) {
        docker compose exec -T airflow-webserver ruff format --check src/ airflow/dags/ scripts/
    }
    
    if ($LASTEXITCODE -eq 0) {
        Log-Pass "Code style is clean."
    } else {
        throw "Ruff failed"
    }
} catch {
    Log-Fail "Ruff found issues. Try running 'ruff format .' to fix automatically."
    # Don't exit yet, run all checks
}

# --- 2. Type Checking (Mypy) ---
Log-Info "2. Running Mypy (Type Checking)..."
try {
    docker compose exec -T airflow-webserver mypy src/
    if ($LASTEXITCODE -eq 0) {
        Log-Pass "Type checking passed."
    } else {
        throw "Mypy failed"
    }
} catch {
    Log-Fail "Mypy found type errors."
}

# --- 3. Unit Tests ---
Log-Info "3. Running Unit Tests..."
try {
    # Run pytest with verbal output
    docker compose exec -T airflow-webserver pytest tests/ -v
    if ($LASTEXITCODE -eq 0) {
        Log-Pass "All unit tests passed."
    } else {
        throw "Tests failed"
    }
} catch {
    Log-Fail "Unit tests failed."
}

# --- Summary ---
Log-Info "----------------------------------------"
Log-Info "✅ CI PIPELINE FINISHED"
Log-Info "----------------------------------------"
