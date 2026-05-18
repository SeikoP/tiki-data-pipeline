#!/usr/bin/env pwsh
# Container-based CI checks for local development.
# Usage: ./scripts/ci.ps1

$ErrorActionPreference = "Stop"

$FailedChecks = New-Object System.Collections.Generic.List[string]

function Log-Info($Message) { Write-Host "[INFO] $Message" -ForegroundColor Cyan }
function Log-Pass($Message) { Write-Host "[PASS] $Message" -ForegroundColor Green }
function Log-Fail($Message) { Write-Host "[FAIL] $Message" -ForegroundColor Red }
function Log-Warn($Message) { Write-Host "[WARN] $Message" -ForegroundColor Yellow }

function Invoke-Check {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [scriptblock]$Command
    )

    Log-Info $Name
    try {
        & $Command
        if ($LASTEXITCODE -ne 0) {
            throw "Command exited with code $LASTEXITCODE"
        }
        Log-Pass $Name
    } catch {
        Log-Fail "$Name failed: $_"
        $script:FailedChecks.Add($Name)
    }
}

Log-Info "Checking local CI environment..."
if (!(Get-Command "docker" -ErrorAction SilentlyContinue)) {
    Log-Fail "Docker is not installed or not in PATH."
    exit 1
}

if (!(Test-Path ".env")) {
    Log-Warn ".env not found. Copying from .env.example..."
    Copy-Item ".env.example" ".env"
}

$runningServices = docker compose ps --services --filter "status=running"
if ($LASTEXITCODE -ne 0 -or ($runningServices -notcontains "airflow-webserver")) {
    Log-Fail "airflow-webserver is not running. Start the stack with 'docker compose up -d --build'."
    exit 1
}

Invoke-Check "Ruff lint" {
    docker compose exec -T airflow-webserver ruff check src/ airflow/dags/ scripts/ tests/
}

Invoke-Check "Ruff format check" {
    docker compose exec -T airflow-webserver ruff format --check src/ airflow/dags/ scripts/ tests/
}

Invoke-Check "Mypy type check" {
    docker compose exec -T airflow-webserver mypy src/
}

Invoke-Check "Airflow DAG validation" {
    docker compose exec -T airflow-webserver python scripts/ci.py validate-dags
}

Invoke-Check "Pytest unit suite" {
    docker compose exec -T airflow-webserver pytest tests/ -m "not integration and not slow" -v
}

Log-Info "----------------------------------------"
if ($FailedChecks.Count -gt 0) {
    Log-Fail "CI failed: $($FailedChecks -join ', ')"
    exit 1
}

Log-Pass "CI pipeline passed."
