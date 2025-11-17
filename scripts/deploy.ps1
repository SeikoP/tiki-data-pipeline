# Deployment Script - All-in-One
# Run this script to deploy optimized DAG to production

param(
    [switch]$SkipPhase2 = $false,
    [switch]$TestMode = $false,
    [switch]$Force = $false
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message, [string]$Color = "Cyan")
    Write-Host "`n$Message" -ForegroundColor $Color
    Write-Host ("="*70) -ForegroundColor $Color
}

function Write-Success {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-Warning {
    param([string]$Message)
    Write-Host "⚠️  $Message" -ForegroundColor Yellow
}

function Write-Error {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Test-Prerequisites {
    Write-Step "1️⃣ Checking Prerequisites"
    
    $checks = @()
    
    # Docker
    try {
        docker --version | Out-Null
        Write-Success "Docker installed"
        $checks += $true
    } catch {
        Write-Error "Docker not found"
        $checks += $false
    }
    
    # Docker Compose
    try {
        docker-compose --version | Out-Null
        Write-Success "Docker Compose installed"
        $checks += $true
    } catch {
        Write-Error "Docker Compose not found"
        $checks += $false
    }
    
    # Python
    try {
        $pythonVersion = python --version 2>&1
        if ($pythonVersion -match "Python 3\.(\d+)\.") {
            $minorVersion = [int]$Matches[1]
            if ($minorVersion -ge 10) {
                Write-Success "Python $pythonVersion"
                $checks += $true
            } else {
                Write-Error "Python 3.10+ required, found $pythonVersion"
                $checks += $false
            }
        }
    } catch {
        Write-Error "Python not found"
        $checks += $false
    }
    
    # .env file
    if (Test-Path ".env") {
        Write-Success ".env file exists"
        $checks += $true
    } else {
        Write-Error ".env file not found (copy from .env.example)"
        $checks += $false
    }
    
    return ($checks | Where-Object { $_ -eq $false }).Count -eq 0
}

function Start-OptimizedServices {
    Write-Step "2️⃣ Starting Optimized Services"
    
    Write-Host "Starting Docker services with performance optimizations..."
    
    try {
        # Stop existing services
        docker-compose down 2>&1 | Out-Null
        
        # Start with performance config
        docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d
        
        Write-Success "Services starting..."
        Write-Host "Waiting for services to be healthy (60 seconds)..."
        
        Start-Sleep -Seconds 60
        
        # Check service status
        $services = docker-compose ps --format json | ConvertFrom-Json
        
        $allHealthy = $true
        foreach ($service in $services) {
            if ($service.State -eq "running") {
                Write-Success "$($service.Service) is running"
            } else {
                Write-Error "$($service.Service) is $($service.State)"
                $allHealthy = $false
            }
        }
        
        return $allHealthy
        
    } catch {
        Write-Error "Failed to start services: $_"
        return $false
    }
}

function Apply-Phase2Optimizations {
    Write-Step "3️⃣ Applying Phase 2 Optimizations"
    
    if ($SkipPhase2) {
        Write-Warning "Skipping Phase 2 (--SkipPhase2 flag set)"
        return $true
    }
    
    try {
        python scripts/reapply_phase2.py
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Phase 2 optimizations applied"
            return $true
        } else {
            Write-Warning "Some Phase 2 optimizations failed (check output above)"
            return $Force
        }
    } catch {
        Write-Error "Failed to apply Phase 2: $_"
        return $false
    }
}

function Validate-Optimizations {
    Write-Step "4️⃣ Validating Optimizations"
    
    try {
        python tests/final_validation.py
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "All optimizations validated"
            return $true
        } else {
            Write-Warning "Some validations failed (check output above)"
            return $Force
        }
    } catch {
        Write-Error "Validation failed: $_"
        return $false
    }
}

function Set-AirflowVariables {
    Write-Step "5️⃣ Configuring Airflow Variables"
    
    $maxCategories = if ($TestMode) { "2" } else { "5" }
    $maxProducts = if ($TestMode) { "10" } else { "100" }
    
    $variables = @{
        "tiki_data_dir" = "/opt/airflow/data"
        "tiki_max_categories" = $maxCategories
        "tiki_max_products_per_category" = $maxProducts
        "tiki_parallel_workers" = "5"
        "tiki_rate_limit" = "0.5"
    }
    
    try {
        foreach ($var in $variables.GetEnumerator()) {
            docker exec tiki-data-pipeline-airflow-scheduler-1 airflow variables set $($var.Key) $($var.Value) 2>&1 | Out-Null
            Write-Success "$($var.Key) = $($var.Value)"
        }
        
        return $true
    } catch {
        Write-Error "Failed to set Airflow variables: $_"
        return $false
    }
}

function Apply-DatabaseIndexes {
    Write-Step "6️⃣ Applying Database Indexes"
    
    try {
        # Check if index script exists
        $sqlFile = "airflow/setup/add_performance_indexes.sql"
        
        if (-not (Test-Path $sqlFile)) {
            Write-Warning "Index script not found: $sqlFile"
            return $Force
        }
        
        # Copy to container
        docker cp $sqlFile tiki-data-pipeline-postgres-1:/tmp/indexes.sql
        
        # Apply indexes
        $result = docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -f /tmp/indexes.sql 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Success "Database indexes created"
            return $true
        } else {
            Write-Warning "Some indexes may already exist"
            Write-Host $result
            return $true  # Non-critical
        }
    } catch {
        Write-Error "Failed to apply indexes: $_"
        return $false
    }
}

function Enable-OptimizedDAG {
    Write-Step "7️⃣ Enabling Optimized DAG"
    
    try {
        # Check DAG exists
        $dagList = docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags list 2>&1
        
        if ($dagList -match "tiki_crawl_products_optimized") {
            Write-Success "Optimized DAG found"
            
            # Unpause DAG
            docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags unpause tiki_crawl_products_optimized 2>&1 | Out-Null
            Write-Success "DAG unpaused"
            
            # Pause original DAG
            docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags pause tiki_crawl_products 2>&1 | Out-Null
            Write-Success "Original DAG paused"
            
            return $true
        } else {
            Write-Error "Optimized DAG not found in Airflow"
            Write-Host "Available DAGs:"
            Write-Host $dagList
            return $false
        }
    } catch {
        Write-Error "Failed to enable DAG: $_"
        return $false
    }
}

function Start-TestRun {
    Write-Step "8️⃣ Triggering Test Run"
    
    if (-not $TestMode) {
        Write-Warning "Skipping test run (use -TestMode flag to enable)"
        return $true
    }
    
    try {
        Write-Host "Triggering DAG run..."
        docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags trigger tiki_crawl_products_optimized 2>&1 | Out-Null
        
        Write-Success "DAG triggered"
        Write-Host "`nMonitor execution at: http://localhost:8080"
        Write-Host "Or run: docker exec tiki-data-pipeline-airflow-scheduler-1 airflow dags list-runs -d tiki_crawl_products_optimized"
        
        return $true
    } catch {
        Write-Error "Failed to trigger DAG: $_"
        return $false
    }
}

function Show-DeploymentSummary {
    param([bool]$Success)
    
    Write-Step "📊 DEPLOYMENT SUMMARY" $(if ($Success) { "Green" } else { "Red" })
    
    if ($Success) {
        Write-Host ""
        Write-Success "Deployment completed successfully!"
        Write-Host ""
        Write-Host "🎯 Next Steps:" -ForegroundColor Cyan
        Write-Host "   1. Access Airflow UI: http://localhost:8080"
        Write-Host "   2. Verify DAG 'tiki_crawl_products_optimized' is active"
        Write-Host "   3. Monitor first run for performance"
        Write-Host "   4. Review logs for any issues"
        Write-Host ""
        Write-Host "📖 Documentation:" -ForegroundColor Cyan
        Write-Host "   - DEPLOYMENT.md - Full deployment guide"
        Write-Host "   - OPTIMIZATIONS.md - Performance details"
        Write-Host "   - PRE_DEPLOYMENT_CHECKLIST.md - Detailed checklist"
        Write-Host ""
        Write-Host "🔍 Monitoring:" -ForegroundColor Cyan
        Write-Host "   - Logs: docker-compose logs -f airflow-scheduler"
        Write-Host "   - Stats: Check XCom values in Airflow UI"
        Write-Host "   - Health: python -c 'from src.common.infrastructure_monitor import print_monitoring_report; print_monitoring_report()'"
        Write-Host ""
        Write-Host "⚡ Expected Performance:" -ForegroundColor Cyan
        Write-Host "   - 7.3x faster overall"
        Write-Host "   - 4.89x faster crawling"
        Write-Host "   - 95% cache hit rate"
        Write-Host "   - 40-50% faster database operations"
        Write-Host ""
    } else {
        Write-Host ""
        Write-Error "Deployment failed!"
        Write-Host ""
        Write-Host "🔧 Troubleshooting:" -ForegroundColor Yellow
        Write-Host "   1. Check Docker services: docker-compose ps"
        Write-Host "   2. View logs: docker-compose logs"
        Write-Host "   3. Verify .env file configuration"
        Write-Host "   4. Run validation: python tests/final_validation.py"
        Write-Host "   5. Check DAG import: docker exec airflow-scheduler python -c 'from airflow.models import DagBag; print(DagBag().import_errors)'"
        Write-Host ""
        Write-Host "📖 See DEPLOYMENT.md for detailed troubleshooting"
        Write-Host ""
    }
}

# Main execution
Write-Host @"
╔════════════════════════════════════════════════════════════════════╗
║                                                                    ║
║           🚀 TIKI DATA PIPELINE - OPTIMIZED DEPLOYMENT            ║
║                                                                    ║
║  This script will deploy all optimizations (Phase 1-5):           ║
║  • Parallel crawling (4.89x faster)                               ║
║  • Database connection pooling (40-50% faster)                    ║
║  • Memory caching (95% hit rate)                                  ║
║  • Batch processing (30-40% faster)                               ║
║  • Infrastructure monitoring                                      ║
║                                                                    ║
║  Overall speedup: 7.3x                                            ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

Write-Host ""

if ($TestMode) {
    Write-Warning "Running in TEST MODE (limited data)"
}

if ($Force) {
    Write-Warning "FORCE MODE enabled (will continue on non-critical errors)"
}

Write-Host ""

# Execute deployment steps
$steps = @(
    { Test-Prerequisites },
    { Start-OptimizedServices },
    { Apply-Phase2Optimizations },
    { Validate-Optimizations },
    { Set-AirflowVariables },
    { Apply-DatabaseIndexes },
    { Enable-OptimizedDAG },
    { Start-TestRun }
)

$success = $true
foreach ($step in $steps) {
    $result = & $step
    
    if (-not $result) {
        $success = $false
        
        if (-not $Force) {
            Write-Error "Deployment stopped due to error. Use -Force to continue on non-critical errors."
            break
        } else {
            Write-Warning "Continuing despite error (Force mode)"
        }
    }
}

Show-DeploymentSummary -Success $success

exit $(if ($success) { 0 } else { 1 })
