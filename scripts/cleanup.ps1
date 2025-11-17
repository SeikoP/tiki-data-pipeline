# ============================================
# TIKI DATA PIPELINE - PROJECT CLEANUP SCRIPT
# ============================================
# Script tự động cleanup toàn bộ dự án
# Usage: .\scripts\cleanup.ps1 [-All] [-Logs] [-Cache] [-Docker] [-Data] [-DryRun]

param(
    [switch]$All,
    [switch]$Logs,
    [switch]$Cache,
    [switch]$Docker,
    [switch]$Data,
    [switch]$DryRun
)

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "🧹 TIKI DATA PIPELINE - CLEANUP" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

$totalCleaned = 0

# ============================================
# 1. CLEANUP PYTHON CACHE
# ============================================
if ($Cache -or $All) {
    Write-Host "📦 Cleaning Python cache..." -ForegroundColor Yellow
    
    $cacheFolders = @(".mypy_cache", ".ruff_cache", ".pytest_cache")
    foreach ($folder in $cacheFolders) {
        if (Test-Path $folder) {
            if (!$DryRun) {
                $size = (Get-ChildItem $folder -Recurse -File | Measure-Object -Property Length -Sum).Sum / 1MB
                Remove-Item $folder -Recurse -Force -ErrorAction SilentlyContinue
                $totalCleaned += $size
                Write-Host "   ✓ Deleted $folder ($([math]::Round($size, 2)) MB)" -ForegroundColor Green
            } else {
                Write-Host "   [DRY RUN] Would delete $folder" -ForegroundColor Gray
            }
        }
    }
    
    # Xóa __pycache__ folders
    $pycacheFolders = Get-ChildItem -Path . -Filter "__pycache__" -Recurse -Directory -ErrorAction SilentlyContinue
    $pycacheCount = ($pycacheFolders | Measure-Object).Count
    if ($pycacheCount -gt 0) {
        if (!$DryRun) {
            $pycacheFolders | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
            Write-Host "   ✓ Deleted $pycacheCount __pycache__ folders" -ForegroundColor Green
        } else {
            Write-Host "   [DRY RUN] Would delete $pycacheCount __pycache__ folders" -ForegroundColor Gray
        }
    }
    
    # Xóa .pyc files
    $pycFiles = Get-ChildItem -Path . -Filter "*.pyc" -Recurse -File -ErrorAction SilentlyContinue
    $pycCount = ($pycFiles | Measure-Object).Count
    if ($pycCount -gt 0) {
        if (!$DryRun) {
            $pycFiles | Remove-Item -Force -ErrorAction SilentlyContinue
            Write-Host "   ✓ Deleted $pycCount .pyc files" -ForegroundColor Green
        } else {
            Write-Host "   [DRY RUN] Would delete $pycCount .pyc files" -ForegroundColor Gray
        }
    }
}

# ============================================
# 2. CLEANUP AIRFLOW LOGS
# ============================================
if ($Logs -or $All) {
    Write-Host "`n📋 Cleaning Airflow logs (older than 7 days)..." -ForegroundColor Yellow
    
    if (Test-Path "airflow/logs") {
        $cutoffDate = (Get-Date).AddDays(-7)
        $oldLogs = Get-ChildItem "airflow/logs" -Recurse -File -ErrorAction SilentlyContinue | 
                   Where-Object {$_.LastWriteTime -lt $cutoffDate}
        
        $logCount = ($oldLogs | Measure-Object).Count
        if ($logCount -gt 0) {
            $logSize = ($oldLogs | Measure-Object -Property Length -Sum).Sum / 1MB
            
            if (!$DryRun) {
                $oldLogs | Remove-Item -Force -ErrorAction SilentlyContinue
                $totalCleaned += $logSize
                Write-Host "   ✓ Deleted $logCount log files ($([math]::Round($logSize, 2)) MB)" -ForegroundColor Green
            } else {
                Write-Host "   [DRY RUN] Would delete $logCount log files ($([math]::Round($logSize, 2)) MB)" -ForegroundColor Gray
            }
        } else {
            Write-Host "   ℹ No old logs to delete" -ForegroundColor Cyan
        }
    }
}

# ============================================
# 3. CLEANUP DOCKER
# ============================================
if ($Docker -or $All) {
    Write-Host "`n🐳 Cleaning Docker resources..." -ForegroundColor Yellow
    
    if (!$DryRun) {
        # Xóa unused images
        Write-Host "   Pruning unused images..." -ForegroundColor Gray
        docker image prune -a -f 2>&1 | Out-Null
        
        # Xóa build cache
        Write-Host "   Pruning build cache..." -ForegroundColor Gray
        $output = docker builder prune -a -f 2>&1 | Select-String "Total:"
        if ($output) {
            Write-Host "   ✓ $output" -ForegroundColor Green
        }
        
        # Xóa unused networks
        Write-Host "   Pruning unused networks..." -ForegroundColor Gray
        docker network prune -f 2>&1 | Out-Null
        
        # Xóa dangling volumes (CẨN THẬN!)
        Write-Host "   Checking for dangling volumes..." -ForegroundColor Gray
        $danglingVolumes = docker volume ls -qf dangling=true
        if ($danglingVolumes) {
            Write-Host "   ⚠ Found dangling volumes (manual cleanup recommended)" -ForegroundColor Yellow
        }
        
        Write-Host "   ✓ Docker cleanup completed" -ForegroundColor Green
    } else {
        Write-Host "   [DRY RUN] Would prune Docker resources" -ForegroundColor Gray
    }
}

# ============================================
# 4. CLEANUP OLD DATA FILES
# ============================================
if ($Data -or $All) {
    Write-Host "`n📊 Cleaning old data files (older than 30 days)..." -ForegroundColor Yellow
    
    $dataDirs = @("data/demo", "data/test_output")
    $cutoffDate = (Get-Date).AddDays(-30)
    
    foreach ($dir in $dataDirs) {
        if (Test-Path $dir) {
            $oldFiles = Get-ChildItem $dir -Recurse -File -ErrorAction SilentlyContinue | 
                       Where-Object {$_.LastWriteTime -lt $cutoffDate}
            
            $fileCount = ($oldFiles | Measure-Object).Count
            if ($fileCount -gt 0) {
                $fileSize = ($oldFiles | Measure-Object -Property Length -Sum).Sum / 1MB
                
                if (!$DryRun) {
                    $oldFiles | Remove-Item -Force -ErrorAction SilentlyContinue
                    $totalCleaned += $fileSize
                    Write-Host "   ✓ Deleted $fileCount files from $dir ($([math]::Round($fileSize, 2)) MB)" -ForegroundColor Green
                } else {
                    Write-Host "   [DRY RUN] Would delete $fileCount files from $dir ($([math]::Round($fileSize, 2)) MB)" -ForegroundColor Gray
                }
            }
        }
    }
}

# ============================================
# SUMMARY
# ============================================
Write-Host "`n=====================================" -ForegroundColor Cyan
Write-Host "✅ CLEANUP SUMMARY" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Cyan

if (!$DryRun) {
    Write-Host "Total space reclaimed: $([math]::Round($totalCleaned, 2)) MB" -ForegroundColor Green
    Write-Host ""
    Write-Host "💡 TIP: Run with -DryRun to preview changes before applying" -ForegroundColor Cyan
} else {
    Write-Host "DRY RUN - No changes were made" -ForegroundColor Yellow
    Write-Host "Run without -DryRun to apply changes" -ForegroundColor Yellow
}

Write-Host ""
