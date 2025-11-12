# PowerShell script to manually sync files to another repository
# Usage: .\scripts\utils\sync_to_other_repo.ps1 [target-repo-path]

param(
    [string]$TargetRepo = "../airflow-firecrawl-data-pipeline"
)

$ErrorActionPreference = "Stop"

# Configuration
$SourceRepo = Get-Location
$TargetRepoUrl = "https://github.com/SeikoP/airflow-firecrawl-data-pipeline.git"

# Files and directories to sync
$FilesToSync = @(
    "docker-compose.yaml"
)

$DirsToSync = @(
    "scripts"
)

Write-Host "=== Sync to Other Repository ===" -ForegroundColor Green
Write-Host ""
Write-Host "Source: $SourceRepo"
Write-Host "Target: $TargetRepo"
Write-Host ""

# Check if target repo exists
if (-not (Test-Path $TargetRepo)) {
    Write-Host "Target repository not found. Cloning..." -ForegroundColor Yellow
    git clone $TargetRepoUrl $TargetRepo
} else {
    Write-Host "Target repository found." -ForegroundColor Green
}

# Navigate to target repo
Push-Location $TargetRepo

try {
    # Update target repo
    Write-Host "Updating target repository..." -ForegroundColor Yellow
    git fetch origin
    $branch = git branch --show-current
    if ($branch -eq "main" -or $branch -eq "master") {
        git pull origin $branch
    } else {
        git checkout main -ErrorAction SilentlyContinue
        if ($LASTEXITCODE -ne 0) {
            git checkout master
        }
        git pull origin main -ErrorAction SilentlyContinue
        if ($LASTEXITCODE -ne 0) {
            git pull origin master
        }
    }

    # Sync files
    Write-Host "Syncing files..." -ForegroundColor Yellow

    # Sync docker-compose.yaml
    $dockerComposePath = Join-Path $SourceRepo "docker-compose.yaml"
    if (Test-Path $dockerComposePath) {
        Write-Host "  - Syncing docker-compose.yaml..." -ForegroundColor Cyan
        Copy-Item $dockerComposePath "docker-compose.yaml" -Force
        Write-Host "  ✓ docker-compose.yaml synced" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ docker-compose.yaml not found" -ForegroundColor Red
    }

    # Sync scripts directory
    $scriptsPath = Join-Path $SourceRepo "scripts"
    if (Test-Path $scriptsPath) {
        Write-Host "  - Syncing scripts directory..." -ForegroundColor Cyan
        if (Test-Path "scripts") {
            Remove-Item "scripts" -Recurse -Force
        }
        Copy-Item $scriptsPath "scripts" -Recurse -Force
        Write-Host "  ✓ scripts directory synced" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ scripts directory not found" -ForegroundColor Red
    }

    # Update .gitignore
    if (-not (Test-Path ".gitignore")) {
        New-Item -ItemType File -Name ".gitignore" -Force | Out-Null
    }

    # Add Python cache patterns to .gitignore if not exists
    $gitignoreContent = Get-Content ".gitignore" -Raw -ErrorAction SilentlyContinue
    if ($gitignoreContent -notmatch "scripts/__pycache__") {
        Add-Content ".gitignore" "`n# Python cache in scripts"
        Add-Content ".gitignore" "scripts/__pycache__/"
        Add-Content ".gitignore" "scripts/**/__pycache__/"
        Add-Content ".gitignore" "*.pyc"
    }

    # Check for changes
    Write-Host ""
    Write-Host "Checking for changes..." -ForegroundColor Yellow
    git add -A

    $changes = git diff --staged --name-only
    if (-not $changes) {
        Write-Host "No changes to commit." -ForegroundColor Yellow
        return
    }

    # Show changes
    Write-Host "Changes detected:" -ForegroundColor Green
    git status --short

    # Ask for confirmation
    $response = Read-Host "Do you want to commit and push these changes? (y/n)"
    if ($response -ne "y" -and $response -ne "Y") {
        Write-Host "Aborted. Changes are staged but not committed." -ForegroundColor Yellow
        return
    }

    # Get source repo info
    $originalLocation = Get-Location
    Set-Location $SourceRepo
    try {
        $sourceOrigin = git remote get-url origin 2>$null
        if (-not $sourceOrigin) {
            $sourceOrigin = "local"
        }
        $sourceCommit = git rev-parse HEAD 2>$null
        if (-not $sourceCommit) {
            $sourceCommit = "unknown"
        }
    } catch {
        $sourceOrigin = "local"
        $sourceCommit = "unknown"
    } finally {
        Set-Location $originalLocation
    }

    # Commit and push
    Write-Host "Committing changes..." -ForegroundColor Yellow
    $commitMsg = @"
chore: Sync docker-compose.yaml and scripts from source repo

Synced from: $sourceOrigin
Source commit: $sourceCommit
Synced at: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
"@

    git commit -m $commitMsg

    Write-Host "Pushing changes..." -ForegroundColor Yellow
    $currentBranch = git branch --show-current
    git push origin $currentBranch

    Write-Host ""
    Write-Host "✅ Sync completed successfully!" -ForegroundColor Green
    Write-Host ""
} finally {
    Pop-Location
}

