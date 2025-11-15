# PowerShell CI/CD Script - Entry point
# Sử dụng: .\ci.ps1 <command>
# Hoặc: .\scripts\ci.ps1 <command>

$scriptPath = if ($PSScriptRoot) {
    Join-Path $PSScriptRoot "scripts\ci.ps1"
} else {
    Join-Path (Get-Location) "scripts\ci.ps1"
}

if (Test-Path $scriptPath) {
    & $scriptPath @args
} else {
    Write-Host "❌ Script not found at: $scriptPath" -ForegroundColor Red
    Write-Host "Current directory: $(Get-Location)" -ForegroundColor Yellow
    exit 1
}

