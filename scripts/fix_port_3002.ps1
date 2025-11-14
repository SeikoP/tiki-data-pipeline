# Script để fix port 3002 conflict

Write-Host "🔍 Checking port 3002 usage..." -ForegroundColor Cyan

# Kiểm tra port 3002
$port3002 = netstat -ano | Select-String ":3002" | Select-String "LISTENING"
if ($port3002) {
    Write-Host "⚠️  Port 3002 is in use:" -ForegroundColor Yellow
    $port3002 | ForEach-Object {
        $line = $_.Line
        $pid = ($line -split '\s+')[-1]
        Write-Host "   PID: $pid" -ForegroundColor White
        
        # Kiểm tra process
        $process = Get-Process -Id $pid -ErrorAction SilentlyContinue
        if ($process) {
            Write-Host "   Process: $($process.ProcessName)" -ForegroundColor White
            Write-Host "   Path: $($process.Path)" -ForegroundColor Gray
        }
    }
    
    Write-Host ""
    Write-Host "💡 Solutions:" -ForegroundColor Yellow
    Write-Host "   1. Stop old containers:" -ForegroundColor White
    Write-Host "      docker-compose down" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   2. Kill process (if not Docker):" -ForegroundColor White
    Write-Host "      Stop-Process -Id <PID> -Force" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   3. Change port in docker-compose.yaml:" -ForegroundColor White
    Write-Host "      Set PORT=3003 in .env file" -ForegroundColor Cyan
} else {
    Write-Host "✅ Port 3002 is free" -ForegroundColor Green
}

Write-Host ""
Write-Host "🔍 Checking Docker containers using port 3002:" -ForegroundColor Cyan
docker ps -a --filter "publish=3002" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"

Write-Host ""
Write-Host "💡 To fix:" -ForegroundColor Yellow
Write-Host "   docker-compose down" -ForegroundColor Cyan
Write-Host '   docker-compose --profile firecrawl up -d' -ForegroundColor Cyan

