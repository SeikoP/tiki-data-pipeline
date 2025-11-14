# Script để kiểm tra và giải phóng port 3002 trên Windows
Write-Host "Đang kiểm tra port 3002..." -ForegroundColor Yellow

# Tìm process đang sử dụng port 3002
$connection = Get-NetTCPConnection -LocalPort 3002 -ErrorAction SilentlyContinue

if ($connection) {
    $processId = $connection.OwningProcess
    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
    
    if ($process) {
        Write-Host "Tìm thấy process đang sử dụng port 3002:" -ForegroundColor Red
        Write-Host "  Process ID: $processId" -ForegroundColor Red
        Write-Host "  Process Name: $($process.ProcessName)" -ForegroundColor Red
        Write-Host "  Command Line: $($process.Path)" -ForegroundColor Red
        
        $answer = Read-Host "Bạn có muốn kill process này không? (y/n)"
        if ($answer -eq "y" -or $answer -eq "Y") {
            Stop-Process -Id $processId -Force
            Write-Host "Đã kill process $processId" -ForegroundColor Green
            Start-Sleep -Seconds 2
            Write-Host "Port 3002 đã được giải phóng" -ForegroundColor Green
        }
    } else {
        Write-Host "Tìm thấy connection nhưng không thể lấy thông tin process" -ForegroundColor Yellow
    }
} else {
    Write-Host "Port 3002 hiện tại không bị chiếm" -ForegroundColor Green
}

# Kiểm tra lại
$connection = Get-NetTCPConnection -LocalPort 3002 -ErrorAction SilentlyContinue
if (-not $connection) {
    Write-Host "Port 3002 đã sẵn sàng!" -ForegroundColor Green
} else {
    Write-Host "Port 3002 vẫn còn bị chiếm" -ForegroundColor Red
}

