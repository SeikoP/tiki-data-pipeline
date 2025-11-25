<#
.SYNOPSIS
    Convert PostgreSQL dump file (.dump) to SQL format and restore to database

.DESCRIPTION
    Script nay giup:
    1. Convert file dump (.dump) sang format SQL
    2. Restore SQL file vao PostgreSQL database
    3. Kiem tra ket qua restore
    Workaround cho loi "unsupported version (1.16) in file header"

.PARAMETER BackupFile
    Duong dan toi file backup (.dump hoac .sql)
    Vi du: "backups/postgres/backup_20251123.dump"

.PARAMETER Database
    Ten database de restore du lieu
    Vi du: "crawl_data"

.EXAMPLE
    .\restore-postgres-advanced.ps1 -BackupFile "backups/postgres/backup_20251123.dump" -Database "crawl_data"

.NOTES
    Yeu cau:
    - Docker Container: tiki-data-pipeline-postgres-1 phai dang chay
    - File .env phai chua POSTGRES_USER va POSTGRES_PASSWORD
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile,
    
    [Parameter(Mandatory=$true)]
    [string]$Database
)

Write-Host "[*] PostgreSQL Dump to SQL Converter & Restore" -ForegroundColor Cyan
Write-Host ""

# Kiem tra file backup
if (-not (Test-Path $BackupFile)) {
    Write-Host "[!] File backup khong ton tai: $BackupFile" -ForegroundColor Red
    exit 1
}

# Lay thong tin tu .env
$envFile = ".env"
$postgresUser = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_USER=").ToString().Split("=")[1]
$postgresPassword = (Get-Content $envFile | Select-String -Pattern "^POSTGRES_PASSWORD=").ToString().Split("=")[1]
$containerName = "tiki-data-pipeline-postgres-1"

if (-not $postgresUser) {
    $postgresUser = "airflow_user"
}

Write-Host "[+] File backup: $BackupFile" -ForegroundColor Cyan
Write-Host "[+] Database: $Database" -ForegroundColor Cyan
Write-Host "[+] User: $postgresUser" -ForegroundColor Cyan
Write-Host ""

# Copy file vao container
Write-Host "[*] Copy file vao container..." -ForegroundColor Yellow
docker cp $BackupFile "${containerName}:/tmp/backup_file"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[!] Loi khi copy file" -ForegroundColor Red
    exit 1
}

# Detect file format (text SQL or binary dump)
$fileExtension = [System.IO.Path]::GetExtension($BackupFile).ToLower()
Write-Host "[*] File extension: $fileExtension" -ForegroundColor Yellow

if ($fileExtension -eq ".sql" -or $fileExtension -eq "") {
    # Text SQL file - use psql directly
    Write-Host "[*] Dang restore SQL file..." -ForegroundColor Yellow
    docker exec -e PGPASSWORD=$postgresPassword $containerName `
        psql -U $postgresUser -d $Database -f /tmp/backup_file 2>&1 | Out-String -Stream | ForEach-Object {
        if ($_ -match "error|ERROR" -and $_ -notmatch "already exists") {
            Write-Host "   [!] $_" -ForegroundColor Red
        } elseif ($_ -match "warning|WARNING") {
            Write-Host "   [W] $_" -ForegroundColor Yellow
        }
    }
} else {
    # Binary dump file - use pg_restore
    Write-Host "[*] Convert dump file sang SQL format..." -ForegroundColor Yellow
    docker exec $containerName pg_restore -f /tmp/backup_converted.sql /tmp/backup_file 2>&1 | Out-String -Stream | ForEach-Object {
        if ($_ -match "error|ERROR") {
            Write-Host "   [!] $_" -ForegroundColor Red
        } elseif ($_ -match "warning|WARNING") {
            Write-Host "   [W] $_" -ForegroundColor Yellow
        }
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[+] Convert thanh cong!" -ForegroundColor Green
        Write-Host ""
        Write-Host "[*] Dang restore SQL file..." -ForegroundColor Yellow
        
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            psql -U $postgresUser -d $Database -f /tmp/backup_converted.sql 2>&1 | Out-String -Stream | ForEach-Object {
            if ($_ -match "error|ERROR" -and $_ -notmatch "already exists") {
                Write-Host "   [!] $_" -ForegroundColor Red
            } elseif ($_ -match "warning|WARNING") {
                Write-Host "   [W] $_" -ForegroundColor Yellow
            }
        }
    } else {
        Write-Host "[!] Convert failed, thu restore truc tiep..." -ForegroundColor Yellow
        docker exec -e PGPASSWORD=$postgresPassword $containerName `
            pg_restore -U $postgresUser -d $Database --no-owner --no-acl /tmp/backup_file
    }
}

# Cleanup
Write-Host ""
Write-Host "[*] Cleanup..." -ForegroundColor Yellow
docker exec $containerName rm -f /tmp/backup_file /tmp/backup_converted.sql 2>$null | Out-Null

# Kiem tra ket qua
Write-Host ""
Write-Host "[*] Kiem tra du lieu sau restore..." -ForegroundColor Cyan
$productCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM products;" 2>$null
$categoryCount = docker exec $containerName psql -U $postgresUser -d $Database -t -c "SELECT COUNT(*) FROM categories;" 2>$null

if ($productCount) {
    $productCount = ($productCount | Out-String).Trim()
    Write-Host "   [+] Products: $productCount" -ForegroundColor Green
}
if ($categoryCount) {
    $categoryCount = ($categoryCount | Out-String).Trim()
    Write-Host "   [+] Categories: $categoryCount" -ForegroundColor Green
}

Write-Host ""
Write-Host "[+] Hoan tat!" -ForegroundColor Green
