$ErrorActionPreference = "Continue"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "   DATITOS NAM PORTFOLIO - SMOKE CHECK" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

$allPass = $true

# Load .env for credentials if exists
$MINIO_USER = "minioadmin"
$MINIO_PASS = "minioadmin"
if (Test-Path ".env") {
    $envContent = Get-Content ".env"
    foreach ($line in $envContent) {
        if ($line -match "MINIO_ROOT_USER=(.*)") { $MINIO_USER = $Matches[1].Trim() }
        if ($line -match "MINIO_ROOT_PASSWORD=(.*)") { $MINIO_PASS = $Matches[1].Trim() }
    }
}

function Test-Check {
    param([string]$Name, [scriptblock]$Action)
    Write-Host -NoNewline "Checking $Name... "
    try {
        $result = Invoke-Command -ScriptBlock $Action
        # $result might be an array of lines, check if it contains expected success indicators
        if ($result -eq $true -or ($result -ne $false -and $?)) {
            Write-Host "PASS" -ForegroundColor Green
        } else {
            Write-Host "FAIL" -ForegroundColor Red
            $script:allPass = $false
        }
    } catch {
        Write-Host "FAIL ($($_.Exception.Message))" -ForegroundColor Red
        $script:allPass = $false
    }
}

# 1. Contenedores
Test-Check "Container pf_airflow_webserver" { (docker ps -f name=pf_airflow_webserver -q).Length -gt 0 }
Test-Check "Container pf_airflow_scheduler" { (docker ps -f name=pf_airflow_scheduler -q).Length -gt 0 }
Test-Check "Container pf_minio" { (docker ps -f name=pf_minio -q).Length -gt 0 }

# 2. Airflow Import Errors
Test-Check "Airflow List-Import-Errors is Clean" {
    $errors = docker exec pf_airflow_scheduler airflow dags list-import-errors
    if ($errors -match "No data found" -or $errors -match "No import errors found") { return $true }
    else { 
        Write-Host "`n$errors" -ForegroundColor Yellow
        return $false 
    }
}

# 3. MinIO Buckets
Test-Check "MinIO Buckets Exist" {
    $cmd = "mc alias set pf http://pf-minio:9000 $MINIO_USER $MINIO_PASS > /dev/null && mc ls pf/"
    $buckets = docker run --rm --network datitos_nam_pf_default --entrypoint /bin/sh minio/mc:latest -c "$cmd"
    ($buckets -match "nam-pf-raw") -and ($buckets -match "nam-pf-curated") -and ($buckets -match "nam-pf-qa") -and ($buckets -match "nam-pf-public")
}

# 4. Published Data
Test-Check "At least one published bank exists (latest/)" {
    $cmd = "mc alias set pf http://pf-minio:9000 $MINIO_USER $MINIO_PASS > /dev/null && mc ls pf/nam-pf-public/latest/"
    $latest = docker run --rm --network datitos_nam_pf_default --entrypoint /bin/sh minio/mc:latest -c "$cmd"
    return ($latest.Length -gt 0)
}

# 5. Dashboard Accessibility
Test-Check "Streamlit App Responding on :8501" {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 5
        return ($response.StatusCode -eq 200)
    } catch {
        return $false
    }
}

Write-Host "------------------------------------------"
if ($allPass) {
    Write-Host "SMOKE CHECK RESULT: ALL PASS" -ForegroundColor Green
    exit 0
} else {
    Write-Host "SMOKE CHECK RESULT: SOME CHECKS FAILED!" -ForegroundColor Red
    exit 1
}
Write-Host "==========================================" -ForegroundColor Cyan
