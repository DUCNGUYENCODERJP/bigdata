<#
.SYNOPSIS
    Starts the NYC Taxi dashboard from any working directory.
.DESCRIPTION
    Uses an existing Python environment, JDK 17 and Hadoop Windows binaries.
    A compatible project snapshot is shown immediately; new Hive month partitions
    are detected and appended automatically by the server.
#>
param(
    [int]$Port = 8080,
    [string]$WarehouseHost = "192.168.100.17",
    [string]$Partition = "all",
    [string]$CleanTable = "default.clean_nyc_taxi_tlc",
    [string]$RawTable = "default.raw_nyc_taxi",
    [switch]$NoBrowser
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = $PSScriptRoot
$ServerScript = Join-Path $ProjectRoot '4-bi\server.py'
if (-not (Test-Path -LiteralPath $ServerScript -PathType Leaf)) {
    throw "Dashboard server not found: $ServerScript"
}

$PythonCandidates = @(
    (Join-Path $ProjectRoot '.venv\Scripts\python.exe'),
    (Join-Path $ProjectRoot '3-analytics\.venv\Scripts\python.exe')
)
$PythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
if ($PythonCommand) { $PythonCandidates += $PythonCommand.Source }
$Python = $null
# Mot venv hong (base Python khong con o may nay) in ra stderr; Stop + stderr cua
# lenh goc nam hoai o day bi PS 5.1 chuyen thanh NativeCommandError va lam script chet
# truoc khi kip thu cac ung vien con lai. Probe chi la phep thu, khong phai loi chay.
$ProbePreference = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
try {
    foreach ($Candidate in ($PythonCandidates | Select-Object -Unique)) {
        if (-not (Test-Path -LiteralPath $Candidate -PathType Leaf)) { continue }
        & $Candidate -c 'import pyspark, numpy' 2>$null
        if ($LASTEXITCODE -eq 0) {
            $Python = $Candidate
            break
        }
    }
} finally {
    $ErrorActionPreference = $ProbePreference
}
if (-not $Python) {
    throw 'No Python environment has both PySpark and NumPy. Install the analytics requirements or set up a project .venv.'
}

$JavaCandidates = @(
    'C:\Program Files\Java\jdk-17',
    'C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot',
    $env:JAVA_HOME
)
$JavaHome = $JavaCandidates | Where-Object { $_ -and (Test-Path -LiteralPath (Join-Path $_ 'bin\java.exe')) } | Select-Object -First 1
if (-not $JavaHome) { throw 'JDK 17 not found. Set JAVA_HOME to an installed JDK 17 directory.' }
$env:JAVA_HOME = $JavaHome
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

$HadoopCandidates = @('C:\hadoop', 'D:\hadoop', $env:HADOOP_HOME)
$HadoopHome = $HadoopCandidates | Where-Object { $_ -and (Test-Path -LiteralPath (Join-Path $_ 'bin\winutils.exe')) } | Select-Object -First 1
if (-not $HadoopHome) { throw 'Hadoop Windows binaries not found. Set HADOOP_HOME to a directory containing bin\winutils.exe.' }
$env:HADOOP_HOME = $HadoopHome
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:PYTHONIOENCODING = 'utf-8'

$CacheFile = Join-Path $ProjectRoot '4-bi\cache\dashboard_hive_cache.json'
$CacheIdentityMatches = $false
if ($Partition -eq 'all' -and (Test-Path -LiteralPath $CacheFile -PathType Leaf)) {
    try {
        $Cached = Get-Content -LiteralPath $CacheFile -Raw -Encoding UTF8 | ConvertFrom-Json
        $Identity = $Cached.identity
        $CacheIdentityMatches = (
            $Cached.version -eq 1 -and
            $Identity.warehouse_host -eq $WarehouseHost -and
            $Identity.partition -eq $Partition -and
            $Identity.clean_table -eq $CleanTable -and
            $Identity.raw_table -eq $RawTable -and
            $Cached.data.kpis.partition -eq $Partition
        )
        if ($CacheIdentityMatches) {
            $Kpis = $Cached.data.kpis
            $StorageSize = $Kpis.storage_size
            $TotalRecords = [string]::Format('{0:N0}', $Kpis.total_records)
            Write-Host "Snapshot hợp lệ: $StorageSize • $TotalRecords chuyến • $($Kpis.period)" -ForegroundColor Cyan
        }
    } catch {
        Write-Host "Không đọc được snapshot; server sẽ kiểm tra cache và Hive: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

Write-Host "Python       : $Python"
Write-Host "Warehouse    : $WarehouseHost"
Write-Host "Partition    : $Partition"
Write-Host "Dashboard    : http://localhost:$Port/dashboard.html"
if (-not $CacheIdentityMatches) {
    Write-Host 'Cache phù hợp chưa có; server sẽ thử cache riêng rồi mới truy vấn Hive.' -ForegroundColor Yellow
}

$ServerArgs = @(
    $ServerScript,
    '--port', [string]$Port,
    '--host', $WarehouseHost,
    '--partition', $Partition,
    '--clean-table', $CleanTable,
    '--raw-table', $RawTable
)
if ($NoBrowser) { $ServerArgs += '--no-browser' }

Push-Location $ProjectRoot
try {
    # Server ghi log bang module logging -> moi dong la one stderr record. Voi Stop,
    # PowerShell 5.1 nam hoai NativeCommandError va cat server ngay dong banner dau tien.
    $ServerPreference = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        & $Python @ServerArgs
        $ServerExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $ServerPreference
    }
} finally {
    Pop-Location
}
exit $ServerExitCode
