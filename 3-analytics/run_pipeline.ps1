<#
.SYNOPSIS
    Runs the NYC Taxi Analytics & ML pipeline against the shared Hive warehouse.
.EXAMPLE
    .\run_pipeline.ps1 -CheckOnly
    .\run_pipeline.ps1 -WarehouseHost 192.168.100.17 -SkipCancelled
#>
param(
    [string]$WarehouseHost = '192.168.100.17',
    [int]$MaxIter = 50,
    [float]$TrainRatio = 0.8,
    [switch]$SkipCancelled,
    [switch]$CheckOnly
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$ProjectRoot = Split-Path -Parent $ScriptDir

$Jdk17Paths = @(
    'C:\Program Files\Java\jdk-17',
    'C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot',
    $env:JAVA_HOME
)
$FoundJava = $Jdk17Paths | Where-Object {
    $_ -and (Test-Path -LiteralPath (Join-Path $_ 'bin\java.exe'))
} | Select-Object -First 1
if (-not $FoundJava) { throw 'JDK 17 not found. Install it or set JAVA_HOME to a JDK 17 directory.' }
$env:JAVA_HOME = $FoundJava

$HadoopPaths = @('C:\hadoop', 'D:\hadoop', $env:HADOOP_HOME)
$FoundHadoop = $HadoopPaths | Where-Object {
    $_ -and (Test-Path -LiteralPath (Join-Path $_ 'bin\winutils.exe'))
} | Select-Object -First 1
if (-not $FoundHadoop) { throw 'Hadoop Windows binaries not found. Set HADOOP_HOME to a directory containing bin\winutils.exe.' }
$env:HADOOP_HOME = $FoundHadoop

$PythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue
if (-not $PythonCommand) { throw 'python.exe not found on PATH.' }
$Python = $PythonCommand.Source
$PySparkDir = & $Python -c 'import os, sys, pyspark; sys.stdout.write(os.path.dirname(pyspark.__file__) + chr(10))' 2>$null
if ($LASTEXITCODE -ne 0 -or -not $PySparkDir -or -not (Test-Path -LiteralPath $PySparkDir)) {
    throw "PySpark is missing from the selected Python environment: $Python"
}
$env:SPARK_HOME = $PySparkDir.Trim()
$env:PYSPARK_PYTHON = $Python
$env:PYSPARK_DRIVER_PYTHON = $Python
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYTHONIOENCODING = 'utf-8'

$SparkSubmit = Get-Command spark-submit -ErrorAction SilentlyContinue
if (-not $SparkSubmit) { throw 'spark-submit is not available. Activate/install PySpark in this Python environment first.' }
$SparkVersion = & $SparkSubmit.Source --version 2>&1 | Out-String
if ($LASTEXITCODE -ne 0) { throw "spark-submit --version failed: $SparkVersion" }
Write-Host "JAVA_HOME   : $env:JAVA_HOME"
Write-Host "HADOOP_HOME : $env:HADOOP_HOME"
Write-Host "SPARK_HOME  : $env:SPARK_HOME"
Write-Host "Python      : $Python"
Write-Host ($SparkVersion.Trim())

if ($CheckOnly) {
    Write-Host 'Environment checks passed; ready to run the pipeline.' -ForegroundColor Green
    return
}

$HostsFileCandidates = @(
    (Join-Path $FoundHadoop 'custom_hosts'),
    (Join-Path $ProjectRoot 'custom_hosts')
)
$HostsFile = $HostsFileCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1

$SparkArgs = @(
    '--conf', 'spark.sql.catalogImplementation=hive',
    '--conf', "spark.hadoop.hive.metastore.uris=thrift://${WarehouseHost}:9083",
    '--conf', "spark.hadoop.fs.defaultFS=hdfs://${WarehouseHost}:8020",
    '--conf', 'spark.hadoop.dfs.client.use.datanode.hostname=true',
    '--conf', "spark.sql.warehouse.dir=hdfs://${WarehouseHost}:8020/opt/hive/data/warehouse",
    'main.py',
    '--clean-table', 'default.clean_nyc_taxi_tlc',
    '--raw-table', 'default.raw_nyc_taxi',
    '--output-base', "hdfs://${WarehouseHost}:8020/user/hive/warehouse/nyc_taxi_analytics",
    '--max-iter', [string]$MaxIter,
    '--train-ratio', [string]$TrainRatio
)
if ($HostsFile -and -not $HostsFile.Contains(' ')) {
    $HostsFileOption = "-Djdk.net.hosts.file=$($HostsFile.Replace('\', '/'))"
    $SparkArgs += @('--conf', "spark.driver.extraJavaOptions=$HostsFileOption")
    $SparkArgs += @('--conf', "spark.executor.extraJavaOptions=$HostsFileOption")
}
if ($SkipCancelled) { $SparkArgs += '--skip-cancelled' }

Write-Host "Starting analytics against $WarehouseHost..." -ForegroundColor Cyan
Push-Location $ScriptDir
try {
    & $SparkSubmit.Source @SparkArgs
    $PipelineExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $PipelineExitCode
