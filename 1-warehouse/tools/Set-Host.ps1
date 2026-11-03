# Set-Host.ps1 - Dong bo dia chi IP cua may vao toan bo cau hinh cluster slice 1.
#
# Vi sao can: Docker Desktop tren Windows cong bo cong 8020/9083/9866 ra moi interface,
# nen client o ngoai mang Docker (Spark chay tren chinh Windows) phai tro dung IP that
# cua may. IP Wi-Fi doi moi lan chuyen mang, va khi do:
#   - DataNode quang ba sai ten -> client bao "could only be written to 0 of the 1
#     minReplication nodes ... 1 node(s) are excluded";
#   - database trong catalog Derby van giu location IP cu -> createTable treo, metastore
#     retry 45 lan x 20s vi khong goi duoc HDFS cu.
#
# Cach dung (PowerShell 5.1 tro len):
#   Set-Location D:\BIGDATA\1-warehouse\tools
#   .\Set-Host.ps1                       # tu phat hien IP cua mang dang co duong dan mac dinh
#   .\Set-Host.ps1 -Ip 192.168.100.19    # chi dinh ro
#   .\Set-Host.ps1 -RebuildCatalog       # + doi ten catalog cu, khoi dong lai metastore schema sach
#
# Script KHONG xoa file nao. Catalog cu chi duoi doi thanh
# `data\metastore\metastore_db_ipcu_<thoi gian>`.

[CmdletBinding()]
param(
  [string]$Ip,
  [switch]$RebuildCatalog
)

$ErrorActionPreference = "Stop"

$root    = Split-Path -Parent $PSScriptRoot
$hdfsXml = Join-Path $root "config\hdfs-site.xml"
$hiveXml = Join-Path $root "config\hive-site.xml"
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Read-Xml([string]$path) {
  # PowerShell 5.1 mac dinh doc theo ANSI (cp1252) se lam hoai tieng Viet trong file.
  return [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
}

function Write-Xml([string]$path, [string]$text) {
  [System.IO.File]::WriteAllText($path, $text, $utf8NoBom)
}

if (-not $Ip) {
  $ifIndex = (Get-NetRoute -DestinationPrefix "0.0.0.0/0" |
              Sort-Object RouteMetric | Select-Object -First 1).ifIndex
  $Ip = (Get-NetIPAddress -AddressFamily IPv4 -InterfaceIndex $ifIndex |
          Where-Object { $_.PrefixOrigin -ne "WellKnown" } |
          Select-Object -First 1).IPAddress
  Write-Host "[auto] IP mang dang co duong dan mac dinh: $Ip"
}

if ($Ip -notmatch '^\d{1,3}(\.\d{1,3}){3}$') { throw "IP khong hop le: $Ip" }
$alias = ($Ip -replace '\.', '-') + ".sslip.io"

# 1) DataNode quang ba mot bia dang (alias) ma ca Windows lan container giai duoc, va
#    KHONG trung voi chuoi dia chi cua chinh client (trung thi DFSClient loai DN khoi
#    pipeline truoc khi kip connect).
$pattern1 = '(?s)(<name>dfs\.datanode\.hostname</name>\s*<value>)[^<]*(</value>)'
# Phai dung ${1}: '$1' + '192.168...' duoc .NET doc thanh nhom "$11" (khong ton tai).
$repl1    = '${1}' + $alias + '${2}'
$text1 = Read-Xml $hdfsXml
$text1 = [regex]::Replace($text1, $pattern1, $repl1)
if ($text1 -notmatch [regex]::Escape("<value>$alias</value>")) {
  throw "Thay doi dfs.datanode.hostname khong thanh cong trong $hdfsXml"
}
Write-Xml $hdfsXml $text1
Write-Host "[hdfs-site.xml] dfs.datanode.hostname   = $alias"

# 2) Warehouse mac dinh cua HMS: moi database tao sau nay se mang location nay.
$pattern2 = '(?s)(<name>metastore\.warehouse\.dir</name>\s*<value>hdfs://)[^<:]+(:8020)'
$repl2    = '${1}' + $Ip + '${2}'
$text2 = Read-Xml $hiveXml
$text2 = [regex]::Replace($text2, $pattern2, $repl2)
if ($text2 -notmatch [regex]::Escape("hdfs://$Ip`:8020")) {
  throw "Thay doi metastore.warehouse.dir khong thanh cong trong $hiveXml"
}
Write-Xml $hiveXml $text2
Write-Host "[hive-site.xml] metastore.warehouse.dir = hdfs://$Ip`:8020/opt/hive/data/warehouse"

Push-Location $root
try {
  # namenode/datanode doc config luc khoi dong, nen bat buoc restart du co rebuild catalog.
  Write-Host "[restart] namenode + datanode de nap hdfs-site.xml moi..."
  docker compose restart namenode datanode | Out-Null

  if ($RebuildCatalog) {
    # Location cua database da bi ghi chet trong Derby, khong theo hive-site moi.
    Write-Host "[catalog] dung metastore de mo khoa Derby..."
    docker compose stop metastore | Out-Null
    $db = Join-Path $root "data\metastore\metastore_db"
    if (Test-Path $db) {
      $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
      $backup = Join-Path $root "data\metastore\metastore_db_ipcu_$stamp"
      Move-Item -Path $db -Destination $backup
      Write-Host "[catalog] giu ban cu: data\metastore\metastore_db_ipcu_$stamp (khong xoa)"
    }
    Write-Host "[catalog] khoi dong metastore voi IS_RESUME=false de schematool tao schema moi..."
    $env:IS_RESUME = "false"
    docker compose up -d metastore | Out-Null
    Remove-Item Env:IS_RESUME -ErrorAction SilentlyContinue
  } else {
    # IS_RESUME duoi vao env cua container luc `up`, nen dung `restart` sau mot lan
    # rebuild (IS_RESUME=false) se khien schematool chay lai tren DB da co schema:
    # "FUNCTION 'NUCLEUS_ASCII' already exists" -> metastore thoat(1). up -d moi la an toan.
    Write-Host "[restart] metastore de nap hive-site.xml moi (IS_RESUME=true)..."
    $env:IS_RESUME = "true"
    docker compose up -d metastore | Out-Null
    Remove-Item Env:IS_RESUME -ErrorAction SilentlyContinue
  }
} finally {
  Pop-Location
}

Write-Host ""
Write-Host "Buoc tiep theo (chay dung thu tu):"
Write-Host "  1. Chay  : docker compose ps                                (ca 4 container healthy)"
Write-Host "  2. Chay  : ..\2-processing\.venv\Scripts\python.exe src\register_hive_tables.py --host $Ip"
Write-Host "  3. Chay  : ..\2-processing\.venv\Scripts\python.exe src\register_raw_nyc_taxi.py --host $Ip"
Write-Host "             (dang ky lai bang raw; du -RebuildCatalog lam rong catalog thi data/ van nguyen ven)"
Write-Host "  4. Chay  : ..\2-processing\.venv\Scripts\python.exe src\register_compat_views.py --host $Ip"
Write-Host "  5. Chay  : ..\2-processing\.venv\Scripts\python.exe src\verify_access.py --host $Ip"
Write-Host ""
Write-Host "Client cua nguoi khac can 2 gia tri: hive.metastore.uris=thrift://$Ip`:9083,"
Write-Host "fs.defaultFS=hdfs://$Ip`:8020, va dfs.client.use.datanode.hostname=true."
