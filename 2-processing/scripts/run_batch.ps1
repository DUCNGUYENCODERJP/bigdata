param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d{4}-(0[1-9]|1[0-2])$')]
    [string]$From,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^\d{4}-(0[1-9]|1[0-2])$')]
    [string]$To,

    # --force ghi lại cả partition đã xong: chỉ bật ở lượt chạy đầu tiên.
    [switch]$Force,

    [switch]$DryRun,

    # Tắt ngủ khi cắm AC trong lúc batch chạy (giá trị cũ được ghi vào transcript để khôi phục).
    [switch]$KeepAwake,

    # JDK 17 và vùng tạm là machine-specific; đổi tham số khi máy khác không có sẵn D:\tmp.
    [string]$JavaHome = 'C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot',

    [string]$TempDir = 'D:\tmp'
)

$ErrorActionPreference = 'Stop'
# Lấy thư mục dự án từ vị trí script để người khác clone về là chạy được, không phải sửa lệnh.
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$Runner = Join-Path $ProjectRoot 'src\batch_runner.py'
$ReportsDir = Join-Path $ProjectRoot 'reports'
$Stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$TranscriptPath = Join-Path $ReportsDir ("transcript_{0}_{1}_{2}.txt" -f $From, $To, $Stamp)

# Console UTF-8 để tiếng Việt của script và log Python không bị vỡ dấu.
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }

# Java 17 (bắt buộc, máy còn Java 25 trong PATH) và vùng tạm luôn trên ổ D:.
$env:JAVA_HOME = $JavaHome
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
$env:TMP = $TempDir
$env:TEMP = $TempDir
# Python ghi log/CSV bằng UTF-8 thay vì codepage console.
$env:PYTHONIOENCODING = 'utf-8'

New-Item -ItemType Directory -Path $ReportsDir -Force | Out-Null
New-Item -ItemType Directory -Path $TempDir -Force | Out-Null

function Write-DiskReport {
    param([string]$Label)
    Write-Host "[$Label] disk space"
    # Máy khác có thể không có ổ D:; lỗi ở đây không được phép giết batch (ErrorActionPreference=Stop).
    foreach ($Drive in @('C', 'D')) {
        try {
            $Info = [System.IO.DriveInfo]::new("${Drive}:\")
            Write-Host ("{0}: {1:N2} GB free" -f $Info.Name, ($Info.AvailableFreeSpace / 1GB))
        }
        catch {
            Write-Host ("{0}: khong co o nay" -f $Drive)
        }
    }
}

function Set-KeepAwake {
    # Batch 188 tháng chạy ~10 giờ; máy vào ngủ giữa chừng sẽ đứt giữa download và giữa phiên Spark.
    # powercfg /change chỉ sửa power plan của user hiện tại khi cắm AC, không cần quyền admin.
    foreach ($Probe in @(
        @{ Name = 'standby'; Query = 'SUB_SLEEP STANDBYIDLE' },
        @{ Name = 'hibernate'; Query = 'SUB_SLEEP HIBERNATEIDLE' },
        @{ Name = 'monitor'; Query = 'SUB_VIDEO VIDEOIDLE' }
    )) {
        $Parts = $Probe.Query.Split()
        Write-Host ("--- gia tri cu cua {0}: powercfg /q SCHEME_CURRENT {1} ---" -f $Probe.Name, $Probe.Query)
        & powercfg /q SCHEME_CURRENT $Parts[0] $Parts[1]
    }
    & powercfg /change standby-timeout-ac 0
    & powercfg /change hibernate-timeout-ac 0
    & powercfg /change monitor-timeout-ac 0
    Write-Host 'Da tat ngu, tat hibernate va tat man hinh khi cap AC.'
    Write-Host 'Muon khoi phuc: lay gia tri cu o 3 doan powercfg /q trong transcript, chay powercfg /change <setting>-timeout-ac <giay>.'
}

$StartTime = Get-Date
Start-Transcript -Path $TranscriptPath | Out-Null
Write-Host "Transcript: $TranscriptPath"
$FlagText = @(
    if ($Force) { '--force' }
    if ($DryRun) { '--dry-run' }
    if ($KeepAwake) { '-KeepAwake' }
) -join ' '
Write-Host "Command: `"$Python`" `"$Runner`" --from $From --to $To $FlagText"
Write-DiskReport 'before'
if ($KeepAwake) { Set-KeepAwake }

$RunnerExitCode = 1
Push-Location $ProjectRoot
try {
    if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
        throw "Python interpreter not found: $Python"
    }
    if (-not (Test-Path -LiteralPath $env:JAVA_HOME -PathType Container)) {
        throw "JAVA_HOME not found: $env:JAVA_HOME (Spark cần JDK 17, không dùng Java 25)"
    }

    # Một lần gọi runner duy nhất: runner tự bỏ qua tháng đã xong và tự ghi tháng lỗi vào run_log.csv.
    # Python logging ghi ra stderr: PS 5.1 biến stderr thành ErrorRecord và có thể ném
    # NativeCommandError khi ErrorActionPreference=Stop, nên hạ xuống Continue cho lệnh native.
    $PreferenceBefore = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $RunnerArgs = @($Runner, '--from', $From, '--to', $To)
    if ($Force) { $RunnerArgs += '--force' }
    if ($DryRun) { $RunnerArgs += '--dry-run' }
    # 2>&1 ép PowerShell đi qua từng dòng (transcript mới ghi đủ log của Python);
    # dòng stderr về dạng ErrorRecord nên phải lấy Exception.Message, không được ToString().
    & $Python @RunnerArgs 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            Write-Host $_.Exception.Message
        }
        else {
            Write-Host $_
        }
    }
    $RunnerExitCode = $LASTEXITCODE
    $ErrorActionPreference = $PreferenceBefore
}
catch {
    Write-Host ("Wrapper error: {0}" -f $_.Exception.Message)
    $RunnerExitCode = 1
}
finally {
    $EndTime = Get-Date
    Write-DiskReport 'after'
    Write-Host ("Duration: {0:hh\:mm\:ss}" -f ($EndTime - $StartTime))
    Write-Host ("Exit code: {0}  (1 = có tháng status=failed, xem reports\run_log.csv)" -f $RunnerExitCode)
    Pop-Location
    Stop-Transcript | Out-Null
}

exit $RunnerExitCode
