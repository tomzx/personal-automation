#Requires -Version 5.1
<#
.SYNOPSIS
    Mounts two VeraCrypt volumes and configures the "Backup" SMB share on R:.

.DESCRIPTION
    Reads volume paths and DPAPI-encrypted passwords saved by setup-veracrypt-credentials.ps1,
    mounts both volumes via the VeraCrypt CLI, then creates (or recreates) the SMB share
    named "Backup" on R:\ with full access for the "backup" user.

    Designed to run at Windows logon via Task Scheduler.
    Register it with: register-startup-task.ps1
#>

# ---------------------------------------------------------------------------
# Configuration — adjust these if needed
# ---------------------------------------------------------------------------
$VeraCryptExe  = "C:\Program Files\VeraCrypt\VeraCrypt.exe"
$ShareDrive    = "R"          # Drive letter that gets shared (no colon)
$ShareName     = "Backup"
$ShareUser     = "backup"     # Windows user granted full access to the share
$CredDir       = "$PSScriptRoot\veracrypt-credentials"
# ---------------------------------------------------------------------------

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $Message"
}

# --- Validate prerequisites ------------------------------------------------

if (-not (Test-Path $VeraCryptExe)) {
    Write-Log "ERROR: VeraCrypt not found at '$VeraCryptExe'. Update `$VeraCryptExe in this script."
    exit 1
}

foreach ($n in 1, 2) {
    if (-not (Test-Path "$CredDir\volume${n}.json") -or -not (Test-Path "$CredDir\volume${n}.pass")) {
        Write-Log "ERROR: Credentials for volume $n not found. Run setup-veracrypt-credentials.ps1 first."
        exit 1
    }
}

# --- Mount volumes ----------------------------------------------------------

function Mount-VeraCryptVolume {
    param(
        [int]$VolumeNumber,
        [string]$CredDir,
        [string]$VeraCryptExe
    )

    $config      = Get-Content "$CredDir\volume${VolumeNumber}.json" -Raw | ConvertFrom-Json
    $volumePath  = $config.VolumePath
    $driveLetter = $config.DriveLetter

    $encPass  = Get-Content "$CredDir\volume${VolumeNumber}.pass" -Raw
    $secPass  = ConvertTo-SecureString $encPass
    $bstr     = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secPass)
    $password = [Runtime.InteropServices.Marshal]::PtrToStringAuto($bstr)
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)

    # Skip if already mounted
    if (Test-Path "${driveLetter}:\") {
        Write-Log "Volume $VolumeNumber: drive ${driveLetter}: already present, skipping mount."
        return
    }

    Write-Log "Mounting volume $VolumeNumber ($volumePath) as ${driveLetter}:..."
    $proc = Start-Process -FilePath $VeraCryptExe `
        -ArgumentList "/volume `"$volumePath`" /letter $driveLetter /password `"$password`" /quit /silent /mountoption ro" `
        -Wait -PassThru -NoNewWindow
    $password = $null  # clear from memory ASAP

    if ($proc.ExitCode -ne 0) {
        Write-Log "ERROR: VeraCrypt exited with code $($proc.ExitCode) for volume $VolumeNumber."
        exit 1
    }
    Write-Log "Volume $VolumeNumber mounted as ${driveLetter}:."
}

Mount-VeraCryptVolume -VolumeNumber 1 -CredDir $CredDir -VeraCryptExe $VeraCryptExe
Mount-VeraCryptVolume -VolumeNumber 2 -CredDir $CredDir -VeraCryptExe $VeraCryptExe

# --- Configure SMB share on R: ---------------------------------------------

$sharePath = "${ShareDrive}:\"

if (-not (Test-Path $sharePath)) {
    Write-Log "ERROR: Drive ${ShareDrive}: not found. Ensure one of the VeraCrypt volumes is mounted there."
    exit 1
}

Write-Log "Configuring SMB share '$ShareName' on ${sharePath}..."

$existing = Get-SmbShare -Name $ShareName -ErrorAction SilentlyContinue
if ($existing) {
    Remove-SmbShare -Name $ShareName -Force
    Write-Log "Removed existing share '$ShareName'."
}

New-SmbShare -Name $ShareName -Path $sharePath -FullAccess $ShareUser | Out-Null
Write-Log "Share '$ShareName' created: \\$env:COMPUTERNAME\$ShareName -> $sharePath (full access: $ShareUser)"

Write-Log "Done."
