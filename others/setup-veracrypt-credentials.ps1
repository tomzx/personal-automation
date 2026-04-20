#Requires -Version 5.1
<#
.SYNOPSIS
    One-time setup: saves VeraCrypt volume paths and encrypted passwords for use by mount-veracrypt-share.ps1.

.DESCRIPTION
    Passwords are encrypted using Windows DPAPI (tied to your user account and machine).
    The encrypted files can only be decrypted by the same Windows user on the same machine.

    Run this script once as the user who will be auto-mounting the volumes.
#>

$credDir = "$PSScriptRoot\veracrypt-credentials"
if (-not (Test-Path $credDir)) {
    New-Item -ItemType Directory -Path $credDir | Out-Null
}

function Save-VolumeConfig {
    param(
        [int]$VolumeNumber,
        [string]$CredDir
    )

    Write-Host "`n--- Volume $VolumeNumber ---"
    $volumePath = Read-Host "Full path to VeraCrypt volume file (e.g. D:\MyVolume.vc)"
    $driveLetter = Read-Host "Drive letter to mount as (single letter, e.g. S)"
    $driveLetter = $driveLetter.Trim().TrimEnd(':').ToUpper()

    $password = Read-Host "Password for volume $VolumeNumber" -AsSecureString
    $encrypted = ConvertFrom-SecureString $password

    $config = @{
        VolumePath  = $volumePath
        DriveLetter = $driveLetter
    }
    $config | ConvertTo-Json | Set-Content "$CredDir\volume${VolumeNumber}.json" -Encoding UTF8
    $encrypted | Set-Content "$CredDir\volume${VolumeNumber}.pass" -Encoding UTF8

    Write-Host "Volume $VolumeNumber config saved."
}

Save-VolumeConfig -VolumeNumber 1 -CredDir $credDir
Save-VolumeConfig -VolumeNumber 2 -CredDir $credDir

Write-Host "`nCredentials saved to: $credDir"
Write-Host "You can now run mount-veracrypt-share.ps1 (or let the startup task do it)."
