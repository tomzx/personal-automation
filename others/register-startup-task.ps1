#Requires -Version 5.1
#Requires -RunAsAdministrator
<#
.SYNOPSIS
    Registers mount-veracrypt-share.ps1 as a Windows Task Scheduler task that runs at logon.

.DESCRIPTION
    Creates a scheduled task named "MountVeraCryptShare" that runs mount-veracrypt-share.ps1
    as the current user (with highest privileges) whenever that user logs on.

    Run this script once from an elevated (Administrator) PowerShell prompt.
#>

$TaskName   = "MountVeraCryptShare"
$ScriptPath = Join-Path $PSScriptRoot "mount-veracrypt-share.ps1"

if (-not (Test-Path $ScriptPath)) {
    Write-Error "Script not found: $ScriptPath"
    exit 1
}

$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name

$action  = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$ScriptPath`""

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable

$principal = New-ScheduledTaskPrincipal `
    -UserId $currentUser `
    -LogonType Interactive `
    -RunLevel Highest   # needed for New-SmbShare (admin privilege)

$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed existing task '$TaskName'."
}

Register-ScheduledTask `
    -TaskName  $TaskName `
    -Action    $action `
    -Trigger   $trigger `
    -Settings  $settings `
    -Principal $principal | Out-Null

Write-Host "Task '$TaskName' registered. It will run mount-veracrypt-share.ps1 each time '$currentUser' logs on."
Write-Host "To test immediately: Start-ScheduledTask -TaskName '$TaskName'"
