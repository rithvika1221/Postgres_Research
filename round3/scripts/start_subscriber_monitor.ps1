# =============================================================================
# Round 3 - start the subscriber monitor.
#
# Run this ONCE on the SUBSCRIBER VM before starting the campaign, then leave
# it. It follows the campaign state written by the publisher and rotates its
# own output file whenever the run changes, so nobody has to touch this machine
# again for the rest of the campaign.
#
#   .\start_subscriber_monitor.ps1
#   .\start_subscriber_monitor.ps1 -PublisherHost R3PUB
#
# It reconnects by itself if PostgreSQL restarts, and it never exits on an
# error. Stop it with:  New-Item C:\r3\STOP -ItemType File
# =============================================================================

param(
    [string]$PublisherHost = "",
    [string]$Root = "C:\r3",
    [switch]$AsTask
)

$ErrorActionPreference = "Stop"
$Scripts = $PSScriptRoot
$Data    = if ($env:R3_DATA) { $env:R3_DATA } else { Join-Path $Root "data" }

if (-not $env:R3_SUB_DSN) {
    Write-Host "R3_SUB_DSN is not set." -ForegroundColor Red
    Write-Host '  $env:R3_SUB_DSN = "host=localhost port=5432 dbname=sub user=postgres password=..."'
    exit 2
}

New-Item -ItemType Directory -Force -Path $Data | Out-Null
Remove-Item (Join-Path $Root "STOP") -ErrorAction SilentlyContinue

# The state file lives on the publisher. Reading it over SMB keeps this VM's
# output files aligned with the campaign. If the share is unreachable the
# monitor keeps recording anyway and the samples are joined by timestamp later.
$StateFile = if ($PublisherHost) {
    "\\$PublisherHost\r3\campaign_state.json"
} else {
    Join-Path $Root "campaign_state.json"
}
$PhaseFile = if ($PublisherHost) {
    "\\$PublisherHost\r3\phase_state.json"
} else {
    Join-Path $Root "phase_state.json"
}

if (-not (Test-Path $StateFile)) {
    Write-Host "State file not visible at $StateFile" -ForegroundColor Yellow
    Write-Host "The monitor will still record; files will be named 'pending' until it appears."
    Write-Host "To share it, run this ON THE PUBLISHER:" -ForegroundColor Yellow
    Write-Host '  New-SmbShare -Name r3 -Path C:\r3 -ReadAccess "Everyone"'
}

$argv = @(
    (Join-Path $Scripts "monitor.py"),
    "--role", "subscriber",
    "--out", $Data,
    "--follow",
    "--state-file", $StateFile,
    "--phase-file", $PhaseFile,
    "--stop-file", (Join-Path $Root "STOP"),
    "--data-volume", "F:\"
)

if ($AsTask) {
    # Survives sign-out. Use this if you will not keep an RDP session open.
    $act = New-ScheduledTaskAction -Execute "py" -Argument ($argv -join " ") -WorkingDirectory $Scripts
    $pri = New-ScheduledTaskPrincipal -UserId "SYSTEM" -RunLevel Highest
    $set = New-ScheduledTaskSettingsSet -ExecutionTimeLimit (New-TimeSpan -Hours 30) `
             -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName "R3SubscriberMonitor" -Action $act `
        -Principal $pri -Settings $set -Force | Out-Null
    Start-ScheduledTask -TaskName "R3SubscriberMonitor"
    Write-Host "Subscriber monitor registered and started as a scheduled task." -ForegroundColor Green
    Write-Host "Stop it with:  New-Item $Root\STOP -ItemType File"
    exit 0
}

Write-Host "Subscriber monitor starting. Leave this window open." -ForegroundColor Green
Write-Host "Stop it with:  New-Item $Root\STOP -ItemType File`n"
& py @argv
