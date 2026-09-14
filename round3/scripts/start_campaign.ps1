# =============================================================================
# Round 3 - start the unattended campaign.
#
# Run this on the PUBLISHER VM, in an elevated PowerShell window, inside a
# session that will survive you disconnecting (see notes at the bottom).
#
#   .\start_campaign.ps1 -Stage calibration   # ~45 min, run first
#   .\start_campaign.ps1 -Stage probe         # ~15 min, run second
#   .\start_campaign.ps1 -Stage full          # A-E in one go, unattended
#   .\start_campaign.ps1 -Stage bcde          # B-E only, after calibration
#   .\start_campaign.ps1 -Stage full -Resume  # continue after any interruption
#   .\start_campaign.ps1 -Stage latency       # attended, do this last
#
# There are NO prompts. Once started it runs to completion on its own.
# =============================================================================

param(
    [ValidateSet("calibration","probe","decode","full","bcde","latency","region")][string]$Stage = "full",
    [string]$Region = "",
    [switch]$Resume,
    [switch]$SkipVerify,
    [string]$Root = "C:\r3"
)

$ErrorActionPreference = "Stop"
$Scripts = $PSScriptRoot
$Data    = if ($env:R3_DATA) { $env:R3_DATA } else { Join-Path $Root "data" }

if (-not $env:R3_PUB_DSN) {
    Write-Host "R3_PUB_DSN is not set." -ForegroundColor Red
    Write-Host '  $env:R3_PUB_DSN = "host=localhost port=5432 dbname=pub user=postgres password=..."'
    exit 2
}
if (-not $env:R3_SUB_DSN) {
    Write-Host "R3_SUB_DSN is not set - health checks and auto-recovery will be limited." -ForegroundColor Yellow
}

New-Item -ItemType Directory -Force -Path $Data | Out-Null
Remove-Item (Join-Path $Root "STOP") -ErrorAction SilentlyContinue

# ---- 1. which campaign ------------------------------------------------------
# Chosen BEFORE verification, because verify_setup's section [8] checks depend
# on it: the length estimate, which matrices must carry provenance, and - the
# one that actually bites - which existing run artefacts this campaign would
# overwrite. Verifying Stage 3 against campaign.json condemns all twelve
# finished A-E runs as leftovers that must be moved out of the way.
$campaign = switch ($Stage) {
    "calibration" { "campaign_calibration.json" }
    "probe"       { "campaign_probe.json" }
    "decode"      { "campaign_decode.json" }
    "full"        { "campaign.json" }
    "bcde"        { "campaign_bcde.json" }
    "latency"     { "campaign_latency.json" }
    "region"      { "campaign_F_lat_$Region.json" }
}

# -Stage region runs family F against ONE remote subscriber. The latency is the
# network's own, so there is nothing to set between levels and nothing to
# calibrate - but the campaign file is per region and R3_SUB_DSN must point at
# that region's machine, so both are checked here rather than failing later.
if ($Stage -eq "region") {
    if (-not $Region) {
        Write-Host "-Stage region needs -Region, e.g. -Region eastus" -ForegroundColor Red
        Write-Host "Generate its files first:  py make_region_family.py --region eastus --rtt MEASURED --vm-size SIZE --apply-mbps MBPS"
        exit 2
    }
    if (-not (Test-Path (Join-Path $Scripts $campaign))) {
        Write-Host "No $campaign in $Scripts." -ForegroundColor Red
        Write-Host "Generate it:  py make_region_family.py --region $Region --rtt MEASURED --vm-size SIZE --apply-mbps MBPS"
        exit 2
    }
    Write-Host "`nRegion run: $Region" -ForegroundColor Cyan
    Write-Host "R3_SUB_DSN must point at the $Region subscriber. It currently reads:"
    Write-Host ("  " + ($env:R3_SUB_DSN -replace 'password=[^ ]*','password=***'))
    Write-Host "Every OTHER subscription must be disabled, or the publisher is feeding"
    Write-Host "more than one standby and the offered load is not what it says."
}

# ---- 2. verify -------------------------------------------------------------
if (-not $SkipVerify) {
    Write-Host "`n--- verifying setup for $campaign ---" -ForegroundColor Cyan
    & py (Join-Path $Scripts "verify_setup.py") --smoke --campaign $campaign
    if ($LASTEXITCODE -eq 2) {
        Write-Host "`nSetup is not ready. Nothing was started." -ForegroundColor Red
        exit 2
    }
}

# ---- 3. environment record -------------------------------------------------
Write-Host "`n--- capturing environment ---" -ForegroundColor Cyan
& py (Join-Path $Scripts "capture_environment.py") --role publisher --tag "campaign-$Stage"
if ($env:R3_SUB_DSN) {
    & py (Join-Path $Scripts "capture_environment.py") --role subscriber --tag "campaign-$Stage"
}

# ---- 4. campaign -----------------------------------------------------------
$argv = @(
    (Join-Path $Scripts "supervisor.py"),
    "--campaign", (Join-Path $Scripts $campaign),
    "--out", $Data
)
if ($Resume) { $argv += "--resume" }

Write-Host "`n--- starting campaign: $campaign ---" -ForegroundColor Green
Write-Host "Progress:   Get-Content $Data\supervisor.log -Wait -Tail 30"
Write-Host "Status:     Get-Content $Root\campaign_state.json | ConvertFrom-Json"
Write-Host "Stop it:    New-Item $Root\STOP -ItemType File"
Write-Host ""

& py @argv
$rc = $LASTEXITCODE

Write-Host "`n--- campaign exited with code $rc ---" -ForegroundColor $(if ($rc -eq 0) {"Green"} else {"Red"})
Write-Host "Report: $Data\campaign_report.md"
if ($rc -ne 0) {
    Write-Host "Resume with:  .\start_campaign.ps1 -Stage $Stage -Resume -SkipVerify" -ForegroundColor Yellow
}
exit $rc

# =============================================================================
# SURVIVING A DISCONNECT
#
# Closing an RDP window with the X button leaves the session running; signing
# out does not. For certainty, register the campaign as a scheduled task under
# SYSTEM. The exact commands are in the Setup Guide, step 17 - they are kept
# out of this file because PowerShell treats a line-ending backtick as a
# continuation even inside a comment, which breaks parsing of the whole script.
#
# A SYSTEM task inherits nothing from your session, so the R3_* variables must
# be set machine-wide first (Setup Guide step 8).
#
# Also disable sleep and screen lock on both VMs:
#   powercfg /change standby-timeout-ac 0
#   powercfg /change monitor-timeout-ac 0
# =============================================================================
