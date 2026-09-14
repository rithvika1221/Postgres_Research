# =============================================================================
# Round 3 - ONE command to make a region subscriber ready. Run on the SUBSCRIBER.
#
#   cd C:\r3\scripts
#   .\start_region_helpers.ps1 -Region eastus
#
# It does, in this order:
#   1. checks both DSNs are set on this machine
#   2. clears the STOP file
#   3. starts state_relay.py in its own window   <- FIRST, deliberately
#   4. waits until campaign_state.json actually appears on this machine
#   5. starts the subscriber monitor in its own window
#   6. proves both are connected, by asking the databases rather than the UI
#   7. runs preflight_region.py and prints the numbers to copy back
#
# Order matters. The relay is what writes campaign_state.json here; until that
# file exists the monitor labels every sample 'pending', and a 'pending' series
# cannot be matched to a run. Starting the monitor first was how family F lost
# its apply-side data on the test rig.
#
# Re-running is safe. It will not start a second copy of either helper.
# =============================================================================

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$Region,
    [string]$Root = "C:\r3",
    [switch]$SkipPreflight,
    [switch]$SkipThroughput
)

$ErrorActionPreference = "Stop"
$Scripts = $PSScriptRoot
$Data    = if ($env:R3_DATA) { $env:R3_DATA } else { Join-Path $Root "data" }

function Say($m, $c = "Gray")  { Write-Host $m -ForegroundColor $c }
function Good($m) { Write-Host "  OK    $m" -ForegroundColor Green }
function Bad($m)  { Write-Host "  FAIL  $m" -ForegroundColor Red }
function Warn($m) { Write-Host "  WARN  $m" -ForegroundColor Yellow }

Say ""
Say "Round 3 - preparing subscriber '$Region'" Cyan
Say ("=" * 70)

# ---- 1. environment ---------------------------------------------------------
Say ""
Say "[1] Environment on THIS machine" White
$fail = 0
foreach ($v in @("R3_SUB_DSN", "R3_PUB_DSN")) {
    $val = [Environment]::GetEnvironmentVariable($v)
    if (-not $val) { $val = (Get-Item "env:$v" -ErrorAction SilentlyContinue).Value }
    if (-not $val) {
        Bad "$v is not set"
        if ($v -eq "R3_PUB_DSN") {
            Say '        state_relay.py needs it and nothing else here does.' DarkGray
            Say '        setx R3_PUB_DSN "host=10.0.1.4 port=5432 dbname=pub user=postgres password=YOURS" /M' DarkGray
        } else {
            Say '        setx R3_SUB_DSN "host=localhost port=5432 dbname=sub user=postgres password=YOURS" /M' DarkGray
        }
        Say '        then open a NEW PowerShell window - setx does not affect this one.' DarkGray
        $fail++
    } else {
        Good ("{0,-12} {1}" -f $v, ($val -replace 'password=[^ ]*','password=***'))
    }
}
if ($fail) { Say ""; Say "Set those, open a new window, and run this again." Red; exit 2 }

if (-not (Test-Path (Join-Path $Scripts "state_relay.py"))) {
    Bad "state_relay.py is not in $Scripts - copy the scripts folder across first"
    exit 2
}
New-Item -ItemType Directory -Force -Path $Data | Out-Null

# ---- 2. clear STOP ----------------------------------------------------------
Say ""
Say "[2] STOP file" White
$stop = Join-Path $Root "STOP"
if (Test-Path $stop) {
    Remove-Item $stop -Force
    Good "removed $stop (both helpers exit the moment it exists)"
} else {
    Good "absent"
}

# ---- helper: is an application_name connected, FROM THIS MACHINE? -----------
#
# The "fromHere" filter is not optional for the relay.
#
# Every region's relay connects to the SAME publisher with the same
# application_name. Counting them without filtering meant that on the second
# machine this script found the FIRST machine's relay, said "already
# connected - not starting a second one", and started nothing. The monitor
# then had no campaign_state.json to read and would have labelled every
# sample 'pending' for the whole region - the exact failure this script was
# written to prevent, caused by the script itself.
#
# inet_client_addr() is the address the publisher sees for THIS probe's own
# connection, so comparing client_addr against it matches only relays running
# on this machine. No hard-coded addresses, and it works whether the DSN says
# localhost or a private IP.
function Connected($dsnVar, $prefix, $fromHere = $false) {
    $extra = ""
    if ($fromHere) {
        $extra = " AND client_addr IS NOT DISTINCT FROM inet_client_addr()"
    }
    $py = @"
import os, sys
try:
    import psycopg2
    c = psycopg2.connect(os.environ['$dsnVar'], connect_timeout=10)
    k = c.cursor()
    k.execute("SELECT count(*) FROM pg_stat_activity "
              "WHERE application_name LIKE '$prefix%'$extra")
    print(k.fetchone()[0])
except Exception as exc:
    print('ERR ' + str(exc).splitlines()[0])
"@
    $out = ($py | & py - 2>&1) -join " "
    if ($out -match '^\s*(\d+)\s*$') { return [int]$Matches[1] }
    return -1
}

# ---- 3. the relay, first ----------------------------------------------------
Say ""
Say "[3] state_relay.py  - started FIRST, it writes campaign_state.json here" White
$already = Connected "R3_PUB_DSN" "r3_state_relay" $true
if ($already -gt 0) {
    Good "already connected to the publisher - not starting a second one"
} else {
    Say "      testing permissions with a single pull ..." DarkGray
    $once = & py (Join-Path $Scripts "state_relay.py") --once --root $Root 2>&1
    if ($LASTEXITCODE -ne 0) {
        Bad "state_relay.py --once failed:"
        $once | ForEach-Object { Say "        $_" DarkGray }
        Say ""
        Say "  The usual cause is R3_PUB_DSN pointing at localhost instead of the" Yellow
        Say "  publisher's private IP, or the publisher's pg_hba.conf not covering" Yellow
        Say "  this subnet. Nothing was started." Yellow
        exit 2
    }
    Good "single pull succeeded"
    Start-Process powershell -ArgumentList @(
        "-NoExit", "-Command",
        "cd '$Scripts'; Write-Host 'STATE RELAY - leave this window open' -ForegroundColor Cyan; py state_relay.py --root '$Root'"
    ) | Out-Null
    Start-Sleep -Seconds 6
    if ((Connected "R3_PUB_DSN" "r3_state_relay" $true) -gt 0) {
        Good "running, connected to the publisher"
    } else {
        Warn "started, but not visible in the publisher's pg_stat_activity yet"
    }
}

# ---- 4. wait for the state file --------------------------------------------
Say ""
Say "[4] campaign_state.json on this machine" White
$state = Join-Path $Root "campaign_state.json"
$deadline = (Get-Date).AddSeconds(45)
while (-not (Test-Path $state) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 2 }
if (Test-Path $state) {
    $age = [int]((Get-Date) - (Get-Item $state).LastWriteTime).TotalSeconds
    Good "present, written ${age}s ago"
} else {
    Warn "not there after 45 s. The monitor will still record, but it will label"
    Say  "        samples 'pending' until the relay writes it. Check the relay window." DarkGray
}

# ---- 5. the monitor ---------------------------------------------------------
Say ""
Say "[5] subscriber monitor" White
$already = Connected "R3_SUB_DSN" "r3_monitor_subscriber"
if ($already -gt 0) {
    Good "already connected - not starting a second one"
} else {
    Start-Process powershell -ArgumentList @(
        "-NoExit", "-Command",
        "cd '$Scripts'; Write-Host 'SUBSCRIBER MONITOR - leave this window open' -ForegroundColor Cyan; .\start_subscriber_monitor.ps1 -Root '$Root'"
    ) | Out-Null
    Start-Sleep -Seconds 8
    $n = Connected "R3_SUB_DSN" "r3_monitor_subscriber"
    if ($n -gt 0) { Good "running, connected to the local subscriber" }
    elseif ($n -eq 0) { Bad "started but is not connected - read the monitor window"; exit 2 }
    else { Bad "could not check - is psycopg2 installed here?  py -m pip install psycopg2-binary"; exit 2 }
}

# ---- 6. both, confirmed -----------------------------------------------------
Say ""
Say "[6] Both helpers, confirmed from the databases" White
# Count, then RE-count if it looks wrong.
#
# A helper that was stopped a moment ago leaves its connection in
# pg_stat_activity for a few seconds while the backend winds down. Counting
# once, eight seconds after starting a new monitor, reported three where there
# was one - and because that was a hard failure, the script exited before the
# preflight and the machine looked broken when it was ready. Settle first;
# only a count that stays wrong is wrong.
function SettledCount($dsnVar, $prefix, $fromHere, $want) {
    for ($i = 0; $i -lt 5; $i++) {
        $n = Connected $dsnVar $prefix $fromHere
        if ($n -eq $want) { return $n }
        if ($i -lt 4) {
            Say ("      ${prefix}: saw $n, expected $want - re-checking in " +
                 "5 s (connections from stopped helpers take a moment to " +
                 "clear)") DarkGray
            Start-Sleep -Seconds 5
        }
    }
    return $n
}
$r = SettledCount "R3_PUB_DSN" "r3_state_relay" $true 1
$m = SettledCount "R3_SUB_DSN" "r3_monitor_subscriber" $false 1
# EXACTLY one of each. More than one is not "extra safe" - monitor.py opens a
# single connection and appends to its CSV, so two monitors interleave two
# sample streams in one file and validate_run.py cannot tell them apart. An
# earlier version of this script printed the count and treated anything above
# zero as fine, which is how three monitors on one machine went unremarked.
if ($r -eq 1) { Good "state relay        1 connection from THIS machine" }
elseif ($r -le 0) { Bad "state relay is NOT connected from this machine" }
else { Bad "state relay: $r connections from this machine - expected exactly 1" }
if ($m -eq 1) { Good "subscriber monitor 1 connection to this server" }
elseif ($m -le 0) { Bad "subscriber monitor is NOT connected" }
else { Bad "subscriber monitor: $m connections - expected exactly 1" }
if ($r -ne 1 -or $m -ne 1) {
    Say ""
    if ($r -gt 1 -or $m -gt 1) {
        Say "  More than one helper of a kind is running on this machine." Red
        Say "  Two monitors append to the SAME CSV and the samples interleave." Red
        Say "  Stop everything here, wait for the windows to close, then run" White
        Say "  this script again - it will start exactly one of each:" White
        Say ""
        Say "      New-Item $stop -ItemType File" Cyan
        Say "      (wait for every helper window to exit)" DarkGray
        Say "      .\start_region_helpers.ps1 -Region $Region" Cyan
    } else {
        Say "  run_region.py phase [8] will refuse to start without both." Red
    }
    exit 2
}

# ---- 7. preflight -----------------------------------------------------------
if ($SkipPreflight) {
    Say ""
    Say "Preflight skipped by request." Yellow
    exit 0
}
Say ""
Say "[7] preflight_region.py  - settings, subscription, RTT, apply throughput" White
Say ("-" * 70) DarkGray
$pfArgs = @((Join-Path $Scripts "preflight_region.py"), "--region", $Region)
if ($SkipThroughput) { $pfArgs += "--skip-throughput" }
& py @pfArgs
$pf = $LASTEXITCODE
Say ("-" * 70) DarkGray

Say ""
if ($pf -eq 0) {
    Say "THIS MACHINE IS READY." Green
    Say ""
    Say "  Leave both helper windows open. Do not sign out of RDP -" White
    Say "  closing the window is fine, signing out kills them." White
    Say ""
    Say "  Copy the measured RTT and apply MB/s printed above into" White
    Say "  region_map.json on the PUBLISHER, under '$Region':" White
    Say '      "expected_rtt_ms": <the median RTT>,' DarkGray
    Say '      "apply_mbps": <the sustained throughput>' DarkGray
    Say ""
    Say "  Then on the PUBLISHER:" White
    Say "      py run_region.py --region $Region --plan" Cyan
    Say "      py run_region.py --region $Region" Cyan
} else {
    Say "PREFLIGHT DID NOT PASS - exit $pf." Red
    Say "  Fix what it named above before starting this region." Red
    Say "  The helpers are running and can stay running." DarkGray
}
Say ""
exit $pf
