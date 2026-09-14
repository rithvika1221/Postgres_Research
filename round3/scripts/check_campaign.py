#!/usr/bin/env python3
"""
Round 3 campaign health check.  STANDALONE.

Run this any time while a campaign is going - or after it has stopped - and it
tells you in one word whether things are fine, and in one page why.

It is READ-ONLY. It starts nothing, stops nothing and writes nothing except its
own report file, so it is always safe to run, including while a level is under
load.

  py check_campaign.py

Every run also writes  campaign_status_<host>_<timestamp>.txt  next to the data,
which is the file to send if you want someone else to look at it.

Exit codes
----------
  0  HEALTHY   - running normally, or finished cleanly
  1  ATTENTION - running, but something is worth a look
  2  STOPPED   - not running, and it did not finish on purpose
"""

import argparse
import csv
import glob
import json
import os
import socket
import sys
from datetime import datetime, timezone

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import psutil
except ImportError:
    psutil = None

DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")
DEFAULT_STATE = os.environ.get("R3_STATE_FILE", r"C:\r3\campaign_state.json")
DEFAULT_PHASE = os.environ.get("R3_PHASE_FILE", r"C:\r3\phase_state.json")
DEFAULT_STOP = os.environ.get("R3_STOP_FILE", r"C:\r3\STOP")

# The supervisor refreshes the heartbeat every 15 s while a run is in progress.
# Anything past a couple of minutes means it is not looping any more.
HEARTBEAT_STALE_SEC = 180
HEARTBEAT_DEAD_SEC = 900

# A phase should not sit still for longer than this. A load phase is capped by
# the level duration; a drain can legitimately run long on an above-knee level,
# so this is generous and only ever a WARNING.
PHASE_SLOW_SEC = 3600

lines = []
problems = []
warnings = []


def say(s=""):
    print(s)
    lines.append(s)


def flag(msg, fatal=False):
    (problems if fatal else warnings).append(msg)


def read_json(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def age_sec(ts):
    t = parse_ts(ts)
    if t is None:
        return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - t).total_seconds()


def human(sec):
    if sec is None:
        return "unknown"
    sec = int(sec)
    if sec < 90:
        return f"{sec}s ago"
    if sec < 5400:
        return f"{sec//60} min ago"
    return f"{sec/3600:.1f} h ago"


def proc_running(needle):
    """Is a python process running <needle>? None if we cannot tell."""
    if psutil is None:
        return None
    try:
        for p in psutil.process_iter(["cmdline"]):
            cl = p.info.get("cmdline") or []
            if any(needle in str(c) for c in cl):
                return True
        return False
    except Exception:
        return None


def q(dsn, sql):
    if psycopg2 is None or not dsn:
        return None
    try:
        c = psycopg2.connect(dsn, connect_timeout=8)
        c.set_session(autocommit=True)
        k = c.cursor()
        k.execute(sql)
        r = k.fetchone()
        k.close()
        c.close()
        return r
    except Exception:
        return None


# --------------------------------------------------------------------------

def section_campaign(st, camp):
    say("CAMPAIGN")
    say("-" * 70)
    if not st:
        say("  no campaign_state.json - nothing has been started, or the path is wrong")
        flag("no campaign state file found", fatal=True)
        return None

    status = st.get("status", "?")
    hb = age_sec(st.get("heartbeat_utc"))
    done = st.get("completed") or []
    failed = st.get("failed") or []
    skipped = st.get("skipped") or []
    planned = None
    if camp:
        try:
            planned = sum(len(f["repeats"]) for f in camp["families"])
        except Exception:
            planned = None

    say(f"  name            {st.get('campaign')}   on {st.get('host')}")
    say(f"  status          {status}")
    say(f"  current run     {st.get('current_run_id')}   "
        f"(family {st.get('current_family')})")
    say(f"  heartbeat       {human(hb)}")
    say(f"  running for     {st.get('elapsed_hours', 0):.2f} h")
    prog = f"{len(done)} completed, {len(failed)} failed, {len(skipped)} skipped"
    if planned:
        prog += f"  (of {planned} planned)"
    say(f"  progress        {prog}")

    # --- time remaining, from what has actually been observed ---------------
    if planned and done:
        per = sum(r.get("elapsed_sec", 0) for r in done) / len(done)
        left = planned - len(done) - len(failed) - len(skipped)
        if left > 0 and per > 0:
            say(f"  estimated left  {left} run(s) x {per/60:.0f} min "
                f"= about {left*per/3600:.1f} h")

    # --- the verdict on liveness -------------------------------------------
    if status == "running":
        if hb is None:
            flag("the heartbeat cannot be read", fatal=True)
        elif hb > HEARTBEAT_DEAD_SEC:
            flag(f"status says running but the heartbeat is {human(hb)} - "
                 f"the supervisor is not looping any more", fatal=True)
        elif hb > HEARTBEAT_STALE_SEC:
            flag(f"heartbeat is {human(hb)}; it should refresh every 15 s "
                 f"while a run is in progress")
    elif status == "finished":
        say("  -> the campaign finished on its own")
    elif status == "halted":
        flag(f"the campaign HALTED: {st.get('halt_reason')}", fatal=True)
    else:
        flag(f"unexpected status '{status}'")

    if failed:
        for r in failed:
            flag(f"run {r.get('run_id')} failed after {r.get('attempt')} "
                 f"attempt(s), orchestrator rc={r.get('orchestrator_rc')}")
    if skipped:
        for r in skipped:
            flag(f"run {r.get('run_id')} was skipped: {r.get('reason')}")
    for r in done:
        if r.get("validator_rc") not in (0, None):
            flag(f"run {r.get('run_id')} completed but its validator reported "
                 f"problems - read validation_{r.get('artefact_run_id', r.get('run_id'))}.txt")
    say()
    return st


def section_processes(st):
    say("PROCESSES")
    say("-" * 70)
    if psutil is None:
        say("  psutil not installed - cannot check (py -m pip install psutil)")
        say()
        return
    running = (st or {}).get("status") == "running"
    for name, needle in (("supervisor", "supervisor.py"),
                         ("publisher monitor", "monitor.py"),
                         ("load generator", "loadgen.py")):
        alive = proc_running(needle)
        mark = "yes" if alive else "no"
        note = ""
        if needle == "loadgen.py" and not alive:
            note = "  (normal unless a level is in its load phase)"
        say(f"  {name:<20} {mark}{note}")
        if running and not alive and needle == "supervisor.py":
            flag("the supervisor process is gone but the state still says "
                 "running - the campaign died", fatal=True)
        if running and not alive and needle == "monitor.py":
            flag("the publisher monitor is not running - samples are being lost")
    say()


def section_phase(ph):
    say("CURRENT PHASE")
    say("-" * 70)
    if not ph:
        say("  no phase file - normal if nothing is running right now")
        say()
        return
    a = age_sec(ph.get("updated"))
    say(f"  phase           {ph.get('phase_state')}   level {ph.get('level_id')}")
    say(f"  last updated    {human(a)}")
    if ph.get("clients"):
        say(f"  workload        {ph.get('clients')} clients, "
            f"{ph.get('rows_per_commit')} rows/commit, "
            f"{ph.get('row_bytes')} B rows, target {ph.get('target_wal_mbps')} MB/s")
    if ph.get("phase_state") == "awaiting_latency":
        # Not a stall. Family F is attended and this phase is the orchestrator
        # waiting for a person, so it must not be reported as trouble.
        want = ph.get("network_latency_ms")
        say(f"  WAITING FOR YOU  set clumsy to {want} ms on the subscriber"
            if want else "  WAITING FOR YOU  stop clumsy (control level)")
        say(f'                   then:  "{want}" | Set-Content C:\\r3\\LATENCY_SET')
        say()
        return
    if a is not None and a > PHASE_SLOW_SEC and ph.get("phase_state") != "idle":
        flag(f"the '{ph.get('phase_state')}' phase has not moved in {human(a)}")
    say()


def section_db(pub_dsn, sub_dsn):
    say("REPLICATION (live)")
    say("-" * 70)
    if psycopg2 is None:
        say("  psycopg2 not installed - cannot check")
        say()
        return
    if not pub_dsn:
        say("  R3_PUB_DSN is not set - cannot check")
        flag("R3_PUB_DSN not set, so replication could not be checked")
        say()
        return

    r = q(pub_dsn, "SELECT current_setting('server_version')")
    if not r:
        say("  publisher        NOT REACHABLE")
        flag("the publisher database is not answering", fatal=True)
        say()
        return
    say(f"  publisher        PostgreSQL {r[0]}")

    r = q(pub_dsn, "SELECT state, "
                   "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),0)::bigint "
                   "FROM pg_stat_replication WHERE application_name='mysub' LIMIT 1")
    if not r:
        say("  replication      NO CONNECTION")
        flag("no active replication connection - the subscriber is not attached",
             fatal=True)
    else:
        say(f"  replication      {r[0]}, backlog {r[1]/1e6:,.1f} MB")

    r = q(pub_dsn, "SELECT wal_status, active, "
                   "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn),0)::bigint "
                   "FROM pg_replication_slots WHERE slot_type='logical' LIMIT 1")
    if r:
        say(f"  slot             {r[0]}, active={r[1]}, retaining {r[2]/1e6:,.1f} MB of WAL")
        if r[0] in ("lost", "unreserved"):
            flag(f"the replication slot is {r[0]} - data has been lost, the "
                 f"affected runs must be repeated", fatal=True)
    else:
        flag("no logical replication slot found", fatal=True)

    if sub_dsn:
        r = q(sub_dsn, "SELECT count(*) FROM ingest_data")
        say(f"  subscriber       {'reachable, ' + format(r[0], ',') + ' rows' if r else 'NOT REACHABLE'}")
        if not r:
            flag("the subscriber database is not answering", fatal=True)
    say()


def section_samples(data_dir, st):
    say("DATA BEING COLLECTED")
    say("-" * 70)
    run = (st or {}).get("current_run_id")
    csvs = sorted(glob.glob(os.path.join(data_dir, "publisher_*.csv")),
                  key=lambda p: os.path.getmtime(p))
    if not csvs:
        say("  no publisher CSV yet")
        flag("the monitor has not written any samples", fatal=True)
        say()
        return
    newest = csvs[-1]
    age = (datetime.now().timestamp() - os.path.getmtime(newest))
    rows = []
    try:
        with open(newest, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    except Exception:
        pass
    say(f"  newest file      {os.path.basename(newest)}")
    say(f"  last written     {human(age)}")
    say(f"  samples in it    {len(rows):,}")

    if (st or {}).get("status") == "running" and age > 120:
        flag(f"the publisher CSV has not been written to in {human(age)} - "
             f"the monitor has stopped sampling", fatal=True)

    if rows:
        last = rows[-1]
        def f(k):
            try:
                return float(last.get(k) or 0)
            except Exception:
                return 0.0
        say(f"  latest sample    WAL {f('wal_mb_per_sec'):.1f} MB/s, "
            f"{f('commits_per_sec'):,.0f} commits/s, "
            f"backlog {f('replay_lag_bytes')/1e6:,.1f} MB")
        blank = sum(1 for r in rows if not (r.get("wal_bytes") or "").strip())
        if blank:
            flag(f"{blank} of {len(rows)} samples have no wal_bytes - the "
                 f"monitor is not reading the statistics views", fatal=True)
    say()


def section_events(data_dir):
    say("NOTABLE EVENTS (last 12)")
    say("-" * 70)
    BAD = {"REPLICATION_DOWN", "SLOT_INVALID", "QUERY_ERROR", "QUERY_UNSUPPORTED",
           "DISK_LOW", "ABORT", "COUNTER_RESET", "ROTATE_FAILED", "FATAL",
           "DERIVED_RATE_ERROR", "SAMPLE_EXCEPTION"}
    seen = []
    for p in glob.glob(os.path.join(data_dir, "*_events.log")):
        try:
            with open(p, encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) >= 3 and parts[1] not in ("RUN", "LEVEL", "START"):
                        seen.append((parts[0], parts[1], parts[2][:90],
                                     os.path.basename(p)))
        except Exception:
            pass
    seen.sort()
    if not seen:
        say("  nothing logged")
    for ts, kind, msg, src in seen[-12:]:
        say(f"  {ts[11:19]}  {kind:<18} {msg}")
    for ts, kind, msg, src in seen:
        if kind in BAD:
            fatal = kind in ("SLOT_INVALID", "FATAL")
            flag(f"{kind} in {src}: {msg[:70]}", fatal=fatal)
    say()


def section_disk(volume, min_free_gb):
    say("DISK")
    say("-" * 70)
    # Same thresholds the supervisor itself uses: it warns below min_free_gb
    # and halts hard below 40% of it.
    halt_at = min_free_gb * 0.4
    try:
        import shutil
        free = shutil.disk_usage(volume).free / 1024**3
        say(f"  {volume:<16} {free:,.0f} GB free   "
            f"(warns below {min_free_gb:.0f}, halts below {halt_at:.0f})")
        if free < halt_at:
            flag(f"only {free:.0f} GB free on {volume} - the campaign halts "
                 f"below {halt_at:.0f} GB", fatal=True)
        elif free < min_free_gb:
            flag(f"{free:.0f} GB free on {volume} - below the {min_free_gb:.0f} GB "
                 f"comfort line, watch it")
    except Exception:
        say(f"  {volume} could not be read")
    say()


def main():
    ap = argparse.ArgumentParser(description="Round 3 campaign health check (read-only).")
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--state-file", default=DEFAULT_STATE)
    ap.add_argument("--phase-file", default=DEFAULT_PHASE)
    ap.add_argument("--stop-file", default=DEFAULT_STOP)
    ap.add_argument("--campaign", default=None,
                    help="the campaign json, to know how many runs were planned")
    ap.add_argument("--pub-dsn", default=os.environ.get("R3_PUB_DSN"))
    ap.add_argument("--sub-dsn", default=os.environ.get("R3_SUB_DSN"))
    ap.add_argument("--data-volume", default="F:\\" if os.name == "nt" else "/")
    ap.add_argument("--min-free-gb", type=float, default=100.0,
                    help="the same figure the campaign was started with")
    ap.add_argument("--no-report", action="store_true",
                    help="print only, do not write the report file")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))

    st = read_json(args.state_file) or read_json(
        os.path.join(args.out, "campaign_state.json"))

    # Find the campaign file that actually produced this state, so the
    # progress count is right whichever stage is running. Matching on the
    # name inside the file beats assuming campaign.json - otherwise a
    # calibration or probe run is measured against the full campaign's
    # twelve runs and the "estimated left" is nonsense.
    camp_path = args.campaign
    if not camp_path:
        want = (st or {}).get("campaign")
        for cand in sorted(glob.glob(os.path.join(here, "campaign*.json"))):
            c = read_json(cand)
            if c and c.get("name") == want:
                camp_path = cand
                break
        camp_path = camp_path or os.path.join(here, "campaign.json")
    ph = read_json(args.phase_file)
    camp = read_json(camp_path)

    say(f"Round 3 campaign check — {socket.gethostname()} — "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    say("=" * 70)
    say()

    section_campaign(st, camp)
    section_processes(st)
    section_phase(ph)
    section_db(args.pub_dsn, args.sub_dsn)
    section_samples(args.out, st)
    section_events(args.out)
    section_disk(args.data_volume, args.min_free_gb)

    if os.path.exists(args.stop_file):
        say(f"NOTE: a stop file exists at {args.stop_file}. The campaign will "
            f"stop at the next level boundary.")
        say()

    # ---- verdict ----------------------------------------------------------
    status = (st or {}).get("status")
    say("=" * 70)
    if problems:
        verdict = "STOPPED" if status not in ("running",) else "ATTENTION"
        if any("halted" in p.lower() or "died" in p.lower() for p in problems):
            verdict = "STOPPED"
        say(f"{verdict} — {len(problems)} problem(s):")
        for p in problems:
            say(f"  x {p}")
        rc = 2 if verdict == "STOPPED" else 1
    elif warnings:
        say(f"HEALTHY, with {len(warnings)} thing(s) worth a look:")
        rc = 1
    else:
        say("HEALTHY — nothing needs your attention.")
        rc = 0
    for w in warnings:
        say(f"  ! {w}")

    if not problems and status == "running":
        say()
        say("The campaign is running normally. Leave it alone.")
    elif not problems and status == "finished":
        say()
        say("The campaign finished. Read campaign_report.md, then collect the data.")

    if not args.no_report:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = os.path.join(args.out,
                            f"campaign_status_{socket.gethostname()}_{stamp}.txt")
        try:
            os.makedirs(args.out, exist_ok=True)
            body = "\n".join(lines).rstrip() + "\n"
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(body)
                fh.flush()
                os.fsync(fh.fileno())
            # Read it back. A status tool that silently hands you an empty
            # file is worse than one that writes nothing, because you send it
            # on and only find out later that it said nothing.
            with open(path, encoding="utf-8") as fh:
                back = fh.read()
            if len(back.strip()) < 200:
                print(f"\n!! The report file at {path} came back "
                      f"{len(back)} bytes - that is not right.")
                print("!! Copy the text above out of the console instead.")
            else:
                print(f"\nSaved to {path}  ({len(back):,} bytes, "
                      f"{len(lines)} lines)")
                print("Send that file if you want someone to look at it.")
        except Exception as exc:
            print(f"\n(could not write the report file: {exc})")
            print("Copy the text above out of the console instead.")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
