#!/usr/bin/env python3
"""
Round 3 orchestrator.  STANDALONE: imports nothing from sibling scripts.
It launches loadgen.py as a subprocess; tell it where with --loadgen.

Guarantees, all of them things Round 2 did not do:

  * Every level starts from a VERIFIED drained state. There is no "give up and
    continue" - a level that will not drain ABORTS the run. Carrying on is what
    produced the path-dependent backlog the reviewer objected to.
  * Level order is RANDOMISED with a recorded seed, so carry-over is
    counterbalanced instead of aliased onto the factor.
  * The run ABORTS on any error by default. Bad data is worse than no data.
  * Drain time is measured, which yields the subscriber's APPLY RATE in bytes
    per second - the service rate needed to model backlog dynamics.
  * Every run writes a complete manifest AND a human-readable report.

Single PostgreSQL version by design. Round 3 does not compare versions.

Exit codes
----------
  0  all levels completed
  2  bad arguments / configuration
  5  the run finished but one or more levels failed (--on-error continue)
  6  pre-flight check failed
  7  aborted mid-run (reason is in the manifest and the report)

Usage
-----
  set R3_PUB_DSN=host=localhost dbname=pub user=postgres password=...

  py orchestrate.py --config matrix_B_concurrency.json --repeat 1 --seed 1011
"""

import argparse
import hashlib
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
import zlib
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.environ.get("R3_DATA", r"C:\r3\data")
DEFAULT_PHASE = os.environ.get("R3_PHASE_FILE", r"C:\r3\phase_state.json")

EXPECTED_MAJOR = 18
DRAIN_THRESHOLD_BYTES = 1_048_576   # 1 MiB
DRAIN_CONSECUTIVE = 10              # consecutive 1 s polls under threshold
SETTLE_AFTER_DRAIN = 20             # quiet seconds before the next level

# Below this the drain is too short to time accurately, so no apply rate is
# reported rather than a number built from two or three samples.
APPLY_RATE_MIN_BACKLOG_BYTES = 100 * 1_048_576

# ...and the catch-up must span several 1 Hz polls, or the poll granularity
# alone dominates the measurement.
APPLY_RATE_MIN_CATCHUP_SEC = 5.0

# How often the lag is sampled while the load runs. The slope of that series
# over the tail of the load is what says whether the subscriber kept up.
LAG_SAMPLE_INTERVAL = 2.0

# --- attended latency gate (family F only) -------------------------------
# Family F is the one family that cannot run unattended: the network delay is
# injected by clumsy on the subscriber and has to be set by hand between
# levels. Rather than run six separate orchestrator invocations - which would
# produce six manifests to stitch together and six chances to mis-name an
# artefact - the run pauses before each level and waits for the operator to
# confirm the setting.
#
# The confirmation must carry the value, not just "go". The failure this
# guards against is not forgetting to press Start, it is forgetting to change
# 100 to 200 - and that one produces a complete, clean, entirely wrong level.
LATENCY_GATE_POLL = 2.0
LATENCY_GATE_TIMEOUT = 3600.0       # an hour is long enough for any RDP fumble
LATENCY_GATE_NAG = 120.0

# Seconds between "still draining" lines. A drain that lasts minutes should say
# so periodically; one that finishes quickly should say nothing at all.
DRAIN_NOTE_INTERVAL = 30.0

# A backlog climbing faster than this over the tail of the load means the
# subscriber is not keeping up. 200 KB/s over the final third is well above
# sampling noise and well below any real shortfall.
CAPACITY_SLOPE_BYTES_PER_SEC = 200_000

# A backlog this size still standing at the end of the load means the
# subscriber did not keep up. 32 MB is far above sampling noise and far below
# anything a level that genuinely kept pace ever leaves behind - on the
# measured data every correct call falls the right side of it.
CAPACITY_MIN_BACKLOG_BYTES = 32 * 1_048_576


class Abort(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


# --------------------------------------------------------------------------

def file_sha256(path):
    """So a manifest can prove which version of a script produced it."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def connect(dsn):
    c = psycopg2.connect(dsn)
    c.set_session(autocommit=True)
    return c


def write_phase(path, state, **kw):
    """Publish the current phase for the monitors to pick up.

    Both monitors poll this file once a second, and on Windows os.replace()
    fails while a reader has it open. This is a signpost, not data - losing
    one write costs nothing, while raising here would abort a good level.
    """
    payload = {"phase_state": state,
               "updated": datetime.now(timezone.utc).isoformat()}
    payload.update(kw)
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        blob = json.dumps(payload)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(blob)
        for attempt in range(5):
            try:
                os.replace(tmp, path)
                return
            except Exception:
                time.sleep(0.2 * (attempt + 1))
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(blob)
        try:
            os.remove(tmp)
        except Exception:
            pass
    except Exception:
        pass


def q1(cur, sql, args=None):
    cur.execute(sql, args)
    r = cur.fetchone()
    return r[0] if r else None


def current_lag(cur):
    v = q1(cur, """SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),0)::bigint
                   FROM pg_stat_replication WHERE application_name='mysub' LIMIT 1""")
    return int(v) if v is not None else None


def slot_status(cur):
    cur.execute("SELECT wal_status, active FROM pg_replication_slots "
                "WHERE slot_type='logical' LIMIT 1")
    r = cur.fetchone()
    return (r[0], r[1]) if r else (None, None)


def free_gb(path):
    try:
        return shutil.disk_usage(path).free / 1024**3
    except Exception:
        return None


# --------------------------------------------------------------------------

def preflight(cur, args):
    """Everything that would waste a run if it were wrong. Fail loudly, now."""
    checks = []

    ver = q1(cur, "SELECT current_setting('server_version_num')")
    major = int(ver) // 10000
    checks.append(("postgres_version", f"{q1(cur, 'SELECT version()')[:60]}",
                   major == EXPECTED_MAJOR))
    if major != EXPECTED_MAJOR:
        raise Abort("WRONG_PG_VERSION",
                    f"connected to PostgreSQL {major}, Round 3 requires "
                    f"{EXPECTED_MAJOR}. Do not mix versions.")

    wl = q1(cur, "SELECT current_setting('wal_level')")
    checks.append(("wal_level", wl, wl == "logical"))
    if wl != "logical":
        raise Abort("WAL_LEVEL", f"wal_level is '{wl}', must be 'logical'")

    ok_tbl = q1(cur, "SELECT to_regclass('public.ingest_data') IS NOT NULL")
    checks.append(("table_exists", "public.ingest_data", bool(ok_tbl)))
    if not ok_tbl:
        raise Abort("NO_SCHEMA", "table public.ingest_data not found - "
                                 "run 01_schema_publisher.sql first")

    lag = current_lag(cur)
    checks.append(("replication_active", "pg_stat_replication row", lag is not None))
    if lag is None:
        raise Abort("REPLICATION_DOWN",
                    "no active row in pg_stat_replication for 'mysub' - "
                    "is the subscriber running?")

    st, active = slot_status(cur)
    checks.append(("slot_status", f"{st} active={active}", st == "reserved"))
    if st in ("lost", "unreserved"):
        raise Abort("SLOT_INVALID", f"replication slot wal_status={st}")

    fg = free_gb(args.data_volume)
    checks.append(("free_space_gb", f"{fg:.1f}" if fg else "unknown",
                   fg is None or fg >= args.min_free_gb))
    if fg is not None and fg < args.min_free_gb:
        raise Abort("DISK_LOW", f"only {fg:.1f} GB free on {args.data_volume}, "
                                f"need {args.min_free_gb}")

    if not os.path.isfile(args.loadgen):
        raise Abort("NO_LOADGEN", f"load generator not found at {args.loadgen}")
    checks.append(("loadgen_present", args.loadgen, True))

    print("\n  pre-flight")
    for name, val, ok in checks:
        print(f"    {'OK ' if ok else 'BAD'}  {name:<20} {val}")
    print()
    return checks


def capture_settings(cur):
    cur.execute("""SELECT name, setting, unit FROM pg_settings WHERE name IN (
        'server_version','wal_level','synchronous_commit','wal_compression',
        'max_wal_size','min_wal_size','checkpoint_timeout','shared_buffers',
        'max_wal_senders','max_replication_slots','wal_keep_size',
        'track_io_timing','track_wal_io_timing','full_page_writes')
        ORDER BY name""")
    return {r[0]: (r[1] + (" " + r[2] if r[2] else "")) for r in cur.fetchall()}


def drain(cur, path_phase, level_id, label, args, log):
    """Wait until the subscriber has ACTUALLY caught up. No early exit."""
    write_phase(path_phase, "drain", level_id=level_id)
    t0 = time.monotonic()
    start = current_lag(cur) or 0
    peak, consec = start, 0
    log(f"  [{label}] backlog at start {start/1e6:,.1f} MB")

    caught_up_at = None
    next_note = DRAIN_NOTE_INTERVAL
    while True:
        time.sleep(1.0)
        lag = current_lag(cur)
        if lag is None:
            raise Abort("REPLICATION_DOWN",
                        f"replication disappeared during {label} of {level_id}")
        peak = max(peak, lag)

        st, _ = slot_status(cur)
        if st in ("lost", "unreserved"):
            raise Abort("SLOT_INVALID",
                        f"slot wal_status={st} during {label} of {level_id}")

        if lag <= DRAIN_THRESHOLD_BYTES:
            consec += 1
            if caught_up_at is None:
                caught_up_at = time.monotonic()   # FIRST time it was caught up
        else:
            consec = 0
            caught_up_at = None
        if consec >= DRAIN_CONSECUTIVE:
            break

        el = time.monotonic() - t0
        if el > args.drain_timeout:
            raise Abort("POST_DRAIN_TIMEOUT" if label.startswith("post") else "PRE_DRAIN_TIMEOUT",
                        f"{level_id} did not drain within {args.drain_timeout:.0f}s; "
                        f"backlog still {lag/1e6:,.1f} MB")
        if el >= next_note:
            next_note += DRAIN_NOTE_INTERVAL
            log(f"  [{label}] {el:5.0f}s  backlog {lag/1e6:9,.1f} MB")

    dur = time.monotonic() - t0
    # The apply rate is backlog / TIME TO CATCH UP - not backlog / total loop
    # time. The loop cannot end before DRAIN_CONSECUTIVE confirmation polls
    # have passed, so dividing by the loop time added a fixed ~10 s to the
    # denominator and biased every apply rate low by a factor that depended on
    # the size of the backlog, i.e. on the very factor under test.
    catchup = (caught_up_at - t0) if caught_up_at is not None else dur
    catchup = max(catchup, 0.001)
    # Two conditions, not one. A 16 MB backlog clears in well under a second
    # at 40 MB/s, but the lag is polled at 1 Hz, so the catch-up reads as 1-2 s
    # and the apply rate comes out 4-5x too low. Requiring the catch-up itself
    # to span several polls is what makes the number trustworthy.
    measurable = (start > APPLY_RATE_MIN_BACKLOG_BYTES
                  and catchup >= APPLY_RATE_MIN_CATCHUP_SEC)
    rate = (start / catchup) if measurable else None
    log(f"  [{label}] drained in {dur:.0f}s (caught up after {catchup:.1f}s)"
        + (f"  ->  apply rate {rate/1e6:.1f} MB/s" if rate else ""))
    return {"drained": True,
            "drain_sec": round(dur, 1),
            "catchup_sec": round(catchup, 2),
            "confirm_sec": round(dur - catchup, 2),
            "start_backlog_bytes": start, "peak_backlog_bytes": peak,
            "apply_rate_measurable": bool(measurable),
            "apply_rate_bytes_per_sec": round(rate, 1) if rate else None,
            "apply_rate_mb_per_sec": round(rate / 1e6, 3) if rate else None}


def wal_counters(cur):
    """(wal_bytes, stats_reset epoch). The reset stamp is what tells us later
    whether the two ends of a difference belong to the same counter epoch."""
    cur.execute("SELECT wal_bytes::bigint, "
                "EXTRACT(EPOCH FROM stats_reset)::float8 FROM pg_stat_wal")
    r = cur.fetchone()
    return (int(r[0]), float(r[1]) if r[1] is not None else None)


def sub_row_count(sub_dsn):
    """Row count on the subscriber.

    None  - the subscriber could not be reached, so we do not know.
    -1    - the count did not finish in 60 s, which on this table means it is
            very large. That is NOT the same as 'unknown': a level must not be
            allowed to start, and without the timeout the orchestrator would
            simply hang here forever with the level never beginning.
    """
    if not sub_dsn:
        return None
    try:
        c = psycopg2.connect(sub_dsn, connect_timeout=10,
                             application_name="r3_orchestrator",
                             options="-c statement_timeout=60000")
        c.set_session(autocommit=True)
        k = c.cursor()
        try:
            k.execute("SELECT count(*) FROM ingest_data")
            n = int(k.fetchone()[0])
        except psycopg2.errors.QueryCanceled:
            n = -1
        k.close(); c.close()
        return n
    except Exception:
        return None


def subscriber_facts(sub_dsn):
    """Everything about the subscriber that the manifest needs to be complete.

    The subscriber's apply behaviour is the object of study, and none of it
    was being recorded: not its version, not its settings, not even whether
    its indexes existed. A campaign could have run against a subscriber that
    was missing three of its four indexes and nothing would have noticed.
    """
    if not sub_dsn:
        return {"reachable": False, "note": "no --sub-dsn given"}
    out = {"reachable": True}
    try:
        c = psycopg2.connect(sub_dsn, connect_timeout=10)
        c.set_session(autocommit=True)
        k = c.cursor()
        k.execute("SELECT version(), current_setting('server_version')")
        v = k.fetchone()
        out["version_string"], out["server_version"] = v[0], v[1]
        k.execute("""SELECT name, setting, unit FROM pg_settings WHERE name IN (
            'shared_buffers','max_wal_size','synchronous_commit','wal_level',
            'max_logical_replication_workers','max_worker_processes',
            'max_parallel_apply_workers_per_subscription',
            'max_sync_workers_per_subscription','logical_decoding_work_mem',
            'wal_receiver_timeout','track_io_timing','full_page_writes',
            'checkpoint_timeout','effective_cache_size','maintenance_work_mem')
            ORDER BY name""")
        out["settings"] = {r[0]: (r[1] + (" " + r[2] if r[2] else ""))
                           for r in k.fetchall()}
        k.execute("SELECT indexname FROM pg_indexes WHERE tablename='ingest_data' "
                  "ORDER BY indexname")
        out["indexes"] = [r[0] for r in k.fetchall()]
        k.execute("SELECT subname, subenabled, substream, subbinary, subsynccommit "
                  "FROM pg_subscription")
        out["subscriptions"] = [
            {"subname": r[0], "enabled": r[1], "streaming": str(r[2]),
             "binary": r[3], "synchronous_commit": r[4]} for r in k.fetchall()]
        k.execute("SELECT count(*) FROM ingest_data")
        out["rows"] = int(k.fetchone()[0])
        k.close(); c.close()
    except Exception as exc:
        out["reachable"] = False
        out["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
    return out


def classify_capacity(lag_series, end_lag):
    """Was the subscriber falling behind, or holding?

    Returns (slope_bytes_per_sec, exceeded, basis). The slope is a least
    squares fit over the last third of the load phase, which is where a
    genuine capacity shortfall shows as a steady climb. A single end-of-load
    sample cannot tell a saturated 20 s level from an unsaturated 60 s one.
    """
    pts = [(t, v) for t, v in lag_series if v is not None]
    if len(pts) >= 6:
        tail = pts[max(1, (len(pts) * 2) // 3):]
        if len(tail) >= 4:
            n = len(tail)
            mt = sum(t for t, _ in tail) / n
            mv = sum(v for _, v in tail) / n
            den = sum((t - mt) ** 2 for t, _ in tail)
            if den > 0:
                slope = sum((t - mt) * (v - mv) for t, v in tail) / den
                # The SLOPE alone was wrong in both directions on real data: a
                # level that ended with 0.2 MB of backlog was flagged over
                # capacity because noise around zero fitted a positive line,
                # while a level carrying 362 MB was flagged fine because the
                # backlog had plateaued - the publisher had hit its own ceiling,
                # so the queue stopped GROWING while staying enormous.
                #
                # What "the subscriber could not keep up" actually means is
                # that a material backlog was still standing at the end of the
                # load. The slope is kept in the manifest as a second signal,
                # but it does not decide this on its own.
                exceeded = mv > CAPACITY_MIN_BACKLOG_BYTES
                return round(slope, 1), bool(exceeded), "mean_backlog_over_load_tail"
    # Not enough samples to fit - fall back to the single end-of-load reading,
    # against the same threshold, and record that this is what happened.
    return (None,
            bool(end_lag is not None and end_lag > CAPACITY_MIN_BACKLOG_BYTES),
            "end_of_load_backlog_only")


def _new_group():
    """Popen kwargs that put the load generator in its own process group, so
    the whole tree can be killed. Without this, terminating the parent on a
    wall-clock overrun left its worker processes writing to the publisher for
    the rest of the campaign."""
    if os.name == "nt":
        return {"creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)}
    return {"start_new_session": True}


# The load generator runs in its own process group (see _new_group) so that
# the whole tree can be killed as a unit. The cost of that isolation is that
# it does NOT die when this process does: a SIGTERM to orchestrate.py used to
# leave sixteen loadgen workers writing to the publisher at full rate, with
# nothing left running that knew about them. Observed on the rig - the
# supervisor parked, orchestrate died, and eighteen loadgen processes kept
# going. These two make the signal path go through kill_tree like every other
# exit path already does.
_live_load = None


def _on_signal(signum, _frame):
    p = _live_load
    if p is not None and p.poll() is None:
        print(f"  !! signal {signum} - stopping the load generator and its "
              f"workers before exiting", flush=True)
        kill_tree(p, lambda m: print(m, flush=True))
    raise SystemExit(130)


def install_signal_handlers():
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _on_signal)
        except (ValueError, OSError):
            pass


def kill_tree(proc, log):
    """Stop a process AND its children. Best effort, never raises."""
    try:
        import psutil
        p = psutil.Process(proc.pid)
        kids = p.children(recursive=True)
    except Exception:
        psutil = None
        kids = []
    for target in kids + [proc]:
        try:
            target.terminate()
        except Exception:
            pass
    try:
        proc.wait(timeout=30)
    except Exception:
        pass
    if os.name != "nt":
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except Exception:
            pass
    for target in kids:
        try:
            if target.is_running():
                target.kill()
        except Exception:
            pass
    try:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    except Exception:
        pass
    still = []
    try:
        for target in kids:
            if target.is_running():
                still.append(target.pid)
    except Exception:
        pass
    if still:
        log(f"  !! could not kill load generator children {still} - "
            f"the next level may be contaminated")


def wait_for_latency(level, args, log):
    """Block until the operator confirms clumsy is set for THIS level.

    Returns a dict for the manifest, or None if the gate is off. Raises Abort
    on stop file or timeout - never proceeds on an unconfirmed setting, because
    a level run at the wrong latency is indistinguishable from a good one in
    the data.
    """
    if not args.latency_gate:
        return None
    want = int(level.get("network_latency_ms") or 0)
    path = args.latency_gate_file
    lid = level["level_id"]

    # A stale confirmation from the previous level is worse than none: it would
    # wave this level straight through. Clear it first, then demand a fresh one.
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception as exc:
        raise Abort("LATENCY_GATE",
                    f"could not clear the previous confirmation at {path}: {exc}")

    log("")
    log("  " + "-" * 62)
    if want == 0:
        log(f"  {lid} is the CONTROL level: press Stop in clumsy, so no")
        log("  injection machinery is running at all.")
    else:
        # What to TYPE and what it PRODUCES are different numbers. clumsy
        # rounds any non-zero lag up by about two Windows timer ticks (~31 ms),
        # measured on this testbed, so the setting is the target minus that.
        # Printing the target here would have the operator type 200 and get
        # 231 - a whole level wrong, silently.
        lag = level.get("clumsy_lag_ms")
        if lag is None:
            log(f"  {lid} needs clumsy STARTED, on the SUBSCRIBER, filter:")
        else:
            log(f"  {lid} needs clumsy Lag = {lag} ms  ->  about {want} ms of")
            log("  added RTT. Type the Lag value, not the RTT. On the")
            log("  SUBSCRIBER, filter:")
        log("      " + (args.clumsy_filter or
                        "inbound and ip.SrcAddr == <publisher IP> and "
                        "tcp.SrcPort == 5432"))
        log("  Lag only, Inbound ticked, Chance 100%, everything else off.")
        log(f"  Verify before confirming:  py check_latency.py --expect {want}")
    log("  Then confirm from the publisher:")
    log(f'      "{want}" | Set-Content {path}')
    log("  " + "-" * 62)

    write_phase(args.phase_file, "awaiting_latency", level_id=lid,
                network_latency_ms=want)

    deadline = time.monotonic() + LATENCY_GATE_TIMEOUT
    next_nag = time.monotonic() + LATENCY_GATE_NAG
    warned = None
    while True:
        if os.path.exists(args.stop_file):
            raise Abort("OPERATOR_STOP", "stop file appeared at the latency gate")
        try:
            with open(path, encoding="utf-8") as fh:
                raw = fh.read().strip()
        except FileNotFoundError:
            raw = None
        except Exception:
            raw = None

        if raw is not None:
            try:
                got = int(float(raw.split()[0])) if raw.split() else None
            except Exception:
                got = None
            if got == want:
                log(f"  confirmed: {want} ms")
                try:
                    os.remove(path)
                except Exception:
                    pass
                return {"gate": "confirmed", "requested_ms": want,
                        "confirmed_utc": datetime.now(timezone.utc).isoformat()}
            # Wrong value. Say so once per distinct wrong value and keep
            # waiting rather than running the level at whatever is set.
            if got != warned:
                warned = got
                log(f"  !! {path} says {raw!r}, this level needs {want}. "
                    f"clumsy is probably still on the previous setting.")
                log("     Fix clumsy, then write the correct value again.")

        if time.monotonic() > deadline:
            raise Abort("LATENCY_GATE",
                        f"no confirmation of {want} ms for {lid} within "
                        f"{LATENCY_GATE_TIMEOUT/60:.0f} minutes")
        if time.monotonic() >= next_nag:
            next_nag = time.monotonic() + LATENCY_GATE_NAG
            left = (deadline - time.monotonic()) / 60.0
            log(f"  still waiting for {want} ms on {lid} "
                f"({left:.0f} min before this run gives up)")
        time.sleep(LATENCY_GATE_POLL)


def run_level(level, args, cur, idx, total, log):
    lid = level["level_id"]
    log(f"\n--- [{idx}/{total}] {lid}  {level.get('label','')} ---")
    params = {k: level.get(k, 0) for k in
              ("clients", "rows_per_commit", "row_bytes",
               "target_wal_mbps", "network_latency_ms")}
    # What was TYPED into clumsy, as distinct from what it produced. The two
    # differ by about 31 ms on this platform, and Methods needs both numbers.
    params["clumsy_lag_ms"] = level.get("clumsy_lag_ms")

    gate = wait_for_latency(level, args, log)

    started_utc = datetime.now(timezone.utc).isoformat()

    # 1. defined starting state
    write_phase(args.phase_file, "reset", level_id=lid, **params)
    cur.execute("SELECT reset_workload_table()")
    cur.execute("VACUUM ANALYZE ingest_data")
    pre = drain(cur, args.phase_file, lid, "pre-drain", args, log)

    time.sleep(SETTLE_AFTER_DRAIN)

    # "Drained" only ever meant "the LSN distance is small". It said nothing
    # about the data: a level could start with a full table on both sides and
    # still be recorded as started_clean. Count the rows.
    pub_rows = int(q1(cur, "SELECT count(*) FROM ingest_data") or 0)
    sub_rows = sub_row_count(args.sub_dsn)
    clean = (pub_rows == 0) and (sub_rows in (0, None))
    if not clean:
        if sub_rows is None:
            sub_txt = "unknown - the subscriber could not be reached"
        elif sub_rows < 0:
            sub_txt = "too many to count in 60 s"
        else:
            sub_txt = format(sub_rows, ",")
        raise Abort("NOT_CLEAN",
                    f"{lid} did not start from an empty table: publisher has "
                    f"{pub_rows:,} rows, subscriber has {sub_txt}")

    wal0, reset0 = wal_counters(cur)
    t0 = time.monotonic()

    # 2. load
    write_phase(args.phase_file, "load", level_id=lid, **params)
    cmd = [sys.executable, args.loadgen,
           "--dsn", args.pub_dsn,
           "--clients", str(level["clients"]),
           "--rows-per-commit", str(level.get("rows_per_commit", 1)),
           "--row-bytes", str(level.get("row_bytes", 1000)),
           "--duration", str(level.get("duration_sec", args.duration)),
           "--op-mix", level.get("op_mix", "insert"),
           "--out", args.out, "--level-id", lid,
           "--seed", str(args.seed + idx),
           "--max-sql-errors", str(args.max_sql_errors)]
    if level.get("target_wal_mbps"):
        cmd += ["--target-wal-mbps", str(level["target_wal_mbps"])]
    elif level.get("commit_rate"):
        cmd += ["--commit-rate", str(level["commit_rate"])]
    else:
        cmd += ["--unthrottled"]

    cap = float(level.get("duration_sec", args.duration)) + args.load_grace
    lag_series = []
    proc = None
    try:
        proc = subprocess.Popen(cmd, **_new_group())
        global _live_load
        _live_load = proc
        deadline = time.monotonic() + cap
        next_lag = time.monotonic()
        rc = None
        while True:
            try:
                rc = proc.wait(timeout=0.5)
                break
            except subprocess.TimeoutExpired:
                pass
            nowm = time.monotonic()
            if nowm >= next_lag:
                next_lag = nowm + LAG_SAMPLE_INTERVAL
                try:
                    lg_now = current_lag(cur)
                except Exception:
                    lg_now = None
                if lg_now is not None:
                    lag_series.append((round(nowm - t0, 2), int(lg_now)))
            if nowm > deadline:
                log(f"  !! load generator overran {cap:.0f}s - terminating")
                kill_tree(proc, log)
                rc = -9
                break
    except Exception:
        if proc is not None:
            kill_tree(proc, log)
        raise
    finally:
        _live_load = None
    load_sec = time.monotonic() - t0
    wal1, reset1 = wal_counters(cur)     # read BEFORE anything else moves
    end_lag = current_lag(cur)

    if rc == 3:
        raise Abort("LOADGEN_ERRORS", f"{lid}: load generator exceeded the SQL error limit")
    if rc == 4:
        raise Abort("LOADGEN_NO_COMMITS", f"{lid}: load generator completed zero transactions")
    if rc == -9:
        raise Abort("LOADGEN_TIMEOUT", f"{lid}: load generator overran its wall-clock cap")
    if rc == 7:
        raise Abort("STATS_RESET",
                    f"{lid}: PostgreSQL statistics were reset during the level "
                    f"(the publisher restarted); the measurement is void")
    if rc == 8:
        raise Abort("LOADGEN_OPEN_LOOP",
                    f"{lid}: the WAL controller never started - the level would "
                    f"have run open loop")
    if rc != 0:
        raise Abort("LOADGEN_EXIT", f"{lid}: load generator exited {rc}")

    if reset0 is not None and reset1 is not None and reset0 != reset1:
        raise Abort("STATS_RESET",
                    f"{lid}: pg_stat_wal was reset during the level; "
                    f"wal_bytes cannot be differenced across it")

    lg = {}
    lgp = os.path.join(args.out, f"loadgen_{lid}.json")
    if os.path.isfile(lgp):
        with open(lgp) as fh:
            lg = json.load(fh)

    measured = (wal1 - wal0) / load_sec / 1_048_576 if load_sec else None
    tgt = level.get("target_wal_mbps") or 0
    # `is not None`, not truthiness: a level that produced EXACTLY 0.0 MB/s is
    # the worst possible outcome, and 0.0 is falsy, so it used to be exempted
    # from the set-point check entirely.
    setpoint_err = ((measured - tgt) / tgt) if (tgt and measured is not None) else None
    if args.strict_setpoint and setpoint_err is not None and abs(setpoint_err) > args.setpoint_tol:
        raise Abort("SETPOINT_MISS",
                    f"{lid}: target {tgt} MB/s, measured {measured:.1f} MB/s "
                    f"({setpoint_err*100:+.0f}%)")

    lag_slope, exceeded, basis = classify_capacity(lag_series, end_lag)

    # 3. drain, which measures the apply rate
    post = drain(cur, args.phase_file, lid, "post-drain", args, log)

    res = {
        "level_id": lid, "label": level.get("label", ""), "order_index": idx,
        "started_utc": started_utc,
        "finished_utc": datetime.now(timezone.utc).isoformat(),
        "loadgen_seed": args.seed + idx,
        "loadgen_command": cmd[1:],
        "params": dict(params, op_mix=level.get("op_mix", "insert"),
                       duration_sec=level.get("duration_sec", args.duration),
                       commit_rate=level.get("commit_rate"),
                       unthrottled=bool(not level.get("target_wal_mbps")
                                        and not level.get("commit_rate"))),
        "latency_gate": gate,
        "started_clean": pre["drained"], "pre_drain": pre,
        "load_seconds": round(load_sec, 1),
        "wal_bytes_generated": wal1 - wal0,
        "measured_wal_mb_per_sec": round(measured, 3) if measured is not None else None,
        "loadgen_measured_wal_mb_per_sec": lg.get("measured_wal_mb_per_sec_overall"),
        "publisher_rows_before": pub_rows,
        "subscriber_rows_before": sub_rows,
        "lag_series": lag_series,
        "lag_growth_bytes_per_sec": lag_slope,
        "peak_lag_during_load_bytes": (max(v for _, v in lag_series)
                                       if lag_series else None),
        "setpoint_error_pct": round(setpoint_err * 100, 1) if setpoint_err is not None else None,
        "lag_at_load_end_bytes": end_lag,
        "post_drain": post,
        # A level whose backlog kept growing has no steady-state lag; it must be
        # reported as a growth rate, not a mean. This is the classification that
        # answers the path-dependence objection.
        # Capacity is exceeded when the backlog is still GROWING at the end of
        # the load, not when one final sample happens to be large. The old
        # single-sample rule classified the same workload differently at 22 s
        # and at 40 s, so it measured duration rather than capacity.
        "exceeded_capacity": exceeded,
        "exceeded_capacity_basis": basis,
        "loadgen": {k: lg.get(k) for k in
                    ("completed_commits", "completed_rows_inserted",
                     "achieved_commits_per_sec", "achieved_rows_per_sec",
                     "sql_errors", "reconnects", "workers_started",
                     "workers_min_alive", "controller_started",
                     "on_target", "setpoint_error_frac", "control")},
    }
    log(f"  measured {res['measured_wal_mb_per_sec']} MB/s | "
        f"lag at end {(end_lag or 0)/1e6:,.1f} MB | "
        f"capacity exceeded: {res['exceeded_capacity']}")
    return res


# --------------------------------------------------------------------------

def num(v):
    """Format a number with thousands separators, or an em-dash if it is not
    a number. Guards every table cell in the report."""
    if isinstance(v, bool) or v is None:
        return "—"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return str(v)


def write_report(man, path):
    L = []
    a = L.append
    a(f"# Run report — {man['run_id']}\n")
    a(f"- **Family**: {man['family']}")
    a(f"- **Repeat**: {man['repeat']}   **Seed**: {man['seed']}   "
      f"**Randomised**: {man['randomised']}")
    a(f"- **PostgreSQL**: {man['pg_version']}   **Host**: {man['host']}")
    a(f"- **Started**: {man['started_utc']}")
    a(f"- **Finished**: {man.get('finished_utc', '(incomplete)')}")
    n_ok = len([r for r in man["results"] if not r.get("failed")])
    n_planned = len(man.get("level_order") or [])
    if man.get("abort"):
        status = "ABORTED — " + man["abort"]["code"]
    elif not man.get("finished_utc"):
        status = "INCOMPLETE — the orchestrator never finished"
    elif n_planned and n_ok < n_planned:
        status = f"INCOMPLETE — {n_ok} of {n_planned} levels"
    else:
        status = "completed"
    a(f"- **Status**: {status}")
    a(f"- **Levels completed**: {n_ok} of {n_planned}")
    if man.get("abort"):
        a(f"\n> **Abort reason**: {man['abort']['message']}\n")
    a(f"\n{man.get('description','')}\n")

    a("## Level order (as executed)\n")
    a(" → ".join(man["level_order"]) + "\n")

    a("## Results\n")
    a("| Level | Clients | Rows/commit | Target MB/s | Measured MB/s | Err % | "
      "Clean start | Lag at end (MB) | Drain s | Apply MB/s | Over capacity |")
    a("|---|---|---|---|---|---|---|---|---|---|---|")
    for r in man["results"]:
        if r.get("failed"):
            a(f"| {r['level_id']} | FAILED: {r.get('error_code')} | | | | | | | | | |")
            continue
        p, pd_ = r["params"], r.get("post_drain", {})
        a(f"| {r['level_id']} | {p.get('clients')} | {p.get('rows_per_commit')} | "
          f"{p.get('target_wal_mbps') or '—'} | {r.get('measured_wal_mb_per_sec') or '—'} | "
          f"{r.get('setpoint_error_pct') if r.get('setpoint_error_pct') is not None else '—'} | "
          f"{'yes' if r.get('started_clean') else 'NO'} | "
          f"{(r.get('lag_at_load_end_bytes') or 0)/1e6:,.1f} | "
          f"{pd_.get('drain_sec','—')} | {pd_.get('apply_rate_mb_per_sec') or '—'} | "
          f"{'YES' if r.get('exceeded_capacity') else 'no'} |")

    a("\n## Load generator (client-side completion counts)\n")
    a("| Level | Commits | Rows inserted | Commits/s | Rows/s | SQL errors | "
      "Steady WAL MB/s |")
    a("|---|---|---|---|---|---|---|")
    for r in man["results"]:
        lg = r.get("loadgen", {}) or {}
        ctl = lg.get("control") or {}
        sm = ctl.get("steady_mean_mbps")
        sd = ctl.get("steady_stdev_mbps")
        # num() rather than a bare ",": a failed level has no loadgen block, so
        # formatting an em-dash with ":," raised ValueError inside the report
        # writer - which meant --on-error continue crashed on its first
        # failure instead of continuing.
        a(f"| {r['level_id']} | {num(lg.get('completed_commits'))} | "
          f"{num(lg.get('completed_rows_inserted'))} | "
          f"{num(lg.get('achieved_commits_per_sec'))} | "
          f"{num(lg.get('achieved_rows_per_sec'))} | "
          f"{num(lg.get('sql_errors'))} | "
          f"{f'{sm} ± {sd}' if sm is not None else '—'} |")

    ap_ = [r["post_drain"]["apply_rate_mb_per_sec"] for r in man["results"]
           if (r.get("post_drain") or {}).get("apply_rate_mb_per_sec")]
    if ap_:
        a(f"\n**Measured apply rate across this run**: "
          f"min {min(ap_):.1f}, median {sorted(ap_)[len(ap_)//2]:.1f}, "
          f"max {max(ap_):.1f} MB/s, from {len(ap_)} of {len(man['results'])} "
          f"levels  \n"
          f"_(backlog cleared ÷ time to catch up, with no load arriving — this "
          f"is the subscriber's service rate. Levels whose backlog was under "
          f"{APPLY_RATE_MIN_BACKLOG_BYTES/1e6:.0f} MB, or that caught up in under "
          f"{APPLY_RATE_MIN_CATCHUP_SEC:.0f}s, are excluded: the 1 Hz lag poll "
          f"cannot time them.)_\n")
    else:
        a(f"\n**No apply rate could be measured in this run** — no level left a "
          f"backlog above {APPLY_RATE_MIN_BACKLOG_BYTES/1e6:.0f} MB that took "
          f"more than {APPLY_RATE_MIN_CATCHUP_SEC:.0f}s to clear. That is "
          f"expected below the knee.\n")

    a("\n## PostgreSQL settings at run time\n")
    a("| Setting | Value |")
    a("|---|---|")
    for k, v in sorted(man.get("pg_settings", {}).items()):
        a(f"| `{k}` | {v} |")

    a("\n## Pre-flight checks\n")
    a("| Check | Value | Passed |")
    a("|---|---|---|")
    for name, val, ok in man.get("preflight", []):
        a(f"| {name} | {val} | {'yes' if ok else 'NO'} |")

    a("\n## Files\n")
    a(f"- `manifest_{man['run_id']}.json` — machine-readable form of this report")
    a(f"- `publisher_{man['run_id']}.csv`, `subscriber_{man['run_id']}.csv` — 1 Hz samples")
    a(f"- `publisher_{man['run_id']}_events.log` — monitor event log")
    a("- `loadgen_<level>.json` — per-level completion counts and control trace\n")

    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(L))


def main():
    install_signal_handlers()
    ap = argparse.ArgumentParser(description="Round 3 orchestrator (standalone).")
    ap.add_argument("--config", required=True)
    ap.add_argument("--repeat", type=int, required=True)
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--pub-dsn", default=None, help="defaults to $R3_PUB_DSN")
    ap.add_argument("--sub-dsn", default=None,
                    help="defaults to $R3_SUB_DSN. Used to verify that each "
                         "level really starts from an empty subscriber table "
                         "and to record the subscriber's configuration.")
    ap.add_argument("--out", default=DEFAULT_DATA)
    ap.add_argument("--phase-file", default=DEFAULT_PHASE)
    ap.add_argument("--loadgen", default=None,
                    help="path to loadgen.py (default: sibling of this file)")
    ap.add_argument("--duration", type=float, default=360.0,
                    help="default seconds per level when not set in the config")
    ap.add_argument("--drain-timeout", type=float, default=3600.0,
                    help="runaway guard only; reaching it ABORTS the run")
    ap.add_argument("--on-error", choices=["abort", "continue"], default="abort")
    ap.add_argument("--max-sql-errors", type=int, default=10)
    ap.add_argument("--strict-setpoint", action="store_true",
                    help="abort if the closed loop misses its target rate")
    ap.add_argument("--setpoint-tol", type=float, default=0.15)
    ap.add_argument("--min-free-gb", type=float, default=50.0)
    ap.add_argument("--data-volume", default="F:\\" if os.name == "nt" else "/")
    ap.add_argument("--no-randomise", action="store_true")
    ap.add_argument("--latency-gate", action="store_true",
                    help="attended family F: pause before each level until the "
                         "operator confirms the clumsy setting")
    ap.add_argument("--clumsy-filter", default=None,
                    help="printed verbatim at each latency gate; defaults to the "
                         "matrix file's clumsy_filter, then to a placeholder")
    ap.add_argument("--latency-gate-file",
                    default=os.environ.get("R3_LATENCY_FILE", r"C:\r3\LATENCY_SET"),
                    help="the operator writes the millisecond value here to "
                         "release the gate")
    ap.add_argument("--run-suffix", default="",
                    help="appended to the run id. The supervisor uses it on a "
                         "retry so the second attempt writes its own CSV, "
                         "event log and manifest instead of appending to the "
                         "failed attempt's files.")
    ap.add_argument("--resume", action="store_true",
                    help="skip levels already completed in an existing manifest")
    ap.add_argument("--load-grace", type=float, default=300.0,
                    help="seconds a load level may overrun before it is killed")
    ap.add_argument("--stop-file", default=os.environ.get("R3_STOP_FILE", r"C:\r3\STOP"),
                    help="abort cleanly between levels if this file appears")
    args = ap.parse_args()

    args.pub_dsn = args.pub_dsn or os.environ.get("R3_PUB_DSN")
    args.sub_dsn = args.sub_dsn or os.environ.get("R3_SUB_DSN")
    if not args.pub_dsn:
        print("ERROR: no DSN. Pass --pub-dsn or set R3_PUB_DSN.")
        return 2
    floor = DRAIN_CONSECUTIVE + 5
    if args.drain_timeout < floor:
        print(f"ERROR: --drain-timeout must be at least {floor}s. The drain "
              f"needs {DRAIN_CONSECUTIVE} consecutive quiet polls before it can "
              f"declare success, so anything below that can never succeed and "
              f"would abort every level with a misleading 'did not drain' "
              f"message while reporting a zero backlog.")
        return 2
    if args.loadgen is None:
        args.loadgen = os.path.join(os.path.dirname(os.path.abspath(__file__)), "loadgen.py")

    with open(args.config) as fh:
        cfg = json.load(fh)
    levels = list(cfg["levels"])
    # The clumsy filter is a property of the testbed, so it lives in the matrix
    # file rather than being hard-coded here or retyped on the command line.
    if not args.clumsy_filter:
        args.clumsy_filter = cfg.get("clumsy_filter")
    if not args.no_randomise:
        # Seed with the family name as well as the numeric seed. shuffle()
        # depends only on the seed and the list LENGTH, so two families with
        # the same number of levels and the same seed - C_batching and
        # D_rowsize both have six - received byte-identical permutations, and
        # carry-over order was perfectly correlated instead of counterbalanced.
        order_seed = zlib.crc32(f"{cfg['family']}|{args.repeat}".encode()) ^ args.seed
        random.Random(order_seed).shuffle(levels)
    else:
        order_seed = None

    run_id = f"{cfg['family']}_rep{args.repeat}{args.run_suffix}"
    os.makedirs(args.out, exist_ok=True)
    man_path = os.path.join(args.out, f"manifest_{run_id}.json")
    rep_path = os.path.join(args.out, f"run_report_{run_id}.md")
    log_path = os.path.join(args.out, f"orchestrator_{run_id}.log")
    logfh = open(log_path, "a", encoding="utf-8")

    def log(msg):
        print(msg)
        logfh.write(f"{datetime.now(timezone.utc).isoformat()}\t{msg}\n")
        logfh.flush()

    conn = connect(args.pub_dsn)
    cur = conn.cursor()

    already_done = set()
    if args.resume:
        prev = None
        try:
            with open(man_path, encoding="utf-8") as fh:
                prev = json.load(fh)
        except Exception:
            pass
        if prev:
            already_done = {r["level_id"] for r in prev.get("results", [])
                            if not r.get("failed")}

    man = {
        "run_id": run_id, "family": cfg["family"],
        "pg_version": None,
        "description": cfg.get("description", ""),
        "repeat": args.repeat, "seed": args.seed,
        "randomised": not args.no_randomise,
        "level_order": [l["level_id"] for l in levels],
        "host": socket.gethostname(),
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "config_file": os.path.abspath(args.config),
        "config_sha256": file_sha256(args.config),
        "config_contents": cfg,
        "script_sha256": {n: file_sha256(os.path.join(HERE, n))
                          for n in ("orchestrate.py", "loadgen.py", "monitor.py")},
        "order_seed": order_seed,
        "options": {"duration": args.duration, "drain_timeout": args.drain_timeout,
                    "on_error": args.on_error, "max_sql_errors": args.max_sql_errors,
                    "strict_setpoint": args.strict_setpoint,
                    "setpoint_tol": args.setpoint_tol,
                    "load_grace": args.load_grace,
                    "min_free_gb": args.min_free_gb,
                    "data_volume": args.data_volume,
                    "run_suffix": args.run_suffix,
                    "out": os.path.abspath(args.out),
                    "loadgen": os.path.abspath(args.loadgen),
                    "drain_threshold_bytes": DRAIN_THRESHOLD_BYTES,
                    "drain_consecutive_polls": DRAIN_CONSECUTIVE,
                    "settle_after_drain_sec": SETTLE_AFTER_DRAIN,
                    "apply_rate_min_backlog_bytes": APPLY_RATE_MIN_BACKLOG_BYTES,
                    "apply_rate_min_catchup_sec": APPLY_RATE_MIN_CATCHUP_SEC,
                    "capacity_slope_bytes_per_sec": CAPACITY_SLOPE_BYTES_PER_SEC,
                    "capacity_min_backlog_bytes": CAPACITY_MIN_BACKLOG_BYTES},
        "resumed": bool(already_done),
        "results": [],
    }
    if already_done:
        man["results"] = [r for r in prev.get("results", []) if not r.get("failed")]
        # Keep the provenance of the attempt these levels actually came from.
        man["previous_attempts"] = (prev.get("previous_attempts") or []) + [{
            "started_utc": prev.get("started_utc"),
            "finished_utc": prev.get("finished_utc"),
            "abort": prev.get("abort"),
            "pg_version": prev.get("pg_version"),
            "level_ids": sorted(already_done),
        }]
        print(f"[orchestrate] resuming - {len(already_done)} levels already done")

    def save():
        with open(man_path, "w", encoding="utf-8") as fh:
            json.dump(man, fh, indent=2, default=str)
        write_report(man, rep_path)

    exit_code = 0
    try:
        log("=" * 70)
        log(f"RUN {run_id}   seed={args.seed}   on-error={args.on_error}")
        # Read before the pre-flight: the report and the validator both key
        # off pg_version, and a pre-flight abort used to leave it unset, which
        # crashed the report writer with KeyError inside the finally block.
        man["pg_version"] = q1(cur, "SELECT current_setting('server_version')")
        man["preflight"] = preflight(cur, args)
        man["pg_settings"] = capture_settings(cur)
        man["subscriber"] = subscriber_facts(args.sub_dsn)
        if not man["subscriber"].get("reachable"):
            log("  !! subscriber not captured - the manifest will be incomplete")
        else:
            nidx = len(man["subscriber"].get("indexes") or [])
            if nidx < 4:
                raise Abort("SUBSCRIBER_SCHEMA",
                            f"the subscriber has {nidx} indexes on ingest_data, "
                            f"expected 4. Apply cost is dominated by index "
                            f"maintenance; re-run 02_schema_subscriber.sql.")
        log(f"PostgreSQL {man['pg_version']} on {man['host']}")
        log(f"order: {' -> '.join(man['level_order'])}")
        log("=" * 70)
        save()

        for i, lv in enumerate(levels, 1):
            if lv["level_id"] in already_done:
                log(f"\n--- [{i}/{len(levels)}] {lv['level_id']} already complete - skipping")
                continue
            if os.path.exists(args.stop_file):
                raise Abort("OPERATOR_STOP", "stop file appeared between levels")
            try:
                man["results"].append(run_level(lv, args, cur, i, len(levels), log))
            except Abort as ab:
                if args.on_error == "abort":
                    raise
                log(f"  !! {ab.code}: {ab.message} (continuing by request)")
                man["results"].append({"level_id": lv["level_id"],
                                       "order_index": i, "failed": True,
                                       "error_code": ab.code,
                                       "error_message": ab.message,
                                       "params": lv})
            save()

    except Abort as ab:
        man["abort"] = {"code": ab.code, "message": ab.message,
                        "at_utc": datetime.now(timezone.utc).isoformat()}
        log(f"\n*** ABORTED [{ab.code}] {ab.message}")
        log("*** Fix the cause and re-run this family. Partial data is kept.")
        exit_code = 7 if man["results"] else 6
    except KeyboardInterrupt:
        man["abort"] = {"code": "INTERRUPTED", "message": "Ctrl-C",
                        "at_utc": datetime.now(timezone.utc).isoformat()}
        log("\n*** interrupted by user")
        exit_code = 7
    except Exception:
        # Anything else - a dropped connection to the publisher is the obvious
        # one over 24 unattended hours. This used to escape as a traceback with
        # exit 1, leaving a manifest with abort=null that the report rendered
        # as "Status: completed" and the validator passed.
        tb = traceback.format_exc()
        man["abort"] = {"code": "UNEXPECTED_ERROR",
                        "message": tb.strip().splitlines()[-1][:300],
                        "traceback": tb,
                        "at_utc": datetime.now(timezone.utc).isoformat()}
        log("\n*** ABORTED [UNEXPECTED_ERROR]\n" + tb)
        exit_code = 7 if man["results"] else 6
        # --on-error continue records a failed level and carries on, which
        # used to leave the exit code at 0. Anything driving the orchestrator
        # by exit status alone would file that run as a success.
        if exit_code == 0 and any(r.get("failed") for r in man["results"]):
            n_bad = len([r for r in man["results"] if r.get("failed")])
            log(f"\n*** {n_bad} level(s) failed but the run continued "
                f"(--on-error continue)")
            exit_code = 5
    finally:
        write_phase(args.phase_file, "idle", level_id="idle")
        man["finished_utc"] = datetime.now(timezone.utc).isoformat()
        save()
        cur.close(); conn.close()
        done = len([r for r in man["results"] if not r.get("failed")])
        log(f"\n[orchestrate] {run_id}: {done}/{len(levels)} levels completed")
        log(f"[orchestrate] manifest {man_path}")
        log(f"[orchestrate] report   {rep_path}")
        logfh.close()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
