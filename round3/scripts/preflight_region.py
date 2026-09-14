#!/usr/bin/env python3
"""
GO / NO-GO for one remote-region subscriber.  STANDALONE.  Read-only.
Run on the SUBSCRIBER, after bootstrap_subscriber.ps1 and after the scripts
folder is in place.

    py preflight_region.py --region eastus

It checks, in one pass, every property that has to hold before that region's
campaign is worth starting - and every one of them is here because getting it
wrong produces a complete, plausible, WRONG dataset rather than an error:

  * this machine and the publisher run the same PostgreSQL minor version
  * the table and its four indexes match the publisher exactly
  * the settings that change apply cost match the publisher's other subscribers
  * exactly one replication slot exists on the publisher, and it is this one
  * the subscription is named mysub, with the same options as every other
    region - the harness matches on application_name='mysub'
  * the publisher's table is empty, so copy_data=false was safe
  * both machines have enough free disk that the monitor will not raise
    DISK_LOW, which validate_run.py treats as a hard failure
  * the path carries far more than the offered load
  * the RTT, measured, which is the number the paper reports

Nothing here writes anything. Run it as often as you like.

Exit codes
----------
  0  GO
  1  NO-GO - at least one blocking check failed
  2  could not run the checks at all
"""

import argparse
import os
import statistics as st
import sys
import time

import psycopg2

TOOL_VERSION = "v1 2026-09-08"

DEFAULT_PUB = os.environ.get("R3_PUB_DSN", "")
DEFAULT_SUB = os.environ.get("R3_SUB_DSN", "")

TARGET_MBPS = 6.0
THROUGHPUT_HEADROOM = 4.0      # want >= 4x the offered load
THROUGHPUT_SECONDS = 20.0
RTT_SAMPLES = 200
RTT_WARMUP = 5

# monitor.py raises DISK_LOW below 25 GB, and validate_run.py counts a
# DISK_LOW event as a hard PROBLEM. A run that trips it is thrown away.
DISK_LOW_GB = 25.0


def azure_instance_metadata():
    """Read this VM's own size, region and zone from Azure IMDS.

    IMDS is a link-local endpoint (169.254.169.254) present on every Azure
    VM, so this needs no credentials and no outbound network. It is here so
    the make_region_family.py command printed at the end carries the REAL
    VM size rather than leaving the author to type it from memory - the four
    subscribers are not all the same size, and a wrong size in the hardware
    record is worse than no record at all.

    Returns {} on anything unexpected. Never raises: a metadata endpoint
    that does not answer must not turn a GO into a crash.
    """
    try:
        import json as _json
        import urllib.request as _u
        req = _u.Request(
            "http://169.254.169.254/metadata/instance/compute"
            "?api-version=2021-02-01",
            headers={"Metadata": "true"})
        with _u.urlopen(req, timeout=3) as fh:
            c = _json.loads(fh.read().decode("utf-8", "replace"))
        return {
            "vm_size": c.get("vmSize") or "",
            "region": c.get("location") or "",
            "zone": c.get("zone") or "",
            "vm_name": c.get("name") or "",
        }
    except Exception:
        return {}
DISK_WANT_GB = 100.0

# Settings that change how much work the apply worker does. If these differ
# between subscribers, apply cost differs for a reason that is not distance.
SUB_SETTINGS = [
    "shared_buffers", "effective_cache_size", "work_mem",
    "maintenance_work_mem", "max_wal_size", "min_wal_size",
    "checkpoint_timeout", "synchronous_commit", "wal_level",
    "max_logical_replication_workers", "max_worker_processes",
    "max_parallel_apply_workers_per_subscription",
    "max_sync_workers_per_subscription", "track_io_timing",
    "track_wal_io_timing", "full_page_writes", "wal_compression",
]

results = []


def note(ok, name, detail, blocking=True):
    results.append((ok, name, detail, blocking))
    if ok:
        tag, col = "OK  ", ""
    elif blocking:
        tag, col = "FAIL", ""
    else:
        tag, col = "warn", ""
    print(f"  {tag}  {name:<34} {detail}")
    return ok


def connect(dsn, name):
    c = psycopg2.connect(dsn, connect_timeout=20,
                         application_name="r3_preflight")
    c.set_session(autocommit=True, readonly=True)
    return c


def one(conn, sql, args=None):
    k = conn.cursor()
    k.execute(sql, args or ())
    r = k.fetchone()
    k.close()
    return r


def allrows(conn, sql, args=None):
    k = conn.cursor()
    k.execute(sql, args or ())
    r = k.fetchall()
    k.close()
    return r


def measure_rtt(dsn):
    c = psycopg2.connect(dsn, connect_timeout=20, application_name="r3_preflight_rtt")
    c.set_session(autocommit=True)
    k = c.cursor()
    t = []
    for i in range(RTT_SAMPLES + RTT_WARMUP):
        t0 = time.perf_counter()
        k.execute("SELECT 1")
        k.fetchone()
        if i >= RTT_WARMUP:
            t.append((time.perf_counter() - t0) * 1000.0)
    k.close()
    c.close()
    return t


def measure_throughput(dsn, seconds):
    """Second-half rate of a COPY stream, in MB/s. Mirrors check_throughput.py:
    a fixed DURATION, not a fixed size - at 200 ms RTT a fixed-size transfer
    measures TCP slow start and nothing else."""
    import io

    class Stop(Exception):
        pass

    class Sink(io.RawIOBase):
        def __init__(self):
            self.n = 0
            self.t0 = None
            self.s = []
            self.nxt = 0.25

        def writable(self):
            return True

        def write(self, b):
            now = time.perf_counter()
            if self.t0 is None:
                self.t0 = now
            self.n += len(b)
            el = now - self.t0
            if el >= self.nxt:
                self.nxt += 0.25
                self.s.append((el, self.n))
            if el >= seconds:
                self.s.append((el, self.n))
                raise Stop
            return len(b)

    sink = Sink()
    c = psycopg2.connect(dsn, connect_timeout=20, application_name="r3_preflight_tput")
    c.set_session(autocommit=True)
    k = c.cursor()
    try:
        k.copy_expert("COPY (SELECT repeat('x',1000) FROM generate_series(1,20000000)) TO STDOUT",
                      sink)
    except Stop:
        pass
    except Exception:
        pass
    finally:
        try:
            c.close()
        except Exception:
            pass
    if len(sink.s) < 4:
        return None
    cut = sink.s[-1][0] / 2.0
    tail = [x for x in sink.s if x[0] >= cut]
    if len(tail) < 2 or tail[-1][0] <= tail[0][0]:
        return None
    return (tail[-1][1] - tail[0][1]) / (tail[-1][0] - tail[0][0]) / 1048576.0


def main():
    ap = argparse.ArgumentParser(
        description="GO/NO-GO check for one region's subscriber (run on the subscriber).")
    ap.add_argument("--region", required=True,
                    help="short name, e.g. eastus - only used for the printed command")
    ap.add_argument("--pub-dsn", default=DEFAULT_PUB)
    ap.add_argument("--sub-dsn", default=DEFAULT_SUB)
    ap.add_argument("--target-mbps", type=float, default=TARGET_MBPS)
    ap.add_argument("--skip-throughput", action="store_true",
                    help="skip the 20 s COPY stream (it moves real bytes and, "
                         "across regions, real money)")
    args = ap.parse_args()

    print("=" * 78)
    print(f"PREFLIGHT - {args.region}    {TOOL_VERSION}")
    print("=" * 78)

    if not args.pub_dsn or not args.sub_dsn:
        print("\nFAIL: set R3_PUB_DSN and R3_SUB_DSN, or pass --pub-dsn/--sub-dsn.")
        print("      Open a NEW PowerShell window if bootstrap just set them.")
        return 2

    try:
        pub = connect(args.pub_dsn, "publisher")
    except Exception as exc:
        print(f"\nFAIL: cannot reach the publisher - {type(exc).__name__}: {str(exc)[:150]}")
        print("\n  Work through these in order:")
        print("    1. az network vnet peering list ... both directions say Connected")
        print("    2. the publisher's NSG allows 5432 from this subnet")
        print("    3. the publisher's Windows Firewall allows 5432 from this subnet")
        print("    4. the publisher's pg_hba.conf has a line for this subnet")
        return 2
    try:
        sub = connect(args.sub_dsn, "subscriber")
    except Exception as exc:
        print(f"\nFAIL: cannot reach this machine's own PostgreSQL - "
              f"{type(exc).__name__}: {str(exc)[:150]}")
        return 2

    # ---- 1. versions ----------------------------------------------------
    print("\n[1] the two servers")
    pv = one(pub, "SHOW server_version")[0]
    sv = one(sub, "SHOW server_version")[0]
    note(pv == sv, "PostgreSQL versions match",
         f"publisher {pv} / subscriber {sv}"
         + ("" if pv == sv else "  <- a minor-version difference is not distance"))
    note(sv.startswith("18"), "subscriber is PostgreSQL 18", sv)

    # ---- 2. schema parity -----------------------------------------------
    print("\n[2] schema parity with the publisher")
    colsql = ("SELECT column_name||':'||data_type FROM information_schema.columns "
              "WHERE table_name='ingest_data' ORDER BY ordinal_position")
    pc = [r[0] for r in allrows(pub, colsql)]
    sc = [r[0] for r in allrows(sub, colsql)]
    note(bool(sc), "subscriber table exists", f"{len(sc)} columns")
    if pc != sc:
        extra = set(sc) ^ set(pc)
        note(False, "columns match the publisher",
             f"differ: {sorted(extra)[:4]}")
    else:
        note(True, "columns match the publisher", f"{len(pc)} columns identical")
    pi = one(pub, "SELECT count(*) FROM pg_indexes WHERE tablename='ingest_data'")[0]
    si = one(sub, "SELECT count(*) FROM pg_indexes WHERE tablename='ingest_data'")[0]
    note(pi == si == 4, "index count matches", f"publisher {pi} / subscriber {si} (want 4)")

    # ---- 3. settings parity ---------------------------------------------
    print("\n[3] settings that change apply cost")
    diffs = []
    for s in SUB_SETTINGS:
        try:
            a = one(pub, "SELECT setting||coalesce(unit,'') FROM pg_settings WHERE name=%s", (s,))
            b = one(sub, "SELECT setting||coalesce(unit,'') FROM pg_settings WHERE name=%s", (s,))
        except Exception:
            continue
        if a and b and a[0] != b[0]:
            diffs.append(f"{s}: pub={a[0]} sub={b[0]}")
    # shared_buffers and the cache estimate SHOULD differ from the publisher if
    # the machines differ in size; what must not differ is subscriber-to-
    # subscriber. Report rather than block.
    note(True, "compared against publisher",
         f"{len(diffs)} difference(s)" if diffs else "identical")
    for d in diffs:
        print(f"        - {d}")
    if diffs:
        print("        Differences from the PUBLISHER are fine and expected.")
        print("        What matters is that they are the SAME on every SUBSCRIBER.")
        print("        Run this on each region and compare these lines.")

    # ---- 4. subscription -------------------------------------------------
    print("\n[4] the subscription")
    subs = allrows(sub, "SELECT subname, subenabled, substream::text, subbinary, "
                        "subsynccommit, subslotname FROM pg_subscription")
    # NO subscription is the CORRECT state before a region runs.
    #
    # run_region.py phase [4] drops every subscription it can reach and phase
    # [5] truncates both tables; phase [6] then creates this region's
    # subscription fresh, which is what makes copy_data = false honest. This
    # check used to demand one already existed - a leftover from when they were
    # created by hand - so a correctly prepared machine reported NO-GO and the
    # only way past it was to skip the preflight, losing the apply-throughput
    # measurement that defends the hardware differences.
    if not subs:
        note(True, "subscription", "none - correct. run_region.py phase [6] "
                                   "creates it fresh for this region.")
    elif len(subs) > 1:
        note(False, "subscriptions here", f"{len(subs)}: " +
             ", ".join(s[0] for s in subs) + " - the harness assumes one")
    else:
        note(True, "exactly one subscription here", subs[0][0])
    if subs:
        nm, en, strm, binr, sc_, slot = subs[0]
        note(nm == "mysub", "named 'mysub'",
             nm + ("" if nm == "mysub" else "  <- monitor.py, orchestrate.py, "
                                            "supervisor.py and verify_setup.py all "
                                            "match on application_name='mysub'"))
        note(bool(en), "enabled", str(en))
        note(strm in ("f", "off", "false"), "streaming = off", strm)
        note(not binr, "binary = false", str(binr))
        note(sc_ in ("off", "f", "false"), "synchronous_commit = off", str(sc_))

    if subs:
        st_ = allrows(sub, "SELECT subname, received_lsn IS NOT NULL FROM "
                           "pg_stat_subscription WHERE relid IS NULL")
        note(bool(st_) and st_[0][1], "apply worker running",
             "receiving" if st_ and st_[0][1] else "no leader apply worker")

    # ---- 5. the publisher's side ----------------------------------------
    print("\n[5] the publisher's side")
    slots = allrows(pub, "SELECT slot_name, active, wal_status, "
                         "pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)/1048576.0 "
                         "FROM pg_replication_slots WHERE slot_type='logical'")
    note(len(slots) == 1, "exactly one logical slot",
         "; ".join(f"{s[0]} active={s[1]} retaining {s[3]:.0f} MB" for s in slots)
         if slots else "none - the subscription is not connected"
         )
    if len(slots) > 1:
        print("        monitor.py's slot query is 'WHERE slot_type=\"logical\" LIMIT 1'")
        print("        with no ORDER BY: with two slots it samples an arbitrary one,")
        print("        and the publisher is feeding two standbys, so the offered load")
        print("        is not what the matrix says. DROP the other subscription.")

    reps = allrows(pub, "SELECT application_name, state FROM pg_stat_replication")
    seen = "; ".join(f"{r[0]}/{r[1]}" for r in reps) if reps else "none"
    if subs:
        # This machine has a subscription, so exactly one walsender - its own.
        note(len(reps) == 1 and reps[0][0] == "mysub",
             "one walsender, named mysub", seen)
    elif len(reps) <= 1:
        # No subscription here yet. Whatever walsender exists belongs to
        # another subscriber - usually the control - and run_region.py phase
        # [4] drops it before this region's own is created. Requiring one
        # here made a correctly prepared machine report NO-GO.
        note(True, "walsenders on the publisher",
             f"{seen} - not this machine's; phase [4] clears it")
    else:
        note(False, "walsenders on the publisher",
             f"{len(reps)}: {seen} - more than one standby is being fed")

    # A non-empty publisher table is only dangerous BEFORE a subscription is
    # created here, because CREATE SUBSCRIPTION defaults to copy_data=true and
    # would drag the whole table across the region boundary. Once this machine
    # has its subscription, the risk has passed and rows are just leftovers -
    # the campaign truncates before every level anyway.
    # BOUNDED. An unbounded count(*) here hung this preflight for over half an
    # hour on every subscriber in turn: after families B-E the publisher's
    # table still holds the last level's rows - tens of millions - and the
    # count is a sequential scan of the whole heap. The number is only used to
    # warn about copy_data, so an estimate is enough, and a slow exact count is
    # never worth blocking on.
    # SET, not SET LOCAL. SET LOCAL applies only inside a transaction, and on
    # an autocommit connection every statement is its own transaction - so the
    # timeout was discarded before the count ran and bounded nothing at all.
    n, how = None, "exact"
    try:
        with pub.cursor() as _c:
            _c.execute("SET statement_timeout = 8000")
            try:
                _c.execute("SELECT count(*) FROM ingest_data")
                n = _c.fetchone()[0]
            finally:
                try:
                    _c.execute("SET statement_timeout = 0")
                except Exception:
                    pass
    except Exception:
        try:
            pub.rollback()
        except Exception:
            pass
        try:
            r = one(pub, "SELECT COALESCE(reltuples,-1)::bigint FROM pg_class "
                         "WHERE relname='ingest_data' AND relkind='r'")
            n = int(r[0]) if r and r[0] is not None else -1
            how = "planner estimate"
        except Exception:
            n, how = -1, "unknown"
    if n is None or n < 0:
        note(True, "publisher table", "could not be counted in 8 s - too large "
                                      "to matter; phase [5] truncates it",
             blocking=False)
        n = 0
        how = "uncounted"
    if how == "uncounted":
        pass
    elif subs:
        note(True, "publisher table", f"{n:,} rows ({how}; subscription "
                                      f"exists, so copy_data is not a risk)")
    elif n == 0:
        note(True, "publisher table", "empty")
    else:
        # Blocking here was right when subscriptions were created by hand -
        # CREATE SUBSCRIPTION defaults to copy_data=true and would drag the
        # table across the region boundary. run_region.py does not work that
        # way: phase [5] truncates BOTH sides while no subscription exists,
        # and only then does phase [6] create one with copy_data=false. After
        # families B-E the publisher's table holds the last level's rows, so
        # blocking on this stopped every region from being prepared.
        note(True, "publisher table", f"{n:,} rows - run_region.py phase [5] "
                                      f"truncates both sides before it creates "
                                      f"the subscription", blocking=False)
        print("        Only create a subscription BY HAND on an empty table -")
        print("        copy_data defaults to true and would copy all of these.")

    # ---- 6. disk ---------------------------------------------------------
    print("\n[6] free disk (monitor raises DISK_LOW below 25 GB, and")
    print("    validate_run.py counts a DISK_LOW event as a hard failure)")
    try:
        import psutil
        vol = "F:\\" if os.name == "nt" else "/"
        free = psutil.disk_usage(vol).free / 1024 ** 3
        if free <= DISK_LOW_GB:
            note(False, f"this machine {vol}",
                 f"{free:.0f} GB free  <- below the {DISK_LOW_GB:.0f} GB DISK_LOW "
                 f"threshold; every run would fail validation")
        elif free < DISK_WANT_GB:
            note(False, f"this machine {vol}",
                 f"{free:.0f} GB free  <- above the {DISK_LOW_GB:.0f} GB floor but "
                 f"under {DISK_WANT_GB:.0f} GB; watch it during the campaign",
                 blocking=False)
        else:
            note(True, f"this machine {vol}", f"{free:.0f} GB free")
    except Exception as exc:
        note(False, "this machine's free disk", f"could not read: {exc}", blocking=False)
    print("    Check the PUBLISHER's F: separately - its monitor raises the same")
    print("    event, and one DISK_LOW there fails every run of this campaign.")

    # ---- 7. the path ------------------------------------------------------
    print("\n[7] the path to the publisher")
    try:
        t = measure_rtt(args.pub_dsn)
    except Exception as exc:
        print(f"  FAIL  could not measure RTT - {exc}")
        return 2
    t.sort()
    med = st.median(t)
    lo, hi = t[len(t) // 4], t[min(len(t) - 1, (3 * len(t)) // 4)]
    note(True, "measured median RTT", f"{med:.1f} ms   (IQR {lo:.1f} - {hi:.1f})")
    note(hi - lo <= max(2.0, 0.15 * med), "RTT is stable",
         f"IQR spread {hi - lo:.1f} ms", blocking=False)

    tp = None
    if args.skip_throughput:
        note(True, "throughput", "skipped (--skip-throughput)", blocking=False)
    else:
        print(f"        streaming for {THROUGHPUT_SECONDS:.0f} s to measure sustained rate ...")
        try:
            tp = measure_throughput(args.pub_dsn, THROUGHPUT_SECONDS)
        except Exception as exc:
            print(f"        could not measure: {exc}")
        want = args.target_mbps * THROUGHPUT_HEADROOM
        if tp is None:
            note(False, "sustained throughput", "could not measure", blocking=False)
        else:
            note(tp >= want, "sustained throughput",
                 f"{tp:.1f} MB/s   (want >= {want:.0f} = {THROUGHPUT_HEADROOM:.0f}x "
                 f"the {args.target_mbps:.0f} MB/s offered load)")
            if tp < want:
                print("        This is the check that was never done for clumsy, and")
                print("        skipping it is what broke that attempt. If a region")
                print("        cannot carry the load with headroom, LOWER THE TARGET")
                print("        FOR EVERY REGION - the offered load has to be identical")
                print("        at every point or the comparison is not of distance.")

    # ---- verdict ----------------------------------------------------------
    blocking_fails = [r for r in results if not r[0] and r[3]]
    warns = [r for r in results if not r[0] and not r[3]]
    print()
    print("=" * 78)
    if blocking_fails:
        print(f"NO-GO - {len(blocking_fails)} blocking problem(s):")
        for _, name, detail, _ in blocking_fails:
            print(f"   x {name}: {detail}")
        print("=" * 78)
        return 1

    print("GO")
    if warns:
        print(f"  ({len(warns)} non-blocking warning(s) above)")
    print("=" * 78)
    print()
    print("  Next, ON THE PUBLISHER:")
    meta = azure_instance_metadata()
    size = meta.get("vm_size") or "PUT-THE-REAL-VM-SIZE-HERE"
    cmd = (f"      py make_region_family.py --region {args.region} "
           f"--rtt {med:.1f} --vm-size {size}")
    if tp is not None:
        cmd += f" --apply-mbps {tp:.1f}"
    print(cmd)
    if not meta.get("vm_size"):
        print("      (could not read this VM's size from Azure instance metadata -")
        print("       fill it in from 'bash hw_inventory.sh' before running)")
    if tp is None:
        print("      (throughput was not measured, so --apply-mbps is missing. That")
        print("       number is what defends the VM-size difference between regions;")
        print("       re-run this preflight without --skip-throughput to get it.)")
    if meta.get("region") and meta["region"] != args.region:
        print(f"      NOTE: this VM reports Azure region '{meta['region']}' but you")
        print(f"      passed --region {args.region}. Use the real one, or the family")
        print("      file will be labelled with a region the machine is not in.")
    print()
    print("  Then back here, in two windows, RELAY FIRST - it is what writes")
    print("  campaign_state.json on this machine, and until it exists the")
    print("  monitor labels every sample 'pending' and the apply-side series")
    print("  cannot be matched to a run:")
    print("      py state_relay.py --once     (tests permissions, then exits)")
    print("      py state_relay.py")
    print("      .\\start_subscriber_monitor.ps1")
    print()
    print("  Then on the PUBLISHER - run_region.py, not start_campaign.ps1.")
    print("  start_campaign runs the campaign and stops there; run_region also")
    print("  refuses to start if something is already running, drops every")
    print("  other region's subscription so only this one is fed, collects")
    print("  this machine's CSVs back over 5432, and re-validates with both")
    print("  halves present:")
    print(f"      py run_region.py --region {args.region} --plan")
    print(f"      py run_region.py --region {args.region}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
