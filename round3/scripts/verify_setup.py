#!/usr/bin/env python3
"""
Round 3 setup verification.  STANDALONE.

Run this before starting an unattended campaign. It checks everything that,
if wrong, would waste hours of machine time - and it can optionally run a short
end-to-end smoke test that actually pushes rows through replication.

Nothing here writes experiment data; it is safe to run at any time.

Exit codes
----------
  0  ready
  1  warnings only - usable but read them
  2  NOT ready - fix the failures first

Usage
-----
  set R3_PUB_DSN=host=localhost dbname=pub user=postgres password=...
  set R3_SUB_DSN=host=10.0.1.5 dbname=sub user=postgres password=...

  py verify_setup.py
  py verify_setup.py --smoke        # also run a 30 s end-to-end replication test
"""

import argparse
import csv
import glob
import json
import os
import subprocess
import shutil
import socket
import sys
import time
from datetime import datetime, timezone

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))

REQUIRED_FILES = [
    "01_schema_publisher.sql", "02_schema_subscriber.sql",
    "capture_environment.py", "monitor.py", "loadgen.py", "orchestrate.py",
    "validate_run.py", "supervisor.py", "check_campaign.py",
    "retarget_from_probe.py", "campaign.json",
    "matrix_A_calibration.json", "matrix_B_concurrency.json",
    "matrix_C_batching.json", "matrix_D_rowsize.json",
    "matrix_E_duration.json", "matrix_F_latency.json",
    "campaign_calibration.json", "campaign_latency.json",
    "campaign_probe.json", "matrix_P_probe.json",
]

# Columns the monitor, orchestrator and load generator actually read.
#
# This exists because PostgreSQL 18 removed wal_write_time and wal_sync_time
# from pg_stat_wal. Nothing in a connection test notices that: the server is
# up, replication is streaming, and every sample query fails. A campaign can
# run for a day and produce a file of empty rows. Checking the catalogue by
# name, before anything is started, turns that into a five-second failure.
CATALOG_NEEDS = {
    "pg_stat_wal": ["wal_records", "wal_fpi", "wal_bytes", "wal_buffers_full"],
    "pg_stat_database": ["xact_commit", "xact_rollback", "tup_inserted",
                         "tup_updated", "tup_deleted", "blks_read", "blks_hit",
                         "blk_read_time", "blk_write_time"],
    "pg_stat_replication": ["application_name", "state", "sent_lsn", "write_lsn",
                            "flush_lsn", "replay_lsn", "write_lag", "flush_lag",
                            "replay_lag"],
    "pg_replication_slots": ["slot_name", "slot_type", "active", "wal_status",
                             "restart_lsn", "safe_wal_size"],
    "pg_stat_activity": ["state", "backend_type", "application_name"],
}

SUB_CATALOG_NEEDS = {
    "pg_stat_database": ["xact_commit", "tup_inserted", "tup_updated",
                         "tup_deleted", "blks_read", "blks_hit", "blk_write_time"],
    "pg_stat_subscription": ["subname", "relid", "pid", "received_lsn",
                             "latest_end_lsn", "last_msg_send_time",
                             "last_msg_receipt_time"],
    "pg_subscription": ["subname", "subenabled", "substream"],
}

COLUMNS_SQL = """
SELECT attname FROM pg_attribute
WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped
"""

REQUIRED_SETTINGS = {
    "wal_level": "logical",
    "wal_compression": "off",
    "track_io_timing": "on",
    "track_wal_io_timing": "on",
}

results = []


def check(name, ok, detail="", fatal=True):
    results.append((name, bool(ok), detail, fatal))
    mark = "OK  " if ok else ("FAIL" if fatal else "WARN")
    print(f"  {mark}  {name:<38} {detail}")
    return bool(ok)


def bounded(dsn, statement_ms=15000, connect_timeout=10):
    """A connection that cannot hang.

    Every statement this script runs is a check, and a check that blocks
    forever is worse than one that fails: it tells you nothing and it looks
    like the script is broken. statement_timeout turns a lock wait into an
    error we can explain.
    """
    return psycopg2.connect(
        dsn, connect_timeout=connect_timeout,
        application_name="r3_verify",
        options=f"-c statement_timeout={int(statement_ms)}")


def blockers(dsn):
    """Who is holding the lock. Returned as text, best effort, never raises."""
    try:
        c = bounded(dsn, 5000)
        c.set_session(autocommit=True)
        k = c.cursor()
        k.execute("""
            SELECT pid, application_name, state,
                   COALESCE(EXTRACT(epoch FROM now() - xact_start), 0)::int,
                   left(regexp_replace(query, '\\s+', ' ', 'g'), 60)
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND (state <> 'idle' OR xact_start IS NOT NULL)
            ORDER BY xact_start NULLS LAST LIMIT 6""")
        rows = k.fetchall()
        k.close(); c.close()
    except Exception:
        return ""
    if not rows:
        return ""
    out = []
    for pid, app, state, age, sql in rows:
        out.append(f"      pid {pid} [{app or '-'}] {state} {age}s: {sql}")
    return "\n" + "\n".join(out)


def campaign_in_progress(pub_dsn, phase_file):
    """Is a run happening right now? The smoke test writes 5,000 rows into the
    live table, so running it during a campaign contaminates whichever level is
    in flight. Two independent signals, because either alone can be stale."""
    reasons = []
    try:
        import psutil
        me = os.getpid()
        for p in psutil.process_iter(["pid", "cmdline"]):
            if p.info["pid"] == me:
                continue
            cl = " ".join(p.info.get("cmdline") or [])
            for name in ("supervisor.py", "orchestrate.py", "loadgen.py"):
                if name in cl:
                    reasons.append(f"{name} is running as pid {p.info['pid']}")
                    break
    except Exception:
        pass
    try:
        r = q(pub_dsn, "SELECT count(*) FROM pg_stat_activity "
                       "WHERE application_name = 'r3_loadgen'")
        if r and r[0]:
            reasons.append(f"{r[0]} load generator connection(s) on the publisher")
    except Exception:
        pass
    try:
        with open(phase_file, encoding="utf-8") as fh:
            ph = json.load(fh)
        st = ph.get("phase_state")
        if st and st != "idle":
            age = None
            try:
                t = datetime.fromisoformat(ph["updated"])
                age = (datetime.now(timezone.utc) - t).total_seconds()
            except Exception:
                pass
            if age is None or age < 900:
                reasons.append(f"phase file says '{st}' on {ph.get('level_id')}")
    except Exception:
        pass
    return reasons


# Above this, the smoke test refuses to run rather than sequentially scanning
# a post-campaign table. It is also the threshold at which the table is plainly
# not in the clean state a campaign requires.
SMOKE_MAX_EXISTING_ROWS = 100_000


def table_size(dsn):
    """How many rows are in ingest_data, without ever hanging.

    Returns (rows, how) with rows=None meaning 'more than we are willing to
    wait for'. The planner estimate is instant but can be stale or -1 on a
    never-analysed table, so it is only used to decide whether an exact count
    is worth attempting - and that count is bounded either way.
    """
    est = -1
    try:
        r = q(dsn, "SELECT COALESCE(reltuples, -1)::bigint FROM pg_class "
                   "WHERE relname = 'ingest_data' AND relkind = 'r'",
              statement_ms=5000)
        if r and r[0] is not None:
            est = int(r[0])
    except Exception:
        pass
    if est > 50 * SMOKE_MAX_EXISTING_ROWS:
        # Far too big for the estimate to be wrong in a way that matters.
        return None, f"~{est:,} estimated"
    try:
        r = q(dsn, "SELECT count(*) FROM ingest_data", statement_ms=20000)
        return int(r[0]), "exact"
    except psycopg2.errors.QueryCanceled:
        return None, "count timed out"


def run_smoke(args):
    """Push 5,000 rows through replication and clean up after itself.

    Every statement here is bounded. The INSERT and the cleanup TRUNCATE used
    to run on plain connections with no statement_timeout, so anything holding
    a conflicting lock on ingest_data - a leftover reset_workload_table() from
    an aborted run is the classic one - hung this script forever with no
    output at all. Now it fails in 30 s and names what is in the way.
    """
    try:
        # Row estimates first. count(*) is a sequential scan, and after a
        # campaign ingest_data holds tens of millions of rows on both sides -
        # the orchestrator truncates at the START of each level, not the end,
        # so the last level's data is still there. Counting that was the first
        # statement in this section and it had no timeout, so the script sat
        # silent for minutes on what looked like a hang. reltuples costs
        # nothing and answers the only question that matters here.
        pub_n, pub_how = table_size(args.pub_dsn)
        sub_n, sub_how = table_size(args.sub_dsn)
        if pub_n is None or sub_n is None or max(pub_n, sub_n) > SMOKE_MAX_EXISTING_ROWS:
            def say_n(n, how):
                return "too large to count in 20 s" if n is None else f"{n:,} ({how})"
            check("ingest_data is clean enough to smoke test", False,
                  f"publisher {say_n(pub_n, pub_how)}, "
                  f"subscriber {say_n(sub_n, sub_how)}")
            print("        Archive the campaign data, then on the publisher run")
            print("        SELECT reset_workload_table();  VACUUM ANALYZE ingest_data;")
            print("        A campaign cannot start from a non-empty table either, "
                  "so this needs doing anyway.")
            return
        before = sub_n
        try:
            c = bounded(args.pub_dsn, 30000)
            c.set_session(autocommit=True)
            k = c.cursor()
            k.execute("""
                INSERT INTO ingest_data
                  (account_id, region_code, status, event_type, quantity,
                   unit_price, total_amount, external_ref, attributes, description)
                SELECT g, 'us-west', 'active', 'smoke.test', 1, 1.0, 1.0,
                       gen_random_uuid(), '{"smoke":true}'::jsonb, repeat('x', 500)
                FROM generate_series(1, 5000) g
            """)
            k.close(); c.close()
        except psycopg2.errors.QueryCanceled:
            check("smoke insert", False,
                  "the INSERT could not get a lock on ingest_data within 30 s."
                  + blockers(args.pub_dsn))
            return
        ok = False
        after = before
        for _ in range(30):
            time.sleep(1)
            after = q(args.sub_dsn, "SELECT count(*) FROM ingest_data")[0]
            if after - before >= 5000:
                ok = True
                break
        check("5,000 rows replicated", ok,
              f"{after - before} arrived" if not ok else "arrived within 30 s")

        lag = q(args.pub_dsn, """SELECT COALESCE(pg_wal_lsn_diff(
                 pg_current_wal_lsn(), replay_lsn),0)::bigint
                 FROM pg_stat_replication WHERE application_name='mysub' LIMIT 1""")
        check("backlog drained", bool(lag) and lag[0] < 1_048_576,
              f"{(lag[0] if lag else 0)/1e6:.2f} MB")

        w = q(args.pub_dsn, "SELECT wal_bytes::bigint FROM pg_stat_wal")
        check("pg_stat_wal.wal_bytes readable", bool(w) and w[0] > 0,
              f"{float(w[0])/1e6:.0f} MB generated since reset" if w else "NULL")

        try:
            c = bounded(args.pub_dsn, 60000)
            c.set_session(autocommit=True)
            k = c.cursor()
            k.execute("SELECT reset_workload_table()")
            k.execute("VACUUM ANALYZE ingest_data")
            k.close(); c.close()
            check("cleanup after smoke test", True, "table truncated")
        except psycopg2.errors.QueryCanceled:
            # This one matters more than the others: the 5,000 smoke rows are
            # still in the table, and the next run would not start clean.
            check("cleanup after smoke test", False,
                  "TRUNCATE blocked - 5,000 smoke rows are STILL in ingest_data. "
                  "Clear them before starting a campaign."
                  + blockers(args.pub_dsn))

        print("\n[7] monitor end-to-end (5 s)")
        smoke_monitor(args.pub_dsn, args.out)

    except Exception as exc:
        check("smoke test", False, f"{type(exc).__name__}: {str(exc)[:70]}")


def q(dsn, sql, timeout=10, statement_ms=15000):
    c = bounded(dsn, statement_ms, timeout)
    c.set_session(autocommit=True)
    k = c.cursor()
    k.execute(sql)
    r = k.fetchone()
    k.close()
    c.close()
    return r


def columns_of(dsn, relname):
    c = psycopg2.connect(dsn, connect_timeout=10)
    c.set_session(autocommit=True)
    k = c.cursor()
    k.execute(COLUMNS_SQL, (relname,))
    cols = {r[0] for r in k.fetchall()}
    k.close()
    c.close()
    return cols


def check_catalog(dsn, needs, role):
    """Confirm every column the scripts read actually exists on this server."""
    for rel, wanted in needs.items():
        try:
            have = columns_of(dsn, rel)
        except Exception as exc:
            check(f"{role} {rel} readable", False, f"{type(exc).__name__}: {str(exc)[:60]}")
            continue
        missing = [c for c in wanted if c not in have]
        check(f"{role} {rel} columns", not missing,
              "missing: " + ", ".join(missing) if missing else f"{len(wanted)} present")


def smoke_monitor(pub_dsn, out):
    """Run the real monitor for a few seconds and read what it produced.

    A column check proves the catalogue is right; this proves the monitor
    itself is right, which is the thing that has to survive unattended.
    """
    mon = os.path.join(HERE, "monitor.py")
    csv_path = os.path.join(out, "publisher_preflight.csv")
    ev_path = os.path.join(out, "publisher_preflight_events.log")
    for p in (csv_path, ev_path):
        try:
            os.remove(p)
        except OSError:
            pass
    cmd = [sys.executable, mon, "--role", "publisher", "--dsn", pub_dsn,
           "--out", out, "--run-id", "preflight", "--level-id", "preflight",
           "--duration", "5", "--no-phase-file",
           # The monitor now refuses to start beside another of its role. This
           # probe writes to its own run id ("preflight"), so it is the one
           # legitimate exception - and without this the check would fail
           # whenever the campaign monitor happens to be running.
           "--allow-duplicate",
           "--stop-file", os.path.join(out, ".preflight-stop-never")]
    try:
        subprocess.run(cmd, timeout=90, stdout=subprocess.DEVNULL,
                       stderr=subprocess.STDOUT)
    except Exception as exc:
        check("monitor ran", False, f"{type(exc).__name__}: {str(exc)[:60]}")
        return

    bad = []
    if os.path.isfile(ev_path):
        with open(ev_path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if "QUERY_ERROR" in line or "QUERY_UNSUPPORTED" in line \
                        or "DERIVED_RATE_ERROR" in line or "SAMPLE_EXCEPTION" in line:
                    bad.append(line.strip()[:100])
    check("monitor logged no query errors", not bad,
          bad[0] if bad else "clean")

    rows = []
    if os.path.isfile(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
    check("monitor wrote samples", len(rows) >= 3, f"{len(rows)} rows in 5 s")
    if rows:
        for col in ("wal_bytes", "wal_records", "xact_commit", "current_wal_lsn"):
            filled = sum(1 for r in rows if (r.get(col) or "").strip())
            check(f"monitor column {col}", filled == len(rows),
                  f"{filled}/{len(rows)} populated")
        got_lag = sum(1 for r in rows if (r.get("replay_lag_bytes") or "").strip())
        check("monitor sees replication lag column", got_lag == len(rows),
              f"{got_lag}/{len(rows)} populated", fatal=False)


def check_campaign_ready(pub_dsn, sub_dsn, out, camp_path, data_volume):
    """The things that only matter on the night, and only once.

    verify_setup used to stop at "the database is healthy". These are the
    checks whose absence costs a whole campaign rather than a level: a
    subscriber monitor nobody started, matrices still carrying placeholder
    targets, or not enough disk for the run you are about to launch.
    """
    # --- is the SUBSCRIBER monitor actually running? ----------------------
    # Answerable from here because every monitor tags its connection.
    if sub_dsn:
        try:
            r = q(sub_dsn, "SELECT count(*), max(backend_start)::text "
                           "FROM pg_stat_activity "
                           "WHERE application_name = 'r3_monitor_subscriber'")
            check("subscriber monitor running", bool(r) and r[0] > 0,
                  f"connected since {r[1][:19]}" if (r and r[0]) else
                  "NOT running - start it on the subscriber before the campaign, "
                  "or its half of the data is lost")
        except Exception as exc:
            check("subscriber monitor running", False,
                  f"could not check: {str(exc)[:50]}", fatal=False)

    # --- do the matrices still hold placeholder targets? ------------------
    stale, total_load = [], 0.0
    try:
        with open(camp_path) as fh:
            camp = json.load(fh)
        for fam in camp["families"]:
            mp = os.path.join(HERE, fam["config"])
            with open(mp) as fh:
                mat = json.load(fh)
            reps = len(fam["repeats"])
            for lv in mat["levels"]:
                total_load += (lv.get("duration_sec", 360) + 70) * reps
            if fam["family"] == "A_calibration":
                continue
            tg = {lv.get("target_wal_mbps") for lv in mat["levels"]}
            if not (tg - {None, 0}):
                continue
            # Every matrix must say where its numbers came from. The region
            # families carry rtt_provenance instead - their RTT is measured on
            # the deployed subscriber rather than derived from calibration, but
            # it is provenance all the same and must be present.
            if not (mat.get("retargeted_from_calibration")
                    or mat.get("retargeted_from_probe")
                    or mat.get("rtt_provenance")):
                stale.append(fam["family"])
        check("matrices retargeted from measured data", not stale,
              "still on placeholders: " + ", ".join(stale) if stale
              else "every family carries its provenance")
    except Exception as exc:
        check("matrices readable", False, str(exc)[:60])

    if total_load:
        print(f"  ..    estimated campaign length            "
              f"{total_load/3600:.1f} h")

    # --- room for the output ---------------------------------------------
    # Two 1 Hz CSVs plus logs run roughly 1 MB per minute of campaign, and
    # the workload table itself is truncated between levels, so the real
    # consumer is WAL and the heap during a level.
    try:
        import shutil
        free = shutil.disk_usage(data_volume).free / 1024**3
        need = 40
        check("room for the campaign", free > need,
              f"{free:,.0f} GB free, want more than {need} GB")
    except Exception:
        pass

    # --- leftovers that would confuse a resume ----------------------------
    # EVERY family, not just B-E. campaign.json includes A_calibration, so
    # "Stage full" re-runs calibration - and the monitor opens its CSV in
    # APPEND mode, so a second run of the same run id interleaves two runs in
    # one file while the manifest is overwritten outright. Leaving the earlier
    # calibration in place silently destroys it.
    # ...but only the ones THIS campaign would actually write over. After the
    # A-E campaign there are twelve validated manifests sitting in the data
    # directory, and family F touches none of them. Failing on those pushes
    # the operator to move finished, validated data out of the way for no
    # reason - which is a good way to lose it.
    old_runs = sorted(os.path.basename(p)[9:-5] for p in
                      glob.glob(os.path.join(out, "manifest_*_rep*.json")))
    planned = set()
    try:
        with open(camp_path) as fh:
            camp2 = json.load(fh)
        for fam in camp2["families"]:
            for rep in fam["repeats"]:
                planned.add(f"{fam['family']}_rep{rep}")
    except Exception:
        planned = None

    if planned is None:
        collide, other = old_runs, []
    else:
        collide = [r for r in old_runs
                   if any(r == p or r.startswith(p + "_") for p in planned)]
        other = [r for r in old_runs if r not in collide]

    check("no leftover run artefacts", not collide,
          (f"{len(collide)} run(s) THIS campaign would OVERWRITE or append to: "
           + ", ".join(collide[:4])
           + ". Move them to an archive folder first")
          if collide else
          (f"clean ({len(other)} unrelated run(s) present, untouched)"
           if other else "clean"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pub-dsn", default=os.environ.get("R3_PUB_DSN"))
    ap.add_argument("--sub-dsn", default=os.environ.get("R3_SUB_DSN"))
    ap.add_argument("--out", default=os.environ.get("R3_DATA", r"C:\r3\data"))
    ap.add_argument("--phase-file",
                    default=os.environ.get("R3_PHASE_FILE", r"C:\r3\phase_state.json"),
                    help="read only, to notice a campaign already in progress")
    ap.add_argument("--data-volume", default="F:\\" if os.name == "nt" else "/")
    ap.add_argument("--min-free-gb", type=float, default=100.0)
    ap.add_argument("--campaign", default="campaign.json",
                    help="the campaign you are about to run. Stage 3 uses "
                         "campaign_latency.json, and every section [8] check - "
                         "length, retargeting, and which artefacts would be "
                         "overwritten - depends on knowing which one it is.")
    ap.add_argument("--smoke", action="store_true",
                    help="push rows through replication and confirm they arrive")
    args = ap.parse_args()

    print(f"\nRound 3 setup verification — {socket.gethostname()}")
    print("=" * 66)

    # ---- 1. files -------------------------------------------------------
    print("\n[1] scripts and configuration")
    missing = [f for f in REQUIRED_FILES if not os.path.isfile(os.path.join(HERE, f))]
    check("all scripts present", not missing,
          "missing: " + ", ".join(missing) if missing else f"{len(REQUIRED_FILES)} files")

    camp = None
    camp_path = args.campaign
    if not os.path.isabs(camp_path):
        camp_path = os.path.join(HERE, camp_path)
    try:
        with open(camp_path) as fh:
            camp = json.load(fh)
        n = sum(len(f["repeats"]) for f in camp["families"])
        check(f"{os.path.basename(camp_path)} parses", True,
              f"{len(camp['families'])} families, {n} runs")
    except Exception as exc:
        check(f"{os.path.basename(camp_path)} parses", False, str(exc))

    # ---- 2. python ------------------------------------------------------
    print("\n[2] python environment")
    check("python >= 3.8", sys.version_info >= (3, 8), sys.version.split()[0])
    check("psycopg2", True, psycopg2.__version__)
    try:
        import psutil
        check("psutil", True, psutil.__version__)
    except ImportError:
        check("psutil", False, "py -m pip install psutil", fatal=False)

    # ---- 3. connections and settings ------------------------------------
    print("\n[3] publisher")
    if not args.pub_dsn:
        check("R3_PUB_DSN set", False, "export it or pass --pub-dsn")
    else:
        try:
            v = q(args.pub_dsn, "SELECT current_setting('server_version_num'), version()")
            major = int(v[0]) // 10000
            check("publisher reachable", True, v[1][:52])
            check("PostgreSQL 18", major == 18, f"found major {major}")
        except Exception as exc:
            check("publisher reachable", False, f"{type(exc).__name__}: {str(exc)[:70]}")
        else:
            for name, want in REQUIRED_SETTINGS.items():
                try:
                    got = q(args.pub_dsn, f"SELECT current_setting('{name}')")[0]
                    check(f"publisher {name} = {want}", got == want, f"got '{got}'",
                          fatal=(name == "wal_level"))
                except Exception:
                    check(f"publisher {name}", False, "could not read", fatal=False)
            try:
                t = q(args.pub_dsn, "SELECT to_regclass('public.ingest_data') IS NOT NULL")[0]
                check("table ingest_data exists", t, "run 01_schema_publisher.sql" if not t else "")
                ix = q(args.pub_dsn, "SELECT count(*) FROM pg_indexes "
                                     "WHERE tablename='ingest_data'")[0]
                check("indexes present", ix >= 4, f"{ix} indexes (expect 4)", fatal=False)
                fn = q(args.pub_dsn, "SELECT to_regproc('reset_workload_table') IS NOT NULL")[0]
                check("reset_workload_table()", fn, "" if fn else "re-run the schema script")
                pb = q(args.pub_dsn, "SELECT count(*) FROM pg_publication WHERE pubname='mypub'")[0]
                check("publication mypub", pb == 1)
                sl = q(args.pub_dsn, "SELECT wal_status FROM pg_replication_slots "
                                     "WHERE slot_type='logical' LIMIT 1")
                check("logical slot healthy", bool(sl) and sl[0] == "reserved",
                      sl[0] if sl else "no logical slot")
                rp = q(args.pub_dsn, "SELECT state FROM pg_stat_replication "
                                     "WHERE application_name='mysub' LIMIT 1")
                check("replication connected", bool(rp), rp[0] if rp else "no 'mysub' connection")
            except Exception as exc:
                check("publisher schema checks", False, str(exc)[:70])

            check_catalog(args.pub_dsn, CATALOG_NEEDS, "publisher")

    print("\n[4] subscriber")
    if not args.sub_dsn:
        check("R3_SUB_DSN set", False,
              "needed for health checks and auto-recovery", fatal=False)
    else:
        try:
            v = q(args.sub_dsn, "SELECT current_setting('server_version_num')")
            check("subscriber reachable", True, f"major {int(v[0])//10000}")
            check("subscriber is PostgreSQL 18", int(v[0]) // 10000 == 18)
            s = q(args.sub_dsn, "SELECT subname, subenabled, substream FROM pg_subscription LIMIT 1")
            check("subscription mysub", bool(s),
                  f"enabled={s[1]} streaming={s[2]}" if s else "not found")
            if s and not s[1]:
                check("subscription enabled", False, "ALTER SUBSCRIPTION mysub ENABLE")
            t = q(args.sub_dsn, "SELECT to_regclass('public.ingest_data') IS NOT NULL")[0]
            check("subscriber table exists", t)
            ix = q(args.sub_dsn, "SELECT count(*) FROM pg_indexes WHERE tablename='ingest_data'")[0]
            check("subscriber indexes", ix >= 4, f"{ix} indexes", fatal=False)
            check_catalog(args.sub_dsn, SUB_CATALOG_NEEDS, "subscriber")
        except Exception as exc:
            check("subscriber reachable", False, f"{type(exc).__name__}: {str(exc)[:70]}")

    # ---- 5. host --------------------------------------------------------
    print("\n[5] host")
    try:
        os.makedirs(args.out, exist_ok=True)
        p = os.path.join(args.out, ".writetest")
        with open(p, "w") as fh:
            fh.write("ok")
        os.remove(p)
        check("data directory writable", True, args.out)
    except Exception as exc:
        check("data directory writable", False, f"{args.out}: {exc}")

    fg = None
    try:
        fg = shutil.disk_usage(args.data_volume).free / 1024**3
        check("free space", fg >= args.min_free_gb,
              f"{fg:.0f} GB free on {args.data_volume} (want >= {args.min_free_gb:.0f})")
    except Exception:
        check("free space", False, f"cannot stat {args.data_volume}", fatal=False)

    stop = os.environ.get("R3_STOP_FILE", r"C:\r3\STOP")
    check("no stale stop file", not os.path.exists(stop),
          f"delete {stop}" if os.path.exists(stop) else stop)

    # A leftover confirmation would release family F's first latency gate
    # before clumsy had been touched. The orchestrator clears it per level, but
    # only once it is already running.
    lat = os.environ.get("R3_LATENCY_FILE", r"C:\r3\LATENCY_SET")
    check("no stale latency confirmation", not os.path.exists(lat),
          f"delete {lat}" if os.path.exists(lat) else lat)

    # ---- 6. smoke test --------------------------------------------------
    if args.smoke and args.pub_dsn and args.sub_dsn:
        print("\n[6] end-to-end smoke test")
        busy = campaign_in_progress(args.pub_dsn, args.phase_file)
        if busy:
            # The smoke test writes 5,000 rows into the live table. During a
            # run that is not a test, it is contamination - and the level it
            # lands in would still validate clean.
            check("safe to run the smoke test", False,
                  "a campaign appears to be RUNNING - smoke test skipped")
            for r in busy:
                print(f"        {r}")
            print("        Wait for it to finish, or stop it, then re-run.")
        else:
            run_smoke(args)
    elif args.smoke:
        print("\n[6] end-to-end smoke test — skipped, both DSNs required")

    print("\n[8] ready to start a campaign")
    check_campaign_ready(args.pub_dsn, args.sub_dsn, args.out,
                         camp_path, args.data_volume)

    # ---- verdict --------------------------------------------------------
    fails = [r for r in results if not r[1] and r[3]]
    warns = [r for r in results if not r[1] and not r[3]]
    print("\n" + "=" * 66)
    if fails:
        print(f"NOT READY — {len(fails)} failure(s):")
        for n, _, d, _ in fails:
            print(f"  x {n}  {d}")
        print("\nFix these before starting the campaign.")
        return 2
    if warns:
        print(f"READY, with {len(warns)} warning(s):")
        for n, _, d, _ in warns:
            print(f"  ! {n}  {d}")
        return 1
    print("READY — start the campaign with:")
    print("  py supervisor.py --campaign campaign.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
