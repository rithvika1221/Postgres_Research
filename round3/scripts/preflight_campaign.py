#!/usr/bin/env python3
"""
Round 3 - ONE GO/NO-GO before starting a multi-hour campaign. Run on the
PUBLISHER.

    py preflight_campaign.py --stage calibration
    py preflight_campaign.py --stage bcde
    py preflight_campaign.py --stage bcde --region centralus

Changes nothing. Every check is a read.

WHY THIS EXISTS
---------------
A twelve-hour unattended campaign has exactly one expensive failure mode:
something that could have been seen in two seconds beforehand is discovered at
hour nine. verify_setup.py checks the database side; this checks the whole
thing, including the parts that were each found the hard way -

  * both servers against the study's own specification, because the control
    subscriber was found running stock defaults with nothing reporting it
  * reset_workload_table() existing, because orchestrate.py calls it before
    EVERY level and a missing function fails all of them identically
  * prior run artefacts still in the data directory, because run ids are
    deterministic and the monitor APPENDS to the previous attempt's event log,
    after which validate_run.py judges tonight against last month
  * clock offset between the two machines, because that is what produced
    Round 2's negative lag samples
  * a monitor already connected, because monitor.py exits 4 rather than
    append to another monitor's CSV, and the supervisor then has no publisher
    samples at all
  * free space on BOTH data volumes, because one DISK_LOW event is a hard
    validation failure for the run it lands in

It finishes by printing what will actually run - every family, every target
rate, every duration - and the wall-clock time it expects to finish, so the
numbers about to be committed to are visible before the commitment.

EXIT CODES
----------
  0  GO
  1  NO-GO, at least one blocking problem
  2  bad arguments, or the publisher could not be reached
"""

import argparse
import glob
import json
import os
import re
import socket
import statistics
import subprocess
import sys
import time

try:
    import psycopg2
except ImportError:
    print("psycopg2 is not installed.  py -m pip install psycopg2-binary")
    raise SystemExit(2)

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""

# monitor.py raises DISK_LOW below this, and validate_run.py counts one such
# event as a hard failure for that run.
DISK_LOW_GB = 25.0
# Enough headroom that a 30-minute level at the highest target cannot reach
# the floor. E05 at 44 MB/s writes roughly 79 GB of WAL plus a comparable
# heap and index footprint.
DISK_WANT_GB = 250.0
CLOCK_WARN_MS = 100.0
CLOCK_FAIL_MS = 1000.0

# Printed in the banner. There is no way to tell two builds of a script apart
# by eye, and an unzip that prompts per-file and is answered "skip" leaves the
# old one in place looking identical. This makes the running version visible.
BUILD = "2026-09-12 20:05 UTC"
CHECK_GROUPS = 12

STAGES = {
    "calibration": "campaign_calibration.json",
    "probe": "campaign_probe.json",
    "decode": "campaign_decode.json",
    "bcde": "campaign_bcde.json",
    "full": "campaign.json",
}

_fail = []
_warn = []


def head(t):
    print(f"\n{B}{t}{Z}")


def ok(n, d=""):
    print(f"  {G}OK{Z}    {n:<36} {d}")


def bad(n, d=""):
    print(f"  {R}FAIL{Z}  {n:<36} {d}")
    _fail.append((n, d))


def warn(n, d=""):
    print(f"  {Y}WARN{Z}  {n:<36} {d}")
    _warn.append((n, d))


def info(n, d=""):
    print(f"        {n:<36} {d}")


def redact(s):
    return re.sub(r"password=\S+", "password=***", s or "")


def connect(dsn, name):
    c = psycopg2.connect(dsn, connect_timeout=20, application_name=name)
    c.autocommit = True
    return c


def one(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        r = cur.fetchone()
    return r


def rows(c, sql, args=None):
    with c.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


# ---------------------------------------------------------------------------
def check_scripts(stage):
    head("[1] Scripts, campaign files and Python packages")
    # start_campaign.ps1 runs verify_setup.py first, and verify_setup exits 2
    # if anything in its own REQUIRED_FILES list is absent - which stops the
    # campaign before it starts. Import that list rather than keeping a second
    # copy, so this check cannot disagree with the thing that actually gates
    # the run. 02_schema_subscriber.sql is in it, for instance: it is a
    # subscriber file, but verify_setup expects a copy beside the publisher
    # scripts, and an incomplete unzip is exactly how it goes missing.
    need = ["supervisor.py", "orchestrate.py", "loadgen.py", "monitor.py",
            "validate_run.py", "verify_setup.py", "capture_environment.py",
            "pg_spec.py", "check_config.py", "archive_prior_runs.py"]
    try:
        import verify_setup
        need = sorted(set(need) | set(verify_setup.REQUIRED_FILES))
        src = "including verify_setup.py's own REQUIRED_FILES"
    except Exception:
        src = "(could not read verify_setup.REQUIRED_FILES)"
    missing = [n for n in need if not os.path.isfile(os.path.join(HERE, n))]
    if missing:
        bad("scripts", f"missing from {HERE}: {', '.join(missing)}")
        info("", "verify_setup.py exits 2 on this and start_campaign.ps1 "
                 "then refuses to start")
    else:
        ok("scripts", f"all {len(need)} present, {src}")

    camp = STAGES[stage]
    cpath = os.path.join(HERE, camp)
    if not os.path.isfile(cpath):
        bad("campaign file", f"{camp} not found")
        return None
    try:
        with open(cpath, encoding="utf-8") as fh:
            c = json.load(fh)
    except Exception as exc:
        bad("campaign file", f"{camp} is not valid JSON: {exc}")
        return None
    ok("campaign file", f"{camp}  ({c.get('name')})")

    for mod, label in (("psutil", "host counters, and the DISK_LOW check"),
                       ("psycopg2", "every database connection")):
        try:
            __import__(mod)
            ok(f"python: {mod}", label)
        except ImportError:
            bad(f"python: {mod}", f"not installed - needed for {label}")

    if not os.access(HERE, os.W_OK):
        bad("scripts folder", f"{HERE} is not writable by this account")
    return c


def check_env():
    head("[2] Environment")
    pub = os.environ.get("R3_PUB_DSN")
    sub = os.environ.get("R3_SUB_DSN")
    data = os.environ.get("R3_DATA") or r"C:\r3\data"
    root = os.path.dirname(data.rstrip("\\/")) or r"C:\r3"

    if not pub:
        bad("R3_PUB_DSN", "not set")
    else:
        ok("R3_PUB_DSN", redact(pub))
    if not sub:
        bad("R3_SUB_DSN", "not set - the orchestrator cannot verify a clean "
                          "start or record the subscriber")
    else:
        ok("R3_SUB_DSN", redact(sub))
    if os.path.isdir(data):
        ok("R3_DATA", data)
    else:
        bad("R3_DATA", f"{data} does not exist")

    stop = os.environ.get("R3_STOP_FILE") or os.path.join(root, "STOP")
    if os.path.exists(stop):
        bad("STOP file", f"{stop} exists - the campaign would stop after its "
                         f"first level. Delete it.")
    else:
        ok("STOP file", f"absent ({stop})")

    # verify_setup.py has a FATAL gate on this one and nothing else did. It is
    # family F's per-level "clumsy has been set" handshake; a leftover file
    # releases the first level's latency gate before anything has been
    # touched, and verify_setup exits 2 rather than let that happen - which
    # stops the campaign starting whether or not family F is in it.
    lat = os.environ.get("R3_LATENCY_FILE") or os.path.join(root,
                                                            "LATENCY_SET")
    if os.path.exists(lat):
        bad("LATENCY_SET file", f"{lat} exists - verify_setup.py fails on a "
                                f"stale latency confirmation and "
                                f"start_campaign.ps1 then starts nothing. "
                                f"Delete it.")
    else:
        ok("LATENCY_SET file", f"absent ({lat})")
    info("hostname", socket.gethostname())
    return pub, sub, data, root


def check_config_spec(pubc, subc):
    head("[3] Both servers against postgresql_settings.md")
    try:
        import check_config
        import pg_spec
        spec_all, src, applied = pg_spec.load()
        pg_spec.announce(src, applied, printer=lambda m: print("  " + m))
    except SystemExit as exc:
        bad("config audit", str(exc))
        return
    except Exception as exc:
        bad("config audit", f"could not load the specification: {exc}")
        return

    for role, conn, label in (("publisher", pubc, "publisher"),
                              ("subscriber", subc, "subscriber")):
        if conn is None:
            bad(f"{label} audit", "not connected")
            continue
        try:
            res = check_config.audit(conn, role, spec_all[role])
        except Exception as exc:
            bad(f"{label} audit", str(exc).strip().splitlines()[0])
            continue
        diffs = [r for r in res if not r[4]]
        blocking = [r for r in diffs if r[3] in ("physics", "monitor")]
        if not diffs:
            ok(label, f"all {len(res)} specified settings match")
            continue
        for name, exp, act, sev, _m, ctx, _n in diffs:
            line = f"{name} = {act}, specified {exp}"
            if ctx == "postmaster":
                line += "  (restart)"
            (bad if sev in ("physics", "monitor") else warn)(label, line)
        if blocking:
            info("", f"py check_config.py --role {role} --fix")


def check_clock(pubc, subc):
    """Clock skew between the two machines.

    Round 2 produced negative lag samples, and the cause was clock offset. The
    publisher and subscriber CSVs are timestamped by their own machines, so
    aligning them during analysis assumes the two clocks agree. This script
    runs ON the publisher, so the local clock IS the publisher's; the offset
    measured against the subscriber is the skew.
    """
    head("[4] Clock agreement between the machines")
    if subc is None:
        bad("clock", "no subscriber connection")
        return
    offsets = []
    try:
        with subc.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
            for _ in range(7):
                t0 = time.time()
                m0 = time.perf_counter()
                cur.execute("SELECT (extract(epoch from clock_timestamp()))")
                srv = float(cur.fetchone()[0])
                rtt = time.perf_counter() - m0
                local_mid = t0 + rtt / 2.0
                offsets.append((srv - local_mid) * 1000.0)
                time.sleep(0.05)
    except Exception as exc:
        warn("clock", f"could not measure: {str(exc).strip().splitlines()[0]}")
        return
    off = statistics.median(offsets)
    spread = max(offsets) - min(offsets)
    if abs(off) >= CLOCK_FAIL_MS:
        bad("subscriber clock offset", f"{off:+.0f} ms - this is what produced "
                                       f"Round 2's negative lag samples")
        info("", "on both machines:  w32tm /resync  then re-run this")
    elif abs(off) >= CLOCK_WARN_MS:
        warn("subscriber clock offset", f"{off:+.0f} ms - record it, or resync "
                                        f"with w32tm /resync on both")
    else:
        ok("subscriber clock offset", f"{off:+.1f} ms   (spread {spread:.1f} ms "
                                      f"over 7 samples)")


def check_schema(pubc, subc):
    head("[5] Schema, publication and the reset function")
    if pubc is None:
        bad("schema", "no publisher connection")
        return
    # orchestrate.py calls reset_workload_table() before EVERY level. If it is
    # missing, every level fails in the same way and the campaign produces
    # nothing - twelve hours for a stack trace repeated eleven times.
    fn = one(pubc, "SELECT to_regproc('reset_workload_table') IS NOT NULL")[0]
    if fn:
        ok("reset_workload_table()", "present on the publisher")
    else:
        bad("reset_workload_table()", "MISSING - orchestrate.py calls it "
                                      "before every level; every level would "
                                      "fail identically")
        info("", "re-apply 01_schema_publisher.sql on the publisher")

    pt = rows(pubc, "SELECT pubname, count(*) FROM pg_publication_tables "
                    "GROUP BY pubname")
    if any(p == "mypub" and n == 1 for p, n in pt):
        ok("publication mypub", "1 table")
    else:
        bad("publication mypub", f"expected exactly 1 table, found {pt}")

    if subc is None:
        bad("subscriber schema", "no subscriber connection")
        return
    cols = ("SELECT column_name, data_type, "
            "coalesce(character_maximum_length, numeric_precision, -1) "
            "FROM information_schema.columns WHERE table_name='ingest_data' "
            "ORDER BY ordinal_position")
    pc, sc = rows(pubc, cols), rows(subc, cols)
    if pc and pc == sc:
        ok("ingest_data columns", f"{len(pc)} columns, identical")
    else:
        bad("ingest_data columns", "publisher and subscriber differ")
    idx = "SELECT count(*) FROM pg_indexes WHERE tablename='ingest_data'"
    pi, si = one(pubc, idx)[0], one(subc, idx)[0]
    if pi == si == 4:
        ok("indexes", "4 on each (primary key + 3 secondary)")
    else:
        bad("indexes", f"publisher {pi}, subscriber {si}, expected 4 and 4")

    ri = one(pubc, "SELECT relreplident FROM pg_class "
                   "WHERE relname='ingest_data'")
    if ri and ri[0] == "d":
        ok("replica identity", "default (the primary key)")
    elif ri:
        warn("replica identity", f"'{ri[0]}' rather than 'd'")


def check_replication(pubc, subc):
    head("[6] Replication")
    if pubc is None:
        return
    slots = rows(pubc, "SELECT slot_name, active, active_pid, wal_status, "
                       "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), "
                       "restart_lsn)) FROM pg_replication_slots "
                       "WHERE slot_type='logical'")
    if len(slots) == 1:
        sn, active, pid, wstat, retained = slots[0]
        if active:
            ok("logical slot", f"'{sn}' active (pid {pid}), wal_status "
                               f"{wstat}, retaining {retained}")
        else:
            bad("logical slot", f"'{sn}' exists but is INACTIVE - no standby "
                                f"is attached, so nothing will replicate")
        if wstat not in ("reserved", "extended"):
            bad("slot wal_status", f"{wstat} - WAL the standby still needs has "
                                   f"been removed or is at risk")
    elif not slots:
        bad("logical slot", "none - there is no subscription to measure")
    else:
        bad("logical slots", f"{len(slots)}: "
                             f"{[s[0] for s in slots]}. The harness takes "
                             f"LIMIT 1 and would read an arbitrary one")

    reps = rows(pubc, "SELECT application_name, state, "
                      "coalesce(write_lag::text,'-'), "
                      "coalesce(replay_lag::text,'-') "
                      "FROM pg_stat_replication")
    mine = [r for r in reps if r[0] == "mysub"]
    if not mine:
        bad("pg_stat_replication", f"no row with application_name='mysub'. "
                                   f"Saw: {[r[0] for r in reps] or 'nothing'}. "
                                   f"Every part of the harness matches on that "
                                   f"exact string.")
    else:
        appname, state, wl, rl = mine[0]
        if state == "streaming":
            ok("pg_stat_replication", f"mysub streaming, write_lag {wl}, "
                                      f"replay_lag {rl}")
        else:
            bad("pg_stat_replication", f"mysub is '{state}', not 'streaming'")

    if subc is None:
        return
    subs = rows(subc, "SELECT subname, subenabled, substream, subbinary, "
                      "subsynccommit, subslotname FROM pg_subscription")
    if len(subs) != 1:
        bad("subscriptions on the subscriber", f"{len(subs)} - expected "
                                               f"exactly 1: {[s[0] for s in subs]}")
    else:
        n, en, st, bi, sc_, slot = subs[0]
        if n != "mysub":
            bad("subscription name", f"'{n}' - must be 'mysub'")
        elif not en:
            bad("subscription", "mysub is DISABLED")
        else:
            ok("subscription", f"mysub enabled, streaming={st}, binary={bi}, "
                               f"synchronous_commit={sc_}, slot={slot}")
        if str(st) not in ("f", "False", "off"):
            warn("subscription streaming", f"{st} - the study specifies off")
        if bi:
            warn("subscription binary", "true - the study specifies false")

    # Caught up? A backlog now becomes the first level's "clean start"
    # failure, and orchestrate.py aborts on it.
    try:
        st = one(subc, "SELECT received_lsn = latest_end_lsn FROM "
                       "pg_stat_subscription WHERE subname='mysub'")
        if st is not None and st[0] is True:
            ok("subscriber caught up", "received_lsn = latest_end_lsn")
        elif st is not None:
            warn("subscriber caught up", "not yet - it will catch up before "
                                         "the first level, but check the "
                                         "backlog is small")
    except Exception:
        pass

    pn = one(pubc, "SELECT count(*) FROM ingest_data")[0]
    sn = one(subc, "SELECT count(*) FROM ingest_data")[0]
    # orchestrate.py truncates before EVERY level, so leftover rows genuinely
    # do not affect the experiment - and an earlier version of this check said
    # exactly that and passed them.
    #
    # But start_campaign.ps1 runs verify_setup.py --smoke FIRST, and that
    # refuses to smoke-test a table holding more than SMOKE_MAX_EXISTING_ROWS
    # rows. It is a fatal check, verify_setup exits 2, and start_campaign
    # prints "Setup is not ready. Nothing was started." So a full table does
    # not spoil the data - it stops the campaign from starting at all, which
    # a GO from this script has no business hiding.
    try:
        import verify_setup as _vs
        cap = int(getattr(_vs, "SMOKE_MAX_EXISTING_ROWS", 100_000))
        src = "verify_setup.SMOKE_MAX_EXISTING_ROWS"
    except Exception:
        cap = 100_000
        src = "assumed 100,000"
    if max(pn or 0, sn or 0) > cap:
        bad("ingest_data row count", f"publisher {pn:,}, subscriber {sn:,} - "
                                    f"over the {cap:,} that verify_setup's "
                                    f"smoke test allows ({src})")
        info("", "start_campaign.ps1 runs verify_setup.py --smoke first and")
        info("", "exits without starting anything. Empty the table:")
        info("", '  py -c "import os,psycopg2;c=psycopg2.connect('
                 'os.environ[\'R3_PUB_DSN\']);c.autocommit=True;'
                 'k=c.cursor();k.execute(\'SELECT reset_workload_table()\');'
                 'k.execute(\'VACUUM ANALYZE ingest_data\')"')
        info("", "TRUNCATE replicates, so the subscriber empties too.")
    else:
        ok("ingest_data row count", f"publisher {pn:,}, subscriber {sn:,} - "
                                    f"under verify_setup's {cap:,} smoke-test "
                                    f"limit")
        info("", "orchestrate.py truncates before every level anyway")


def check_helpers(pubc, subc, sub_dsn):
    """The two subscriber-side windows.

    These were in RUN_GUIDE_CENTRALUS.md for the region families and missing
    from the A-E guide entirely, because start_campaign.ps1 says nothing
    about them. Without the monitor there is no apply-side series for any run
    in the campaign, and validate_run.py reports that as a WARNING and still
    says PASS - so eleven runs complete, every report looks fine, and
    rows_applied_per_sec does not exist for any of them.

    The two processes run on the same machine but connect to DIFFERENT
    servers, so they have to be looked for in different places: the monitor
    in the SUBSCRIBER's pg_stat_activity, the relay in the PUBLISHER's.
    """
    head("[7] Subscriber-side helpers")
    if subc is None:
        bad("subscriber helpers", "no subscriber connection")
        return
    seen = {a for (a,) in rows(subc,
            "SELECT application_name FROM pg_stat_activity "
            "WHERE application_name <> ''")}
    if any(a.startswith("r3_monitor_subscriber") for a in seen):
        ok("subscriber monitor", "connected (r3_monitor_subscriber)")
    else:
        bad("subscriber monitor", "NOT running on the subscriber")
        info("", "Without it this campaign records no apply-side data at all,")
        info("", "and validate_run.py reports that as a warning, not a")
        info("", "failure - so every run would 'pass' with the subscriber")
        info("", "series missing. On the SUBSCRIBER:")
        info("", "   cd C:\\r3\\scripts ;  .\\start_subscriber_monitor.ps1")

    host = ""
    m = re.search(r"host=(\S+)", sub_dsn or "")
    if m:
        host = m.group(1)
    relays = [a for (a,) in rows(pubc,
              "SELECT coalesce(host(client_addr), 'local') "
              "FROM pg_stat_activity "
              "WHERE application_name LIKE 'r3_state_relay%%'")] \
        if pubc is not None else []
    mine = [a for a in relays
            if a == host or (a == "local" and host in ("127.0.0.1",
                                                       "localhost", "::1"))]
    if mine:
        ok("state relay", f"connected to the publisher from {mine[0]}")
    elif relays:
        bad("state relay", f"a relay is connected, but from {relays}, not "
                           f"from {host}")
    else:
        bad("state relay", "no r3_state_relay connected to the publisher")
        info("", "Without it the subscriber cannot tell which run is in")
        info("", "progress and labels every sample 'pending', which")
        info("", "validate_run.py cannot match to a run. On the SUBSCRIBER:")
        info("", "   cd C:\\r3\\scripts ;  py state_relay.py")


def check_disk(pubc, subc, sub_root):
    head("[8] Free space on both data volumes")
    try:
        import psutil
    except ImportError:
        warn("publisher disk", "psutil not installed - cannot measure")
        psutil = None

    if pubc is not None:
        dd = one(pubc, "SELECT current_setting('data_directory')")[0]
        info("publisher data_directory", dd)
        if psutil:
            vol = os.path.splitdrive(dd)[0] + os.sep if os.name == "nt" else "/"
            try:
                free = psutil.disk_usage(vol).free / 1024 ** 3
                if free <= DISK_LOW_GB:
                    bad("publisher free space", f"{free:.0f} GB on {vol} - at "
                                                f"or below the {DISK_LOW_GB:.0f} "
                                                f"GB DISK_LOW floor")
                elif free < DISK_WANT_GB:
                    warn("publisher free space", f"{free:.0f} GB on {vol} - "
                                                 f"above the floor but below "
                                                 f"the {DISK_WANT_GB:.0f} GB "
                                                 f"this campaign wants")
                else:
                    ok("publisher free space", f"{free:.0f} GB on {vol}")
            except Exception as exc:
                warn("publisher free space", str(exc))

    # Free space is not available through SQL, so read it out of the most
    # recent capture_environment.py output ON the subscriber, pulled over
    # port 5432 the same way state_relay.py pulls the campaign state.
    if subc is None:
        return
    try:
        sroot = sub_root.replace("\\", "/").rstrip("/")
        sdata = f"{sroot}/data"
        names = [n for (n,) in rows(subc, "SELECT pg_ls_dir(%s)", (sdata,))
                 if n.startswith("environment_subscriber") and
                 n.endswith(".json")]
        if not names:
            warn("subscriber free space", f"no environment_subscriber_*.json "
                                          f"in {sdata} on that machine - run "
                                          f"capture_environment.py there")
            return
        newest = sorted(names)[-1]
        # (x) is a parenthesised string, not a one-element tuple. psycopg2
        # then tries to bind each character and reports "not all arguments
        # converted during string formatting", which names neither the query
        # nor the cause. The trailing comma is the whole fix.
        txt = one(subc, "SELECT pg_read_file(%s)",
                  (f"{sdata}/{newest}",))[0]
        env = json.loads(txt)
        parts = (env.get("host") or {}).get("disk_partitions") or []
        cap = env.get("captured_utc", "?")
        dd = one(subc, "SELECT current_setting('data_directory')")[0]
        hit = _match_volume(dd, parts)
        if hit is None:
            warn("subscriber free space", f"could not match {dd} to a volume "
                                          f"in {newest}")
            return
        free = float(hit.get("free_gb") or 0)
        detail = f"{free:.0f} GB on {hit.get('device')}  (from {newest}, {cap})"
        if free <= DISK_LOW_GB:
            bad("subscriber free space", detail + " - at or below the "
                                                  "DISK_LOW floor")
        elif free < DISK_WANT_GB:
            warn("subscriber free space", detail + f" - below the "
                                                   f"{DISK_WANT_GB:.0f} GB "
                                                   f"this campaign wants")
        else:
            ok("subscriber free space", detail)
    except Exception as exc:
        warn("subscriber free space", f"could not read it: "
                                      f"{str(exc).strip().splitlines()[0]}")


def _match_volume(data_dir, parts):
    """Find the volume a data directory sits on, from a psutil partition list.

    os.path.splitdrive() is the obvious way to pull "F:" off "F:/pgdata18",
    but on Linux os.path IS posixpath, which does not recognise drive letters
    at all - so that version of this function could only ever work on the
    machine it was going to run on, and could not be tested anywhere else.
    A drive letter is a two-character pattern; matching it directly works on
    either platform, and the POSIX branch makes the whole check exercisable
    in the Linux test rig.
    """
    dd = str(data_dir or "")
    m = re.match(r"^([A-Za-z]):", dd)
    if m:
        want = (m.group(1) + ":").upper()
        for p in parts:
            if str(p.get("device", "")).upper().startswith(want):
                return p
        return None
    # POSIX: the volume is the partition with the longest mountpoint that is
    # a prefix of the path. "/" always matches, so it is the natural fallback.
    best = None
    for p in parts:
        mp = str(p.get("mountpoint") or p.get("device") or "")
        if not mp:
            continue
        if dd == mp or dd.startswith(mp.rstrip("/") + "/") or mp == "/":
            if best is None or len(mp) > len(str(best.get("mountpoint") or "")):
                best = p
    return best


def check_prior(camp, data):
    head("[9] Prior run artefacts in the data directory")
    fams = [f["family"] for f in camp.get("families", [])]
    try:
        names = os.listdir(data)
    except Exception as exc:
        bad("data directory", str(exc))
        return
    clash = {}
    for n in names:
        if not os.path.isfile(os.path.join(data, n)):
            continue
        for fam in fams:
            if re.search(rf"_{re.escape(fam)}_rep\d+", "_" + n) or \
               re.search(rf"(?:^|_){re.escape(fam)}_rep\d+", n):
                clash.setdefault(fam, []).append(n)
                break
    ck = [n for n in names
          if n.startswith("checkpoint_") and
          camp.get("name", "") in n]
    if not clash and not ck:
        ok("no clashing artefacts", f"nothing for {', '.join(fams)}")
        return
    for fam in sorted(clash):
        bad(f"prior files for {fam}", f"{len(clash[fam])} file(s) - the "
                                      f"monitor will APPEND to their event "
                                      f"logs")
        # NAME them. An earlier version reported only a count, and when
        # archive_prior_runs.py disagreed about which files were run
        # artefacts there was no way for anyone - including me - to see what
        # the disagreement was about.
        for n in sorted(clash[fam]):
            info("  ", n)
    for n in ck:
        bad("resume checkpoint present", f"{n} - the supervisor would treat "
                                         f"those runs as already done")
    info("", "archive them, keeping them as evidence:")
    info("", "  py archive_prior_runs.py")
    info("", "  py archive_prior_runs.py --apply --reason \"...\"")


def check_running(pubc, subc, data, root):
    head("[10] Nothing already running")
    if pubc is not None:
        who = [a for (a,) in rows(pubc,
               "SELECT application_name FROM pg_stat_activity "
               "WHERE application_name <> '' AND pid <> pg_backend_pid()")]
        mon = [a for a in who if a.startswith("r3_monitor_publisher")]
        orc = [a for a in who if a.startswith("r3_orchestrator")]
        lg = [a for a in who if a.startswith("r3_loadgen")]
        if mon:
            bad("publisher monitor", "already connected. monitor.py exits 4 "
                                     "rather than append to another monitor's "
                                     "CSV, so the supervisor would run with no "
                                     "publisher samples at all.")
        else:
            ok("publisher monitor", "not running - the supervisor will start it")
        if orc or lg:
            bad("campaign in progress", f"{orc + lg} connected - something is "
                                        f"already running")
        else:
            ok("no orchestrator or loadgen", "clear")
    ph = os.environ.get("R3_PHASE_FILE") or os.path.join(root,
                                                         "phase_state.json")
    if os.path.exists(ph):
        try:
            with open(ph, encoding="utf-8") as fh:
                p = json.load(fh)
            st = p.get("phase") or p.get("phase_state")
            if st and st not in ("idle", "done", "adhoc"):
                warn("phase_state.json", f"says '{st}' - a previous campaign "
                                         f"may not have finished cleanly")
            else:
                ok("phase_state.json", f"'{st}'")
        except Exception:
            warn("phase_state.json", "present but unreadable")
    else:
        ok("phase_state.json", "absent")


def check_plan(camp, stage):
    head("[11] What will actually run")
    total = 0.0
    OVERHEAD = 90.0
    print(f"  {'family':<16}{'levels':>7}{'reps':>6}   {'rate':<30}"
          f"{'hours':>7}")
    print("  " + "-" * 66)
    for f in camp.get("families", []):
        mpath = os.path.join(HERE, f["config"])
        if not os.path.isfile(mpath):
            bad(f"matrix for {f['family']}", f"{f['config']} not found")
            continue
        try:
            with open(mpath, encoding="utf-8") as fh:
                m = json.load(fh)
        except Exception as exc:
            bad(f"matrix for {f['family']}", f"not valid JSON: {exc}")
            continue
        lv = m.get("levels") or []
        if not lv:
            bad(f"matrix for {f['family']}", "no levels")
            continue

        # The campaign's family name and the matrix's family field must be
        # the same string. The supervisor builds the run id the SUBSCRIBER
        # monitor sees (via campaign_state.json and state_relay.py) from the
        # campaign's name; orchestrate.py builds the run id the PUBLISHER
        # files use from the MATRIX's family field. If they differ you get
        # manifest_A_calibration_rep1.json beside
        # subscriber_A_rigtest_rep1.csv, validate_run.py can never pair them,
        # and it exits 2 with no explanation of why. Demonstrated in the rig.
        mf = m.get("family")
        if mf != f["family"]:
            bad(f"family name mismatch",
                f"campaign says '{f['family']}', {f['config']} says "
                f"'{mf}'. The publisher and subscriber CSVs would get "
                f"different run ids and validate_run.py could not pair them.")
        reps = f.get("repeats") or [1]
        load = sum(l.get("duration_sec", 360) for l in lv)
        hrs = (load + OVERHEAD * len(lv)) * len(reps) / 3600.0
        total += hrs
        # Use orchestrate.py's OWN rule for how a level's rate is set
        # (lines 658-663): target_wal_mbps wins, else commit_rate, else
        # --unthrottled. Absence of both is therefore LEGAL and means
        # unthrottled - which is how family A's A07-A10 and the whole probe
        # are written. An earlier version of this check demanded one of the
        # three keys and declared a perfectly good calibration matrix a
        # NO-GO on ten levels.
        def mode(l):
            if l.get("target_wal_mbps"):
                return "wal"
            if l.get("commit_rate"):
                return "commit"
            return "unthrottled"

        modes = [mode(l) for l in lv]
        bits = []
        if "wal" in modes:
            tg = sorted({l["target_wal_mbps"] for l in lv
                         if mode(l) == "wal"})
            bits.append(", ".join(str(t) for t in tg) + " MB/s")
        if "commit" in modes:
            cr = sorted({l["commit_rate"] for l in lv if mode(l) == "commit"})
            bits.append(f"{cr[0]}-{cr[-1]}/s" if len(cr) > 1
                        else f"{cr[0]}/s")
        if "unthrottled" in modes:
            bits.append(f"unthrottled x{modes.count('unthrottled')}")
        tgs = " + ".join(bits)
        print(f"  {f['family']:<16}{len(lv):>7}{len(reps):>6}   {tgs:<30}"
              f"{hrs:>7.2f}")
        rh = m.get("retarget_history")
        if rh:
            info("", f"  retargeted {len(rh)}x, last: "
                     f"{rh[-1].get('reason', '')[:60]}")

        no_dur = [l.get("level_id") for l in lv if not l.get("duration_sec")]
        if no_dur:
            bad(f"{f['family']} levels", f"no duration_sec: {no_dur}")
        # Both keys present is not fatal - orchestrate silently prefers
        # target_wal_mbps - but it means the file says two things and only
        # one of them is what runs.
        both = [l.get("level_id") for l in lv
                if l.get("target_wal_mbps") and l.get("commit_rate")]
        if both:
            warn(f"{f['family']} levels", f"{both} set BOTH target_wal_mbps "
                                          f"and commit_rate; orchestrate.py "
                                          f"uses target_wal_mbps and ignores "
                                          f"the other")
        no_cl = [l.get("level_id") for l in lv if not l.get("clients")]
        if no_cl:
            bad(f"{f['family']} levels", f"no clients: {no_cl}")

    print("  " + "-" * 66)
    print(f"  {'TOTAL':<16}{'':>7}{'':>6}   {'':<30}{total:>7.2f}")
    fin = time.localtime(time.time() + total * 3600)
    print()
    info("starts", time.strftime("%a %H:%M", time.localtime()))
    info("expected finish", time.strftime("%a %H:%M", fin) +
         "   (plus validation between runs)")
    return total


# ---------------------------------------------------------------------------
def _verify_setup_gates():
    """Every check() call in verify_setup.py, read out of its source.

    WHY THIS IS PARSED RATHER THAN LISTED
    -------------------------------------
    Three times now I mirrored a hand-picked subset of verify_setup.py's fatal
    gates into this script and missed one - most recently the row-count gate,
    which stops start_campaign.ps1 dead before anything runs. A list I maintain
    by hand is wrong the moment verify_setup gains a check. So the list is
    derived from verify_setup's own syntax tree, and group [12] below then runs
    the real thing rather than re-implementing any of it.

    Returns (all_gates, smoke_only) where each is a list of
    (name, fatal, enclosing_function).
    """
    import ast
    src = os.path.join(HERE, "verify_setup.py")
    with open(src, encoding="utf-8") as fh:
        tree = ast.parse(fh.read(), src)

    def name_of(node):
        a = node.args[0] if node.args else None
        if isinstance(a, ast.Constant) and isinstance(a.value, str):
            return a.value
        if isinstance(a, ast.JoinedStr):
            out = []
            for v in a.values:
                if isinstance(v, ast.Constant):
                    out.append(str(v.value))
                else:
                    out.append("{...}")
            return "".join(out)
        return "<computed>"

    def is_fatal(node):
        for kw in node.keywords:
            if kw.arg == "fatal":
                if isinstance(kw.value, ast.Constant):
                    return bool(kw.value.value)
                return True
        return True

    out = []
    for fn in ast.walk(tree):
        if not isinstance(fn, ast.FunctionDef):
            continue
        for n in ast.walk(fn):
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) \
                    and n.func.id == "check":
                out.append((name_of(n), is_fatal(n), fn.name))
    smoke = [g for g in out if g[2] in ("run_smoke", "smoke_monitor")]
    return out, smoke


def check_verify_setup(stage, camp_file, pub_dsn, sub_dsn, data, root,
                       with_smoke):
    """Run the launcher's own gate, instead of guessing at it.

    start_campaign.ps1 step 2 is:

        py verify_setup.py --smoke --campaign <campaign>
        if ($LASTEXITCODE -eq 2) { "Setup is not ready. Nothing was started." }

    So verify_setup has an absolute veto on the campaign starting, and a GO
    from this script that has not consulted it is worthless. Every earlier
    build mirrored a subset of its checks by hand. This runs it.

    Without --with-smoke the child is invoked WITHOUT --smoke, so it stays a
    read-only check like the rest of this script: verify_setup's smoke test
    inserts 5,000 rows, TRUNCATEs the table and runs the monitor for five
    seconds. The gates that live inside that block are listed by name below,
    every time, so what was not exercised is never invisible.
    """
    head("[12] verify_setup.py - the gate start_campaign.ps1 actually runs")
    vs = os.path.join(HERE, "verify_setup.py")
    if not os.path.isfile(vs):
        bad("verify_setup.py", "missing - start_campaign.ps1 would fail here")
        return

    try:
        gates, smoke_gates = _verify_setup_gates()
        n_fatal = sum(1 for g in gates if g[1])
        n_smoke_fatal = sum(1 for g in smoke_gates if g[1])
        info("gates declared in verify_setup",
             f"{len(gates)} checks, {n_fatal} of them fatal")
    except Exception as exc:
        gates = smoke_gates = []
        n_fatal = n_smoke_fatal = 0
        warn("could not parse verify_setup's gates", str(exc)[:70])

    cmd = [sys.executable, vs, "--campaign", camp_file]
    if with_smoke:
        cmd.append("--smoke")
    env = os.environ.copy()
    # Pass through whatever THIS script resolved, so a --pub-dsn given on the
    # command line is the DSN the child checks too. Anything not overridden is
    # inherited untouched, which is exactly what start_campaign.ps1 gives it.
    if pub_dsn:
        env["R3_PUB_DSN"] = pub_dsn
    if sub_dsn:
        env["R3_SUB_DSN"] = sub_dsn
    if data:
        env["R3_DATA"] = data
    env.setdefault("R3_STOP_FILE", os.path.join(root, "STOP"))
    env.setdefault("R3_LATENCY_FILE", os.path.join(root, "LATENCY_SET"))

    shown = " ".join(["py", "verify_setup.py"] + cmd[2:])
    info("running", shown)
    t0 = time.time()
    try:
        p = subprocess.run(cmd, cwd=HERE, env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           timeout=600, text=True,
                           encoding="utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        bad("verify_setup.py", "did not finish in 10 minutes. Run it on its "
                               "own: py verify_setup.py --campaign "
                               + camp_file)
        return
    except Exception as exc:
        bad("verify_setup.py", f"{type(exc).__name__}: {str(exc)[:70]}")
        return
    out = p.stdout or ""
    dt = time.time() - t0

    # verify_setup prints "  OK    name   detail" / "  FAIL  ..." / "  WARN  ..."
    line_re = re.compile(r"^  (OK|FAIL|WARN)\s{2,}(\S.*?)\s{2,}(.*)$")
    seen_ok = seen_fail = seen_warn = 0
    fails, warns = [], []
    for ln in out.splitlines():
        m = line_re.match(ln.rstrip())
        if not m:
            continue
        kind, name, detail = m.group(1), m.group(2).strip(), m.group(3).strip()
        if kind == "OK":
            seen_ok += 1
        elif kind == "FAIL":
            seen_fail += 1
            fails.append((name, detail))
        else:
            seen_warn += 1
            warns.append((name, detail))

    info("verify_setup ran", f"exit {p.returncode} in {dt:.0f}s - "
                             f"{seen_ok} OK, {seen_fail} FAIL, "
                             f"{seen_warn} WARN")

    for name, detail in fails:
        bad(f"verify_setup: {name}", detail[:110])
    for name, detail in warns:
        warn(f"verify_setup: {name}", detail[:110])

    if p.returncode == 2 and not fails:
        # It vetoed the campaign and this script could not say why. Never
        # swallow that - print what it said.
        bad("verify_setup verdict", "exit 2 (NOT READY) but no FAIL line was "
                                    "parsed. Its output, verbatim:")
        for ln in out.splitlines()[-40:]:
            info("  ", ln[:110])
    elif p.returncode == 2:
        info("", "start_campaign.ps1 would print \"Setup is not ready. "
                 "Nothing was started.\"")
    elif p.returncode in (0, 1) and not fails:
        ok("verify_setup verdict",
           f"READY (exit {p.returncode}) - every fatal gate passed")
    elif p.returncode not in (0, 1, 2):
        bad("verify_setup exit code", f"{p.returncode} - unexpected. Its "
                                      f"output, verbatim:")
        for ln in out.splitlines()[-40:]:
            info("  ", ln[:110])

    if with_smoke:
        ok("smoke test", "exercised - the same command the launcher runs")
        return

    # Name what was skipped. Every time.
    info("", "")
    info("NOT exercised (no --smoke)",
         f"{len(smoke_gates)} gate(s), {n_smoke_fatal} fatal, live inside "
         f"verify_setup's smoke test:")
    _seen = set()
    for name, fatal, fn in smoke_gates:
        if name in _seen:
            continue
        _seen.add(name)
        info("  ", ("FATAL  " if fatal else "warn   ") + name)
    info("", "The one that has actually stopped a campaign - the ingest_data")
    info("", "row count - is checked directly in group [6] above.")
    info("", "To exercise all of them, exactly as start_campaign.ps1 will:")
    info("", f"  py preflight_campaign.py --stage {stage} --with-smoke")
    info("", "(that one is NOT read-only: it writes 5,000 rows, TRUNCATEs")
    info("", " ingest_data and runs the monitor for 5 s)")

# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="One GO/NO-GO before a Round 3 campaign. Reads only.")
    ap.add_argument("--stage", required=True, choices=sorted(STAGES),
                    help="which campaign you are about to start")
    ap.add_argument("--pub-dsn", default=None)
    ap.add_argument("--sub-dsn", default=None)
    ap.add_argument("--out", default=None, help="data directory")
    ap.add_argument("--root", default=None,
                    help="the publisher's C:\\r3 equivalent, used to locate "
                         "the STOP and phase files")
    ap.add_argument("--with-smoke", action="store_true",
                    help="in group [12], run verify_setup.py --smoke, which "
                         "is what start_campaign.ps1 runs. NOT read-only: it "
                         "inserts 5,000 rows, TRUNCATEs ingest_data and runs "
                         "the monitor for 5 s. Use it for the final GO.")
    ap.add_argument("--sub-root", default=None,
                    help="the SUBSCRIBER's C:\\r3 equivalent. Defaults to "
                         "--root, which is right when both machines use the "
                         "same layout, as they do here.")
    a = ap.parse_args()

    print(f"\n{B}Round 3 campaign pre-flight - stage '{a.stage}'{Z}")
    mode = ("SMOKE TEST INCLUDED - writes 5,000 rows, then TRUNCATEs"
            if a.with_smoke else "reads only, changes nothing")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S %Z')}     {mode}")
    print(f"  build {BUILD}     {CHECK_GROUPS} check groups")

    camp = check_scripts(a.stage)
    pub_dsn, sub_dsn, data, root = check_env()
    pub_dsn = a.pub_dsn or pub_dsn
    sub_dsn = a.sub_dsn or sub_dsn
    data = a.out or data
    root = a.root or root
    sub_root = a.sub_root or root

    pubc = subc = None
    if pub_dsn:
        try:
            pubc = connect(pub_dsn, "r3_preflight")
            v = one(pubc, "SELECT current_setting('server_version'), "
                          "inet_server_addr()::text")
            ok("publisher reachable", f"PG {v[0]} at {v[1] or 'local'}")
        except Exception as exc:
            bad("publisher", str(exc).strip().splitlines()[0])
    if sub_dsn:
        try:
            subc = connect(sub_dsn, "r3_preflight")
            v = one(subc, "SELECT current_setting('server_version'), "
                          "inet_server_addr()::text")
            ok("subscriber reachable", f"PG {v[0]} at {v[1] or 'local'}")
        except Exception as exc:
            bad("subscriber", str(exc).strip().splitlines()[0])

    if pubc is None:
        print(f"\n{R}Cannot reach the publisher. Nothing else can be "
              f"checked.{Z}\n")
        return 2

    pv = one(pubc, "SELECT current_setting('server_version_num')::int")[0]
    if subc is not None:
        sv = one(subc, "SELECT current_setting('server_version_num')::int")[0]
        if pv == sv:
            ok("server_version_num", str(pv))
        else:
            bad("server_version_num", f"publisher {pv}, subscriber {sv}")
    if pv // 10000 != 18:
        bad("PostgreSQL major", f"{pv // 10000} - Round 3 is 18 only")

    check_config_spec(pubc, subc)
    check_clock(pubc, subc)
    check_schema(pubc, subc)
    check_replication(pubc, subc)
    check_helpers(pubc, subc, sub_dsn)
    check_disk(pubc, subc, sub_root)
    if camp:
        check_prior(camp, data)
    check_running(pubc, subc, data, root)
    if camp:
        check_plan(camp, a.stage)
    check_verify_setup(a.stage, STAGES[a.stage], pub_dsn, sub_dsn, data, root,
                       a.with_smoke)

    print("\n" + "=" * 78)
    if _fail:
        print(f"{R}NO-GO - {len(_fail)} blocking problem(s){Z}")
        for n, d in _fail:
            print(f"   x {n}: {d}")
        if _warn:
            print(f"{Y}plus {len(_warn)} warning(s){Z}")
            for n, d in _warn:
                print(f"   ! {n}: {d}")
        print("=" * 78)
        print("\n  Fix these, then run this again. Nothing was changed.\n")
        return 1

    if _warn:
        print(f"{Y}GO with {len(_warn)} warning(s){Z}")
        for n, d in _warn:
            print(f"   ! {n}: {d}")
    else:
        print(f"{G}GO - every check passed{Z}")
    print("=" * 78)
    print(f"\n  Start it:\n\n      .\\start_campaign.ps1 -Stage {a.stage}\n")
    print("  Watch it:   Get-Content C:\\r3\\data\\supervisor.log -Wait -Tail 30")
    print("  Stop it:    New-Item C:\\r3\\STOP -ItemType File")
    print()
    print("  AFTERWARDS, and this is not optional - the subscriber's CSVs are")
    print("  written on the SUBSCRIBER and validate_run.py reads both CSVs")
    print("  from this machine's data directory:")
    print()
    print("      py collect_subscriber.py --dry-run")
    print("      py collect_subscriber.py")
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted\n")
        sys.exit(1)
