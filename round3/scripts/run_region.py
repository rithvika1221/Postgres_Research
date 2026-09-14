#!/usr/bin/env python3
"""
Round 3 - run ONE region's campaign end to end, from the PUBLISHER.

    py run_region.py --init                       write regions.json template
    py run_region.py --region centralus --plan     read-only, changes nothing
    py run_region.py --region centralus            do it

Run the four regions in this order, one at a time:

    centralus  ->  eastus  ->  northeurope  ->  centralindia

Order matters for one reason only: centralus is the 0 ms control, and if the
harness is broken you want to find out on the machine next door rather than
after paying for a round trip to India. Nothing in the analysis depends on the
order.

WHAT THIS REPLACES
------------------
The per-region sequence used to be eleven manual steps across two machines,
and three of them were silent-failure traps:

  * forgetting to disable the other regions' subscriptions, so the publisher
    feeds two standbys and the offered load is not what the matrix says
  * CREATE SUBSCRIPTION with the default copy_data = true, which drags the
    whole table across the ocean before the run starts
  * leaving the subscriber's CSV on the subscriber, where validate_run.py
    cannot see it, so every apply-side metric is lost after the VM is gone

All three are now impossible rather than documented.

THE ONE-SLOT RULE
-----------------
Every subscriber's subscription is named mysub, because monitor.py,
orchestrate.py, supervisor.py, validate_run.py and verify_setup.py all match
on application_name = 'mysub' and slot_type = 'logical' LIMIT 1. A per-region
subscription name would make the harness blind to the replication it is
measuring.

That means the four subscriptions cannot coexist: they would all want the slot
named mysub on the publisher, and the harness's LIMIT 1 would pick an
arbitrary one. So this script keeps exactly ONE subscription alive at a time -
it drops the others, creates this region's fresh, and drops it again at the
end. That also removes the WAL-retention problem: an idle slot pins WAL on the
publisher for as long as it exists, and three idle slots across a
multi-region campaign is how a publisher runs out of disk overnight.

WHAT STILL HAPPENS ON THE SUBSCRIBER
------------------------------------
Two windows, started before this script and left running:

    py state_relay.py
    .\\start_subscriber_monitor.ps1

They stay there because monitor.py --role subscriber records host counters -
CPU, disk queue, free space - with psutil, and psutil reports the machine it
is running ON. Running the subscriber monitor from the publisher would record
the publisher's hardware under the subscriber's name. This script verifies
both are alive before it starts the campaign, and refuses to start if they
are not.

EXIT CODES
----------
  0  region finished and every run validated
  1  something failed; nothing is left half-configured
  2  bad arguments or config
  3  the campaign ran but at least one run failed validation
"""

import argparse
import datetime as dt
import json
import os
import re
import signal
import shutil
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
CONFIG = os.path.join(HERE, "regions.json")

# Offered load. Must be identical at every point or the comparison is not of
# distance. Kept here only to size the smoke test and the sanity checks; the
# authoritative copy is LEVEL in make_region_family.py.
TARGET_MBPS = 6

RTT_SAMPLES = 200
SMOKE_ROWS = 2000
SMOKE_TIMEOUT = 120
STREAM_TIMEOUT = 120
READ_CHUNK = 1 << 20          # 1 MiB per pg_read_file call

G = "\033[32m"; R = "\033[31m"; Y = "\033[33m"; B = "\033[1m"; Z = "\033[0m"
if os.name == "nt" and not os.environ.get("WT_SESSION"):
    G = R = Y = B = Z = ""

_fails = []
_warns = []


def head(msg):
    print(f"\n{B}{msg}{Z}")


def ok(name, detail=""):
    print(f"  {G}OK{Z}    {name:<34} {detail}")


def bad(name, detail=""):
    print(f"  {R}FAIL{Z}  {name:<34} {detail}")
    _fails.append((name, detail))


def warn(name, detail=""):
    print(f"  {Y}WARN{Z}  {name:<34} {detail}")
    _warns.append((name, detail))


def info(name, detail=""):
    print(f"        {name:<34} {detail}")


def act(msg):
    print(f"  {B}DO{Z}    {msg}")


def die(msg, code=1):
    print(f"\n{R}STOP{Z}  {msg}\n")
    raise SystemExit(code)


def redact(dsn):
    return re.sub(r"password=\S+", "password=***", dsn or "")


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
TEMPLATE = {
    "_comment": [
        "Round 3 region map. Passwords are NOT stored here - this file is safe",
        "to keep with the scripts. The password comes from the environment:",
        "set R3_PGPASSWORD once, machine-wide, on the publisher.",
        "",
        "Fill in every 'host'. Get them from:  bash hw_inventory.sh",
        "vm_size, disk_caching and apply_mbps are the hardware record that goes",
        "into the manuscript. apply_mbps is what preflight_region.py measured on",
        "that machine; leave it null until you have run preflight there.",
    ],
    "order": ["centralus", "eastus", "northeurope", "centralindia"],
    "publisher": {
        "host": "10.0.1.4",
        "port": 5432,
        "dbname": "pub",
        "user": "postgres",
        "repl_user": "repl_user",
        "azure_region": "centralus",
        "root": "C:\\r3",
    },
    "regions": {
        "centralus": {
            "vm": "r3-sub", "host": "10.0.1.5", "port": 5432, "dbname": "sub",
            "user": "postgres", "root": "C:\\r3",
            "vm_size": "Standard_D8ads_v6", "disk_caching": "None",
            "apply_mbps": None, "expected_rtt_ms": 0.4,
        },
        "eastus": {
            "vm": "r3-sub-eastus", "host": "10.1.1.4", "port": 5432,
            "dbname": "sub", "user": "postgres", "root": "C:\\r3",
            "vm_size": "Standard_D8nds_v6", "disk_caching": "ReadWrite",
            "apply_mbps": None, "expected_rtt_ms": 27.0,
        },
        "northeurope": {
            "vm": "FILL-ME", "host": "10.2.1.4", "port": 5432,
            "dbname": "sub", "user": "postgres", "root": "C:\\r3",
            "vm_size": "Standard_D8ds_v6", "disk_caching": "FILL-ME",
            "apply_mbps": None, "expected_rtt_ms": 100.0,
        },
        "centralindia": {
            "vm": "r3-sub-centralindia", "host": "10.3.1.4", "port": 5432,
            "dbname": "sub", "user": "postgres", "root": "C:\\r3",
            "vm_size": "CONFIRM-WITH-hw_inventory", "disk_caching": "ReadWrite",
            "apply_mbps": None, "expected_rtt_ms": 215.0,
        },
    },
}


def write_template(path):
    if os.path.exists(path):
        die(f"{path} already exists. Delete it first if you really want a "
            f"fresh template - it may be the only record of your host "
            f"addresses.", 2)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(TEMPLATE, fh, indent=2)
    print(f"\nwrote {path}\n")
    print("Now do three things:")
    print("  1. replace every FILL-ME (bash hw_inventory.sh prints all of them)")
    print("  2. set the password once, machine-wide, on the publisher:")
    print('       setx R3_PGPASSWORD "your-postgres-password" /M')
    print("     then open a NEW window so it is visible")
    print("  3. py run_region.py --region centralus --plan\n")


def load_config(path):
    if not os.path.exists(path):
        die(f"no {path}.  Create it with:  py run_region.py --init", 2)
    try:
        with open(path, encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as exc:
        die(f"{path} is not valid JSON: {exc}", 2)
    for key in ("publisher", "regions", "order"):
        if key not in cfg:
            die(f"{path} has no '{key}' section.", 2)
    return cfg


def dsn_of(node, password):
    missing = [k for k in ("host", "dbname", "user") if not node.get(k)]
    if missing:
        return None, f"missing {', '.join(missing)}"
    if str(node["host"]).upper().startswith("FILL"):
        return None, "host is still FILL-ME"
    parts = [f"host={node['host']}", f"port={node.get('port', 5432)}",
             f"dbname={node['dbname']}", f"user={node['user']}"]
    if password:
        parts.append(f"password={password}")
    parts.append("connect_timeout=20")
    return " ".join(parts), None


# ---------------------------------------------------------------------------
# sql helpers
# ---------------------------------------------------------------------------
def connect(dsn, appname):
    c = psycopg2.connect(dsn, application_name=appname)
    c.autocommit = True          # CREATE/DROP SUBSCRIPTION refuse a tx block
    return c


def one(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        row = cur.fetchone()
    return row


def rows(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)
        return cur.fetchall()


def run(conn, sql, args=None):
    with conn.cursor() as cur:
        cur.execute(sql, args)


# ---------------------------------------------------------------------------
# phases
# ---------------------------------------------------------------------------
_child = None
# A supervisor writes its heartbeat every few seconds. Anything fresher than
# this means a campaign is live right now.
HEARTBEAT_STALE_SEC = 300.0


def _stop_child(signum, _frame):
    """Take the supervisor down with us, then leave."""
    c = _child
    if c is not None and c.poll() is None:
        print(f"\n{Y}signal {signum} - stopping the supervisor "
              f"(pid {c.pid}) before exiting{Z}")
        try:
            c.terminate()
            c.wait(timeout=60)
        except Exception:
            try:
                c.kill()
            except Exception:
                pass
        print("supervisor stopped. The subscription and the publisher slot "
              "are still in place; re-run with --resume, or with "
              "--keep-subscription to leave them for a manual look.")
    raise SystemExit(130)


def phase_connect(cfg, region, password):
    """Open the publisher and every reachable subscriber."""
    head("[1] Connections")

    pub_dsn, err = dsn_of(cfg["publisher"], password)
    if err:
        die(f"publisher entry in regions.json: {err}", 2)
    try:
        pub = connect(pub_dsn, "r3_run_region")
    except Exception as exc:
        die(f"cannot reach the publisher at {redact(pub_dsn)}\n      {exc}")
    ver, num = one(pub, "SELECT version(), current_setting("
                        "'server_version_num')::int")
    ok("publisher", f"{cfg['publisher']['host']}  PG {num // 10000}."
                    f"{num % 10000 // 100}")

    subs = {}
    unreachable = []
    for name, node in cfg["regions"].items():
        dsn, err = dsn_of(node, password)
        if err:
            if name == region:
                die(f"regions.json entry for '{region}': {err}", 2)
            warn(f"skipping {name}", err)
            unreachable.append(name)
            continue
        try:
            subs[name] = (connect(dsn, "r3_run_region"), dsn, node)
            mark = " <- this run" if name == region else ""
            ok(f"subscriber {name}", f"{node['host']}{mark}")
        except Exception as exc:
            if name == region:
                die(f"cannot reach the {region} subscriber at "
                    f"{redact(dsn)}\n      {exc}\n\n      This is the machine "
                    f"this run is about. Check the VM is running, the peering "
                    f"is Connected both ways, and the publisher's pg_hba.conf "
                    f"covers its subnet.")
            msg = str(exc).strip().splitlines()[0]
            warn(f"subscriber {name}", f"unreachable - {msg[:60]}")
            unreachable.append(name)

    if unreachable:
        info("", "unreachable subscribers cannot have their subscription")
        info("", "dropped from here, so exclusivity is verified on the")
        info("", "PUBLISHER's slot list instead, which is authoritative.")
    return pub, pub_dsn, subs, unreachable


def phase_busy(pub, plan, root, out):
    """Refuse to touch a publisher that is already running something.

    THIS IS THE ONE THAT COSTS A CAMPAIGN.

    Phase [4] drops every subscription it can reach and phase [5] TRUNCATEs
    ingest_data. Both are correct at the start of a region run and catastrophic
    in the middle of somebody else's. Family F comes straight after the ten-hour
    B-E campaign, so "is B-E actually finished?" is a question that gets asked
    at exactly the wrong moment - and until now nothing here asked it at all.

    A second way in is an orphan: killing run_region.py does NOT kill the
    supervisor it spawned. The supervisor keeps running, with a live
    subscription and a live campaign, and the next region run walks straight
    into it. The publisher monitor's connection is what gives that away.
    """
    head("[0] Nothing already running on the publisher")
    busy = []

    try:
        who = [a for (a,) in rows(pub,
               "SELECT application_name FROM pg_stat_activity "
               "WHERE application_name <> '' AND pid <> pg_backend_pid()")]
    except Exception as exc:
        warn("pg_stat_activity", str(exc).strip().splitlines()[0])
        who = []

    for prefix, what in (("r3_orchestrator", "an orchestrator"),
                         ("r3_loadgen", "a load generator"),
                         ("r3_monitor_publisher", "a publisher monitor")):
        hits = [a for a in who if a.startswith(prefix)]
        if hits:
            busy.append(f"{what} ({', '.join(sorted(set(hits)))})")

    # A supervisor waiting in its health gate holds no connection of its own,
    # but it writes a heartbeat every few seconds.
    state = os.path.join(root, "campaign_state.json")
    if os.path.exists(state):
        try:
            with open(state, encoding="utf-8") as fh:
                st = json.load(fh)
            hb = st.get("heartbeat_utc") or ""
            age = None
            if hb:
                t = dt.datetime.fromisoformat(hb.replace("Z", "+00:00"))
                if t.tzinfo is None:
                    t = t.replace(tzinfo=dt.timezone.utc)
                age = (dt.datetime.now(dt.timezone.utc) - t).total_seconds()
            # A fresh heartbeat is not enough on its own. The supervisor
            # writes one final heartbeat as it parks, so for the next few
            # minutes a FINISHED campaign looks exactly like a running one -
            # which had this refusing to start a region run right after the
            # campaign before it ended. The state file says outright when it
            # is over: status goes terminal and finished_utc appears.
            done = bool(st.get("finished_utc")) or \
                str(st.get("status", "")).lower() in ("finished", "halted",
                                                      "aborted", "failed",
                                                      "stopped")
            if done:
                info("campaign_state.json",
                     f"'{st.get('campaign', '?')}' status "
                     f"'{st.get('status')}'"
                     + (f", finished {age/60:.0f} min ago" if age is not None
                        else "") + " - not running")
            elif age is not None and age < HEARTBEAT_STALE_SEC:
                busy.append(f"a supervisor heartbeat {age:.0f} s old in "
                            f"{os.path.basename(state)} "
                            f"(campaign '{st.get('campaign', '?')}', status "
                            f"'{st.get('status')}')")
            elif age is not None:
                warn("campaign_state.json",
                     f"'{st.get('campaign', '?')}' never recorded a finish, "
                     f"and its last heartbeat was {age/60:.0f} min ago - it "
                     f"died rather than stopped. Treated as not running.")
        except Exception as exc:
            warn("campaign_state.json", f"unreadable: {str(exc)[:50]}")

    phase = os.environ.get("R3_PHASE_FILE") or os.path.join(root,
                                                            "phase_state.json")
    if os.path.exists(phase):
        try:
            with open(phase, encoding="utf-8") as fh:
                pj = json.load(fh)
            ph = pj.get("phase") or pj.get("phase_state")
            if ph and ph not in ("idle", "done", "adhoc"):
                busy.append(f"phase_state.json says '{ph}'")
        except Exception:
            pass

    if not busy:
        ok("publisher is idle", "no orchestrator, loadgen or monitor connected")
        return True

    for b in busy:
        bad("already running", b)
    info("", "Phase [4] drops every subscription it can reach and phase [5]")
    info("", "TRUNCATEs ingest_data. Doing that now would destroy whatever is")
    info("", "running, and the run that was in progress would not report it.")
    info("", "")
    info("", "If B-E is still going, wait for it. If you killed run_region.py")
    info("", "and the supervisor it started is still alive, stop that first:")
    info("", f"   New-Item {os.path.join(root, 'STOP')} -ItemType File")
    info("", "and wait for the supervisor window to exit.")
    if plan:
        warn("plan mode", "continuing the plan, but a LIVE run would stop here")
        return False
    die("the publisher is busy. Nothing was changed.", 4)


def phase_parity(pub, subs, region):
    """Everything that must be identical, checked rather than assumed."""
    head("[2] Parity between publisher and this subscriber")
    sub, sub_dsn, node = subs[region]

    pv = one(pub, "SELECT current_setting('server_version_num')::int")[0]
    sv = one(sub, "SELECT current_setting('server_version_num')::int")[0]
    if pv == sv:
        ok("server_version_num", str(pv))
    else:
        bad("server_version_num", f"publisher {pv}, subscriber {sv} - "
                                  f"do not mix versions")
    if pv // 10000 != 18:
        bad("PostgreSQL major", f"{pv // 10000} - Round 3 is PostgreSQL 18 only")

    pubrel = rows(pub, "SELECT pubname, count(*) FROM pg_publication_tables "
                       "GROUP BY pubname")
    if any(p == "mypub" and n == 1 for p, n in pubrel):
        ok("publication mypub", "1 table")
    else:
        bad("publication mypub", f"expected exactly 1 table, found {pubrel}")

    # Column-by-column, because a type that differs only in precision applies
    # fine and then silently rounds every value.
    cols_sql = ("SELECT column_name, data_type, "
                "coalesce(character_maximum_length, numeric_precision, -1) "
                "FROM information_schema.columns "
                "WHERE table_name='ingest_data' ORDER BY ordinal_position")
    pc, sc = rows(pub, cols_sql), rows(sub, cols_sql)
    if pc == sc:
        ok("ingest_data columns", f"{len(pc)} columns, identical")
    else:
        bad("ingest_data columns", "publisher and subscriber differ")
        pset, sset = set(pc), set(sc)
        for c in sorted(pset - sset):
            info("  only on publisher", str(c))
        for c in sorted(sset - pset):
            info("  only on subscriber", str(c))

    idx_sql = ("SELECT count(*) FROM pg_indexes WHERE tablename='ingest_data'")
    pi, si = one(pub, idx_sql)[0], one(sub, idx_sql)[0]
    if pi == si == 4:
        ok("indexes on ingest_data", "4 on each (primary key + 3 secondary)")
    else:
        bad("indexes on ingest_data", f"publisher {pi}, subscriber {si}, "
                                      f"expected 4 and 4")
    return



def phase_settings(pub, subs, region, plan):
    """Audit BOTH servers against postgresql_settings.md.

    The four ad-hoc checks this replaces compared wal_level,
    wal_compression, synchronous_commit and track_commit_timestamp between the
    two servers and said nothing about the other nineteen settings the study
    specifies. That was enough to notice that the Central US control
    subscriber had synchronous_commit = on, and nowhere near enough to notice
    WHY: the machine had never received the Round 3 configuration block at
    all, and was running stock defaults - shared_buffers 128MB against the
    specified 8GB, checkpoint_timeout 5min against 15min, track_io_timing off.

    Comparing the two servers to EACH OTHER would not have caught it either,
    because most of these settings legitimately differ between a publisher and
    a subscriber. Each server has to be compared against its own section of
    the specification, which is what this does.
    """
    head("[3] Both servers against postgresql_settings.md")
    sub, _, node = subs[region]

    try:
        import check_config
        import pg_spec
        _spec_all, _src, _applied = pg_spec.load()
        pg_spec.announce(_src, _applied, printer=lambda m: print("  " + m))
    except SystemExit as exc:
        bad("config audit", str(exc))
        return
    except Exception as exc:
        bad("config audit", f"could not import check_config.py: {exc}")
        return

    worst = 0
    for role, conn, label in (("publisher", pub, "publisher"),
                              ("subscriber", sub, f"{region} subscriber")):
        try:
            rows = check_config.audit(conn, role, _spec_all[role])
        except Exception as exc:
            bad(f"{label} audit", str(exc).strip().splitlines()[0])
            continue
        diffs = [r for r in rows if not r[4]]
        blocking = [r for r in diffs if r[3] in ("physics", "monitor")]
        soft = [r for r in diffs if r[3] not in ("physics", "monitor")]

        if not diffs:
            ok(f"{label}", f"all {len(rows)} specified settings match")
            continue

        for name, expected, actual, severity, _m, context, _n in diffs:
            line = f"{name} = {actual}, specified {expected}"
            if context == "postmaster":
                line += "  (restart)"
            if severity in ("physics", "monitor"):
                bad(f"{label}", line)
            else:
                warn(f"{label}", line)
        worst = max(worst, len(blocking))

        if blocking:
            info("", f"{len(blocking)} of these change what is measured or "
                     f"blank a recorded column.")
        if soft:
            info("", f"{len(soft)} are reported in Methods and must match "
                     f"across machines, but do not move the measurement.")
        info("", f"See every one, with the reason:  py check_config.py "
                 f"--role {role}")
        info("", f"Fix them all:                   py check_config.py "
                 f"--role {role} --fix")

    if worst and not plan:
        info("", "")
        info("", "A campaign run now would not be comparable with the other")
        info("", "regions. Fix the configuration, RESTART that server, then")
        info("", "re-run this command.")


def phase_exclusivity(pub, subs, region, unreachable, plan):
    """Exactly one subscription alive, on this region, with one slot."""
    head("[4] One subscriber at a time")

    # EVERY reachable subscriber, this region included. This region's own
    # subscription is dropped too, not kept: it is recreated in phase [6]
    # after both tables are empty, which is the only sequence in which
    # copy_data = false is honest. Leaving it in place would also leave its
    # slot active here, which cannot be told apart from a foreign standby
    # still being fed - the exact thing this phase exists to rule out.
    for name in sorted(subs):
        sub, _, node = subs[name]
        mine = " (this region - will be recreated fresh)" if name == region else ""
        existing = rows(sub, "SELECT subname, subenabled FROM pg_subscription")
        if not existing:
            ok(f"{name}", f"no subscription - nothing to remove{mine}")
            continue
        for subname, enabled in existing:
            if plan:
                act(f"drop subscription '{subname}' on {name} "
                    f"({'enabled' if enabled else 'disabled'}){mine}")
                continue
            # Order matters. Disable first so the apply worker stops, then
            # detach the slot so DROP is purely local - otherwise DROP tries
            # to reach the publisher to delete the slot and wedges if it
            # cannot. The publisher-side slot is cleaned up below, from the
            # publisher, where it actually lives.
            try:
                run(sub, f'ALTER SUBSCRIPTION "{subname}" DISABLE')
                run(sub, f'ALTER SUBSCRIPTION "{subname}" SET (slot_name = NONE)')
                run(sub, f'DROP SUBSCRIPTION "{subname}"')
                ok(f"{name}", f"dropped subscription '{subname}'{mine}")
            except Exception as exc:
                bad(f"{name}", f"could not drop '{subname}': "
                               f"{str(exc).strip().splitlines()[0]}")

    # Read the slot list AFTER the drops above, not before: a walsender takes
    # a moment to exit once its subscription is gone, and reporting the
    # pre-drop state would call every slot active.
    if not plan:
        deadline = time.time() + 30
        while time.time() < deadline:
            still = rows(pub, "SELECT count(*) FROM pg_replication_slots "
                              "WHERE slot_type='logical' AND active")
            if not still or still[0][0] == 0:
                break
            time.sleep(0.5)

    # The publisher's slot list is the authoritative answer to "how many
    # standbys am I feeding", and it does not depend on reaching every VM.
    slots = rows(pub, "SELECT slot_name, active, active_pid, wal_status, "
                      "pg_size_pretty(pg_wal_lsn_diff("
                      "pg_current_wal_lsn(), restart_lsn)) "
                      "FROM pg_replication_slots WHERE slot_type='logical'")
    for sname, active, pid, wstat, retained in slots:
        info("slot on publisher", f"{sname}  active={active}  "
                                  f"wal_status={wstat}  retaining {retained}")

    if plan:
        for sname, active, pid, wstat, retained in slots:
            act(f"drop publisher slot '{sname}' (it will be recreated for "
                f"{region})")
        act(f"create subscription mysub on {region} with copy_data = false")
        return

    for sname, active, pid, wstat, retained in slots:
        if active:
            bad("active slot", f"'{sname}' is still active (pid {pid}). A "
                               f"standby is being fed and the offered load "
                               f"would not be what the matrix says.")
            if unreachable:
                info("", f"unreachable: {', '.join(unreachable)} - one of "
                         f"those is probably it")
                info("", f"RDP to it and run:  ALTER SUBSCRIPTION mysub "
                         f"DISABLE;  then DROP SUBSCRIPTION mysub;")
            continue
        try:
            run(pub, "SELECT pg_drop_replication_slot(%s)", (sname,))
            ok("dropped stale slot", f"'{sname}' - WAL it was pinning is "
                                     f"now releasable")
        except Exception as exc:
            bad("slot cleanup", f"'{sname}': "
                                f"{str(exc).strip().splitlines()[0]}")

    if _fails:
        return
    left = rows(pub, "SELECT slot_name FROM pg_replication_slots "
                     "WHERE slot_type='logical'")
    if left:
        bad("slots remaining", f"{[s for (s,) in left]} - expected none "
                               f"before creating this region's")
    else:
        ok("publisher slots", "none - clean slate")


def phase_truncate(pub, subs, region, plan):
    head("[5] Empty both tables before the subscription exists")
    sub, _, node = subs[region]
    pn = one(pub, "SELECT count(*) FROM ingest_data")[0]
    sn = one(sub, "SELECT count(*) FROM ingest_data")[0]
    info("rows now", f"publisher {pn:,}   subscriber {sn:,}")

    if plan:
        act("TRUNCATE ingest_data on the publisher")
        act(f"TRUNCATE ingest_data on {region}")
        info("", "done with NO subscription attached, so nothing replicates "
                 "and copy_data = false is then correct")
        return

    # With no subscription attached this is a local operation on each side.
    # Doing it in this order - and before CREATE SUBSCRIPTION - is what makes
    # copy_data = false honest: both sides are genuinely empty, so there is
    # nothing to copy. copy_data = true here would drag the whole table over
    # the link before the run even starts.
    run(pub, "TRUNCATE TABLE ingest_data")
    run(sub, "TRUNCATE TABLE ingest_data")
    pn = one(pub, "SELECT count(*) FROM ingest_data")[0]
    sn = one(sub, "SELECT count(*) FROM ingest_data")[0]
    if pn == 0 and sn == 0:
        ok("both tables empty", "publisher 0, subscriber 0")
    else:
        bad("truncate", f"publisher {pn}, subscriber {sn} - expected 0 and 0")


def phase_subscribe(pub, subs, region, cfg, password, plan):
    head("[6] Create this region's subscription")
    sub, _, node = subs[region]
    p = cfg["publisher"]
    # The address the SUBSCRIBER uses to reach the publisher is not always the
    # address this script uses. This script runs ON the publisher, so it
    # connects locally; the subscriber has to come in over the peered VNet.
    # repl_host/repl_port allow those to differ - and are what lets the test
    # rig put a latency proxy on the replication path without also slowing
    # down the driver's own connection.
    conninfo = (f"host={p.get('repl_host') or p['host']} "
                f"port={p.get('repl_port') or p.get('port', 5432)} "
                f"dbname={p['dbname']} user={p.get('repl_user', 'repl_user')} "
                f"password={password}")
    stmt = ('CREATE SUBSCRIPTION mysub CONNECTION %s PUBLICATION mypub '
            'WITH (copy_data = false, streaming = off, '
            'synchronous_commit = off, binary = false)')

    if plan:
        act(f"CREATE SUBSCRIPTION mysub on {region}")
        info("  connection", redact(conninfo))
        info("  options", "copy_data=false, streaming=off, "
                          "synchronous_commit=off, binary=false")
        act("wait for pg_stat_replication to show mysub streaming")
        return False

    try:
        with sub.cursor() as cur:
            cur.execute(stmt.replace("%s", "%(c)s"), {"c": conninfo})
    except Exception as exc:
        bad("CREATE SUBSCRIPTION", str(exc).strip().splitlines()[0])
        info("", "the commonest cause is the publisher's pg_hba.conf not "
                 "covering this subscriber's subnet, or repl_user's password")
        return False
    ok("subscription mysub", "created with copy_data = false")

    # It is not enough that the statement succeeded. The walsender must
    # actually be streaming, with application_name = 'mysub', because that is
    # the exact string every part of the harness matches on.
    deadline = time.time() + STREAM_TIMEOUT
    seen = None
    while time.time() < deadline:
        r = rows(pub, "SELECT application_name, state FROM "
                      "pg_stat_replication")
        for appname, state in r:
            if appname == "mysub" and state == "streaming":
                ok("pg_stat_replication", "application_name='mysub', "
                                          "state='streaming'")
                seen = True
                break
        if seen:
            break
        if r:
            seen = r
        time.sleep(1)
    if seen is not True:
        bad("pg_stat_replication", f"no streaming 'mysub' after "
                                   f"{STREAM_TIMEOUT}s; saw {seen}")
        return False

    n = one(pub, "SELECT count(*) FROM pg_replication_slots "
                 "WHERE slot_type='logical'")[0]
    if n == 1:
        ok("logical slots", "exactly 1, as the harness assumes")
    else:
        bad("logical slots", f"{n} - the harness takes LIMIT 1 and would "
                             f"read an arbitrary one")
        return False
    return True


def phase_smoke(pub, subs, region, plan):
    """Prove rows actually arrive, before committing to a 22-minute run."""
    head("[7] Smoke test - do rows really cross")
    if plan:
        act(f"insert {SMOKE_ROWS} rows on the publisher and time their arrival")
        act("then truncate both sides again")
        return
    sub, _, node = subs[region]
    t0 = time.time()
    run(pub, """
        INSERT INTO ingest_data
            (account_id, region_code, status, event_type, quantity,
             unit_price, total_amount, external_ref, attributes, description)
        SELECT g, 'SMOKE', 'ok', 'smoke', 1, 1.0, 1.0,
               gen_random_uuid(), '{}'::jsonb, repeat('x', 200)
        FROM generate_series(1, %s) g
        """, (SMOKE_ROWS,))
    inserted = time.time() - t0

    deadline = time.time() + SMOKE_TIMEOUT
    got = 0
    while time.time() < deadline:
        got = one(sub, "SELECT count(*) FROM ingest_data")[0]
        if got >= SMOKE_ROWS:
            break
        time.sleep(0.25)
    arrived = time.time() - t0

    if got >= SMOKE_ROWS:
        ok("rows replicated", f"{got:,} in {arrived:.2f}s "
                              f"(insert took {inserted:.2f}s)")
    else:
        bad("rows replicated", f"only {got:,} of {SMOKE_ROWS:,} after "
                               f"{SMOKE_TIMEOUT}s")
        lag = rows(pub, "SELECT write_lag, flush_lag, replay_lag FROM "
                        "pg_stat_replication WHERE application_name='mysub'")
        info("", f"pg_stat_replication lag: {lag}")
        err = rows(sub, "SELECT last_error_time, last_error_message FROM "
                        "pg_stat_subscription_stats "
                        "WHERE subname='mysub'") if _has_substats(sub) else []
        if err and err[0][1]:
            info("subscriber error", str(err[0][1])[:160])
        return

    lag = rows(pub, "SELECT write_lag, flush_lag, replay_lag FROM "
                    "pg_stat_replication WHERE application_name='mysub'")
    if lag:
        info("lag right after the burst", str(lag[0]))

    run(pub, "TRUNCATE TABLE ingest_data")
    deadline = time.time() + 60
    while time.time() < deadline:
        if one(sub, "SELECT count(*) FROM ingest_data")[0] == 0:
            break
        time.sleep(0.25)
    pn = one(pub, "SELECT count(*) FROM ingest_data")[0]
    sn = one(sub, "SELECT count(*) FROM ingest_data")[0]
    if pn == 0 and sn == 0:
        ok("cleaned up", "both tables back to 0 (TRUNCATE is replicated)")
    else:
        run(sub, "TRUNCATE TABLE ingest_data")
        sn = one(sub, "SELECT count(*) FROM ingest_data")[0]
        if pn == 0 and sn == 0:
            ok("cleaned up", "both tables back to 0 (subscriber truncated "
                             "locally)")
        else:
            bad("cleanup", f"publisher {pn}, subscriber {sn}")


def _has_substats(conn):
    try:
        return one(conn, "SELECT to_regclass('pg_stat_subscription_stats') "
                         "IS NOT NULL")[0]
    except Exception:
        return False


def phase_helpers(pub, subs, region, plan):
    """The two subscriber-side windows. Forgetting them loses the region.

    These two processes run on the SAME machine but connect to DIFFERENT
    servers, so they have to be looked for in different places:

      monitor.py --role subscriber   connects to the SUBSCRIBER, so it shows
                                     up in the subscriber's pg_stat_activity
      state_relay.py                 connects to the PUBLISHER - that is the
                                     whole point of it, pulling the
                                     publisher's campaign_state.json through
                                     port 5432 - so it shows up in the
                                     PUBLISHER's pg_stat_activity, with
                                     client_addr equal to this subscriber's
                                     address

    Looking for the relay on the subscriber finds nothing even when it is
    running perfectly.
    """
    head("[8] Subscriber-side helpers must already be running")
    sub, _, node = subs[region]
    host = str(node.get("host", ""))
    if plan:
        act("look for r3_monitor_subscriber in the subscriber's "
            "pg_stat_activity")
        act(f"look for r3_state_relay in the PUBLISHER's pg_stat_activity "
            f"with client_addr {host}")
        return

    seen_sub = {a for (a,) in rows(sub,
                "SELECT application_name FROM pg_stat_activity "
                "WHERE application_name <> ''")}
    if any(a.startswith("r3_monitor_subscriber") for a in seen_sub):
        ok("subscriber monitor", "connected (r3_monitor_subscriber)")
    else:
        bad("subscriber monitor", "not connected to this subscriber")
        info("", "Without it there is NO apply-side data for this region and")
        info("", "validate_run.py will fail every run. On the subscriber:")
        info("", "   cd C:\\r3\\scripts ;  .\\start_subscriber_monitor.ps1")

    relays = rows(pub, "SELECT coalesce(host(client_addr), 'local') "
                       "FROM pg_stat_activity "
                       "WHERE application_name LIKE 'r3_state_relay%%'")
    addrs = [a for (a,) in relays]
    mine = [a for a in addrs
            if a == host or (a == "local" and host in ("127.0.0.1", "localhost",
                                                       "::1"))]
    if mine:
        ok("state relay", f"connected to the publisher from {mine[0]}")
    elif addrs:
        bad("state relay", f"a relay is connected, but from {addrs} - not "
                           f"from this region's subscriber ({host})")
        info("", "that is another region's relay. Start one on THIS machine,")
        info("", "or its samples will be labelled with the wrong run.")
    else:
        bad("state relay", "no r3_state_relay connected to the publisher")
        info("", "Without it the subscriber cannot see which run is in")
        info("", "progress and labels every sample 'pending', which")
        info("", "validate_run.py cannot match to a run. On the subscriber:")
        info("", "   cd C:\\r3\\scripts ;  py state_relay.py")

    if len(addrs) > 1:
        warn("state relays", f"{len(addrs)} relays are connected: {addrs}. "
                             f"Harmless, but a leftover relay on a region you "
                             f"are not running means that VM is still up and "
                             f"costing money.")


def phase_rtt(subs, region, given, plan):
    head("[9] Round-trip time")
    if plan:
        if given is not None:
            act(f"use the supplied RTT of {given} ms")
        else:
            act(f"measure {RTT_SAMPLES} application-level round trips to "
                f"{region}")
        return given, "planned"
    if given is not None:
        ok("RTT", f"{given:.1f} ms  (supplied with --rtt, measured on the "
                  f"subscriber)")
        return given, "subscriber"

    sub, _, node = subs[region]
    samples = []
    with sub.cursor() as cur:
        cur.execute("SELECT 1")          # warm the connection
        for _ in range(RTT_SAMPLES):
            t = time.perf_counter()
            cur.execute("SELECT 1")
            cur.fetchone()
            samples.append((time.perf_counter() - t) * 1000.0)
    samples.sort()
    med = statistics.median(samples)
    lo = samples[len(samples) // 4]
    hi = samples[min(len(samples) - 1, 3 * len(samples) // 4)]
    ok("measured median RTT", f"{med:.1f} ms   (IQR {lo:.1f} - {hi:.1f}, "
                              f"n={len(samples)})")
    expected = node.get("expected_rtt_ms")
    if expected and expected > 1 and abs(med - expected) > max(20.0, 0.5 * expected):
        warn("RTT vs expectation", f"measured {med:.1f} ms against an "
                                   f"expected {expected:.0f} ms - not an "
                                   f"error, but check the region is the one "
                                   f"you think")
    if hi - lo > max(2.0, 0.15 * med):
        warn("RTT stability", f"IQR spread {hi - lo:.1f} ms is wide for a "
                              f"fixed path - record the spread in Methods "
                              f"alongside the median")
    return med, "publisher"


def phase_family(region, rtt, node, rtt_source, plan, repeats, duration):
    head("[10] Build this region's matrix and campaign")
    # make_region_family.py writes the matrix and campaign next to itself.
    # Checked rather than discovered from a traceback halfway through: on a
    # machine where the scripts folder is not writable by the account running
    # the campaign, everything up to here succeeds and then this fails, after
    # the subscription has already been rebuilt.
    if not os.access(HERE, os.W_OK):
        bad("scripts folder", f"{HERE} is not writable by this account, and "
                              f"the matrix and campaign files are written "
                              f"there")
        info("", "run this from an elevated prompt, or move the scripts "
                 "somewhere writable")
        return False
    cmd = [sys.executable, os.path.join(HERE, "make_region_family.py"),
           "--region", region, "--rtt", f"{rtt:.1f}",
           "--vm-size", str(node.get("vm_size") or "UNKNOWN"),
           "--repeats", str(repeats)]
    if duration:
        cmd += ["--duration", str(duration)]
    if node.get("apply_mbps"):
        cmd += ["--apply-mbps", str(node["apply_mbps"])]
    if node.get("disk_caching"):
        cmd += ["--disk-caching", str(node["disk_caching"])]
    if plan:
        act(" ".join(os.path.basename(c) if c == cmd[1] else c for c in cmd[1:]))
        if not node.get("apply_mbps"):
            info("", "apply_mbps is null in regions.json. That number is what")
            info("", "defends the VM-size difference between regions - fill it")
            info("", "in from this region's preflight before the real run.")
        return True
    if not node.get("apply_mbps"):
        warn("apply_mbps", "null in regions.json - the hardware record for "
                           "this region will have no throughput headroom "
                           "figure")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    print(proc.stdout.rstrip())
    if proc.returncode != 0:
        bad("make_region_family.py", f"exit {proc.returncode}")
        print(proc.stderr.rstrip())
        return False
    for name in (f"matrix_F_lat_{region}.json", f"campaign_F_lat_{region}.json"):
        if os.path.exists(os.path.join(HERE, name)):
            ok("wrote", name)
        else:
            bad("missing", name)
            return False
    if rtt_source == "publisher":
        _patch_provenance(region, rtt)
    return True


def _patch_provenance(region, rtt):
    """Record honestly WHERE the RTT was measured from.

    make_region_family.py's default provenance says the median was taken on
    the subscriber. When this script measures it, the samples were taken from
    the publisher over the same peered path. Same path, same number to within
    the noise - but the file must say which, or the Methods sentence is not
    supported by the data behind it.
    """
    path = os.path.join(HERE, f"matrix_F_lat_{region}.json")
    try:
        with open(path, encoding="utf-8") as fh:
            m = json.load(fh)
        m.setdefault("rtt_provenance", {})["how"] = (
            f"run_region.py, {RTT_SAMPLES} application-level round trips "
            f"(SELECT 1) issued FROM THE PUBLISHER to the {region} subscriber "
            f"on one already-established connection over the peered VNet, so "
            f"TCP and TLS setup are not counted. Median reported. The path is "
            f"the same one replication uses, traversed in the opposite "
            f"direction; measuring from the publisher is what allows the "
            f"whole region run to be one command.")
        m["rtt_provenance"]["measured_from"] = "publisher"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(m, fh, indent=2)
        ok("provenance", "recorded that the RTT was measured from the publisher")
    except Exception as exc:
        warn("provenance", f"could not annotate {path}: {exc}")


_RUNID = re.compile(r"^(?:manifest|publisher|subscriber|run_report|validation|"
                    r"orchestrator|checkpoint)_(?P<rid>.+?)"
                    r"(?:_events)?(?:_final)?\.(?:json|csv|log|md|txt)$")


def _belongs_to(filename, family):
    """True if this artefact belongs to exactly this family.

    Artefacts are named <kind>_<run_id>[_suffix].<ext> and a run id is
    <family>_rep<N>. Compared on the parsed run id rather than by substring,
    so region names that are prefixes of each other cannot be confused.
    """
    m = _RUNID.match(filename)
    if not m:
        return False
    rid = m.group("rid")
    return rid == family or re.fullmatch(re.escape(family) + r"_rep\d+", rid) \
        is not None


def phase_prior(region, out, plan, keep):
    """Move a previous attempt's artefacts out of the way.

    monitor.py names its files <role>_<run_id>.csv and its event log
    <role>_<run_id>_events.log, and a run id is deterministic:
    F_lat_<region>_rep<N>. So re-running a region after fixing something
    reuses the same names and the monitor APPENDS to the previous attempt's
    event log.

    validate_run.py then reads that log and judges the new run against the
    OLD attempt's events. This was found the only way it could be found - by
    running the same region twice in the rig. The second run was clean, and
    validation failed it on ten DISK_LOW events timestamped eight minutes
    before it started. Nothing in the output said the events were stale.

    orchestrate.py has --run-suffix for the supervisor's automatic retries.
    A human re-running a whole region does not get that, so this does it
    instead: the old attempt is moved intact into a dated subfolder, where it
    is still available as evidence but cannot contaminate the new one.
    """
    head("[11a] Previous attempts at this region")
    fam = f"F_lat_{region}"
    try:
        names = os.listdir(out)
    except FileNotFoundError:
        names = []
    # Match the family EXACTLY. A substring test would let "eastus" match
    # "F_lat_eastus2_rep1", and those two regions are at different distances.
    stale = [n for n in names
             if _belongs_to(n, fam) and os.path.isfile(os.path.join(out, n))]
    if not stale:
        ok("no prior artefacts", f"nothing for {region} in {out}")
        return True
    dest = os.path.join(out, f"superseded_{region}_"
                             f"{time.strftime('%Y%m%d_%H%M%S')}")
    if plan:
        act(f"move {len(stale)} file(s) from a previous {region} attempt into "
            f"{os.path.basename(dest)}/")
        for n in sorted(stale)[:8]:
            info("  ", n)
        if len(stale) > 8:
            info("  ", f"... and {len(stale) - 8} more")
        return True
    if keep:
        warn("prior artefacts", f"{len(stale)} file(s) for {region} left in "
                                f"place (--keep-prior). The monitor will "
                                f"APPEND to the old event log and "
                                f"validate_run will judge this run against "
                                f"the previous attempt's events.")
        return True
    os.makedirs(dest, exist_ok=True)
    moved = 0
    for n in sorted(stale):
        try:
            shutil.move(os.path.join(out, n), os.path.join(dest, n))
            moved += 1
        except Exception as exc:
            bad("could not move", f"{n}: {exc}")
            return False
    ok("quarantined", f"{moved} file(s) from a previous attempt -> "
                      f"{os.path.basename(dest)}/")
    info("", "kept, not deleted: if this attempt is worse than the last one "
             "you still have both")
    return True


def phase_prior_resume(region, out, plan):
    """On a resume, quarantine only the artefacts of runs that did NOT finish.

    THE BUG THIS FIXES
    ------------------
    A resume must keep the completed runs' manifests - the supervisor reads
    them to know what is already done - so run_region deliberately did not
    move anything aside. But the run that was INTERRUPTED also left artefacts,
    and monitor.py opens its CSV in append mode: restarting that repeat appends
    the new attempt's samples underneath the aborted attempt's.

    Measured on the rig. A three-repeat region was interrupted during repeat 2
    and resumed. Repeat 2 then carried 107 load samples where its clean
    siblings carried 86 - the aborted attempt's 21 samples were still in the
    file, and validate_run.py passed it. Every rate and every lag median for
    that repeat was computed over two attempts mixed together, silently.

    So: read the checkpoint, keep everything belonging to a COMPLETED run, and
    move everything else for this family out of the way.
    """
    head("[resume-a] Artefacts of the run that did not finish")
    fam = f"F_lat_{region}"
    cp_path = os.path.join(out, f"checkpoint_round3_{fam}.json")
    if not os.path.exists(cp_path):
        warn("no checkpoint", f"{os.path.basename(cp_path)} is not in {out}. "
                              f"The supervisor will start this region fresh, "
                              f"so run it WITHOUT --resume instead - that path "
                              f"quarantines properly.")
        return False

    done = set()
    try:
        with open(cp_path, encoding="utf-8") as fh:
            cp = json.load(fh)
        for e in cp.get("completed", []):
            for k in ("artefact_run_id", "run_id"):
                if e.get(k):
                    done.add(str(e[k]))
    except Exception as exc:
        bad("checkpoint unreadable", str(exc)[:60])
        return False
    info("completed already", ", ".join(sorted(done)) or "none")

    rid_re = re.compile(rf"({re.escape(fam)}_rep\d+(?:_retry\d+)?)")
    try:
        names = os.listdir(out)
    except FileNotFoundError:
        names = []
    stale = []
    for n in names:
        full = os.path.join(out, n)
        if not os.path.isfile(full):
            continue
        if n == os.path.basename(cp_path):
            continue                      # the supervisor needs this
        if not _belongs_to(n, fam):
            continue
        m = rid_re.search(n)
        if not m:
            continue
        if m.group(1) not in done:
            stale.append(n)

    if not stale:
        ok("nothing to quarantine", "every artefact here belongs to a "
                                    "completed run")
        return True

    dest = os.path.join(out, f"superseded_{region}_resume_"
                             f"{time.strftime('%Y%m%d_%H%M%S')}")
    if plan:
        act(f"move {len(stale)} file(s) from the interrupted run into "
            f"{os.path.basename(dest)}/")
        for n in sorted(stale)[:8]:
            info("  ", n)
        return True
    try:
        os.makedirs(dest, exist_ok=True)
        for n in sorted(stale):
            shutil.move(os.path.join(out, n), os.path.join(dest, n))
        ok("quarantined", f"{len(stale)} file(s) from the interrupted run -> "
                          f"{os.path.basename(dest)}/")
        for n in sorted(stale)[:6]:
            info("  ", n)
        info("", "kept, not deleted - the partial attempt is still evidence")
    except Exception as exc:
        bad("could not quarantine", str(exc)[:70])
        info("", "resuming now would append the new attempt to the aborted")
        info("", "one's CSV. Move those files aside by hand first.")
        return False
    return True


def phase_campaign(region, out, plan, extra):
    head("[11] Run the campaign")
    campaign = os.path.join(HERE, f"campaign_F_lat_{region}.json")
    cmd = [sys.executable, os.path.join(HERE, "supervisor.py"),
           "--campaign", campaign, "--out", out] + list(extra)
    if plan:
        act(f"supervisor.py --campaign campaign_F_lat_{region}.json --out {out}")
        # In plan mode phase [10] did not write the campaign file, so this is
        # allowed to be absent. Reading it when it happens to exist from an
        # earlier run is useful; treating its absence as an error is not.
        if os.path.exists(campaign):
            try:
                with open(campaign, encoding="utf-8") as fh:
                    c = json.load(fh)
                reps = c["families"][0]["repeats"]
                lv = json.load(open(os.path.join(
                    HERE, f"matrix_F_lat_{region}.json"), encoding="utf-8"))
                dur = lv["levels"][0]["duration_sec"]
                info("", f"existing file: {len(reps)} repeat(s) x {dur} s")
            except Exception:
                pass
        else:
            info("", "the campaign file does not exist yet; phase [10] writes "
                     "it when this runs for real")
        return True
    print(f"        {' '.join(cmd[1:])}\n")
    t0 = time.time()
    # Popen, not subprocess.run, so the handlers installed in main() can put
    # this child down. Killing run_region.py used to leave the supervisor
    # running as an orphan - with a live subscription, a live campaign, and
    # none of the collect/validate/release phases that were supposed to
    # follow it.
    global _child
    proc = subprocess.Popen(cmd)
    _child = proc
    try:
        proc.wait()
    finally:
        _child = None
    mins = (time.time() - t0) / 60.0
    if proc.returncode == 0:
        ok("campaign", f"finished in {mins:.0f} min")
        return True
    bad("campaign", f"supervisor exited {proc.returncode} after {mins:.0f} min")
    info("", f"resume with:  py run_region.py --region {region} --resume")
    return False


def phase_collect(subs, region, out, plan):
    """Pull the subscriber's CSVs to the publisher, over port 5432.

    validate_run.py reads publisher_<run>.csv AND subscriber_<run>.csv from
    ONE directory - the publisher's --out. The subscriber monitor writes its
    CSV on the subscriber. Nothing copies it across on its own, and once the
    VM is deallocated it is gone, along with every apply-side metric for that
    region. There is no file share between these machines, so the transport
    is the one channel that definitely works: pg_read_file over the
    replication port, the same trick state_relay.py uses in the other
    direction.
    """
    head("[12] Collect the subscriber's CSVs")
    sub, _, node = subs[region]
    sub_root = (node.get("root") or "C:\\r3").rstrip("\\/")
    sub_data = f"{sub_root}/data".replace("\\", "/")

    if plan:
        act(f"list {sub_data} on the subscriber via pg_ls_dir")
        act(f"pull subscriber_*.csv into {out} via pg_read_file")
        return True

    if not one(sub, "SELECT pg_has_role(current_user, 'pg_read_server_files', "
                    "'USAGE') OR (SELECT rolsuper FROM pg_roles WHERE "
                    "rolname = current_user)")[0]:
        bad("file read rights", "this login can neither read server files nor "
                               "is it superuser; cannot pull the CSVs")
        return False

    try:
        listing = [n for (n,) in rows(sub, "SELECT pg_ls_dir(%s)", (sub_data,))]
    except Exception as exc:
        bad("pg_ls_dir", f"{sub_data}: {str(exc).strip().splitlines()[0]}")
        info("", "check the subscriber monitor's --out really is that folder")
        return False

    want = [n for n in listing if n.startswith("subscriber_") and
            n.endswith(".csv")]
    if not want:
        bad("subscriber CSVs", f"none in {sub_data} - the subscriber monitor "
                               f"never wrote any, so this region has no "
                               f"apply-side data")
        info("", f"files there: {', '.join(sorted(listing)[:12]) or 'none'}")
        return False

    okc = 0
    for name in sorted(want):
        remote = f"{sub_data}/{name}"
        try:
            # The subscriber monitor runs in --follow mode and is still alive
            # after the campaign ends, so the CSV can still be growing.
            # Copying it mid-write silently loses the last samples - which are
            # the drain samples, the most interesting ones. Wait for the size
            # to stop changing before reading a byte.
            size = _settle(sub, remote)
            buf = []
            off = 0
            while off < size:
                chunk = one(sub, "SELECT pg_read_file(%s, %s, %s)",
                            (remote, off, min(READ_CHUNK, size - off)))[0]
                if not chunk:
                    break
                buf.append(chunk)
                off += len(chunk.encode("utf-8", "replace"))
            text = "".join(buf)
            # A run-id-named file is unique across regions. The monitor's
            # pending/idle/startup files are NOT - every subscriber writes
            # subscriber_pending.csv - so those get the region in the name,
            # or region four silently overwrites region one's.
            local = name if _belongs_to(name, f"F_lat_{region}") \
                else name.replace("subscriber_", f"subscriber_{region}_", 1)
            dest = os.path.join(out, local)
            with open(dest, "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
            got = len(text.encode("utf-8", "replace"))
            lines = text.count("\n")
            if got == size:
                ok("pulled", f"{local}  {size:,} bytes, {lines:,} lines")
                okc += 1
            else:
                # Never report a short copy as success: this file is the only
                # apply-side record for the region and the VM is about to be
                # deallocated.
                bad("short copy", f"{name}: got {got:,} of {size:,} bytes")
        except Exception as exc:
            bad("pull failed", f"{name}: {str(exc).strip().splitlines()[0]}")

    # The events log is small and says why a run went wrong, so it is worth
    # having next to the CSV.
    for name in sorted(n for n in listing
                       if n.startswith("subscriber_") and n.endswith(".log")):
        remote = f"{sub_data}/{name}"
        try:
            text = one(sub, "SELECT pg_read_file(%s)", (remote,))[0]
            local = name if _belongs_to(name, f"F_lat_{region}") \
                else name.replace("subscriber_", f"subscriber_{region}_", 1)
            with open(os.path.join(out, local), "w", encoding="utf-8") as fh:
                fh.write(text)
            ok("pulled", local)
        except Exception:
            pass
    return okc > 0


def _settle(conn, remote, tries=20, gap=1.0):
    """Wait until a remote file's size stops changing, and return it."""
    last = -1
    for _ in range(tries):
        size = one(conn, "SELECT (pg_stat_file(%s)).size", (remote,))[0]
        if size == last:
            return size
        last = size
        time.sleep(gap)
    return last


def phase_validate(out, region, plan):
    head("[13] Validate every run")
    if plan:
        act("validate_run.py for each manifest written by this campaign")
        return True, []
    ids = []
    fam_want = f"F_lat_{region}"
    for name in sorted(os.listdir(out)):
        m = re.fullmatch(r"manifest_(.+)\.json", name)
        if not m:
            continue
        if not _belongs_to(name, fam_want):
            continue
        try:
            with open(os.path.join(out, name), encoding="utf-8") as fh:
                man = json.load(fh)
        except Exception:
            continue
        # Cross-check against the manifest's own family, not just the
        # filename, so a file copied under the wrong name is caught.
        if str(man.get("family", "")) not in ("", fam_want):
            warn("manifest family", f"{name} says family "
                                    f"'{man.get('family')}' but is named for "
                                    f"{fam_want} - skipped")
            continue
        ids.append(m.group(1))
    if not ids:
        bad("manifests", f"none for {region} in {out}")
        return False, []

    results = []
    for rid in ids:
        print()

        # The apply-side series, checked as a FAILURE rather than left to
        # validate_run.py - which reports a missing subscriber CSV as a
        # warning and still says PASS. On the rig this fired for real: the
        # subscriber monitor could not read the campaign state, labelled every
        # sample 'pending', and the run validated clean with no apply-side
        # data for it at all. For family F that is the whole point of the run,
        # because rows_applied_per_sec at 200 ms is the measurement.
        scsv = os.path.join(out, f"subscriber_{rid}.csv")
        if not os.path.isfile(scsv):
            stray = sorted(n for n in os.listdir(out)
                           if n.startswith("subscriber_")
                           and n.endswith(".csv"))
            bad(f"apply-side series for {rid}",
                f"no subscriber_{rid}.csv in {out}")
            info("", "validate_run.py treats this as a warning and still")
            info("", "passes the run. It is not a warning for family F.")
            if stray:
                info("", f"what IS here: {', '.join(stray[:6])}")
                if any("pending" in n or "idle" in n for n in stray):
                    info("", "a 'pending' name means the subscriber monitor "
                             "could not read")
                    info("", "the campaign state - state_relay.py was not "
                             "running, or was")
                    info("", "pointed at the wrong --root. The samples exist "
                             "but cannot be")
                    info("", "matched to a run, and re-running is the only "
                             "fix.")
        else:
            try:
                with open(scsv, encoding="utf-8") as fh:
                    nlines = sum(1 for _ in fh)
            except Exception:
                nlines = 0
            if nlines < 10:
                bad(f"apply-side series for {rid}",
                    f"subscriber_{rid}.csv has only {nlines} line(s)")
            else:
                ok(f"apply-side series for {rid}", f"{nlines - 1} samples")

        proc = subprocess.run(
            [sys.executable, os.path.join(HERE, "validate_run.py"),
             "--run-id", rid, "--out", out],
            capture_output=True, text=True)
        # The supervisor already ran validate_run once, BEFORE the subscriber
        # CSV existed on this machine, so validation_<id>.txt on disk carries
        # a "no subscriber CSV" warning that is no longer true. Write this
        # run - the one that actually saw both CSVs - under its own name so
        # the record is not the stale one.
        try:
            with open(os.path.join(out, f"validation_{rid}_final.txt"),
                      "w", encoding="utf-8") as fh:
                fh.write(proc.stdout)
                if proc.stderr:
                    fh.write("\n--- stderr ---\n" + proc.stderr)
        except Exception:
            pass
        tail = [l for l in proc.stdout.splitlines() if l.strip()][-1:] or [""]
        if proc.returncode == 0:
            ok(f"run {rid}", "PASS")
        else:
            bad(f"run {rid}", f"validate_run exit {proc.returncode} - {tail[0][:70]}")
            print(proc.stdout[-2500:])
        results.append((rid, proc.returncode))
    return all(rc == 0 for _, rc in results), results


def phase_release(pub, subs, region, plan, keep):
    head("[14] Release the link")
    if keep:
        info("--keep-subscription", "leaving mysub in place on " + region)
        return
    sub, _, node = subs[region]
    if plan:
        act(f"drop subscription mysub on {region} and its publisher slot")
        info("", "so the next region starts from one slot, and no idle slot "
                 "pins WAL on the publisher")
        return
    try:
        run(sub, "ALTER SUBSCRIPTION mysub DISABLE")
        run(sub, "ALTER SUBSCRIPTION mysub SET (slot_name = NONE)")
        run(sub, "DROP SUBSCRIPTION mysub")
        ok("subscription", f"dropped on {region}")
    except Exception as exc:
        warn("subscription", f"could not drop on {region}: "
                             f"{str(exc).strip().splitlines()[0]}")
    for (sname,) in rows(pub, "SELECT slot_name FROM pg_replication_slots "
                              "WHERE slot_type='logical' AND NOT active"):
        try:
            run(pub, "SELECT pg_drop_replication_slot(%s)", (sname,))
            ok("slot", f"dropped '{sname}' on the publisher")
        except Exception as exc:
            warn("slot", f"'{sname}': {str(exc).strip().splitlines()[0]}")


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Run one region's campaign end to end, from the publisher.")
    ap.add_argument("--region", help="which region, e.g. centralus")
    ap.add_argument("--config", default=CONFIG)
    ap.add_argument("--plan", action="store_true",
                    help="print every action and change nothing")
    ap.add_argument("--init", action="store_true",
                    help="write a regions.json template and exit")
    ap.add_argument("--rtt", type=float, default=None,
                    help="median RTT in ms measured ON THE SUBSCRIBER by "
                         "check_latency.py. Omit and this script measures it "
                         "from the publisher over the same path.")
    ap.add_argument("--out", default=None,
                    help="publisher data directory. Default $R3_DATA, then "
                         "<publisher root>\\data")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--duration", type=int, default=None,
                    help="override seconds per level (testing only - the "
                         "paper's runs are the matrix default)")
    ap.add_argument("--resume", action="store_true",
                    help="continue a campaign that was interrupted; skips "
                         "setup and goes straight to the supervisor")
    ap.add_argument("--keep-prior", action="store_true",
                    help="do not move a previous attempt's files aside. "
                         "Almost never right: the monitor appends to the old "
                         "event log and validate_run then fails this run on "
                         "the old attempt's problems.")
    ap.add_argument("--keep-subscription", action="store_true",
                    help="do not drop mysub at the end")
    ap.add_argument("--skip-campaign", action="store_true",
                    help="do everything except run the load. Use this to "
                         "prove a region is ready without paying for it.")
    ap.add_argument("--no-helper-check", action="store_true",
                    help="do not require the subscriber monitor and state "
                         "relay. Only for a --skip-campaign dry run.")
    args, extra = ap.parse_known_args()
    # A bare "--" separator survives parse_known_args and would be passed
    # through to the supervisor, which rejects it.
    extra = [a for a in extra if a != "--"]

    if args.init:
        write_template(args.config)
        return 0
    if not args.region:
        print("--region is required.  py run_region.py --region centralus "
              "--plan")
        return 2

    cfg = load_config(args.config)
    if args.region not in cfg["regions"]:
        print(f"'{args.region}' is not in {args.config}. Known regions: "
              f"{', '.join(cfg['regions'])}")
        return 2

    password = os.environ.get("R3_PGPASSWORD")
    if not password:
        m = re.search(r"password=(\S+)", os.environ.get("R3_PUB_DSN", ""))
        password = m.group(1) if m else None
    if not password:
        die("no password. Set it once, machine-wide, on the publisher:\n"
            '        setx R3_PGPASSWORD "your-postgres-password" /M\n'
            "      then open a NEW window so it is visible.", 2)

    root = cfg["publisher"].get("root") or "C:\\r3"
    out = args.out or os.environ.get("R3_DATA") or os.path.join(root, "data")
    if not args.plan:
        os.makedirs(out, exist_ok=True)

    order = cfg.get("order") or list(cfg["regions"])
    pos = order.index(args.region) + 1 if args.region in order else 0

    print(f"\n{B}Round 3 region run: {args.region}"
          f"{f'   ({pos} of {len(order)})' if pos else ''}{Z}")
    print(f"mode: {'PLAN - nothing will change' if args.plan else 'LIVE'}"
          f"     data: {out}")
    if pos:
        print(f"order: {'  ->  '.join(order)}")

    for sig in (getattr(signal, "SIGINT", None),
                getattr(signal, "SIGTERM", None),
                getattr(signal, "SIGBREAK", None)):
        if sig is not None:
            try:
                signal.signal(sig, _stop_child)
            except (ValueError, OSError):
                pass

    pub, pub_dsn, subs, unreachable = phase_connect(cfg, args.region, password)
    sub_dsn = subs[args.region][1]
    node = subs[args.region][2]

    # Before anything reads, writes, drops or truncates.
    phase_busy(pub, args.plan, root, out)

    # orchestrate.py, supervisor.py, monitor.py and verify_setup.py all read
    # these from the environment. Setting them here means the operator cannot
    # run a region's campaign with R3_SUB_DSN still pointing at the previous
    # region - which would attribute one region's apply data to another and be
    # invisible in the output.
    os.environ["R3_PUB_DSN"] = pub_dsn
    os.environ["R3_SUB_DSN"] = sub_dsn
    os.environ["R3_DATA"] = out
    info("R3_PUB_DSN", redact(pub_dsn))
    info("R3_SUB_DSN", redact(sub_dsn))

    if args.resume:
        head("[resume] skipping setup, going straight to the supervisor")
        info("", "completed runs' artefacts are kept - the supervisor needs "
                 "their manifests to know what is already done. The "
                 "INTERRUPTED run's artefacts are moved aside, because the "
                 "monitor would otherwise append to them.")
        if not phase_prior_resume(args.region, out, args.plan):
            return 1
        if not phase_campaign(args.region, out, False, extra + ["--resume"]):
            return 1
        phase_collect(subs, args.region, out, False)
        good, _ = phase_validate(out, args.region, False)
        return 0 if good else 3

    phase_parity(pub, subs, args.region)
    phase_settings(pub, subs, args.region, args.plan)
    if _fails and not args.plan:
        return finish(args, 1)

    phase_exclusivity(pub, subs, args.region, unreachable, args.plan)
    if _fails and not args.plan:
        return finish(args, 1)

    phase_truncate(pub, subs, args.region, args.plan)
    if _fails and not args.plan:
        return finish(args, 1)

    created = phase_subscribe(pub, subs, args.region, cfg, password, args.plan)
    if _fails and not args.plan:
        return finish(args, 1)

    phase_smoke(pub, subs, args.region, args.plan)
    if _fails and not args.plan:
        return finish(args, 1)

    if not args.no_helper_check:
        phase_helpers(pub, subs, args.region, args.plan)
        if _fails:
            return finish(args, 1)

    rtt, source = phase_rtt(subs, args.region, args.rtt, args.plan)
    if args.plan:
        rtt = rtt if rtt is not None else (node.get("expected_rtt_ms") or 1.0)

    if not phase_family(args.region, rtt, node, source, args.plan,
                        args.repeats, args.duration):
        return finish(args, 1)

    if args.skip_campaign:
        head("[11] Campaign skipped (--skip-campaign)")
        info("", f"this region is ready. Run it with:  py run_region.py "
                 f"--region {args.region}")
        return finish(args, 0)

    if not phase_prior(args.region, out, args.plan, args.keep_prior):
        return finish(args, 1)

    if not phase_campaign(args.region, out, args.plan, extra):
        return finish(args, 1)

    if not phase_collect(subs, args.region, out, args.plan):
        if not args.plan:
            bad("collection", "the subscriber's CSVs are NOT on the publisher. "
                              "Do not deallocate that VM yet.")
            return finish(args, 1)

    good, results = phase_validate(out, args.region, args.plan)
    phase_release(pub, subs, args.region, args.plan, args.keep_subscription)

    if args.plan:
        return finish(args, 0)
    return finish(args, 0 if good else 3)


def finish(args, code):
    print("\n" + "=" * 78)
    if _fails:
        print(f"{R}{len(_fails)} failure(s){Z}")
        for name, detail in _fails:
            print(f"   x {name}: {detail}")
    if _warns:
        print(f"{Y}{len(_warns)} warning(s){Z}")
        for name, detail in _warns:
            print(f"   ! {name}: {detail}")
    if not _fails and not _warns:
        print(f"{G}clean{Z}")
    print("=" * 78)

    if args.plan:
        print("\nPLAN ONLY - nothing was changed. Run it for real with the "
              "same command minus --plan.\n")
        return 0
    if code == 0 and not args.skip_campaign:
        print(f"\n{G}{args.region} is done and validated.{Z}")
        print("  Next region: see the 'order' list at the top of this output.")
        print("  Do NOT deallocate this region's VM until you have confirmed")
        print(f"  subscriber_*.csv for it is in {os.environ.get('R3_DATA')}.\n")
    elif code == 3:
        print(f"\n{Y}The campaign ran but at least one run failed "
              f"validation.{Z}")
        print("  Read run_report_<run-id>.md before re-running anything: a")
        print("  failed validation usually names the exact sample that broke.\n")
    return code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\ninterrupted\n")
        raise SystemExit(1)
